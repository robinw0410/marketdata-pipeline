"""Tests fuer fetch_histdata.py - nur synthetische Daten, HTTP gemockt."""
import io
import json
import re
import threading
import zipfile
from datetime import date, datetime, timedelta, timezone

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import fetch_dukascopy as dk
import fetch_histdata as hd

UTC = timezone.utc
BASE_PRICE = {"XAUUSD": 2050.0, "XAGUSD": 23.0, "EURUSD": 1.09}
SPREAD = {"XAUUSD": 0.25, "XAGUSD": 0.02, "EURUSD": 0.00008}


# ------------------------------------------------------------ synthetik ---
def market_utc(symbol, start, end, step_s=60, early_close=None):
    """UTC-Zeitpunkte mit Handel (New-York-Kalender): So open .. Fr 17:00,
    Metalle mit Tagespause 17-18 Uhr NY. early_close: {date_ny: stunde}."""
    idx = pd.date_range(start, end, freq=f"{step_s}s", inclusive="left", tz="UTC")
    idx = idx + pd.Timedelta(milliseconds=137)
    ny = idx.tz_convert("America/New_York")
    wd, h = ny.dayofweek, ny.hour + ny.minute / 60
    open_h = hd.OPEN_H_NY[symbol]
    closed = ((wd == 4) & (h >= 17)) | (wd == 5) | ((wd == 6) & (h < open_h))
    if symbol in hd.METALS:
        closed |= (wd <= 3) & (h >= 17) & (h < 18)
    for d, hh in (early_close or {}).items():
        closed |= (ny.date == d) & (h >= hh) & (h < 17)
    return idx[~np.asarray(closed)]


def raw_stamps(utc_idx, stamping):
    """ny: New-York-Ortszeit mit Sommerzeit (so stempelt HistData Ticks, gemessen);
    est: fest UTC-5 (Annahme fuer M1); utc: falsch fuer beide Modi."""
    if stamping == "est":
        return (utc_idx - pd.Timedelta(hours=5)).tz_localize(None)
    if stamping == "ny":
        return utc_idx.tz_convert("America/New_York").tz_localize(None)
    assert stamping == "utc"
    return utc_idx.tz_localize(None)


def raw_month_ticks(symbol, y, m, stamping="ny", step_s=60, price=None, early_close=None,
                    switch=None):
    """Ticks eines ROH-Monats: DataFrame raw (naiv), utc, bid, ask (Dateireihenfolge).
    Default New-York-Ortszeit (Tick-Modus); M1-Tests stempeln explizit "est".
    switch=(utc_zeitpunkt, stamping): ab dann anders gestempelt (Regimewechsel)."""
    lo = pd.Timestamp(y, m, 1, tz="UTC") - pd.Timedelta(days=1)
    hi = pd.Timestamp(y, m, 1, tz="UTC") + pd.offsets.MonthBegin(1) + pd.Timedelta(days=1)
    utc = market_utc(symbol, lo, hi, step_s, early_close)
    raw = raw_stamps(utc, stamping)
    if switch is not None:
        raw = raw.where(utc < pd.Timestamp(switch[0], tz="UTC"), raw_stamps(utc, switch[1]))
    keep = (raw >= pd.Timestamp(y, m, 1)) & (raw < pd.Timestamp(y, m, 1) + pd.offsets.MonthBegin(1))
    raw, utc = raw[keep], utc[keep]
    base = price if price is not None else BASE_PRICE[symbol]
    i = (utc.as_unit("ms").asi8 // 60_000).astype(float)
    bid = np.round(base * (1 + 0.002 * np.sin(i / 97.0)), 5)
    return pd.DataFrame({"raw": raw, "utc": utc, "bid": bid, "ask": bid + SPREAD[symbol]})


def tick_csv(df):
    stamp = pd.Index(df["raw"]).strftime("%Y%m%d %H%M%S%f").str[:-3]
    lines = [f"{s},{b:.5f},{a:.5f},0" for s, b, a in zip(stamp, df["bid"], df["ask"])]
    return ("\n".join(lines) + "\n").encode()


def m1_csv(df):
    raw = pd.Index(df["raw"]).floor("1min")
    g = pd.DataFrame({"raw": raw, "bid": df["bid"].to_numpy()}).groupby("raw", sort=True)["bid"]
    o, h, l, c = g.first(), g.max(), g.min(), g.last()
    stamp = o.index.strftime("%Y%m%d %H%M%S")
    lines = [f"{s};{a:.5f};{b:.5f};{cc:.5f};{d:.5f};0" for s, a, b, cc, d in zip(stamp, o, h, l, c)]
    return ("\n".join(lines) + "\n").encode()


def make_zip(symbol, code, period, csv, status="HistData.com status report\n"):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(f"DAT_ASCII_{symbol}_{code}_{period}.csv", csv)
        if status is not None:
            zf.writestr(f"DAT_ASCII_{symbol}_{code}_{period}.txt", status)
    return buf.getvalue()


def tick_zip(symbol, y, m, **kw):
    status = kw.pop("status", "HistData.com status report\n")
    return make_zip(symbol, "T", f"{y}{m:02d}", tick_csv(raw_month_ticks(symbol, y, m, **kw)), status)


# ------------------------------------------------------------ http-mock ---
PAGE = """<html><body>
<form id="file_status" name="file_status" method="POST" action="/getStatus.php">
  <input type="hidden" name="tk" id="tk" value="WRONG-STATUS-TOKEN" />
</form>
<form id="file_down" name="file_down" method="POST" action="/get.php">
  <input type="hidden" name="tk" id="tk" value="tok-{period}" />
  <input type="hidden" name="date" id="date" value="{year}" />
  <input type="hidden" name="datemonth" id="datemonth" value="{datemonth}" />
  <input type="hidden" name="platform" id="platform" value="ASCII" />
  <input type="hidden" name="timeframe" id="timeframe" value="{code}" />
  <input type="hidden" name="fxpair" id="fxpair" value="{pair}" />
</form></body></html>"""


class Resp:
    def __init__(self, status=200, content=b"", headers=None):
        self.status_code, self.content, self.headers = status, content, headers or {}

    @property
    def text(self):
        return self.content.decode("utf-8", "replace")


class Clock:
    """Wanduhr + monotone Uhr + Schlaf in einem (Sekunden)."""

    def __init__(self, t=1_700_000_000.0):
        self.t = t
        self.slept = []

    def __call__(self):
        return self.t

    def sleep(self, s):
        self.slept.append(s)
        self.t += s


class Site:
    """Fake-HistData: files[(code, period)] -> ZIP-Bytes; queue: vorab geskriptete Antworten."""

    def __init__(self, clock, files=None):
        self.clock, self.files, self.queue, self.calls = clock, dict(files or {}), [], []
        self.post_override = None

    def _next(self):
        return self.queue.pop(0) if self.queue else None

    def get(self, url, timeout=None, **kw):
        self.calls.append(("GET", url, self.clock.t))
        r = self._next()
        if r is not None:
            return r
        m = re.search(r"/ascii/([a-z0-9-]+)/([a-z]+)/(\d{4})(?:/(\d{1,2}))?$", url)
        tf, pair, year, month = m.groups()
        code = "T" if tf == "tick-data-quotes" else "M1"
        period = f"{year}{int(month):02d}" if month else year
        html = PAGE.format(period=period, year=year, datemonth=period if month else "",
                           code=code, pair=pair.upper())
        return Resp(200, html.encode())

    def post(self, url, data=None, headers=None, timeout=None, **kw):
        self.calls.append(("POST", url, self.clock.t))
        assert data["tk"].startswith("tok-")                 # nicht das file_status-Token
        assert headers["Referer"].startswith(hd.BASE)
        r = self._next()
        if r is not None:
            return r
        if self.post_override is not None:
            return Resp(200, self.post_override)
        period = data["datemonth"] or data["date"]
        return Resp(200, self.files.get((data["timeframe"], period), b""))

    @property
    def requests(self):
        return len(self.calls)


@pytest.fixture
def clock():
    return Clock()


def kw(clock, site, **extra):
    d = dict(pause=0.0, attempts=3, backoff_base=1.0, max_backoff=10.0, jitter=0.0,
             sleep=clock.sleep, clock=clock, wall=clock, session=site)
    d.update(extra)
    return d


NOW = datetime(2024, 12, 1, 12, tzinfo=UTC)


def at(t):
    return lambda: t


def run(tmp_path, clock, site, sym="XAUUSD", start=(2024, 2), end=None, now=NOW, **extra):
    end = end or start
    mode = extra.pop("mode", "tick")
    return hd.run(sym, date(*start, 1), date(*end, 1), tmp_path, mode=mode, now=at(now)
                  if isinstance(now, datetime) else now, stop=extra.pop("stop", None),
                  **kw(clock, site, **extra))


def feb_site(clock, sym="XAUUSD", **kwz):
    return Site(clock, {("T", "202401"): tick_zip(sym, 2024, 1, **kwz),
                        ("T", "202402"): tick_zip(sym, 2024, 2, **kwz)})


# --------------------------------------------------------------- parsing ---
def test_parse_tick_csv_with_milliseconds_and_utc_conversion():
    csv = b"20240102 170003660,2063.12,2063.45,0\n20240102 170003661,2063.10,2063.40,0\n" \
          b"20240102 170059999,2063.30,2063.60,0\n20240102 170100000,2063.00,2063.20,0\n"
    df = hd.parse_csv(csv, "tick")
    assert df["raw_ms"].iloc[0] == int(datetime(2024, 1, 2, 17, 0, 3, 660000, tzinfo=UTC).timestamp() * 1000)
    assert df["raw_ms"].iloc[1] - df["raw_ms"].iloc[0] == 1
    bars = hd.ticks_to_bars(df)
    assert list(bars["ts"]) == [pd.Timestamp("2024-01-02 22:00", tz="UTC"),
                                pd.Timestamp("2024-01-02 22:01", tz="UTC")]
    b0 = bars.iloc[0]
    assert (b0.bid_open, b0.bid_high, b0.bid_low, b0.bid_close) == (2063.12, 2063.30, 2063.10, 2063.30)
    assert (b0.ask_open, b0.ask_close, b0.tick_count) == (2063.45, 2063.60, 3)
    assert (bars["source"] == "histdata").all()
    # Sommer: Tick-Rohzeit ist New York mit Sommerzeit (EDT, UTC-4)
    summer = hd.parse_csv(b"20240702 165958000,2330.1,2330.4,0\n", "tick")
    assert hd.ticks_to_bars(summer)["ts"].iloc[0] == pd.Timestamp("2024-07-02 20:59", tz="UTC")


@pytest.mark.parametrize("csv,mode", [
    (b"20240102 170003;2063.1;2063.2;2063.0;2063.1;0\n", "tick"),   # M1-Zeile im Tickmodus
    (b"20240102 170003660123,2063.1,2063.2,0\n", "tick"),          # Mikrosekunden
    (b"2024-01-02 17:00:03,2063.1,2063.2,0\n", "tick"),
    (b"20240102 170003660,abc,2063.2,0\n", "tick"),
    (b"20240102 170003660,2063.1,2063.2,0\n", "m1"),
])
def test_parse_rejects_unknown_format(csv, mode):
    with pytest.raises(hd.FormatError):
        hd.parse_csv(csv, mode)


def test_read_zip_requires_expected_csv():
    u = hd.Unit("XAUUSD", "tick", 2024, 2)
    ok = make_zip("XAUUSD", "T", "202402", b"x\n")
    assert hd.read_zip(ok, u)[0] == b"x\n"
    with pytest.raises(hd.CorruptZip):
        hd.read_zip(b"PK\x03\x04kaputt", u)
    with pytest.raises(hd.CorruptZip):                     # nur .txt, keine CSV
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("DAT_ASCII_XAUUSD_T_202402.txt", "status")
        hd.read_zip(buf.getvalue(), u)
    with pytest.raises(hd.CorruptZip):                     # falscher Monat
        hd.read_zip(make_zip("XAUUSD", "T", "202401", b"x\n"), u)


def test_parse_status_report_gaps():
    txt = ("HistData.com (c) 2024\nFile: DAT_ASCII_XAUUSD_T_202402.csv Status Report\n"
           "Gap of 120s found between 20240205100000 and 20240205100200.\n"
           "Gap of 4000s found between 20240206120000 and 20240206130640.\n")
    s = hd.parse_status(txt)
    assert s["gaps_reported"] == 2 and s["gap_seconds_total"] == 4120
    assert s["largest_gaps"][0] == {"from_utc": "2024-02-06T17:00:00+00:00",
                                    "to_utc": "2024-02-06T18:06:40+00:00", "seconds": 4000}
    unknown = hd.parse_status("irgendein anderes Format\nzweite Zeile\n")
    assert unknown["gaps_reported"] == 0 and unknown["head"][0] == "irgendein anderes Format"


# ------------------------------------------------------- zeitzonen-check ---
def _raw_ms(df):
    return pd.DatetimeIndex(df["raw"]).as_unit("ms").asi8.astype(np.int64)


@pytest.mark.parametrize("sym", ["XAUUSD", "XAGUSD", "EURUSD"])
@pytest.mark.parametrize("ym,off", [((2024, 1), -5), ((2024, 7), -4)])
def test_tz_tick_new_york_local_passes_winter_and_summer(sym, ym, off):
    rep = hd.check_timezone(_raw_ms(raw_month_ticks(sym, *ym, stamping="ny")), sym)
    assert rep["ok"], rep
    assert rep["raw_tz"] == "America/New_York"
    assert rep["median_offset_h"] == off and rep["median_deviation_h"] == 0
    assert rep["share_expected"] == 1.0 and set(rep["expected_offsets"]) == {off}
    assert set(rep["deviations"]) == {0}


def test_tz_tick_dst_change_inside_month_passes():
    """Maerz/November: erwarteter Offset wechselt je Anker (-5 -> -4 bzw. -4 -> -5)."""
    for ym, offs in (((2024, 3), {-5, -4}), ((2024, 11), {-4, -5})):
        rep = hd.check_timezone(_raw_ms(raw_month_ticks("XAUUSD", *ym)), "XAUUSD",
                                hd.raw_month_bounds_ms(ym))
        assert rep["ok"], rep["reason"]
        assert set(rep["expected_offsets"]) == offs and set(rep["deviations"]) == {0}


@pytest.mark.parametrize("sym", ["XAUUSD", "EURUSD"])
def test_tz_tick_fixed_est_fails_in_summer(sym):
    """Alte Annahme (fest UTC-5) passt fuer Ticks im Sommer nicht mehr."""
    rep = hd.check_timezone(_raw_ms(raw_month_ticks(sym, 2024, 7, stamping="est")), sym)
    assert not rep["ok"] and rep["median_offset_h"] == -5 and rep["median_deviation_h"] == -1
    assert set(rep["expected_offsets"]) == {-4}
    # im Winter ist fest UTC-5 = New-York-Ortszeit, nicht unterscheidbar (und korrekt)
    assert hd.check_timezone(_raw_ms(raw_month_ticks(sym, 2024, 1, stamping="est")), sym)["ok"]


@pytest.mark.parametrize("sym", ["XAUUSD", "XAGUSD", "EURUSD"])
@pytest.mark.parametrize("ym", [(2024, 1), (2024, 7)])
def test_tz_m1_fixed_est_passes_winter_and_summer(sym, ym):
    rep = hd.check_timezone(_raw_ms(raw_month_ticks(sym, *ym, stamping="est")), sym, mode="m1")
    assert rep["ok"], rep
    assert rep["raw_tz"] == "Etc/GMT+5" and set(rep["expected_offsets"]) == {-5}
    assert rep["median_offset_h"] == -5 and rep["share_expected"] == 1.0


@pytest.mark.parametrize("sym", ["XAUUSD", "EURUSD"])
def test_tz_m1_new_york_local_with_dst_fails_in_summer(sym):
    rep = hd.check_timezone(_raw_ms(raw_month_ticks(sym, 2024, 7, stamping="ny")), sym, mode="m1")
    assert not rep["ok"] and rep["median_offset_h"] == -4 and rep["median_deviation_h"] == 1
    assert hd.check_timezone(_raw_ms(raw_month_ticks(sym, 2024, 1, stamping="ny")), sym,
                             mode="m1")["ok"]


@pytest.mark.parametrize("mode", ["tick", "m1"])
@pytest.mark.parametrize("ym", [(2024, 1), (2024, 7)])
def test_tz_utc_stamped_fails(ym, mode):
    rep = hd.check_timezone(_raw_ms(raw_month_ticks("XAUUSD", *ym, stamping="utc")), "XAUUSD",
                            mode=mode)
    assert not rep["ok"] and rep["median_offset_h"] in (0, 0.0)


def test_tz_shift_in_mid_month_fails():
    """Erste Monatshaelfte New-York-Ortszeit, zweite fest UTC-5 -> Regimewechsel."""
    ny = raw_month_ticks("XAUUSD", 2024, 7, stamping="ny")
    est = raw_month_ticks("XAUUSD", 2024, 7, stamping="est")
    cut = pd.Timestamp("2024-07-15", tz="UTC")
    mixed = pd.concat([ny[ny["utc"] < cut], est[est["utc"] >= cut]])
    rep = hd.check_timezone(_raw_ms(mixed), "XAUUSD")
    assert not rep["ok"] and rep["longest_deviating_run"] >= hd.TZ_MAX_RUN


def test_tz_one_hour_regression_fails():
    """Wiederholte Stunde (Uhr zurueckgestellt) in Dateireihenfolge -> Quarantaene."""
    df = raw_month_ticks("XAUUSD", 2024, 10)
    ms = _raw_ms(df)
    cut = int(np.searchsorted(ms, ms[0] + 20 * 86_400_000))
    shifted = np.concatenate([ms[:cut], ms[cut:] - 3_600_000])
    rep = hd.check_timezone(shifted, "XAUUSD")
    assert not rep["ok"] and rep["large_reversals"] and "Ruecksprung" in rep["reason"]


def test_tz_tolerates_holiday_early_close_and_small_reversals():
    df = raw_month_ticks("XAUUSD", 2024, 7, early_close={date(2024, 7, 5): 13})
    ms = _raw_ms(df)
    ms[100], ms[101] = ms[101], ms[100]                 # kleine Vertauschung (Sekunden)
    rep = hd.check_timezone(ms, "XAUUSD")
    assert rep["ok"], rep
    assert rep["reversals"] == 1 and rep["share_expected"] < 1.0


def test_tz_needs_enough_anchors():
    df = raw_month_ticks("EURUSD", 2024, 7)
    few = df[df["utc"] < pd.Timestamp("2024-07-04", tz="UTC")]
    rep = hd.check_timezone(_raw_ms(few), "EURUSD")
    assert not rep["ok"] and "Ankerpunkte" in rep["reason"]


# ---------------------------------------------------------- monatsgrenze ---
def test_month_boundary_stitching(tmp_path, clock):
    site = feb_site(clock)
    rep = run(tmp_path, clock, site)
    assert rep["exit_code"] == 0, rep
    assert rep["summary"]["months_written"] == ["2024-02"]
    df = pd.read_parquet(hd.store_path(tmp_path, "XAUUSD", 2024, 2))
    # erwartete Minuten direkt aus den synthetischen UTC-Ticks beider Rohmonate
    utc = pd.concat([raw_month_ticks("XAUUSD", 2024, k)["utc"] for k in (1, 2)])
    utc = utc[(utc >= pd.Timestamp("2024-02-01", tz="UTC")) & (utc < pd.Timestamp("2024-03-01", tz="UTC"))]
    assert list(df["ts"]) == sorted(set(utc.dt.floor("1min")))
    # 1.2. 00:00-05:00 UTC stammt aus dem Rohmonat Januar (31.1. 19:00-24:00 EST)
    assert df["ts"].iloc[0] == pd.Timestamp("2024-02-01 00:00", tz="UTC")
    jan = raw_month_ticks("XAUUSD", 2024, 1)
    first = jan[jan["utc"] >= pd.Timestamp("2024-02-01", tz="UTC")].iloc[0]
    assert first["raw"] == pd.Timestamp("2024-01-31 19:00:00.137")
    assert df["bid_open"].iloc[0] == first["bid"]
    # die letzten 5 h des Rohmonats Februar gehoeren zu UTC-Maerz
    assert df["ts"].iloc[-1] < pd.Timestamp("2024-03-01", tz="UTC")
    assert df["ts"].iloc[-1] >= pd.Timestamp("2024-02-29 21:00", tz="UTC")
    mrep = json.loads(hd.report_path(tmp_path, "XAUUSD", 2024, 2).read_text())
    assert set(mrep["raw_months"]) == {"2024-01", "2024-02"}
    assert mrep["raw_months"]["2024-02"]["timezone"]["ok"] is True
    assert mrep["spread_available"] is True and mrep["spread_median"] == pytest.approx(0.25)


def test_month_not_written_without_previous_raw_month(tmp_path, clock):
    site = Site(clock, {("T", "202402"): tick_zip("XAUUSD", 2024, 2)})
    site.post_override = None
    orig_post = site.post

    def post(url, data=None, **k):
        if data["datemonth"] == "202401":
            site.calls.append(("POST", url, clock.t))
            return Resp(503)
        return orig_post(url, data=data, **k)

    site.post = post
    rep = run(tmp_path, clock, site, attempts=2)
    assert rep["exit_code"] == 3 and rep["status"] == "gaps"
    assert rep["summary"]["months_incomplete"] == ["2024-02"]
    assert rep["gaps"][0]["file"] == "HISTDATA_COM_ASCII_XAUUSD_T202401.zip"
    assert not hd.store_path(tmp_path, "XAUUSD", 2024, 2).exists()


def test_output_schema_identical_to_dukascopy(tmp_path, clock):
    run(tmp_path, clock, feb_site(clock))
    t_hd = pq.read_table(hd.store_path(tmp_path, "XAUUSD", 2024, 2))
    jan = raw_month_ticks("XAUUSD", 2024, 1).head(500)
    ticks = pd.DataFrame({"ts": pd.DatetimeIndex(jan["utc"]).as_unit("ms"), "bid": jan["bid"],
                          "ask": jan["ask"], "bid_volume": 1.0, "ask_volume": 2.0})
    duk = dk.aggregate_1m(ticks)
    out = tmp_path / "duk.parquet"
    dk.write_parquet(out, duk, "ticks")
    t_dk = pq.read_table(out)
    assert t_hd.schema.names == t_dk.schema.names == dk.COLUMNS
    assert t_hd.schema.types == t_dk.schema.types
    a, b = t_hd.to_pandas(), t_dk.to_pandas()
    assert list(a.dtypes) == list(b.dtypes)
    meta = t_hd.schema.metadata
    assert meta[b"histdata_mode"] == b"histdata_tick" and meta[b"spread_available"] == b"true"


# ------------------------------------------------------------------ m1 ---
def test_m1_mode_bid_only_yearly_zip(tmp_path, clock):
    jun = raw_month_ticks("XAUUSD", 2024, 6, stamping="est")      # M1: fest UTC-5
    jul = raw_month_ticks("XAUUSD", 2024, 7, stamping="est")
    csv = m1_csv(pd.concat([jun, jul]))
    lines = csv.decode().splitlines()
    dup_exact = lines[10]                                   # Juni: exaktes Duplikat
    k = next(i for i, ln in enumerate(lines) if ln.startswith("20240710 1"))
    s, o, h, l, c, v = lines[k].split(";")                  # Juli: abweichendes Duplikat
    dup_conflict = ";".join([s, o, f"{float(h) + 1.0:.5f}", l, c, v])
    lines = lines[:11] + [dup_exact] + lines[11:k + 1] + [dup_conflict] + lines[k + 1:]
    site = Site(clock, {("M1", "2024"): make_zip("XAUUSD", "M1", "2024", ("\n".join(lines) + "\n").encode())})
    now = datetime(2025, 3, 1, tzinfo=UTC)
    rep = run(tmp_path, clock, site, start=(2024, 7), now=now, mode="m1")
    assert rep["exit_code"] == 0, rep
    assert site.requests == 2                                # eine Jahresdatei
    assert "1-minute-bar-quotes/xauusd/2024" in site.calls[0][1]
    t = pq.read_table(hd.store_path(tmp_path, "XAUUSD", 2024, 7, "m1"))
    assert "source=histdata_m1" in str(hd.store_path(tmp_path, "XAUUSD", 2024, 7, "m1"))
    assert not hd.store_path(tmp_path, "XAUUSD", 2024, 7).exists()      # Tick-Partition unberuehrt
    assert t.schema.metadata[b"histdata_mode"] == b"histdata_m1"
    assert t.schema.metadata[b"spread_available"] == b"false"
    df = t.to_pandas()
    for k in ("open", "high", "low", "close"):
        assert (df[f"ask_{k}"] == df[f"bid_{k}"]).all()
    assert (df["tick_count"] == 1).all()
    mrep = json.loads(hd.report_path(tmp_path, "XAUUSD", 2024, 7, "m1").read_text())
    assert (df["source"] == "histdata_m1").all()
    assert mrep["mode"] == "histdata_m1" and mrep["spread_available"] is False
    assert mrep["raw_months"]["2024-06"]["file_rows"]["m1_exact_duplicates_dropped"] == 1
    assert mrep["m1_conflicting_minutes_merged"] == 1
    ts = pd.Timestamp(datetime.strptime(s, "%Y%m%d %H%M%S") + timedelta(hours=5), tz="UTC")
    row = df[df["ts"] == ts].iloc[0]
    assert row["bid_high"] == pytest.approx(float(h) + 1.0) and row["bid_open"] == float(o)


# --------------------------------------------------------- fortsetzen ---
def test_resume_makes_no_requests(tmp_path, clock):
    site = feb_site(clock)
    assert run(tmp_path, clock, site)["exit_code"] == 0
    assert site.requests == 4
    rep = run(tmp_path, clock, site)
    assert rep["exit_code"] == 0 and site.requests == 4
    assert rep["summary"]["months_skipped_existing"] == 1
    hd.store_path(tmp_path, "XAUUSD", 2024, 2).unlink()           # aus dem Roh-Cache neu bauen
    rep = run(tmp_path, clock, Site(clock))
    assert rep["exit_code"] == 0 and rep["summary"]["requests"] == 0
    assert hd.store_path(tmp_path, "XAUUSD", 2024, 2).exists()


def test_pause_between_requests(tmp_path, clock):
    site = feb_site(clock)
    run(tmp_path, clock, site, pause=5.0)
    times = [t for _, _, t in site.calls]
    assert all(b - a >= 5.0 for a, b in zip(times, times[1:]))


@pytest.mark.parametrize("garbage", [b"kein zip", "only_txt"])
def test_corrupt_cached_zip_is_set_aside_and_refetched(tmp_path, clock, garbage):
    u = hd.Unit("XAUUSD", "tick", 2024, 2)
    p = u.path(tmp_path)
    p.parent.mkdir(parents=True)
    if garbage == "only_txt":
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("DAT_ASCII_XAUUSD_T_202402.txt", "status")
        garbage = buf.getvalue()
    p.write_bytes(garbage)
    site = feb_site(clock)
    rep = run(tmp_path, clock, site)
    assert rep["exit_code"] == 0, rep
    assert u.marker(tmp_path, "corrupt").read_bytes() == garbage
    assert hd.read_zip(p.read_bytes(), u)
    assert hd.store_path(tmp_path, "XAUUSD", 2024, 2).exists()


def test_html_instead_of_zip_is_failure_not_data(tmp_path, clock):
    site = feb_site(clock)
    site.post_override = b"<html><body>Please complete the captcha</body></html>"
    rep = run(tmp_path, clock, site, attempts=2)
    assert rep["exit_code"] == 3
    assert "HTML/Text statt ZIP" in rep["content_errors"][0]["error"]
    assert not any(hd.raw_dir(tmp_path, "XAUUSD", "tick").glob("*.zip"))
    assert not hd.store_path(tmp_path, "XAUUSD", 2024, 2).exists()
    assert site.requests == 4                    # je Datei 1 Versuch (dauerhafter Fehler)
    assert hd.Unit("XAUUSD", "tick", 2024, 2).marker(tmp_path, "failed").exists()


def test_page_without_form_is_failure(tmp_path, clock):
    site = feb_site(clock)
    site.queue = [Resp(200, b"<html>Access denied</html>")] * 3
    rep = run(tmp_path, clock, site, attempts=3)
    assert rep["exit_code"] == 3 and "Download-Formular" in rep["content_errors"][0]["error"]


# ------------------------------------------------------------------ 429 ---
def test_429_does_not_consume_attempts_and_honours_retry_after(tmp_path, clock):
    site = feb_site(clock)
    site.queue = [Resp(429, headers={"Retry-After": "1200"}), Resp(429)]
    t0 = clock.t
    rep = run(tmp_path, clock, site, attempts=1)           # 1 Versuch reicht trotz 2x 429
    assert rep["exit_code"] == 0, rep
    assert clock.t - t0 >= 1200 + 900
    rl = rep["summary"]["rate_limit"]
    assert rl["hits_429"] == 2 and rl["min_pause_s"] == 900
    assert rep["summary"]["global_cooldowns"] == 0          # keine Eskalation
    assert rep["summary"]["http_status_counts"]["429"] == 2
    state = json.loads(hd.ratelimit_state_path(tmp_path).read_text())
    assert state["blocked_until"] >= t0 + 1200
    # Abstand zwischen 429 und naechstem Request >= 15 min
    times = [t for _, _, t in site.calls]
    assert times[1] - times[0] >= 1200 and times[2] - times[1] >= 900


def test_429_block_persists_across_runs(tmp_path, clock):
    site = feb_site(clock)
    site.queue = [Resp(429, headers={"Retry-After": "3000"})]
    stop = threading.Event()
    t0 = clock.t
    rep = run(tmp_path, clock, site, sleep=lambda s: stop.set(), stop=stop)   # SIGTERM in der Pause
    assert rep["exit_code"] == 3 and rep["status"] == "interrupted" and site.requests == 1
    site2 = feb_site(clock)                                 # neuer Prozess, gleicher Zustand
    rep = run(tmp_path, clock, site2)
    assert rep["exit_code"] == 0
    assert site2.calls[0][2] >= t0 + 3000                   # erst nach Ablauf der Sperre


def test_rate_limit_budget_exhausted_exit_3_with_report(tmp_path, clock):
    site = feb_site(clock)
    site.queue = [Resp(429)] * 10
    rep = run(tmp_path, clock, site, max_rate_limit_wait=1000)
    assert rep["exit_code"] == 3 and rep["status"] == "rate_limited"
    saved = json.loads(hd.run_report_path(tmp_path, "XAUUSD").read_text())
    assert saved["status"] == "rate_limited" and "429" in saved["error"]
    assert saved["summary"]["rate_limit"]["exhausted"]
    assert saved["summary"]["months_incomplete"] == ["2024-02"]


def test_cli_rejects_short_rate_limit_pause():
    with pytest.raises(SystemExit) as e:
        hd.main(["--symbol", "XAUUSD", "--start", "2024-01", "--rate-limit-pause", "600"])
    assert e.value.code == 2


# ------------------------------------------- pending / keine daten ---
def test_recent_empty_month_is_pending(tmp_path, clock):
    site = Site(clock, {("T", "202409"): tick_zip("XAUUSD", 2024, 9)})   # Oktober fehlt noch
    rep = run(tmp_path, clock, site, start=(2024, 10))
    assert rep["exit_code"] == 3 and rep["status"] == "pending"
    assert "2024-10" in rep["summary"]["months_pending"]
    assert rep["pending"][0]["file"] == "HISTDATA_COM_ASCII_XAUUSD_T202410.zip"
    n = site.requests
    rep = run(tmp_path, clock, site, start=(2024, 10), now=NOW + timedelta(hours=3))
    assert rep["exit_code"] == 3 and site.requests == n     # < 24 h: keine erneute Anfrage
    rep = run(tmp_path, clock, site, start=(2024, 10), now=NOW + timedelta(days=2))
    assert rep["status"] == "pending" and site.requests == n + 2   # nie bestaetigt innerhalb Frist


def test_old_empty_month_confirmed_after_24h(tmp_path, clock):
    # Ticks erst ab Rohmonat 2023-06: leere Monate davor = vor Beginn der Tick-Historie
    site = Site(clock, {("T", "202306"): tick_zip("XAUUSD", 2023, 6, price=1950.0)})
    rep = run(tmp_path, clock, site, start=(2023, 5), end=(2023, 6))
    assert rep["exit_code"] == 3 and rep["status"] == "gaps"
    assert len(rep["unconfirmed"]) == 2
    rep = run(tmp_path, clock, site, start=(2023, 5), end=(2023, 6), now=NOW + timedelta(hours=25))
    assert rep["exit_code"] == 0, rep
    assert "2023-05" in rep["summary"]["months_unavailable"]
    assert rep["summary"]["months_written"] == ["2023-06"]
    info = json.loads(hd.Unit("XAUUSD", "tick", 2023, 5).marker(tmp_path, "empty").read_text())
    assert info["confirmed"] == "repeat" and info["checks"] == 2


def test_months_before_history_need_no_requests(tmp_path, clock):
    site = Site(clock, {("T", "200905"): tick_zip("XAGUSD", 2009, 5, price=14.0)})
    rep = run(tmp_path, clock, site, sym="XAGUSD", start=(2009, 1), end=(2009, 5))
    assert rep["exit_code"] == 0, rep
    assert site.requests == 2                               # nur Rohmonat 2009-05
    un = rep["summary"]["months_unavailable"]
    assert set(un) == {"2009-01", "2009-02", "2009-03", "2009-04"}
    assert rep["summary"]["months_written"] == ["2009-05"]


def test_running_month_is_never_requested(tmp_path, clock):
    site = Site(clock)
    # November endet in UTC am 1.12. 00:00, im Rohmonat (EST) erst 05:00; + 6 h Settle
    rep = run(tmp_path, clock, site, start=(2024, 11), end=(2024, 12),
              now=datetime(2024, 12, 1, 10, 59, tzinfo=UTC))
    assert rep["exit_code"] == 0 and site.requests == 0 and rep["summary"]["months_considered"] == 0
    with pytest.raises(ValueError):
        ctx = hd.Ctx("XAUUSD", "tick", tmp_path, hd.Client(tmp_path, session=site), at(NOW))
        ctx.get(hd.Unit("XAUUSD", "tick", 2024, 12))


# ----------------------------------------------------------- exit-codes ---
def test_validation_error_exit_2(tmp_path, clock):
    site = feb_site(clock, price=20.0)                       # Gold bei 20 USD: unplausibel
    rep = run(tmp_path, clock, site)
    assert rep["exit_code"] == 2 and rep["validation_errors"][0]["month"] == "2024-02"
    assert not hd.store_path(tmp_path, "XAUUSD", 2024, 2).exists()


def test_timezone_mismatch_quarantine_exit_4(tmp_path, clock):
    """Ticks fest UTC-5 gestempelt (alte Annahme) -> im Sommer Quarantaene."""
    site = Site(clock, {("T", "202406"): tick_zip("XAUUSD", 2024, 6, stamping="est"),
                        ("T", "202407"): tick_zip("XAUUSD", 2024, 7, stamping="est")})
    rep = run(tmp_path, clock, site, start=(2024, 7))
    assert rep["exit_code"] == 4 and rep["status"] == "quarantine"
    q = rep["quarantine"][0]
    assert "Zeitzone" in q["reason"] and q["check"]["median_offset_h"] == -5
    assert q["check"]["median_deviation_h"] == -1
    marker = json.loads(hd.Unit("XAUUSD", "tick", 2024, 6).qmarker(tmp_path, (2024, 6)).read_text())
    assert marker["tz_check_version"] == hd.TZ_CHECK_VERSION
    assert marker["raw_tz"] == "America/New_York"
    assert hd.Unit("XAUUSD", "tick", 2024, 6).qmarker(tmp_path, (2024, 6)).exists()
    assert not hd.store_path(tmp_path, "XAUUSD", 2024, 7).exists()
    n = site.requests
    rep = run(tmp_path, clock, site, start=(2024, 7))
    assert rep["exit_code"] == 4 and site.requests == n


def test_undecodable_format_quarantine_exit_4(tmp_path, clock):
    bad = make_zip("XAUUSD", "T", "202402", b"20240201 000000;1;2;0;1;0\n")
    site = Site(clock, {("T", "202401"): tick_zip("XAUUSD", 2024, 1), ("T", "202402"): bad})
    rep = run(tmp_path, clock, site)
    assert rep["exit_code"] == 4 and "Format" in rep["quarantine"][0]["reason"]


def test_lock_held_exit_3(tmp_path, clock):
    fh = hd.acquire_lock(tmp_path, "XAUUSD")
    try:
        rep = run(tmp_path, clock, Site(clock))
        assert rep["exit_code"] == 3 and rep["status"] == "locked"
        # eigener Lock-Name: Dukascopy-Lock desselben Symbols blockiert nicht
        assert hd.lock_path(tmp_path, "XAUUSD") != dk.lock_path(tmp_path, "XAUUSD")
    finally:
        fh.close()


def test_sigterm_before_start_exit_3_with_report(tmp_path, clock):
    stop = threading.Event()
    stop.set()
    site = feb_site(clock)
    rep = run(tmp_path, clock, site, stop=stop)
    assert rep["exit_code"] == 3 and rep["status"] == "interrupted" and site.requests == 0
    assert hd.run_report_path(tmp_path, "XAUUSD").exists()


def test_circuit_breaker(tmp_path, clock):
    site = feb_site(clock)
    site.queue = [Resp(500)] * 50
    rep = run(tmp_path, clock, site, attempts=5, max_consecutive_failures=3)
    assert rep["exit_code"] == 3 and rep["status"] == "circuit_breaker"
    assert site.requests == 3


def test_status_report_gaps_in_run_report(tmp_path, clock):
    status = "Gap of 7200s found between 20240205100000 and 20240205120000\n"
    site = Site(clock, {("T", "202401"): tick_zip("XAUUSD", 2024, 1),
                        ("T", "202402"): tick_zip("XAUUSD", 2024, 2, status=status)})
    rep = run(tmp_path, clock, site)
    g = rep["histdata_reported_gaps"]["HISTDATA_COM_ASCII_XAUUSD_T202402.zip"]
    assert g["gaps_reported"] == 1 and g["largest_gaps"][0]["from_utc"] == "2024-02-05T15:00:00+00:00"


def test_orphan_temp_files_removed(tmp_path, clock):
    d = hd.store_dir(tmp_path, "XAUUSD") / "year=2024"
    d.mkdir(parents=True)
    orphan = d / ".XAUUSD-1m-2024-02.parquet.1.2.part"
    orphan.write_bytes(b"x")
    run(tmp_path, clock, feb_site(clock))
    assert not orphan.exists()


# --------------------------------------------------------- verify-month ---
def test_verify_month_ok_and_tz_failure(tmp_path, clock):
    site = feb_site(clock)
    rep = hd.verify_month("XAUUSD", (2024, 2), tmp_path, now=at(NOW), **kw(clock, site))
    assert rep["exit_code"] == 0 and rep["ok"] and site.requests == 4
    saved = json.loads(hd.verify_report_path(tmp_path, "XAUUSD", 2024, 2).read_text())
    assert saved["raw_months"]["2024-01"]["timezone"]["median_offset_h"] == -5
    assert not hd.store_path(tmp_path, "XAUUSD", 2024, 2).exists()

    site = Site(clock, {("T", "202406"): tick_zip("XAUUSD", 2024, 6, stamping="utc"),
                        ("T", "202407"): tick_zip("XAUUSD", 2024, 7, stamping="utc")})
    rep = hd.verify_month("XAUUSD", (2024, 7), tmp_path, now=at(NOW), **kw(clock, site))
    assert rep["exit_code"] == 4 and not rep["ok"]


def test_verify_month_rejects_running_month(tmp_path, clock):
    with pytest.raises(ValueError):
        hd.verify_month("XAUUSD", (2024, 12), tmp_path, now=at(NOW), **kw(clock, Site(clock)))


def test_cli_verify_month(tmp_path, clock, monkeypatch):
    site = feb_site(clock)
    site.headers = {}
    monkeypatch.setattr(hd.requests, "Session", lambda: site)
    monkeypatch.setenv("MARKETDATA_DATA_ROOT", str(tmp_path))
    code = hd.main(["--symbol", "XAUUSD", "--verify-month", "2024-02", "--pause", "0"])
    assert code == 0 and site.requests == 4
    assert "@" not in site.headers["User-Agent"]
    assert hd.verify_report_path(tmp_path, "XAUUSD", 2024, 2).exists()


# ===================================================== Review-Regressionen ---
def _zip_from(df, sym, y, m, status="HistData.com status report\n"):
    return make_zip(sym, "T", f"{y}{m:02d}", tick_csv(df), status)


# Regimewechsel kurz vor Monatsende (Hypothese: Quelle stellt wie die EU um):
# tick: New York (EDT) bis 27.10. 01:00 UTC, danach fest UTC-5;
# m1: fest UTC-5 bis 31.3. 01:00 UTC, danach New York (EDT).
SWITCH = {"tick": ((2024, 10), "ny", ("2024-10-27 01:00", "est")),
          "m1": ((2024, 3), "est", ("2024-03-31 01:00", "ny"))}


@pytest.mark.parametrize("mode", ["tick", "m1"])
def test_review1_dst_switch_at_month_end_is_caught(mode):
    ym, stamping, switch = SWITCH[mode]
    ms = _raw_ms(raw_month_ticks("XAUUSD", *ym, stamping=stamping, switch=switch))
    rep = hd.check_timezone(ms, "XAUUSD", hd.raw_month_bounds_ms(ym), mode=mode)
    assert hd.check_timezone(ms, "XAUUSD", mode=mode)["edge_violations"]   # Rand auch ohne bounds
    assert not rep["ok"] and rep["edge_violations"] and "Monatsrand" in rep["reason"]
    assert {e["deviation_h"] for e in rep["edge_violations"]} == {-1 if mode == "tick" else 1}
    # Feiertag am Monatsrand (frueher Schluss, >= 2 h) bleibt erlaubt
    hol = raw_month_ticks("XAUUSD", 2024, 7, stamping="ny" if mode == "tick" else "est",
                          early_close={date(2024, 7, 3): 13})
    assert hd.check_timezone(_raw_ms(hol), "XAUUSD", hd.raw_month_bounds_ms((2024, 7)), mode=mode)["ok"]


def test_review1_dst_switch_at_month_end_not_written(tmp_path, clock):
    ym, stamping, switch = SWITCH["tick"]
    oct_ = raw_month_ticks("XAUUSD", *ym, stamping=stamping, switch=switch)
    site = Site(clock, {("T", "202409"): tick_zip("XAUUSD", 2024, 9),
                        ("T", "202410"): _zip_from(oct_, "XAUUSD", 2024, 10)})
    rep = run(tmp_path, clock, site, start=(2024, 10))
    assert rep["exit_code"] == 4 and not hd.store_path(tmp_path, "XAUUSD", 2024, 10).exists()


def test_review1_following_month_starting_shifted_blocks_month(tmp_path, clock):
    files = {("T", "202402"): tick_zip("XAUUSD", 2024, 2), ("T", "202403"): tick_zip("XAUUSD", 2024, 3),
             ("T", "202404"): tick_zip("XAUUSD", 2024, 4, stamping="est")}
    site = Site(clock, files)
    rep = run(tmp_path, clock, site, start=(2024, 3), end=(2024, 4))
    assert rep["exit_code"] == 4 and rep["summary"]["months_written"] == []
    q = {r["month"]: r for r in rep["quarantined"]}
    assert "Folge-Rohmonat 2024-04" in q["2024-03"]["reason"] and "2024-04" in q
    # nur Maerz angefragt: April nicht im Cache -> keine Nachbarpruefung, Maerz geschrieben
    rep = run(tmp_path / "b", clock, Site(clock, files), start=(2024, 3))
    assert rep["exit_code"] == 0 and rep["summary"]["months_written"] == ["2024-03"]


def test_review6_thin_market_night_gaps_are_no_break_anchors():
    df = raw_month_ticks("XAGUSD", 2024, 1)
    raw = pd.DatetimeIndex(df["raw"])
    thin = df[~((raw.hour == 2) & (raw.dayofweek <= 3))]     # 02-03 Uhr roh ohne Ticks
    rep = hd.check_timezone(_raw_ms(thin), "XAGUSD", hd.raw_month_bounds_ms((2024, 1)))
    assert rep["ok"], rep["reason"]
    assert rep["ignored_break_gaps"] > 0 and rep["share_expected"] == 1.0
    est = raw_month_ticks("XAGUSD", 2024, 7, stamping="est")      # Ticks fest UTC-5: falsch
    raw = pd.DatetimeIndex(est["raw"])
    thin_est = est[~((raw.hour == 2) & (raw.dayofweek <= 3))]
    assert not hd.check_timezone(_raw_ms(thin_est), "XAGUSD", hd.raw_month_bounds_ms((2024, 7)))["ok"]


def test_review2_429_on_post_restarts_with_fresh_token(tmp_path, clock):
    site = feb_site(clock)
    orig, n = site.post, {"k": 0}

    def post(url, data=None, **k):
        n["k"] += 1
        if n["k"] == 1:
            site.calls.append(("POST", url, clock.t))
            return Resp(429, headers={"Retry-After": "1200"})
        return orig(url, data=data, **k)

    site.post = post
    rep = run(tmp_path, clock, site, attempts=1)
    assert rep["exit_code"] == 0, rep
    methods = [m for m, _, _ in site.calls]
    assert methods[:4] == ["GET", "POST", "GET", "POST"]          # neues Token nach der Pause
    assert site.calls[2][2] - site.calls[1][2] >= 1200
    assert rep["summary"]["global_cooldowns"] == 0 and rep["content_errors"] == []


def _no_form_site(clock, bad_months, months=range(1, 7)):
    site = Site(clock, {("T", f"2024{m:02d}"): tick_zip("XAUUSD", 2024, m) for m in months})
    og = site.get

    def get(url, **k):
        if any(url.endswith(f"/2024/{m}") for m in bad_months):
            site.calls.append(("GET", url, clock.t))
            return Resp(200, b"<html>no form</html>")
        return og(url, **k)

    site.get = get
    return site


def test_review3_permanent_content_error_does_not_stall_backfill(tmp_path, clock):
    site = _no_form_site(clock, bad_months=(1, 2))
    rep = run(tmp_path, clock, site, start=(2024, 2), end=(2024, 6), attempts=5,
              max_consecutive_failures=10)
    assert rep["exit_code"] == 3 and not rep["summary"]["circuit_breaker"]
    assert rep["summary"]["months_written"] == ["2024-04", "2024-05", "2024-06"]
    assert sorted(r["file"] for r in rep["content_errors"]) == [
        "HISTDATA_COM_ASCII_XAUUSD_T202401.zip", "HISTDATA_COM_ASCII_XAUUSD_T202402.zip"]
    bad_calls = [c for c in site.calls if c[1].endswith(("/2024/1", "/2024/2"))]
    assert len(bad_calls) == 2                                    # 1 Versuch je Datei
    n = site.requests
    rep = run(tmp_path, clock, site, start=(2024, 2), end=(2024, 6))
    assert site.requests == n and rep["exit_code"] == 3          # < 24 h: zurueckgestellt
    assert {r["status"] for r in rep["content_errors"]} == {"deferred"}
    rep = run(tmp_path, clock, site, start=(2024, 2), end=(2024, 6), now=NOW + timedelta(hours=25))
    assert site.requests == n + 2                                 # nach 24 h genau 1 Versuch je Datei


def test_review3_content_errors_in_a_row_abort_run(tmp_path, clock):
    site = _no_form_site(clock, bad_months=range(1, 7))
    rep = run(tmp_path, clock, site, start=(2024, 2), end=(2024, 6))
    assert rep["exit_code"] == 3 and rep["status"] == "aborted"
    assert "Inhaltsfehler in Folge" in rep["summary"]["aborted"] and site.requests == 3


def test_review4_systematic_empty_answers_in_history_abort(tmp_path, clock):
    site = Site(clock, {("T", "202401"): tick_zip("XAUUSD", 2024, 1)})
    rep = run(tmp_path, clock, site, start=(2024, 2), end=(2024, 6))
    assert rep["exit_code"] == 3 and rep["status"] == "aborted"
    assert "leere Antworten in Folge" in rep["summary"]["aborted"]
    assert rep["summary"]["history_known_from"] == "2024-01"
    assert site.requests == 2 + 3 * 2                             # Jan + 3 leere Monate, dann Stopp


def test_review4_empties_before_first_data_do_not_abort(tmp_path, clock):
    site = Site(clock, {("T", f"2009{m:02d}"): tick_zip("XAUUSD", 2009, m, price=950.0)
                        for m in (8, 9)})                         # Ticks erst ab 2009-08
    rep = run(tmp_path, clock, site, start=(2009, 3), end=(2009, 9))
    assert rep["status"] != "aborted" and rep["summary"]["months_written"] == ["2009-09"]


def test_review4_confirmed_empty_inside_history_is_exit_4_until_accepted(tmp_path, clock):
    site = Site(clock, {("T", "202401"): tick_zip("XAUUSD", 2024, 1)})
    assert run(tmp_path, clock, site)["exit_code"] == 3            # Feb leer, unbestaetigt
    t1 = NOW + timedelta(hours=25)
    rep = run(tmp_path, clock, site, now=t1)
    assert rep["exit_code"] == 4 and rep["status"] == "quarantine"
    assert rep["quarantined"][0]["kind"] == "missing_in_history"
    assert "2024-02" in rep["summary"]["months_missing_in_history"]
    marker = hd.Unit("XAUUSD", "tick", 2024, 2).marker(tmp_path, "empty")
    info = json.loads(marker.read_text())
    assert info["confirmed"] == "repeat" and info["confirmed_at"]
    n = site.requests
    assert run(tmp_path, clock, site, now=t1 + timedelta(days=1))["exit_code"] == 4
    assert site.requests == n                                     # keine Anfrage vor 30 Tagen
    run(tmp_path, clock, site, now=t1 + timedelta(days=31))
    assert site.requests == n + 2                                 # nach 30 Tagen erneut geprueft
    run(tmp_path, clock, site, now=t1 + timedelta(days=32), rebuild=True)
    assert site.requests == n + 4                                 # --rebuild: sofort erneut
    info = json.loads(marker.read_text())
    marker.write_text(json.dumps({**info, "accepted": True}))
    rep = run(tmp_path, clock, site, now=t1 + timedelta(days=33))
    assert rep["exit_code"] == 0 and "2024-02" in rep["summary"]["months_unavailable"]


def test_review5_quarantine_visible_when_gaps_win(tmp_path, clock, caplog):
    site = Site(clock, {("T", "202401"): tick_zip("XAUUSD", 2024, 1),
                        ("T", "202402"): tick_zip("XAUUSD", 2024, 2, stamping="utc")})
    with caplog.at_level("ERROR", logger="histdata"):
        rep = run(tmp_path, clock, site, start=(2024, 2), end=(2024, 3),
                  now=datetime(2024, 4, 2, tzinfo=UTC))
    assert rep["exit_code"] == 3 and rep["status"] == "pending+quarantine"
    assert [r["month"] for r in rep["quarantined"]] == ["2024-02"]
    assert any("MANUELLE PRUEFUNG" in r.getMessage() and "2024-02" in r.getMessage()
               for r in caplog.records)


def test_review7_m1_never_overwrites_or_mixes_with_tick_store(tmp_path, clock):
    out = hd.store_path(tmp_path, "XAUUSD", 2024, 2)              # Tick-Partition
    bars = hd.ticks_to_bars(hd.parse_csv(tick_csv(raw_month_ticks("XAUUSD", 2024, 2).head(50)), "tick"))
    hd.write_parquet(out, bars, "m1")                              # Altlast: m1 in Tick-Partition
    before = out.read_bytes()
    rep = run(tmp_path, clock, feb_site(clock), rebuild=True)
    assert rep["exit_code"] == 4 and rep["quarantined"][0]["kind"] == "store_mode_conflict"
    assert out.read_bytes() == before


def test_review_low_short_first_month_checked_jointly(tmp_path, clock):
    may = raw_month_ticks("XAGUSD", 2009, 5, price=14.0)
    may = may[may["raw"] >= pd.Timestamp("2009-05-29 10:00")]       # nur ein Freitag
    files = {("T", "200905"): _zip_from(may, "XAGUSD", 2009, 5),
             ("T", "200906"): tick_zip("XAGUSD", 2009, 6, price=14.0)}
    rep = run(tmp_path, clock, Site(clock, files), sym="XAGUSD", start=(2009, 5), end=(2009, 6))
    assert rep["exit_code"] == 0, rep["quarantined"]
    mrep = json.loads(hd.report_path(tmp_path, "XAGUSD", 2009, 5).read_text())
    tz = mrep["raw_months"]["2009-05"]["timezone"]
    assert tz["joint_with"] == ["2009-05", "2009-06"] and tz["alone"]["anchors"] < hd.TZ_MIN_ANCHORS
    # ohne Nachbarmonat: nicht pruefbar -> Quarantaene mit klarer Meldung
    rep = run(tmp_path / "b", clock, Site(clock, files), sym="XAGUSD", start=(2009, 5))
    assert rep["exit_code"] == 4 and "NICHT PRUEFBAR" in rep["quarantined"][0]["reason"]


def test_review_low_new_download_clears_quarantine(tmp_path, clock):
    bad = {("T", "202406"): tick_zip("XAUUSD", 2024, 6, stamping="est"),
           ("T", "202407"): tick_zip("XAUUSD", 2024, 7, stamping="est")}
    assert run(tmp_path, clock, Site(clock, bad), start=(2024, 7))["exit_code"] == 4
    for m in (6, 7):                                                # ZIP loeschen = neu laden
        hd.Unit("XAUUSD", "tick", 2024, m).path(tmp_path).unlink()
    good = Site(clock, {("T", "202406"): tick_zip("XAUUSD", 2024, 6),
                        ("T", "202407"): tick_zip("XAUUSD", 2024, 7)})
    rep = run(tmp_path, clock, good, start=(2024, 7))
    assert rep["exit_code"] == 0 and good.requests == 4
    assert not list(hd.raw_dir(tmp_path, "XAUUSD", "tick").glob("*.quarantine"))


def test_review4_no_data_ever_is_not_silently_unavailable(tmp_path, clock):
    site = Site(clock)                                            # immer leer (WAF/Token?)
    assert run(tmp_path, clock, site)["exit_code"] == 3
    rep = run(tmp_path, clock, site, now=NOW + timedelta(hours=25))
    assert rep["exit_code"] == 4 and rep["summary"]["months_unavailable"] == {}
    assert rep["quarantined"][0]["kind"] == "missing_in_history"


# ================================== Zeitzone: New York mit Sommerzeit (Ticks) ---
def _ms(s):
    return int(pd.Timestamp(s).value // 1_000_000)


def test_raw_to_utc_ms_new_york_edges_and_m1_fixed():
    ok = {"2024-03-10 01:59:59.999": "2024-03-10 06:59:59.999",   # letzte EST-ms
          "2024-03-10 03:00:00": "2024-03-10 07:00:00",            # erste EDT-ms
          "2024-11-03 00:59:59.999": "2024-11-03 04:59:59.999",
          "2024-11-03 02:00:00": "2024-11-03 07:00:00",
          "2024-07-15 12:00": "2024-07-15 16:00", "2024-01-15 12:00": "2024-01-15 17:00"}
    raw = np.array([_ms(k) for k in ok], dtype=np.int64)
    assert list(hd.raw_to_utc_ms(raw, "tick")) == [_ms(v) for v in ok.values()]
    assert list(hd.raw_to_utc_ms(raw, "m1")) == list(raw + 5 * 3_600_000)      # fest UTC-5
    assert hd.raw_to_utc_ms(np.array([], dtype=np.int64), "tick").dtype == np.int64
    for bad, word in (("2024-03-10 02:00:00", "nicht existierender"),
                      ("2024-03-10 02:59:59.999", "nicht existierender"),
                      ("2024-11-03 01:00:00", "mehrdeutiger"),
                      ("2024-11-03 01:59:59.999", "mehrdeutiger"),
                      ("2006-04-02 02:30:00", "nicht existierender")):     # alte US-Regel
        with pytest.raises(hd.TimezoneError, match=word) as e:
            hd.raw_to_utc_ms(np.array([_ms("2024-07-01"), _ms(bad)]), "tick")
        assert bad[:19] in str(e.value) and isinstance(e.value, hd.FormatError)
    # m1 (fest UTC-5) kennt keine Luecken/Doppelstunden
    assert hd.raw_to_utc_ms(np.array([_ms("2024-11-03 01:30")]), "m1")[0] == _ms("2024-11-03 06:30")


def test_month_boundary_stitching_summer(tmp_path, clock):
    site = Site(clock, {("T", "202406"): tick_zip("XAUUSD", 2024, 6),
                        ("T", "202407"): tick_zip("XAUUSD", 2024, 7)})
    rep = run(tmp_path, clock, site, start=(2024, 7))
    assert rep["exit_code"] == 0, rep
    t = pq.read_table(hd.store_path(tmp_path, "XAUUSD", 2024, 7))
    assert t.schema.metadata[b"source_timezone"] == hd.SOURCE_TZ_META["tick"].encode()
    df = t.to_pandas()
    utc = pd.concat([raw_month_ticks("XAUUSD", 2024, k)["utc"] for k in (6, 7)])
    utc = utc[(utc >= pd.Timestamp("2024-07-01", tz="UTC")) & (utc < pd.Timestamp("2024-08-01", tz="UTC"))]
    assert list(df["ts"]) == sorted(set(utc.dt.floor("1min")))
    # 1.7. 00:00-04:00 UTC stammt aus Rohmonat Juni (30.6. 20:00-24:00 EDT), nur 4 h
    jun = raw_month_ticks("XAUUSD", 2024, 6)
    first = jun[jun["utc"] >= pd.Timestamp("2024-07-01", tz="UTC")].iloc[0]
    assert first["raw"] == pd.Timestamp("2024-06-30 20:00:00.137")
    assert df["ts"].iloc[0] == pd.Timestamp("2024-07-01 00:00", tz="UTC")
    assert df["bid_open"].iloc[0] == first["bid"]
    assert df["ts"].iloc[-1] == pd.Timestamp("2024-07-31 23:59", tz="UTC")
    mrep = json.loads(hd.report_path(tmp_path, "XAUUSD", 2024, 7).read_text())
    assert set(mrep["raw_months"]["2024-07"]["timezone"]["expected_offsets"]) == {"-4"}
    assert mrep["raw_months"]["2024-07"]["timezone"]["median_offset_h"] == -4


def test_needed_raw_months_and_month_end_follow_dst():
    assert hd.needed("XAUUSD", (2024, 7)) == [(2024, 6), (2024, 7)]
    assert hd.needed("XAUUSD", (2024, 1), "m1") == [(2023, 12), (2024, 1)]
    # Rohmonat Juli endet (EDT) am 1.8. 04:00 UTC, M1 (fest UTC-5) um 05:00; + 6 h Settle
    assert hd.raw_month_end_utc((2024, 7)) == datetime(2024, 8, 1, 4, tzinfo=UTC)
    assert hd.Unit("XAUUSD", "tick", 2024, 7).end_utc == datetime(2024, 8, 1, 4, tzinfo=UTC)
    assert hd.Unit("XAUUSD", "m1", 2024, 7).end_utc == datetime(2024, 8, 1, 5, tzinfo=UTC)
    assert hd._month_closed(datetime(2024, 8, 1, 10, tzinfo=UTC), (2024, 7), "tick")
    assert not hd._month_closed(datetime(2024, 8, 1, 9, 59, tzinfo=UTC), (2024, 7), "tick")
    assert not hd._month_closed(datetime(2024, 8, 1, 10, 59, tzinfo=UTC), (2024, 7), "m1")


def test_tick_at_ambiguous_local_time_quarantines_month(tmp_path, clock):
    """Tick in der doppelten Stunde (3.11. 01:30 New York): nie raten -> Exit 4."""
    nov = raw_month_ticks("XAUUSD", 2024, 11)
    extra = nov.iloc[[0]].assign(raw=pd.Timestamp("2024-11-03 01:30:00"))
    nov = pd.concat([nov, extra]).sort_values("raw", kind="stable")
    site = Site(clock, {("T", "202410"): tick_zip("XAUUSD", 2024, 10),
                        ("T", "202411"): _zip_from(nov, "XAUUSD", 2024, 11)})
    rep = run(tmp_path, clock, site, start=(2024, 11), now=datetime(2025, 1, 15, tzinfo=UTC))
    assert rep["exit_code"] == 4, rep
    q = rep["quarantine"][0]
    assert q["raw_month"] == "2024-11" and "mehrdeutiger Ortszeit" in q["reason"]
    assert "2024-11-03 01:30:00.000" in q["reason"]
    assert not hd.store_path(tmp_path, "XAUUSD", 2024, 11).exists()
    assert hd.Unit("XAUUSD", "tick", 2024, 11).qmarker(tmp_path, (2024, 11)).exists()


OLD_TZ_META = b"EST fixed UTC-5, converted to UTC, checked per month"


def _make_old_store_file(path, shift_h):
    """Monatsdatei wie von der alten Version (fest UTC-5) geschrieben."""
    t = pq.read_table(path)
    df = t.to_pandas()
    df["ts"] = df["ts"] + pd.Timedelta(hours=shift_h)
    meta = dict(t.schema.metadata)
    meta[b"source_timezone"] = OLD_TZ_META
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False).replace_schema_metadata(meta), path)


def test_stale_timezone_store_files_rebuilt_from_cache(tmp_path, clock, caplog):
    files = {("T", f"{y}{m:02d}"): tick_zip("XAUUSD", y, m)
             for y, m in ((2023, 12), (2024, 1), (2024, 6), (2024, 7))}
    site = Site(clock, files)
    for ym in ((2024, 1), (2024, 7)):
        assert run(tmp_path, clock, site, start=ym)["exit_code"] == 0
    good = {ym: pd.read_parquet(hd.store_path(tmp_path, "XAUUSD", *ym)) for ym in ((2024, 1), (2024, 7))}
    # Winter war zufaellig richtig, Sommer um 1 h verschoben - beide neu bauen
    _make_old_store_file(hd.store_path(tmp_path, "XAUUSD", 2024, 1), 0)
    _make_old_store_file(hd.store_path(tmp_path, "XAUUSD", 2024, 7), -1)
    empty = Site(clock)
    for ym, df in good.items():
        with caplog.at_level("WARNING", logger="histdata"):
            rep = run(tmp_path, clock, empty, start=ym)
        assert rep["exit_code"] == 0, rep
        assert empty.requests == 0                                  # nur Roh-Cache
        assert rep["rebuilt_timezone"] == [f"{ym[0]}-{ym[1]:02d}"]
        assert rep["summary"]["months_rebuilt_timezone"] == rep["rebuilt_timezone"]
        out = hd.store_path(tmp_path, "XAUUSD", *ym)
        assert hd.stored_timezone(out) == hd.SOURCE_TZ_META["tick"]
        pd.testing.assert_frame_equal(pd.read_parquet(out), df)
        # danach nichts mehr zu tun
        rep = run(tmp_path, clock, empty, start=ym)
        assert rep["rebuilt_timezone"] == [] and rep["summary"]["months_skipped_existing"] == 1
    assert any("source_timezone" in r.getMessage() and r.levelname == "WARNING" for r in caplog.records)
    # m1-Datei mit altem Metadatum wird ebenso neu gebaut (Inhalt gleich, fest UTC-5)
    m1 = make_zip("XAUUSD", "M1", "2024", m1_csv(pd.concat(
        [raw_month_ticks("XAUUSD", 2024, k, stamping="est") for k in (6, 7)])))
    assert run(tmp_path, clock, Site(clock, {("M1", "2024"): m1}), start=(2024, 7), mode="m1",
               now=datetime(2025, 3, 1, tzinfo=UTC))["exit_code"] == 0
    out = hd.store_path(tmp_path, "XAUUSD", 2024, 7, "m1")
    _make_old_store_file(out, 0)
    rep = run(tmp_path, clock, Site(clock), start=(2024, 7), mode="m1", now=datetime(2025, 3, 1, tzinfo=UTC))
    assert rep["exit_code"] == 0 and rep["rebuilt_timezone"] == ["2024-07"]
    assert hd.stored_timezone(out) == hd.SOURCE_TZ_META["m1"]


def test_old_hypothesis_quarantine_marker_is_rechecked(tmp_path, clock, caplog):
    files = {("T", "202406"): tick_zip("XAUUSD", 2024, 6), ("T", "202407"): tick_zip("XAUUSD", 2024, 7)}
    assert run(tmp_path, clock, Site(clock, files), start=(2024, 7))["exit_code"] == 0
    hd.store_path(tmp_path, "XAUUSD", 2024, 7).unlink()
    # Marker wie von der alten Pruefung (v1, fest UTC-5) geschrieben
    old = {"reason": "Zeitzone passt nicht zu fest UTC-5: Median-Offset -4 h statt -5 h; "
                     "nur 0% der Anker bei -5 h",
           "check": {"expected_offset_h": -5, "median_offset_h": -4.0, "first_offset_h": -4},
           "since": "2025-11-01T00:00:00+00:00", "accepted": False}
    markers = [hd.Unit("XAUUSD", "tick", 2024, m).qmarker(tmp_path, (2024, m)) for m in (6, 7)]
    for q in markers:
        q.write_text(json.dumps(old))
    empty = Site(clock)
    with caplog.at_level("WARNING", logger="histdata"):
        rep = run(tmp_path, clock, empty, start=(2024, 7))
    assert rep["exit_code"] == 0, rep["quarantined"]
    assert empty.requests == 0 and rep["summary"]["months_written"] == ["2024-07"]
    assert not any(q.exists() for q in markers)                    # neu geprueft: ok
    assert any("aelterer Zeitzonen-Hypothese" in r.getMessage() for r in caplog.records)
    # Marker der aktuellen Version blockiert weiterhin (ohne Neupruefung)
    hd.store_path(tmp_path, "XAUUSD", 2024, 7).unlink()
    cur = {**old, "reason": "Zeitzone passt nicht zu America/New_York: test",
           "raw_tz": "America/New_York", "tz_check_version": hd.TZ_CHECK_VERSION}
    markers[1].write_text(json.dumps(cur))
    rep = run(tmp_path, clock, empty, start=(2024, 7))
    assert rep["exit_code"] == 4 and rep["quarantine"][0]["reason"].endswith("test")
    # alter, aber manuell freigegebener Marker gilt weiter (Entscheidung des Menschen)
    markers[1].write_text(json.dumps({**old, "accepted": True}))
    rep = run(tmp_path, clock, empty, start=(2024, 7))
    assert rep["exit_code"] == 0 and markers[1].exists()


def test_stale_store_file_leaves_store_even_if_rebuild_fails(tmp_path, clock):
    """Scheitert der Neubau, sieht der Bot eine Luecke statt eines verschobenen Monats."""
    files = {("T", "202406"): tick_zip("XAUUSD", 2024, 6), ("T", "202407"): tick_zip("XAUUSD", 2024, 7)}
    assert run(tmp_path, clock, Site(clock, files), start=(2024, 7))["exit_code"] == 0
    out = hd.store_path(tmp_path, "XAUUSD", 2024, 7)
    _make_old_store_file(out, -1)
    for z in hd.raw_dir(tmp_path, "XAUUSD", "tick").rglob("*.zip"):      # kein Roh-Cache mehr
        z.unlink()
    rep = run(tmp_path, clock, Site(clock), start=(2024, 7))           # Server liefert nichts
    assert rep["exit_code"] != 0 and rep["rebuilt_timezone"] == ["2024-07"]
    assert not out.exists()
    assert rep["summary"]["months_stale_timezone_not_rebuilt"] == {"2024-07": OLD_TZ_META.decode()}
    stale = list((tmp_path / "metals" / "stale").rglob("XAUUSD-1m-2024-07.parquet.*.stale"))
    assert len(stale) == 1 and hd.stored_timezone(stale[0]) == OLD_TZ_META.decode()
    # ParquetSource des Bots liest nur metals/source=*/... -> die alte Datei ist unsichtbar
    assert not list((tmp_path / "metals").glob("source=*/**/*.stale"))
