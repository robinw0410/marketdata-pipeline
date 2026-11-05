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
BASE_PRICE = {"XAUUSD": 2050.0, "XAGUSD": 23.0}
SPREAD = {"XAUUSD": 0.25, "XAGUSD": 0.02}


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
    if stamping == "berlin":                  # berlin_minus_6 (Ticks, im Homelab gemessen)
        return utc_idx.tz_convert("Europe/Berlin").tz_localize(None) - pd.Timedelta(hours=6)
    if stamping == "ny":
        return utc_idx.tz_convert("America/New_York").tz_localize(None)
    assert stamping == "utc"
    return utc_idx.tz_localize(None)


def raw_month_ticks(symbol, y, m, stamping="berlin", step_s=60, price=None, early_close=None,
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


@pytest.mark.parametrize("sym", ["XAUUSD", "XAGUSD"])
@pytest.mark.parametrize("ym,off", [((2024, 1), -5), ((2024, 7), -4)])
def test_tz_tick_berlin_minus_6_passes_winter_and_summer(sym, ym, off):
    rep = hd.check_timezone(_raw_ms(raw_month_ticks(sym, *ym, stamping="berlin")), sym)
    assert rep["ok"], rep
    assert rep["rule"] == "berlin_minus_6"
    assert rep["median_offset_h"] == off and rep["median_deviation_h"] == 0
    assert rep["share_expected"] == 1.0 and set(rep["expected_offsets"]) == {off}
    assert set(rep["deviations"]) == {0}


def test_tz_tick_dst_change_inside_month_passes():
    """Maerz/Oktober (EU-Umstellung 31.3./27.10.): erwarteter Offset wechselt je
    Anker (-5 -> -4 bzw. -4 -> -5); November bleibt bei -5."""
    for ym, offs in (((2024, 3), {-5, -4}), ((2024, 10), {-4, -5}), ((2024, 11), {-5})):
        rep = hd.check_timezone(_raw_ms(raw_month_ticks("XAUUSD", *ym)), "XAUUSD",
                                hd.raw_month_bounds_ms(ym))
        assert rep["ok"], rep["reason"]
        assert set(rep["expected_offsets"]) == offs and set(rep["deviations"]) == {0}


@pytest.mark.parametrize("sym", ["XAUUSD", "XAGUSD"])
def test_tz_tick_fixed_est_fails_in_summer(sym):
    """Alte Annahme (fest UTC-5) passt fuer Ticks im Sommer nicht mehr."""
    rep = hd.check_timezone(_raw_ms(raw_month_ticks(sym, 2024, 7, stamping="est")), sym)
    assert not rep["ok"] and rep["median_offset_h"] == -5 and rep["median_deviation_h"] == -1
    assert set(rep["expected_offsets"]) == {-4}
    # im Winter ist fest UTC-5 = Berlin - 6 h, nicht unterscheidbar (und korrekt)
    assert hd.check_timezone(_raw_ms(raw_month_ticks(sym, 2024, 1, stamping="est")), sym)["ok"]


@pytest.mark.parametrize("sym", ["XAUUSD", "XAGUSD"])
@pytest.mark.parametrize("ym", [(2024, 1), (2024, 7)])
def test_tz_m1_fixed_est_passes_winter_and_summer(sym, ym):
    rep = hd.check_timezone(_raw_ms(raw_month_ticks(sym, *ym, stamping="est")), sym, mode="m1")
    assert rep["ok"], rep
    assert rep["rule"] == "fixed_est" and set(rep["expected_offsets"]) == {-5}
    assert rep["median_offset_h"] == -5 and rep["share_expected"] == 1.0


@pytest.mark.parametrize("sym", ["XAUUSD", "XAGUSD"])
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
    ny = raw_month_ticks("XAUUSD", 2024, 7, stamping="berlin")
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
    # frueher Schluss 13:00: Luecke mehrdeutig -> kein Anker, nur Warnung (v4)
    assert rep["reversals"] == 1 and rep["ambiguous_gaps"] >= 1 and rep["share_expected"] == 1.0


def test_tz_needs_enough_anchors():
    df = raw_month_ticks("XAUUSD", 2024, 7)
    few = df[df["utc"] < pd.Timestamp("2024-07-02 12:00", tz="UTC")]     # nur die Pause am 1.7.
    rep = hd.check_timezone(_raw_ms(few), "XAUUSD")
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
    # Nachbar-Rohmonate Mai/August liegen schon im Cache (fruehere Laeufe): sonst
    # waere das Ergebnis vorlaeufig und wuerde nicht als Marker gespeichert
    for m in (5, 8):
        p = hd.Unit("XAUUSD", "tick", 2024, m).path(tmp_path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(tick_zip("XAUUSD", 2024, m, stamping="est"))
    rep = run(tmp_path, clock, site, start=(2024, 7))
    assert rep["exit_code"] == 4 and rep["status"] == "quarantine"
    q = rep["quarantine"][0]
    assert "Zeitzone" in q["reason"] and q["check"]["median_offset_h"] == -5
    assert q["check"]["median_deviation_h"] == -1 and "VORLAEUFIG" not in q["reason"]
    marker = json.loads(hd.Unit("XAUUSD", "tick", 2024, 6).qmarker(tmp_path, (2024, 6)).read_text())
    assert marker["tz_check_version"] == hd.TZ_CHECK_VERSION
    assert marker["tz_rule"] == "berlin_minus_6" and marker["raw_tz"] == "Europe/Berlin"
    assert marker["tz_shift_h"] == -6
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
SWITCH = {"tick": ((2024, 10), "berlin", ("2024-10-27 01:00", "ny")),   # US-Regel letzte Woche
          "m1": ((2024, 3), "est", ("2024-03-31 01:00", "ny"))}


@pytest.mark.parametrize("mode", ["tick", "m1"])
def test_review1_dst_switch_at_month_end_is_caught(mode):
    """Wechsel kurz vor Monatsende: ein bestaetigter Lauf (>= 2 unabhaengige Luecken,
    ggf. mit Kontext aus dem Folgemonat) ist hart; ein einzelner Eintrag ohne
    Folgemonat ist nur Warnung, aber trailing_suspect (raw_slice haelt zurueck)."""
    ym, stamping, switch = SWITCH[mode]
    ms = _raw_ms(raw_month_ticks("XAUUSD", *ym, stamping=stamping, switch=switch))
    nxt = _raw_ms(raw_month_ticks("XAUUSD", *hd._ym_add(ym, 1), stamping=switch[1]))
    rep = hd.check_timezone(ms, "XAUUSD", hd.raw_month_bounds_ms(ym), mode=mode,
                            neighbours=(None, nxt[nxt < nxt[0] + hd.TZ_PAD_MS]))
    dev = 1
    assert not rep["ok"] and rep["switch_runs"] and "Stempelungswechsel" in rep["reason"], rep["reason"]
    assert {e["deviation_h"] for r in rep["switch_runs"] for e in r} == {dev}
    assert rep["edge_violations"]                                   # Bericht: Rand betroffen
    alone = hd.check_timezone(ms, "XAUUSD", hd.raw_month_bounds_ms(ym), mode=mode)
    if mode == "tick":                       # Oktober: Mo-Do-Pausen bestaetigen sich selbst
        assert not alone["ok"]
    else:                                    # Maerz: nur die Sonntagsoeffnung 31.3.
        assert alone["ok"] and alone["trailing_suspect"] and alone["warnings"]
    # Feiertag am Monatsrand (frueher Schluss, >= 2 h) bleibt erlaubt
    hol = raw_month_ticks("XAUUSD", 2024, 7, stamping="berlin" if mode == "tick" else "est",
                          early_close={date(2024, 7, 3): 13})
    assert hd.check_timezone(_raw_ms(hol), "XAUUSD", hd.raw_month_bounds_ms((2024, 7)), mode=mode)["ok"]


def test_review1_dst_switch_at_month_end_not_written(tmp_path, clock):
    ym, stamping, switch = SWITCH["tick"]
    oct_ = raw_month_ticks("XAUUSD", *ym, stamping=stamping, switch=switch)
    site = Site(clock, {("T", "202409"): tick_zip("XAUUSD", 2024, 9),
                        ("T", "202410"): _zip_from(oct_, "XAUUSD", 2024, 10)})
    rep = run(tmp_path, clock, site, start=(2024, 10))
    assert rep["exit_code"] == 4 and not hd.store_path(tmp_path, "XAUUSD", 2024, 10).exists()


def test_review1_switch_exactly_at_month_edge_keeps_previous_month(tmp_path, clock):
    files = {("T", "202402"): tick_zip("XAUUSD", 2024, 2), ("T", "202403"): tick_zip("XAUUSD", 2024, 3),
             ("T", "202404"): tick_zip("XAUUSD", 2024, 4, stamping="est")}
    site = Site(clock, files)
    rep = run(tmp_path, clock, site, start=(2024, 3), end=(2024, 4))
    # April ab dem 1. fest UTC-5: der Wechsel liegt genau auf dem Monatsrand, der
    # Maerz (und damit UTC-Maerz) ist richtig -> geschrieben; April nicht (v4: nur
    # ein Lauf, der in den Vormonat reicht, sperrt den Vormonat)
    assert rep["exit_code"] == 4 and rep["summary"]["months_written"] == ["2024-03"]
    q = {r["month"]: r for r in rep["quarantined"]}
    assert "2024-04" in q and "2024-03" not in q
    out = hd.store_path(tmp_path, "XAUUSD", 2024, 3)
    assert pq.read_schema(out).metadata[b"tz_right_padding"] == b"true"
    # nur Maerz angefragt: April nicht im Cache -> ohne rechten Kontext geprueft,
    # geschrieben mit tz_right_padding=false; sobald April im Cache ist, nachgeprueft
    b_ = tmp_path / "b"
    rep = run(b_, clock, Site(clock, files), start=(2024, 3))
    assert rep["exit_code"] == 0 and rep["summary"]["months_written"] == ["2024-03"]
    out = hd.store_path(b_, "XAUUSD", 2024, 3)
    assert pq.read_schema(out).metadata[b"tz_right_padding"] == b"false"
    assert hd.classify_store_file(out, "tick", (2024, 3))[0] == "recheck_padding"
    site = Site(clock, files)
    rep = run(b_, clock, site, start=(2024, 4))                 # holt April, Maerz wird nachgeprueft
    assert rep["relabelled_timezone"] == ["2024-03"] and site.requests == 2
    assert pq.read_schema(out).metadata[b"tz_right_padding"] == b"true"


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


# ============================ Zeitzone: Stempelungsregeln (v5, TzRule) ---
def _ms(s):
    return int(pd.Timestamp(s).value // 1_000_000)


def test_raw_to_utc_ms_rules_edges():
    B, N, E = (hd.TZ_RULES[k] for k in ("berlin_minus_6", "ny_local", "fixed_est"))
    # berlin_minus_6: Umstellung letzter So Maerz/Okt 01:00 UTC = roh Sa 20:00
    ok = {"2024-03-30 19:59:59.999": "2024-03-31 00:59:59.999",   # letzte Winter-ms
          "2024-03-30 21:00:00": "2024-03-31 01:00:00",            # erste Sommer-ms
          "2024-03-29 15:59:59": "2024-03-29 20:59:59",            # Fr-Schluss im Maerz: -5 h
          "2024-10-26 19:59:59.999": "2024-10-26 23:59:59.999",
          "2024-10-26 21:00:00": "2024-10-27 02:00:00",
          "2024-07-15 12:00": "2024-07-15 16:00", "2024-01-15 12:00": "2024-01-15 17:00"}
    raw = np.array([_ms(k) for k in ok], dtype=np.int64)
    assert list(hd.raw_to_utc_ms(raw, B)) == [_ms(v) for v in ok.values()]
    assert list(hd.raw_to_utc_ms(raw, E)) == list(raw + 5 * 3_600_000)      # fest UTC-5
    assert hd.raw_to_utc_ms(np.array([], dtype=np.int64), B).dtype == np.int64
    for rule, bad, word in ((B, "2024-03-30 20:00:00", "nicht existierender"),
                            (B, "2024-03-30 20:59:59.999", "nicht existierender"),
                            (B, "2024-10-26 20:00:00", "mehrdeutiger"),
                            (B, "2024-10-26 20:59:59.999", "mehrdeutiger"),
                            (N, "2024-03-10 02:30:00", "nicht existierender"),
                            (N, "2024-11-03 01:30:00", "mehrdeutiger")):
        with pytest.raises(hd.TimezoneError, match=word) as e:
            hd.raw_to_utc_ms(np.array([_ms("2024-07-01"), _ms(bad)]), rule)
        assert bad[:19] in str(e.value) and isinstance(e.value, hd.FormatError)
    # fixed_est kennt keine Luecken/Doppelstunden
    assert hd.raw_to_utc_ms(np.array([_ms("2024-10-26 20:30")]), E)[0] == _ms("2024-10-27 01:30")
    # Regel je Rohmonat aus dem Fahrplan
    assert list(hd.raw_to_utc_ms_mode(raw, "tick")) == [_ms(v) for v in ok.values()]
    rt = hd.utc_to_raw_ms(hd.raw_to_utc_ms(raw, B), B)
    assert list(rt) == list(raw)


def test_tz_config_validation(tmp_path, monkeypatch):
    cfg = hd.load_tz_config()
    assert hd.rule_for("tick", (2024, 3), cfg).name == "berlin_minus_6"
    assert hd.rule_for("m1", (2024, 3), cfg).name == "fixed_est"
    good = tmp_path / "ok.yaml"
    good.write_text("rules:\n  - {name: london_minus_5, zone: Europe/London, shift_h: -5}\n"
                    "schedule:\n  tick:\n    - {from: '2000-01', rule: fixed_est}\n"
                    "    - {from: '2012-01', rule: london_minus_5}\n"
                    "  m1:\n    - {from: '2000-01', rule: fixed_est}\n")
    c = hd.load_tz_config(good)
    assert hd.rule_for("tick", (2011, 12), c).name == "fixed_est"
    assert hd.rule_for("tick", (2012, 1), c) == hd.TzRule("london_minus_5", "Europe/London", -5)
    with pytest.raises(ValueError):
        hd.rule_for("tick", (1999, 12), c)
    bads = {"unknown_rule": "schedule:\n  tick:\n    - {from: '2000-01', rule: nope}\n"
                            "  m1:\n    - {from: '2000-01', rule: fixed_est}\n",
            "unsorted": "schedule:\n  tick:\n    - {from: '2012-01', rule: fixed_est}\n"
                        "    - {from: '2000-01', rule: ny_local}\n  m1:\n    - {from: '2000-01', rule: fixed_est}\n",
            "duplicate": "schedule:\n  tick:\n    - {from: '2012-01', rule: fixed_est}\n"
                         "    - {from: '2012-01', rule: ny_local}\n  m1:\n    - {from: '2000-01', rule: fixed_est}\n",
            "bad_zone": "rules:\n  - {name: x, zone: Mars/Base, shift_h: 0}\n",
            "dup_name": "rules:\n  - {name: ny_local, zone: UTC, shift_h: 0}\n",
            "missing_mode": "schedule:\n  tick:\n    - {from: '2000-01', rule: fixed_est}\n"}
    for name, text in bads.items():
        p = tmp_path / f"{name}.yaml"
        p.write_text(text)
        with pytest.raises(ValueError):
            hd.load_tz_config(p)
    # Umgebungsvariable: andere Datei; fehlt sie, Fehler statt still Default
    monkeypatch.setenv(hd.TZ_CONFIG_ENV, str(good))
    assert hd.rule_for("tick", (2013, 5)).name == "london_minus_5"
    monkeypatch.setenv(hd.TZ_CONFIG_ENV, str(tmp_path / "fehlt.yaml"))
    with pytest.raises(ValueError):
        hd.load_tz_config()


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
    """Tick in der doppelten Stunde (roh Sa 26.10. 20:30 = Berlin 02:30, doppelte Stunde): nie raten -> Exit 4."""
    oct_ = raw_month_ticks("XAUUSD", 2024, 10)
    extra = oct_.iloc[[0]].assign(raw=pd.Timestamp("2024-10-26 20:30:00"))
    oct_ = pd.concat([oct_, extra]).sort_values("raw", kind="stable")
    site = Site(clock, {("T", "202409"): tick_zip("XAUUSD", 2024, 9),
                        ("T", "202410"): _zip_from(oct_, "XAUUSD", 2024, 10)})
    rep = run(tmp_path, clock, site, start=(2024, 10), now=datetime(2025, 1, 15, tzinfo=UTC))
    assert rep["exit_code"] == 4, rep
    q = rep["quarantine"][0]
    assert q["raw_month"] == "2024-10" and "mehrdeutiger Ortszeit" in q["reason"]
    assert "2024-10-26 20:30:00.000" in q["reason"]
    assert not hd.store_path(tmp_path, "XAUUSD", 2024, 10).exists()
    assert hd.Unit("XAUUSD", "tick", 2024, 10).qmarker(tmp_path, (2024, 10)).exists()


OLD_TZ_META = b"EST fixed UTC-5, converted to UTC, checked per month"


def _make_old_store_file(path, shift_h):
    """Monatsdatei wie von der alten Version (fest UTC-5) geschrieben."""
    t = pq.read_table(path)
    df = t.to_pandas()
    df["ts"] = df["ts"] + pd.Timedelta(hours=shift_h)
    meta = dict(t.schema.metadata)
    meta[b"source_timezone"] = OLD_TZ_META
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False).replace_schema_metadata(meta), path)


def _stale_files(root):
    return sorted(p.name for p in (root / "metals" / "stale").rglob("*.stale"))


def test_stale_timezone_store_files_rebuilt_from_cache(tmp_path, clock, caplog):
    files = {("T", f"{y}{m:02d}"): tick_zip("XAUUSD", y, m)
             for y, m in ((2023, 12), (2024, 1), (2024, 6), (2024, 7))}
    site = Site(clock, files)
    for ym in ((2024, 1), (2024, 7)):
        assert run(tmp_path, clock, site, start=ym)["exit_code"] == 0
    good = {ym: pd.read_parquet(hd.store_path(tmp_path, "XAUUSD", *ym)) for ym in ((2024, 1), (2024, 7))}
    # Winter war zufaellig richtig (gleiche UTC-Zeiten: aus dem Cache nachgeprueft
    # und nur neu gekennzeichnet), Sommer um 1 h verschoben (sofort aus dem Store,
    # neu gebaut)
    _make_old_store_file(hd.store_path(tmp_path, "XAUUSD", 2024, 1), 0)
    _make_old_store_file(hd.store_path(tmp_path, "XAUUSD", 2024, 7), -1)
    assert hd.classify_store_file(hd.store_path(tmp_path, "XAUUSD", 2024, 1), "tick", (2024, 1))[0] == "recheck"
    assert hd.classify_store_file(hd.store_path(tmp_path, "XAUUSD", 2024, 7), "tick", (2024, 7))[0] \
        == "stale_shifted"
    empty = Site(clock)
    with caplog.at_level("WARNING", logger="histdata"):
        rep = run(tmp_path, clock, empty, start=(2024, 1))
    assert rep["exit_code"] == 0 and empty.requests == 0, rep        # nur Roh-Cache
    assert rep["relabelled_timezone"] == ["2024-01"] and rep["rebuilt_timezone"] == []
    assert rep["stale_retired_out_of_range"] == ["2024-07"]          # alle Dateien geprueft
    assert not hd.store_path(tmp_path, "XAUUSD", 2024, 7).exists()
    rep = run(tmp_path, clock, empty, start=(2024, 7))
    assert rep["exit_code"] == 0 and empty.requests == 0, rep
    assert rep["summary"]["months_written"] == ["2024-07"]
    for ym, df in good.items():
        out = hd.store_path(tmp_path, "XAUUSD", *ym)
        assert hd.stored_timezone(out) == hd.SOURCE_TZ_META["tick"]
        pd.testing.assert_frame_equal(pd.read_parquet(out), df, check_dtype=False)
        rep = run(tmp_path, clock, empty, start=ym)                   # danach nichts mehr zu tun
        assert rep["rebuilt_timezone"] == [] and rep["summary"]["months_skipped_existing"] == 1
    assert len(_stale_files(tmp_path)) == 1 and "2024-07" in _stale_files(tmp_path)[0]
    assert any("source_timezone" in r.getMessage() and r.levelname == "WARNING" for r in caplog.records)


def _seed_raw(root, months, stamping="berlin", sym="XAUUSD"):
    """Rohmonate direkt in den Cache legen (wie nach frueheren Laeufen)."""
    for y, m in months:
        p = hd.Unit(sym, "tick", y, m).path(root)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(tick_zip(sym, y, m, stamping=stamping))


def test_m1_old_metadata_is_rechecked_then_relabelled(tmp_path, clock):
    """M1 rechnet unveraendert fest UTC-5: altes Metadatum = gleiche Umrechnung,
    aber aeltere Pruefversion -> Rohmonate aus dem Cache neu pruefen (kein
    Download); besteht -> nur Metadaten erneuern, nie verschieben. Ohne Roh-Cache
    bleibt die Datei und wird als unverified_after_version_bump gemeldet."""
    m1 = make_zip("XAUUSD", "M1", "2024", m1_csv(pd.concat(
        [raw_month_ticks("XAUUSD", 2024, k, stamping="est") for k in (5, 6, 7, 8)])))
    now = datetime(2025, 3, 1, tzinfo=UTC)
    assert run(tmp_path, clock, Site(clock, {("M1", "2024"): m1}), start=(2024, 7), mode="m1",
               now=now)["exit_code"] == 0
    out = hd.store_path(tmp_path, "XAUUSD", 2024, 7, "m1")
    _make_old_store_file(out, 0)
    before = pd.read_parquet(out)
    assert hd.classify_store_file(out, "m1", (2024, 7))[0] == "recheck"
    site = Site(clock)
    rep = run(tmp_path, clock, site, start=(2024, 7), mode="m1", now=now)
    assert rep["exit_code"] == 0 and site.requests == 0, rep
    assert rep["rebuilt_timezone"] == [] and rep["relabelled_timezone"] == ["2024-07"]
    assert rep["summary"]["months_skipped_existing"] == 1
    assert hd.stored_timezone(out) == hd.SOURCE_TZ_META["m1"]
    pd.testing.assert_frame_equal(pd.read_parquet(out), before)
    assert not (tmp_path / "metals" / "stale").exists()
    # Roh-Cache weg: nicht nachpruefbar -> Datei bleibt (richtige Daten nicht wegwerfen)
    _make_old_store_file(out, 0)
    for z in hd.raw_dir(tmp_path, "XAUUSD", "m1").rglob("*.zip"):
        z.unlink()
    rep = run(tmp_path, clock, Site(clock), start=(2024, 7), mode="m1", now=now)
    assert rep["unverified_after_version_bump"] == ["2024-07"] and out.exists()
    assert hd.stored_timezone(out) == OLD_TZ_META.decode()


def test_v3_store_file_failing_v4_is_retired(tmp_path, clock):
    """Pruefversion im Metadatum aelter (v3), gleiche Zone: aus dem Cache neu pruefen;
    faellt durch -> aus dem Store, Quarantaene (die Version im Label zaehlt)."""
    _seed_raw(tmp_path, [(2024, 5), (2024, 8)])
    _seed_raw(tmp_path, [(2024, 6), (2024, 7)], stamping="utc")    # falsch gestempelt
    out = hd.store_path(tmp_path, "XAUUSD", 2024, 7)
    bars = hd.ticks_to_bars(hd.parse_csv(tick_csv(raw_month_ticks("XAUUSD", 2024, 7).head(50)), "tick"))
    hd.write_parquet(out, bars, "tick")
    t = pq.read_table(out)
    meta = dict(t.schema.metadata)
    meta[b"source_timezone"] = b"America/New_York -> UTC, je Rohmonat geprueft (tz_check v3)"
    pq.write_table(t.replace_schema_metadata(meta), out)
    assert hd.classify_store_file(out, "tick", (2024, 7))[0] == "recheck"
    site = Site(clock)
    rep = run(tmp_path, clock, site, start=(2024, 12))               # anderer Monat
    assert site.requests == 0 and not out.exists()
    assert "2024-07" in rep["retired_after_recheck"]
    assert hd.Unit("XAUUSD", "tick", 2024, 7).qmarker(tmp_path, (2024, 7)).exists() or \
        hd.Unit("XAUUSD", "tick", 2024, 6).qmarker(tmp_path, (2024, 6)).exists()


def test_source_tz_meta_is_derived_from_rules(tmp_path, clock, monkeypatch):
    for m in ("tick", "m1"):
        rule = hd.rule_for(m, (2024, 7))
        assert rule.name in hd.SOURCE_TZ_META[m] and rule.zone in hd.SOURCE_TZ_META[m]
        assert f"v{hd.TZ_CHECK_VERSION}" in hd.SOURCE_TZ_META[m]
        assert hd.label_rules(hd.SOURCE_TZ_META[m]) == [(rule.zone, rule.shift_h)]
    assert hd.label_rules(OLD_TZ_META.decode()) == [("Etc/GMT+5", 0)]
    assert hd.label_rules("America/New_York -> UTC, je Rohmonat geprueft (tz_check v4)") == \
        [("America/New_York", 0)]
    # M1-Datei der aktuellen Version; dann der M1-Fahrplan per Konfiguration auf
    # ny_local umgestellt (wie DATA.md es nach einer Messung vorsieht): alte
    # Dateien sind veraltet, Sommer verschoben, Winter gleiche Zeiten
    m1 = make_zip("XAUUSD", "M1", "2024", m1_csv(pd.concat(
        [raw_month_ticks("XAUUSD", 2024, k, stamping="est") for k in (1, 6, 7)])))
    now = datetime(2025, 3, 1, tzinfo=UTC)
    site = Site(clock, {("M1", "2023"): b"", ("M1", "2024"): m1})
    assert run(tmp_path, clock, site, start=(2024, 7), mode="m1", now=now)["exit_code"] == 0
    out = hd.store_path(tmp_path, "XAUUSD", 2024, 7, "m1")
    cfgf = tmp_path / "tz.yaml"
    cfgf.write_text("schedule:\n  tick:\n    - {from: '2000-01', rule: berlin_minus_6}\n"
                    "  m1:\n    - {from: '2000-01', rule: ny_local}\n")
    monkeypatch.setenv(hd.TZ_CONFIG_ENV, str(cfgf))
    assert hd.classify_store_file(out, "m1", (2024, 7))[0] == "stale_shifted"
    assert hd.classify_store_file(out, "m1", (2024, 1))[0] == "recheck"    # Winter: gleiche Zeiten


def test_stale_files_outside_range_are_retired_and_reported(tmp_path, clock):
    files = {("T", f"{y}{m:02d}"): tick_zip("XAUUSD", y, m)
             for y, m in ((2023, 12), (2024, 1), (2024, 6), (2024, 7))}
    site = Site(clock, files)
    for ym in ((2024, 1), (2024, 7)):
        assert run(tmp_path, clock, site, start=ym)["exit_code"] == 0
    jan, jul = (hd.store_path(tmp_path, "XAUUSD", *ym) for ym in ((2024, 1), (2024, 7)))
    _make_old_store_file(jan, 0)
    _make_old_store_file(jul, -1)
    jan_bytes = jan.read_bytes()
    # --verify-month berichtet, aendert aber nichts
    v = hd.verify_month("XAUUSD", (2024, 7), tmp_path, now=at(NOW), **kw(clock, Site(clock)))
    assert v["store_file"]["state"] == "stale_shifted" and jul.exists()
    assert v["store_file"]["source_timezone"] == OLD_TZ_META.decode()
    assert hd.stored_timezone(jul) == OLD_TZ_META.decode()
    # Lauf fuer einen ganz anderen (noch offenen) Monat: Juli trotzdem aus dem Store
    empty = Site(clock)
    rep = run(tmp_path, clock, empty, start=(2024, 12), end=(2024, 12))
    assert rep["exit_code"] == 0 and empty.requests == 0, rep
    assert rep["stale_retired_out_of_range"] == ["2024-07"] and not jul.exists()
    assert rep["rebuilt_timezone"] == []
    # Januar: gleiche UTC-Zeiten -> aus dem Cache nachgeprueft, nur neu gekennzeichnet
    assert rep["relabelled_timezone"] == ["2024-01"] and jan.exists()
    assert jan.read_bytes() != jan_bytes and hd.stored_timezone(jan) == hd.SOURCE_TZ_META["tick"]
    # spaeter im Bereich: beide neu gebaut, ohne Requests
    rep = run(tmp_path, clock, empty, start=(2024, 7))
    assert rep["exit_code"] == 0 and rep["summary"]["months_written"] == ["2024-07"], rep
    assert empty.requests == 0


def _old_marker_v1(**extra):
    return {"reason": "Zeitzone passt nicht zu fest UTC-5: Median-Offset -4 h statt -5 h; "
                      "nur 0% der Anker bei -5 h",
            "check": {"expected_offset_h": -5, "median_offset_h": -4.0, "first_offset_h": -4},
            "since": "2025-11-01T00:00:00+00:00", "accepted": False, **extra}


def test_old_hypothesis_quarantine_marker_is_rechecked(tmp_path, clock, caplog):
    files = {("T", "202406"): tick_zip("XAUUSD", 2024, 6), ("T", "202407"): tick_zip("XAUUSD", 2024, 7)}
    _seed_raw(tmp_path, [(2024, 5), (2024, 8)])      # Nachbarn da: Ergebnisse endgueltig
    assert run(tmp_path, clock, Site(clock, files), start=(2024, 7))["exit_code"] == 0
    out = hd.store_path(tmp_path, "XAUUSD", 2024, 7)
    out.unlink()
    markers = [hd.Unit("XAUUSD", "tick", 2024, m).qmarker(tmp_path, (2024, m)) for m in (6, 7)]
    for q in markers:
        q.write_text(json.dumps(_old_marker_v1()))
    empty = Site(clock)
    with caplog.at_level("WARNING", logger="histdata"):
        rep = run(tmp_path, clock, empty, start=(2024, 7))
    assert rep["exit_code"] == 0, rep["quarantined"]
    assert empty.requests == 0 and rep["summary"]["months_written"] == ["2024-07"]
    assert not any(q.exists() for q in markers)                    # neu geprueft: ok
    assert any("aelterer Zeitzonen-Pruefung" in r.getMessage() for r in caplog.records)
    # nicht freigegebener Marker der aktuellen Version blockiert (ohne Neupruefung)
    out.unlink()
    cur = _old_marker_v1(reason="Zeitzone passt nicht zu berlin_minus_6: test", tz_rule="berlin_minus_6",
                         raw_tz="Europe/Berlin", tz_shift_h=-6, tz_check_version=hd.TZ_CHECK_VERSION)
    markers[1].write_text(json.dumps(cur))
    rep = run(tmp_path, clock, empty, start=(2024, 7))
    assert rep["exit_code"] == 4 and rep["quarantine"][0]["reason"].endswith("test")
    # nicht freigegebener Marker der Vorversion v2 (gleiche Zone) wird neu geprueft
    markers[1].write_text(json.dumps({**cur, "tz_check_version": 2}))
    rep = run(tmp_path, clock, empty, start=(2024, 7))
    assert rep["exit_code"] == 0 and not markers[1].exists()
    # freigegebener Marker mit gleicher Umrechnung (v2/v3) gilt weiter
    out.unlink()
    markers[1].write_text(json.dumps({**cur, "tz_check_version": 2, "accepted": True}))
    rep = run(tmp_path, clock, empty, start=(2024, 7))
    assert rep["exit_code"] == 0 and markers[1].exists()
    mrep = json.loads(hd.report_path(tmp_path, "XAUUSD", 2024, 7).read_text())
    assert mrep["raw_months"]["2024-07"]["timezone"]["accepted_manually"] is True
    # v1-Freigabe auf richtigen Daten: neu geprueft, besteht -> Marker weg
    out.unlink()
    markers[1].write_text(json.dumps(_old_marker_v1(accepted=True)))
    rep = run(tmp_path, clock, empty, start=(2024, 7))
    assert rep["exit_code"] == 0 and not markers[1].exists()
    assert "accepted_manually" not in json.loads(hd.report_path(tmp_path, "XAUUSD", 2024, 7)
                                                 .read_text())["raw_months"]["2024-07"]["timezone"]


def test_v1_accepted_marker_on_data_failing_v2_blocks_again(tmp_path, clock):
    """Freigabe unter fest UTC-5 (v1) auf fest-EST-gestempelten Sommerdaten: unter
    New York 1 h verschoben -> neu pruefen, durchgefallen, Marker accepted false."""
    files = {("T", "202406"): tick_zip("XAUUSD", 2024, 6, stamping="est"),
             ("T", "202407"): tick_zip("XAUUSD", 2024, 7, stamping="est")}
    site = Site(clock, files)
    for m in (6, 7):
        q = hd.Unit("XAUUSD", "tick", 2024, m).qmarker(tmp_path, (2024, m))
        q.parent.mkdir(parents=True, exist_ok=True)
        q.write_text(json.dumps(_old_marker_v1(accepted=True)))
    hd.Unit("XAUUSD", "tick", 2024, 6).path(tmp_path).write_bytes(files[("T", "202406")])
    hd.Unit("XAUUSD", "tick", 2024, 7).path(tmp_path).write_bytes(files[("T", "202407")])
    _seed_raw(tmp_path, [(2024, 5), (2024, 8)], stamping="est")
    rep = run(tmp_path, clock, site, start=(2024, 7))
    assert rep["exit_code"] == 4 and site.requests == 0, rep
    assert not hd.store_path(tmp_path, "XAUUSD", 2024, 7).exists()
    info = json.loads(hd.Unit("XAUUSD", "tick", 2024, 6).qmarker(tmp_path, (2024, 6)).read_text())
    assert info["accepted"] is False and info["tz_check_version"] == hd.TZ_CHECK_VERSION
    assert info["tz_rule"] == "berlin_minus_6" and "Fruehere Freigabe" in info["note"]
    assert "Fruehere Freigabe" in rep["quarantine"][0]["reason"]
    # bleibt gesperrt (jetzt ein aktueller Marker)
    rep = run(tmp_path, clock, site, start=(2024, 7))
    assert rep["exit_code"] == 4 and not hd.store_path(tmp_path, "XAUUSD", 2024, 7).exists()


def test_unreadable_quarantine_marker_keeps_blocking(tmp_path, clock, caplog):
    files = {("T", "202406"): tick_zip("XAUUSD", 2024, 6), ("T", "202407"): tick_zip("XAUUSD", 2024, 7)}
    q = hd.Unit("XAUUSD", "tick", 2024, 7).qmarker(tmp_path, (2024, 7))
    q.parent.mkdir(parents=True, exist_ok=True)
    broken = '{"reason": "x", "accepted": true,,}'                  # Tippfehler beim Freigeben
    q.write_text(broken)
    for (_, period), blob in files.items():                         # Roh-Cache vorhanden
        hd.Unit("XAUUSD", "tick", int(period[:4]), int(period[4:])).path(tmp_path).write_bytes(blob)
    with caplog.at_level("ERROR", logger="histdata"):
        rep = run(tmp_path, clock, Site(clock, files), start=(2024, 7))
    assert rep["exit_code"] == 4 and "unlesbar" in rep["quarantine"][0]["reason"]
    assert q.read_text() == broken                                  # nicht geloescht
    assert any("unlesbar" in r.getMessage() and r.levelname == "ERROR" for r in caplog.records)
    assert not hd.store_path(tmp_path, "XAUUSD", 2024, 7).exists()


def test_accepted_marker_does_not_bypass_ambiguous_local_time(tmp_path, clock):
    """Freigegeben (aktuelle Umrechnung), aber ein Tick in der doppelten Stunde:
    nie raten -> bleibt gesperrt."""
    oct_ = raw_month_ticks("XAUUSD", 2024, 10)
    extra = oct_.iloc[[0]].assign(raw=pd.Timestamp("2024-10-26 20:30:00"))
    oct_ = pd.concat([oct_, extra]).sort_values("raw", kind="stable")
    site = Site(clock, {("T", "202409"): tick_zip("XAUUSD", 2024, 9),
                        ("T", "202410"): _zip_from(oct_, "XAUUSD", 2024, 10)})
    now = datetime(2025, 1, 15, tzinfo=UTC)
    assert run(tmp_path, clock, site, start=(2024, 10), now=now)["exit_code"] == 4
    q = hd.Unit("XAUUSD", "tick", 2024, 10).qmarker(tmp_path, (2024, 10))
    info = json.loads(q.read_text())
    q.write_text(json.dumps({**info, "accepted": True}))
    rep = run(tmp_path, clock, site, start=(2024, 10), now=now)
    assert rep["exit_code"] == 4 and "mehrdeutiger Ortszeit" in rep["quarantine"][0]["reason"]
    assert not hd.store_path(tmp_path, "XAUUSD", 2024, 10).exists()


def test_month_boundary_stitching_across_year_end(tmp_path, clock):
    """UTC-Januar aus Rohmonaten Dezember (letzte 5 h, EST) und Januar."""
    site = Site(clock, {("T", "202412"): tick_zip("XAUUSD", 2024, 12),
                        ("T", "202501"): tick_zip("XAUUSD", 2025, 1)})
    rep = run(tmp_path, clock, site, start=(2025, 1), now=datetime(2025, 3, 15, tzinfo=UTC))
    assert rep["exit_code"] == 0, rep
    df = pd.read_parquet(hd.store_path(tmp_path, "XAUUSD", 2025, 1))
    utc = pd.concat([raw_month_ticks("XAUUSD", *ym)["utc"] for ym in ((2024, 12), (2025, 1))])
    utc = utc[(utc >= pd.Timestamp("2025-01-01", tz="UTC")) & (utc < pd.Timestamp("2025-02-01", tz="UTC"))]
    assert list(df["ts"]) == sorted(set(utc.dt.floor("1min")))
    dec = raw_month_ticks("XAUUSD", 2024, 12)
    first = dec[dec["utc"] >= pd.Timestamp("2025-01-01", tz="UTC")].iloc[0]
    assert first["raw"] == pd.Timestamp("2024-12-31 19:00:00.137")          # 19:00 EST = 00:00 UTC
    assert df["ts"].iloc[0] == pd.Timestamp("2025-01-01 00:00", tz="UTC")
    assert df["bid_open"].iloc[0] == first["bid"]
    mrep = json.loads(hd.report_path(tmp_path, "XAUUSD", 2025, 1).read_text())
    assert set(mrep["raw_months"]) == {"2024-12", "2025-01"}


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


# ============================== andere Sommerzeitregel nur am Monatsende ---
def _us_rule_oct_2025():
    """Rohmonate 2025-09..11 XAUUSD; Oktober nach US-Regel (ny_local) gestempelt:
    bis 26.10. 01:00 UTC gleich berlin_minus_6, danach 1 h daneben (+1)."""
    sep = raw_month_ticks("XAUUSD", 2025, 9, price=3400.0)
    oct_ = raw_month_ticks("XAUUSD", 2025, 10, stamping="ny", price=3400.0)
    nov = raw_month_ticks("XAUUSD", 2025, 11, price=3400.0)
    return sep, oct_, nov


def test_us_rule_last_week_of_october_fails():
    """Wie im Homelab gemessen (2024-10): ab der EU-Umstellung gilt -5 h; eine
    Stempelung nach US-Regel waere die letzte Oktoberwoche +1 h daneben."""
    sep, oct_, nov = _us_rule_oct_2025()
    ms, b = _raw_ms(oct_), hd.raw_month_bounds_ms((2025, 10))
    pad = hd.TZ_PAD_MS
    s_, n_ = _raw_ms(sep), _raw_ms(nov)
    nb = (s_[s_ >= s_[-1] - pad], n_[n_ <= n_[0] + pad])
    rep = hd.check_timezone(ms, "XAUUSD", b, neighbours=nb)
    assert not rep["ok"] and rep["switch_runs"], rep["reason"]
    assert {e["deviation_h"] for r in rep["switch_runs"] for e in r} == {1}
    # richtige Daten (berlin_minus_6) bestehen in denselben Monaten
    for df, ym in ((sep, (2025, 9)), (raw_month_ticks("XAUUSD", 2025, 10, price=3400.0), (2025, 10)),
                   (nov, (2025, 11))):
        r = hd.check_timezone(_raw_ms(df), "XAUUSD", hd.raw_month_bounds_ms(ym))
        assert r["ok"] and not r["warnings"], (ym, r["reason"], r["warnings"])


def test_us_rule_last_week_of_october_is_not_written(tmp_path, clock):
    sep, oct_, nov = _us_rule_oct_2025()
    files = {("T", "202509"): _zip_from(sep, "XAUUSD", 2025, 9),
             ("T", "202510"): _zip_from(oct_, "XAUUSD", 2025, 10),
             ("T", "202511"): _zip_from(nov, "XAUUSD", 2025, 11)}
    now = datetime(2026, 1, 20, tzinfo=UTC)
    rep = run(tmp_path, clock, Site(clock, files), start=(2025, 10), end=(2025, 11), now=now)
    assert rep["exit_code"] == 4, rep
    assert rep["summary"]["months_written"] == []
    assert {r["raw_month"] for r in rep["quarantine"]} == {"2025-10"}
    files[("T", "202510")] = _zip_from(raw_month_ticks("XAUUSD", 2025, 10, price=3400.0), "XAUUSD", 2025, 10)
    rep = run(tmp_path / "ok", clock, Site(clock, files), start=(2025, 10), end=(2025, 11), now=now)
    assert rep["exit_code"] == 0 and rep["summary"]["months_written"] == ["2025-10", "2025-11"], rep


def test_eurusd_removed_from_histdata(tmp_path, clock, capsys):
    """EURUSD kommt nicht mehr von HistData (keine Tagespause, zu wenige Anker,
    2024-11 bestand faelschlich) - Hinweis auf den Tages-Referenzkurs."""
    assert "EURUSD" not in hd.SYMBOLS
    with pytest.raises(SystemExit) as e:
        hd.main(["--symbol", "EURUSD", "--start", "2024-01"])
    assert e.value.code == 2
    err = capsys.readouterr().err
    assert "Referenzkurs" in err and "EURUSD" in err
    with pytest.raises(ValueError, match="Referenzkurs"):
        hd.run("EURUSD", date(2024, 1, 1), date(2024, 1, 1), tmp_path, **kw(clock, Site(clock)))
    with pytest.raises(ValueError, match="Referenzkurs"):
        hd.verify_month("EURUSD", (2024, 1), tmp_path, now=at(NOW), **kw(clock, Site(clock)))
    with pytest.raises(ValueError, match="Referenzkurs"):
        hd.evaluate_raw_month(tmp_path, "EURUSD", (2024, 1))
