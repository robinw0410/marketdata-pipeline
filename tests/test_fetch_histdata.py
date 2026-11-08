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


# ----------------------------------------------------- Fahrplan der Tests ---
# Die meisten Tests pruefen Mechanik (Quarantaene, Marker, Altbestand, ...) und
# laufen mit einem Fahrplan "berlin_minus_6 fuer alle Rohmonate" und so
# gestempelten Daten (wie v5/v6). Tests mit der Fixture v7_default nutzen den
# ausgelieferten Fahrplan (ny_local bis 2018-11, berlin_minus_6_utc_midnight ab
# 2018-12) und Daten nach berlin_minus_6_utc_midnight ("daily"); v6_schedule
# den v6-Fahrplan mit "undecided" (die Mechanik gibt es weiter).
STAMPING = {"default": "berlin"}
BERLIN_EVERYWHERE = """\
rules: []
schedule:
  tick:
    - {from: "2000-01", rule: berlin_minus_6}
  m1:
    - {from: "2000-01", rule: fixed_est}
"""
V6_SCHEDULE = """\
rules: []
schedule:
  tick:
    - {from: "2000-01", rule: ny_local}
    - {from: "2018-12", rule: undecided, candidates: [ny_local, berlin_minus_6]}
    - {from: "2021-03", rule: berlin_minus_6}
  m1:
    - {from: "2000-01", rule: fixed_est}
"""


def _use_config(tmp_path_factory, monkeypatch, text, stamping):
    p = tmp_path_factory.mktemp("cfg") / "histdata_timezone.yaml"
    p.write_text(text)
    monkeypatch.setenv(hd.TZ_CONFIG_ENV, str(p))
    monkeypatch.setitem(STAMPING, "default", stamping)
    return p


# Bereich wie vor v8 (HistData-Listenbeginn, vorlaeufige Quarantaene aus) fuer die
# Mechanik-Tests; die v8-Tests (Fixture v8_range) nutzen config/histdata.yaml
OLD_RANGE = """\
history:
  start: {XAUUSD: "2009-03", XAGUSD: "2009-05"}
  end: "2099-12"
provisional:
  window_days: 0
  recheck_days: 30
  max_rechecks: 3
"""


def _use_range(tmp_path_factory, monkeypatch, text):
    p = tmp_path_factory.mktemp("rng") / "histdata.yaml"
    p.write_text(text)
    monkeypatch.setenv(hd.RANGE_CONFIG_ENV, str(p))
    return p


@pytest.fixture(autouse=True)
def _berlin_everywhere(tmp_path_factory, monkeypatch):
    _use_range(tmp_path_factory, monkeypatch, OLD_RANGE)
    return _use_config(tmp_path_factory, monkeypatch, BERLIN_EVERYWHERE, "berlin")


@pytest.fixture
def v8_range(monkeypatch):
    """Ausgelieferte config/histdata.yaml (Beginn 2009-06, Ende 2026-09, vorlaeufig 60/30/3)."""
    monkeypatch.setenv(hd.RANGE_CONFIG_ENV, str(hd.DEFAULT_RANGE_CONFIG))
    return hd.DEFAULT_RANGE_CONFIG


@pytest.fixture
def v6_schedule(tmp_path_factory, monkeypatch):
    return _use_config(tmp_path_factory, monkeypatch, V6_SCHEDULE, "berlin")


@pytest.fixture
def v7_default(monkeypatch):
    """Ausgelieferter Fahrplan (config/histdata_timezone.yaml), Daten "daily"."""
    monkeypatch.setenv(hd.TZ_CONFIG_ENV, str(hd.DEFAULT_TZ_CONFIG))
    monkeypatch.setitem(STAMPING, "default", "daily")
    return hd.DEFAULT_TZ_CONFIG


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
    if stamping == "berlin":                  # berlin_minus_6 (Wechsel So 01:00 UTC, v5/v6-Annahme)
        return utc_idx.tz_convert("Europe/Berlin").tz_localize(None) - pd.Timedelta(hours=6)
    if stamping == "daily":                   # berlin_minus_6_utc_midnight (Survey v7): Offset
        # der Berliner Zone um 00:00 UTC des UTC-Tages, also Wechsel erst Mo 00:00 UTC
        day = utc_idx.floor("D")
        off = day.tz_convert("Europe/Berlin").tz_localize(None) - day.tz_localize(None)
        return utc_idx.tz_localize(None) + off - pd.Timedelta(hours=6)
    if stamping == "ny":
        return utc_idx.tz_convert("America/New_York").tz_localize(None)
    assert stamping == "utc"
    return utc_idx.tz_localize(None)


def raw_month_ticks(symbol, y, m, stamping=None, step_s=60, price=None, early_close=None,
                    switch=None):
    """Ticks eines ROH-Monats: DataFrame raw (naiv), utc, bid, ask (Dateireihenfolge).
    Default New-York-Ortszeit (Tick-Modus); M1-Tests stempeln explizit "est".
    switch=(utc_zeitpunkt, stamping): ab dann anders gestempelt (Regimewechsel)."""
    stamping = stamping or STAMPING["default"]
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


def _oct_with_saturday_tick():
    oct_ = raw_month_ticks("XAUUSD", 2024, 10)
    extra = oct_.iloc[[0]].assign(raw=pd.Timestamp("2024-10-26 20:30:00"))   # Berlin 02:30, doppelt
    oct_ = pd.concat([oct_, extra]).sort_values("raw", kind="stable")
    return {("T", "202409"): tick_zip("XAUUSD", 2024, 9),
            ("T", "202410"): _zip_from(oct_, "XAUUSD", 2024, 10)}


def test_weekend_tick_in_impossible_hour_can_be_released(tmp_path, clock):
    """Review v5 (4): ein verirrter Tick in der unmoeglichen Stunde (roh Sa 20:30 in
    der EU-Umstellungsnacht, Markt zu) sperrt den Rohmonat - per Default. Nach
    "accepted": true werden genau diese Ticks VERWORFEN (nie umgerechnet) und gemeldet."""
    site = Site(clock, _oct_with_saturday_tick())
    now = datetime(2025, 1, 15, tzinfo=UTC)
    rep = run(tmp_path, clock, site, start=(2024, 10), now=now)
    assert rep["exit_code"] == 4 and "mehrdeutiger Ortszeit" in rep["quarantine"][0]["reason"]
    assert "freigebbar" in rep["quarantine"][0]["reason"]
    q = hd.Unit("XAUUSD", "tick", 2024, 10).qmarker(tmp_path, (2024, 10))
    info = json.loads(q.read_text())
    assert info["releasable_weekend"] == {"reversal_ticks": 0, "impossible_hour_ticks": 1,
                                          "raw_ranges": [["2024-10-26 20:30:00.000",
                                                          "2024-10-26 20:30:00.000"]]}
    assert info["accepted"] is False and "tz_error_releasable" not in info
    assert not hd.store_path(tmp_path, "XAUUSD", 2024, 10).exists()
    q.write_text(json.dumps({**info, "accepted": True}))
    rep = run(tmp_path, clock, site, start=(2024, 10), now=now)
    assert rep["exit_code"] == 0 and rep["summary"]["months_written"] == ["2024-10"], rep
    mrep = json.loads(hd.report_path(tmp_path, "XAUUSD", 2024, 10).read_text())
    tz = mrep["raw_months"]["2024-10"]["timezone"]
    assert tz["accepted_manually"] and tz["dropped_ticks"] == 1
    assert tz["dropped_raw"] == ["2024-10-26 20:30:00.000"]
    df = pd.read_parquet(hd.store_path(tmp_path, "XAUUSD", 2024, 10))
    utc = pd.concat([raw_month_ticks("XAUUSD", 2024, k)["utc"] for k in (9, 10)])
    utc = utc[(utc >= pd.Timestamp("2024-10-01", tz="UTC")) & (utc < pd.Timestamp("2024-11-01", tz="UTC"))]
    assert list(df["ts"]) == sorted(set(utc.dt.floor("1min")))       # Tick verworfen, nichts geraten


def test_non_weekend_impossible_tick_is_never_releasable(tmp_path, clock, monkeypatch):
    """Liegt ein nicht umrechenbarer Tick nicht im geschlossenen Wochenendmarkt,
    hilft auch "accepted": true nicht (hier per Ersatzfunktion simuliert)."""
    assert hd.weekend_closed_raw(np.array([_ms("2024-10-26 20:30"), _ms("2024-03-30 20:00")])).all()
    # Regel mit +14 h: die unmoegliche Stunde laege roh am Sonntagnachmittag (Markt-naehe)
    plus14 = hd.TzRule("berlin_plus_14", "Europe/Berlin", 14)
    with pytest.raises(hd.TimezoneError) as e:
        hd.raw_to_utc_ms(np.array([_ms("2024-10-27 16:30")]), plus14)
    assert not hd.weekend_closed_raw(np.array([_ms("2024-10-27 16:30")])).any()
    assert e.value.bad_mask.tolist() == [True]
    site = Site(clock, _oct_with_saturday_tick())
    now = datetime(2025, 1, 15, tzinfo=UTC)
    monkeypatch.setattr(hd, "weekend_closed_raw", lambda raw: np.zeros(len(raw), bool))
    rep = run(tmp_path, clock, site, start=(2024, 10), now=now)
    assert rep["exit_code"] == 4 and "nicht freigebbar" in rep["quarantine"][0]["reason"]
    q = hd.Unit("XAUUSD", "tick", 2024, 10).qmarker(tmp_path, (2024, 10))
    info = json.loads(q.read_text())
    assert "releasable_weekend" not in info
    q.write_text(json.dumps({**info, "accepted": True}))
    monkeypatch.undo()
    rep = run(tmp_path, clock, site, start=(2024, 10), now=now)
    assert rep["exit_code"] == 4 and not hd.store_path(tmp_path, "XAUUSD", 2024, 10).exists()


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


# ================================================ Review v5: Konfiguration ---
def test_schedule_must_cover_earliest_raw_month(tmp_path):
    """Review v5 (5): der Fahrplan muss ab Historienbeginn minus 1 Monat (2009-02)
    gelten - sonst Fehler beim Laden, nicht Exit 1 mitten im Lauf."""
    late = tmp_path / "late.yaml"
    late.write_text("schedule:\n  tick:\n    - {from: '2009-03', rule: berlin_minus_6}\n"
                    "  m1:\n    - {from: '2000-01', rule: fixed_est}\n")
    with pytest.raises(ValueError, match="2009-02"):
        hd.load_tz_config(late)
    ok = tmp_path / "ok.yaml"
    ok.write_text("schedule:\n  tick:\n    - {from: '2009-02', rule: berlin_minus_6}\n"
                  "  m1:\n    - {from: '2009-02', rule: fixed_est}\n")
    assert hd.rule_for("tick", (2009, 2), hd.load_tz_config(ok)).name == "berlin_minus_6"
    noschedule = tmp_path / "nos.yaml"
    noschedule.write_text("rules: []\n")
    with pytest.raises(ValueError, match="schedule"):
        hd.load_tz_config(noschedule)


def test_missing_default_config_is_an_error(tmp_path, monkeypatch):
    """Fehlt config/histdata_timezone.yaml, kein stiller Rueckfall auf eingebaute Werte."""
    monkeypatch.delenv(hd.TZ_CONFIG_ENV, raising=False)
    monkeypatch.setattr(hd, "DEFAULT_TZ_CONFIG", tmp_path / "gibt_es_nicht.yaml")
    with pytest.raises(ValueError, match="fehlt"):
        hd.load_tz_config()
    with pytest.raises(ValueError, match="fehlt"):
        hd.rule_for("tick", (2024, 1))


def test_old_eurusd_store_is_retired_on_any_run(tmp_path, clock, caplog):
    """Review v5 (6): HistData-EURUSD-Dateien aus v4 liegen sonst ewig im Store
    (EURUSD wird abgelehnt). Jeder Lauf verschiebt das Verzeichnis nach metals/stale/;
    Roh-Cache und Marker bleiben."""
    sdir = hd.store_dir(tmp_path, "EURUSD", "tick")
    f = sdir / "year=2024" / "EURUSD-1m-2024-07.parquet"
    f.parent.mkdir(parents=True)
    f.write_bytes(b"alt")
    raw = hd.raw_dir(tmp_path, "EURUSD", "tick") / "HISTDATA_COM_ASCII_EURUSD_T202407.zip"
    raw.parent.mkdir(parents=True)
    raw.write_bytes(b"PK")
    (raw.parent / (raw.name + ".quarantine")).write_text("{}")
    with caplog.at_level("WARNING", logger="histdata"):
        rep = run(tmp_path, clock, feb_site(clock))
    assert rep["exit_code"] == 0 and not sdir.exists()
    assert [r["symbol"] for r in rep["retired_removed_symbols"]] == ["EURUSD"]
    moved = list((tmp_path / "metals" / "stale").rglob("EURUSD-1m-2024-07.parquet"))
    assert len(moved) == 1 and moved[0].read_bytes() == b"alt"
    assert not list((tmp_path / "metals").glob("source=*/symbol=EURUSD*"))
    assert raw.exists() and (raw.parent / (raw.name + ".quarantine")).exists()
    assert sum("EURUSD" in r.getMessage() for r in caplog.records) == 1
    rep = run(tmp_path, clock, feb_site(clock))                      # danach: nichts mehr zu tun
    assert rep["retired_removed_symbols"] == []


# ====================================================== v6: Survey-Fahrplan ---
def _v6_site(clock, months, stamping):
    return Site(clock, {("T", f"{y}{m:02d}"): tick_zip("XAUUSD", y, m, stamping=stamping)
                        for y, m in months})


def _quarantine_markers(root):
    return sorted(p.name for p in (root / "metals").rglob("*.quarantine"))


def test_v7_default_schedule_is_survey_result():
    cfg = hd.load_tz_config(hd.DEFAULT_TZ_CONFIG)
    assert hd.rule_for("tick", (2018, 11), cfg).name == "ny_local"
    for ym in ((2018, 12), (2019, 3), (2021, 2), (2024, 10), (2026, 11)):
        r = hd.rule_for("tick", ym, cfg)
        assert r == hd.TZ_RULES["berlin_minus_6_utc_midnight"], ym
    assert r == hd.TzRule("berlin_minus_6_utc_midnight", "Europe/Berlin", -6, kind="dst_daily_utc")
    assert not any(isinstance(r, hd.UndecidedRule) for _, r in cfg.schedule["tick"])
    assert hd.rule_for("m1", (2015, 3), cfg).name == "fixed_est"
    assert hd.TZ_CHECK_VERSION == 7


def test_v6_undecided_identical_month_is_written(tmp_path, clock, v6_schedule):
    """2019-02: ny_local und berlin_minus_6 rechnen identisch (keine Lueckenwoche) ->
    geschrieben, gekennzeichnet mit der ersten Regel plus Kandidaten."""
    site = _v6_site(clock, [(2019, 1), (2019, 2)], "ny")
    rep = run(tmp_path, clock, site, start=(2019, 2))
    assert rep["exit_code"] == 0 and rep["summary"]["months_written"] == ["2019-02"], rep
    assert rep["months_rule_undecided"] == {}
    out = hd.store_path(tmp_path, "XAUUSD", 2019, 2)
    label = hd.stored_timezone(out)
    assert "undecided" in label and "ny_local" in label and f"v{hd.TZ_CHECK_VERSION}" in label
    utc = pd.concat([raw_month_ticks("XAUUSD", 2019, k, stamping="ny")["utc"] for k in (1, 2)])
    utc = utc[(utc >= pd.Timestamp("2019-02-01", tz="UTC")) & (utc < pd.Timestamp("2019-03-01", tz="UTC"))]
    assert list(pd.read_parquet(out)["ts"]) == sorted(set(utc.dt.floor("1min")))
    assert hd.classify_store_file(out, "tick", (2019, 2))[0] in ("current", "recheck_padding")


@pytest.mark.parametrize("stamping", ["ny", "berlin"])
def test_v6_undecided_march_with_ticks_in_window_is_held(tmp_path, clock, v6_schedule, stamping):
    """2019-03 (undecided): Ticks in der Lueckenwoche 10.-31.03. rechnen die Kandidaten
    verschieden um -> nicht geschrieben, Exit 4, kein Marker; Folgelauf ohne Requests.
    UTC-April braucht vom Rohmaerz nur die letzten Stunden (identisch) -> geschrieben."""
    site = _v6_site(clock, [(2019, 2), (2019, 3), (2019, 4)], stamping)
    rep = run(tmp_path, clock, site, start=(2019, 3))
    assert rep["exit_code"] == 4 and rep["status"] == "rule_undecided", rep
    und = rep["months_rule_undecided"]["2019-03"]
    assert und == rep["summary"]["months_rule_undecided"]["2019-03"]
    assert und["raw_month"] == "2019-03" and und["differing_ticks"] > 0
    assert und["raw_ranges"][0][0].startswith("2019-03-1")         # ab US-Beginn 10.03.
    assert all(a <= "2019-03-31" for a, _ in und["raw_ranges"])
    assert [r["kind"] for r in rep["quarantined"]] == ["rule_undecided"]
    assert rep["summary"]["months_written"] == [] and rep["quarantine"] == []
    assert not hd.store_path(tmp_path, "XAUUSD", 2019, 3).exists()
    assert _quarantine_markers(tmp_path) == []
    empty = Site(clock)
    rep2 = run(tmp_path, clock, empty, start=(2019, 3))
    assert empty.requests == 0 and rep2["exit_code"] == 4
    assert rep2["months_rule_undecided"] == rep["months_rule_undecided"]
    rep3 = run(tmp_path, clock, site, start=(2019, 4))                # laedt nur Rohmonat April
    assert rep3["exit_code"] == 0 and rep3["summary"]["months_written"] == ["2019-04"], rep3
    with pytest.raises(hd.RuleUndecided) as e:
        hd.raw_to_utc_ms(np.array([_ms("2019-03-12 10:00")]), hd.rule_for("tick", (2019, 3)))
    assert not isinstance(e.value, hd.FormatError) and "2019-03-12 10:00:00" in str(e.value)
    v = hd.verify_month("XAUUSD", (2019, 3), tmp_path, now=at(NOW), **kw(clock, empty))
    assert v["exit_code"] == 4 and v["status"] == "rule_undecided" and empty.requests == 0


def test_v6_weekend_closed_raw_window():
    assert hd.WEEKEND_CLOSED_RAW == ((4, 18), (6, 16))
    cases = {"2024-10-25 17:59:59": False, "2024-10-25 18:00:00": True, "2024-10-26 12:00": True,
             "2024-10-27 15:59:59": True, "2024-10-27 16:00:00": False, "2024-10-23 20:30": False}
    got = hd.weekend_closed_raw(np.array([_ms(k) for k in cases]))
    assert got.tolist() == list(cases.values())


STRAY = ["2024-10-26 20:20:00", "2024-10-26 20:50:00", "2024-10-26 21:00:00",    # Berlin 02:xx CEST, 03:00
         "2024-10-26 20:00:00", "2024-10-26 20:25:00"]                           # ... zurueck auf 02:00 CET


def _oct_with_stray_reversal():
    """Verirrte Ticks im geschlossenen Markt der EU-Umstellungsnacht: roh Sa 21:00 ->
    20:00 in Dateireihenfolge (60 min Ruecksprung), 4 davon in der doppelten Stunde."""
    oct_ = raw_month_ticks("XAUUSD", 2024, 10)
    i = int(np.searchsorted(oct_["raw"].to_numpy(), np.datetime64("2024-10-26 12:00")))
    extra = pd.DataFrame({"raw": pd.to_datetime(STRAY), "utc": pd.NaT,
                          "bid": oct_["bid"].iloc[i - 1], "ask": oct_["ask"].iloc[i - 1]})
    oct_ = pd.concat([oct_.iloc[:i], extra, oct_.iloc[i:]], ignore_index=True)
    return oct_, {("T", "202409"): tick_zip("XAUUSD", 2024, 9),
                  ("T", "202410"): _zip_from(oct_, "XAUUSD", 2024, 10),
                  ("T", "202411"): tick_zip("XAUUSD", 2024, 11)}


def test_v6_reversal_detail_fields():
    oct_, _ = _oct_with_stray_reversal()
    rep = hd.check_timezone(_raw_ms(oct_), "XAUUSD")
    assert not rep["ok"] and rep["large_reversals"] == [{
        "at_raw": "2024-10-26 21:00:00", "back_to_raw": "2024-10-26 20:00:00", "minutes": 60.0,
        "n_ticks": 5, "first_raw": "2024-10-26 20:00:00.000", "last_raw": "2024-10-26 21:00:00.000",
        "weekend_closed": True}]
    assert rep["reversal_ticks_total"] == 5
    assert rep["releasable_weekend"] == {
        "reversal_ticks": 5, "impossible_hour_ticks": 4,
        "raw_ranges": [["2024-10-26 20:00:00.000", "2024-10-26 21:00:00.000"]]}


def test_v6_releasable_weekend_reversal_dropped_after_accept(tmp_path, clock, monkeypatch, capsys):
    oct_, files = _oct_with_stray_reversal()
    site = Site(clock, files)
    now = datetime(2025, 1, 15, tzinfo=UTC)
    rep = run(tmp_path, clock, site, start=(2024, 10), now=now)
    assert rep["exit_code"] == 4 and "--accept-weekend-ticks 2024-10" in rep["quarantine"][0]["reason"]
    q = hd.Unit("XAUUSD", "tick", 2024, 10).qmarker(tmp_path, (2024, 10))
    info = json.loads(q.read_text())
    assert info["accepted"] is False and info["releasable_weekend"]["reversal_ticks"] == 5
    assert not hd.store_path(tmp_path, "XAUUSD", 2024, 10).exists()
    ev = hd.evaluate_raw_month(tmp_path, "XAUUSD", (2024, 10))
    assert not ev["ok"] and ev["dropped_weekend_ticks"] == 0 and ev["releasable_weekend"]["reversal_ticks"] == 5
    ev = hd.evaluate_raw_month(tmp_path, "XAUUSD", (2024, 10), drop_releasable_weekend=True)
    assert ev["ok"] and ev["dropped_weekend_ticks"] == 5, ev
    # Freigabe per CLI (kein Download), gibt die zu verwerfenden Ticks aus
    monkeypatch.setenv("MARKETDATA_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(hd.requests, "Session", lambda: pytest.fail("kein Download"))
    assert hd.main(["--symbol", "XAUUSD", "--accept-weekend-ticks", "2024-10"]) == 0
    out = capsys.readouterr().out
    assert all(f"{t}.000" in out for t in STRAY)
    assert json.loads(q.read_text())["accepted"] is True
    n = site.requests
    rep = run(tmp_path, clock, site, start=(2024, 10), now=now)
    assert rep["exit_code"] == 0 and rep["summary"]["months_written"] == ["2024-10"], rep
    assert site.requests == n
    mrep = json.loads(hd.report_path(tmp_path, "XAUUSD", 2024, 10).read_text())
    assert mrep["dropped_ticks"] == 5 and len(mrep["dropped_raw"]) == 5 and "VERWORFEN" in mrep["warning"]
    tz = mrep["raw_months"]["2024-10"]["timezone"]
    assert tz["dropped_ticks"] == 5 and tz["ok"] and tz["accepted_manually"]
    df = pd.read_parquet(hd.store_path(tmp_path, "XAUUSD", 2024, 10))
    utc = pd.concat([raw_month_ticks("XAUUSD", 2024, k)["utc"] for k in (9, 10)])
    utc = utc[(utc >= pd.Timestamp("2024-10-01", tz="UTC")) & (utc < pd.Timestamp("2024-11-01", tz="UTC"))]
    assert list(df["ts"]) == sorted(set(utc.dt.floor("1min")))       # verirrte Ticks nicht drin


def test_v6_weekday_reversal_is_never_releasable(tmp_path, clock, monkeypatch, capsys):
    """Ruecksprung Mi 10:00 -> 09:00 (Markt offen): nie freigebbar, auch nicht mit
    --accept-weekend-ticks oder einem von Hand eingetragenen releasable_weekend."""
    oct_ = raw_month_ticks("XAUUSD", 2024, 10)
    r = oct_["raw"]
    j = int(np.nonzero((r >= "2024-10-16 09:00").to_numpy())[0][0])
    k = int(np.nonzero((r >= "2024-10-16 10:00").to_numpy())[0][0])
    moved = oct_.iloc[[j]]
    oct_ = pd.concat([oct_.iloc[:j], oct_.iloc[j + 1:k + 1], moved, oct_.iloc[k + 1:]], ignore_index=True)
    rep = hd.check_timezone(_raw_ms(oct_), "XAUUSD")
    rv = rep["large_reversals"][0]
    assert rv["at_raw"].startswith("2024-10-16 10:00") and rv["weekend_closed"] is False
    assert rv["n_ticks"] == rep["reversal_ticks_total"] >= 60 and "releasable_weekend" not in rep
    files = {("T", "202409"): tick_zip("XAUUSD", 2024, 9), ("T", "202410"): _zip_from(oct_, "XAUUSD", 2024, 10),
             ("T", "202411"): tick_zip("XAUUSD", 2024, 11)}
    site = Site(clock, files)
    now = datetime(2025, 1, 15, tzinfo=UTC)
    rep = run(tmp_path, clock, site, start=(2024, 10), end=(2024, 11), now=now)
    assert rep["exit_code"] == 4 and "nicht freigebbar" in rep["quarantine"][0]["reason"]
    q = hd.Unit("XAUUSD", "tick", 2024, 10).qmarker(tmp_path, (2024, 10))
    info = json.loads(q.read_text())
    assert "releasable_weekend" not in info
    monkeypatch.setenv("MARKETDATA_DATA_ROOT", str(tmp_path))
    assert hd.main(["--symbol", "XAUUSD", "--accept-weekend-ticks", "2024-10"]) == 2
    assert "nicht freigebbar" in capsys.readouterr().err
    assert json.loads(q.read_text())["accepted"] is False
    assert hd.main(["--symbol", "XAUUSD", "--accept-weekend-ticks", "2024-09"]) == 2    # kein Marker
    # von Hand gefaelschte Freigabe: Daten werden neu bewertet -> bleibt gesperrt
    forged = {"reversal_ticks": 1, "impossible_hour_ticks": 0, "raw_ranges": [["2024-10-16 09:00:00.137"] * 2]}
    q.write_text(json.dumps({**info, "accepted": True, "releasable_weekend": forged}))
    rep = run(tmp_path, clock, site, start=(2024, 10), end=(2024, 11), now=now)
    assert rep["exit_code"] == 4 and not hd.store_path(tmp_path, "XAUUSD", 2024, 10).exists()
    assert "passt nicht mehr" in rep["quarantine"][0]["reason"]


V5_BERLIN = "berlin_minus_6 (Europe/Berlin -6 h) -> UTC, je Rohmonat geprueft (tz_check v5)"


def test_v6_migrates_v5_berlin_labelled_files(tmp_path, clock, v7_default):
    """v5 schrieb 2015 unter berlin_minus_6. v6 (ny_local bis 2018-11): 2015-03 hat in
    der Lueckenwoche andere UTC-Zeiten -> nach metals/stale/, aus dem Roh-Cache unter
    ny_local neu gebaut; 2015-01 ist identisch -> nur neu gekennzeichnet."""
    months = [(2014, 12), (2015, 1), (2015, 2), (2015, 3), (2015, 4)]
    site = _v6_site(clock, months, "ny")
    for ym in ((2015, 1), (2015, 3)):
        assert run(tmp_path, clock, site, start=ym)["exit_code"] == 0
    truth = {ym: pd.read_parquet(hd.store_path(tmp_path, "XAUUSD", *ym)) for ym in ((2015, 1), (2015, 3))}
    for ym, shift in (((2015, 1), 0), ((2015, 3), -1)):
        out = hd.store_path(tmp_path, "XAUUSD", *ym)
        t = pq.read_table(out)
        df = t.to_pandas()
        lo, hi = pd.Timestamp("2015-03-08 07:00", tz="UTC"), pd.Timestamp("2015-03-29 01:00", tz="UTC")
        win = (df["ts"] >= lo) & (df["ts"] < hi)
        df.loc[win, "ts"] = df.loc[win, "ts"] + pd.Timedelta(hours=shift)   # wie v5 umgerechnet
        meta = dict(t.schema.metadata)
        meta[b"source_timezone"] = V5_BERLIN.encode()
        pq.write_table(pa.Table.from_pandas(df, preserve_index=False).replace_schema_metadata(meta), out)
    assert hd.classify_store_file(hd.store_path(tmp_path, "XAUUSD", 2015, 1), "tick", (2015, 1))[0] == "recheck"
    assert hd.classify_store_file(hd.store_path(tmp_path, "XAUUSD", 2015, 3), "tick", (2015, 3))[0] \
        == "stale_shifted"
    empty = Site(clock)
    rep = run(tmp_path, clock, empty, start=(2015, 1), end=(2015, 3))
    assert rep["exit_code"] == 0 and empty.requests == 0, rep
    assert rep["relabelled_timezone"] == ["2015-01"] and rep["rebuilt_timezone"] == ["2015-03"]
    assert rep["summary"]["months_rebuilt_timezone"] == ["2015-03"]
    assert any("2015-03" in n for n in _stale_files(tmp_path))
    for ym, df in truth.items():
        out = hd.store_path(tmp_path, "XAUUSD", *ym)
        label = hd.stored_timezone(out)
        assert "ny_local" in label and f"v{hd.TZ_CHECK_VERSION}" in label and "berlin" not in label
        pd.testing.assert_frame_equal(pd.read_parquet(out), df, check_dtype=False)



# =============================================== Review v6: Regressionen ---
def _with_ticks(df, stamps):
    """Zusaetzliche Ticks (roh, Dateireihenfolge wie angegeben) vor dem ersten Tick
    am/nach dem ersten Stempel einfuegen."""
    i = int(np.searchsorted(df["raw"].to_numpy(), np.datetime64(stamps[0])))
    extra = pd.DataFrame({"raw": pd.to_datetime(stamps), "utc": pd.NaT,
                          "bid": df["bid"].iloc[i - 1], "ask": df["ask"].iloc[i - 1]})
    return pd.concat([df.iloc[:i], extra, df.iloc[i:]], ignore_index=True)


def _switch_2019_10_site(clock, stray):
    """Wahrheit: ny_local bis Rohmonat 2019-09, berlin_minus_6 ab 2019-10 (Fahrplan:
    undecided). Rohmonat November ohne Ticks 1.-3.11. (Ausfall); optional ein
    verirrter Tick roh Sa 2019-10-26 20:30 (doppelte Stunde von berlin_minus_6)."""
    oct_ = raw_month_ticks("XAUUSD", 2019, 10, stamping="berlin")
    if stray:
        oct_ = _with_ticks(oct_, ["2019-10-26 20:30:00"])
    nov = raw_month_ticks("XAUUSD", 2019, 11, stamping="berlin")
    nov = nov[nov["raw"] >= pd.Timestamp("2019-11-03 17:00")]
    files = {("T", "201908"): tick_zip("XAUUSD", 2019, 8, stamping="ny"),
             ("T", "201909"): tick_zip("XAUUSD", 2019, 9, stamping="ny"),
             ("T", "201910"): _zip_from(oct_, "XAUUSD", 2019, 10),
             ("T", "201911"): _zip_from(nov, "XAUUSD", 2019, 11),
             ("T", "201912"): tick_zip("XAUUSD", 2019, 12, stamping="berlin")}
    return Site(clock, files), oct_


@pytest.mark.parametrize("stray", [True, False], ids=["stray", "no_stray"])
def test_review_v6_undecided_erroring_candidate_still_compared(tmp_path, clock, v6_schedule, stray):
    """HIGH 2: wirft ein Kandidat (berlin_minus_6) wegen EINES verirrten Ticks
    TimezoneError, muessen seine uebrigen Umrechnungen trotzdem verglichen werden.
    Vorher: nur der verirrte Tick zaehlte als abweichend, UTC-November wurde aus den
    Ticks vom 31.10. abends nach ny_local geschrieben (54 Minuten verschoben)."""
    site, oct_ = _switch_2019_10_site(clock, stray)
    und = hd.rule_for("tick", (2019, 10))
    with pytest.raises(hd.RuleUndecided) as e:
        hd.raw_to_utc_ms(_raw_ms(oct_), und)
    assert len(e.value.differing_raw) > 1000                    # ganze Lueckenwoche, nicht 1
    rep = run(tmp_path, clock, site, start=(2019, 9), end=(2019, 11))
    assert rep["exit_code"] == 4, rep
    assert not hd.store_path(tmp_path, "XAUUSD", 2019, 11).exists()
    assert not hd.store_path(tmp_path, "XAUUSD", 2019, 10).exists()
    held = rep["months_rule_undecided"]
    assert {"2019-10", "2019-11"} <= set(held), rep["quarantined"]
    assert held["2019-11"]["raw_month"] == "2019-10"
    assert held["2019-10"]["differing_ticks"] > 1000
    assert rep["summary"]["months_written"] == ["2019-09"]


def test_review_v6_plain_accept_never_converts_reversal_or_impossible_ticks(tmp_path, clock, caplog):
    """MEDIUM 3: ein einfaches "accepted": true (ohne releasable_weekend) hebt eine
    Quarantaene wegen Ruecksprung oder nicht eindeutiger Ortszeit NICHT auf."""
    now = datetime(2025, 1, 15, tzinfo=UTC)
    # a) Ruecksprung unter der Woche (nicht freigebbar)
    oct_ = raw_month_ticks("XAUUSD", 2024, 10)
    r = oct_["raw"]
    j = int(np.nonzero((r >= "2024-10-16 09:00").to_numpy())[0][0])
    k = int(np.nonzero((r >= "2024-10-16 10:00").to_numpy())[0][0])
    oct_ = pd.concat([oct_.iloc[:j], oct_.iloc[j + 1:k + 1], oct_.iloc[[j]], oct_.iloc[k + 1:]],
                     ignore_index=True)
    files = {("T", "202409"): tick_zip("XAUUSD", 2024, 9), ("T", "202410"): _zip_from(oct_, "XAUUSD", 2024, 10),
             ("T", "202411"): tick_zip("XAUUSD", 2024, 11)}
    site = Site(clock, files)
    rep = run(tmp_path, clock, site, start=(2024, 10), end=(2024, 11), now=now)
    assert rep["exit_code"] == 4
    assert "--accept-weekend-ticks" in rep["hint"] and "NICHT bei Rueckspruengen" in rep["hint"]
    q = hd.Unit("XAUUSD", "tick", 2024, 10).qmarker(tmp_path, (2024, 10))
    q.write_text(json.dumps({**json.loads(q.read_text()), "accepted": True}))
    with caplog.at_level("ERROR", logger="histdata"):
        rep = run(tmp_path, clock, site, start=(2024, 10), end=(2024, 11), now=now)
    assert rep["exit_code"] == 4 and not hd.store_path(tmp_path, "XAUUSD", 2024, 10).exists()
    reason = rep["quarantine"][0]["reason"]
    assert "IGNORIERT" in reason and "nicht freigebbar" in reason
    assert rep["quarantine"][0]["check"]["accepted_ignored"] is True
    assert any("IGNORIERT" in x.getMessage() for x in caplog.records if x.levelname == "ERROR")
    # b) Tick in der doppelten Stunde (Wochenende), Marker ohne releasable_weekend
    root = tmp_path / "b"
    site = Site(clock, _oct_with_saturday_tick())
    run(root, clock, site, start=(2024, 10), now=now)
    q = hd.Unit("XAUUSD", "tick", 2024, 10).qmarker(root, (2024, 10))
    info = json.loads(q.read_text())
    info.pop("releasable_weekend")
    q.write_text(json.dumps({**info, "accepted": True}))
    rep = run(root, clock, site, start=(2024, 10), now=now)
    assert rep["exit_code"] == 4 and not hd.store_path(root, "XAUUSD", 2024, 10).exists()
    assert "IGNORIERT" in rep["quarantine"][0]["reason"]
    assert "--accept-weekend-ticks 2024-10" in rep["quarantine"][0]["reason"]
    # c) andere manuelle Gruende bleiben freigebbar (Zeitzone passt nicht, kein Ruecksprung)
    root = tmp_path / "c"
    for m in (5, 6, 7, 8, 9):
        p = hd.Unit("XAUUSD", "tick", 2024, m).path(root)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(tick_zip("XAUUSD", 2024, m, stamping="est"))
    empty = Site(clock)
    for _ in range(5):                         # Rohmonate werden nacheinander geprueft
        rep = run(root, clock, empty, start=(2024, 7))
        if rep["exit_code"] != 4:
            break
        for qm in (root / "metals").rglob("*.quarantine"):
            info = json.loads(qm.read_text())
            assert "IGNORIERT" not in (info.get("reason") or "")
            qm.write_text(json.dumps({**info, "accepted": True}))
    assert rep["exit_code"] == 0 and rep["summary"]["months_written"] == ["2024-07"], rep


def test_review_v6_reversal_first_last_raw_are_min_max():
    """LOW 4: first_raw/last_raw = frueheste/spaeteste Rohzeit des Intervalls (nicht
    Dateireihenfolge); n_ticks je eigenem Intervall, Ueberlappung zaehlt doppelt."""
    base = raw_month_ticks("XAUUSD", 2024, 10)
    df = _with_ticks(base, ["2024-10-19 20:30", "2024-10-19 21:00", "2024-10-19 20:00", "2024-10-19 20:40"])
    rep = hd.check_timezone(_raw_ms(df), "XAUUSD")
    assert rep["large_reversals"] == [{
        "at_raw": "2024-10-19 21:00:00", "back_to_raw": "2024-10-19 20:00:00", "minutes": 60.0,
        "n_ticks": 4, "first_raw": "2024-10-19 20:00:00.000", "last_raw": "2024-10-19 21:00:00.000",
        "weekend_closed": True}]
    assert rep["reversal_ticks_total"] == 4
    df = _with_ticks(base, ["2024-10-19 20:30", "2024-10-19 21:00", "2024-10-19 20:00",
                            "2024-10-19 20:50", "2024-10-19 20:10"])
    rep = hd.check_timezone(_raw_ms(df), "XAUUSD")
    assert [r["n_ticks"] for r in rep["large_reversals"]] == [5, 3]
    assert rep["reversal_ticks_total"] == 5
    assert rep["large_reversals"][1]["first_raw"] == "2024-10-19 20:10:00.000"
    assert rep["large_reversals"][1]["last_raw"] == "2024-10-19 20:50:00.000"


def test_review_v6_undecided_impossible_hours_are_union_of_candidates(tmp_path, clock, v6_schedule, monkeypatch):
    """LOW 6: unter "undecided" sind die verbotenen Stunden die Vereinigung ueber alle
    Kandidaten: ein berlin_minus_6-Streutick (roh Sa 20:02, ausserhalb des Ruecksprungs)
    wird mit freigegeben und verworfen; vorher zaehlte nur ny_local (erster Kandidat)."""
    und = hd.rule_for("tick", (2019, 10))
    stray = np.array([_ms("2019-10-26 20:02"), _ms("2019-10-26 20:30")])
    assert hd._impossible_mask(stray, rule=hd.TZ_RULES["ny_local"]).tolist() == [False, False]
    assert hd._impossible_mask(stray, rule=und).tolist() == [True, True]
    oct_ = _with_ticks(raw_month_ticks("XAUUSD", 2019, 10, stamping="berlin"),
                       ["2019-10-26 20:02", "2019-10-26 20:20", "2019-10-26 20:50", "2019-10-26 21:00",
                        "2019-10-26 20:05", "2019-10-26 20:30"])
    rep = hd.check_timezone(_raw_ms(oct_), "XAUUSD")
    assert rep["releasable_weekend"]["reversal_ticks"] == 5
    assert rep["releasable_weekend"]["impossible_hour_ticks"] == 5
    assert rep["releasable_weekend"]["raw_ranges"] == [["2019-10-26 20:02:00.000", "2019-10-26 21:00:00.000"]]
    files = {("T", "201909"): tick_zip("XAUUSD", 2019, 9, stamping="ny"),
             ("T", "201910"): _zip_from(oct_, "XAUUSD", 2019, 10),
             ("T", "201911"): tick_zip("XAUUSD", 2019, 11, stamping="berlin")}
    site = Site(clock, files)
    rep = run(tmp_path, clock, site, start=(2019, 10), end=(2019, 11))
    assert rep["exit_code"] == 4 and rep["quarantine"][0]["raw_month"] == "2019-10"
    q = hd.Unit("XAUUSD", "tick", 2019, 10).qmarker(tmp_path, (2019, 10))
    assert json.loads(q.read_text())["releasable_weekend"]["impossible_hour_ticks"] == 5
    monkeypatch.setenv("MARKETDATA_DATA_ROOT", str(tmp_path))
    assert hd.main(["--symbol", "XAUUSD", "--accept-weekend-ticks", "2019-10"]) == 0
    rep = run(tmp_path, clock, site, start=(2019, 10), end=(2019, 11))
    # Streuticks verworfen; Oktober bleibt nur wegen der Lueckenwoche offen (undecided)
    assert rep["quarantine"] == [] and "2019-10" in rep["months_rule_undecided"], rep["quarantined"]
    ranges = rep["months_rule_undecided"]["2019-10"]["raw_ranges"]
    assert not any(a.startswith("2019-10-26 20:") for a, _ in ranges), ranges


# ========================== v7: berlin_minus_6_utc_midnight (Survey 2009-2025) ---
DAILY = hd.TZ_RULES["berlin_minus_6_utc_midnight"]


def _last_sunday(y, m):
    d = date(y, m + 1, 1) - timedelta(days=1)
    return d - timedelta(days=(d.weekday() - 6) % 7)


@pytest.mark.parametrize("y", range(2019, 2026))
def test_v7_conversion_around_both_transition_nights(y):
    """Herbst: roh So 19:59:59 -> 23:59:59 UTC, der naechste Tick roh 19:00:00 (Ruecksprung in
    Dateireihenfolge) -> Mo 00:00:00 UTC. Fruehjahr: roh So 18:59:59 -> 23:59:59 UTC,
    roh 20:00:00 -> Mo 00:00:00 UTC (roh 19:00-19:59:59 existiert nicht)."""
    for m, raw, utc in ((10, ["19:30:00", "19:59:59", "19:00:00", "19:30:00", "20:00:00"],
                         ["23:30:00", "23:59:59", "+00:00:00", "+00:30:00", "+01:00:00"]),
                        (3, ["18:59:59", "20:00:00", "20:30:00"], ["23:59:59", "+00:00:00", "+00:30:00"])):
        sun = _last_sunday(y, m)
        mon = sun + timedelta(days=1)
        r = np.array([_ms(f"{sun} {t}") for t in raw])
        want = [_ms(f"{mon} {t[1:]}") if t.startswith("+") else _ms(f"{sun} {t}") for t in utc]
        assert list(hd.raw_to_utc_ms(r, DAILY)) == want, (y, m)
        assert list(hd.utc_to_raw_ms(np.array(want), DAILY)) == list(r)
        assert hd.tz_offset_h(_ms(f"{sun} 23:59:59"), DAILY) == (-4 if m == 10 else -5)
        assert hd.tz_offset_h(_ms(f"{mon} 00:00"), DAILY) == (-5 if m == 10 else -4)
    sun = _last_sunday(y, 10)
    with pytest.raises(hd.TimezoneError, match="OHNE Ruecksprung") as e:   # sortiert: keine Seite bekannt
        hd.raw_to_utc_ms(np.array([_ms(f"{sun} 19:10"), _ms(f"{sun} 19:20"), _ms(f"{sun} 20:10")]), DAILY)
    assert e.value.bad_mask.tolist() == [True, True, False]
    sun = _last_sunday(y, 3)
    with pytest.raises(hd.TimezoneError, match="nicht existierender") as e:
        hd.raw_to_utc_ms(np.array([_ms(f"{sun} 18:59"), _ms(f"{sun} 19:30"), _ms(f"{sun} 20:00")]), DAILY)
    assert e.value.bad_mask.tolist() == [False, True, False]


def test_v7_transition_nights_and_expected_reversal_in_check():
    oct_ = raw_month_ticks("XAUUSD", 2024, 10, stamping="daily")
    raw = _raw_ms(oct_)
    rep = hd.check_timezone(raw, "XAUUSD", hd.raw_month_bounds_ms((2024, 10)), rule=DAILY)
    assert rep["ok"], rep["reason"]
    assert rep["large_reversals"] == [] and rep["reversal_ticks_total"] == 0
    assert "releasable_weekend" not in rep
    assert [(x["at_raw"][:16], x["back_to_raw"][:16]) for x in rep["expected_reversals"]] == \
        [("2024-10-27 19:59", "2024-10-27 19:00")]
    tn = [t for t in rep["transition_nights"] if t["rule_transition"]]
    assert len(tn) == 1 and tn[0]["date"] == "2024-10-27" and tn[0]["kind"] == "fall"
    assert tn[0]["status"] == "ok" and tn[0]["reversal_found"] and tn[0]["ticks_in_fold"] == 120
    assert tn[0]["last_before_raw"].startswith("2024-10-27 19:59")
    assert tn[0]["first_after_raw"].startswith("2024-10-27 19:00")
    # die erwartete doppelte Stunde ist nie verwerfbar
    drop, rel = hd.weekend_release(raw, "tick", DAILY)
    assert rel is None and not drop.any()
    # sortiert (Dateireihenfolge verloren): nicht aufloesbar -> unexpected + Fehler
    rep = hd.check_timezone(np.sort(raw), "XAUUSD", hd.raw_month_bounds_ms((2024, 10)), rule=DAILY)
    tn = [t for t in rep["transition_nights"] if t["rule_transition"]][0]
    assert tn["status"] == "unexpected" and not tn["reversal_found"] and not rep["ok"]
    assert "Umstellungsnacht 2024-10-27" in rep["reason"]
    # alte Annahme berlin_minus_6 (Wechsel So 01:00 UTC) passt NICHT zu diesen Daten
    assert not hd.check_timezone(raw, "XAUUSD", hd.raw_month_bounds_ms((2024, 10)),
                                 rule=hd.TZ_RULES["berlin_minus_6"])["ok"]
    # Fruehjahr: Luecke roh So 19:00-19:59, Daten auf beiden Seiten -> ok
    mar = _raw_ms(raw_month_ticks("XAUUSD", 2024, 3, stamping="daily"))
    rep = hd.check_timezone(mar, "XAUUSD", hd.raw_month_bounds_ms((2024, 3)), rule=DAILY)
    tn = [t for t in rep["transition_nights"] if t["rule_transition"]]
    assert rep["ok"] and tn[0]["kind"] == "spring" and tn[0]["status"] == "ok" and tn[0]["ticks_in_gap"] == 0
    assert tn[0]["last_before_raw"].startswith("2024-03-31 18:59")
    assert tn[0]["first_after_raw"].startswith("2024-03-31 20:00")
    bad = np.sort(np.concatenate([mar, [_ms("2024-03-31 19:30")]]))
    rep = hd.check_timezone(bad, "XAUUSD", hd.raw_month_bounds_ms((2024, 3)), rule=DAILY)
    tn = [t for t in rep["transition_nights"] if t["rule_transition"]][0]
    assert tn["status"] == "unexpected" and tn["ticks_in_gap"] == 1 and not rep["ok"]
    # andere Regel ohne EU-Wechsel: EU-Sonntag nur beobachtet
    ny = hd.check_timezone(raw, "XAUUSD", hd.raw_month_bounds_ms((2024, 10)), rule=hd.TZ_RULES["ny_local"])
    obs = [t for t in ny["transition_nights"] if not t["rule_transition"]]
    assert obs and obs[0]["date"] == "2024-10-27" and obs[0]["observed"] == "fold_reversal"


def test_v7_run_writes_october_and_march_with_transition_nights(tmp_path, clock, v7_default):
    """UTC-Oktober 2024 aus Rohdaten nach berlin_minus_6_utc_midnight: doppelte Rohstunde
    nach Dateireihenfolge aufgeloest, kein Tick verloren, UTC monoton; Maerz ebenso."""
    months = [(2024, k) for k in (2, 3, 4, 9, 10, 11)]
    site = Site(clock, {("T", f"{y}{m:02d}"): tick_zip("XAUUSD", y, m) for y, m in months})
    now = datetime(2025, 1, 15, tzinfo=UTC)
    for ym in ((2024, 3), (2024, 10)):
        rep = run(tmp_path, clock, site, start=ym, end=hd._ym_add(ym, 1), now=now)
        assert rep["exit_code"] == 0, rep["quarantined"]
        assert rep["summary"]["months_written"] == [hd._ymstr(ym), hd._ymstr(hd._ym_add(ym, 1))]
        ticks = pd.concat([raw_month_ticks("XAUUSD", *hd._ym_add(ym, k)) for k in (-1, 0, 1)])
        u = ticks["utc"]
        lo = pd.Timestamp(datetime(*ym, 1, tzinfo=UTC))
        u = u[(u >= lo) & (u < lo + pd.offsets.MonthBegin(1))]
        df = pd.read_parquet(hd.store_path(tmp_path, "XAUUSD", *ym))
        assert list(df["ts"]) == sorted(set(u.dt.floor("1min")))
        assert int(df["tick_count"].sum()) == len(u)                 # nichts verloren
        conv = hd.raw_to_utc_ms(_raw_ms(raw_month_ticks("XAUUSD", *ym)), DAILY)
        assert (np.diff(conv) > 0).all()                            # UTC monoton
        tz = json.loads(hd.report_path(tmp_path, "XAUUSD", *ym).read_text())["raw_months"][hd._ymstr(ym)]["timezone"]
        night = [t for t in tz["transition_nights"] if t["rule_transition"]][0]
        assert night["status"] == "ok" and night["kind"] == ("spring" if ym[1] == 3 else "fall")
        assert len(tz["expected_reversals"]) == (1 if ym[1] == 10 else 0)
        assert "berlin_minus_6_utc_midnight" in hd.stored_timezone(hd.store_path(tmp_path, "XAUUSD", *ym))


def test_v7_weekend_strays_still_releasable_but_fold_never(tmp_path, clock, v7_default, monkeypatch):
    """Echte Wochenend-Streuticks (Ruecksprung Sa 21:00 -> 20:00) bleiben freigebbar; die
    erwartete doppelte Stunde So 19:xx wird dabei weder verworfen noch gezaehlt."""
    oct_ = _with_ticks(raw_month_ticks("XAUUSD", 2024, 10),
                       ["2024-10-26 20:20", "2024-10-26 20:50", "2024-10-26 21:00",
                        "2024-10-26 20:00", "2024-10-26 20:25"])
    rep = hd.check_timezone(_raw_ms(oct_), "XAUUSD", hd.raw_month_bounds_ms((2024, 10)))
    assert rep["releasable_weekend"] == {"reversal_ticks": 5, "impossible_hour_ticks": 0,
                                         "raw_ranges": [["2024-10-26 20:00:00.000", "2024-10-26 21:00:00.000"]]}
    assert len(rep["expected_reversals"]) == 1 and len(rep["large_reversals"]) == 1
    files = {("T", f"2024{m:02d}"): tick_zip("XAUUSD", 2024, m) for m in (9, 11)}
    files[("T", "202410")] = _zip_from(oct_, "XAUUSD", 2024, 10)
    site = Site(clock, files)
    now = datetime(2025, 1, 15, tzinfo=UTC)
    rep = run(tmp_path, clock, site, start=(2024, 10), end=(2024, 11), now=now)
    assert rep["exit_code"] == 4 and "freigebbar" in rep["quarantine"][0]["reason"]
    monkeypatch.setenv("MARKETDATA_DATA_ROOT", str(tmp_path))
    assert hd.main(["--symbol", "XAUUSD", "--accept-weekend-ticks", "2024-10"]) == 0
    rep = run(tmp_path, clock, site, start=(2024, 10), end=(2024, 11), now=now)
    assert rep["exit_code"] == 0 and "2024-10" in rep["summary"]["months_written"], rep["quarantined"]
    mrep = json.loads(hd.report_path(tmp_path, "XAUUSD", 2024, 10).read_text())
    assert mrep["dropped_ticks"] == 5                            # nur die Sa-Streuticks
    df = pd.read_parquet(hd.store_path(tmp_path, "XAUUSD", 2024, 10))
    u = pd.concat([raw_month_ticks("XAUUSD", 2024, k)["utc"] for k in (9, 10)])
    u = u[(u >= pd.Timestamp("2024-10-01", tz="UTC")) & (u < pd.Timestamp("2024-11-01", tz="UTC"))]
    assert int(df["tick_count"].sum()) == len(u)


V6_BERLIN = "berlin_minus_6 (Europe/Berlin -6 h) -> UTC, je Rohmonat geprueft (tz_check v6)"


def test_v7_migrates_v6_files_markers_and_undecided_months(tmp_path, clock, v7_default):
    """v6 -> v7: berlin_minus_6-Datei fuer 2024-03 (anders am Umstellungssonntag) ->
    metals/stale/ und neu gebaut; 2024-01 gleich -> neu gekennzeichnet; v6-Quarantaene des
    Oktober-Ruecksprungs -> neu geprueft, bestanden, geschrieben; ein unter v6 als
    undecided zurueckgehaltener Monat (2019-03) wird jetzt geschrieben. Ohne Requests."""
    months = [(2023, 12)] + [(2024, m) for m in range(1, 12)] + [(2019, 2), (2019, 3), (2019, 4)]
    _seed_raw(tmp_path, months, stamping="daily")
    empty = Site(clock)
    now = datetime(2025, 1, 15, tzinfo=UTC)
    for ym in ((2024, 1), (2024, 3)):
        assert run(tmp_path, clock, empty, start=ym, now=now)["exit_code"] == 0
    truth = {ym: pd.read_parquet(hd.store_path(tmp_path, "XAUUSD", *ym)) for ym in ((2024, 1), (2024, 3))}
    assert truth[(2024, 3)]["ts"].isin([pd.Timestamp("2024-03-31 22:00", tz="UTC")]).any()
    for ym, shift in (((2024, 1), 0), ((2024, 3), 1)):
        out = hd.store_path(tmp_path, "XAUUSD", *ym)
        t = pq.read_table(out)
        df = t.to_pandas()
        sun = (df["ts"] >= pd.Timestamp("2024-03-31 01:00", tz="UTC")) & \
            (df["ts"] < pd.Timestamp("2024-04-01", tz="UTC"))
        df.loc[sun, "ts"] = df.loc[sun, "ts"] - pd.Timedelta(hours=shift)   # wie v6 umgerechnet
        meta = dict(t.schema.metadata)
        meta[b"source_timezone"] = V6_BERLIN.encode()
        pq.write_table(pa.Table.from_pandas(df, preserve_index=False).replace_schema_metadata(meta), out)
    assert hd.classify_store_file(hd.store_path(tmp_path, "XAUUSD", 2024, 1), "tick", (2024, 1))[0] == "recheck"
    assert hd.classify_store_file(hd.store_path(tmp_path, "XAUUSD", 2024, 3), "tick", (2024, 3))[0] \
        == "stale_shifted"
    q = hd.Unit("XAUUSD", "tick", 2024, 10).qmarker(tmp_path, (2024, 10))
    q.write_text(json.dumps({"reason": "Zeitzone passt nicht zu berlin_minus_6: 1 Ruecksprung", "check": None,
                             "since": "2025-01-01T00:00:00+00:00", "accepted": False,
                             "tz_rule": "berlin_minus_6", "raw_tz": "Europe/Berlin", "tz_shift_h": -6,
                             "tz_check_version": 6}))
    rep = run(tmp_path, clock, empty, start=(2024, 1), end=(2024, 10), now=now)
    assert rep["exit_code"] == 0 and empty.requests == 0, rep["quarantined"]
    assert rep["relabelled_timezone"] == ["2024-01"] and rep["rebuilt_timezone"] == ["2024-03"]
    assert any("2024-03" in n for n in _stale_files(tmp_path)) and not q.exists()
    assert "2024-10" in rep["summary"]["months_written"]
    for ym, df in truth.items():
        out = hd.store_path(tmp_path, "XAUUSD", *ym)
        assert hd.stored_timezone(out) == hd.store_tz_label("tick", ym)
        assert "berlin_minus_6_utc_midnight" in hd.stored_timezone(out) and "v7" in hd.stored_timezone(out)
        pd.testing.assert_frame_equal(pd.read_parquet(out), df, check_dtype=False)
    rep = run(tmp_path, clock, empty, start=(2019, 3), now=now)
    assert rep["exit_code"] == 0 and rep["summary"]["months_written"] == ["2019-03"], rep["quarantined"]
    assert rep["months_rule_undecided"] == {}


def test_v7_rule_kind_in_config(tmp_path):
    ok = tmp_path / "ok.yaml"
    ok.write_text("rules:\n  - {name: paris_daily, zone: Europe/Paris, shift_h: -6, kind: dst_daily_utc}\n"
                  "schedule:\n  tick:\n    - {from: '2000-01', rule: paris_daily}\n"
                  "  m1:\n    - {from: '2000-01', rule: fixed_est}\n")
    r = hd.rule_for("tick", (2024, 10), hd.load_tz_config(ok))
    assert r == hd.TzRule("paris_daily", "Europe/Paris", -6, kind="dst_daily_utc")
    bad = tmp_path / "bad.yaml"
    bad.write_text(ok.read_text().replace("dst_daily_utc", "weekly"))
    with pytest.raises(ValueError, match="kind"):
        hd.load_tz_config(bad)
    assert hd.label_rules(hd.source_tz_label(DAILY)) == [("Europe/Berlin", -6, "dst_daily_utc")]
    assert hd.rule_identity(DAILY) == ("Europe/Berlin|dst_daily_utc", -6)


# ========================================================= Review v7 ---
def _fold_conv(stamps, **kw):
    return hd.raw_to_utc_ms(np.array([_ms(s) for s in stamps]), DAILY, **kw)


S21 = "2021-10-31 "


def test_review_v7_fold_by_shared_helper_consistent_everywhere():
    """Items 3/4/5: >= 30 min an EINER Stelle (_big_drops); Umrechnung, expected_reversals
    und transition_nights nutzen denselben Faltensprung (fold_reversal); ein Tick, der
    nach der Aufloesung die UTC-Reihenfolge bricht, macht die Nacht unaufgeloest."""
    cases = {  # Ticks roh (Dateireihenfolge) -> aufloesbar?
        "regular": (["18:59", "19:40", "19:59:59", "19:00", "19:30", "20:10"], True),
        "exact_30": (["18:59", "19:30", "19:00", "19:10"], True),
        "drop_20": (["18:59", "19:20", "19:00", "20:10"], False),
        "late_pre_tick": (["19:50", "19:00", "19:30", "19:58", "19:40"], False),   # fold.py
        "stray_first": (["19:10", "19:58", "19:15", "19:59", "19:00", "19:05"], False),
    }
    bounds = hd.raw_month_bounds_ms((2021, 10))
    for name, (ts, ok) in cases.items():
        raw = np.array([_ms(S21 + t) for t in ts])
        try:
            u = hd.raw_to_utc_ms(raw, DAILY)
            conv = True
            assert (np.diff(u) >= 0).all(), name
        except hd.TimezoneError as e:
            conv = False
            assert e.fold_unresolved["date"] == "2021-10-31", name
        rep = hd.check_timezone(raw, "XAUUSD", bounds, rule=DAILY)
        night = [t for t in rep["transition_nights"] if t["rule_transition"]][0]
        assert conv is ok, name
        assert bool(rep["expected_reversals"]) is ok and night["reversal_found"] is ok, name
        assert (night["status"] == "ok") is ok, name
        if ok:
            assert not rep["large_reversals"], name
    # genau 30 min: auch ausserhalb der Falte ein Ruecksprung (gleicher Vergleich)
    wd = np.array([_ms("2021-10-13 10:30"), _ms("2021-10-13 10:00")])
    assert hd._big_drops(wd).tolist() == [1]
    assert len(hd.check_timezone(wd, "XAUUSD", bounds, rule=DAILY)["large_reversals"]) == 1
    assert hd._big_drops(np.array([_ms("2021-10-13 10:30"), _ms("2021-10-13 10:00:00.001")])).tolist() == []
    # Uebergrosse Falte (letzter Tick vor dem Sprung ist 19:58 nach 19:00-Sprung) -> 19:58 nicht 1 h verschoben
    with pytest.raises(hd.TimezoneError):
        _fold_conv([S21 + "19:50:00", S21 + "19:00:00", S21 + "19:30:00", S21 + "19:58:00", S21 + "19:40:00"])


def test_review_v7_manual_fold_resolution_in_conversion():
    ts = [S21 + t for t in ("18:59:00", "19:10:00", "19:20:00", "19:00:00", "19:15:00", "20:05:00")]
    with pytest.raises(hd.TimezoneError) as e:
        _fold_conv(ts)                                     # Sprung nur 20 min: unaufgeloest
    fu = e.value.fold_unresolved
    assert fu == {"date": "2021-10-31", "ticks": 4, "first_raw": "2021-10-31 19:00:00.000",
                  "last_raw": "2021-10-31 19:20:00.000"}
    res = {**fu, "side": "split-at", "split_at_raw": "2021-10-31 19:00:00.000"}
    got = [hd._raw_iso(int(x)) for x in _fold_conv(ts, fold_resolution=res)]
    assert got == ["2021-10-31 22:59:00", "2021-10-31 23:10:00", "2021-10-31 23:20:00",
                   "2021-11-01 00:00:00", "2021-11-01 00:15:00", "2021-11-01 01:05:00"]
    for bad in ({**res, "ticks": 5}, {**res, "split_at_raw": "2021-10-31 19:05:00.000"}):
        with pytest.raises(hd.TimezoneError, match="passt nicht"):
            _fold_conv(ts, fold_resolution=bad)
    with pytest.raises(hd.TimezoneError, match="passt nicht"):    # pre: nicht monoton
        _fold_conv(ts, fold_resolution={**fu, "side": "pre"})


def _oct_without_second_pass():
    """Oktober 2024 nach der Tagesregel, aber ohne die zweite Runde der doppelten Stunde
    (keine Ticks Mo 00:00-01:00 UTC): kein Ruecksprung, Seite nicht erkennbar."""
    oct_ = raw_month_ticks("XAUUSD", 2024, 10)
    gap = (oct_["utc"] >= pd.Timestamp("2024-10-28 00:00", tz="UTC")) & \
        (oct_["utc"] < pd.Timestamp("2024-10-28 01:00", tz="UTC"))
    return oct_[~gap.to_numpy()].reset_index(drop=True)


def test_review_v7_resolve_fold_cli(tmp_path, clock, v7_default, monkeypatch, capsys):
    """Item 2: unaufgeloeste Falte -> Marker mit fold_unresolved; nur der Mensch loest auf
    (--resolve-fold ... --side pre), dann geschrieben; geaenderte Daten -> ungueltig."""
    oct_ = _oct_without_second_pass()
    files = {("T", "202409"): tick_zip("XAUUSD", 2024, 9), ("T", "202410"): _zip_from(oct_, "XAUUSD", 2024, 10),
             ("T", "202411"): tick_zip("XAUUSD", 2024, 11)}
    site = Site(clock, files)
    now = datetime(2025, 1, 15, tzinfo=UTC)
    rep = run(tmp_path, clock, site, start=(2024, 10), end=(2024, 11), now=now)
    assert rep["exit_code"] == 4 and "--resolve-fold 2024-10" in rep["quarantine"][0]["reason"]
    q = hd.Unit("XAUUSD", "tick", 2024, 10).qmarker(tmp_path, (2024, 10))
    info = json.loads(q.read_text())
    assert info["fold_unresolved"] == {"date": "2024-10-27", "ticks": 60,
                                       "first_raw": "2024-10-27 19:00:00.137",
                                       "last_raw": "2024-10-27 19:59:00.137"}
    assert "releasable_weekend" not in info
    monkeypatch.setenv("MARKETDATA_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(hd.requests, "Session", lambda: pytest.fail("kein Download"))
    # falsche Aufrufe: Exit 2, Marker unveraendert
    assert hd.main(["--symbol", "XAUUSD", "--resolve-fold", "2024-10", "--side", "split-at"]) == 2
    assert hd.main(["--symbol", "XAUUSD", "--resolve-fold", "2024-10", "--side", "split-at",
                    "2024-10-27", "19:30:00"]) == 2                          # kein Tick dort
    assert hd.main(["--symbol", "XAUUSD", "--resolve-fold", "2024-10", "--side", "middle"]) == 2
    assert hd.main(["--symbol", "XAUUSD", "--resolve-fold", "2024-09", "--side", "pre"]) == 2    # kein Marker
    assert "fold_resolution" not in json.loads(q.read_text())
    capsys.readouterr()
    assert hd.main(["--symbol", "XAUUSD", "--resolve-fold", "2024-10", "--side", "pre"]) == 0
    out = capsys.readouterr().out
    assert "roh 2024-10-27 19:59:00.137 -> UTC 2024-10-27 23:59:00.137" in out
    info = json.loads(q.read_text())
    assert info["fold_resolution"]["side"] == "pre" and info["accepted"] is False
    n = site.requests
    rep = run(tmp_path, clock, site, start=(2024, 10), end=(2024, 11), now=now)
    assert rep["exit_code"] == 0 and rep["summary"]["months_written"] == ["2024-10", "2024-11"], \
        rep["quarantined"]
    assert site.requests == n and q.exists()                              # Entscheidung bleibt
    df = pd.read_parquet(hd.store_path(tmp_path, "XAUUSD", 2024, 10))
    u = pd.concat([raw_month_ticks("XAUUSD", 2024, 9)["utc"], oct_["utc"]])
    u = u[(u >= pd.Timestamp("2024-10-01", tz="UTC")) & (u < pd.Timestamp("2024-11-01", tz="UTC"))]
    assert list(df["ts"]) == sorted(set(u.dt.floor("1min"))) and int(df["tick_count"].sum()) == len(u)
    tz = json.loads(hd.report_path(tmp_path, "XAUUSD", 2024, 10).read_text())["raw_months"]["2024-10"]["timezone"]
    assert tz["fold_resolved_manually"]["side"] == "pre"
    night = [t for t in tz["transition_nights"] if t["rule_transition"]][0]
    assert night["status"] == "ok" and night["resolved_manually"] is True
    # Daten aendern sich (ein Tick mehr in der Stunde): Aufloesung gilt nicht mehr
    changed = _with_ticks(oct_, ["2024-10-27 19:59:30"])
    hd.Unit("XAUUSD", "tick", 2024, 10).path(tmp_path).write_bytes(_zip_from(changed, "XAUUSD", 2024, 10))
    hd.store_path(tmp_path, "XAUUSD", 2024, 10).unlink()
    rep = run(tmp_path, clock, Site(clock), start=(2024, 10), now=now)
    assert rep["exit_code"] == 4 and "passt nicht" in rep["quarantine"][0]["reason"]
    assert not hd.store_path(tmp_path, "XAUUSD", 2024, 10).exists()
    assert json.loads(q.read_text())["fold_resolution"]["side"] == "pre"      # Marker bleibt


def test_review_v7_undecided_with_daily_candidate_skips_expected_fold():
    """Item 6: "undecided" mit der Tagesregel als (nicht erstem) Kandidaten: der erwartete
    Herbstsprung wird nicht als verwerfbarer Ruecksprung gezaehlt."""
    und = hd.UndecidedRule(candidates=(hd.TZ_RULES["ny_local"], DAILY))
    raw = _raw_ms(raw_month_ticks("XAUUSD", 2024, 10, stamping="daily"))
    drop, rel = hd.weekend_release(raw, "tick", und)
    assert not drop.any() and rel is None
    assert len(hd.expected_fold_jumps(raw, "tick", und)) == 1
    rep = hd.check_timezone(raw, "XAUUSD", hd.raw_month_bounds_ms((2024, 10)), rule=und)
    assert rep["large_reversals"] == [] and len(rep["expected_reversals"]) == 1


# ================================ v8: Historienbereich, vorlaeufige Quarantaene ---
def test_v8_range_config_defaults_and_validation(tmp_path, v8_range, monkeypatch):
    rc = hd.load_range_config()
    assert rc.start == {"XAUUSD": (2009, 6), "XAGUSD": (2009, 6)} and rc.end == (2026, 9)
    assert (rc.window_days, rc.recheck_days, rc.max_rechecks) == (60, 30, 3)
    assert hd.HISTORY_START["XAUUSD"] == hd.history_start("XAGUSD") == (2009, 6)
    assert hd.history_end() == (2026, 9) and min(hd.HISTORY_START.values()) == (2009, 6)
    good = hd.DEFAULT_RANGE_CONFIG.read_text()
    for bad, word in ((good.replace('end: "2026-09"', 'end: "2026-13"'), "YYYY-MM"),
                      (good.replace('end: "2026-09"', 'end: "2008-12"'), "vor dem Historienbeginn"),
                      (good.replace("max_rechecks: 3", "max_rechecks: 0"), "max_rechecks"),
                      (good.replace("recheck_days: 30", "recheck_days: x"), "recheck_days"),
                      (good.replace("  window_days: 60", "  window_days: 60\n  extra: 1"), "provisional"),
                      (good.replace(", XAGUSD: \"2009-06\"", ""), "Symbole")):
        p = tmp_path / f"bad{len(word)}.yaml"
        p.write_text(bad)
        with pytest.raises(ValueError, match=word):
            hd.load_range_config(p)
    monkeypatch.setenv(hd.RANGE_CONFIG_ENV, str(tmp_path / "fehlt.yaml"))
    with pytest.raises(ValueError, match="fehlt"):
        hd.load_range_config()


def test_v8_months_before_start_never_loaded_and_first_month_partial(tmp_path, clock, v8_range, caplog):
    """--start 2009-03 (HistData-Liste) wird auf den konfigurierten Beginn 2009-06
    hochgesetzt: 2009-03..05 ohne Request, nicht geschrieben, kein Exit 2/3/4; UTC 2009-06
    nur aus Rohmonaten >= 2009-06 (partial_start); ms_zero_share im Monatsreport."""
    site = Site(clock, {("T", f"2009{m:02d}"): tick_zip("XAUUSD", 2009, m) for m in range(3, 9)})
    with caplog.at_level("WARNING", logger="histdata"):
        rep = run(tmp_path, clock, site, start=(2009, 3), end=(2009, 7))
    assert rep["exit_code"] == 0 and rep["status"] == "complete", rep["quarantined"]
    assert rep["summary"]["months_written"] == ["2009-06", "2009-07"]
    assert rep["clamped_before_start"] == ["2009-03", "2009-04", "2009-05"]
    un = rep["summary"]["months_unavailable"]
    assert set(un) == {"2009-03", "2009-04", "2009-05"} and all("konfiguriertem" in v for v in un.values())
    asked = [u for verb, u, _ in site.calls if verb == "GET"]
    assert asked and not any(re.search(r"/2009/[345]$", u) for u in asked), asked
    assert site.requests == 4                                   # nur Rohmonate 2009-06, 2009-07
    assert any("vor dem konfigurierten Historienbeginn" in r.getMessage() for r in caplog.records)
    jun = json.loads(hd.report_path(tmp_path, "XAUUSD", 2009, 6).read_text())
    jul = json.loads(hd.report_path(tmp_path, "XAUUSD", 2009, 7).read_text())
    assert jun["partial_start"] is True and jul["partial_start"] is False
    assert list(jun["raw_months"]) == ["2009-06"]               # Rohmonat 2009-05 nie gelesen
    assert jun["ms_zero_share"] == 0.0 and jun["raw_months"]["2009-06"]["ms_zero_share"] == 0.0
    df = pd.read_parquet(hd.store_path(tmp_path, "XAUUSD", 2009, 6))
    assert df["ts"].min() > pd.Timestamp("2009-06-01 00:00", tz="UTC")     # erste Stunden fehlen
    assert not hd.Unit("XAUUSD", "tick", 2009, 5).path(tmp_path).exists()


def test_v8_store_files_before_start_are_retired(tmp_path, clock, v8_range):
    site = Site(clock, {("T", f"2009{m:02d}"): tick_zip("XAUUSD", 2009, m) for m in (6, 7)})
    assert run(tmp_path, clock, site, start=(2009, 6))["exit_code"] == 0
    old = hd.store_path(tmp_path, "XAUUSD", 2009, 5)
    old.parent.mkdir(parents=True, exist_ok=True)
    old.write_bytes(hd.store_path(tmp_path, "XAUUSD", 2009, 6).read_bytes())
    rep = run(tmp_path, clock, site, start=(2009, 6))
    assert not old.exists() and list(rep["retired_before_start"]) == ["2009-05"]
    assert any("2009-05" in n for n in _stale_files(tmp_path))
    assert hd.store_path(tmp_path, "XAUUSD", 2009, 6).exists()


def test_v8_cli_history_uses_configured_range(monkeypatch, v8_range, tmp_path):
    seen = {}

    def fake_run(sym, start, end, *a, **kw):
        seen.update(sym=sym, start=start, end=end, history=kw.get("history"))
        return {"exit_code": 0}
    monkeypatch.setattr(hd, "run", fake_run)
    assert hd.main(["--symbol", "XAGUSD", "--history"]) == 0
    assert seen == {"sym": "XAGUSD", "start": date(2009, 6, 1), "end": date(2026, 9, 1), "history": True}
    for extra in (["--start", "2010-01"], ["--end", "2026-10"]):
        with pytest.raises(SystemExit):
            hd.main(["--symbol", "XAGUSD", "--history", *extra])
    cfg = tmp_path / "r.yaml"
    cfg.write_text(hd.DEFAULT_RANGE_CONFIG.read_text().replace('end: "2026-09"', 'end: "2026-12"'))
    monkeypatch.setenv(hd.RANGE_CONFIG_ENV, str(cfg))
    assert hd.main(["--symbol", "XAUUSD", "--history"]) == 0 and seen["end"] == date(2026, 12, 1)
    assert hd.main(["--symbol", "XAUUSD", "--start", "2009-01", "--end", "2009-08"]) == 0
    assert seen["start"] == date(2009, 1, 1) and seen["history"] is False      # run setzt hoch


NOW8 = datetime(2026, 11, 8, 12, tzinfo=UTC)


def _oct26(fixed=False):
    """Rohmonat 2026-10 nach der Tagesregel; fixed=False: ohne zweite Runde der doppelten
    Stunde (Mo 26.10. 00:00-01:00 UTC) und mit .000 ms - wie die vorlaeufige Datei."""
    oct_ = raw_month_ticks("XAUUSD", 2026, 10)
    if fixed:
        return oct_
    gap = (oct_["utc"] >= pd.Timestamp("2026-10-26 00:00", tz="UTC")) & \
        (oct_["utc"] < pd.Timestamp("2026-10-26 01:00", tz="UTC"))
    oct_ = oct_[~gap.to_numpy()].reset_index(drop=True)
    return oct_.assign(raw=oct_["raw"].dt.floor("1s"))


def _site26(clock, fixed=False):
    files = {("T", f"2026{m:02d}"): tick_zip("XAUUSD", 2026, m) for m in (8, 9)}
    files[("T", "202610")] = _zip_from(_oct26(fixed), "XAUUSD", 2026, 10)
    return Site(clock, files)


def test_v8_provisional_quarantine_exit_0_then_rechecks(tmp_path, clock, v8_range, v7_default, caplog):
    """metals-fetch-artiger Lauf 2026-09..2026-10, 2026-10 mit unaufgeloester doppelter
    Stunde: vorlaeufig (kein Exit 4/3), months_provisional, ms_zero_share 1.0. Nach 30 Tagen
    ZIP neu laden: faellt durch -> rechecks + 1; nach 3 Fehlschlaegen dauerhaft (Exit 4)."""
    q = hd.Unit("XAUUSD", "tick", 2026, 10).qmarker(tmp_path, (2026, 10))
    zp = hd.Unit("XAUUSD", "tick", 2026, 10).path(tmp_path)
    site = _site26(clock)
    with caplog.at_level("WARNING", logger="histdata"):
        rep = run(tmp_path, clock, site, start=(2026, 9), end=(2026, 10), now=NOW8)
    assert rep["exit_code"] == 0 and rep["status"] == "provisional_pending", rep["quarantined"]
    assert rep["summary"]["months_written"] == ["2026-09"] and rep["quarantined"] == []
    prov = rep["months_provisional"]["2026-10"]
    assert prov["status"] == "provisional_pending" and prov["recheck_after"].startswith("2026-12-08")
    assert prov["rechecks"] == 0 and rep["summary"]["months_provisional"] == rep["months_provisional"]
    assert any("VORLAEUFIGE QUARANTAENE" in r.getMessage() and r.levelname == "WARNING"
               for r in caplog.records)
    info = json.loads(q.read_text())
    assert info["provisional"]["rechecks"] == 0 and info["ms_zero_share"] == 1.0
    assert info["fold_unresolved"]["date"] == "2026-10-25"
    # noch nicht faellig: keine Requests, weiter Exit 0
    n = site.requests
    rep = run(tmp_path, clock, site, start=(2026, 9), end=(2026, 10), now=NOW8 + timedelta(days=10))
    assert rep["exit_code"] == 0 and site.requests == n and rep["provisional_rechecked"] == []
    # faellig, HistData liefert dieselbe Datei: 3 Neupruefungen, danach dauerhaft
    t = NOW8
    for k in (1, 2, 3):
        t += timedelta(days=31)
        n = site.requests
        rep = run(tmp_path, clock, site, start=(2026, 9), end=(2026, 10), now=t)
        assert site.requests - n == 2, k                             # nur Rohmonat 2026-10 neu
        assert zp.with_name(zp.name + ".superseded").exists() and zp.exists()
        info = json.loads(q.read_text())
        assert rep["provisional_rechecked"][0]["raw_month"] == "2026-10"
        if k < 3:
            assert rep["exit_code"] == 0 and info["provisional"]["rechecks"] == k
            assert info["provisional"]["recheck_after"].startswith(f"{(t + timedelta(days=30)):%Y-%m-%d}")
            assert rep["provisional_rechecked"][0]["result"] == "still_provisional"
        else:
            assert rep["exit_code"] == 4 and "provisional" not in info
            assert info["provisional_expired"]["rechecks"] == 3
            assert rep["provisional_rechecked"][0]["result"] == "permanent_quarantine"
            assert [x["month"] for x in rep["quarantined"]] == ["2026-10"]
    n = site.requests
    rep = run(tmp_path, clock, site, start=(2026, 9), end=(2026, 10), now=t + timedelta(days=40))
    assert rep["exit_code"] == 4 and site.requests == n                   # dauerhaft, kein Neuladen


def test_v8_provisional_recheck_passes_with_corrected_file(tmp_path, clock, v8_range, v7_default):
    site = _site26(clock)
    assert run(tmp_path, clock, site, start=(2026, 9), end=(2026, 10), now=NOW8)["exit_code"] == 0
    site.files[("T", "202610")] = _zip_from(_oct26(fixed=True), "XAUUSD", 2026, 10)
    n = site.requests
    rep = run(tmp_path, clock, site, start=(2026, 9), end=(2026, 9), now=NOW8 + timedelta(days=31))
    assert site.requests - n == 2 and rep["exit_code"] == 0, rep["quarantined"]
    assert "2026-10" in rep["summary"]["months_written"]          # ausserhalb des Bereichs nachgeholt
    assert rep["provisional_rechecked"][0]["result"] == "passed"
    assert not hd.Unit("XAUUSD", "tick", 2026, 10).qmarker(tmp_path, (2026, 10)).exists()
    assert rep["months_provisional"] == {}
    mrep = json.loads(hd.report_path(tmp_path, "XAUUSD", 2026, 10).read_text())
    assert mrep["ms_zero_share"] == 0.0


def test_v8_history_run_does_not_touch_after_end(tmp_path, clock, v8_range, v7_default):
    site = _site26(clock)
    assert run(tmp_path, clock, site, start=(2026, 9), end=(2026, 10), now=NOW8)["exit_code"] == 0
    n = site.requests
    rep = hd.run("XAUUSD", date(2026, 9, 1), date(2026, 9, 1), tmp_path, now=at(NOW8 + timedelta(days=31)),
                 history=True, **kw(clock, site))
    assert rep["exit_code"] == 0 and site.requests == n and rep["provisional_rechecked"] == []
    assert json.loads(hd.Unit("XAUUSD", "tick", 2026, 10).qmarker(tmp_path, (2026, 10)).read_text()
                      )["provisional"]["rechecks"] == 0


def test_v8_old_month_quarantine_is_not_provisional(tmp_path, clock, v8_range, v7_default):
    """Rohmonat lange vorbei: echte Quarantaene, Exit 4 wie bisher."""
    oct_ = _oct26()
    files = {("T", "202609"): tick_zip("XAUUSD", 2026, 9), ("T", "202610"): _zip_from(oct_, "XAUUSD", 2026, 10)}
    rep = run(tmp_path, clock, Site(clock, files), start=(2026, 10), end=(2026, 10),
              now=NOW8 + timedelta(days=90))
    assert rep["exit_code"] == 4 and rep["months_provisional"] == {}
    info = json.loads(hd.Unit("XAUUSD", "tick", 2026, 10).qmarker(tmp_path, (2026, 10)).read_text())
    assert "provisional" not in info
