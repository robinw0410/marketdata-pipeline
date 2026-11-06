"""
Regressionstests Zeitzone HistData (fetch_histdata.py).

Befund (Homelab, echte Ticks aller drei Symbole): Tick-Zeitstempel =
Europe/Berlin - 6 h (Regel berlin_minus_6): UTC-5 im EU-Winter, UTC-4 in der
EU-Sommerzeit. 2024-01 -5 h, 2024-07 -4 h (Tagespause roh 16:59:58 ->
18:00:01); 2024-03 bleibt nach der US-Umstellung (10.03.) bis 31.03. bei -5 h
(Tagespause roh 15:59:59 -> 17:00:00), 2024-10 ab 27.10. schon -5 h.
Marktzeiten bleiben New York. M1 bleibt (unverifiziert) fest EST = Etc/GMT+5.

Erwartete Werte werden hier unabhaengig mit zoneinfo berechnet, nicht mit
Helfern aus fetch_histdata. Nur synthetische Daten, HTTP gemockt.
"""
import functools
import io
import json
import re
import zipfile
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import fetch_histdata as fh

UTC = timezone.utc
NY = ZoneInfo("America/New_York")
BERLIN = ZoneInfo("Europe/Berlin")
SYM = "XAUUSD"
OLD_TZ_META = b"EST fixed UTC-5, converted to UTC, checked per month"
NOW = datetime(2025, 3, 1, 12, tzinfo=UTC)


BERLIN_EVERYWHERE = """\
rules: []
schedule:
  tick:
    - {from: "2000-01", rule: berlin_minus_6}
  m1:
    - {from: "2000-01", rule: fixed_est}
"""


@pytest.fixture
def berlin_schedule(tmp_path_factory, monkeypatch):
    """Fahrplan wie v5 (berlin_minus_6 fuer ALLE Rohmonate): fuer Logiktests, die
    ueber viele Jahre mit einer einzigen Regel rechnen (der vorlaeufige v6-
    Fahrplan hat ny_local bis 2018-11 und "undecided" 2018-12..2021-02)."""
    p = tmp_path_factory.mktemp("cfg") / "berlin_everywhere.yaml"
    p.write_text(BERLIN_EVERYWHERE)
    monkeypatch.setenv(fh.TZ_CONFIG_ENV, str(p))
    return p


# ------------------------------------------------------------ synthetik ---
def _ms(*args) -> int:
    """Rohzeit-ms: Wanduhr-Ziffern als UTC gelesen (wie parse_csv)."""
    return int(datetime(*args, tzinfo=UTC).timestamp() * 1000)


def _ny_to_utc(*args) -> datetime:
    """Unabhaengige Referenz: NY-Ortszeit -> UTC via zoneinfo."""
    return datetime(*args, tzinfo=NY).astimezone(UTC)


def _raw_to_utc_ref(*args) -> datetime:
    """Unabhaengige Referenz berlin_minus_6: Rohzeit + 6 h ist Berliner Ortszeit."""
    local = datetime(*args) + timedelta(hours=6)
    return local.replace(tzinfo=BERLIN).astimezone(UTC)


BERLIN6 = fh.TZ_RULES["berlin_minus_6"]
FIXED_EST = fh.TZ_RULES["fixed_est"]


@functools.lru_cache(maxsize=None)
def raw_month(y: int, m: int, stamping: str = "berlin", step_s: int = 30) -> pd.DataFrame:
    """
    Ticks eines ROH-Monats wie beobachtet, in New-York-Marktzeiten:
      So 18:00:01 .. Fr 16:59:58, Mo-Do Tagespause 16:59:58 -> 18:00:01.
    Regulaere Ticks alle step_s Sekunden auf Sekunde :01 (und :31), dazu je
    Handelstag Mo-Fr ein letzter Tick 16:59:58.
    stamping: berlin (berlin_minus_6, richtig), ny (NY-Ortszeit, US-Regel),
    est (fest UTC-5), utc.
    Rueckgabe (nicht veraendern - gecached): raw (naiv), utc, bid, ask.
    """
    lo = pd.Timestamp(y, m, 1) - pd.Timedelta(days=3)
    hi = pd.Timestamp(y, m, 1) + pd.offsets.MonthBegin(1) + pd.Timedelta(days=3)
    grid = pd.date_range(lo + pd.Timedelta(seconds=1), hi, freq=f"{step_s}s", inclusive="left")
    wd, h = grid.dayofweek, grid.hour
    is_open = (((wd <= 3) & ((h < 17) | (h >= 18))) | ((wd == 4) & (h < 17))
               | ((wd == 6) & (h >= 18)))
    days = pd.date_range(lo.normalize(), hi, freq="D")
    closes = days[days.dayofweek <= 4] + pd.Timedelta(hours=16, minutes=59, seconds=58)
    local = grid[np.asarray(is_open)].append(closes).sort_values()
    local = local[(local >= lo) & (local < hi)]
    # Wochenende deckt 02:00-02:59 (Fruehjahr) und 01:00-01:59 (Herbst) ab -> eindeutig
    utc = local.tz_localize("America/New_York").tz_convert("UTC")
    if stamping == "berlin":
        raw = utc.tz_convert(BERLIN).tz_localize(None) - pd.Timedelta(hours=6)
    elif stamping == "ny":
        raw = local
    elif stamping == "est":
        raw = (utc - pd.Timedelta(hours=5)).tz_localize(None)
    else:
        assert stamping == "utc"
        raw = utc.tz_localize(None)
    keep = (raw >= pd.Timestamp(y, m, 1)) & (raw < pd.Timestamp(y, m, 1) + pd.offsets.MonthBegin(1))
    raw, utc = raw[keep], utc[keep]
    sec = (utc.as_unit("ms").asi8 // 1000).astype(float)
    bid = np.round(2050.0 + 6.0 * np.sin(sec / 7200.0) + 0.4 * np.sin(sec / 53.0), 2)
    spread = 0.25 + 0.05 * ((sec // 30) % 3)
    ask = np.round(bid + spread, 2)
    return pd.DataFrame({"raw": raw, "utc": utc, "bid": bid, "ask": ask}).reset_index(drop=True)


def raw_ms_of(df: pd.DataFrame) -> np.ndarray:
    return pd.DatetimeIndex(df["raw"]).as_unit("ms").asi8.astype(np.int64)


def tick_csv(df: pd.DataFrame) -> bytes:
    stamp = pd.DatetimeIndex(df["raw"]).strftime("%Y%m%d %H%M%S%f").str[:-3]
    lines = [f"{s},{b:.2f},{a:.2f},0" for s, b, a in zip(stamp, df["bid"], df["ask"])]
    return ("\n".join(lines) + "\n").encode()


def make_zip(y: int, m: int, csv: bytes) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(f"DAT_ASCII_{SYM}_T_{y}{m:02d}.csv", csv)
        zf.writestr(f"DAT_ASCII_{SYM}_T_{y}{m:02d}.txt", "HistData.com status report\n")
    return buf.getvalue()


@functools.lru_cache(maxsize=None)
def tick_zip(y: int, m: int) -> bytes:
    return make_zip(y, m, tick_csv(raw_month(y, m)))


# ------------------------------------------------------------ http-mock ---
PAGE = """<html><body>
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
    def __init__(self, t=1_700_000_000.0):
        self.t = t

    def __call__(self):
        return self.t

    def sleep(self, s):
        self.t += s


class Site:
    """Fake-HistData: files[(code, period)] -> ZIP; unbekannt = leere Antwort."""

    def __init__(self, clock, files=None):
        self.clock, self.files, self.calls = clock, dict(files or {}), []

    def get(self, url, timeout=None, **kw):
        self.calls.append(("GET", url))
        m = re.search(r"/ascii/([a-z0-9-]+)/([a-z]+)/(\d{4})(?:/(\d{1,2}))?$", url)
        tf, pair, year, month = m.groups()
        code = "T" if tf == "tick-data-quotes" else "M1"
        period = f"{year}{int(month):02d}" if month else year
        html = PAGE.format(period=period, year=year, datemonth=period if month else "",
                           code=code, pair=pair.upper())
        return Resp(200, html.encode())

    def post(self, url, data=None, headers=None, timeout=None, **kw):
        self.calls.append(("POST", url))
        period = data["datemonth"] or data["date"]
        return Resp(200, self.files.get((data["timeframe"], period), b""))

    @property
    def requests(self):
        return len(self.calls)


def site_for(clock, months, overrides=None):
    files = {("T", f"{y}{m:02d}"): tick_zip(y, m) for y, m in months}
    files.update(overrides or {})
    return Site(clock, files)


def do_run(root, site, clock, start, end=None, now=NOW, **extra):
    return fh.run(SYM, date(*start, 1), date(*(end or start), 1), root, mode="tick",
                  now=lambda: now, pause=0.0, attempts=2, backoff_base=1.0, max_backoff=10.0,
                  jitter=0.0, sleep=clock.sleep, clock=clock, wall=clock, session=site, **extra)


# --------------------------------------------------------------- helfer ---
def _offset_set(x) -> set[int]:
    """expected_offsets als Menge ganzer Stunden (dict {offset: n} oder Liste)."""
    if isinstance(x, dict):
        return {int(float(k)) for k in x}
    return {int(v) for v in x}


def _rebuilt(rep: dict):
    v = rep.get("rebuilt_timezone")
    if v is None:
        v = (rep.get("summary") or {}).get("rebuilt_timezone")
    return v


def _months(v) -> set[str]:
    """Monate aus einer Report-Liste (Strings 'YYYY-MM...' oder dicts mit 'month')."""
    out = set()
    for item in (v or []):
        if isinstance(item, dict):
            item = item.get("month") or json.dumps(item)
        out.add(str(item)[:7])
    return out


def _meta_value(v) -> bytes:
    return v.encode() if isinstance(v, str) else v


def _check(ms, ym, mode):
    return fh.check_timezone(ms, SYM, fh.raw_month_bounds_ms(ym), mode=mode)


# ======================================================= neue Schnittstelle ---
def test_rule_constants_and_error_type():
    assert fh.TZ_RULES["berlin_minus_6"] == fh.TzRule("berlin_minus_6", "Europe/Berlin", -6)
    assert fh.TZ_RULES["ny_local"] == fh.TzRule("ny_local", "America/New_York", 0)
    assert fh.TZ_RULES["fixed_est"] == fh.TzRule("fixed_est", "Etc/GMT+5", 0)
    assert fh.rule_for("tick", (2024, 3)) == BERLIN6 and fh.rule_for("m1", (2024, 3)) == FIXED_EST
    assert issubclass(fh.TimezoneError, fh.FormatError)
    assert set(fh.SOURCE_TZ_META) >= {"tick", "m1"}
    tick_meta = _meta_value(fh.SOURCE_TZ_META["tick"])
    assert tick_meta != OLD_TZ_META               # sonst erkennt der Lauf alte Dateien nicht
    assert tick_meta != _meta_value(fh.SOURCE_TZ_META["m1"])
    assert b"berlin_minus_6" in tick_meta and b"Europe/Berlin" in tick_meta
    assert f"v{fh.TZ_CHECK_VERSION}".encode() in tick_meta and fh.TZ_CHECK_VERSION == 6


# =========================================== 1. Anker wie beobachtet: ok ---
@pytest.mark.parametrize("ym,offset", [((2024, 1), -5), ((2024, 7), -4)])
def test_observed_anchors_pass_in_tick_mode(ym, offset):
    df = raw_month(*ym)
    raw = pd.DatetimeIndex(df["raw"])
    # Generator-Sanity: Tagespause / Wochenrand exakt wie beobachtet
    fri = raw[raw.dayofweek == 4]
    assert set(fri[fri.hour >= 16].time.astype(str)) >= {"16:59:58"}
    assert not ((raw.dayofweek <= 3) & (raw.hour == 17)).any()
    sun = raw[raw.dayofweek == 6]
    assert sun.min().time() == datetime(2000, 1, 1, 18, 0, 1).time()

    rep = _check(raw_ms_of(df), ym, "tick")
    for k in ("status", "ok", "reason", "anchors", "rule", "expected_offsets", "deviations",
              "median_deviation_h", "median_offset_h", "edge_violations"):
        assert k in rep, k
    assert rep["status"] == "ok" and rep["ok"] is True, rep["reason"]
    assert rep["rule"] == "berlin_minus_6"
    assert rep["anchors"] >= 20
    assert rep["median_deviation_h"] == 0
    assert rep["median_offset_h"] == offset
    assert _offset_set(rep["expected_offsets"]) == {offset}
    assert not rep["edge_violations"]


def test_homelab_march_2024_gap_weeks_measured():
    """Wie im Homelab gemessen: nach der US-Umstellung (10.3.) bleibt die Rohzeit
    bis 31.3. bei -5 h - Tagespause roh 15:59:xx -> 17:00:xx. berlin_minus_6
    besteht, ny_local (US-Regel) faellt mit bestaetigtem Wechsel durch."""
    df = raw_month(2024, 3)
    raw = pd.DatetimeIndex(df["raw"])
    tue = raw[(raw >= pd.Timestamp("2024-03-12 15:00")) & (raw < pd.Timestamp("2024-03-12 18:00"))]
    gap = np.diff(tue.asi8).argmax()
    assert (str(tue[gap].time()), str(tue[gap + 1].time())) == ("15:59:58", "17:00:01")
    ok = _check(raw_ms_of(df), (2024, 3), "tick")
    assert ok["ok"] and ok["rule"] == "berlin_minus_6", ok["reason"]
    bad = fh.check_timezone(raw_ms_of(df), SYM, fh.raw_month_bounds_ms((2024, 3)), "tick",
                            rule=fh.TZ_RULES["ny_local"])
    assert not bad["ok"] and bad["switch_runs"]
    assert {e["deviation_h"] for r in bad["switch_runs"] for e in r} == {-1}
    # Oktober: ab 27.10. (EU) schon -5 h
    oct_ = raw_month(2024, 10)
    raw = pd.DatetimeIndex(oct_["raw"])
    tue = raw[(raw >= pd.Timestamp("2024-10-29 15:00")) & (raw < pd.Timestamp("2024-10-29 18:00"))]
    gap = np.diff(tue.asi8).argmax()
    assert (str(tue[gap].time()), str(tue[gap + 1].time())) == ("15:59:58", "17:00:01")
    assert _check(raw_ms_of(oct_), (2024, 10), "tick")["ok"]
    assert not fh.check_timezone(raw_ms_of(oct_), SYM, fh.raw_month_bounds_ms((2024, 10)), "tick",
                                 rule=fh.TZ_RULES["ny_local"])["ok"]


@pytest.mark.parametrize("ym,offsets", [((2024, 3), {-5, -4}), ((2024, 10), {-4, -5})])
def test_dst_switch_inside_month_passes_in_tick_mode(ym, offsets):
    """EU-Umstellung im Rohmonat (31.3. bzw. 27.10.): erwarteter Offset je Anker
    -> keine Abweichung, kein Randverstoss."""
    rep = _check(raw_ms_of(raw_month(*ym)), ym, "tick")
    assert rep["ok"], rep["reason"]
    assert rep["median_deviation_h"] == 0 and not rep["edge_violations"]
    assert _offset_set(rep["expected_offsets"]) == offsets


# ===================================== 2. falsche Hypothesen schlagen fehl ---
def test_july_tick_data_fails_under_m1_hypothesis():
    rep = _check(raw_ms_of(raw_month(2024, 7)), (2024, 7), "m1")
    assert rep["status"] == "failed" and rep["ok"] is False
    assert rep["rule"] == "fixed_est"
    assert _offset_set(rep["expected_offsets"]) == {-5}
    assert rep["median_deviation_h"] == 1 and rep["median_offset_h"] == -4
    assert _offset_set(rep["deviations"]) == {1}          # +1 an jedem Anker


def test_january_tick_data_passes_under_m1_hypothesis():
    """Im Winter sind Berlin - 6 h und fest EST identisch."""
    rep = _check(raw_ms_of(raw_month(2024, 1)), (2024, 1), "m1")
    assert rep["ok"] and rep["median_deviation_h"] == 0


def test_july_fixed_est_stamped_fails_in_tick_mode():
    df = raw_month(2024, 7, stamping="est")
    raw = pd.DatetimeIndex(df["raw"])
    tue = raw[(raw >= pd.Timestamp("2024-07-09 15:00")) & (raw < pd.Timestamp("2024-07-09 18:00"))]
    gap = np.diff(tue.asi8).argmax()
    assert (str(tue[gap].time()), str(tue[gap + 1].time())) == ("15:59:58", "17:00:01")
    rep = _check(raw_ms_of(df), (2024, 7), "tick")
    assert rep["status"] == "failed" and not rep["ok"]
    assert rep["median_deviation_h"] == -1 and rep["median_offset_h"] == -5


@pytest.mark.parametrize("ym,dev", [((2024, 1), 5), ((2024, 7), 4)])
def test_utc_stamped_fails_in_tick_mode(ym, dev):
    rep = _check(raw_ms_of(raw_month(*ym, stamping="utc")), ym, "tick")
    assert rep["status"] == "failed" and not rep["ok"]
    assert rep["median_deviation_h"] == dev and rep["median_offset_h"] == 0


# ============================================= 3. Umrechnung Rohzeit->UTC ---
TICK_CASES = [
    ((2024, 1, 12, 16, 59, 58), (2024, 1, 12, 21, 59, 58)),
    ((2024, 7, 12, 16, 59, 58), (2024, 7, 12, 20, 59, 58)),
    ((2024, 3, 10, 18, 0, 1), (2024, 3, 10, 23, 0, 1)),     # So nach US-Umstellung: noch -5 h
    ((2024, 3, 12, 15, 59, 59), (2024, 3, 12, 20, 59, 59)),  # gemessene Pause 15:59:59 = 16:59:59 EDT
    ((2024, 3, 31, 18, 0, 1), (2024, 3, 31, 22, 0, 1)),     # erster So-Start nach EU-Umstellung
    ((2024, 10, 28, 15, 59, 58), (2024, 10, 28, 20, 59, 58)),  # nach EU-Rueckstellung, NY noch EDT
    ((2024, 11, 3, 18, 0, 1), (2024, 11, 3, 23, 0, 1)),
]


@pytest.mark.parametrize("raw,utc", TICK_CASES)
def test_raw_to_utc_tick_values(raw, utc):
    assert _raw_to_utc_ref(*raw) == datetime(*utc, tzinfo=UTC)        # Referenz stimmt
    out = fh.raw_to_utc_ms(np.array([_ms(*raw)], dtype=np.int64), BERLIN6)
    assert np.asarray(out).dtype == np.int64
    assert int(out[0]) == _ms(*utc)
    assert int(fh.raw_to_utc_ms_mode(np.array([_ms(*raw)], dtype=np.int64), "tick")[0]) == _ms(*utc)


def test_raw_to_utc_tick_vectorised_and_milliseconds():
    raws = [r for r, _ in TICK_CASES] + [(2024, 7, 12, 16, 59, 58, 999_000)]
    arr = np.array([_ms(*r) for r in raws], dtype=np.int64)
    out = fh.raw_to_utc_ms(arr, BERLIN6)
    exp = [int(_raw_to_utc_ref(*r).timestamp() * 1000) for r in raws]
    assert [int(v) for v in out] == exp
    assert [int(v) for v in fh.utc_to_raw_ms(out, BERLIN6)] == [int(v) for v in arr]


def test_raw_to_utc_m1_is_fixed_plus_5h():
    out = fh.raw_to_utc_ms(np.array([_ms(2024, 7, 12, 16, 59)], dtype=np.int64), FIXED_EST)
    assert int(out[0]) == _ms(2024, 7, 12, 21, 59)
    arr = np.array([_ms(2024, 1, 12, 16, 59, 58), _ms(2024, 7, 12, 16, 59, 58)], dtype=np.int64)
    assert [int(v) for v in fh.raw_to_utc_ms_mode(arr, "m1")] == [int(v) + 5 * 3_600_000 for v in arr]


def test_ticks_to_bars_uses_berlin_minus_6_and_m1_to_bars_fixed_est():
    ticks = fh.parse_csv(b"20240312 155959000,2150.10,2150.35,0\n"
                         b"20240312 170000000,2150.20,2150.45,0\n", "tick")
    bars = fh.ticks_to_bars(ticks)
    assert list(bars["ts"]) == [pd.Timestamp("2024-03-12 20:59", tz="UTC"),
                                pd.Timestamp("2024-03-12 22:00", tz="UTC")]
    m1 = fh.parse_csv(b"20240712 165900;2400.1;2400.5;2399.9;2400.2;0\n", "m1")
    b, _ = fh.m1_to_bars(m1)
    assert list(b["ts"]) == [pd.Timestamp("2024-07-12 21:59", tz="UTC")]


def test_ticks_to_bars_rejects_nonexistent_local_time():
    ticks = fh.parse_csv(b"20240330 203000000,2100.10,2100.35,0\n", "tick")    # Berlin 02:30
    with pytest.raises(fh.TimezoneError):
        fh.ticks_to_bars(ticks)


# ================================ 4. nicht existent / mehrdeutig (tick) ---
def _is_nonexistent(*a) -> bool:
    d = (datetime(*a) + timedelta(hours=6)).replace(tzinfo=BERLIN)
    return d.astimezone(UTC).astimezone(BERLIN).replace(tzinfo=None) != d.replace(tzinfo=None)


def _is_ambiguous(*a) -> bool:
    d = datetime(*a) + timedelta(hours=6)
    return d.replace(fold=0, tzinfo=BERLIN).utcoffset() != d.replace(fold=1, tzinfo=BERLIN).utcoffset()


BAD = [  # (Rohzeit, Art): roh Samstag 20:00-21:00 in den EU-Umstellungsnaechten
    ((2024, 3, 30, 20, 30), "nonexistent"),
    ((2024, 10, 26, 20, 30), "ambiguous"),
    ((2024, 3, 30, 20, 0, 0), "nonexistent"),
    ((2024, 3, 30, 20, 59, 59, 999_000), "nonexistent"),
    ((2024, 10, 26, 20, 0, 0), "ambiguous"),
    ((2024, 10, 26, 20, 59, 59, 999_000), "ambiguous"),
    ((2010, 3, 27, 20, 30), "nonexistent"),
    ((2010, 10, 30, 20, 30), "ambiguous"),
    ((2026, 3, 28, 20, 30), "nonexistent"),
    ((2026, 10, 24, 20, 30), "ambiguous"),
    ((2009, 3, 28, 20, 0), "nonexistent"),
    ((2019, 10, 26, 20, 0), "ambiguous"),
]
GOOD_EDGES = [
    (2024, 3, 30, 19, 59, 59, 999_000),
    (2024, 3, 30, 21, 0, 0),
    (2024, 10, 26, 19, 59, 59, 999_000),
    (2024, 10, 26, 21, 0, 0),
    (2026, 3, 28, 21, 0, 0),
    (2026, 10, 24, 21, 0, 0),
    (2024, 3, 10, 2, 30),          # US-Luecke: unter berlin_minus_6 eindeutig
    (2024, 11, 3, 1, 30),
]


@pytest.mark.parametrize("raw,kind", BAD)
def test_nonexistent_and_ambiguous_raise_in_tick_mode(raw, kind):
    assert (_is_nonexistent if kind == "nonexistent" else _is_ambiguous)(*raw)   # Referenz
    with pytest.raises(fh.TimezoneError):
        fh.raw_to_utc_ms(np.array([_ms(*raw)], dtype=np.int64), BERLIN6)
    # auch eingebettet zwischen gueltigen Zeiten: nie raten
    arr = np.array([_ms(2024, 1, 5, 12), _ms(*raw), _ms(2024, 7, 5, 12)], dtype=np.int64)
    with pytest.raises(fh.TimezoneError):
        fh.raw_to_utc_ms(arr, BERLIN6)


@pytest.mark.parametrize("raw", GOOD_EDGES)
def test_dst_edges_just_outside_are_converted(raw):
    assert not _is_nonexistent(*raw) and not _is_ambiguous(*raw)
    out = fh.raw_to_utc_ms(np.array([_ms(*raw)], dtype=np.int64), BERLIN6)
    assert int(out[0]) == int(_raw_to_utc_ref(*raw).timestamp() * 1000)


@pytest.mark.parametrize("raw,kind", BAD)
def test_m1_mode_never_raises_on_dst_hours(raw, kind):
    out = fh.raw_to_utc_ms_mode(np.array([_ms(*raw)], dtype=np.int64), "m1")
    assert int(out[0]) == _ms(*raw) + 5 * 3_600_000


# ========================================== 5./6. Lauf mit gemocktem HTTP ---
STITCH = {  # UTC-Monat: (Rohmonate, erster Roh-Tick des UTC-Monats, letzte UTC-Minute)
    (2024, 7): ([(2024, 6), (2024, 7)], "2024-06-30 20:00:01", "2024-07-31 23:59"),
    (2024, 2): ([(2024, 1), (2024, 2)], "2024-01-31 19:00:01", "2024-02-29 23:59"),
}


@pytest.fixture(scope="module")
def built(tmp_path_factory):
    cache = {}

    def get(ym):
        if ym not in cache:
            root = tmp_path_factory.mktemp(f"run{ym[0]}{ym[1]:02d}")
            clock = Clock()
            site = site_for(clock, STITCH[ym][0])
            rep = do_run(root, site, clock, ym)
            cache[ym] = (root, rep, site)
        return cache[ym]
    return get


@pytest.mark.parametrize("ym", list(STITCH))
def test_month_stitching_uses_rule_offset(built, ym):
    root, rep, site = built(ym)
    assert rep["exit_code"] == 0, rep
    assert rep["summary"]["months_written"] == [f"{ym[0]}-{ym[1]:02d}"]
    assert site.requests == 4                                  # genau Rohmonate M-1 und M
    df = pd.read_parquet(fh.store_path(root, SYM, *ym))
    lo = pd.Timestamp(datetime(*ym, 1, tzinfo=UTC))
    hi = lo + pd.offsets.MonthBegin(1)
    # erwartete Minuten direkt aus den UTC-Zeitpunkten beider Rohmonate
    ticks = pd.concat([raw_month(*r) for r in STITCH[ym][0]], ignore_index=True)
    ticks = ticks[(ticks["utc"] >= lo) & (ticks["utc"] < hi)]
    assert list(df["ts"]) == sorted(set(ticks["utc"].dt.floor("1min")))
    # Monatsanfang stammt aus dem Vor-Rohmonat (NY-Abend)
    first_raw, last_utc = pd.Timestamp(STITCH[ym][1]), pd.Timestamp(STITCH[ym][2], tz="UTC")
    first = ticks.sort_values("utc").iloc[0]
    assert first["raw"] == first_raw
    assert first["utc"] == pd.Timestamp(_raw_to_utc_ref(*first_raw.timetuple()[:6]))
    assert df["ts"].iloc[0] == lo
    assert df["bid_open"].iloc[0] == pytest.approx(first["bid"])
    assert df["ts"].iloc[-1] == last_utc


def test_july_stitch_sunday_evening_hours(built):
    """Roh 2024-06-30 20:00-23:59 (So, EDT) = UTC 2024-07-01 00:00-03:59 im Juli-File;
    roh 18:00-19:59 = UTC 30.6. 22:00-23:59 gehoert NICHT dazu."""
    root, rep, _ = built((2024, 7))
    assert rep["exit_code"] == 0, rep
    df = pd.read_parquet(fh.store_path(root, SYM, 2024, 7))
    jun = raw_month(2024, 6)
    sun = jun[jun["raw"] >= pd.Timestamp("2024-06-30 20:00")]
    exp = sorted(set(sun["utc"].dt.floor("1min")))
    assert exp[0] == pd.Timestamp("2024-07-01 00:00", tz="UTC")
    assert exp[-1] == pd.Timestamp("2024-07-01 03:59", tz="UTC")
    got = df[df["ts"] < pd.Timestamp("2024-07-01 04:00", tz="UTC")]
    assert list(got["ts"]) == exp
    # Preise der ersten Stunde = Juni-Ticks
    by_min = sun.assign(ts=sun["utc"].dt.floor("1min")).groupby("ts")["bid"].first()
    assert np.allclose(got.set_index("ts")["bid_open"].to_numpy(), by_min.loc[got["ts"]].to_numpy())


def test_summer_month_written_in_utc_with_break_at_21_utc(built):
    root, rep, _ = built((2024, 7))
    assert rep["exit_code"] == 0, rep
    t = pq.read_table(fh.store_path(root, SYM, 2024, 7))
    meta = t.schema.metadata
    assert meta[b"source_timezone"] == _meta_value(fh.SOURCE_TZ_META["tick"])
    assert meta[b"histdata_mode"] == b"histdata_tick"
    df = t.to_pandas()
    assert str(df["ts"].dt.tz) == "UTC"
    ts = df["ts"]
    assert not (ts.dt.hour == 21).any()                    # 17-18 EDT = 21-22 UTC
    for d in pd.date_range("2024-07-01", "2024-07-31", freq="D", tz="UTC"):
        day = ts[ts.dt.normalize() == d]
        wd = d.dayofweek
        if wd <= 3:                                        # Mo-Do: Pause 20:59 -> 22:00
            assert d + pd.Timedelta("20:59:00") in set(day), d
            assert d + pd.Timedelta("22:00:00") in set(day), d
        elif wd == 4:                                      # Fr: letzte Minute 20:59
            assert day.max() == d + pd.Timedelta("20:59:00"), d
        elif wd == 6:                                      # So: erste Minute 22:00
            assert day.min() == d + pd.Timedelta("22:00:00"), d
        else:
            assert day.empty, d
    mrep = json.loads(fh.report_path(root, SYM, 2024, 7).read_text())
    assert mrep["raw_months"]["2024-07"]["timezone"]["ok"] is True
    assert mrep["raw_months"]["2024-07"]["timezone"]["median_offset_h"] == -4


def test_verify_month_summer_uses_cache_and_passes(built):
    root, rep, _ = built((2024, 7))
    assert rep["exit_code"] == 0, rep
    clock = Clock()
    site = Site(clock)
    v = fh.verify_month(SYM, (2024, 7), root, now=lambda: NOW, pause=0.0, attempts=1,
                        jitter=0.0, sleep=clock.sleep, clock=clock, wall=clock, session=site)
    assert v["exit_code"] == 0 and v["ok"] and site.requests == 0
    assert v["raw_months"]["2024-06"]["timezone"]["median_offset_h"] == -4
    assert v["raw_months"]["2024-07"]["timezone"]["median_deviation_h"] == 0


# ===================================== 7. alte Store-Dateien neu bauen ---
# Exakt so von der alten Pruefung (git HEAD vor der Umstellung, fest UTC-5)
# fuer die synthetischen Rohmonate erzeugt: Ctx._quarantine -> dk.write_json_atomic.
OLD_FORMAT_MARKERS = {(2024, 6): {'reason': 'Zeitzone passt nicht zu fest UTC-5: 4 Anker am Monatsrand (erste/letzte 72 '
                       'h) mit genau 1 h Abweichung, z.B. 2024-06-03 16:59:58 pause_beginn -> -4 h '
                       '(Uhrumstellung am Monatsrand?); Median-Offset -4 h statt -5 h; nur 0% der '
                       'Anker bei -5 h; 40 aufeinanderfolgende Anker mit gleichem abweichendem '
                       'Offset (Wechsel mitten im Monat?)',
             'check': {'expected_offset_h': -5,
                       'rows': 55940,
                       'reversals': 0,
                       'max_reversal_s': 0.0,
                       'large_reversals': [],
                       'anchors': 40,
                       'ignored_break_gaps': 0,
                       'share_expected': 0.0,
                       'median_offset_h': -4.0,
                       'offsets': {'-4': 40},
                       'first_offset_h': -4,
                       'last_offset_h': -4,
                       'longest_deviating_run': 40,
                       'edge_violations': [{'raw': '2024-06-03 16:59:58',
                                            'kind': 'pause_beginn',
                                            'implied_offset_h': -4},
                                           {'raw': '2024-06-03 18:00:01',
                                            'kind': 'pause_ende',
                                            'implied_offset_h': -4},
                                           {'raw': '2024-06-28 16:59:58',
                                            'kind': 'wochenschluss',
                                            'implied_offset_h': -4},
                                           {'raw': '2024-06-30 18:00:01',
                                            'kind': 'wochenstart',
                                            'implied_offset_h': -4}],
                       'deviating_examples': [{'raw': '2024-06-03 16:59:58',
                                               'kind': 'pause_beginn',
                                               'expected_ny': '17:00',
                                               'implied_offset_h': -4},
                                              {'raw': '2024-06-03 18:00:01',
                                               'kind': 'pause_ende',
                                               'expected_ny': '18:00',
                                               'implied_offset_h': -4},
                                              {'raw': '2024-06-04 16:59:58',
                                               'kind': 'pause_beginn',
                                               'expected_ny': '17:00',
                                               'implied_offset_h': -4},
                                              {'raw': '2024-06-04 18:00:01',
                                               'kind': 'pause_ende',
                                               'expected_ny': '18:00',
                                               'implied_offset_h': -4},
                                              {'raw': '2024-06-05 16:59:58',
                                               'kind': 'pause_beginn',
                                               'expected_ny': '17:00',
                                               'implied_offset_h': -4},
                                              {'raw': '2024-06-05 18:00:01',
                                               'kind': 'pause_ende',
                                               'expected_ny': '18:00',
                                               'implied_offset_h': -4},
                                              {'raw': '2024-06-06 16:59:58',
                                               'kind': 'pause_beginn',
                                               'expected_ny': '17:00',
                                               'implied_offset_h': -4},
                                              {'raw': '2024-06-06 18:00:01',
                                               'kind': 'pause_ende',
                                               'expected_ny': '18:00',
                                               'implied_offset_h': -4},
                                              {'raw': '2024-06-07 16:59:58',
                                               'kind': 'wochenschluss',
                                               'expected_ny': '17:00',
                                               'implied_offset_h': -4},
                                              {'raw': '2024-06-09 18:00:01',
                                               'kind': 'wochenstart',
                                               'expected_ny': '18:00',
                                               'implied_offset_h': -4}],
                       'status': 'failed',
                       'ok': False,
                       'reason': '4 Anker am Monatsrand (erste/letzte 72 h) mit genau 1 h '
                                 'Abweichung, z.B. 2024-06-03 16:59:58 pause_beginn -> -4 h '
                                 '(Uhrumstellung am Monatsrand?); Median-Offset -4 h statt -5 h; '
                                 'nur 0% der Anker bei -5 h; 40 aufeinanderfolgende Anker mit '
                                 'gleichem abweichendem Offset (Wechsel mitten im Monat?)'},
             'since': '2024-12-01T12:00:00+00:00',
             'accepted': False},
 (2024, 7): {'reason': 'Zeitzone passt nicht zu fest UTC-5: 12 Anker am Monatsrand (erste/letzte '
                       '72 h) mit genau 1 h Abweichung, z.B. 2024-07-01 16:59:58 pause_beginn -> '
                       '-4 h (Uhrumstellung am Monatsrand?); Median-Offset -4 h statt -5 h; nur 0% '
                       'der Anker bei -5 h; 46 aufeinanderfolgende Anker mit gleichem abweichendem '
                       'Offset (Wechsel mitten im Monat?)',
             'check': {'expected_offset_h': -5,
                       'rows': 63503,
                       'reversals': 0,
                       'max_reversal_s': 0.0,
                       'large_reversals': [],
                       'anchors': 46,
                       'ignored_break_gaps': 0,
                       'share_expected': 0.0,
                       'median_offset_h': -4.0,
                       'offsets': {'-4': 46},
                       'first_offset_h': -4,
                       'last_offset_h': -4,
                       'longest_deviating_run': 46,
                       'edge_violations': [{'raw': '2024-07-01 16:59:58',
                                            'kind': 'pause_beginn',
                                            'implied_offset_h': -4},
                                           {'raw': '2024-07-01 18:00:01',
                                            'kind': 'pause_ende',
                                            'implied_offset_h': -4},
                                           {'raw': '2024-07-02 16:59:58',
                                            'kind': 'pause_beginn',
                                            'implied_offset_h': -4},
                                           {'raw': '2024-07-02 18:00:01',
                                            'kind': 'pause_ende',
                                            'implied_offset_h': -4},
                                           {'raw': '2024-07-03 16:59:58',
                                            'kind': 'pause_beginn',
                                            'implied_offset_h': -4},
                                           {'raw': '2024-07-03 18:00:01',
                                            'kind': 'pause_ende',
                                            'implied_offset_h': -4},
                                           {'raw': '2024-07-29 16:59:58',
                                            'kind': 'pause_beginn',
                                            'implied_offset_h': -4},
                                           {'raw': '2024-07-29 18:00:01',
                                            'kind': 'pause_ende',
                                            'implied_offset_h': -4},
                                           {'raw': '2024-07-30 16:59:58',
                                            'kind': 'pause_beginn',
                                            'implied_offset_h': -4},
                                           {'raw': '2024-07-30 18:00:01',
                                            'kind': 'pause_ende',
                                            'implied_offset_h': -4},
                                           {'raw': '2024-07-31 16:59:58',
                                            'kind': 'pause_beginn',
                                            'implied_offset_h': -4},
                                           {'raw': '2024-07-31 18:00:01',
                                            'kind': 'pause_ende',
                                            'implied_offset_h': -4}],
                       'deviating_examples': [{'raw': '2024-07-01 16:59:58',
                                               'kind': 'pause_beginn',
                                               'expected_ny': '17:00',
                                               'implied_offset_h': -4},
                                              {'raw': '2024-07-01 18:00:01',
                                               'kind': 'pause_ende',
                                               'expected_ny': '18:00',
                                               'implied_offset_h': -4},
                                              {'raw': '2024-07-02 16:59:58',
                                               'kind': 'pause_beginn',
                                               'expected_ny': '17:00',
                                               'implied_offset_h': -4},
                                              {'raw': '2024-07-02 18:00:01',
                                               'kind': 'pause_ende',
                                               'expected_ny': '18:00',
                                               'implied_offset_h': -4},
                                              {'raw': '2024-07-03 16:59:58',
                                               'kind': 'pause_beginn',
                                               'expected_ny': '17:00',
                                               'implied_offset_h': -4},
                                              {'raw': '2024-07-03 18:00:01',
                                               'kind': 'pause_ende',
                                               'expected_ny': '18:00',
                                               'implied_offset_h': -4},
                                              {'raw': '2024-07-04 16:59:58',
                                               'kind': 'pause_beginn',
                                               'expected_ny': '17:00',
                                               'implied_offset_h': -4},
                                              {'raw': '2024-07-04 18:00:01',
                                               'kind': 'pause_ende',
                                               'expected_ny': '18:00',
                                               'implied_offset_h': -4},
                                              {'raw': '2024-07-05 16:59:58',
                                               'kind': 'wochenschluss',
                                               'expected_ny': '17:00',
                                               'implied_offset_h': -4},
                                              {'raw': '2024-07-07 18:00:01',
                                               'kind': 'wochenstart',
                                               'expected_ny': '18:00',
                                               'implied_offset_h': -4}],
                       'status': 'failed',
                       'ok': False,
                       'reason': '12 Anker am Monatsrand (erste/letzte 72 h) mit genau 1 h '
                                 'Abweichung, z.B. 2024-07-01 16:59:58 pause_beginn -> -4 h '
                                 '(Uhrumstellung am Monatsrand?); Median-Offset -4 h statt -5 h; '
                                 'nur 0% der Anker bei -5 h; 46 aufeinanderfolgende Anker mit '
                                 'gleichem abweichendem Offset (Wechsel mitten im Monat?)'},
             'since': '2024-12-01T12:00:00+00:00',
             'accepted': False}}


def _old_marker(root, ym):
    """Quarantaene-Marker der alten Pruefung (fest UTC-5, ohne Versionsfeld),
    geschrieben wie damals dk.write_json_atomic (indent=2, ensure_ascii=False)."""
    q = fh.Unit(SYM, "tick", *ym).qmarker(root, ym)
    q.parent.mkdir(parents=True, exist_ok=True)
    q.write_text(json.dumps(OLD_FORMAT_MARKERS[ym], indent=2, ensure_ascii=False) + "\n")
    return q


@pytest.mark.parametrize("with_marker", [False, True], ids=["plain", "old_tz_marker"])
def test_store_file_with_old_source_timezone_is_rebuilt_from_cache(tmp_path, with_marker):
    clock = Clock()
    site = site_for(clock, [(2024, 6), (2024, 7)])
    rep = do_run(tmp_path, site, clock, (2024, 7))
    assert rep["exit_code"] == 0, rep
    out = fh.store_path(tmp_path, SYM, 2024, 7)
    ref = pd.read_parquet(out)
    # Altlast simulieren: Inhalt alt (fest +5 h = 1 h spaeter), Metadatum alt
    t = pq.read_table(out)
    old = t.to_pandas()
    old["ts"] = old["ts"] + pd.Timedelta(hours=1)
    old = old[old["ts"] < pd.Timestamp("2024-08-01", tz="UTC")].reset_index(drop=True)
    stale = pa.Table.from_pandas(old, preserve_index=False)
    meta = dict(t.schema.metadata)
    meta[b"source_timezone"] = OLD_TZ_META
    pq.write_table(stale.replace_schema_metadata(meta), out)
    if with_marker:
        _old_marker(tmp_path, (2024, 7))
        _old_marker(tmp_path, (2024, 6))

    site2 = Site(clock)                                    # keine Dateien: Anfrage waere Fehler
    rep = do_run(tmp_path, site2, clock, (2024, 7))
    assert site2.requests == 0 and rep["summary"]["requests"] == 0
    assert rep["exit_code"] == 0, rep
    assert "2024-07" in _months(_rebuilt(rep))
    saved = json.loads(fh.run_report_path(tmp_path, SYM).read_text())
    assert "2024-07" in _months(_rebuilt(saved))
    t = pq.read_table(out)
    assert t.schema.metadata[b"source_timezone"] == _meta_value(fh.SOURCE_TZ_META["tick"])
    assert t.schema.metadata[b"histdata_mode"] == b"histdata_tick"
    pd.testing.assert_frame_equal(t.to_pandas(), ref)

    # dritter Lauf: aktuelle Datei bleibt, nichts mehr neu gebaut
    rep = do_run(tmp_path, Site(clock), clock, (2024, 7))
    assert rep["exit_code"] == 0 and rep["summary"]["months_skipped_existing"] == 1
    assert "2024-07" not in _months(_rebuilt(rep))


# ============================== 8. Tick in nicht existenter Stunde: Quarantaene ---
@pytest.mark.parametrize("bad_raw,raw_ym,utc_months", [
    ("2024-03-30 20:30:00", (2024, 3), [(2024, 3), (2024, 4)]),     # Fruehjahr (Berlin 02:30): nicht existent
    ("2024-10-26 20:30:00", (2024, 10), [(2024, 10), (2024, 11)]),  # Herbst (Berlin 02:30): mehrdeutig
], ids=["nonexistent", "ambiguous"])
def test_tick_in_dst_gap_quarantines_raw_month(tmp_path, bad_raw, raw_ym, utc_months):
    df = raw_month(*raw_ym)
    bad = pd.DataFrame({"raw": [pd.Timestamp(bad_raw)], "utc": [pd.NaT], "bid": [2050.0],
                        "ask": [2050.3]})
    df = pd.concat([df, bad], ignore_index=True).sort_values("raw", kind="mergesort")
    prev, nxt = fh._ym_add(raw_ym, -1), fh._ym_add(raw_ym, 1)
    clock = Clock()
    site = site_for(clock, [prev, nxt], {("T", f"{raw_ym[0]}{raw_ym[1]:02d}"):
                                         make_zip(*raw_ym, tick_csv(df))})
    now = datetime(2025, 3, 1, 12, tzinfo=UTC)
    rep = do_run(tmp_path, site, clock, utc_months[0], utc_months[-1], now=now)
    assert rep["exit_code"] == 4, rep
    assert rep["summary"]["months_written"] == []
    for ym in utc_months:                                  # beide UTC-Monate brauchen raw_ym
        assert not fh.store_path(tmp_path, SYM, *ym).exists(), ym
    assert f"{utc_months[0][0]}-{utc_months[0][1]:02d}" in rep["summary"]["months_quarantine"]
    q = fh.Unit(SYM, "tick", *raw_ym).qmarker(tmp_path, raw_ym)
    assert q.exists()
    assert {r["raw_month"] for r in rep["quarantine"]} == {f"{raw_ym[0]}-{raw_ym[1]:02d}"}
    # Kontrolle: dieselben Monate ohne den einen Tick werden geschrieben
    # (die Quarantaene kommt also wirklich von der nicht eindeutigen Ortszeit)
    clean = site_for(clock, [prev, raw_ym, nxt])
    rep = do_run(tmp_path / "clean", clean, clock, utc_months[0], utc_months[-1], now=now)
    assert rep["exit_code"] == 0, rep
    assert rep["summary"]["months_written"] == [f"{y}-{m:02d}" for y, m in utc_months]


# ============== 9. Homelab-Zustand: alte TZ-Quarantaene, keine Store-Datei ---
HOMELAB_RAW = [(2023, 12), (2024, 1), (2024, 6), (2024, 7)]


def _seed_cache(root, months, stamping="berlin"):
    """Roh-ZIPs direkt in den Cache legen (wie nach frueheren Laeufen)."""
    for y, m in months:
        p = fh.Unit(SYM, "tick", y, m).path(root)
        p.parent.mkdir(parents=True, exist_ok=True)
        blob = tick_zip(y, m) if stamping == "berlin" else make_zip(y, m, tick_csv(raw_month(y, m, stamping)))
        p.write_bytes(blob)


def _verify(root, clock, site, ym, now=NOW):
    return fh.verify_month(SYM, ym, root, now=lambda: now, pause=0.0, attempts=1, jitter=0.0,
                           sleep=clock.sleep, clock=clock, wall=clock, session=site)


def _expected_minutes(ym, raws):
    lo = pd.Timestamp(datetime(*ym, 1, tzinfo=UTC))
    hi = lo + pd.offsets.MonthBegin(1)
    t = pd.concat([raw_month(*r) for r in raws], ignore_index=True)
    t = t[(t["utc"] >= lo) & (t["utc"] < hi)]
    return sorted(set(t["utc"].dt.floor("1min")))


@pytest.mark.parametrize("entry", ["run", "verify_then_run"])
def test_homelab_old_tz_quarantine_is_rechecked_without_requests(tmp_path, entry):
    """Rohmonate gecached, Juni/Juli mit Quarantaene-Marker der alten
    Fest-EST-Pruefung, KEINE Store-Datei: neu pruefen (New York), bestehen,
    schreiben - 0 HTTP-Requests, alter Marker blockiert nicht mehr."""
    _seed_cache(tmp_path, HOMELAB_RAW)
    for ym in [(2024, 6), (2024, 7)]:
        _old_marker(tmp_path, ym)
    assert not fh.store_path(tmp_path, SYM, 2024, 7).exists()
    clock = Clock()
    site = Site(clock)                                     # leer: jede Anfrage waere ein Fehler

    if entry == "verify_then_run":
        v = _verify(tmp_path, clock, site, (2024, 7))
        assert v["exit_code"] == 0 and v["ok"], v.get("reason")
        assert site.requests == 0 and v["requests"] == 0
        for r in ("2024-06", "2024-07"):
            tz = v["raw_months"][r]["timezone"]
            assert tz["ok"] is True and tz["median_offset_h"] == -4 and tz["median_deviation_h"] == 0
        assert not fh.store_path(tmp_path, SYM, 2024, 7).exists()      # verify schreibt kein Parquet

    rep = do_run(tmp_path, site, clock, (2024, 7))
    assert rep["exit_code"] == 0, rep
    assert site.requests == 0 and rep["summary"]["requests"] == 0
    assert rep["summary"]["months_written"] == ["2024-07"]
    assert rep["quarantined"] == [] and rep["summary"]["months_quarantine"] == []
    out = fh.store_path(tmp_path, SYM, 2024, 7)
    t = pq.read_table(out)
    assert t.schema.metadata[b"source_timezone"] == _meta_value(fh.SOURCE_TZ_META["tick"])
    assert t.schema.metadata[b"histdata_mode"] == b"histdata_tick"
    assert list(t.to_pandas()["ts"]) == _expected_minutes((2024, 7), [(2024, 6), (2024, 7)])
    mrep = json.loads(fh.report_path(tmp_path, SYM, 2024, 7).read_text())
    for r in ("2024-06", "2024-07"):
        assert mrep["raw_months"][r]["timezone"]["ok"] is True
        assert mrep["raw_months"][r]["timezone"]["median_offset_h"] == -4

    # Winter (Dez/Jan, unter der alten Pruefung ok) ebenfalls ohne Requests
    rep = do_run(tmp_path, site, clock, (2024, 1))
    assert rep["exit_code"] == 0 and rep["summary"]["months_written"] == ["2024-01"], rep
    assert site.requests == 0
    assert list(pd.read_parquet(fh.store_path(tmp_path, SYM, 2024, 1))["ts"]) == \
        _expected_minutes((2024, 1), [(2023, 12), (2024, 1)])

    # alter Marker blockiert auch spaeter nicht: Store-Datei weg -> erneut gebaut
    out.unlink()
    rep = do_run(tmp_path, site, clock, (2024, 7))
    assert rep["exit_code"] == 0 and rep["summary"]["months_written"] == ["2024-07"], rep
    assert site.requests == 0
    v = _verify(tmp_path, clock, site, (2024, 7))
    assert v["exit_code"] == 0 and v["ok"] and site.requests == 0


def _markers(root) -> dict:
    d = fh.raw_dir(root, SYM, "tick")
    return {p.name: p.read_bytes() for p in sorted(d.glob("*.quarantine"))}


@pytest.mark.parametrize("entry", ["run", "verify"])
def test_new_format_quarantine_of_bad_month_keeps_blocking(tmp_path, entry):
    """Ein wirklich falscher Monat (UTC-gestempelt) wird von der NEUEN Pruefung
    quarantaeniert; deren Marker blockiert weiter: kein erneutes Pruefen/
    Neuschreiben des Markers, keine Requests, nichts geschrieben."""
    # mit Nachbar-Rohmonaten im Cache (sonst ist das Ergebnis vorlaeufig und wird
    # bewusst nicht als Marker gespeichert, v4)
    _seed_cache(tmp_path, [(2024, 5), (2024, 6), (2024, 7), (2024, 8)], stamping="utc")
    clock = Clock()
    site = Site(clock)
    rep = do_run(tmp_path, site, clock, (2024, 7))
    assert rep["exit_code"] == 4 and rep["status"] == "quarantine", rep
    assert site.requests == 0
    assert not fh.store_path(tmp_path, SYM, 2024, 7).exists()
    before = _markers(tmp_path)
    assert before, "kein Quarantaene-Marker geschrieben"
    for blob in before.values():
        info = json.loads(blob)
        assert info["accepted"] is False and info["reason"]

    for k in range(1, 3):                                  # spaeteres "now": ein neu
        later = NOW + timedelta(days=k)                    # geschriebener Marker haette
        if entry == "run":                                 # ein anderes "since"
            rep = do_run(tmp_path, site, clock, (2024, 7), now=later)
            assert rep["exit_code"] == 4 and rep["status"] == "quarantine", rep
            assert rep["summary"]["months_written"] == []
        else:
            v = _verify(tmp_path, clock, site, (2024, 7), now=later)
            assert v["exit_code"] == 4 and not v["ok"]
        assert site.requests == 0
        assert _markers(tmp_path) == before                # Marker unveraendert (kein Neupruefen)
        assert not fh.store_path(tmp_path, SYM, 2024, 7).exists()


# ====== 10. Review: andere Sommerzeitregeln / duenne Daten ueber viele Jahre ---
# Modell "wuerde geschrieben": UTC-Monat M wird geschrieben, wenn die Rohmonate
# M-1 und M bestehen und der Folge-Rohmonat nicht mit +-1 h Abweichung beginnt
# (check_following). Geprueft wird wie in raw_slice: mit den Randticks der
# Nachbar-Rohmonate. Ein falsch umgerechneter Tick in einem geschriebenen
# UTC-Monat ist ein Fehler.
_H = 3_600_000


def _market_utc(sym, lo, hi, step_s, thin=None, seed=1):
    idx = pd.date_range(lo, hi, freq=f"{step_s}s", inclusive="left", tz="UTC") + pd.Timedelta(milliseconds=137)
    ny = idx.tz_convert(NY)
    wd, h = ny.dayofweek, ny.hour + ny.minute / 60
    open_h = 17 if sym == "EURUSD" else 18
    closed = ((wd == 4) & (h >= 17)) | (wd == 5) | ((wd == 6) & (h < open_h))
    if sym != "EURUSD":
        closed |= (wd <= 3) & (h >= 17) & (h < 18)
    idx = idx[~np.asarray(closed)]
    if thin is not None:
        idx = idx[np.random.default_rng(seed).random(len(idx)) < thin]
    return idx


def _sunday(y, m, n=1, last=False):
    if last:
        d = pd.Timestamp(y, m, 1) + pd.offsets.MonthEnd(0)
        return d - pd.Timedelta(days=(d.dayofweek - 6) % 7)
    d = pd.Timestamp(y, m, 1)
    return d + pd.Timedelta(days=(6 - d.dayofweek) % 7) + pd.Timedelta(weeks=n - 1)


def _stamp(utc, how):
    """Rohzeit-ms: ny = New York (richtig), est = fest UTC-5, eu = UTC-5/-4 nach
    EU-Umstellung (letzter So Maerz/Oktober 01:00 UTC), us2006 = US-Regel vor
    2007 (erster So April 2:00 bis letzter So Oktober 2:00) fuer ALLE Jahre."""
    if how == "ny":
        return np.asarray(utc.tz_convert(NY).tz_localize(None).as_unit("ms").asi8, dtype=np.int64)
    u = np.asarray(utc.as_unit("ms").asi8, dtype=np.int64)
    off = np.full(len(u), -5 * _H, dtype=np.int64)
    if how == "est":
        return u + off
    naive = utc.tz_localize(None)
    for y in np.unique(utc.year):
        if how == "eu":
            s, e = _sunday(y, 3, last=True) + pd.Timedelta(hours=1), _sunday(y, 10, last=True) + pd.Timedelta(hours=1)
        else:
            s, e = _sunday(y, 4, 1) + pd.Timedelta(hours=7), _sunday(y, 10, last=True) + pd.Timedelta(hours=6)
        off[np.asarray((naive >= s) & (naive < e))] = -4 * _H
    return u + off


def _written_with_wrong_ticks(sym, how, y0, y1, step_s, thin=None):
    utc = _market_utc(sym, pd.Timestamp(y0 - 1, 11, 1, tz="UTC"), pd.Timestamp(y1 + 1, 3, 1, tz="UTC"),
                      step_s, thin)
    u = np.asarray(utc.as_unit("ms").asi8, dtype=np.int64)
    raw = _stamp(utc, how)
    order = np.argsort(raw, kind="stable")
    raw, u = raw[order], u[order]
    months = [(y, m) for y in range(y0 - 1, y1 + 2) for m in range(1, 13)
              if (y0 - 1, 12) <= (y, m) <= (y1 + 1, 1)]
    res, wrong_in = {}, {}
    for ym in months[1:-1]:
        lo, hi = fh.raw_month_bounds_ms(ym)
        a, b = int(np.searchsorted(raw, lo)), int(np.searchsorted(raw, hi))
        pad = fh.TZ_PAD_MS                                      # wie Ctx._edge_ms
        nb = (raw[int(np.searchsorted(raw, lo - pad)):a] if a else None,
              raw[b:int(np.searchsorted(raw, hi + pad))] if b < len(raw) else None)
        chk = fh.check_timezone(raw[a:b], sym, (lo, hi), "tick", neighbours=nb)
        try:
            bad = fh.raw_to_utc_ms_mode(raw[a:b], "tick", ym) != u[a:b]
        except fh.TimezoneError:
            chk, bad = {"ok": False}, np.ones(b - a, bool)
        res[ym] = chk
        days = np.unique(u[a:b][bad] // 86_400_000)
        wrong_in[ym] = {(d.year, d.month) for d in pd.to_datetime(days, unit="D")}
    out, fails = [], []
    for M in months[2:-2]:
        p, n = fh._ym_add(M, -1), fh._ym_add(M, 1)
        if not res[M]["ok"]:
            fails.append((M, res[M].get("reason")))
        written = res[p]["ok"] and res[M]["ok"] and not (not res[n]["ok"] and res[n].get("switch_reaches_prev_month"))
        if written and any(M in wrong_in[r] for r in (p, M)):
            out.append((M, dict(res[M].get("deviations") or {})))
    return out, fails


@pytest.mark.usefixtures("berlin_schedule")
@pytest.mark.parametrize("sym", ["XAUUSD", "XAGUSD"])
@pytest.mark.parametrize("how", ["ny", "us2006"])
def test_other_dst_rules_never_written_2001_2026(sym, how):
    """US-Regel (ny_local) bzw. Vor-2007-US-Regel statt berlin_minus_6: kein falsch
    umgerechneter Tick landet in einem geschriebenen UTC-Monat (die EU/US-Luecken-
    wochen im Maerz und Okt/Nov werden als bestaetigter Wechsel erkannt)."""
    wrong, fails = _written_with_wrong_ticks(sym, how, 2001, 2026, step_s=180)
    assert wrong == []
    assert len(fails) >= 30                                     # die Abweichungen werden gesehen


@pytest.mark.usefixtures("berlin_schedule")
@pytest.mark.parametrize("sym", ["XAUUSD", "XAGUSD"])
def test_berlin_minus_6_stamping_passes_every_month_2001_2026(sym):
    wrong, fails = _written_with_wrong_ticks(sym, "eu", 2001, 2026, step_s=180)
    assert wrong == [] and fails == []


@pytest.mark.usefixtures("berlin_schedule")
@pytest.mark.parametrize("how", ["eu", "est", "ny"])
def test_thin_silver_one_tick_per_20_min(how):
    """Review 8: ~1 Tick je 20 min (XAGUSD nachts/frueh). Vorher fiel jeder Monat
    an der +-1-h-Randregel durch (letzter Tick > 30 min vor 17:00 rundete auf -1).
    Jetzt: berlin_minus_6 ("eu") besteht ueberall; falsch gestempelte Monate
    (fest EST, US-Regel) werden nie geschrieben."""
    wrong, fails = _written_with_wrong_ticks("XAGUSD", how, 2014, 2025, step_s=60, thin=0.05)
    assert wrong == []
    if how == "eu":
        assert fails == []
    else:
        assert len(fails) >= (90 if how == "est" else 20)


# ======= 11. v4: Datenfehler bei RICHTIGER Stempelung sind keine Quarantaene ---
# (Re-Review: verirrte Ticks, Feed-Kanten genau 1 h daneben, 17:15-Schluss,
# Ausfaelle >= 24 h unter der Woche, Ausfall genau 60 min auf vollen Stunden)
def _ny_local(sym, lo, hi, step_s=30):
    """Ticks in New-York-Ortszeit (naiv) mit den Marktzeiten von _market_utc."""
    utc = _market_utc(sym, pd.Timestamp(lo, tz="UTC"), pd.Timestamp(hi, tz="UTC"), step_s)
    return pd.DatetimeIndex(utc.tz_convert(NY).tz_localize(None))


def _to_raw(loc):
    """NY-Ortszeit (Marktzeit, naiv) -> Rohzeit nach berlin_minus_6."""
    utc = pd.DatetimeIndex(loc).tz_localize(NY, ambiguous="NaT", nonexistent="NaT").dropna()
    return utc.tz_convert(BERLIN).tz_localize(None) - pd.Timedelta(hours=6)


def _check_with_context(loc, sym, ym):
    """Wie Ctx.raw_slice: eigener Rohmonat + TZ_PAD_MS Kontext der Nachbarmonate.
    loc: Marktzeiten in NY-Ortszeit, gestempelt nach berlin_minus_6."""
    raw = np.asarray(_to_raw(loc).sort_values().as_unit("ms").asi8, dtype=np.int64)
    lo, hi = fh.raw_month_bounds_ms(ym)
    a, b = int(np.searchsorted(raw, lo)), int(np.searchsorted(raw, hi))
    pad = fh.TZ_PAD_MS
    nb = (raw[int(np.searchsorted(raw, lo - pad)):a], raw[b:int(np.searchsorted(raw, hi + pad))])
    return fh.check_timezone(raw[a:b], sym, (lo, hi), "tick", neighbours=nb)


def _nth(ym, wd, n):
    d = [x for x in pd.date_range(pd.Timestamp(*ym, 1), pd.Timestamp(*ym, 1) + pd.offsets.MonthEnd(0))
         if x.dayofweek == wd]
    return d[n]


def _drop(loc, a, b):
    return loc[~((loc >= a) & (loc < b))]


def _fault(case, sym, ym, loc):
    op = 17 if sym == "EURUSD" else 18
    H = pd.Timedelta(hours=1)
    fri, sun, tue = _nth(ym, 4, 1), _nth(ym, 6, 1), _nth(ym, 1, 1)
    if case == "stray_fri_1706":
        return loc.append(pd.DatetimeIndex([fri + 17 * H + pd.Timedelta(minutes=6)]))
    if case == "stray_fri_1720":
        return loc.append(pd.DatetimeIndex([fri + 17 * H + pd.Timedelta(minutes=20)]))
    if case == "stray_sun_minus10":
        return loc.append(pd.DatetimeIndex([sun + op * H - pd.Timedelta(minutes=10)]))
    if case == "stray_sun_minus30":
        return loc.append(pd.DatetimeIndex([sun + op * H - pd.Timedelta(minutes=30)]))
    if case == "fri_feed_stops_1600":
        return _drop(loc, fri + 16 * H, fri + 17 * H)
    if case == "sun_opens_1h_late":
        return _drop(loc, sun + op * H, sun + (op + 1) * H)
    if case == "fri_1600_and_sun_late_same_weekend":
        return _drop(_drop(loc, fri + 16 * H, fri + 17 * H), fri + 2 * 24 * H + op * H, fri + 2 * 24 * H + (op + 1) * H)
    if case == "close_1715_every_week":           # alter CME-Plan: Fr-Schluss/Tagespause ab 17:15
        extra = pd.date_range(loc[0].normalize(), loc[-1], freq="30s") + pd.Timedelta(seconds=1)
        wd, hm = extra.dayofweek, extra.hour * 60 + extra.minute
        add = extra[np.asarray(((wd <= 4) & (hm >= 17 * 60) & (hm < 17 * 60 + 15)))]
        return loc.append(add)
    if case == "midweek_outage_24h30":
        return _drop(loc, tue + 10 * H, tue + 34 * H + pd.Timedelta(minutes=30))
    if case == "exact_hour_outage_2000":         # 60 min auf vollen Stunden (Kandidat fuer "Pause")
        return _drop(loc, tue + 20 * H, tue + 21 * H + pd.Timedelta(minutes=1))
    if case == "holiday_early_close_fri_1300":
        return _drop(loc, fri + 13 * H, fri + 17 * H)
    raise ValueError(case)


FAULTS = ["stray_fri_1706", "stray_fri_1720", "stray_sun_minus10", "stray_sun_minus30",
          "fri_feed_stops_1600", "sun_opens_1h_late", "fri_1600_and_sun_late_same_weekend",
          "close_1715_every_week", "midweek_outage_24h30", "exact_hour_outage_2000",
          "holiday_early_close_fri_1300"]


@functools.lru_cache(maxsize=None)
def _base_local(sym, ym):
    lo = pd.Timestamp(*ym, 1) - pd.Timedelta(days=12)
    return _ny_local(sym, lo, pd.Timestamp(*ym, 1) + pd.offsets.MonthBegin(1) + pd.Timedelta(days=12))


@pytest.mark.parametrize("ym", [(2021, 3), (2019, 7)], ids=["2021-03_transition", "2019-07"])
@pytest.mark.parametrize("sym", ["XAUUSD", "XAGUSD"])
@pytest.mark.parametrize("case", FAULTS)
def test_v4_data_faults_on_correct_stamping_pass(case, sym, ym):
    """Jeder dieser Fehler liess v3 den ganzen Monat durchfallen; jetzt hoechstens Warnung."""
    loc = _fault(case, sym, ym, _base_local(sym, ym)).sort_values()
    rep = _check_with_context(loc, sym, ym)
    assert rep["ok"], (case, rep["reason"])
    if case == "midweek_outage_24h30":
        assert rep["outages"]                    # gemeldet, kein Wochenend-Anker


@pytest.mark.parametrize("sym", ["XAUUSD", "XAGUSD"])
def test_v4_two_independent_gaps_with_same_shift_fail(sym):
    """Gegenprobe: eine Woche +1 h (Sonntag bis Sonntag) mitten im Monat -> Wechsel."""
    ym = (2021, 3)
    loc = _base_local(sym, ym)
    s = pd.Timestamp(2021, 3, 21, 12)                     # Sonntag (Ortszeit), nach der Umstellung
    shifted = loc.where(~((loc >= s) & (loc < s + pd.Timedelta(days=7))), loc + pd.Timedelta(hours=1))
    rep = _check_with_context(pd.DatetimeIndex(shifted).sort_values(), sym, ym)
    assert not rep["ok"] and rep["switch_runs"], rep["reason"]
    assert {e["deviation_h"] for r in rep["switch_runs"] for e in r} == {1}


def test_v4_e2e_week_shift_not_written(tmp_path):
    """Re-Review e2e_week (jetzt XAUUSD, berlin_minus_6): +1 h So 2025-05-25 22:00 UTC
    .. 06-01, die ersten 4 Minuten nach der verschobenen Oeffnung fehlen. Im echten
    Lauf (Backfill April..Juni, Juli veroeffentlicht) darf UTC-Mai nicht geschrieben werden."""
    import test_fetch_histdata as tf
    sym = "XAUUSD"
    utc = tf.market_utc(sym, pd.Timestamp("2025-02-20", tz="UTC"), pd.Timestamp("2025-07-05", tz="UTC"), 60)
    s, e = pd.Timestamp("2025-05-25 22:00", tz="UTC"), pd.Timestamp("2025-06-01 22:00", tz="UTC")
    utc = utc[~((utc >= s) & (utc < s + pd.Timedelta(minutes=4)))]
    raw = utc.tz_convert(BERLIN).tz_localize(None) - pd.Timedelta(hours=6)
    shift = (utc >= s) & (utc < e)
    raw = raw.where(~shift, raw + pd.Timedelta(hours=1))
    i = (utc.as_unit("ms").asi8 // 60_000).astype(float)
    bid = np.round(3300.0 * (1 + 0.002 * np.sin(i / 97.0)), 2)
    df = pd.DataFrame({"raw": raw, "utc": utc, "bid": bid, "ask": bid + 0.25})
    files = {}
    for (y, m) in ((2025, 3), (2025, 4), (2025, 5), (2025, 6), (2025, 7)):
        sel = (df.raw >= pd.Timestamp(y, m, 1)) & (df.raw < pd.Timestamp(y, m, 1) + pd.offsets.MonthBegin(1))
        files[("T", f"{y}{m:02d}")] = tf._zip_from(df[sel], sym, y, m)
    now = datetime(2025, 8, 25, tzinfo=UTC)
    clock = tf.Clock()
    rep = fh.run(sym, date(2025, 4, 1), date(2025, 6, 1), tmp_path, mode="tick", now=lambda: now,
                 **tf.kw(clock, tf.Site(clock, files)))
    assert "2025-05" not in rep["summary"]["months_written"], rep["quarantine"]
    assert "2025-04" in rep["summary"]["months_written"]
    # Monat fuer Monat veroeffentlicht (Folgemonat noch nicht da): auch zwischendurch
    # nie ein UTC-Monat mit verschobenen Ticks im Store
    root = tmp_path / "monthly"
    for ym in ((2025, 4), (2025, 5), (2025, 6)):
        avail = {k: v for k, v in files.items() if (int(k[1][:4]), int(k[1][4:])) <= ym}
        t = datetime(*fh._ym_add(ym, 1), 20, tzinfo=UTC)
        fh.run(sym, date(2025, 4, 1), date(*ym, 1), root, mode="tick", now=lambda: t,
               **tf.kw(clock, tf.Site(clock, avail)))
        assert not fh.store_path(root, sym, 2025, 5).exists(), ym
    fh.run(sym, date(2025, 4, 1), date(2025, 6, 1), root, mode="tick", now=lambda: now,
           **tf.kw(clock, tf.Site(clock, files)))
    assert not fh.store_path(root, sym, 2025, 5).exists()


def test_v4_verify_month_writes_and_deletes_no_markers(tmp_path):
    """--verify-month ist nur Bericht: alte Marker bleiben, neue entstehen nicht."""
    _seed_cache(tmp_path, [(2024, 5), (2024, 6), (2024, 7), (2024, 8)], stamping="utc")
    q6 = _old_marker(tmp_path, (2024, 6))
    before = q6.read_bytes()
    clock = Clock()
    v = _verify(tmp_path, clock, Site(clock), (2024, 7))
    assert v["exit_code"] == 4 and not v["ok"]
    assert q6.read_bytes() == before
    assert list(_markers(tmp_path)) == [q6.name]


def test_v4_failure_without_expected_neighbour_is_not_persisted(tmp_path):
    """Fehlt ein Nachbar-Rohmonat, der existieren sollte, ist das Ergebnis vorlaeufig:
    Quarantaene in diesem Lauf, aber kein Marker (naechster Lauf prueft neu)."""
    _seed_cache(tmp_path, [(2024, 6), (2024, 7)], stamping="utc")
    clock = Clock()
    rep = do_run(tmp_path, Site(clock), clock, (2024, 7))
    assert rep["exit_code"] == 4 and all("VORLAEUFIG" in q["reason"] for q in rep["quarantine"])
    assert not _markers(tmp_path)
    _seed_cache(tmp_path, [(2024, 5), (2024, 8)], stamping="utc")   # Nachbarn jetzt da
    rep = do_run(tmp_path, Site(clock), clock, (2024, 7))
    assert rep["exit_code"] == 4 and _markers(tmp_path)
    assert not any("VORLAEUFIG" in q["reason"] for q in rep["quarantine"])


# ===== 12. Review v5: mehrdeutiges Wochenende am EU-Umstellungssonntag 31.10. ---
def _thin_silver_us_rule_2010(seed):
    """XAGUSD 2010-09..12, duenn (~1 Tick je 3,5 min), nach US-Regel (ny_local)
    gestempelt, Fahrplan berlin_minus_6. 2010 ist die EU-Umstellung am So 31.10.:
    das Wochenende Fr 29.10. -> So 31.10. ist mehrdeutig."""
    import test_fetch_histdata as tf
    rng = np.random.default_rng(seed)
    utc = tf.market_utc("XAGUSD", pd.Timestamp("2010-08-25", tz="UTC"), pd.Timestamp("2011-01-05", tz="UTC"), 60)
    utc = utc[rng.random(len(utc)) < 1 / 3.5]
    raw = utc.tz_convert(NY).tz_localize(None)
    i = (utc.as_unit("ms").asi8 // 60_000).astype(float)
    bid = np.round(18.0 * (1 + 0.002 * np.sin(i / 97.0)), 4)
    df = pd.DataFrame({"raw": raw, "utc": utc, "bid": bid, "ask": bid + 0.02})
    files = {}
    for y, m in ((2010, 9), (2010, 10), (2010, 11), (2010, 12)):
        sel = (df.raw >= pd.Timestamp(y, m, 1)) & (df.raw < pd.Timestamp(y, m, 1) + pd.offsets.MonthBegin(1))
        files[("T", f"{y}{m:02d}")] = tf._zip_from(df[sel], "XAGUSD", y, m)
    return utc, files


@pytest.mark.usefixtures("berlin_schedule")
@pytest.mark.parametrize("seed", [1, 5])     # mit der alten Logik: UTC-Oktober mit 43/35 falschen Minuten
def test_v5_ambiguous_weekend_zero_side_does_not_hide_switch(tmp_path, seed):
    """Review v5 (1): der Null-Eintrag DESSELBEN mehrdeutigen Wochenendes (sein
    Freitagsschluss) darf den Lauf nicht vom Vormonat trennen - sonst wurde UTC-
    Oktober mit verschobenem So 31.10. 22-23 UTC geschrieben."""
    import test_fetch_histdata as tf
    utc, files = _thin_silver_us_rule_2010(seed)
    clock = tf.Clock()
    rep = fh.run("XAGUSD", date(2010, 10, 1), date(2010, 11, 1), tmp_path, mode="tick",
                 now=lambda: datetime(2011, 3, 1, tzinfo=UTC), **tf.kw(clock, tf.Site(clock, files)))
    for ym in ((2010, 10), (2010, 11)):
        p = fh.store_path(tmp_path, "XAGUSD", *ym)
        if p.exists():                                 # wenn geschrieben, dann ohne verschobene Bars
            lo = pd.Timestamp(datetime(*ym, 1, tzinfo=UTC))
            hi = lo + pd.offsets.MonthBegin(1)
            truth = set(utc[(utc >= lo) & (utc < hi)].floor("1min"))
            assert set(pd.read_parquet(p)["ts"]) == truth, (ym, rep["summary"]["months_written"])
    assert "2010-10" not in rep["summary"]["months_written"]
