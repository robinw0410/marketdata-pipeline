"""Regressionstests tz_check v6: Regel "undecided" (ny_local oder berlin_minus_6),
Wochenend-Ruecksprung in der EU-Rueckstellungsnacht, Survey mit Perioden.

Befund: 2010-2018 ny_local, 2021 und 2024 berlin_minus_6. Beide Regeln
unterscheiden sich nur in zwei Fenstern je Jahr:
  2. So Maerz 07:00 UTC (US-Beginn) .. letzter So Maerz 01:00 UTC (EU-Beginn)
  letzter So Oktober 01:00 UTC (EU-Ende) .. 1. So November 06:00 UTC (US-Ende)
2021-10 und 2024-10 enthalten verirrte Ticks im geschlossenen Wochenendmarkt
der EU-Rueckstellungsnacht: roh Sa 21:00 -> 20:00 (60-min-Ruecksprung).

Nur synthetische Daten, HTTP gemockt. Erwartete Werte unabhaengig mit
zoneinfo/pandas, nie mit Helfern aus fetch_histdata.
"""
import calendar
import functools
import io
import json
import re
import zipfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import fetch_histdata as fh
import histdata_tz_survey as sv

UTC = timezone.utc
BER = ZoneInfo("Europe/Berlin")
NYZ = ZoneInfo("America/New_York")
HOUR_MS, DAY_MS, MIN_MS = 3_600_000, 86_400_000, 60_000
BASE_PRICE = {"XAUUSD": 2050.0, "XAGUSD": 23.0}
SPREAD = {"XAUUSD": 0.25, "XAGUSD": 0.02}
NOW = datetime(2026, 1, 15, 12, tzinfo=UTC)
ENV = "HISTDATA_TZ_CONFIG"
B6, NYL = "berlin_minus_6", "ny_local"
V5_BERLIN_LABEL = "berlin_minus_6 (Europe/Berlin -6 h) -> UTC, je Rohmonat geprueft (tz_check v5)"
YEARS = range(2009, 2026)


# Ab v7 ist der Default-Fahrplan ny_local -> berlin_minus_6_utc_midnight (2018-12).
# Diese Tests pruefen die v6-Mechanik (undecided, berlin_minus_6) und laufen daher mit
# einem EXPLIZITEN Fahrplan im v6-Stand (einzelne Tests setzen ihren eigenen).
V6_SCHEDULE = ['{from: "2000-01", rule: ny_local}',
               '{from: "2018-12", rule: undecided, candidates: [ny_local, berlin_minus_6]}',
               '{from: "2021-03", rule: berlin_minus_6}']


@pytest.fixture(scope="session")
def v6_config_file(tmp_path_factory) -> Path:
    p = tmp_path_factory.mktemp("v6cfg") / "histdata_timezone_v6.yaml"
    lines = ["rules: []", "schedule:", "  tick:"] + [f"    - {x}" for x in V6_SCHEDULE] + \
        ["  m1:", '    - {from: "2000-01", rule: fixed_est}']
    p.write_text("\n".join(lines) + "\n")
    return p


@pytest.fixture(autouse=True)
def _v6_config(monkeypatch, v6_config_file):
    monkeypatch.setenv(ENV, str(v6_config_file))


@pytest.fixture
def no_http(monkeypatch):
    import requests

    def boom(*a, **k):
        raise AssertionError("HTTP-Request nicht erlaubt")

    monkeypatch.setattr(requests.Session, "request", boom)
    monkeypatch.setattr(requests.Session, "get", boom)
    monkeypatch.setattr(requests.Session, "post", boom)


# ------------------------------------------------------- referenz (zoneinfo) ---
def last_sunday(y, m):
    d = date(y, m, calendar.monthrange(y, m)[1])
    return d - timedelta(days=(d.weekday() - 6) % 7)


def nth_sunday(y, m, n):
    d = date(y, m, 1)
    d += timedelta(days=(6 - d.weekday()) % 7)
    return d + timedelta(weeks=n - 1)


def at_utc(d: date, h: int) -> pd.Timestamp:
    return pd.Timestamp(datetime(d.year, d.month, d.day, h, tzinfo=UTC))


def windows(y):
    """Die beiden Fenster, in denen ny_local und berlin_minus_6 verschieden stempeln (UTC)."""
    return [(at_utc(nth_sunday(y, 3, 2), 7), at_utc(last_sunday(y, 3), 1)),
            (at_utc(last_sunday(y, 10), 1), at_utc(nth_sunday(y, 11, 1), 6))]


def in_windows(u: pd.DatetimeIndex) -> np.ndarray:
    out = np.zeros(len(u), dtype=bool)
    for y in sorted(set(u.year)):
        for a, b in windows(y):
            out |= np.asarray((u >= a) & (u < b))
    return out


def rms(*a) -> int:
    """Rohzeit-Ziffern -> ms (als UTC gelesen, wie fetch_histdata Rohzeit fuehrt)."""
    return int(datetime(*a, tzinfo=UTC).timestamp() * 1000)


def utc_dt(*a) -> int:
    return int(datetime(*a, tzinfo=UTC).timestamp() * 1000)


def rule(name):
    return fh.TZ_RULES[name]


def stamp(utc: pd.DatetimeIndex, stamping: str) -> pd.DatetimeIndex:
    if stamping == B6:
        return utc.tz_convert(BER).tz_localize(None) - pd.Timedelta(hours=6)
    assert stamping == NYL
    return utc.tz_convert(NYZ).tz_localize(None)


def unique_under(raw: pd.DatetimeIndex, stamping: str) -> np.ndarray:
    """Rohzeit eindeutig nach UTC umrechenbar (zoneinfo/pandas, nicht fh)."""
    if stamping == B6:
        loc = (raw + pd.Timedelta(hours=6)).tz_localize(BER, ambiguous="NaT", nonexistent="NaT")
    else:
        loc = raw.tz_localize(NYZ, ambiguous="NaT", nonexistent="NaT")
    return np.asarray(~loc.isna())


def ms_of(idx: pd.DatetimeIndex) -> np.ndarray:
    return idx.as_unit("ms").asi8


# ------------------------------------------------------------ synthetik ---
def _closed(idx: pd.DatetimeIndex) -> np.ndarray:
    """CME-Metalle, New-York-Kalender: Fr 17:00 .. So 18:00 zu, Mo-Do 17-18 Pause."""
    ny = idx.tz_convert(NYZ)
    wd, h = ny.dayofweek, ny.hour
    return np.asarray(((wd == 4) & (h >= 17)) | (wd == 5) | ((wd == 6) & (h < 18))
                      | ((wd <= 3) & (h == 17)))


def market_utc(lo, hi) -> pd.DatetimeIndex:
    grid = pd.date_range(lo, hi, freq="60s", inclusive="left", tz="UTC")
    openg = grid[~_closed(grid)]
    last = openg[_closed(openg + pd.Timedelta(seconds=60))]
    return openg.append(last + pd.Timedelta(seconds=59)).sort_values()


@functools.lru_cache(maxsize=None)
def raw_month(symbol, y, m, stamping=B6) -> pd.DataFrame:
    """Ticks eines ROH-Monats in Dateireihenfolge: raw (naiv), utc, bid, ask. Nicht veraendern."""
    start = pd.Timestamp(y, m, 1)
    end = start + pd.offsets.MonthBegin(1)
    utc = market_utc(start.tz_localize("UTC") - pd.Timedelta(days=2),
                     end.tz_localize("UTC") + pd.Timedelta(days=2))
    raw = stamp(utc, stamping)
    keep = np.asarray((raw >= start) & (raw < end))
    raw, utc = raw[keep], utc[keep]
    order = np.argsort(ms_of(raw), kind="stable")
    raw, utc = raw[order], utc[order]
    i = (ms_of(utc) // 60_000).astype(float)
    bid = np.round(BASE_PRICE[symbol] * (1 + 0.002 * np.sin(i / 97.0)), 4)
    return pd.DataFrame({"raw": raw, "utc": utc, "bid": bid, "ask": bid + SPREAD[symbol]})


def raw_ms_of(df) -> np.ndarray:
    return ms_of(pd.DatetimeIndex(df["raw"]))


def month_bounds(y, m):
    return rms(y, m, 1), rms(*((y + 1, 1) if m == 12 else (y, m + 1)), 1)


def ym_add(ym, k):
    i = ym[0] * 12 + ym[1] - 1 + k
    return i // 12, i % 12 + 1


def neighbours(symbol, ym, stamping=B6, pad_days=8):
    prev = raw_ms_of(raw_month(symbol, *ym_add(ym, -1), stamping))
    nxt = raw_ms_of(raw_month(symbol, *ym_add(ym, 1), stamping))
    return (prev[prev >= prev.max() - pad_days * DAY_MS], nxt[nxt <= nxt.min() + pad_days * DAY_MS])


def with_extra(df: pd.DataFrame, raws_ms: list[int], after_ms: int) -> pd.DataFrame:
    """Ticks raws_ms in GENAU dieser Reihenfolge in die Datei einfuegen, direkt nach dem
    letzten Tick mit roh <= after_ms (Dateireihenfolge sonst unveraendert)."""
    ms = raw_ms_of(df)
    pos = int(np.searchsorted(ms, after_ms, side="right"))
    p = BASE_PRICE.get("XAUUSD")
    extra = pd.DataFrame({"raw": pd.to_datetime(np.array(raws_ms, dtype=np.int64), unit="ms"),
                          "bid": p, "ask": p + SPREAD["XAUUSD"]})
    base = df[["raw", "bid", "ask"]]
    out = pd.concat([base.iloc[:pos], extra, base.iloc[pos:]], ignore_index=True)
    out["raw"] = pd.DatetimeIndex(out["raw"]).as_unit("ms")
    return out


def tick_csv(df) -> bytes:
    st = pd.DatetimeIndex(df["raw"]).strftime("%Y%m%d %H%M%S%f").str[:-3]
    return ("\n".join(f"{s},{b:.5f},{a:.5f},0" for s, b, a in zip(st, df["bid"], df["ask"]))
            + "\n").encode()


def zip_of(symbol, y, m, df) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(f"DAT_ASCII_{symbol}_T_{y}{m:02d}.csv", tick_csv(df))
        zf.writestr(f"DAT_ASCII_{symbol}_T_{y}{m:02d}.txt", "HistData.com status report\n")
    return buf.getvalue()


@functools.lru_cache(maxsize=None)
def tick_zip(symbol, y, m, stamping=B6) -> bytes:
    return zip_of(symbol, y, m, raw_month(symbol, y, m, stamping))


# --------------------------------------------------------------- pfade ---
def raw_zip_path(root: Path, sym, y, m) -> Path:
    return (root / "metals" / "raw" / "source=histdata" / f"symbol={sym}" / "mode=tick"
            / f"HISTDATA_COM_ASCII_{sym}_T{y}{m:02d}.zip")


def qmarker_path(root: Path, sym, y, m) -> Path:
    p = raw_zip_path(root, sym, y, m)
    return p.with_name(p.name + ".quarantine")


def store_file(root: Path, sym, y, m) -> Path:
    return (root / "metals" / "source=histdata" / f"symbol={sym}" / f"year={y}"
            / f"{sym}-1m-{y}-{m:02d}.parquet")


def month_report_file(root: Path, sym, y, m) -> Path:
    return root / "metals" / "reports" / f"{sym}-histdata-1m-{y}-{m:02d}.json"


def seed_raw(root, months, stamping=B6, sym="XAUUSD"):
    for ym in months:
        st = stamping(ym) if callable(stamping) else stamping
        write_raw(root, ym, tick_zip(sym, *ym, st), sym)


def write_raw(root, ym, blob, sym="XAUUSD"):
    p = raw_zip_path(root, sym, *ym)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(blob)


def stale_files(root):
    d = root / "metals" / "stale"
    return sorted(d.rglob("*.stale")) if d.exists() else []


def markers(root):
    return sorted(root.rglob("*.quarantine"))


def label_of(p: Path) -> str:
    v = (pq.read_schema(p).metadata or {}).get(b"source_timezone")
    return v.decode() if v else ""


# ------------------------------------------------------------ http-mock ---
PAGE = """<html><body>
<form id="file_down" name="file_down" method="POST" action="/get.php">
  <input type="hidden" name="tk" id="tk" value="tok-{period}" />
  <input type="hidden" name="date" id="date" value="{year}" />
  <input type="hidden" name="datemonth" id="datemonth" value="{period}" />
  <input type="hidden" name="platform" id="platform" value="ASCII" />
  <input type="hidden" name="timeframe" id="timeframe" value="T" />
  <input type="hidden" name="fxpair" id="fxpair" value="{pair}" />
</form></body></html>"""


class Resp:
    def __init__(self, status=200, content=b""):
        self.status_code, self.content, self.headers = status, content, {}

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
    """Fake-HistData (nur Tick-Monatsdateien): files[(SYM, 'YYYYMM')] -> ZIP; sonst leer."""

    def __init__(self, files=None):
        self.files, self.calls = dict(files or {}), []

    def get(self, url, timeout=None, **kw):
        self.calls.append(("GET", url))
        m = re.search(r"/ascii/tick-data-quotes/([a-z]+)/(\d{4})/(\d{1,2})$", url)
        assert m, url
        pair, year, month = m.groups()
        return Resp(200, PAGE.format(period=f"{year}{int(month):02d}", year=year,
                                     pair=pair.upper()).encode())

    def post(self, url, data=None, headers=None, timeout=None, **kw):
        self.calls.append(("POST", url))
        return Resp(200, self.files.get((data["fxpair"], data["datemonth"]), b""))

    @property
    def requests(self):
        return len(self.calls)


def do_run(root, site, start, end=None, sym="XAUUSD"):
    clock = Clock()
    return fh.run(sym, date(*start, 1), date(*(end or start), 1), root, mode="tick",
                  now=lambda: NOW, pause=0.0, attempts=2, backoff_base=1.0, max_backoff=10.0,
                  jitter=0.0, sleep=clock.sleep, clock=clock, wall=clock, session=site)


def summary_get(rep, key):
    s = rep.get("summary") or {}
    return s[key] if key in s else rep[key]


def bars_ms(path: Path) -> np.ndarray:
    return ms_of(pd.DatetimeIndex(pd.read_parquet(path)["ts"]))


def expected_minutes(sym, ym, raws) -> set:
    """UTC-Minuten des UTC-Monats ym aus den Generator-UTC-Zeiten der Rohmonate."""
    lo, hi = month_bounds(*ym)
    out = set()
    for r, st in raws.items():
        u = ms_of(pd.DatetimeIndex(raw_month(sym, *r, st)["utc"]).floor("min"))
        out |= set(int(x) for x in u[(u >= lo) & (u < hi)])
    return out


def find_key(obj, key):
    """Alle Werte zu `key` irgendwo in einer JSON-Struktur."""
    out = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k == key:
                out.append(v)
            out += find_key(v, key)
    elif isinstance(obj, list):
        for v in obj:
            out += find_key(v, key)
    return out


def call_main(argv):
    try:
        return fh.main(argv)
    except SystemExit as e:
        return e.code


# ==================================================================== 1 ====
def test_reference_windows_are_exactly_where_stampings_differ_2009_2025():
    """Selbsttest der Fakten (nur zoneinfo/pandas): Stempelung ny_local und
    berlin_minus_6 desselben UTC-Zeitpunkts weicht genau in den zwei Fenstern ab."""
    u = pd.date_range("2009-01-01", "2026-01-01", freq="20min", inclusive="left",
                      tz="UTC") + pd.Timedelta(minutes=7, milliseconds=123)
    differ = np.asarray(stamp(u, NYL) != stamp(u, B6))
    win = in_windows(u)
    assert (differ == win).all()
    for y in YEARS:
        for a, b in windows(y):
            assert a < b and (b - a) >= pd.Timedelta(days=6), (y, a, b)   # nie leer


def test_fh_converts_identically_outside_and_differently_inside_windows():
    u = pd.date_range("2009-01-01", "2026-01-01", freq="20min", inclusive="left",
                      tz="UTC") + pd.Timedelta(minutes=7, milliseconds=123)
    want = ms_of(u)
    win = in_windows(u)
    for st in (NYL, B6):
        raw = stamp(u, st)
        ok = unique_under(raw, st)
        assert (~ok).sum() > 0                                        # Rueckstellungsstunden
        got = fh.raw_to_utc_ms(ms_of(raw[ok]), rule(st))
        assert np.array_equal(got, want[ok]), st
        # dieselben Rohdaten unter der ANDEREN Regel: gleich ausserhalb, anders innerhalb
        other = B6 if st == NYL else NYL
        ok2 = ok & unique_under(raw, other)
        got2 = fh.raw_to_utc_ms(ms_of(raw[ok2]), rule(other))
        same = got2 == want[ok2]
        assert np.array_equal(same, ~win[ok2]), st
        assert (~same).sum() > 17 * 2 * 6 * 72 * 0.9                  # wirklich je Fenster
        # Abweichung im Fenster immer genau 1 h
        assert set(np.unique(np.abs(got2[~same] - want[ok2][~same]))) == {HOUR_MS}


@pytest.mark.parametrize("year", list(YEARS))
def test_jan_jul_and_apr_sep_raw_months_convert_identically(year):
    for m in (1, 4, 5, 6, 7, 8, 9):
        lo, hi = month_bounds(year, m)
        raw = np.arange(lo, hi, 13 * MIN_MS + 17, dtype=np.int64)
        a = fh.raw_to_utc_ms(raw, rule(NYL))
        b = fh.raw_to_utc_ms(raw, rule(B6))
        assert np.array_equal(a, b), (year, m)
    for m in (3, 10, 11):            # Gegenprobe: dort verschieden
        lo, hi = month_bounds(year, m)
        raw = np.arange(lo, hi, 13 * MIN_MS + 17, dtype=np.int64)
        idx = pd.to_datetime(raw, unit="ms")
        ok = unique_under(idx, NYL) & unique_under(idx, B6)
        assert (fh.raw_to_utc_ms(raw[ok], rule(NYL)) != fh.raw_to_utc_ms(raw[ok], rule(B6))).any()


def test_v6_schedule_config(v6_config_file):
    """Der v6-Fahrplan (explizit, s. _v6_config) wird weiter geladen: undecided-Eintrag."""
    assert fh.TZ_CHECK_VERSION == 7                  # aktuelle Version (v6-Mechanik bleibt)
    cfg = fh.load_tz_config()
    assert Path(cfg.path) == v6_config_file
    sched = cfg.schedule["tick"]
    assert [ym for ym, _ in sched] == [(2000, 1), (2018, 12), (2021, 3)]
    assert sched[0][1] == rule(NYL) and sched[2][1] == rule(B6)
    assert isinstance(sched[1][1], fh.UndecidedRule)
    assert sched[1][1].name == "undecided" and sched[1][1].candidates == (rule(NYL), rule(B6))
    assert [ym for ym, _ in cfg.schedule["m1"]] == [(2000, 1)]
    assert cfg.schedule["m1"][0][1] == rule("fixed_est")
    for ym in ((2000, 1), (2009, 3), (2015, 3), (2018, 11)):
        assert fh.rule_for("tick", ym, cfg) == rule(NYL), ym
    for ym in ((2018, 12), (2019, 3), (2019, 7), (2020, 10), (2021, 2)):
        r = fh.rule_for("tick", ym, cfg)
        assert isinstance(r, fh.UndecidedRule), ym
        assert r.name == "undecided"
        assert isinstance(r.candidates, tuple)
        assert all(isinstance(c, fh.TzRule) for c in r.candidates)
        assert [c.name for c in r.candidates] == [NYL, B6]
        assert tuple(r.candidates) == (rule(NYL), rule(B6))
    for ym in ((2021, 3), (2021, 10), (2024, 3), (2025, 11)):
        assert fh.rule_for("tick", ym, cfg) == rule(B6), ym


def test_rule_undecided_is_not_a_format_error():
    assert issubclass(fh.RuleUndecided, Exception)
    assert not issubclass(fh.RuleUndecided, fh.FormatError)


@pytest.mark.parametrize("st", [NYL, B6])
def test_undecided_rule_conversion(st):
    und = fh.rule_for("tick", (2019, 7))
    assert isinstance(und, fh.UndecidedRule)
    # identischer Monat (Juli): Werte wie beide Kandidaten
    jul = raw_month("XAUUSD", 2019, 7, st)
    got = fh.raw_to_utc_ms(raw_ms_of(jul), und)
    assert got.dtype == np.int64
    assert np.array_equal(got, ms_of(pd.DatetimeIndex(jul["utc"])))
    # Maerz nur vor dem US-Beginn (10.03.2019): beide gleich -> Werte
    mar = raw_month("XAUUSD", 2019, 3, st)
    early = raw_ms_of(mar)
    early = early[early < rms(2019, 3, 9)]
    assert len(early) > 1000
    assert np.array_equal(fh.raw_to_utc_ms(early, und),
                          ms_of(pd.DatetimeIndex(mar["utc"]))[:len(early)])
    # ganzer Maerz mit Ticks im Fenster -> RuleUndecided (nie raten)
    with pytest.raises(fh.RuleUndecided):
        fh.raw_to_utc_ms(raw_ms_of(mar), und)
    # ein einziger Tick im Fenster genuegt
    with pytest.raises(fh.RuleUndecided):
        fh.raw_to_utc_ms(np.array([rms(2019, 3, 5, 12), rms(2019, 3, 13, 12)], dtype=np.int64), und)
    # Okt/Nov ebenso
    with pytest.raises(fh.RuleUndecided):
        fh.raw_to_utc_ms(raw_ms_of(raw_month("XAUUSD", 2020, 10, st)), und)


# ==================================================================== 2 ====
def test_run_undecided_identical_month_is_written(tmp_path):
    seed_raw(tmp_path, [(2019, 5), (2019, 6), (2019, 7), (2019, 8)], stamping=NYL)
    site = Site()
    rep = do_run(tmp_path, site, (2019, 7))
    assert rep["exit_code"] == 0, rep.get("quarantined") or rep
    assert site.requests == 0
    out = store_file(tmp_path, "XAUUSD", 2019, 7)
    assert set(int(x) for x in bars_ms(out)) == expected_minutes(
        "XAUUSD", (2019, 7), {(2019, 6): NYL, (2019, 7): NYL})
    assert "2019-07" in summary_get(rep, "months_written")
    assert not summary_get(rep, "months_rule_undecided")
    assert f"v{fh.TZ_CHECK_VERSION}" in label_of(out)
    assert markers(tmp_path) == []


def test_run_undecided_march_is_held_without_marker(tmp_path):
    seed_raw(tmp_path, [(2019, 1), (2019, 4)], stamping=NYL)
    site = Site({("XAUUSD", "201902"): tick_zip("XAUUSD", 2019, 2, NYL),
                 ("XAUUSD", "201903"): tick_zip("XAUUSD", 2019, 3, NYL)})
    rep = do_run(tmp_path, site, (2019, 3))
    assert rep["exit_code"] == 4, rep
    assert site.requests == 4                                  # nur die zwei fehlenden Rohmonate
    assert "2019-03" in summary_get(rep, "months_rule_undecided")
    assert not store_file(tmp_path, "XAUUSD", 2019, 3).exists()
    assert markers(tmp_path) == []
    assert "2019-03" not in summary_get(rep, "months_written")
    # zweiter Lauf: alles im Cache, keine Requests, weiter zurueckgehalten
    site2 = Site()
    rep2 = do_run(tmp_path, site2, (2019, 3))
    assert rep2["exit_code"] == 4
    assert site2.requests == 0
    assert "2019-03" in summary_get(rep2, "months_rule_undecided")
    assert markers(tmp_path) == []
    assert not store_file(tmp_path, "XAUUSD", 2019, 3).exists()


def test_run_undecided_gaps_take_precedence(tmp_path):
    """Exit 3 (Luecke: Rohmonat 2019-04 leer/unbestaetigt) geht vor 4 (undecided)."""
    seed_raw(tmp_path, [(2019, 1), (2019, 2), (2019, 3)], stamping=NYL)
    rep = do_run(tmp_path, Site(), (2019, 3), (2019, 4))
    assert rep["exit_code"] == 3, rep
    assert "2019-03" in summary_get(rep, "months_rule_undecided")
    assert markers(tmp_path) == []


@pytest.mark.parametrize("ym,st", [((2015, 3), NYL), ((2024, 3), B6)])
def test_run_decided_march_written_correctly(tmp_path, ym, st):
    seed_raw(tmp_path, [ym_add(ym, k) for k in (-2, -1, 0, 1)], stamping=st)
    site = Site()
    rep = do_run(tmp_path, site, ym)
    assert rep["exit_code"] == 0, rep.get("quarantined") or rep
    assert site.requests == 0
    out = store_file(tmp_path, "XAUUSD", *ym)
    ts = set(int(x) for x in bars_ms(out))
    assert ts == expected_minutes("XAUUSD", ym, {ym_add(ym, -1): st, ym: st})
    y = ym[0]
    us, eu = nth_sunday(y, 3, 2), last_sunday(y, 3)
    # Mittwoch im Fenster: Pause 21-22 UTC (17-18 EDT), egal wie gestempelt
    wed = us + timedelta(days=3)
    assert utc_dt(y, 3, wed.day, 20, 59) in ts and utc_dt(y, 3, wed.day, 22, 0) in ts
    assert not any(utc_dt(y, 3, wed.day, 21) <= x < utc_dt(y, 3, wed.day, 22) for x in ts)
    assert wed < eu
    assert st in label_of(out) and f"v{fh.TZ_CHECK_VERSION}" in label_of(out)


# ==================================================================== 3 ====
FIRST_FOLD = [(20, 10), (20, 20), (20, 30), (20, 40), (20, 50), (20, 59)]
SECOND_FOLD = [(20, 5), (20, 15), (20, 25), (20, 35), (20, 45), (20, 55)]
N_STRAY = len(FIRST_FOLD) + len(SECOND_FOLD)
FALL_SATURDAYS = {(2024, 10): date(2024, 10, 26), (2021, 10): date(2021, 10, 30)}


def stray_ms(sat: date) -> list[int]:
    return [rms(sat.year, sat.month, sat.day, h, mi) for h, mi in FIRST_FOLD + SECOND_FOLD]


def weekend_reversal_month(ym, sym="XAUUSD") -> pd.DataFrame:
    sat = FALL_SATURDAYS[ym]
    assert sat == last_sunday(*ym) - timedelta(days=1)
    return with_extra(raw_month(sym, *ym, B6), stray_ms(sat), rms(sat.year, sat.month, sat.day, 20))


def seed_weekend_case(root, ym, sym="XAUUSD"):
    seed_raw(root, [ym_add(ym, k) for k in (-2, -1, 1)])
    write_raw(root, ym, zip_of(sym, *ym, weekend_reversal_month(ym, sym)), sym)


def test_weekend_generator_selftest():
    df = weekend_reversal_month((2024, 10))
    ms = raw_ms_of(df)
    d = np.diff(ms)
    big = np.nonzero(d <= -30 * MIN_MS)[0]
    assert len(big) == 1
    i = int(big[0])
    assert (ms[i], ms[i + 1]) == (rms(2024, 10, 26, 20, 59), rms(2024, 10, 26, 20, 5))
    # vor/nach den Streuticks: Freitagsschluss roh 16:59:59, Sonntagsoeffnung roh 17:00
    j = i - len(FIRST_FOLD) + 1
    assert ms[j - 1] == rms(2024, 10, 25, 16, 59, 59)
    assert ms[i + len(SECOND_FOLD) + 1] == rms(2024, 10, 27, 17, 0)
    # alle Streuticks in der doppelten Berliner Stunde (Referenz zoneinfo)
    idx = pd.to_datetime(np.array(stray_ms(date(2024, 10, 26))), unit="ms")
    assert not unique_under(idx, B6).any()
    assert unique_under(idx, NYL).all()


@pytest.mark.parametrize("ym", sorted(FALL_SATURDAYS))
def test_check_timezone_reports_weekend_reversal(ym):
    sat = FALL_SATURDAYS[ym]
    ms = raw_ms_of(weekend_reversal_month(ym))
    rep = fh.check_timezone(ms, "XAUUSD", month_bounds(*ym), mode="tick", rule=rule(B6),
                            neighbours=neighbours("XAUUSD", ym))
    assert rep["status"] == "failed"
    lr = rep["large_reversals"]
    assert len(lr) == 1
    r = lr[0]
    day = f"{sat:%Y-%m-%d}"
    assert r["at_raw"].startswith(f"{day} 20:59")
    assert r["back_to_raw"].startswith(f"{day} 20:05")
    assert r["minutes"] == 54.0                          # 20:59 -> 20:05
    assert r["n_ticks"] == N_STRAY
    assert r["first_raw"].startswith(f"{day} 20:05:00")   # min der betroffenen Rohzeiten
    assert r["last_raw"].startswith(f"{day} 20:59:00")    # max
    assert r["weekend_closed"] is True
    assert rep["reversal_ticks_total"] == N_STRAY
    rw = rep["releasable_weekend"]
    assert isinstance(rw, dict)
    assert rw["reversal_ticks"] == N_STRAY
    assert rw["impossible_hour_ticks"] == N_STRAY
    assert rw["raw_ranges"]
    assert all(day in str(x) for x in rw["raw_ranges"])


def test_weekend_closed_raw_clock_exact_edges():
    """Geschlossenes Wochenende nach ROHuhr: Fr >= 18:00, ganzer Samstag, So < 16:00."""
    cases = [((2024, 7, 11, 23, 59, 59, 999_000), False),   # Do
             ((2024, 7, 12, 17, 59, 0), False), ((2024, 7, 12, 17, 59, 59, 999_000), False),
             ((2024, 7, 12, 18, 0, 0), True),
             ((2024, 7, 13, 0, 0, 0), True), ((2024, 7, 13, 12, 0, 0), True),
             ((2024, 7, 13, 23, 59, 59, 999_000), True),
             ((2024, 7, 14, 0, 0, 0), True), ((2024, 7, 14, 15, 59, 0), True),
             ((2024, 7, 14, 15, 59, 59, 999_000), True),
             ((2024, 7, 14, 16, 0, 0), False), ((2024, 7, 14, 18, 0, 0), False),
             ((2024, 7, 15, 3, 0, 0), False)]                  # Mo
    raw = np.array([rms(*c) for c, _ in cases], dtype=np.int64)
    got = np.asarray(fh.weekend_closed_raw(raw))
    assert got.dtype == bool
    assert list(got) == [want for _, want in cases]


# (Ticks in Dateireihenfolge als (Tag, h, min) im Juli 2024, Minuten, freigebbar)
WEEKEND_EDGES = {
    "fri_1759_open": ([(12, 18, 40), (12, 17, 59)], 41.0, False),
    "fri_1800_closed": ([(12, 18, 40), (12, 18, 0)], 40.0, True),
    "sat_whole_day": ([(13, 23, 59), (13, 0, 0)], 1439.0, True),
    "sun_1559_closed": ([(14, 15, 59), (14, 15, 19)], 40.0, True),
    "sun_1600_open": ([(14, 16, 0), (14, 15, 20)], 40.0, False),
}


@pytest.mark.parametrize("case", sorted(WEEKEND_EDGES))
def test_weekend_reversal_releasable_exactly_at_raw_clock_edges(case):
    """Beobachtbar ueber check_timezone: ein Ruecksprung ist nur freigebbar, wenn ALLE
    betroffenen Ticks (roh in [back_to, at]) im geschlossenen Wochenende liegen."""
    ticks, minutes, closed = WEEKEND_EDGES[case]
    ms = [rms(2024, 7, d, h, mi) for d, h, mi in ticks]
    df = with_extra(raw_month("XAUUSD", 2024, 7, B6), ms, min(ms) - 1)
    rep = fh.check_timezone(raw_ms_of(df), "XAUUSD", month_bounds(2024, 7), mode="tick",
                            rule=rule(B6), neighbours=neighbours("XAUUSD", (2024, 7)))
    lr = rep["large_reversals"]
    assert len(lr) == 1 and lr[0]["minutes"] == minutes
    assert lr[0]["n_ticks"] == 2
    assert lr[0]["weekend_closed"] is closed
    assert rep["reversal_ticks_total"] == 2
    if closed:
        assert rep["releasable_weekend"]["reversal_ticks"] == 2
        assert rep["releasable_weekend"]["impossible_hour_ticks"] == 0
    else:
        assert "releasable_weekend" not in rep


def test_reversal_first_last_raw_are_min_max_of_affected_ticks():
    """Dateireihenfolge 20:30, 21:00, 20:00, 20:40 (Sa): first_raw = 20:00, last_raw = 21:00."""
    ms = [rms(2024, 7, 13, h, mi) for h, mi in ((20, 30), (21, 0), (20, 0), (20, 40))]
    df = with_extra(raw_month("XAUUSD", 2024, 7, B6), ms, min(ms) - 1)
    rep = fh.check_timezone(raw_ms_of(df), "XAUUSD", month_bounds(2024, 7), mode="tick",
                            rule=rule(B6), neighbours=neighbours("XAUUSD", (2024, 7)))
    lr = rep["large_reversals"]
    assert len(lr) == 1
    r = lr[0]
    assert r["at_raw"].startswith("2024-07-13 21:00:00")
    assert r["back_to_raw"].startswith("2024-07-13 20:00:00")
    assert r["minutes"] == 60.0
    assert r["n_ticks"] == 4
    assert r["first_raw"].startswith("2024-07-13 20:00:00")
    assert r["last_raw"].startswith("2024-07-13 21:00:00")
    assert r["weekend_closed"] is True


@pytest.mark.parametrize("when,closed", [
    ((2024, 7, 12, 17, 30), False),      # Fr 17:30 roh: noch kein geschlossenes Wochenende
    ((2024, 7, 12, 18, 10), True),       # Fr >= 18:00
    ((2024, 7, 13, 3, 0), True),         # Samstag
    ((2024, 7, 13, 21, 0), True),
    ((2024, 7, 14, 15, 0), True),        # So < 16:00
    ((2024, 7, 14, 16, 30), False),      # So 16:30: zu nah an der Oeffnung
])
def test_weekend_closed_by_raw_clock(when, closed):
    """Kleiner 58-min-Ruecksprung (roh T, T+59, T+1) im geschlossenen Markt (Juli, ohne
    Zeitumstellung): weekend_closed/releasable nur Fr >= 18:00, Sa, So < 16:00 roh."""
    t = rms(*when)
    df = with_extra(raw_month("XAUUSD", 2024, 7, B6), [t, t + 59 * MIN_MS, t + MIN_MS], t - 1)
    rep = fh.check_timezone(raw_ms_of(df), "XAUUSD", month_bounds(2024, 7), mode="tick",
                            rule=rule(B6), neighbours=neighbours("XAUUSD", (2024, 7)))
    lr = rep["large_reversals"]
    assert len(lr) == 1 and lr[0]["minutes"] == 58.0
    assert lr[0]["weekend_closed"] is closed
    assert lr[0]["n_ticks"] == 2
    assert rep["reversal_ticks_total"] == 2
    if closed:
        rw = rep["releasable_weekend"]
        assert rw["reversal_ticks"] == 2 and rw["impossible_hour_ticks"] == 0
    else:
        assert "releasable_weekend" not in rep


def test_reversal_on_wednesday_is_never_releasable():
    df = raw_month("XAUUSD", 2024, 10, B6)
    wed = [rms(2024, 10, 23, 11, mi) for mi in range(0, 60, 10)]
    df = with_extra(df, wed, rms(2024, 10, 23, 12))
    rep = fh.check_timezone(raw_ms_of(df), "XAUUSD", month_bounds(2024, 10), mode="tick",
                            rule=rule(B6), neighbours=neighbours("XAUUSD", (2024, 10)))
    assert rep["status"] == "failed"
    assert [r["weekend_closed"] for r in rep["large_reversals"]] == [False]
    assert rep["large_reversals"][0]["minutes"] == 60.0
    assert "releasable_weekend" not in rep
    # zusammen mit den Wochenend-Streuticks: auch nicht freigebbar (nicht ALLE im Wochenende)
    both = with_extra(weekend_reversal_month((2024, 10)), wed, rms(2024, 10, 23, 12))
    rep2 = fh.check_timezone(raw_ms_of(both), "XAUUSD", month_bounds(2024, 10), mode="tick",
                             rule=rule(B6), neighbours=neighbours("XAUUSD", (2024, 10)))
    assert sorted(r["weekend_closed"] for r in rep2["large_reversals"]) == [False, True]
    assert "releasable_weekend" not in rep2


@pytest.mark.parametrize("ym", sorted(FALL_SATURDAYS))
def test_weekend_reversal_quarantine_accept_and_write(tmp_path, ym, monkeypatch, capsys):
    sat = FALL_SATURDAYS[ym]
    seed_weekend_case(tmp_path, ym)
    site = Site()
    rep = do_run(tmp_path, site, ym)
    assert rep["exit_code"] == 4, rep
    assert site.requests == 0
    assert not store_file(tmp_path, "XAUUSD", *ym).exists()
    q = qmarker_path(tmp_path, "XAUUSD", *ym)
    assert q.exists(), markers(tmp_path)
    info = json.loads(q.read_text())
    assert info["accepted"] is False
    rw = info["releasable_weekend"]
    assert isinstance(rw, dict) and rw["reversal_ticks"] == N_STRAY

    # CLI: freigeben (nur Marker, keine Downloads)
    import requests

    def boom(*a, **k):
        raise AssertionError("HTTP bei --accept-weekend-ticks")

    monkeypatch.setattr(requests.Session, "get", boom)
    monkeypatch.setattr(requests.Session, "post", boom)
    monkeypatch.setattr(requests.Session, "request", boom)
    monkeypatch.setenv("MARKETDATA_DATA_ROOT", str(tmp_path))
    capsys.readouterr()
    code = call_main(["--symbol", "XAUUSD", "--accept-weekend-ticks", f"{ym[0]}-{ym[1]:02d}"])
    assert code == 0
    out = capsys.readouterr()
    text = out.out + out.err
    assert f"{sat:%Y-%m-%d}" in text and "20:05" in text and "20:59" in text, text
    assert str(N_STRAY) in text
    assert json.loads(q.read_text())["accepted"] is True
    assert not store_file(tmp_path, "XAUUSD", *ym).exists()            # CLI baut nichts

    # naechster Lauf: genau die Streuticks verworfen, Rest geprueft und geschrieben
    site = Site()
    rep = do_run(tmp_path, site, ym)
    assert rep["exit_code"] == 0, rep.get("quarantined") or rep
    assert site.requests == 0
    out_f = store_file(tmp_path, "XAUUSD", *ym)
    ts = set(int(x) for x in bars_ms(out_f))
    assert ts == expected_minutes("XAUUSD", ym, {ym_add(ym, -1): B6, ym: B6})
    # kein Bar im geschlossenen Wochenende (Fr 17:00 EDT .. So 18:00 EDT)
    fri = sat - timedelta(days=1)
    lo = utc_dt(fri.year, fri.month, fri.day, 21)
    hi = utc_dt(sat.year, sat.month, sat.day + 1, 22)
    assert not any(lo <= x < hi for x in ts)
    assert utc_dt(fri.year, fri.month, fri.day, 20, 59) in ts and hi in ts
    mrep = json.loads(month_report_file(tmp_path, "XAUUSD", *ym).read_text())
    assert mrep["dropped_ticks"] == N_STRAY, mrep
    draw = mrep["dropped_raw"]
    assert isinstance(draw, list) and draw
    strays = {f"{sat:%Y-%m-%d} {h:02d}:{mi:02d}:00" for h, mi in FIRST_FOLD + SECOND_FOLD}
    assert all(str(x)[:19] in strays for x in draw), draw


def test_wednesday_reversal_cannot_be_accepted(tmp_path, monkeypatch, capsys):
    ym = (2024, 7)
    seed_raw(tmp_path, [(2024, 5), (2024, 6), (2024, 8)])
    wed = [rms(2024, 7, 10, 11, mi) for mi in range(0, 60, 10)]
    write_raw(tmp_path, ym, zip_of("XAUUSD", *ym, with_extra(raw_month("XAUUSD", *ym, B6), wed,
                                                              rms(2024, 7, 10, 12))))
    rep = do_run(tmp_path, Site(), ym)
    assert rep["exit_code"] == 4, rep
    q = qmarker_path(tmp_path, "XAUUSD", *ym)
    assert q.exists()
    before = json.loads(q.read_text())
    assert before["accepted"] is False
    assert "releasable_weekend" not in before
    monkeypatch.setenv("MARKETDATA_DATA_ROOT", str(tmp_path))
    code = call_main(["--symbol", "XAUUSD", "--accept-weekend-ticks", "2024-07"])
    assert code == 2
    after = json.loads(q.read_text())
    assert after["accepted"] is False
    site = Site()
    rep = do_run(tmp_path, site, ym)
    assert rep["exit_code"] == 4 and site.requests == 0
    assert not store_file(tmp_path, "XAUUSD", *ym).exists()
    # kein Marker -> ebenfalls Exit 2
    assert call_main(["--symbol", "XAUUSD", "--accept-weekend-ticks", "2024-08"]) == 2


def test_evaluate_raw_month_drop_releasable_weekend(tmp_path, no_http):
    seed_weekend_case(tmp_path, (2024, 10))
    plain = fh.evaluate_raw_month(tmp_path, "XAUUSD", (2024, 10), mode="tick", rule=B6)
    assert plain["status"] == "failed"
    assert plain["releasable_weekend"]
    dropped = fh.evaluate_raw_month(tmp_path, "XAUUSD", (2024, 10), mode="tick", rule=B6,
                                    drop_releasable_weekend=True)
    assert dropped["status"] == "ok", dropped.get("reason")
    assert dropped["dropped_weekend_ticks"] == N_STRAY
    assert dropped["releasable_weekend"]["reversal_ticks"] == N_STRAY
    ny = fh.evaluate_raw_month(tmp_path, "XAUUSD", (2024, 10), mode="tick", rule=NYL,
                               drop_releasable_weekend=True)
    assert ny["status"] == "failed"                       # 28.-31.10 bleiben verschoben
    assert markers(tmp_path) == []


# ==================================================================== 6 ====
def test_v5_berlin_store_file_of_2015_03_is_retired_and_rebuilt_as_ny_local(tmp_path):
    seed_raw(tmp_path, [(2015, 1), (2015, 2), (2015, 3), (2015, 4)], stamping=NYL)
    assert do_run(tmp_path, Site(), (2015, 3))["exit_code"] == 0
    out = store_file(tmp_path, "XAUUSD", 2015, 3)
    good = pd.read_parquet(out)
    assert set(int(x) for x in bars_ms(out)) == expected_minutes(
        "XAUUSD", (2015, 3), {(2015, 2): NYL, (2015, 3): NYL})
    # so haette v5 (berlin_minus_6) dieselben ny_local-Rohdaten umgerechnet
    t = pq.read_table(out)
    df = t.to_pandas()
    raw = df["ts"].dt.tz_convert(NYZ).dt.tz_localize(None)
    df["ts"] = (raw + pd.Timedelta(hours=6)).dt.tz_localize(BER).dt.tz_convert("UTC").dt.as_unit("ms")
    moved = df["ts"] != good["ts"]
    a, b = windows(2015)[0]
    win = (good["ts"] >= a) & (good["ts"] < b)
    assert moved.sum() > 1000 and (moved == win).all()
    assert ((df["ts"] - good["ts"])[moved] == pd.Timedelta(hours=1)).all()
    meta = dict(t.schema.metadata)
    meta[b"source_timezone"] = V5_BERLIN_LABEL.encode()
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False).replace_schema_metadata(meta), out)
    site = Site()
    rep = do_run(tmp_path, site, (2015, 3))
    assert rep["exit_code"] == 0 and site.requests == 0, rep
    st = stale_files(tmp_path)
    assert len(st) == 1 and "XAUUSD-1m-2015-03.parquet" in st[0].name
    assert "/metals/stale/" in str(st[0]).replace("\\", "/")
    pd.testing.assert_series_equal(pd.read_parquet(st[0])["ts"], df["ts"], check_names=False)
    pd.testing.assert_frame_equal(pd.read_parquet(out), good)
    lab = label_of(out)
    assert "ny_local" in lab and f"v{fh.TZ_CHECK_VERSION}" in lab and "berlin" not in lab
    rep = do_run(tmp_path, site, (2015, 3))
    assert rep["exit_code"] == 0 and site.requests == 0 and len(stale_files(tmp_path)) == 1


# ==================================================================== 5 ====
def periods_of(years, months=(3, 10, 11)):
    return [f"{y}-{m:02d}" for y in years for m in months]


def seed_years(root, years, stamping, months=(3, 10, 11), sym="XAUUSD"):
    seed_raw(root, [(y, m) for y in years for m in months], stamping=stamping, sym=sym)


def write_cfg(path: Path, tick_lines) -> Path:
    lines = ["rules: []", "schedule:", "  tick:"] + [f"    - {x}" for x in tick_lines] + \
        ["  m1:", '    - {from: "2000-01", rule: fixed_est}']
    path.write_text("\n".join(lines) + "\n")
    return path


def survey_cli(tmp_path, monkeypatch, capsys, root, periods=None, extra=()):
    monkeypatch.setenv("MARKETDATA_DATA_ROOT", str(root))
    j, md = tmp_path / "survey.json", tmp_path / "survey.md"
    argv = ["--symbols", "XAUUSD", "--offline", "--json", str(j), "--markdown", str(md)]
    if periods is not None:
        argv += ["--periods", *periods]
    capsys.readouterr()
    code = sv.main(argv + list(extra))
    out = capsys.readouterr().out
    res = json.loads(j.read_text())
    assert res["exit_code"] == code
    return code, res, out, (md.read_text() if md.exists() else None)


def sched(res):
    return [(e["from"], e["rule"]) for e in res["schedule"]]


def month_rows(res):
    return {m["month"]: m for m in res["months"] if m.get("symbol", "XAUUSD") == "XAUUSD"}


def test_survey_preset_full_plan(tmp_path, monkeypatch, capsys, no_http):
    root = tmp_path / "data"
    root.mkdir()
    code, res, out, _ = survey_cli(tmp_path, monkeypatch, capsys, root, extra=["--preset", "full"])
    months = set(month_rows(res))
    first = min(ym[0] for ym in fh.HISTORY_START.values())
    assert first == 2009
    # ab v7 gehoeren Kontrollmonate (mindestens 07) zum Preset (Kontroll-Ausschluss, s. v7)
    trans = {f"{y}-{m:02d}" for y in range(first, 2026) for m in (3, 10, 11)}
    assert trans | {f"{y}-07" for y in range(first, 2026)} <= months
    kinds = {m["month"]: m["kind"] for m in res["months"]}
    assert all(kinds[m] == "transition" for m in trans)
    assert all(kinds[m] == "control" for m in months - trans)
    assert code == 3                                     # offline, nichts im Cache
    assert res["schedule"] == []


def test_survey_one_switch_with_open_years_proposes_undecided_block(tmp_path, monkeypatch, capsys,
                                                                     no_http):
    root = tmp_path / "data"
    seed_years(root, (2017, 2018), NYL)
    seed_years(root, (2021, 2022), B6)
    code, res, out, md = survey_cli(tmp_path, monkeypatch, capsys, root,
                                    periods_of(range(2017, 2023)))
    rows = month_rows(res)
    for y in (2017, 2018):
        assert rows[f"{y}-03"]["decisive"] and rows[f"{y}-03"]["winner"] == NYL
        assert rows[f"{y}-10"]["winner"] == NYL
    for y in (2021, 2022):
        assert rows[f"{y}-03"]["decisive"] and rows[f"{y}-03"]["winner"] == B6
        assert rows[f"{y}-10"]["winner"] == B6
    for y in (2019, 2020):
        assert not rows[f"{y}-03"]["evaluated"]
    assert code == 3                                     # 2019/2020 fehlen (offline), kein Konflikt
    assert sched(res) == [("2000-01", NYL), ("2018-12", "undecided"), ("2021-03", B6)]
    und = res["schedule"][1]
    assert list(und["candidates"]) == [NYL, B6]
    assert re.search(r'from: "2018-12", rule: undecided, candidates: \[ny_local, berlin_minus_6\]',
                     out), out
    assert re.search(r'from: "2021-03", rule: berlin_minus_6', out)
    # Markdown: Periodentabelle
    assert md is not None
    assert "|" in md and "Maerz" in md and "Okt/Nov" in md
    for y in range(2017, 2023):
        assert str(y) in md
    table_rows = [ln for ln in md.splitlines() if ln.startswith("|")]
    assert len(table_rows) >= 2 + 6                    # Kopf, Trenner, >= 1 Zeile je Jahr


def test_survey_switch_without_open_period_has_no_undecided_block(tmp_path, monkeypatch, capsys,
                                                                   no_http):
    root = tmp_path / "data"
    seed_years(root, (2017, 2018, 2019), NYL)
    seed_raw(root, [(2020, 3)], stamping=B6)
    seed_years(root, (2021, 2022), B6)
    code, res, out, md = survey_cli(tmp_path, monkeypatch, capsys, root,
                                    periods_of(range(2017, 2023)))
    rows = month_rows(res)
    assert rows["2019-03"]["winner"] == NYL and rows["2019-10"]["winner"] == NYL
    assert rows["2020-03"]["decisive"] and rows["2020-03"]["winner"] == B6
    assert code == 3, out                              # 2020-10/11 fehlen (offline)
    # v7: keine offenen Umstellungsmonate zwischen dem letzten ny_local-Beleg (2019-11) und
    # dem ersten berlin_minus_6-Beleg (2020-03); dazwischen stempeln beide gleich -> der
    # Wechsel wird direkt nach dem letzten alten Beleg vorgeschlagen
    assert sched(res) == [("2000-01", NYL), ("2019-12", B6)]
    assert "undecided" not in [e["rule"] for e in res["schedule"]]


def test_survey_second_switch_is_exit_4_without_proposal(tmp_path, monkeypatch, capsys, no_http):
    # Fahrplan ab 2018-12 offen: die konfigurierte Regel faellt nirgends durch,
    # Exit 4 kommt allein aus der Wechselanalyse
    monkeypatch.setenv(ENV, str(write_cfg(tmp_path / "tz.yaml", [
        '{from: "2000-01", rule: ny_local}',
        '{from: "2018-12", rule: undecided, candidates: [ny_local, berlin_minus_6]}'])))
    root = tmp_path / "data"
    seed_years(root, (2017, 2018), NYL)
    seed_years(root, (2020, 2021), B6)
    seed_years(root, (2022, 2023), NYL)
    code, res, out, md = survey_cli(tmp_path, monkeypatch, capsys, root,
                                    periods_of(range(2017, 2024)))
    rows = month_rows(res)
    assert rows["2021-03"]["winner"] == B6 and rows["2022-03"]["winner"] == NYL
    assert not res.get("configured_failed")
    assert code == 4, out
    assert res["schedule"] == []
    assert "schedule:" not in out


def test_survey_weekend_reversal_month_is_decisive_for_berlin(tmp_path, monkeypatch, capsys,
                                                               no_http):
    root = tmp_path / "data"
    seed_years(root, (2021, 2024), B6)
    write_raw(root, (2024, 10), zip_of("XAUUSD", 2024, 10, weekend_reversal_month((2024, 10))))
    code, res, out, md = survey_cli(tmp_path, monkeypatch, capsys, root,
                                    periods_of((2021, 2024)))
    row = month_rows(res)["2024-10"]
    assert row["decisive"] and row["winner"] == B6
    assert row["results"][B6]["status"] == "ok"
    assert row["results"][B6]["dropped_weekend_ticks"] == N_STRAY
    assert row["results"][NYL]["status"] == "failed"
    assert code == 0, out
    lines = [ln for ln in out.splitlines() if ln.startswith("2024-10")]
    assert lines, out
    assert any(re.search(rf"\b{N_STRAY}\b", ln) and re.search(r"verworfen|dropped|Wochenend", ln)
               for ln in lines), lines
    assert md is not None and str(N_STRAY) in md


# ============================================================ review v6 ====
def test_plain_accepted_on_wednesday_reversal_does_not_release(tmp_path):
    """Ein von Hand gesetztes "accepted": true auf der Quarantaene eines NICHT
    freigebbaren Ruecksprungs (Mittwoch) schreibt den Monat nicht."""
    ym = (2024, 7)
    seed_raw(tmp_path, [(2024, 5), (2024, 6), (2024, 8)])
    wed = [rms(2024, 7, 10, 11, mi) for mi in range(0, 60, 10)]
    write_raw(tmp_path, ym, zip_of("XAUUSD", *ym, with_extra(raw_month("XAUUSD", *ym, B6), wed,
                                                              rms(2024, 7, 10, 12))))
    rep = do_run(tmp_path, Site(), ym)
    assert rep["exit_code"] == 4, rep
    q = qmarker_path(tmp_path, "XAUUSD", *ym)
    info = json.loads(q.read_text())
    assert "releasable_weekend" not in info
    info["accepted"] = True
    q.write_text(json.dumps(info))
    site = Site()
    rep = do_run(tmp_path, site, ym)
    assert rep["exit_code"] == 4, rep
    assert site.requests == 0
    assert "2024-07" in rep["summary"]["months_quarantine"]
    assert "2024-07" not in rep["summary"]["months_written"]
    assert not store_file(tmp_path, "XAUUSD", *ym).exists()
    assert not month_report_file(tmp_path, "XAUUSD", *ym).exists()
    assert q.exists()


# ---- HIGH 1: undecided mit verirrtem Tick in der verbotenen Stunde einer Regel ----
STRAY_2019 = rms(2019, 10, 26, 20, 30)      # Sa vor dem letzten So Okt: berlin_minus_6 mehrdeutig


def oct2019_with_stray() -> pd.DataFrame:
    return with_extra(raw_month("XAUUSD", 2019, 10, B6), [STRAY_2019], STRAY_2019 - 1)


def nov2019_from_4th() -> pd.DataFrame:
    df = raw_month("XAUUSD", 2019, 11, B6)
    return df[raw_ms_of(df) >= rms(2019, 11, 4)].reset_index(drop=True)


def ref_candidates_utc(raw_ms: np.ndarray):
    """Referenz (zoneinfo/pandas): UTC-ms je Kandidat, None-Maske fuer nicht eindeutige."""
    idx = pd.to_datetime(raw_ms, unit="ms")
    out = {}
    for st in (NYL, B6):
        loc = (idx + pd.Timedelta(hours=6) if st == B6 else idx).tz_localize(
            BER if st == B6 else NYZ, ambiguous="NaT", nonexistent="NaT")
        valid = np.asarray(~loc.isna())
        ms = np.zeros(len(idx), dtype=np.int64)
        ms[valid] = ms_of(loc[valid].tz_convert("UTC"))
        out[st] = (ms, valid)
    return out


def differing_counts(raw_ms: np.ndarray, ym) -> tuple[int, int]:
    """(differing ticks im Rohmonat gesamt, davon unter irgendeinem Kandidaten im UTC-Monat ym)."""
    ref = ref_candidates_utc(raw_ms)
    (a, va), (b, vb) = ref[NYL], ref[B6]
    diff = ~va | ~vb | (a != b)
    lo, hi = month_bounds(*ym)
    land = (va & (a >= lo) & (a < hi)) | (vb & (b >= lo) & (b < hi))
    return int(diff.sum()), int((diff & land).sum())


def test_undecided_stray_tick_reference_selftest():
    raw = raw_ms_of(oct2019_with_stray())
    ref = ref_candidates_utc(raw)
    i = int(np.nonzero(raw == STRAY_2019)[0][0])
    assert ref[NYL][1][i] and not ref[B6][1][i]               # nur unter berlin_minus_6 mehrdeutig
    total, in_oct = differing_counts(raw, (2019, 10))
    _, in_nov = differing_counts(raw, (2019, 11))
    assert total > 4 * 1300                                    # die ganze Fensterwoche 27.-31.10
    # Ticks vom 31.10 19:00-19:59 roh liegen unter ny_local im Oktober, unter berlin_minus_6
    # im November: in beiden UTC-Monaten mitgezaehlt
    assert 0 < in_nov < total and in_oct + in_nov == total + 60
    # die Abend-Ticks vom 31.10 landen unter BEIDEN Kandidaten im UTC-November
    ev = raw[(raw >= rms(2019, 10, 31, 20)) & (raw < rms(2019, 11, 1))]
    assert len(ev) > 100
    r2 = ref_candidates_utc(ev)
    lo = utc_dt(2019, 11, 1)
    assert (r2[NYL][0] >= lo).all() and (r2[B6][0] >= lo).all()
    assert (r2[NYL][0] != r2[B6][0]).all()
    # Rohmonat November ab dem 4.: beide Kandidaten identisch
    nov = raw_ms_of(nov2019_from_4th())
    r3 = ref_candidates_utc(nov)
    assert r3[NYL][1].all() and r3[B6][1].all() and np.array_equal(r3[NYL][0], r3[B6][0])


def test_undecided_stray_tick_holds_october_and_november(tmp_path):
    seed_raw(tmp_path, [(2019, 8), (2019, 9)], stamping=NYL)
    seed_raw(tmp_path, [(2019, 12)], stamping=B6)
    oct_df, nov_df = oct2019_with_stray(), nov2019_from_4th()
    write_raw(tmp_path, (2019, 10), zip_of("XAUUSD", 2019, 10, oct_df))
    write_raw(tmp_path, (2019, 11), zip_of("XAUUSD", 2019, 11, nov_df))
    assert isinstance(fh.rule_for("tick", (2019, 10)), fh.UndecidedRule)
    site = Site()
    rep = do_run(tmp_path, site, (2019, 10), (2019, 11))
    assert rep["exit_code"] == 4, rep
    assert site.requests == 0
    und = rep["summary"]["months_rule_undecided"]
    assert set(und) >= {"2019-10", "2019-11"}, und
    assert rep["summary"]["months_written"] == []
    for m in (10, 11):
        assert not store_file(tmp_path, "XAUUSD", 2019, m).exists()
    assert markers(tmp_path) == []
    raw = raw_ms_of(oct_df)
    total, in_oct = differing_counts(raw, (2019, 10))
    _, in_nov = differing_counts(raw, (2019, 11))
    assert und["2019-10"]["raw_month"] == "2019-10"
    assert und["2019-10"]["differing_ticks"] in (in_oct, total), (und["2019-10"], in_oct, total)
    assert und["2019-10"]["differing_ticks"] > 1000            # ganze Fensterwoche, nicht 1
    assert und["2019-11"]["raw_month"] == "2019-10"
    assert und["2019-11"]["differing_ticks"] in (in_nov, total), (und["2019-11"], in_nov, total)
    assert any("2019-10-31" in str(r) for r in und["2019-11"]["raw_ranges"])
    # zweiter Lauf: unveraendert gehalten, keine Requests
    site2 = Site()
    rep2 = do_run(tmp_path, site2, (2019, 11))
    assert rep2["exit_code"] == 4 and site2.requests == 0
    assert "2019-11" in rep2["summary"]["months_rule_undecided"]
    assert not store_file(tmp_path, "XAUUSD", 2019, 11).exists()


# ---- HIGH 2: Survey-Vorschlag bei Wechsel im Okt/Nov-Paar ----
def test_survey_switch_at_raw_2019_11_real_data(tmp_path, monkeypatch, capsys, no_http):
    """Okt 2019 ny_local (entscheidend), Nov 2019 berlin_minus_6 (nur EINE Fensterluecke:
    nicht entscheidend): 2019-11 darf nicht unter ny_local landen."""
    root = tmp_path / "data"
    seed_years(root, (2018,), NYL)
    seed_raw(root, [(2019, 3), (2019, 10)], stamping=NYL)
    seed_raw(root, [(2019, 11)], stamping=B6)
    seed_years(root, (2020,), B6)
    code, res, out, md = survey_cli(tmp_path, monkeypatch, capsys, root, periods_of((2018, 2019, 2020)))
    rows = month_rows(res)
    assert rows["2019-10"]["decisive"] and rows["2019-10"]["winner"] == NYL
    assert not rows["2019-11"]["decisive"]
    assert rows["2019-11"]["results"][B6]["status"] == "ok"
    assert rows["2020-03"]["decisive"] and rows["2020-03"]["winner"] == B6
    assert sched(res) == [("2000-01", NYL), ("2019-11", "undecided"), ("2020-03", B6)], out
    assert res["schedule"][1]["candidates"] == [NYL, B6]
    assert code == 0, out


DEC_NY = {NYL: "ok", B6: "failed", "fixed_est": "failed"}
DEC_B6 = {NYL: "failed", B6: "ok", "fixed_est": "failed"}
BOTH = {NYL: "ok", B6: "ok", "fixed_est": "failed"}


def fake_month_results(table: dict):
    """evaluate_raw_month-Ersatz: Status je (Rohmonat, Regel) aus `table`."""
    def ev(data_root, symbol, ym, mode="tick", rule=None, drop_releasable_weekend=False):
        name = getattr(rule, "name", rule)
        st = table[tuple(ym)].get(name, "failed")
        return {"status": st, "ok": st == "ok", "reason": None if st == "ok" else "synthetisch",
                "cached": True, "anchors": 40, "median_deviation_h": 0.0 if st == "ok" else 1.0,
                "dropped_weekend_ticks": 0, "releasable_weekend": None, "large_reversals": [],
                "rule": name, "symbol": symbol, "month": f"{ym[0]}-{ym[1]:02d}"}
    return ev


def survey_mocked(tmp_path, monkeypatch, capsys, table, cfg_lines=None):
    if cfg_lines is not None:
        monkeypatch.setenv(ENV, str(write_cfg(tmp_path / "tz.yaml", cfg_lines)))
    root = tmp_path / "data"
    for ym in table:
        p = fh.Unit("XAUUSD", "tick", *ym).path(root)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(b"PK-synthetisch")
    monkeypatch.setattr(fh, "evaluate_raw_month", fake_month_results(table))
    return survey_cli(tmp_path, monkeypatch, capsys, root,
                      [f"{y}-{m:02d}" for y, m in sorted(table)])


OPEN_FROM_2018_12 = ['{from: "2000-01", rule: ny_local}',
                     '{from: "2018-12", rule: undecided, candidates: [ny_local, berlin_minus_6]}']


def test_survey_mocked_switch_in_november_not_decisive(tmp_path, monkeypatch, capsys, no_http):
    table = {(2018, 3): DEC_NY, (2018, 10): DEC_NY, (2018, 11): DEC_NY,
             (2019, 3): DEC_NY, (2019, 10): DEC_NY, (2019, 11): BOTH,
             (2020, 3): DEC_B6, (2020, 10): DEC_B6, (2020, 11): BOTH}
    code, res, out, _ = survey_mocked(tmp_path, monkeypatch, capsys, table, OPEN_FROM_2018_12)
    assert sched(res) == [("2000-01", NYL), ("2019-11", "undecided"), ("2020-03", B6)], out
    assert res["schedule"][1]["candidates"] == [NYL, B6]
    assert code == 0, out


def test_survey_mocked_undecided_runs_until_first_decisive_berlin_month(tmp_path, monkeypatch,
                                                                         capsys, no_http):
    table = {(2018, 3): DEC_NY, (2018, 10): DEC_NY, (2018, 11): DEC_NY,
             (2019, 3): DEC_NY, (2019, 10): DEC_NY, (2019, 11): BOTH,
             (2020, 3): BOTH, (2020, 10): DEC_B6, (2020, 11): BOTH,
             (2021, 3): DEC_B6, (2021, 10): DEC_B6, (2021, 11): BOTH}
    code, res, out, _ = survey_mocked(tmp_path, monkeypatch, capsys, table, OPEN_FROM_2018_12)
    assert sched(res) == [("2000-01", NYL), ("2019-11", "undecided"), ("2020-10", B6)], out
    assert res["schedule"][1]["candidates"] == [NYL, B6]
    assert code == 0, out


def test_survey_mocked_mirror_october_not_decisive(tmp_path, monkeypatch, capsys, no_http):
    """EU-Ende am 31.10. (2021): Oktober nicht entscheidend, November entscheidend
    berlin_minus_6 -> berlin_minus_6 erst ab 2021-11, davor undecided ab 2021-04."""
    assert last_sunday(2021, 10) == date(2021, 10, 31)
    table = {(2020, 3): DEC_NY, (2020, 10): DEC_NY, (2020, 11): DEC_NY,
             (2021, 3): DEC_NY, (2021, 10): BOTH, (2021, 11): DEC_B6,
             (2022, 3): DEC_B6, (2022, 10): DEC_B6, (2022, 11): BOTH}
    code, res, out, _ = survey_mocked(tmp_path, monkeypatch, capsys, table, OPEN_FROM_2018_12)
    assert sched(res) == [("2000-01", NYL), ("2021-04", "undecided"), ("2021-11", B6)], out
    assert res["schedule"][1]["candidates"] == [NYL, B6]
    assert code == 0, out


def test_survey_mocked_switch_exactly_at_october_november_boundary(tmp_path, monkeypatch, capsys,
                                                                   no_http):
    table = {(2018, 3): DEC_NY, (2018, 10): DEC_NY, (2018, 11): DEC_NY,
             (2019, 3): DEC_NY, (2019, 10): DEC_NY, (2019, 11): DEC_B6,
             (2020, 3): DEC_B6, (2020, 10): DEC_B6, (2020, 11): DEC_B6}
    code, res, out, _ = survey_mocked(tmp_path, monkeypatch, capsys, table, OPEN_FROM_2018_12)
    assert sched(res) == [("2000-01", NYL), ("2019-11", B6)], out
    assert code == 0, out


def test_survey_mocked_weak_evidence_is_exit_4(tmp_path, monkeypatch, capsys, no_http):
    """berlin_minus_6 nur in EINEM Rohmonat eines Symbols entscheidend: kein Vorschlag, Exit 4."""
    table = {(2017, 3): DEC_NY, (2017, 10): DEC_NY, (2018, 3): DEC_NY,
             (2021, 3): BOTH, (2021, 10): DEC_B6, (2021, 11): BOTH}
    code, res, out, _ = survey_mocked(tmp_path, monkeypatch, capsys, table, OPEN_FROM_2018_12)
    assert res["schedule"] == []
    assert "schedule:" not in out
    assert res["weak_evidence"]
    assert code == 4, out
