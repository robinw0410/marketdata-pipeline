"""Regressionstests: HistData-Tick-Stempelung = Europe/Berlin - 6 h (TzRule, ab tz_check v5).

Befund (Homelab, echte HistData-Ticks XAUUSD/XAGUSD/EURUSD): von der US-Umstellung
(2. So Maerz) bis zur EU-Umstellung (letzter So Maerz) bleibt die Rohzeit bei
UTC-5, die Tagespause der Metalle liegt dann roh bei 15:59:59 -> 17:00:00; ab der
EU-Umstellung im Oktober ist sie schon wieder UTC-5. Rohzeit = Berlin - 6 h. Der
Markt (CME) bleibt New-York-basiert: Pause 17-18 NY, Fr 17:00 Schluss, So 18:00 Start.

Nur synthetische Daten, HTTP gemockt. Erwartete Werte werden hier unabhaengig mit
zoneinfo/pandas berechnet, nie mit Helfern aus fetch_histdata.
"""
import calendar
import dataclasses
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

UTC = timezone.utc
BER = ZoneInfo("Europe/Berlin")
NYZ = ZoneInfo("America/New_York")
EST = ZoneInfo("Etc/GMT+5")
HOUR_MS, DAY_MS = 3_600_000, 86_400_000
METALS = ("XAUUSD", "XAGUSD")
BASE_PRICE = {"XAUUSD": 2050.0, "XAGUSD": 23.0}
SPREAD = {"XAUUSD": 0.25, "XAGUSD": 0.02}
NOW = datetime(2024, 12, 1, 12, tzinfo=UTC)
ENV = "HISTDATA_TZ_CONFIG"
# source_timezone, wie v4 (RAW_TZ tick = America/New_York) es geschrieben hat
V4_NY_LABEL = "America/New_York -> UTC, je Rohmonat geprueft (tz_check v4)"
REF_RULES = {"berlin_minus_6": (BER, -6), "ny_local": (NYZ, 0), "fixed_est": (EST, 0)}


@pytest.fixture(autouse=True)
def _no_env_config(monkeypatch):
    monkeypatch.delenv(ENV, raising=False)


# ------------------------------------------------------- referenz (zoneinfo) ---
def last_sunday(y, m):
    d = date(y, m, calendar.monthrange(y, m)[1])
    return d - timedelta(days=(d.weekday() - 6) % 7)


def nth_sunday(y, m, n):
    d = date(y, m, 1)
    d += timedelta(days=(6 - d.weekday()) % 7)
    return d + timedelta(weeks=n - 1)


def ref_raw_to_utc(raw: datetime, rule: str):
    """Naive Rohzeit -> UTC (aware) nach Regel; None = nicht existent/mehrdeutig."""
    zone, shift = REF_RULES[rule]
    local = raw - timedelta(hours=shift)              # roh = lokal + shift
    a, b = local.replace(tzinfo=zone, fold=0), local.replace(tzinfo=zone, fold=1)
    if a.utcoffset() != b.utcoffset():
        return None
    return a.astimezone(UTC)


def rms(*a) -> int:
    """Rohzeit-Ziffern -> ms (als UTC gelesen, wie fetch_histdata Rohzeit fuehrt)."""
    return int(datetime(*a, tzinfo=UTC).timestamp() * 1000)


def dt_ms(d: datetime) -> int:
    return int(d.timestamp() * 1000)


def naive(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, UTC).replace(tzinfo=None)


def rule(name):
    return fh.TZ_RULES[name]


# ------------------------------------------------------------ synthetik ---
def _closed(idx: pd.DatetimeIndex) -> np.ndarray:
    """CME-Metalle, New-York-Kalender: Fr 17:00 .. So 18:00 zu, Mo-Do 17-18 Pause."""
    ny = idx.tz_convert("America/New_York")
    wd, h = ny.dayofweek, ny.hour
    return np.asarray(((wd == 4) & (h >= 17)) | (wd == 5) | ((wd == 6) & (h < 18))
                      | ((wd <= 3) & (h == 17)))


def market_utc(lo, hi) -> pd.DatetimeIndex:
    """Ein Tick je Minute (:00.000) bei offenem Markt, dazu ein Tick :59.000 in der
    letzten Minute vor jedem Schluss (letzter Tick 16:59:59, erster 18:00:00 NY)."""
    grid = pd.date_range(lo, hi, freq="60s", inclusive="left", tz="UTC")
    openg = grid[~_closed(grid)]
    last = openg[_closed(openg + pd.Timedelta(seconds=60))]
    return openg.append(last + pd.Timedelta(seconds=59)).sort_values()


def stamp(utc: pd.DatetimeIndex, stamping: str) -> pd.DatetimeIndex:
    if stamping == "berlin_minus_6":
        return utc.tz_convert("Europe/Berlin").tz_localize(None) - pd.Timedelta(hours=6)
    if stamping == "ny_local":
        return utc.tz_convert("America/New_York").tz_localize(None)
    assert stamping == "fixed_est"
    return utc.tz_localize(None) - pd.Timedelta(hours=5)


@functools.lru_cache(maxsize=None)
def raw_month(symbol, y, m, stamping="berlin_minus_6") -> pd.DataFrame:
    """Ticks eines ROH-Monats in Dateireihenfolge: raw (naiv), utc, bid, ask.
    Nicht veraendern (gecached)."""
    start = pd.Timestamp(y, m, 1)
    end = start + pd.offsets.MonthBegin(1)
    utc = market_utc(start.tz_localize("UTC") - pd.Timedelta(days=2),
                     end.tz_localize("UTC") + pd.Timedelta(days=2))
    raw = stamp(utc, stamping)
    keep = np.asarray((raw >= start) & (raw < end))
    raw, utc = raw[keep], utc[keep]
    order = np.argsort(raw.as_unit("ms").asi8, kind="stable")
    raw, utc = raw[order], utc[order]
    i = (utc.as_unit("ms").asi8 // 60_000).astype(float)
    bid = np.round(BASE_PRICE[symbol] * (1 + 0.002 * np.sin(i / 97.0)), 4)
    return pd.DataFrame({"raw": raw, "utc": utc, "bid": bid, "ask": bid + SPREAD[symbol]})


def raw_ms_of(df) -> np.ndarray:
    return pd.DatetimeIndex(df["raw"]).as_unit("ms").asi8


def month_bounds(y, m):
    return rms(y, m, 1), rms(*((y + 1, 1) if m == 12 else (y, m + 1)), 1)


def ym_add(ym, k):
    i = ym[0] * 12 + ym[1] - 1 + k
    return i // 12, i % 12 + 1


def neighbours(symbol, ym, stamping="berlin_minus_6", pad_days=8):
    """(letzte pad_days des Vor-, erste pad_days des Folge-Rohmonats) als raw-ms."""
    prev = raw_ms_of(raw_month(symbol, *ym_add(ym, -1), stamping))
    nxt = raw_ms_of(raw_month(symbol, *ym_add(ym, 1), stamping))
    return (prev[prev >= prev.max() - pad_days * DAY_MS], nxt[nxt <= nxt.min() + pad_days * DAY_MS])


def tick_csv(df) -> bytes:
    st = pd.DatetimeIndex(df["raw"]).strftime("%Y%m%d %H%M%S%f").str[:-3]
    return ("\n".join(f"{s},{b:.5f},{a:.5f},0" for s, b, a in zip(st, df["bid"], df["ask"]))
            + "\n").encode()


@functools.lru_cache(maxsize=None)
def tick_zip(symbol, y, m, stamping="berlin_minus_6") -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(f"DAT_ASCII_{symbol}_T_{y}{m:02d}.csv", tick_csv(raw_month(symbol, y, m, stamping)))
        zf.writestr(f"DAT_ASCII_{symbol}_T_{y}{m:02d}.txt", "HistData.com status report\n")
    return buf.getvalue()


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


def seed_raw(root, months, stamping="berlin_minus_6", sym="XAUUSD"):
    for ym in months:
        st = stamping(ym) if callable(stamping) else stamping
        p = raw_zip_path(root, sym, *ym)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(tick_zip(sym, *ym, st))


def stale_files(root):
    d = root / "metals" / "stale"
    return sorted(d.rglob("*.stale")) if d.exists() else []


def label_of(p: Path) -> str:
    v = (pq.read_schema(p).metadata or {}).get(b"source_timezone")
    return v.decode() if v else ""


# ------------------------------------------------------------ http-mock ---
PAGE = """<html><body>
<form id="file_status" method="POST" action="/getStatus.php">
  <input type="hidden" name="tk" id="tk" value="WRONG" /></form>
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
    """Fake-HistData (nur Tick-Monatsdateien): files[(SYM, 'YYYYMM')] -> ZIP."""

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
        assert data["tk"].startswith("tok-")
        return Resp(200, self.files.get((data["fxpair"], data["datemonth"]), b""))

    @property
    def requests(self):
        return len(self.calls)


def do_run(root, site, start, end=None, sym="XAUUSD"):
    clock = Clock()
    return fh.run(sym, date(*start, 1), date(*(end or start), 1), root, mode="tick",
                  now=lambda: NOW, pause=0.0, attempts=2, backoff_base=1.0, max_backoff=10.0,
                  jitter=0.0, sleep=clock.sleep, clock=clock, wall=clock, session=site)


def bars_ms(path: Path) -> np.ndarray:
    return pd.DatetimeIndex(pd.read_parquet(path)["ts"]).as_unit("ms").asi8


def expected_minutes(sym, ym, raws) -> set:
    """UTC-Minuten des UTC-Monats ym aus den Generator-UTC-Zeiten der Rohmonate
    raws = {(y, m): stamping} (unabhaengig von fetch_histdata)."""
    lo, hi = month_bounds(*ym)
    out = set()
    for r, st in raws.items():
        u = pd.DatetimeIndex(raw_month(sym, *r, st)["utc"]).floor("min").as_unit("ms").asi8
        out |= set(int(x) for x in u[(u >= lo) & (u < hi)])
    return out


def utc_dt(*a) -> int:
    return dt_ms(datetime(*a, tzinfo=UTC))


# ==================================================================== 0 ====
def test_generator_reproduces_observed_raw_anchors():
    """Selbsttest des Generators: Rohdaten zeigen genau die beobachteten Anker."""
    def gaps(sym, ym):
        ms = raw_ms_of(raw_month(sym, *ym))
        d = np.diff(ms)
        idx = np.nonzero(d >= 45 * 60_000)[0]
        return {naive(int(ms[i])).date(): (naive(int(ms[i])).strftime("%a %H:%M:%S"),
                                           naive(int(ms[i + 1])).strftime("%a %d %H:%M:%S"))
                for i in idx}

    for sym in METALS:
        mar = gaps(sym, (2024, 3))
        for d in (4, 5, 6, 7):                                    # vor 10.03: normal
            assert mar[date(2024, 3, d)][0].endswith("16:59:59"), mar[date(2024, 3, d)]
            assert mar[date(2024, 3, d)][1].endswith("18:00:00")
        for d in (11, 12, 13, 14, 18, 19, 20, 21, 25, 26, 27, 28):  # US- vor EU-Umstellung
            assert mar[date(2024, 3, d)] == (mar[date(2024, 3, d)][0][:4] + "15:59:59",
                                             mar[date(2024, 3, d)][1][:7] + "17:00:00")
        assert mar[date(2024, 3, 8)] == ("Fri 16:59:59", "Sun 10 17:00:00")
        assert mar[date(2024, 3, 15)] == ("Fri 15:59:59", "Sun 17 17:00:00")
        assert mar[date(2024, 3, 29)] == ("Fri 15:59:59", "Sun 31 18:00:00")
        octo = gaps(sym, (2024, 10))
        for d in (21, 22, 23, 24):
            assert octo[date(2024, 10, d)][0].endswith("16:59:59")
        assert octo[date(2024, 10, 25)] == ("Fri 16:59:59", "Sun 27 17:00:00")
        for d in (28, 29, 30, 31):                                # ab EU-Umstellung verschoben
            assert octo[date(2024, 10, d)][0].endswith("15:59:59")
            assert octo[date(2024, 10, d)][1].endswith("17:00:00")
        nov = gaps(sym, (2024, 11))
        assert nov[date(2024, 11, 1)] == ("Fri 15:59:59", "Sun 03 18:00:00")
        for d in (4, 5, 6, 7):
            assert nov[date(2024, 11, d)][0].endswith("16:59:59")
            assert nov[date(2024, 11, d)][1].endswith("18:00:00")


# ==================================================================== 1 ====
ANCHOR_MONTHS = [(2024, 3), (2024, 10), (2024, 11), (2024, 1), (2024, 7)]


@pytest.mark.parametrize("sym", METALS)
@pytest.mark.parametrize("ym", ANCHOR_MONTHS)
@pytest.mark.parametrize("with_context", [False, True])
def test_observed_anchors_pass_under_berlin_minus_6(sym, ym, with_context):
    df = raw_month(sym, *ym)
    ms = raw_ms_of(df)
    kw = {"neighbours": neighbours(sym, ym)} if with_context else {}
    rep = fh.check_timezone(ms, sym, month_bounds(*ym), mode="tick",
                            rule=rule("berlin_minus_6"), **kw)
    assert rep["status"] == "ok" and rep["ok"] is True, rep.get("reason")
    assert rep["median_deviation_h"] == 0
    assert "berlin_minus_6" in str(rep["rule"])
    assert {int(k) for k in rep["deviations"]} == {0}, rep["deviations"]
    # erwartete Offsets je Anker aus der Regel: genau die Stempel-Offsets der Daten
    stamped = {int(round(x / HOUR_MS)) for x in
               np.unique(ms - pd.DatetimeIndex(df["utc"]).as_unit("ms").asi8)}
    assert {int(k) for k in rep["expected_offsets"]} == stamped
    want = {(2024, 3): {-5, -4}, (2024, 10): {-4, -5}, (2024, 11): {-5}, (2024, 1): {-5},
            (2024, 7): {-4}}[ym]
    assert stamped == want


@pytest.mark.parametrize("sym", METALS)
@pytest.mark.parametrize("ym", [(2024, 3), (2024, 10), (2024, 11)])
def test_observed_anchors_fail_under_ny_local(sym, ym):
    """Unter New-York-Ortszeit weichen die EU/US-Lueckenwochen bestaetigt ab
    (November: Fr 01.11 setzt den Lauf 28.-31.10 aus dem Vormonats-Kontext fort)."""
    ms = raw_ms_of(raw_month(sym, *ym))
    rep = fh.check_timezone(ms, sym, month_bounds(*ym), mode="tick", rule=rule("ny_local"),
                            neighbours=neighbours(sym, ym))
    assert rep["status"] == "failed" and rep["ok"] is False, rep
    assert rep["reason"]
    assert "ny_local" in str(rep["rule"])
    assert -1 in {int(k) for k in rep["deviations"]} or rep["median_deviation_h"] != 0 \
        or rep.get("switch_runs"), rep


@pytest.mark.parametrize("sym", METALS)
@pytest.mark.parametrize("ym", [(2024, 1), (2024, 7)])
def test_jan_jul_pass_under_ny_local_too(sym, ym):
    """Jan/Jul: beide Regeln liefern dieselben UTC-Zeiten (daher 'Jan/Jul passen')."""
    rep = fh.check_timezone(raw_ms_of(raw_month(sym, *ym)), sym, month_bounds(*ym), mode="tick",
                            rule=rule("ny_local"), neighbours=neighbours(sym, ym))
    assert rep["status"] == "ok", rep.get("reason")


def test_single_shifted_break_is_only_a_warning():
    """Einzelausreisser (eine Tagespause 1 h daneben) bleibt Warnung, nicht failed."""
    df = raw_month("XAUUSD", 2024, 3)
    ms = raw_ms_of(df).copy()
    # Mi 06.03 roh 16:59:59 -> 18:00:00: Pause um 1 h nach vorn (16:00-17:00) versetzen
    lo, hi = rms(2024, 3, 6, 16), rms(2024, 3, 6, 17)
    keep = ~((ms >= lo) & (ms < hi))
    extra = np.arange(rms(2024, 3, 6, 17), rms(2024, 3, 6, 18), 60_000, dtype=np.int64)
    ms = np.sort(np.concatenate([ms[keep], extra]))
    rep = fh.check_timezone(ms, "XAUUSD", month_bounds(2024, 3), mode="tick",
                            rule=rule("berlin_minus_6"), neighbours=neighbours("XAUUSD", (2024, 3)))
    assert rep["status"] == "ok", rep.get("reason")
    assert rep["median_deviation_h"] == 0
    assert {int(k) for k in rep["deviations"]} == {0, -1}, rep["deviations"]   # Ausreisser gesehen
    assert rep.get("warnings"), rep


# ==================================================================== 2 ====
CONVERSIONS = [
    ((2024, 3, 15, 15, 59, 59), (2024, 3, 15, 20, 59, 59)),     # = 16:59:59 EDT
    ((2024, 3, 15, 17, 0, 0), (2024, 3, 15, 22, 0, 0)),         # = 18:00 EDT
    ((2024, 4, 2, 16, 59, 59), (2024, 4, 2, 20, 59, 59)),
    ((2024, 10, 29, 15, 59, 59), (2024, 10, 29, 20, 59, 59)),
    ((2024, 11, 5, 16, 59, 59), (2024, 11, 5, 21, 59, 59)),
    ((2024, 1, 10, 16, 59, 59), (2024, 1, 10, 21, 59, 59)),
    ((2024, 7, 10, 16, 59, 59), (2024, 7, 10, 20, 59, 59)),
    # 2012: US 11.03 / 04.11, EU 25.03 / 28.10
    ((2012, 3, 14, 15, 59, 59), (2012, 3, 14, 20, 59, 59)),
    ((2012, 3, 23, 15, 59, 59), (2012, 3, 23, 20, 59, 59)),
    ((2012, 3, 26, 16, 59, 59), (2012, 3, 26, 20, 59, 59)),
    ((2012, 10, 30, 15, 59, 59), (2012, 10, 30, 20, 59, 59)),
    ((2012, 11, 6, 16, 59, 59), (2012, 11, 6, 21, 59, 59)),
    # 2018: US 11.03 / 04.11, EU 25.03 / 28.10
    ((2018, 3, 14, 15, 59, 59), (2018, 3, 14, 20, 59, 59)),
    ((2018, 3, 27, 16, 59, 59), (2018, 3, 27, 20, 59, 59)),
    ((2018, 10, 31, 15, 59, 59), (2018, 10, 31, 20, 59, 59)),
    ((2018, 11, 7, 16, 59, 59), (2018, 11, 7, 21, 59, 59)),
    # 2021: US 14.03 / 07.11, EU 28.03 / 31.10
    ((2021, 3, 17, 15, 59, 59), (2021, 3, 17, 20, 59, 59)),
    ((2021, 3, 30, 16, 59, 59), (2021, 3, 30, 20, 59, 59)),
    ((2021, 11, 2, 15, 59, 59), (2021, 11, 2, 20, 59, 59)),
    ((2021, 11, 9, 16, 59, 59), (2021, 11, 9, 21, 59, 59)),
]


@pytest.mark.parametrize("raw,utc", CONVERSIONS)
def test_raw_to_utc_berlin_minus_6_values(raw, utc):
    assert ref_raw_to_utc(datetime(*raw), "berlin_minus_6") == datetime(*utc, tzinfo=UTC)  # Referenz
    out = fh.raw_to_utc_ms(np.array([rms(*raw)], dtype=np.int64), rule("berlin_minus_6"))
    assert out.dtype == np.int64
    assert int(out[0]) == utc_dt(*utc)


@pytest.mark.parametrize("year", [2012, 2018, 2021, 2024])
@pytest.mark.parametrize("name", ["berlin_minus_6", "ny_local", "fixed_est"])
def test_raw_to_utc_matches_zoneinfo_over_whole_year(year, name):
    """Alle 20 min eines Rohjahres (+ krumme ms) gegen zoneinfo; nicht eindeutige
    Ortszeiten ausgelassen (die pruefen die Kantentests)."""
    start = datetime(year, 1, 1)
    raws = [start + timedelta(minutes=20 * k, milliseconds=(k * 7919) % 1000)
            for k in range(366 * 72)]
    refs = [ref_raw_to_utc(r, name) for r in raws]
    ok = [i for i, u in enumerate(refs) if u is not None]
    assert len(raws) - len(ok) == (0 if name == "fixed_est" else 6)   # 2 Stunden je 3 Ticks
    raw_arr = np.array([rms(*raws[i].timetuple()[:6]) + raws[i].microsecond // 1000 for i in ok],
                       dtype=np.int64)
    want = np.array([dt_ms(refs[i]) for i in ok], dtype=np.int64)
    got = fh.raw_to_utc_ms(raw_arr, rule(name))
    assert np.array_equal(got, want)


@pytest.mark.parametrize("year", [2012, 2018, 2021, 2024])
def test_berlin_minus_6_switches_on_last_sundays_0100_utc(year):
    """Offset roh-UTC: -5 h bis letzter So Maerz 01:00 UTC, dann -4 h bis letzter So
    Oktober 01:00 UTC, dann wieder -5 h (US-Umstellungen ohne Wirkung)."""
    r = rule("berlin_minus_6")
    for d, before, after in ((last_sunday(year, 3), -5, -4), (last_sunday(year, 10), -4, -5)):
        sw = dt_ms(datetime(d.year, d.month, d.day, 1, tzinfo=UTC))
        # UTC 1 h vor / 1 h nach dem Wechsel (ausserhalb der nicht eindeutigen
        # Rohstunde Sa 20-21 Uhr): roh = utc + offset
        u0, u1 = sw - HOUR_MS - 1, sw + HOUR_MS
        got = fh.raw_to_utc_ms(np.array([u0 + before * HOUR_MS, u1 + after * HOUR_MS],
                                        dtype=np.int64), r)
        assert list(got) == [u0, u1]
        # eine Woche vorher/nachher gilt jeweils der alte/neue Offset
        for u, off in ((sw - 7 * DAY_MS, before), (sw + 7 * DAY_MS, after)):
            assert int(fh.raw_to_utc_ms(np.array([u + off * HOUR_MS]), r)[0]) == u
    for d, off in ((nth_sunday(year, 3, 2) + timedelta(days=3), -5),   # Mi nach US-Beginn
                   (nth_sunday(year, 11, 1) - timedelta(days=3), -5),  # Do vor US-Ende
                   (last_sunday(year, 3) + timedelta(days=2), -4)):
        raw = rms(d.year, d.month, d.day, 15, 59, 59)
        assert int(fh.raw_to_utc_ms(np.array([raw]), r)[0]) == raw - off * HOUR_MS


# ==================================================================== 3 ====
def test_timezone_error_is_format_error():
    assert issubclass(fh.TimezoneError, fh.FormatError)


@pytest.mark.parametrize("raw", [(2024, 3, 30, 20, 30), (2024, 10, 26, 20, 30)])
def test_nonexistent_and_ambiguous_berlin_local_time_raise(raw):
    assert ref_raw_to_utc(datetime(*raw), "berlin_minus_6") is None            # Referenz
    with pytest.raises(fh.TimezoneError):
        fh.raw_to_utc_ms(np.array([rms(*raw)], dtype=np.int64), rule("berlin_minus_6"))
    # auch eingebettet in sonst gueltige Zeiten: nie raten, ganzer Aufruf scheitert
    arr = np.array([rms(2024, 3, 15, 12), rms(*raw), rms(2024, 7, 1, 12)], dtype=np.int64)
    with pytest.raises(fh.TimezoneError):
        fh.raw_to_utc_ms(arr, rule("berlin_minus_6"))


@pytest.mark.parametrize("year", [2012, 2018, 2021, 2024, 2025])
@pytest.mark.parametrize("month", [3, 10])
def test_transition_night_edges(year, month):
    """Sa vor dem letzten So: roh 19:59:59.999 ok, 20:00:00.000 .. 20:59:59.999
    nicht eindeutig, 21:00:00.000 ok."""
    sat = last_sunday(year, month) - timedelta(days=1)
    r = rule("berlin_minus_6")

    def conv(h, mi, s, ms):
        return fh.raw_to_utc_ms(np.array([rms(sat.year, sat.month, sat.day, h, mi, s, ms * 1000)],
                                         dtype=np.int64), r)

    before, after = (5, 4) if month == 3 else (4, 5)   # UTC = roh + Stunden
    raw0 = rms(sat.year, sat.month, sat.day, 19, 59, 59, 999_000)
    assert int(conv(19, 59, 59, 999)[0]) == raw0 + before * HOUR_MS
    raw1 = rms(sat.year, sat.month, sat.day, 21)
    assert int(conv(21, 0, 0, 0)[0]) == raw1 + after * HOUR_MS
    for h, mi, s, ms in ((20, 0, 0, 0), (20, 30, 0, 0), (20, 59, 59, 999)):
        assert ref_raw_to_utc(datetime(sat.year, sat.month, sat.day, h, mi, s, ms * 1000),
                              "berlin_minus_6") is None
        with pytest.raises(fh.TimezoneError):
            conv(h, mi, s, ms)


def test_us_transition_nights_are_fine_under_berlin_minus_6():
    """Die New-York-Problemstunden sind unter Berlin-6 normale Zeiten."""
    raws = [rms(2024, 3, 10, 2, 30), rms(2024, 11, 3, 1, 30)]
    got = fh.raw_to_utc_ms(np.array(raws, dtype=np.int64), rule("berlin_minus_6"))
    assert list(got) == [r + 5 * HOUR_MS for r in raws]
    with pytest.raises(fh.TimezoneError):
        fh.raw_to_utc_ms(np.array([raws[0]], dtype=np.int64), rule("ny_local"))
    assert int(fh.raw_to_utc_ms(np.array([raws[0]]), rule("fixed_est"))[0]) == raws[0] + 5 * HOUR_MS


# ==================================================================== 4 ====
def test_tz_rule_dataclass_and_builtin_rules():
    r = fh.TzRule("berlin_minus_6", "Europe/Berlin", -6)
    assert dataclasses.is_dataclass(r)
    with pytest.raises(dataclasses.FrozenInstanceError):
        r.shift_h = 0
    assert fh.TZ_RULES["berlin_minus_6"] == r
    assert fh.TZ_RULES["ny_local"] == fh.TzRule("ny_local", "America/New_York", 0)
    assert fh.TZ_RULES["fixed_est"] == fh.TzRule("fixed_est", "Etc/GMT+5", 0)
    for name, rr in fh.TZ_RULES.items():
        assert rr.name == name


def test_default_config():
    """Zeitplan nach Survey (LXC): ny_local bis 2018 belegt, berlin_minus_6 ab 2021-03
    belegt, dazwischen undecided (v6). M1 unveraendert fixed_est."""
    cfg = fh.load_tz_config()
    for ym in ((2000, 1), (2009, 3), (2015, 12), (2016, 1), (2018, 11)):
        assert fh.rule_for("tick", ym, cfg) == rule("ny_local"), ym
    for ym in ((2018, 12), (2019, 3), (2020, 11), (2021, 2)):
        r = fh.rule_for("tick", ym, cfg)
        assert r.name == "undecided" and {c.name for c in r.candidates} == {"ny_local", "berlin_minus_6"}, ym
    for ym in ((2021, 3), (2024, 3), (2024, 10), (2026, 11)):
        assert fh.rule_for("tick", ym, cfg) == rule("berlin_minus_6"), ym
    for ym in ((2000, 1), (2009, 3), (2019, 3), (2024, 3), (2026, 11)):
        assert fh.rule_for("m1", ym, cfg) == rule("fixed_est"), ym
    assert fh.rule_for("tick", (2024, 3)) == rule("berlin_minus_6")      # cfg=None


def _write_cfg(path: Path, tick, m1=(("2000-01", "fixed_est"),), rules=None) -> Path:
    lines = []
    if rules is not None:
        lines.append("rules:")
        for r in rules:
            lines.append(f"  - {{name: {r[0]}, zone: \"{r[1]}\", shift_h: {r[2]}}}")
    lines.append("schedule:")
    for mode, entries in (("tick", tick), ("m1", m1)):
        lines.append(f"  {mode}:")
        for f, n in entries:
            lines.append(f"    - {{from: \"{f}\", rule: {n}}}")
    path.write_text("\n".join(lines) + "\n")
    return path


SWITCH_2016 = (("2000-01", "ny_local"), ("2016-01", "berlin_minus_6"))


def test_schedule_selects_rule_per_raw_month(tmp_path):
    cfg = fh.load_tz_config(_write_cfg(tmp_path / "tz.yaml", SWITCH_2016))
    for y in (2014, 2015, 2016, 2017):
        for m in range(1, 13):
            want = "ny_local" if (y, m) <= (2015, 12) else "berlin_minus_6"
            assert fh.rule_for("tick", (y, m), cfg).name == want, (y, m)
            assert fh.rule_for("m1", (y, m), cfg).name == "fixed_est"
    # die gewaehlte Regel passt jeweils zu so gestempelten Daten (und nur dazu)
    for ym, st in (((2015, 3), "ny_local"), ((2016, 3), "berlin_minus_6"),
                   ((2015, 10), "ny_local"), ((2016, 10), "berlin_minus_6")):
        ms = raw_ms_of(raw_month("XAUUSD", *ym, st))
        nb = neighbours("XAUUSD", ym, st)
        good = fh.check_timezone(ms, "XAUUSD", month_bounds(*ym), mode="tick",
                                 rule=fh.rule_for("tick", ym, cfg), neighbours=nb)
        assert good["status"] == "ok", (ym, good.get("reason"))
        other = "berlin_minus_6" if st == "ny_local" else "ny_local"
        bad = fh.check_timezone(ms, "XAUUSD", month_bounds(*ym), mode="tick", rule=rule(other),
                                neighbours=nb)
        assert bad["status"] == "failed", ym


def test_extra_rule_definitions(tmp_path):
    cfg = fh.load_tz_config(_write_cfg(
        tmp_path / "tz.yaml", (("2000-01", "berlin_minus_6"), ("2030-01", "chicago_plus_1")),
        rules=[("chicago_plus_1", "America/Chicago", 1)]))
    r = fh.rule_for("tick", (2030, 7), cfg)
    assert r == fh.TzRule("chicago_plus_1", "America/Chicago", 1)
    assert fh.rule_for("tick", (2029, 12), cfg) == rule("berlin_minus_6")
    got = fh.raw_to_utc_ms(np.array([rms(2024, 7, 10, 16, 59, 59)], dtype=np.int64), r)
    assert int(got[0]) == utc_dt(2024, 7, 10, 20, 59, 59)


BAD_CONFIGS = {
    "unknown_rule": dict(tick=(("2000-01", "berlin_minus_6"), ("2016-01", "no_such_rule"))),
    "unsorted": dict(tick=(("2016-01", "berlin_minus_6"), ("2000-01", "ny_local"))),
    "duplicate_from": dict(tick=(("2000-01", "ny_local"), ("2016-01", "ny_local"),
                                 ("2016-01", "berlin_minus_6"))),
    "bad_zone": dict(tick=(("2000-01", "mars"),), rules=[("mars", "Mars/Olympus_Mons", 0)]),
    "from_not_month_boundary": dict(tick=(("2000-01", "ny_local"), ("2016-01-15", "berlin_minus_6"))),
    "from_bad_month": dict(tick=(("2000-13", "berlin_minus_6"),)),
}


@pytest.mark.parametrize("case", sorted(BAD_CONFIGS))
def test_config_validation_errors_raise(tmp_path, case):
    p = _write_cfg(tmp_path / f"{case}.yaml", **BAD_CONFIGS[case])
    with pytest.raises(ValueError):
        fh.load_tz_config(p)


def test_env_override(tmp_path, monkeypatch):
    p = _write_cfg(tmp_path / "ny.yaml", (("2000-01", "ny_local"),))
    monkeypatch.setenv(ENV, str(p))
    assert fh.rule_for("tick", (2024, 3), fh.load_tz_config()).name == "ny_local"
    assert fh.rule_for("tick", (2024, 3)).name == "ny_local"                  # cfg=None
    monkeypatch.delenv(ENV)
    assert fh.rule_for("tick", (2024, 3), fh.load_tz_config()).name == "berlin_minus_6"


def test_rule_switch_applies_at_raw_month_boundary_end_to_end(tmp_path, monkeypatch):
    """ny_local bis Rohmonat 2024-03, berlin_minus_6 ab 2024-04 (per Env-Konfiguration).
    UTC-April = letzte Stunden von Rohmonat Maerz (ny_local!) + Rohmonat April
    (berlin_minus_6): jeder Rohmonat wird ganz mit seiner Regel umgerechnet."""
    p = _write_cfg(tmp_path / "switch.yaml", (("2000-01", "ny_local"), ("2024-04", "berlin_minus_6")))
    monkeypatch.setenv(ENV, str(p))
    st = {(2024, 1): "ny_local", (2024, 2): "ny_local", (2024, 3): "ny_local",
          (2024, 4): "berlin_minus_6", (2024, 5): "berlin_minus_6"}
    root = tmp_path / "data"
    seed_raw(root, [(2024, 1), (2024, 5)], stamping=lambda ym: st[ym])
    site = Site({("XAUUSD", f"{y}{m:02d}"): tick_zip("XAUUSD", y, m, st[(y, m)])
                 for (y, m) in ((2024, 2), (2024, 3), (2024, 4))})
    rep = do_run(root, site, (2024, 3), (2024, 4))
    assert rep["exit_code"] == 0, rep.get("quarantined") or rep
    assert site.requests == 6
    for ym, raws in (((2024, 3), {(2024, 2): "ny_local", (2024, 3): "ny_local"}),
                     ((2024, 4), {(2024, 3): "ny_local", (2024, 4): "berlin_minus_6"})):
        assert set(int(x) for x in bars_ms(store_file(root, "XAUUSD", *ym))) == \
            expected_minutes("XAUUSD", ym, raws), ym
    assert "ny_local" in label_of(store_file(root, "XAUUSD", 2024, 3))
    # Gegenprobe: dieselben NY-gestempelten Rohdaten unter der Default-Konfiguration
    monkeypatch.delenv(ENV)
    root2 = tmp_path / "data2"
    seed_raw(root2, [(2024, 1), (2024, 2), (2024, 3), (2024, 4)], stamping=lambda ym: st[ym])
    rep = do_run(root2, Site(), (2024, 3))
    assert rep["exit_code"] == 4, rep
    assert not store_file(root2, "XAUUSD", 2024, 3).exists()


# ==================================================================== 5 ====
def _check_march_breaks(ts: set):
    def has(*a):
        return utc_dt(*a) in ts

    def none_in(lo, hi):
        return not any(lo <= x < hi for x in ts)

    for d in (11, 12, 13, 14, 18, 19, 20, 21, 25, 26, 27, 28):   # Pause 21-22 UTC = 17-18 EDT
        assert has(2024, 3, d, 20, 59) and has(2024, 3, d, 22, 0), d
        assert none_in(utc_dt(2024, 3, d, 21), utc_dt(2024, 3, d, 22)), d
    for d in (4, 5, 6, 7):                                       # vor 10.03: 22-23 UTC = 17-18 EST
        assert has(2024, 3, d, 21, 59) and has(2024, 3, d, 23, 0), d
        assert none_in(utc_dt(2024, 3, d, 22), utc_dt(2024, 3, d, 23)), d
    # Wochenenden: Fr 22:00 UTC (EST) bzw. 21:00 UTC (EDT) zu, So 23:00 / 22:00 UTC auf
    assert has(2024, 3, 8, 21, 59) and none_in(utc_dt(2024, 3, 8, 22), utc_dt(2024, 3, 10, 22))
    assert has(2024, 3, 10, 22, 0)
    assert has(2024, 3, 29, 20, 59) and none_in(utc_dt(2024, 3, 29, 21), utc_dt(2024, 3, 31, 22))
    assert has(2024, 3, 31, 22, 0) and has(2024, 3, 31, 23, 59)


def test_end_to_end_march_2024_default_config(tmp_path):
    root = tmp_path
    seed_raw(root, [(2024, 1), (2024, 4)])                         # Kontext, kein Download
    site = Site({("XAUUSD", "202402"): tick_zip("XAUUSD", 2024, 2),
                 ("XAUUSD", "202403"): tick_zip("XAUUSD", 2024, 3)})
    rep = do_run(root, site, (2024, 3))
    assert rep["exit_code"] == 0, rep.get("quarantined") or rep
    assert site.requests == 4
    out = store_file(root, "XAUUSD", 2024, 3)
    df = pd.read_parquet(out)
    ts = set(int(x) for x in bars_ms(out))
    assert ts == expected_minutes("XAUUSD", (2024, 3), {(2024, 2): "berlin_minus_6",
                                                        (2024, 3): "berlin_minus_6"})
    _check_march_breaks(ts)
    # Monatsrand: UTC 01.03 00:00-04:59 kommt aus Rohmonat Februar (29.02 19:00-23:59 roh)
    feb = raw_month("XAUUSD", 2024, 2)
    fu = pd.DatetimeIndex(feb["utc"])
    first = df[df["ts"] < pd.Timestamp("2024-03-01 05:00", tz="UTC")]
    assert len(first) == 300
    for _, b in first.iloc[[0, 150, 299]].iterrows():
        sel = feb[np.asarray((fu >= b["ts"]) & (fu < b["ts"] + pd.Timedelta(minutes=1)))]
        assert len(sel) and b["bid_open"] == pytest.approx(sel["bid"].iloc[0])
    assert naive(int(raw_ms_of(feb)[np.searchsorted(fu.as_unit("ms").asi8, utc_dt(2024, 3, 1))])) \
        == datetime(2024, 2, 29, 19, 0)
    assert df["ts"].min() == pd.Timestamp("2024-03-01 00:00", tz="UTC")
    assert df["ts"].max() == pd.Timestamp("2024-03-31 23:59", tz="UTC")
    lab = label_of(out)
    assert "berlin_minus_6" in lab and re.search(rf"v{fh.TZ_CHECK_VERSION}\b", lab), lab
    assert fh.TZ_CHECK_VERSION >= 5


@pytest.mark.parametrize("ym", [(2024, 10), (2024, 11)])
def test_end_to_end_autumn_gap_week(tmp_path, ym):
    """28.-31.10: Pause 21-22 UTC (17-18 EDT), obwohl roh schon 16:00-17:00."""
    seed_raw(tmp_path, [ym_add(ym, k) for k in (-2, -1, 0, 1)])
    rep = do_run(tmp_path, Site(), ym)
    assert rep["exit_code"] == 0, rep.get("quarantined") or rep
    out = store_file(tmp_path, "XAUUSD", *ym)
    ts = set(int(x) for x in bars_ms(out))
    assert ts == expected_minutes("XAUUSD", ym, {ym_add(ym, -1): "berlin_minus_6",
                                                 ym: "berlin_minus_6"})
    if ym == (2024, 10):
        for d in (28, 29, 30, 31):
            assert utc_dt(2024, 10, d, 20, 59) in ts and utc_dt(2024, 10, d, 22, 0) in ts
            assert not any(utc_dt(2024, 10, d, 21) <= x < utc_dt(2024, 10, d, 22) for x in ts)
    else:
        assert utc_dt(2024, 11, 1, 20, 59) in ts                         # Fr 16:59 EDT
        assert not any(utc_dt(2024, 11, 1, 21) <= x < utc_dt(2024, 11, 3, 23) for x in ts)
        assert utc_dt(2024, 11, 3, 23, 0) in ts                          # So 18:00 EST


# ==================================================================== 6 ====
def _rewrite_as_v4(path: Path, ny_shift: bool) -> pd.DataFrame:
    """Store-Datei so umschreiben, wie v4 (Rohzeit = New-York-Ortszeit) sie aus
    denselben Berlin-6-Rohdaten gebaut haette; Label v4."""
    t = pq.read_table(path)
    df = t.to_pandas()
    if ny_shift:
        raw = df["ts"].dt.tz_convert("Europe/Berlin").dt.tz_localize(None) - pd.Timedelta(hours=6)
        df["ts"] = raw.dt.tz_localize("America/New_York").dt.tz_convert("UTC").dt.as_unit("ms")
    meta = dict(t.schema.metadata)
    meta[b"source_timezone"] = V4_NY_LABEL.encode()
    meta[b"tz_right_padding"] = b"true"
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False).replace_schema_metadata(meta), path)
    return df


def test_v4_shifted_store_file_is_retired_and_rebuilt_from_cache(tmp_path):
    seed_raw(tmp_path, [(2024, 1), (2024, 2), (2024, 3), (2024, 4)])
    assert do_run(tmp_path, Site(), (2024, 3))["exit_code"] == 0
    out = store_file(tmp_path, "XAUUSD", 2024, 3)
    good = pd.read_parquet(out)
    assert set(int(x) for x in bars_ms(out)) == expected_minutes(
        "XAUUSD", (2024, 3), {(2024, 2): "berlin_minus_6", (2024, 3): "berlin_minus_6"})
    v4 = _rewrite_as_v4(out, ny_shift=True)
    moved = (v4["ts"] != good["ts"])
    # verschoben genau im Fenster 10.03 07:00 UTC .. 31.03 01:00 UTC, um -1 h
    win = (good["ts"] >= pd.Timestamp("2024-03-10 07:00", tz="UTC")) & \
          (good["ts"] < pd.Timestamp("2024-03-31 01:00", tz="UTC"))
    assert moved.sum() > 1000 and (moved == win).all()
    assert ((good["ts"] - v4["ts"])[moved] == pd.Timedelta(hours=1)).all()
    site = Site()
    rep = do_run(tmp_path, site, (2024, 3))
    assert rep["exit_code"] == 0 and site.requests == 0, rep
    st = stale_files(tmp_path)
    assert len(st) == 1 and "XAUUSD-1m-2024-03.parquet" in st[0].name
    pd.testing.assert_series_equal(pd.read_parquet(st[0])["ts"], v4["ts"], check_names=False)
    pd.testing.assert_frame_equal(pd.read_parquet(out), good)
    lab = label_of(out)
    assert "berlin_minus_6" in lab and f"v{fh.TZ_CHECK_VERSION}" in lab and lab != V4_NY_LABEL
    _check_march_breaks(set(int(x) for x in bars_ms(out)))
    # danach stabil: nichts mehr zu tun
    rep = do_run(tmp_path, site, (2024, 3))
    assert rep["exit_code"] == 0 and site.requests == 0 and len(stale_files(tmp_path)) == 1


def test_v4_shifted_store_file_outside_range_is_retired(tmp_path):
    seed_raw(tmp_path, [(2024, 1), (2024, 2), (2024, 3), (2024, 4)])
    assert do_run(tmp_path, Site(), (2024, 3))["exit_code"] == 0
    out = store_file(tmp_path, "XAUUSD", 2024, 3)
    _rewrite_as_v4(out, ny_shift=True)
    site = Site()
    rep = do_run(tmp_path, site, (2024, 2))                # anderer Monat, Rohmonate gecached
    assert site.requests == 0
    assert not out.exists() and len(stale_files(tmp_path)) == 1, rep


@pytest.mark.parametrize("ym", [(2024, 1), (2024, 7)])
def test_v4_unshifted_store_file_is_relabelled_not_moved(tmp_path, ym):
    seed_raw(tmp_path, [ym_add(ym, k) for k in (-2, -1, 0, 1)])
    assert do_run(tmp_path, Site(), ym)["exit_code"] == 0
    out = store_file(tmp_path, "XAUUSD", *ym)
    good = pd.read_parquet(out)
    _rewrite_as_v4(out, ny_shift=False)
    assert label_of(out) == V4_NY_LABEL
    site = Site()
    rep = do_run(tmp_path, site, ym)
    assert rep["exit_code"] == 0 and site.requests == 0, rep
    assert out.exists() and stale_files(tmp_path) == []
    pd.testing.assert_frame_equal(pd.read_parquet(out), good)
    lab = label_of(out)
    assert "berlin_minus_6" in lab and f"v{fh.TZ_CHECK_VERSION}" in lab
    assert f"{ym[0]}-{ym[1]:02d}" in rep["relabelled_timezone"]


def test_old_v4_tz_quarantine_marker_is_rechecked_under_new_rule(tmp_path):
    seed_raw(tmp_path, [(2024, 1), (2024, 2), (2024, 3), (2024, 4)])
    q = qmarker_path(tmp_path, "XAUUSD", 2024, 3)
    q.write_text(json.dumps({
        "reason": "Zeitzone passt nicht zu America/New_York: 1 Stempelungswechsel: 24 unabhaengige "
                  "Luecken in Folge mit -1 h Abweichung von America/New_York",
        "check": {"status": "failed", "median_deviation_h": -1.0}, "since": "2025-11-01T00:00:00+00:00",
        "accepted": False, "raw_tz": "America/New_York", "tz_check_version": 4}))
    site = Site()
    rep = do_run(tmp_path, site, (2024, 3))
    assert rep["exit_code"] == 0, rep.get("quarantined") or rep
    assert site.requests == 0
    assert rep["summary"]["months_written"] == ["2024-03"]
    assert not q.exists()
    assert set(int(x) for x in bars_ms(store_file(tmp_path, "XAUUSD", 2024, 3))) == expected_minutes(
        "XAUUSD", (2024, 3), {(2024, 2): "berlin_minus_6", (2024, 3): "berlin_minus_6"})


# ==================================================================== 7 ====
def _snapshot(root: Path) -> dict:
    return {str(p.relative_to(root)): p.stat().st_mtime_ns for p in root.rglob("*")
            if p.is_file() and not p.name.startswith(".lock-")}


@pytest.fixture
def no_http(monkeypatch):
    import requests

    def boom(*a, **k):
        raise AssertionError("HTTP-Request in evaluate_raw_month")

    monkeypatch.setattr(requests.Session, "request", boom)
    monkeypatch.setattr(requests.Session, "get", boom)
    monkeypatch.setattr(requests.Session, "post", boom)


def test_evaluate_raw_month_uses_cache_only(tmp_path, no_http):
    seed_raw(tmp_path, [(2024, 2), (2024, 3), (2024, 4)])
    before = _snapshot(tmp_path)
    ok = fh.evaluate_raw_month(tmp_path, "XAUUSD", (2024, 3), mode="tick", rule="berlin_minus_6")
    assert ok["status"] == "ok" and ok["ok"] is True, ok.get("reason")
    assert ok["median_deviation_h"] == 0
    assert ok["cached"] and "neighbours" in ok
    bad = fh.evaluate_raw_month(tmp_path, "XAUUSD", (2024, 3), mode="tick", rule="ny_local")
    assert bad["status"] == "failed" and bad["ok"] is False
    assert "ny_local" in str(bad["rule"])
    dflt = fh.evaluate_raw_month(tmp_path, "XAUUSD", (2024, 3))
    assert dflt["status"] == "ok"
    after = _snapshot(tmp_path)
    assert after == before                                     # keine Marker, kein Store, nichts
    assert not list(tmp_path.rglob("*.quarantine"))


# ==================================================================== 8 ====
def test_eurusd_removed_from_histdata_fetcher(tmp_path, monkeypatch, capsys, caplog, no_http):
    assert "EURUSD" not in fh.SYMBOLS
    assert "XAUUSD" in fh.SYMBOLS and "XAGUSD" in fh.SYMBOLS
    monkeypatch.setenv("MARKETDATA_DATA_ROOT", str(tmp_path))
    try:
        code = fh.main(["--symbol", "EURUSD", "--start", "2024-01", "--end", "2024-01"])
    except SystemExit as e:
        code = e.code
    assert code not in (0, None)
    out = capsys.readouterr()
    text = out.out + out.err + caplog.text
    assert re.search(r"refer", text, re.IGNORECASE), text
    assert not (tmp_path / "metals" / "source=histdata" / "symbol=EURUSD").exists()
    with pytest.raises(ValueError):
        fh.run("EURUSD", date(2024, 1, 1), date(2024, 1, 1), tmp_path, now=lambda: NOW)
