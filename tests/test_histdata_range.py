"""Regressionstests: Historienbereich (config/histdata.yaml) und vorlaeufige Quarantaene.

Befunde des Owners (vollstaendiger Historienlauf, Exit 2):
1. XAUUSD 2009-03..05 und XAGUSD 2009-05: in 76-99 % der Bars ask_close < bid_close
   (Bid/Ask in fruehen HistData-Dateien vermutlich vertauscht). Entscheidung:
   Historienbeginn 2009-06 fuer beide Symbole per Konfiguration; fruehere Monate werden
   nie geladen oder geschrieben, nichts wird getauscht oder geraten.
2. 2026-10 (XAU+XAG) in Quarantaene: doppelte Rohstunde 25.10 19:00-19:59 ohne
   Ruecksprung, Beispiel-Ticks alle .000 ms (vermutlich vorlaeufige Dateiversion).
   Entscheidung: konfigurierbares Historienende (vorerst 2026-09); spaetere Monate holt
   metals-fetch, sobald sich die Quarantaene erledigt (ZIP nach 30 Tagen neu laden und
   pruefen); bis dahin kein Exit 2/4.
3. Der Historienlauf endet mit Exit 0, wenn nur diese bekannten Ausnahmen vorliegen.

Nur synthetische Daten, HTTP gemockt. Erwartete Werte unabhaengig (zoneinfo/pandas).
"""
import calendar
import functools
import io
import json
import logging
import re
import zipfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import pytest

import fetch_histdata as fh

UTC = timezone.utc
BER = ZoneInfo("Europe/Berlin")
NYZ = ZoneInfo("America/New_York")
HOUR_MS, DAY_MS, MIN_MS = 3_600_000, 86_400_000, 60_000
# Preisniveau je Jahr = Jahresanker der Plausibilitaetspruefung (fetch_dukascopy.ANCHORS),
# sonst lehnt validate() z.B. 2026er Monate mit 2009er Preisen ab (Faktor > 2.5).
import fetch_dukascopy as dk  # noqa: E402


def base_price(symbol: str, y: int) -> float:
    return float(dk.ANCHORS[symbol][y])


SPREAD = {"XAUUSD": 0.4, "XAGUSD": 0.03}
SYMS = ("XAUUSD", "XAGUSD")
TZ_ENV, RANGE_ENV = "HISTDATA_TZ_CONFIG", "HISTDATA_CONFIG"
NYL, NEW = "ny_local", "berlin_minus_6_utc_midnight"
# frueh: vertauschte Spalten (Befund 1)
SWAPPED = {"XAUUSD": [(2009, 3), (2009, 4), (2009, 5)], "XAGUSD": [(2009, 5)]}
FALL_2026 = date(2026, 10, 25)


@pytest.fixture(autouse=True)
def _default_configs(monkeypatch):
    monkeypatch.delenv(TZ_ENV, raising=False)
    monkeypatch.delenv(RANGE_ENV, raising=False)


# ------------------------------------------------------- referenz (zoneinfo) ---
def last_sunday(y, m):
    d = date(y, m, calendar.monthrange(y, m)[1])
    return d - timedelta(days=(d.weekday() - 6) % 7)


def rms(*a) -> int:
    return int(datetime(*a, tzinfo=UTC).timestamp() * 1000)


def ms_of(idx) -> np.ndarray:
    return pd.DatetimeIndex(idx).as_unit("ms").asi8


def ym_add(ym, k):
    i = ym[0] * 12 + ym[1] - 1 + k
    return i // 12, i % 12 + 1


def ymstr(ym) -> str:
    return f"{ym[0]}-{ym[1]:02d}"


def month_bounds(y, m):
    return rms(y, m, 1), rms(*ym_add((y, m), 1), 1)


def stamp_ms(utc_ms: np.ndarray, stamping: str) -> np.ndarray:
    """UTC-ms -> Roh-ms nach Stempelung (Referenz, nicht fh)."""
    u = pd.to_datetime(utc_ms, unit="ms", utc=True)
    if stamping == NYL:
        return ms_of(u.tz_convert(NYZ).tz_localize(None))
    assert stamping == NEW            # Berlin-utcoffset um 00:00 UTC des UTC-Datums - 6 h
    mid = u.floor("D")
    off = (mid.tz_convert(BER).tz_localize(None) - mid.tz_localize(None)) / pd.Timedelta(hours=1) - 6
    return utc_ms + (np.asarray(off) * HOUR_MS).astype(np.int64)


def _closed(utc_ms: np.ndarray) -> np.ndarray:
    ny = pd.to_datetime(utc_ms, unit="ms", utc=True).tz_convert(NYZ)
    wd, h = ny.dayofweek, ny.hour
    return np.asarray(((wd == 4) & (h >= 17)) | (wd == 5) | ((wd == 6) & (h < 18))
                      | ((wd <= 3) & (h == 17)))


# ------------------------------------------------------------ synthetik ---
@functools.lru_cache(maxsize=None)
def gen_month(symbol, y, m, stamping, *, swapped=False, ms_jitter=True, order="utc",
              dense_fall=False) -> pd.DataFrame:
    """Ticks eines ROH-Monats: raw (ms), utc (ms), bid, ask, in Dateireihenfolge.
    Je offene Minute ein Tick (+ Tick :59 vor jedem Schluss); ms_jitter: krumme ms
    (sonst alle .000); swapped: in 90 % der Ticks ask < bid (vertauschte Spalten);
    dense_fall: dichte Ticks (alle 2 s) So 22:00 .. Mo 01:00 UTC der EU-Herbstnacht;
    order: "utc" (echte Reihenfolge) oder "raw" (nach Rohzeit sortiert = Falte ohne
    Ruecksprung)."""
    lo = rms(y, m, 1) - 3 * DAY_MS
    hi = rms(*ym_add((y, m), 1), 1) + 3 * DAY_MS
    grid = np.arange(lo, hi, MIN_MS, dtype=np.int64)
    grid = grid[~_closed(grid)]
    last = grid[_closed(grid + MIN_MS)] + 59_000
    u = np.concatenate([grid, last])
    if dense_fall:
        sun = last_sunday(y, 10)
        a = rms(sun.year, sun.month, sun.day, 22)
        u = np.concatenate([u, np.arange(a, a + 3 * HOUR_MS, 2000, dtype=np.int64),
                            np.array([a + 2 * HOUR_MS - 1000], dtype=np.int64)])
    u = np.unique(u)
    if ms_jitter:
        u = u + (np.arange(len(u)) * 7919) % 997 + 1          # nie .000
        u = np.unique(u)
    r = stamp_ms(u, stamping)
    a0, b0 = month_bounds(y, m)
    k = (r >= a0) & (r < b0)
    r, u = r[k], u[k]
    if order == "raw":
        o = np.argsort(r, kind="stable")
        r, u = r[o], u[o]
    bid = np.round(base_price(symbol, y) * (1 + 0.002 * np.sin(u / 6.0e6)), 4)
    ask = bid + SPREAD[symbol]
    if swapped:
        flip = (np.arange(len(bid)) % 10) != 0                # 90 % vertauscht
        bid, ask = np.where(flip, ask, bid), np.where(flip, bid, ask)
    return pd.DataFrame({"raw": r, "utc": u, "bid": bid, "ask": ask})


def tick_csv(df) -> bytes:
    st = pd.to_datetime(df["raw"].to_numpy(), unit="ms").strftime("%Y%m%d %H%M%S%f").str[:-3]
    return ("\n".join(f"{s},{b:.5f},{a:.5f},0" for s, b, a in zip(st, df["bid"], df["ask"]))
            + "\n").encode()


@functools.lru_cache(maxsize=None)
def zip_bytes(symbol, y, m, stamping, **kw) -> bytes:
    df = gen_month(symbol, y, m, stamping, **kw)
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(f"DAT_ASCII_{symbol}_T_{y}{m:02d}.csv", tick_csv(df))
        zf.writestr(f"DAT_ASCII_{symbol}_T_{y}{m:02d}.txt", "HistData.com status report\n")
    return buf.getvalue()


def early_zip(symbol, ym):
    """2009: ny_local; vertauscht in den Befund-Monaten."""
    return zip_bytes(symbol, *ym, NYL, swapped=ym in SWAPPED[symbol])


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


def month_report(root: Path, sym, y, m) -> dict:
    return json.loads((root / "metals" / "reports" / f"{sym}-histdata-1m-{y}-{m:02d}.json").read_text())


def run_report(root: Path, sym) -> dict:
    return json.loads((root / "metals" / "reports" / f"{sym}-histdata-run.json").read_text())


def write_raw(root, sym, ym, blob):
    p = raw_zip_path(root, sym, *ym)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(blob)


def store_months(root, sym) -> list[str]:
    d = root / "metals" / "source=histdata" / f"symbol={sym}"
    return sorted(re.search(r"(\d{4}-\d{2})\.parquet$", p.name).group(1) for p in d.rglob("*.parquet")) \
        if d.exists() else []


def stale_files(root):
    d = root / "metals" / "stale"
    return sorted(d.rglob("*.stale")) if d.exists() else []


def bars_ms(path: Path) -> np.ndarray:
    return ms_of(pd.read_parquet(path)["ts"])


def find_key(obj, key):
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


def write_range_cfg(path: Path, start=("2009-06", "2009-06"), end="2026-09", window=60, recheck=30,
                    max_rechecks=3) -> Path:
    path.write_text(
        "history:\n"
        f"  start: {{XAUUSD: \"{start[0]}\", XAGUSD: \"{start[1]}\"}}\n"
        f"  end: \"{end}\"\n"
        "provisional:\n"
        f"  window_days: {window}\n  recheck_days: {recheck}\n  max_rechecks: {max_rechecks}\n")
    return path


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
    """Fake-HistData: files[(SYM, 'YYYYMM')] -> ZIP (sonst leer); periods = angefragte Monate."""

    def __init__(self, files=None):
        self.files, self.calls, self.periods = dict(files or {}), [], []

    def get(self, url, timeout=None, **kw):
        self.calls.append(("GET", url))
        m = re.search(r"/ascii/tick-data-quotes/([a-z]+)/(\d{4})/(\d{1,2})$", url)
        assert m, url
        pair, year, month = m.groups()
        self.periods.append((pair.upper(), int(year), int(month)))
        return Resp(200, PAGE.format(period=f"{year}{int(month):02d}", year=year,
                                     pair=pair.upper()).encode())

    def post(self, url, data=None, headers=None, timeout=None, **kw):
        self.calls.append(("POST", url))
        return Resp(200, self.files.get((data["fxpair"], data["datemonth"]), b""))

    @property
    def requests(self):
        return len(self.calls)


def do_run(root, site, start, end, sym="XAUUSD", now=datetime(2026, 11, 8, 12, tzinfo=UTC)):
    clock = Clock()
    return fh.run(sym, date(*start, 1), date(*end, 1), root, mode="tick",
                  now=lambda: now, pause=0.0, attempts=1, backoff_base=1.0, max_backoff=1.0,
                  jitter=0.0, sleep=clock.sleep, clock=clock, wall=clock, session=site)


def cli(argv, root, site, monkeypatch):
    """fh.main mit gemocktem HTTP (requests.Session.get/post -> Site), ohne Pausen."""
    import requests
    monkeypatch.setenv("MARKETDATA_DATA_ROOT", str(root))
    monkeypatch.setattr(requests.Session, "get", lambda self, url, **kw: site.get(url, **kw))
    monkeypatch.setattr(requests.Session, "post", lambda self, url, **kw: site.post(url, **kw))
    try:
        return fh.main(["--pause", "0", *argv])
    except SystemExit as e:
        return e.code


def expected_minutes(sym, ym, raws: dict) -> set:
    """UTC-Minuten des UTC-Monats ym aus den Generator-UTC-Zeiten der Rohmonate raws."""
    lo, hi = month_bounds(*ym)
    out = set()
    for r, df in raws.items():
        u = df["utc"].to_numpy() // MIN_MS * MIN_MS
        out |= set(int(x) for x in u[(u >= lo) & (u < hi)])
    return out


def early_site(sym):
    return Site({(sym, f"{y}{m:02d}"): early_zip(sym, (y, m))
                 for y, m in [(2009, 3), (2009, 4), (2009, 5), (2009, 6), (2009, 7), (2009, 8)]})


def short_history(tmp_path, monkeypatch, start=("2009-06", "2009-06"), end="2009-07"):
    """Kurzer Historienbereich (gleicher Beginn wie der Default) per HISTDATA_CONFIG."""
    p = write_range_cfg(tmp_path / f"range-{start[0]}-{end}.yaml", start=start, end=end)
    monkeypatch.setenv(RANGE_ENV, str(p))
    return p


# ==================================================================== 0 ====
def test_generator_selftest_swapped_and_good():
    for sym in SYMS:
        for ym in [(2009, 3), (2009, 4), (2009, 5), (2009, 6)]:
            df = gen_month(sym, *ym, NYL, swapped=ym in SWAPPED[sym])
            share = float((df["ask"] < df["bid"]).mean())
            if ym in SWAPPED[sym]:
                assert 0.85 < share < 0.95
            else:
                assert share == 0.0
    # ms: krumm vs .000
    assert float((gen_month("XAUUSD", 2026, 9, NEW)["raw"] % 1000 == 0).mean()) == 0.0
    bad = gen_month("XAGUSD", 2026, 10, NEW, ms_jitter=False, order="raw", dense_fall=True)
    assert float((bad["raw"] % 1000 == 0).mean()) == 1.0
    r = bad["raw"].to_numpy()
    fold = (r >= rms(2026, 10, 25, 19)) & (r < rms(2026, 10, 25, 20))
    assert fold.sum() > 3000
    assert (np.diff(r) >= 0).all()                               # kein Ruecksprung
    good = gen_month("XAGUSD", 2026, 10, NEW, dense_fall=True)
    rg = good["raw"].to_numpy()
    assert (np.diff(rg) <= -30 * MIN_MS).sum() == 1              # echter Ruecksprung


# ==================================================================== 7 ====
def test_default_range_config():
    assert fh.load_range_config() is not None
    assert fh.history_start("XAUUSD") == (2009, 6)
    assert fh.history_start("XAGUSD") == (2009, 6)
    assert fh.HISTORY_START == {"XAUUSD": (2009, 6), "XAGUSD": (2009, 6)}
    p = Path(fh.__file__).resolve().parents[1] / "config" / "histdata.yaml"
    assert p.exists()
    import yaml
    raw = yaml.safe_load(p.read_text())
    assert raw["history"]["start"] == {"XAUUSD": "2009-06", "XAGUSD": "2009-06"}
    assert raw["history"]["end"] == "2026-09"
    assert raw["provisional"] == {"window_days": 60, "recheck_days": 30, "max_rechecks": 3}


def test_range_config_env_override(tmp_path, monkeypatch):
    p = write_range_cfg(tmp_path / "r.yaml", start=("2010-01", "2011-02"), end="2020-12")
    monkeypatch.setenv(RANGE_ENV, str(p))
    fh.load_range_config()
    assert fh.history_start("XAUUSD") == (2010, 1)
    assert fh.history_start("XAGUSD") == (2011, 2)
    assert fh.load_range_config(p) is not None


@pytest.mark.parametrize("case", ["missing", "bad_month_13", "bad_month_short", "bad_text",
                                  "start_after_end", "missing_symbol"])
def test_range_config_validation(tmp_path, monkeypatch, case):
    p = tmp_path / f"{case}.yaml"
    if case == "missing":
        p = tmp_path / "gibt_es_nicht.yaml"
    elif case == "bad_month_13":
        write_range_cfg(p, start=("2009-13", "2009-06"))
    elif case == "bad_month_short":
        write_range_cfg(p, start=("2009-6", "2009-06"))
    elif case == "bad_text":
        write_range_cfg(p, end="september")
    elif case == "start_after_end":
        write_range_cfg(p, start=("2009-06", "2027-01"), end="2026-09")
    elif case == "missing_symbol":
        p.write_text('history:\n  start: {XAUUSD: "2009-06"}\n  end: "2026-09"\n'
                     "provisional:\n  window_days: 60\n  recheck_days: 30\n  max_rechecks: 3\n")
    with pytest.raises(ValueError):
        fh.load_range_config(p)
    monkeypatch.setenv(RANGE_ENV, str(p))
    with pytest.raises(ValueError):
        fh.load_range_config()


# ==================================================================== 1 ====
@pytest.mark.parametrize("sym", SYMS)
def test_history_run_skips_swapped_early_months(tmp_path, monkeypatch, caplog, sym):
    short_history(tmp_path, monkeypatch)
    root = tmp_path / "data"
    write_raw(root, sym, (2009, 8), early_zip(sym, (2009, 8)))          # rechter Nachbar im Cache
    site = early_site(sym)
    code = cli(["--symbol", sym, "--history"], root, site, monkeypatch)
    assert code == 0, run_report(root, sym)
    # nie angefragt: alles vor 2009-06
    assert all((y, m) >= (2009, 6) for _, y, m in site.periods), site.periods
    assert sorted({(y, m) for _, y, m in site.periods}) == [(2009, 6), (2009, 7)]
    assert site.requests == 4
    for ym in [(2009, 3), (2009, 4), (2009, 5)]:
        assert not raw_zip_path(root, sym, *ym).exists()
        assert not store_file(root, sym, *ym).exists()
    assert store_months(root, sym) == ["2009-06", "2009-07"]
    # UTC 2009-06 nur aus Rohmonat 06: die ersten Stunden (roh Mai 20:00-23:59) fehlen
    ts = set(int(x) for x in bars_ms(store_file(root, sym, 2009, 6)))
    jun = gen_month(sym, 2009, 6, NYL)
    assert ts == expected_minutes(sym, (2009, 6), {(2009, 6): jun})
    may_part = expected_minutes(sym, (2009, 6), {(2009, 5): gen_month(sym, 2009, 5, NYL, swapped=True)})
    assert may_part and not (ts & may_part)
    assert min(ts) == rms(2009, 6, 1, 4)                          # roh 01.06 00:00 = 04:00 UTC (EDT)
    rep6 = month_report(root, sym, 2009, 6)
    assert rep6["partial_start"] is True
    assert month_report(root, sym, 2009, 7).get("partial_start") in (None, False)
    rr = run_report(root, sym)
    assert rr["exit_code"] == 0
    assert rr["summary"]["months_written"] == ["2009-06", "2009-07"]


@pytest.mark.parametrize("sym", SYMS)
def test_start_2009_03_config_exits_2_and_data_untouched(tmp_path, monkeypatch, sym):
    """Gegenprobe: mit Historienbeginn 2009-03 sind die fruehen Monate wirklich kaputt
    (Validierung, Exit 2); die Rohdaten werden dabei nicht veraendert (kein Tausch)."""
    short_history(tmp_path, monkeypatch, start=("2009-03", "2009-03"))
    root = tmp_path / "data"
    write_raw(root, sym, (2009, 8), early_zip(sym, (2009, 8)))
    site = early_site(sym)
    code = cli(["--symbol", sym, "--history"], root, site, monkeypatch)
    assert code == 2, run_report(root, sym)
    rr = run_report(root, sym)
    bad = {ymstr(ym) for ym in SWAPPED[sym]}
    assert bad <= set(rr["summary"]["months_validation_error"]), rr["summary"]
    for ym in SWAPPED[sym]:
        assert not store_file(root, sym, *ym).exists()
        assert raw_zip_path(root, sym, *ym).read_bytes() == early_zip(sym, ym)   # unveraendert
    assert "2009-07" in rr["summary"]["months_written"]


@pytest.mark.parametrize("sym", SYMS)
def test_start_before_configured_is_clamped_with_warning(tmp_path, monkeypatch, caplog, sym):
    short_history(tmp_path, monkeypatch)
    root = tmp_path / "data"
    write_raw(root, sym, (2009, 8), early_zip(sym, (2009, 8)))
    site = early_site(sym)
    caplog.set_level(logging.WARNING)
    code = cli(["--symbol", sym, "--start", "2009-03", "--end", "2009-07"], root, site, monkeypatch)
    assert code == 0, run_report(root, sym)
    assert any(r.levelno == logging.WARNING and "2009-06" in r.getMessage() for r in caplog.records), \
        [r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING]
    assert sorted({(y, m) for _, y, m in site.periods}) == [(2009, 6), (2009, 7)]
    assert store_months(root, sym) == ["2009-06", "2009-07"]
    assert month_report(root, sym, 2009, 6)["partial_start"] is True


def test_history_is_exclusive_with_start_end(tmp_path, monkeypatch, capsys):
    short_history(tmp_path, monkeypatch)
    root = tmp_path / "data"
    for extra in (["--start", "2009-06"], ["--end", "2009-07"]):
        site = Site()
        capsys.readouterr()
        code = cli(["--symbol", "XAUUSD", "--history", *extra], root, site, monkeypatch)
        err = capsys.readouterr().err
        assert code == 2
        assert "unrecognized" not in err and "--history" in err, err
        assert site.requests == 0


# ==================================================================== 3 ====
def test_store_file_before_start_is_retired(tmp_path, monkeypatch):
    sym = "XAUUSD"
    short_history(tmp_path, monkeypatch)
    root = tmp_path / "data"
    write_raw(root, sym, (2009, 8), early_zip(sym, (2009, 8)))
    assert cli(["--symbol", sym, "--history"], root, early_site(sym), monkeypatch) == 0
    # alte Datei fuer UTC 2009-05 (z.B. aus einem frueheren Lauf mit Beginn 2009-03)
    old = store_file(root, sym, 2009, 5)
    old.write_bytes(store_file(root, sym, 2009, 6).read_bytes())
    site = Site()
    code = cli(["--symbol", sym, "--history"], root, site, monkeypatch)
    assert code == 0, run_report(root, sym)
    assert site.requests == 0
    assert not old.exists()
    st = stale_files(root)
    assert len(st) == 1 and f"{sym}-1m-2009-05.parquet" in st[0].name
    assert "/metals/stale/" in str(st[0]).replace("\\", "/")
    assert store_months(root, sym) == ["2009-06", "2009-07"]


# ==================================================================== 4 ====
def test_history_end_2026_09_never_requests_2026_10(tmp_path, monkeypatch):
    sym = "XAUUSD"
    short_history(tmp_path, monkeypatch, start=("2026-08", "2026-08"), end="2026-09")
    root = tmp_path / "data"
    files = {(sym, f"2026{m:02d}"): zip_bytes(sym, 2026, m, NEW) for m in (8, 9)}
    files[(sym, "202610")] = zip_bytes(sym, 2026, 10, NEW, ms_jitter=False, order="raw",
                                       dense_fall=True)
    site = Site(files)
    code = cli(["--symbol", sym, "--history"], root, site, monkeypatch)
    assert code == 0, run_report(root, sym)
    assert (sym, 2026, 10) not in site.periods
    assert sorted({(y, m) for _, y, m in site.periods}) == [(2026, 8), (2026, 9)]
    assert store_months(root, sym) == ["2026-08", "2026-09"]
    assert not list(root.rglob("*.quarantine"))


# ==================================================================== 5 ====
T0 = datetime(2026, 11, 8, 12, tzinfo=UTC)


def provisional_files(sym, good_oct=False):
    oct_kw = dict(dense_fall=True) if good_oct else dict(ms_jitter=False, order="raw", dense_fall=True)
    return {(sym, "202609"): zip_bytes(sym, 2026, 9, NEW),
            (sym, "202610"): zip_bytes(sym, 2026, 10, NEW, **oct_kw)}


def seed_provisional(root, sym):
    for m in (7, 8):
        write_raw(root, sym, (2026, m), zip_bytes(sym, 2026, m, NEW))


def prov_entry(rep: dict, month: str):
    v = (rep.get("summary") or {}).get("months_provisional", rep.get("months_provisional"))
    if isinstance(v, dict):
        return v.get(month)
    for e in v or []:
        if isinstance(e, dict) and e.get("month") == month:
            return e
        if e == month:
            return {}
    return None


def parse_iso(s: str) -> datetime:
    return datetime.fromisoformat(str(s))


def first_quarantine(tmp_path, sym):
    root = tmp_path / "data"
    seed_provisional(root, sym)
    site = Site(provisional_files(sym))
    rep = do_run(root, site, (2026, 9), (2026, 10), sym=sym, now=T0)
    return root, site, rep


@pytest.mark.parametrize("sym", SYMS)
def test_provisional_quarantine_first_run(tmp_path, sym):
    root, site, rep = first_quarantine(tmp_path, sym)
    assert rep["exit_code"] == 0, rep.get("quarantined") or rep
    assert site.requests == 4
    e = prov_entry(rep, "2026-10")
    assert e is not None, rep["summary"]
    assert rep["summary"]["months_written"] == ["2026-09"]
    assert not store_file(root, sym, 2026, 10).exists()
    q = qmarker_path(root, sym, 2026, 10)
    info = json.loads(q.read_text())
    prov = info["provisional"]
    since, after = parse_iso(prov["since"]), parse_iso(prov["recheck_after"])
    assert since == T0
    assert after == since + timedelta(days=30)
    assert prov["rechecks"] == 0
    assert parse_iso(e["recheck_after"]) == after
    assert info["fold_unresolved"]["ticks"] > 3000
    assert info["ms_zero_share"] == pytest.approx(1.0, abs=1e-9)
    # ms_zero_share im Monatsreport (UTC 2026-09: krumme ms)
    shares = find_key(month_report(root, sym, 2026, 9), "ms_zero_share")
    assert shares and all(0.0 <= s < 0.01 for s in shares), shares
    ts = set(int(x) for x in bars_ms(store_file(root, sym, 2026, 9)))
    assert ts == expected_minutes(sym, (2026, 9), {(2026, 8): gen_month(sym, 2026, 8, NEW),
                                                   (2026, 9): gen_month(sym, 2026, 9, NEW)})
    assert not rep["quarantined"] or all(x.get("kind") != "quarantine" for x in rep["quarantined"])


@pytest.mark.parametrize("sym", SYMS)
def test_provisional_rerun_before_due_makes_no_requests(tmp_path, sym):
    root, _, _ = first_quarantine(tmp_path, sym)
    q = qmarker_path(root, sym, 2026, 10)
    before = json.loads(q.read_text())
    site = Site(provisional_files(sym, good_oct=True))
    rep = do_run(root, site, (2026, 9), (2026, 10), sym=sym, now=T0 + timedelta(days=12))
    assert rep["exit_code"] == 0, rep
    assert site.requests == 0
    assert prov_entry(rep, "2026-10") is not None
    assert json.loads(q.read_text())["provisional"] == before["provisional"]
    assert not store_file(root, sym, 2026, 10).exists()


@pytest.mark.parametrize("sym", SYMS)
def test_provisional_recheck_after_30_days_with_corrected_zip(tmp_path, sym):
    root, _, _ = first_quarantine(tmp_path, sym)
    write_raw(root, sym, (2026, 11), zip_bytes(sym, 2026, 11, NEW))   # Kontext, kein Download
    site = Site(provisional_files(sym, good_oct=True))
    now = T0 + timedelta(days=30, hours=1)
    # Lauf nur fuer 2026-09 (schon geschrieben): faellige Marker werden trotzdem bearbeitet
    rep = do_run(root, site, (2026, 9), (2026, 9), sym=sym, now=now)
    assert rep["exit_code"] == 0, rep.get("quarantined") or rep
    assert site.requests == 2
    assert site.periods == [(sym, 2026, 10)]
    assert not qmarker_path(root, sym, 2026, 10).exists()
    out = store_file(root, sym, 2026, 10)
    assert out.exists()
    good = gen_month(sym, 2026, 10, NEW, dense_fall=True)
    ts = set(int(x) for x in bars_ms(out))
    assert ts == expected_minutes(sym, (2026, 10), {(2026, 9): gen_month(sym, 2026, 9, NEW),
                                                    (2026, 10): good})
    bars = pd.read_parquet(out)
    lo, hi = month_bounds(2026, 10)
    u = np.concatenate([gen_month(sym, 2026, 9, NEW)["utc"].to_numpy(), good["utc"].to_numpy()])
    assert int(bars["tick_count"].sum()) == int(((u >= lo) & (u < hi)).sum())
    assert raw_zip_path(root, sym, 2026, 10).read_bytes() == provisional_files(sym, True)[(sym, "202610")]
    assert prov_entry(rep, "2026-10") is None


@pytest.mark.parametrize("sym", SYMS)
def test_provisional_bad_three_times_becomes_permanent(tmp_path, sym):
    root, _, _ = first_quarantine(tmp_path, sym)
    write_raw(root, sym, (2026, 11), zip_bytes(sym, 2026, 11, NEW))
    q = qmarker_path(root, sym, 2026, 10)
    now = T0
    codes, rechecks = [], []
    for k in range(1, 4):
        prov = json.loads(q.read_text())["provisional"]
        now = parse_iso(prov["recheck_after"]) + timedelta(hours=1)
        site = Site(provisional_files(sym))                       # dieselbe kaputte Datei
        rep = do_run(root, site, (2026, 9), (2026, 10), sym=sym, now=now)
        assert site.requests == 2, k
        codes.append(rep["exit_code"])
        info = json.loads(q.read_text())
        rechecks.append((info.get("provisional") or {}).get("rechecks", info.get("rechecks")))
        if k < 3:
            assert rep["exit_code"] == 0, (k, rep)
            assert info["provisional"]["rechecks"] == k
            assert parse_iso(info["provisional"]["recheck_after"]) == now + timedelta(days=30) \
                or parse_iso(info["provisional"]["recheck_after"]) > now
            assert prov_entry(rep, "2026-10") is not None
    assert codes[:2] == [0, 0]
    assert rechecks[:2] == [1, 2]
    assert codes[2] == 4, rep
    info = json.loads(q.read_text())
    assert not info.get("provisional"), info                       # normale Quarantaene
    assert "2026-10" in rep["summary"]["months_quarantine"]
    assert prov_entry(rep, "2026-10") is None
    assert not store_file(root, sym, 2026, 10).exists()
    # weitere Laeufe: keine Downloads mehr, Exit 4
    site = Site(provisional_files(sym))
    rep = do_run(root, site, (2026, 9), (2026, 10), sym=sym, now=now + timedelta(days=40))
    assert rep["exit_code"] == 4 and site.requests == 0


# ==================================================================== 6 ====
def test_old_month_quarantine_is_not_provisional(tmp_path):
    sym = "XAUUSD"
    root = tmp_path / "data"
    for m in (1, 2, 4):
        write_raw(root, sym, (2015, m), zip_bytes(sym, 2015, m, NYL))
    df = gen_month(sym, 2015, 3, NYL)
    # 60-min-Ruecksprung an einem Mittwoch (nicht freigebbar)
    t = rms(2015, 3, 11, 12)
    pos = int(np.searchsorted(df["raw"].to_numpy(), t, side="right"))
    extra = pd.DataFrame({"raw": np.arange(t - HOUR_MS, t - HOUR_MS + 6 * 600_000, 600_000) + 123,
                          "utc": 0, "bid": base_price(sym, 2015), "ask": base_price(sym, 2015) + SPREAD[sym]})
    bad = pd.concat([df.iloc[:pos], extra, df.iloc[pos:]], ignore_index=True)
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(f"DAT_ASCII_{sym}_T_201503.csv", tick_csv(bad))
        zf.writestr(f"DAT_ASCII_{sym}_T_201503.txt", "x\n")
    write_raw(root, sym, (2015, 3), buf.getvalue())
    site = Site()
    rep = do_run(root, site, (2015, 3), (2015, 3), sym=sym, now=T0)
    assert rep["exit_code"] == 4, rep
    assert site.requests == 0
    info = json.loads(qmarker_path(root, sym, 2015, 3).read_text())
    assert not info.get("provisional")
    assert "2015-03" in rep["summary"]["months_quarantine"]
    assert prov_entry(rep, "2015-03") is None
    assert "ms_zero_share" in info
