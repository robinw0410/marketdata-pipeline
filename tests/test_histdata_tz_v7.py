"""Regressionstests tz_check v7: Regel berlin_minus_6_utc_midnight (kind "dst_daily_utc").

Befund (Homelab, echte Ticks 2009-2025, XAUUSD/XAGUSD): ny_local bis Rohmonat 2018-11,
ab 2018-12 Offset roh-UTC im UTC-Zeitpunkt t = Europe/Berlin-utcoffset um 00:00 UTC
des UTC-Datums von t, minus 6 h (-5 EU-Winter, -4 EU-Sommer). Der Wechsel liegt um
00:00 UTC am Montag nach dem letzten Sonntag im Maerz bzw. Oktober:
- Oktober: roh So 19:59:59 (= 23:59:59 UTC, -4) -> roh 19:00:00 (= Mo 00:00 UTC, -5);
  roh So 19:00-19:59:59.999 doppelt, aufgeloest nach DATEIREIHENFOLGE (vor dem
  Ruecksprung -4, danach -5); ohne erkennbaren Ruecksprung -> TimezoneError.
- Maerz: roh So 18:59:59 (= 23:59:59 UTC, -5) -> roh 20:00:00 (= Mo 00:00 UTC, -4);
  roh So 19:00-19:59:59.999 existiert nicht -> TimezoneError.
Die Oktober-Rueckspruenge sind echte Sonntagabend-Ticks, keine Wochenend-Streuticks.

Nur synthetische Daten, HTTP gemockt. Erwartete Werte unabhaengig mit zoneinfo/pandas.
Genaue Roh-Zeitstempel je Jahr (ueber das Muster hinaus) liegen nicht vor.
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
NEW, B6, NYL = "berlin_minus_6_utc_midnight", "berlin_minus_6", "ny_local"
SYMS = ("XAUUSD", "XAGUSD")
YEARS = range(2019, 2026)
FALL = {2019: date(2019, 10, 27), 2020: date(2020, 10, 25), 2021: date(2021, 10, 31),
        2022: date(2022, 10, 30), 2023: date(2023, 10, 29), 2024: date(2024, 10, 27),
        2025: date(2025, 10, 26)}
SPRING = {2019: date(2019, 3, 31), 2020: date(2020, 3, 29), 2021: date(2021, 3, 28),
          2022: date(2022, 3, 27), 2023: date(2023, 3, 26), 2024: date(2024, 3, 31),
          2025: date(2025, 3, 30)}
V6_B6_LABEL = "berlin_minus_6 (Europe/Berlin -6 h) -> UTC, je Rohmonat geprueft (tz_check v6)"
V6_UND_LABEL = ("ny_local (America/New_York +0 h) [undecided: ny_local|berlin_minus_6, hier "
                "identisch] -> UTC, je Rohmonat geprueft (tz_check v6)")


@pytest.fixture(autouse=True)
def _no_env_config(monkeypatch):
    monkeypatch.delenv(ENV, raising=False)


@pytest.fixture
def no_http(monkeypatch):
    import requests

    def boom(*a, **k):
        raise AssertionError("HTTP-Request nicht erlaubt")

    for name in ("request", "get", "post"):
        monkeypatch.setattr(requests.Session, name, boom)


# ------------------------------------------------------- referenz (zoneinfo) ---
def last_sunday(y, m):
    d = date(y, m, calendar.monthrange(y, m)[1])
    return d - timedelta(days=(d.weekday() - 6) % 7)


def rms(*a) -> int:
    """Rohzeit-Ziffern -> ms (als UTC gelesen, wie fetch_histdata Rohzeit fuehrt)."""
    return int(datetime(*a, tzinfo=UTC).timestamp() * 1000)


utc_dt = rms


def ms_of(idx) -> np.ndarray:
    return pd.DatetimeIndex(idx).as_unit("ms").asi8


def off_new_h(utc: pd.DatetimeIndex) -> np.ndarray:
    """Referenz der neuen Regel: Berlin-utcoffset um 00:00 UTC des UTC-Datums, minus 6 h."""
    mid = utc.floor("D")
    berlin = mid.tz_convert(BER).tz_localize(None) - mid.tz_localize(None)
    return np.asarray(berlin / pd.Timedelta(hours=1)) - 6


def stamp(utc: pd.DatetimeIndex, stamping: str) -> pd.DatetimeIndex:
    if stamping == NEW:
        return utc.tz_localize(None) + pd.to_timedelta(off_new_h(utc), unit="h")
    if stamping == B6:
        return utc.tz_convert(BER).tz_localize(None) - pd.Timedelta(hours=6)
    assert stamping == NYL
    return utc.tz_convert(NYZ).tz_localize(None)


def new_raw(*utc_parts) -> int:
    """Rohzeit-ms der neuen Regel fuer einen UTC-Zeitpunkt (Referenz)."""
    u = pd.DatetimeIndex([pd.Timestamp(datetime(*utc_parts, tzinfo=UTC))])
    return int(ms_of(stamp(u, NEW))[0])


def ref_convert_unique(raw_ms: np.ndarray, stamping: str):
    """Referenz-Umrechnung roh -> UTC fuer EINDEUTIGE Rohzeiten (sonst valid False)."""
    idx = pd.to_datetime(np.asarray(raw_ms, dtype=np.int64), unit="ms")
    if stamping == NEW:
        out = np.zeros(len(idx), dtype=np.int64)
        n = np.zeros(len(idx), dtype=int)
        for off in (-4, -5):
            u = (idx - pd.Timedelta(hours=off)).tz_localize("UTC")
            ok = off_new_h(u) == off
            out[ok] = ms_of(u[ok])
            n += ok
        return out, n == 1
    zone, shift = (BER, 6) if stamping == B6 else (NYZ, 0)
    loc = (idx + pd.Timedelta(hours=shift)).tz_localize(zone, ambiguous="NaT", nonexistent="NaT")
    valid = np.asarray(~loc.isna())
    out = np.zeros(len(idx), dtype=np.int64)
    out[valid] = ms_of(loc[valid].tz_convert("UTC"))
    return out, valid


def rule(name):
    return fh.TZ_RULES[name]


# ------------------------------------------------------------ synthetik ---
def _closed(idx: pd.DatetimeIndex) -> np.ndarray:
    ny = idx.tz_convert(NYZ)
    wd, h = ny.dayofweek, ny.hour
    return np.asarray(((wd == 4) & (h >= 17)) | (wd == 5) | ((wd == 6) & (h < 18))
                      | ((wd <= 3) & (h == 17)))


def market_utc(lo, hi) -> pd.DatetimeIndex:
    grid = pd.date_range(lo, hi, freq="60s", inclusive="left", tz="UTC")
    openg = grid[~_closed(grid)]
    last = openg[_closed(openg + pd.Timedelta(seconds=60))]
    return openg.append(last + pd.Timedelta(seconds=59)).sort_values()


def dense_sunday_utc(sun: date) -> pd.DatetimeIndex:
    """Dichte Ticks So 22:00 UTC (Oeffnung 18:00 EDT) .. Mo 01:00 UTC: alle 3 s, dazu
    23:59:59 und Mo 00:00:00 (= roh 19:59:59 -> 19:00:00 bzw. 18:59:59 -> 20:00:00)."""
    lo = pd.Timestamp(datetime(sun.year, sun.month, sun.day, 22, tzinfo=UTC))
    g = pd.date_range(lo, lo + pd.Timedelta(hours=3), freq="3s", inclusive="left")
    return g.append(pd.DatetimeIndex([lo + pd.Timedelta(seconds=7199),
                                      lo + pd.Timedelta(hours=2)]))


@functools.lru_cache(maxsize=None)
def raw_month(symbol, y, m, stamping=NEW, dense=True) -> pd.DataFrame:
    """Ticks eines ROH-Monats in DATEIREIHENFOLGE (= UTC-Reihenfolge): raw (naiv), utc,
    bid, ask. dense: dichte Sonntagabend-Ticks in der EU-Umstellungsnacht. Nicht veraendern."""
    start = pd.Timestamp(y, m, 1)
    end = start + pd.offsets.MonthBegin(1)
    utc = market_utc(start.tz_localize("UTC") - pd.Timedelta(days=2),
                     end.tz_localize("UTC") + pd.Timedelta(days=2))
    if dense and m in (3, 10):
        utc = utc.append(dense_sunday_utc(last_sunday(y, m)))
    utc = pd.DatetimeIndex(np.unique(ms_of(utc)).astype("datetime64[ms]")).tz_localize("UTC")
    utc = utc[~_closed(utc.floor("min"))]
    raw = stamp(utc, stamping)
    keep = np.asarray((raw >= start) & (raw < end))
    raw, utc = raw[keep], utc[keep]                 # utc aufsteigend = Dateireihenfolge
    i = (ms_of(utc) // 1000).astype(float)
    bid = np.round(BASE_PRICE[symbol] * (1 + 0.002 * np.sin(i / 5820.0)), 4)
    return pd.DataFrame({"raw": raw, "utc": utc, "bid": bid, "ask": bid + SPREAD[symbol]})


def raw_ms_of(df) -> np.ndarray:
    return ms_of(pd.DatetimeIndex(df["raw"]))


def utc_ms_of(df) -> np.ndarray:
    return ms_of(pd.DatetimeIndex(df["utc"]))


def month_bounds(y, m):
    return rms(y, m, 1), rms(*((y + 1, 1) if m == 12 else (y, m + 1)), 1)


def ym_add(ym, k):
    i = ym[0] * 12 + ym[1] - 1 + k
    return i // 12, i % 12 + 1


def neighbours(symbol, ym, stamping=NEW, pad_days=8):
    prev = raw_ms_of(raw_month(symbol, *ym_add(ym, -1), stamping))
    nxt = raw_ms_of(raw_month(symbol, *ym_add(ym, 1), stamping))
    return (prev[prev >= prev.max() - pad_days * DAY_MS], nxt[nxt <= nxt.min() + pad_days * DAY_MS])


def with_extra(df: pd.DataFrame, raws_ms: list[int], after_ms: int, symbol="XAUUSD") -> pd.DataFrame:
    """Ticks raws_ms (in dieser Reihenfolge) direkt nach dem letzten Tick mit roh <= after_ms
    in Dateireihenfolge einfuegen (Spalte utc entfaellt)."""
    ms = raw_ms_of(df)
    pos = int(np.nonzero(ms <= after_ms)[0].max()) + 1
    p = BASE_PRICE[symbol]
    extra = pd.DataFrame({"raw": pd.to_datetime(np.array(raws_ms, dtype=np.int64), unit="ms"),
                          "bid": p, "ask": p + SPREAD[symbol]})
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
def tick_zip(symbol, y, m, stamping=NEW) -> bytes:
    return zip_of(symbol, y, m, raw_month(symbol, y, m, stamping))


# --------------------------------------------------------------- pfade ---
def raw_zip_path(root: Path, sym, y, m) -> Path:
    return (root / "metals" / "raw" / "source=histdata" / f"symbol={sym}" / "mode=tick"
            / f"HISTDATA_COM_ASCII_{sym}_T{y}{m:02d}.zip")


def store_file(root: Path, sym, y, m) -> Path:
    return (root / "metals" / "source=histdata" / f"symbol={sym}" / f"year={y}"
            / f"{sym}-1m-{y}-{m:02d}.parquet")


def month_report_file(root: Path, sym, y, m) -> Path:
    return root / "metals" / "reports" / f"{sym}-histdata-1m-{y}-{m:02d}.json"


def write_raw(root, ym, blob, sym="XAUUSD"):
    p = raw_zip_path(root, sym, *ym)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(blob)


def seed_raw(root, months, stamping=NEW, sym="XAUUSD"):
    for ym in months:
        st = stamping(ym) if callable(stamping) else stamping
        write_raw(root, ym, tick_zip(sym, *ym, st), sym)


def markers(root):
    return sorted(root.rglob("*.quarantine"))


def stale_files(root):
    d = root / "metals" / "stale"
    return sorted(d.rglob("*.stale")) if d.exists() else []


def label_of(p: Path) -> str:
    v = (pq.read_schema(p).metadata or {}).get(b"source_timezone")
    return v.decode() if v else ""


def write_cfg(path: Path, tick_lines) -> Path:
    lines = ["rules: []", "schedule:", "  tick:"] + [f"    - {x}" for x in tick_lines] + \
        ["  m1:", '    - {from: "2000-01", rule: fixed_est}']
    path.write_text("\n".join(lines) + "\n")
    return path


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


def bars_of(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path)


def ticks_in_utc_month(sym, ym, raws: dict) -> np.ndarray:
    """Generator-UTC-ms aller Ticks der Rohmonate `raws` ({ym: stamping}) im UTC-Monat ym."""
    lo, hi = month_bounds(*ym)
    u = np.concatenate([utc_ms_of(raw_month(sym, *r, st)) for r, st in raws.items()])
    return u[(u >= lo) & (u < hi)]


def nights(rep, day: date):
    return [n for v in find_key(rep, "transition_nights") for n in (v or [])
            if str(n.get("date", "")).startswith(f"{day:%Y-%m-%d}")]


# ==================================================================== 0 ====
@pytest.mark.parametrize("y", list(YEARS))
def test_eu_sundays_verified_with_zoneinfo(y):
    for d, m, before, after in ((FALL[y], 10, 2, 1), (SPRING[y], 3, 1, 2)):
        assert d == last_sunday(y, m) and d.weekday() == 6
        t = datetime(d.year, d.month, d.day, 1, tzinfo=UTC)
        assert (t - timedelta(seconds=1)).astimezone(BER).utcoffset() == timedelta(hours=before)
        assert t.astimezone(BER).utcoffset() == timedelta(hours=after)


@pytest.mark.parametrize("y", list(YEARS))
def test_reference_rule_pattern(y):
    """Selbsttest der Referenz: Muster aus den Homelab-Daten."""
    sun, spr = FALL[y], SPRING[y]
    mon = sun + timedelta(days=1)
    assert new_raw(sun.year, 10, sun.day, 23, 59, 59) == rms(sun.year, 10, sun.day, 19, 59, 59)
    assert new_raw(mon.year, mon.month, mon.day, 0, 0, 0) == rms(sun.year, 10, sun.day, 19, 0, 0)
    assert new_raw(sun.year, 10, sun.day, 22) == rms(sun.year, 10, sun.day, 18)    # Oeffnung EDT
    smon = spr + timedelta(days=1)
    assert new_raw(spr.year, 3, spr.day, 22) == rms(spr.year, 3, spr.day, 17)      # Oeffnung EDT
    assert new_raw(spr.year, 3, spr.day, 23, 59, 59) == rms(spr.year, 3, spr.day, 18, 59, 59)
    assert new_raw(smon.year, smon.month, smon.day, 0) == rms(spr.year, 3, spr.day, 20)
    # Jan -5, Jul -4
    assert new_raw(y, 1, 15, 12) == rms(y, 1, 15, 7) and new_raw(y, 7, 15, 12) == rms(y, 7, 15, 8)


# ==================================================================== 1 ====
def test_tz_rule_kind_and_new_builtin_rule():
    assert fh.TZ_CHECK_VERSION == 7
    r = fh.TzRule("x_rule", "Europe/Berlin", -6)
    assert r.kind == "zone_shift"
    new = fh.TZ_RULES[NEW]
    assert (new.name, new.zone, new.shift_h, new.kind) == (NEW, "Europe/Berlin", -6, "dst_daily_utc")
    assert new == fh.TzRule(NEW, "Europe/Berlin", -6, kind="dst_daily_utc")
    assert fh.TZ_RULES[B6].kind == "zone_shift" and fh.TZ_RULES[NYL].kind == "zone_shift"


def test_default_config_v7():
    cfg = fh.load_tz_config()
    for ym in ((2000, 1), (2009, 3), (2015, 10), (2018, 11)):
        assert fh.rule_for("tick", ym, cfg) == rule(NYL), ym
    for ym in ((2018, 12), (2019, 3), (2019, 10), (2021, 2), (2024, 10), (2026, 1)):
        assert fh.rule_for("tick", ym, cfg) == rule(NEW), ym
    names = [getattr(r, "name", None) for _, r in cfg.schedule["tick"]]
    assert names == [NYL, NEW]
    assert [ym for ym, _ in cfg.schedule["tick"]] == [(2000, 1), (2018, 12)]
    assert "undecided" not in names
    assert fh.rule_for("m1", (2024, 7), cfg) == rule("fixed_est")


@pytest.mark.parametrize("y", list(YEARS))
def test_october_fold_resolved_by_file_order(y):
    sun = FALL[y]
    mon = sun + timedelta(days=1)
    utc = [(sun.year, 10, sun.day, 22, 0), (sun.year, 10, sun.day, 23, 30),
           (sun.year, 10, sun.day, 23, 59, 59), (mon.year, mon.month, mon.day, 0, 0),
           (mon.year, mon.month, mon.day, 0, 30), (mon.year, mon.month, mon.day, 1, 0),
           (mon.year, mon.month, mon.day, 15, 0)]
    raw = np.array([new_raw(*u) for u in utc], dtype=np.int64)
    d = sun.day
    assert list(raw) == [rms(y, 10, d, 18), rms(y, 10, d, 19, 30), rms(y, 10, d, 19, 59, 59),
                         rms(y, 10, d, 19), rms(y, 10, d, 19, 30), rms(y, 10, d, 20),
                         rms(mon.year, mon.month, mon.day, 10)]
    got = fh.raw_to_utc_ms(raw, rule(NEW))
    assert got.dtype == np.int64
    assert list(got) == [utc_dt(*u) for u in utc]


@pytest.mark.parametrize("y", list(YEARS))
def test_march_gap(y):
    spr = SPRING[y]
    mon = spr + timedelta(days=1)
    utc = [(y, 3, spr.day, 22), (y, 3, spr.day, 23, 59, 59), (mon.year, mon.month, mon.day, 0),
           (mon.year, mon.month, mon.day, 0, 30)]
    raw = np.array([new_raw(*u) for u in utc], dtype=np.int64)
    assert list(raw) == [rms(y, 3, spr.day, 17), rms(y, 3, spr.day, 18, 59, 59),
                         rms(y, 3, spr.day, 20), rms(y, 3, spr.day, 20, 30)]
    assert list(fh.raw_to_utc_ms(raw, rule(NEW))) == [utc_dt(*u) for u in utc]
    for bad in (rms(y, 3, spr.day, 19, 30), rms(y, 3, spr.day, 19), rms(y, 3, spr.day, 19, 59, 59, 999_000)):
        assert not ref_convert_unique(np.array([bad]), NEW)[1][0]
        with pytest.raises(fh.TimezoneError):
            fh.raw_to_utc_ms(np.array([bad], dtype=np.int64), rule(NEW))
        with pytest.raises(fh.TimezoneError):
            fh.raw_to_utc_ms(np.array([raw[0], raw[1], bad, raw[2]], dtype=np.int64), rule(NEW))


@pytest.mark.parametrize("y", list(YEARS))
def test_unchanged_vs_berlin_minus_6_outside_transition_weekends(y):
    """Ueberall gleich wie berlin_minus_6 ausser roh [Sa 20:00, So 20:00) der beiden
    EU-Umstellungswochenenden (dort UTC So 01:00 .. Mo 00:00 anders). Samstag tagsueber
    und alle anderen Sonntage unveraendert. Werte gegen die Referenz."""
    lo, hi = rms(y, 1, 1), rms(y + 1, 1, 1)
    raw = np.arange(lo, hi, 17 * MIN_MS + 371, dtype=np.int64)
    special = np.zeros(len(raw), dtype=bool)
    for d in (FALL[y], SPRING[y]):
        sat = d - timedelta(days=1)
        special |= (raw >= rms(sat.year, sat.month, sat.day, 20)) & (raw < rms(d.year, d.month, d.day, 20))
    want_new, ok_new = ref_convert_unique(raw, NEW)
    _, ok_b6 = ref_convert_unique(raw, B6)
    assert ok_new[~special].all()
    sel = ~special & ok_b6
    a = fh.raw_to_utc_ms(raw[sel], rule(NEW))
    b = fh.raw_to_utc_ms(raw[sel], rule(B6))
    assert np.array_equal(a, b)
    assert np.array_equal(a, want_new[sel])
    # Samstag tagsueber vor den Umstellungen ausdruecklich
    for d in (FALL[y], SPRING[y]):
        sat = d - timedelta(days=1)
        r = np.arange(rms(sat.year, sat.month, sat.day), rms(sat.year, sat.month, sat.day, 20),
                      7 * MIN_MS, dtype=np.int64)
        assert np.array_equal(fh.raw_to_utc_ms(r, rule(NEW)), fh.raw_to_utc_ms(r, rule(B6)))
    # im Sonderbereich: eindeutige Rohzeiten nach Referenz (Sonntag tagsueber anders als B6)
    sp = special & ok_new & ~(((raw - rms(y, 1, 1)) % DAY_MS >= 19 * HOUR_MS)
                              & ((raw - rms(y, 1, 1)) % DAY_MS < 20 * HOUR_MS))
    got = fh.raw_to_utc_ms(raw[sp], rule(NEW))
    assert np.array_equal(got, want_new[sp])
    sun_day = sp & ok_b6 & (((raw // DAY_MS) + 3) % 7 == 6)
    assert sun_day.any()
    assert (fh.raw_to_utc_ms(raw[sun_day], rule(B6)) != want_new[sun_day]).all()


# ==================================================================== 2 ====
@pytest.mark.parametrize("y", [2019, 2024])
def test_fold_without_reversal_raises(y):
    d = FALL[y]

    def r(h, mi, s=0):
        return rms(y, 10, d.day, h, mi, s)

    only_second = np.array([r(18, 59), r(19, 10), r(19, 40), r(20, 10)], dtype=np.int64)
    only_first = np.array([r(18, 59), r(19, 10), r(19, 40), r(19, 59, 59), r(20, 10)], dtype=np.int64)
    for arr in (only_second, only_first, np.array([r(19, 30)], dtype=np.int64)):
        with pytest.raises(fh.TimezoneError):
            fh.raw_to_utc_ms(arr, rule(NEW))
    # kleiner Ruecksprung (< 30 min) im Fenster ist KEIN Umstellungs-Ruecksprung
    small = np.array([r(19, 40), r(19, 50), r(19, 30), r(19, 45)], dtype=np.int64)
    with pytest.raises(fh.TimezoneError):
        fh.raw_to_utc_ms(small, rule(NEW))


@pytest.mark.parametrize("y", [2019, 2024])
def test_fold_with_reversal_keeps_all_ticks_monotonic(y):
    d = FALL[y]
    lo = pd.Timestamp(datetime(y, 10, d.day, 21, tzinfo=UTC))
    utc = pd.date_range(lo, lo + pd.Timedelta(hours=5), freq="10s", inclusive="left")
    raw = ms_of(stamp(utc, NEW))
    assert (np.diff(raw) < 0).sum() == 1                         # genau ein Ruecksprung
    got = fh.raw_to_utc_ms(raw, rule(NEW))
    assert len(got) == len(utc)
    assert np.array_equal(got, ms_of(utc))
    assert (np.diff(got) > 0).all()
    # duenn: zwei Ticks je Durchgang, Ruecksprung 45 min
    thin = np.array([rms(y, 10, d.day, 19, 40), rms(y, 10, d.day, 19, 50),
                     rms(y, 10, d.day, 19, 5), rms(y, 10, d.day, 19, 20)], dtype=np.int64)
    mon = d + timedelta(days=1)
    assert list(fh.raw_to_utc_ms(thin, rule(NEW))) == [
        utc_dt(y, 10, d.day, 23, 40), utc_dt(y, 10, d.day, 23, 50),
        utc_dt(mon.year, mon.month, mon.day, 0, 5), utc_dt(mon.year, mon.month, mon.day, 0, 20)]


# ==================================================================== 3 ====
def _generator_selftest(sym, y):
    df = raw_month(sym, y, 10, NEW)
    raw, utc = raw_ms_of(df), utc_ms_of(df)
    d = FALL[y]
    fold = (raw >= rms(y, 10, d.day, 19)) & (raw < rms(y, 10, d.day, 20))
    assert fold.sum() > 2000                                     # tausende Sonntagabend-Ticks
    big = np.nonzero(np.diff(raw) <= -30 * MIN_MS)[0]
    assert len(big) == 1
    i = int(big[0])
    assert (raw[i], raw[i + 1]) == (rms(y, 10, d.day, 19, 59, 59), rms(y, 10, d.day, 19))
    assert (np.diff(utc) > 0).all()
    return fold


@pytest.mark.parametrize("sym", SYMS)
@pytest.mark.parametrize("y", [2019, 2024])
def test_run_october_fold_written_every_tick_accounted(tmp_path, sym, y):
    fold = _generator_selftest(sym, y)
    seed_raw(tmp_path, [(y, m) for m in (8, 9, 10, 11, 12)], sym=sym)
    site = Site()
    rep = do_run(tmp_path, site, (y, 10), (y, 11), sym=sym)
    assert rep["exit_code"] == 0, rep.get("quarantined") or rep
    assert site.requests == 0
    assert rep["summary"]["months_written"] == [f"{y}-10", f"{y}-11"]
    assert markers(tmp_path) == []
    for ym in ((y, 10), (y, 11)):
        out = store_file(tmp_path, sym, *ym)
        bars = bars_of(out)
        want = ticks_in_utc_month(sym, ym, {ym_add(ym, -1): NEW, ym: NEW})
        assert int(bars["tick_count"].sum()) == len(want), ym
        assert set(ms_of(bars["ts"])) == set(int(x) for x in (want // MIN_MS) * MIN_MS)
        assert NEW in label_of(out) and "v7" in label_of(out)
    # Sonntagabend: Minuten So 22:00 .. Mo 00:59 UTC lueckenlos, je 20 Ticks (alle 3 s)
    d = FALL[y]
    bars = bars_of(store_file(tmp_path, sym, y, 10)).set_index("ts")
    for h, mi in ((22, 0), (23, 30), (23, 59)):
        assert bars.loc[pd.Timestamp(datetime(y, 10, d.day, h, mi, tzinfo=UTC)), "tick_count"] >= 20
    mon = d + timedelta(days=1)
    nov_or_oct = bars_of(store_file(tmp_path, sym, mon.year, mon.month)).set_index("ts")
    assert nov_or_oct.loc[pd.Timestamp(datetime(mon.year, mon.month, mon.day, 0, 0, tzinfo=UTC)),
                          "tick_count"] >= 20
    # Bericht: Umstellungsnacht ok, erwarteter Ruecksprung, kein grosser Ruecksprung
    mrep = json.loads(month_report_file(tmp_path, sym, y, 10).read_text())
    ns = nights(mrep, d)
    assert ns, mrep
    n = ns[0]
    assert n["kind"] == "fall" and n["status"] == "ok" and n["reversal_found"] is True
    assert n["ticks_in_fold"] == int(fold.sum())
    assert str(n["last_before_raw"]).startswith(f"{d:%Y-%m-%d} 19:59:59")
    assert str(n["first_after_raw"]).startswith(f"{d:%Y-%m-%d} 19:00:00")
    assert "expected" in n
    exp = [x for v in find_key(mrep, "expected_reversals") for x in (v or [])]
    assert exp and any(f"{d:%Y-%m-%d} 19:59:59" in json.dumps(x) for x in exp)
    assert all(not v for v in find_key(mrep, "large_reversals"))


def test_october_fold_under_berlin_minus_6_or_ny_local_fails(tmp_path, monkeypatch):
    y = 2024
    seed_raw(tmp_path, [(y, m) for m in (8, 9, 10, 11, 12)])
    ny = fh.evaluate_raw_month(tmp_path, "XAUUSD", (y, 10), mode="tick", rule=NYL)
    assert ny["status"] == "failed"
    b6 = fh.evaluate_raw_month(tmp_path, "XAUUSD", (y, 10), mode="tick", rule=B6)
    assert b6["status"] == "failed"
    new = fh.evaluate_raw_month(tmp_path, "XAUUSD", (y, 10), mode="tick", rule=NEW)
    assert new["status"] == "ok", new.get("reason")
    assert new["expected_reversals"]
    assert not new["large_reversals"]
    n = nights(new, FALL[y])
    assert n and n[0]["status"] == "ok"
    # Lauf mit expliziter Konfiguration berlin_minus_6: Quarantaene, nichts geschrieben
    monkeypatch.setenv(ENV, str(write_cfg(tmp_path / "b6.yaml",
                                          ['{from: "2000-01", rule: berlin_minus_6}'])))
    site = Site()
    rep = do_run(tmp_path, site, (y, 10))
    assert rep["exit_code"] == 4, rep
    assert site.requests == 0
    assert not store_file(tmp_path, "XAUUSD", y, 10).exists()
    assert "2024-10" in rep["summary"]["months_quarantine"]


# ==================================================================== 4 ====
@pytest.mark.parametrize("y", [2019, 2024])
def test_run_march_gap_ok_and_injected_gap_tick_quarantined(tmp_path, y):
    spr = SPRING[y]
    df = raw_month("XAUUSD", y, 3, NEW)
    raw = raw_ms_of(df)
    gap = (raw >= rms(y, 3, spr.day, 19)) & (raw < rms(y, 3, spr.day, 20))
    assert gap.sum() == 0
    assert rms(y, 3, spr.day, 18, 59, 59) in set(raw) and rms(y, 3, spr.day, 20) in set(raw)
    root = tmp_path / "ok"
    seed_raw(root, [(y, m) for m in (1, 2, 3, 4)])
    site = Site()
    rep = do_run(root, site, (y, 3))
    assert rep["exit_code"] == 0, rep.get("quarantined") or rep
    assert site.requests == 0
    bars = bars_of(store_file(root, "XAUUSD", y, 3))
    want = ticks_in_utc_month("XAUUSD", (y, 3), {(y, 2): NEW, (y, 3): NEW})
    assert int(bars["tick_count"].sum()) == len(want)
    mrep = json.loads(month_report_file(root, "XAUUSD", y, 3).read_text())
    ns = nights(mrep, spr)
    assert ns, mrep
    n = ns[0]
    assert n["kind"] == "spring" and n["status"] == "ok" and n["ticks_in_gap"] == 0
    assert str(n["last_before_raw"]).startswith(f"{spr:%Y-%m-%d} 18:59:59")
    assert str(n["first_after_raw"]).startswith(f"{spr:%Y-%m-%d} 20:00:00")
    # ein Tick roh So 19:30 (existiert nicht) -> Quarantaene
    root2 = tmp_path / "bad"
    seed_raw(root2, [(y, m) for m in (1, 2, 4)])
    t = rms(y, 3, spr.day, 19, 30)
    write_raw(root2, (y, 3), zip_of("XAUUSD", y, 3, with_extra(df, [t], t - 1)))
    rep = do_run(root2, Site(), (y, 3))
    assert rep["exit_code"] == 4, rep
    assert f"{y}-03" in rep["summary"]["months_quarantine"]
    assert not store_file(root2, "XAUUSD", y, 3).exists()
    assert markers(root2)


# ==================================================================== 5 ====
def survey_cli(tmp_path, monkeypatch, capsys, root, argv):
    monkeypatch.setenv("MARKETDATA_DATA_ROOT", str(root))
    j, md = tmp_path / "survey.json", tmp_path / "survey.md"
    capsys.readouterr()
    code = sv.main(["--symbols", "XAUUSD", "--offline", "--json", str(j), "--markdown", str(md)]
                   + list(argv))
    out = capsys.readouterr().out
    res = json.loads(j.read_text())
    assert res["exit_code"] == code
    return code, res, out, md.read_text()


def month_rows(res):
    return {m["month"]: m for m in res["months"] if m.get("symbol", "XAUUSD") == "XAUUSD"}


def test_survey_finds_new_rule_from_2018_12(tmp_path, monkeypatch, capsys, no_http):
    root = tmp_path / "data"
    for y in (2017, 2018):
        seed_raw(root, [(y, m) for m in (3, 7, 10, 11)], stamping=NYL)
    for y in (2019, 2020):
        seed_raw(root, [(y, m) for m in (3, 7, 10, 11)], stamping=NEW)
    code, res, out, md = survey_cli(tmp_path, monkeypatch, capsys, root,
                                    ["--years", "2017", "2018", "2019", "2020", "--months", "3",
                                     "10", "11", "--controls", "7"])
    rows = month_rows(res)
    # 2019-03: ny_local faellt durch; fixed_est besteht, ist aber durch die Juli-Kontrolle
    # desselben Jahres ausgeschlossen
    r = rows["2019-03"]
    assert r["results"][NYL]["status"] == "failed"
    assert r["results"]["fixed_est"]["status"] == "ok"
    assert r["results"][NEW]["status"] == "ok"
    assert rows["2019-07"]["results"]["fixed_est"]["status"] == "failed"
    assert "fixed_est" in r["excluded"]
    assert NYL not in r["excluded"] and NEW not in r["excluded"]
    assert r["winner"] in (None, B6, NEW) and r["winner"] != "fixed_est"
    # Oktober-Faltung: nur die neue Regel besteht
    for y in (2019, 2020):
        o = rows[f"{y}-10"]
        assert o["decisive"] and o["winner"] == NEW, o
        assert o["results"][B6]["status"] == "failed"
    for y in (2017, 2018):
        assert rows[f"{y}-10"]["winner"] == NYL
    assert [(e["from"], e["rule"]) for e in res["schedule"]] == [("2000-01", NYL), ("2018-12", NEW)]
    assert code == 0, out
    # Umstellungsnaechte: Ausgabe, JSON, Markdown
    for d in (FALL[2019], FALL[2020], SPRING[2019], SPRING[2020]):
        assert f"{d:%Y-%m-%d}" in out
        assert f"{d:%Y-%m-%d}" in md
    tn = res["transition_nights"]
    for d, kind in ((FALL[2019], "fall"), (FALL[2020], "fall"), (SPRING[2019], "spring"),
                    (SPRING[2020], "spring")):
        hit = [x for x in tn if str(x.get("date", "")).startswith(f"{d:%Y-%m-%d}")
               and x.get("rule", NEW) == NEW]
        assert hit and all(x["kind"] == kind and x["status"] == "ok" for x in hit), (d, hit)
    assert re.search(r'from: "2018-12", rule: berlin_minus_6_utc_midnight', out)


def test_survey_preset_full_includes_july_controls(tmp_path, monkeypatch, capsys, no_http):
    root = tmp_path / "data"
    root.mkdir()
    code, res, out, _ = survey_cli(tmp_path, monkeypatch, capsys, root, ["--preset", "full"])
    rows = month_rows(res)
    first = min(ym[0] for ym in fh.HISTORY_START.values())
    need = {f"{y}-{m:02d}" for y in range(first, 2026) for m in (3, 7, 10, 11)}
    assert need <= set(rows)
    assert {rows[f"{y}-07"]["kind"] for y in range(first, 2026)} == {"control"}
    assert {rows[f"{y}-{m:02d}"]["kind"] for y in range(first, 2026) for m in (3, 10, 11)} \
        == {"transition"}
    extra = set(rows) - need                       # weitere Monate nur als Kontrolle (z.B. 01)
    assert all(rows[m]["kind"] == "control" for m in extra), extra
    assert code == 3
    assert res["schedule"] == []


# ==================================================================== 6 ====
def _relabel(path: Path, label: str, ts=None):
    t = pq.read_table(path)
    df = t.to_pandas()
    if ts is not None:
        df["ts"] = ts
    meta = dict(t.schema.metadata)
    meta[b"source_timezone"] = label.encode()
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False).replace_schema_metadata(meta), path)
    return df


def test_v6_berlin_store_file_2019_10_rebuilt_under_new_rule(tmp_path):
    seed_raw(tmp_path, [(2019, m) for m in (8, 9, 10, 11, 12)])
    assert do_run(tmp_path, Site(), (2019, 10))["exit_code"] == 0
    out = store_file(tmp_path, "XAUUSD", 2019, 10)
    good = pd.read_parquet(out)
    # so haette berlin_minus_6 dieselben Rohzeiten gelesen: So-Abend 1 h spaeter
    raw = stamp(pd.DatetimeIndex(good["ts"]), NEW)
    b6, ok = ref_convert_unique(ms_of(raw), B6)
    assert ok.all()
    ts_b6 = pd.Series(pd.to_datetime(b6, unit="ms", utc=True)).dt.as_unit("ms")
    keep = ~ts_b6.duplicated().to_numpy()
    df = _relabel(out, V6_B6_LABEL, ts_b6)
    t = pq.read_table(out)
    pq.write_table(pa.Table.from_pandas(df[keep].reset_index(drop=True), preserve_index=False)
                   .replace_schema_metadata(t.schema.metadata), out)
    old = pd.read_parquet(out)
    assert not old["ts"].equals(good["ts"])
    site = Site()
    rep = do_run(tmp_path, site, (2019, 10))
    assert rep["exit_code"] == 0 and site.requests == 0, rep
    st = stale_files(tmp_path)
    assert len(st) == 1 and "XAUUSD-1m-2019-10.parquet" in st[0].name
    pd.testing.assert_frame_equal(pd.read_parquet(out), good)
    lab = label_of(out)
    assert NEW in lab and "v7" in lab
    rep = do_run(tmp_path, site, (2019, 10))
    assert rep["exit_code"] == 0 and site.requests == 0 and len(stale_files(tmp_path)) == 1


@pytest.mark.parametrize("label", [V6_B6_LABEL, V6_UND_LABEL])
def test_v6_store_file_without_changed_times_is_relabelled(tmp_path, label):
    seed_raw(tmp_path, [(2019, m) for m in (5, 6, 7, 8)])
    assert do_run(tmp_path, Site(), (2019, 7))["exit_code"] == 0
    out = store_file(tmp_path, "XAUUSD", 2019, 7)
    good = pd.read_parquet(out)
    _relabel(out, label)
    site = Site()
    rep = do_run(tmp_path, site, (2019, 7))
    assert rep["exit_code"] == 0 and site.requests == 0, rep
    assert stale_files(tmp_path) == []
    assert "2019-07" in rep["relabelled_timezone"]
    pd.testing.assert_frame_equal(pd.read_parquet(out), good)
    assert NEW in label_of(out) and "v7" in label_of(out)
