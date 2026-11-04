#!/usr/bin/env python3
"""
Historische Tick-Daten von Dukascopy (XAUUSD, XAGUSD, EURUSD) laden und zu
1-Minuten-Bars mit Bid- UND Ask-OHLC verdichten.

- laedt stuendliche .bi5-Dateien, cached sie roh (Neubau ohne Download)
- schreibt nur ABGESCHLOSSENE UTC-Monate; der laufende Monat wird weder
  geladen noch geschrieben (analog fetch_klines)
- validiert hart, bevor irgendetwas geschrieben wird; weiche Auffaelligkeiten
  (Spread-Ausreisser, Preisspruenge, Minuten ohne Ticks) nur als Report
- schreibt hive-partitioniertes Parquet (zstd), alle Zeiten UTC

Quelle / Format (Dukascopy datafeed, nicht offiziell dokumentiert):
    URL     {BASE}/{SYM}/{YYYY}/{MM0:02d}/{DD:02d}/{HH:02d}h_ticks.bi5
            MONAT 0-BASIERT (Januar = 00), Stunde in UTC.
    Inhalt  LZMA-komprimiert; 0 Bytes = Stunde ohne Ticks; 404 = fehlt.
            Records a 20 Byte, big-endian ('>3I2f'):
              uint32 ms seit Stundenbeginn, uint32 ask, uint32 bid,
              float32 ask_volume, float32 bid_volume
            Preis = int / POINT_DIVISOR[symbol]

Ablage (DATA_ROOT = $MARKETDATA_DATA_ROOT, sonst <repo>/data):
    raw/dukascopy/{SYM}/{YYYY}/{MM}/{DD}/{HH}h_ticks.bi5
        Rohdatei, Monat hier 1-BASIERT (Januar = 01), anders als in der URL.
        Ein 404 wird als leere Markerdatei {HH}h_ticks.404 daneben gecached,
        damit ein Neubau nicht jede fehlende Stunde erneut anfragt. Marker
        loeschen = Stunde beim naechsten Lauf erneut anfragen.
    metals/source=dukascopy/symbol={SYM}/year={YYYY}/{SYM}-1m-{YYYY}-{MM}.parquet
    metals/reports/{SYM}-1m-{YYYY}-{MM}.json     Validierungsreport je Monat

Lizenz: Dukascopy-Nutzungsbedingungen selbst pruefen. Daten (roh wie
abgeleitet) nicht weitergeben und nicht committen (data/ ist git-ignored).

Usage:
    python fetch_dukascopy.py --symbol XAUUSD --start 2019-01
    python fetch_dukascopy.py --symbol EURUSD --start 2024-01 --end 2024-06 --workers 2
    python fetch_dukascopy.py --symbol XAUUSD --start 2024-01 --end 2024-01 --rebuild
"""
from __future__ import annotations

import argparse
import json
import logging
import lzma
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import requests

BASE = "https://datafeed.dukascopy.com/datafeed"
ROOT = Path(__file__).resolve().parents[1]
SOURCE = "dukascopy"
USER_AGENT = "marketdata-pipeline/1.0"

# [Annahme, zu verifizieren] Divisor int -> Preis je Symbol. Nicht offiziell
# dokumentiert, aus verbreiteten Implementierungen uebernommen. Abgesichert
# durch PLAUSIBLE: ein falscher Divisor verschiebt Preise um Zehnerpotenzen
# und fuehrt dort zum harten Abbruch statt zu still verdorbenen Daten.
POINT_DIVISOR = {"XAUUSD": 1000, "XAGUSD": 1000, "EURUSD": 100000}

# Plausibilitaetsbereich je Symbol (alle OHLC-Werte, Bid und Ask). Bewusst
# grob: faengt Divisor-Fehler (Faktor 10+), nicht echte Marktbewegungen.
PLAUSIBLE = {"XAUUSD": (200.0, 20000.0), "XAGUSD": (3.0, 500.0), "EURUSD": (0.5, 2.0)}

# Zweite, engere Absicherung gegen Zehnerpotenz-Fehler: grobe Jahres-
# Durchschnittspreise aus allgemeinem Vorwissen (NICHT aus diesen Daten,
# gerundet, 2025/2026 unsicher). Der Monatsmedian des Mid muss innerhalb
# Faktor ANCHOR_BAND davon liegen. Ein Divisor-Fehler (Faktor 10) faellt
# damit sicher auf, echte Jahresbewegungen (selbst +/-60 %) nicht.
ANCHOR_BAND = 2.5
ANCHORS = {
    "XAUUSD": {2003: 360, 2004: 410, 2005: 445, 2006: 605, 2007: 695, 2008: 870,
               2009: 970, 2010: 1225, 2011: 1570, 2012: 1670, 2013: 1410, 2014: 1265,
               2015: 1160, 2016: 1250, 2017: 1255, 2018: 1270, 2019: 1390, 2020: 1770,
               2021: 1800, 2022: 1800, 2023: 1940, 2024: 2390, 2025: 3400, 2026: 4200},
    "XAGUSD": {2003: 4.9, 2004: 6.7, 2005: 7.3, 2006: 11.5, 2007: 13.4, 2008: 15.0,
               2009: 14.7, 2010: 20.2, 2011: 35.1, 2012: 31.2, 2013: 23.8, 2014: 19.1,
               2015: 15.7, 2016: 17.1, 2017: 17.0, 2018: 15.7, 2019: 16.2, 2020: 20.5,
               2021: 25.1, 2022: 21.8, 2023: 23.4, 2024: 28.3, 2025: 40.0, 2026: 70.0},
    "EURUSD": {},   # PLAUSIBLE (0.5..2.0) ist fuer EURUSD bereits eng genug
}

RECORD = np.dtype([("ms", ">u4"), ("ask", ">u4"), ("bid", ">u4"),
                   ("ask_volume", ">f4"), ("bid_volume", ">f4")])   # == '>3I2f'
assert RECORD.itemsize == 20
HOUR_MS = 3_600_000

TIMEOUT_S = 30
MAX_RETRIES = 5            # Versuche insgesamt = MAX_RETRIES + 1
BACKOFF_S = 1.0            # Wartezeit vor Retry n: BACKOFF_S * 2**n
RETRY_STATUS = {429, 500, 502, 503, 504}
PAUSE_S = 0.25             # Pause je Worker nach jedem echten Request

MAX_CROSSED_SHARE = 0.001  # > 0.1 % gekreuzte Bars (ask_close < bid_close) -> Abbruch
SPREAD_OUTLIER_X = 20.0    # weich: Spread > 20x Monatsmedian
JUMP_THRESHOLD = 0.02      # weich: |Mid-Rendite 1m| > 2 %
N_EXAMPLES = 10

BID = ["bid_open", "bid_high", "bid_low", "bid_close"]
ASK = ["ask_open", "ask_high", "ask_low", "ask_close"]
COLUMNS = ["ts", *BID, *ASK, "tick_count", "bid_volume", "ask_volume", "source"]
NUMERIC = [*BID, *ASK, "tick_count", "bid_volume", "ask_volume"]

log = logging.getLogger("dukascopy")
_sleep = time.sleep        # in Tests ersetzbar


def default_data_root() -> Path:
    env = os.environ.get("MARKETDATA_DATA_ROOT")
    return Path(env) if env else ROOT / "data"


class ValidationError(Exception):
    pass


class DownloadError(Exception):
    pass


# ------------------------------------------------------------------ pfade ---
def url_for(symbol: str, hour: datetime) -> str:
    # Monat in der URL 0-basiert (Januar = 00)
    return (f"{BASE}/{symbol}/{hour.year:04d}/{hour.month - 1:02d}/"
            f"{hour.day:02d}/{hour.hour:02d}h_ticks.bi5")


def raw_path(data_root: Path, symbol: str, hour: datetime) -> Path:
    # Monat im Cache 1-basiert (Januar = 01), damit Verzeichnisse lesbar sind
    return (data_root / "raw" / "dukascopy" / symbol / f"{hour.year:04d}" /
            f"{hour.month:02d}" / f"{hour.day:02d}" / f"{hour.hour:02d}h_ticks.bi5")


def store_path(data_root: Path, symbol: str, y: int, m: int) -> Path:
    return (data_root / "metals" / f"source={SOURCE}" / f"symbol={symbol}" /
            f"year={y}" / f"{symbol}-1m-{y}-{m:02d}.parquet")


def report_path(data_root: Path, symbol: str, y: int, m: int) -> Path:
    return data_root / "metals" / "reports" / f"{symbol}-1m-{y}-{m:02d}.json"


# --------------------------------------------------------------- download ---
def download_bi5(url: str, retries: int = MAX_RETRIES,
                 backoff: float = BACKOFF_S) -> bytes | None:
    """
    Laedt eine .bi5-Datei. None bei 404, b"" bei leerer Datei (keine Ticks).
    Retries mit Exponential-Backoff bei Netzfehlern, 429/5xx und bei nicht
    dekomprimierbarem Inhalt (abgeschnittene Uebertragung). Andere HTTP-
    Fehler brechen sofort ab.
    """
    err: Exception | None = None
    for attempt in range(retries + 1):
        try:
            r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=TIMEOUT_S)
        except requests.RequestException as e:
            err = e
        else:
            if r.status_code == 404:
                return None
            if r.status_code == 200:
                blob = r.content
                if not blob:
                    return b""
                try:
                    lzma.decompress(blob, format=lzma.FORMAT_AUTO)
                    return blob
                except lzma.LZMAError as e:
                    err = e
            elif r.status_code in RETRY_STATUS:
                err = DownloadError(f"HTTP {r.status_code}")
            else:
                raise DownloadError(f"HTTP {r.status_code}: {url}")
        if attempt < retries:
            wait = backoff * 2 ** attempt
            log.debug("retry %d/%d in %.1fs %s (%s)", attempt + 1, retries, wait, url, err)
            _sleep(wait)
    raise DownloadError(f"{url}: {retries + 1} Versuche fehlgeschlagen ({err})")


def _atomic_write(dest: Path, blob: bytes) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.name + ".part")
    tmp.write_bytes(blob)
    os.replace(tmp, dest)


def get_hour(data_root: Path, symbol: str, hour: datetime,
             pause: float = PAUSE_S) -> tuple[str, bytes | None]:
    """
    Rohdatei einer Stunde aus dem Cache oder per Download.
    Rueckgabe (status, blob): status in {cached, cached_404, ok, 404}.
    """
    dest = raw_path(data_root, symbol, hour)
    marker = dest.with_name(f"{hour.hour:02d}h_ticks.404")
    if dest.exists():
        return "cached", dest.read_bytes()
    if marker.exists():
        return "cached_404", None

    blob = download_bi5(url_for(symbol, hour))
    if pause:
        _sleep(pause)
    if blob is None:
        _atomic_write(marker, b"")
        return "404", None
    _atomic_write(dest, blob)
    return "ok", blob


# ---------------------------------------------------------------- parsing ---
def decode_bi5(blob: bytes | None, hour_start_ms: int, divisor: int) -> pd.DataFrame:
    """Rohdatei einer Stunde -> Ticks (ts UTC ms, bid, ask, Volumina)."""
    raw = lzma.decompress(blob, format=lzma.FORMAT_AUTO) if blob else b""
    if len(raw) % RECORD.itemsize:
        raise ValidationError(f"bi5-Laenge {len(raw)} kein Vielfaches von {RECORD.itemsize}")
    rec = np.frombuffer(raw, dtype=RECORD)
    if len(rec) and int(rec["ms"].max()) >= HOUR_MS:
        raise ValidationError(f"ms-Offset {int(rec['ms'].max())} >= 1h (Stunde {hour_start_ms})")

    ts = hour_start_ms + rec["ms"].astype(np.int64)
    return pd.DataFrame({
        "ts": pd.to_datetime(ts, unit="ms", utc=True).as_unit("ms"),
        "bid": rec["bid"].astype(np.float64) / divisor,
        "ask": rec["ask"].astype(np.float64) / divisor,
        "bid_volume": rec["bid_volume"].astype(np.float64),
        "ask_volume": rec["ask_volume"].astype(np.float64),
    })


# ------------------------------------------------------------- aggregation ---
def aggregate_1m(ticks: pd.DataFrame) -> pd.DataFrame:
    """
    Ticks -> 1m-Bars, Intervall [t, t+1min), Label links. Nur Minuten mit
    mindestens einem Tick; es wird nichts aufgefuellt.
    """
    if ticks.empty:
        return pd.DataFrame(columns=COLUMNS)

    # stabil sortieren: gleiche Zeitstempel behalten ihre Dateireihenfolge
    t = ticks.sort_values("ts", kind="mergesort")
    g = t.groupby(t["ts"].dt.floor("1min"), sort=True)
    bars = pd.DataFrame({
        "bid_open": g["bid"].first(), "bid_high": g["bid"].max(),
        "bid_low": g["bid"].min(), "bid_close": g["bid"].last(),
        "ask_open": g["ask"].first(), "ask_high": g["ask"].max(),
        "ask_low": g["ask"].min(), "ask_close": g["ask"].last(),
        "tick_count": g.size().astype("int64"),
        "bid_volume": g["bid_volume"].sum(), "ask_volume": g["ask_volume"].sum(),
    })
    bars.index.name = "ts"
    bars = bars.reset_index()
    bars["ts"] = bars["ts"].dt.as_unit("ms")
    bars["source"] = SOURCE
    return bars[COLUMNS]


# -------------------------------------------------------------- validation ---
def _ohlc_bad(df: pd.DataFrame, side: str) -> int:
    o, h, l, c = (df[f"{side}_{k}"] for k in ("open", "high", "low", "close"))
    return int(((l > np.minimum(o, c)) | (h < np.maximum(o, c)) | (h < l)).sum())


def validate(df: pd.DataFrame, symbol: str, label: str,
             month_start: pd.Timestamp, month_end: pd.Timestamp) -> dict:
    """
    Harte Checks werfen ValidationError. Weiche Auffaelligkeiten landen nur im
    Report. Bewusst KEINE harte Lueckengrenze wie bei Krypto: Metalle/FX
    haben Tagespausen und Wochenenden; die Einordnung geplant/ungeplant
    macht der Bot mit seinem Handelskalender.
    """
    if df.empty:
        raise ValidationError(f"{label}: leer")
    rep: dict = {"label": label, "symbol": symbol, "rows": len(df)}

    dupes = int(df["ts"].duplicated().sum())
    if dupes:
        raise ValidationError(f"{label}: {dupes} doppelte ts")
    if not df["ts"].is_monotonic_increasing:
        raise ValidationError(f"{label}: ts nicht monoton")
    if df["ts"].iloc[0] < month_start or df["ts"].iloc[-1] >= month_end:
        raise ValidationError(f"{label}: Bars ausserhalb des Monats")

    nan = int(df[NUMERIC].isna().sum().sum())
    if nan:
        raise ValidationError(f"{label}: {nan} NaN in numerischen Spalten")
    if (df[BID + ASK] <= 0).any().any():
        raise ValidationError(f"{label}: Preise <= 0")
    if (df[["bid_volume", "ask_volume"]] < 0).any().any():
        raise ValidationError(f"{label}: negatives Volumen")
    if (df["tick_count"] < 1).any():
        raise ValidationError(f"{label}: Bar ohne Ticks")
    for side in ("bid", "ask"):
        bad = _ohlc_bad(df, side)
        if bad:
            raise ValidationError(f"{label}: {bad} Zeilen mit inkonsistentem {side}-OHLC")

    lo, hi = PLAUSIBLE[symbol]
    pmin, pmax = float(df[BID + ASK].min().min()), float(df[BID + ASK].max().max())
    if pmin < lo or pmax > hi:
        raise ValidationError(
            f"{label}: Preise {pmin:g}..{pmax:g} ausserhalb Plausibilitaetsbereich "
            f"{lo:g}..{hi:g} - POINT_DIVISOR[{symbol}]={POINT_DIVISOR[symbol]} pruefen")

    anchor = ANCHORS.get(symbol, {}).get(month_start.year)
    if anchor:
        med = float(((df["bid_close"] + df["ask_close"]) / 2).median())
        if not (anchor / ANCHOR_BAND <= med <= anchor * ANCHOR_BAND):
            raise ValidationError(
                f"{label}: Monatsmedian {med:g} weicht mehr als Faktor {ANCHOR_BAND:g} vom "
                f"Jahresanker {anchor:g} ab - POINT_DIVISOR[{symbol}]={POINT_DIVISOR[symbol]} pruefen")
        rep["anchor_ratio"] = med / anchor

    crossed = int((df["ask_close"] < df["bid_close"]).sum())
    rep["crossed"] = crossed
    if crossed / len(df) > MAX_CROSSED_SHARE:
        raise ValidationError(
            f"{label}: {crossed}/{len(df)} Bars mit ask_close < bid_close "
            f"(> {MAX_CROSSED_SHARE:.1%})")

    # ----------------------------------------------- weich: nur Report ---
    def examples(mask: pd.Series, col: pd.Series) -> list[dict]:
        sel = col[mask].abs().nlargest(N_EXAMPLES).index
        return [{"ts": df.at[i, "ts"].isoformat(), "value": float(col.at[i])} for i in sel]

    spread = df["ask_close"] - df["bid_close"]
    med = float(spread.median())
    rep["spread_median"] = med
    out_mask = spread > SPREAD_OUTLIER_X * med if med > 0 else pd.Series(False, index=df.index)
    rep["spread_outliers"] = int(out_mask.sum())
    rep["spread_outlier_examples"] = examples(out_mask, spread)

    mid = (df["ask_close"] + df["bid_close"]) / 2
    ret = mid.pct_change()
    jump_mask = ret.abs() > JUMP_THRESHOLD
    rep["price_jumps"] = int(jump_mask.sum())
    rep["price_jump_examples"] = examples(jump_mask, ret)

    # grobes Raster: alle Minuten Mo-Fr (UTC). Enthaelt Tagespausen und den
    # Freitagabend/Sonntagabend-Rand -> nur Orientierung, kein Fehler.
    grid = pd.date_range(month_start, month_end, freq="1min", inclusive="left")
    grid = grid[grid.dayofweek < 5]
    rep["weekday_grid_minutes"] = int(len(grid))
    rep["weekday_minutes_without_ticks"] = int((~grid.isin(df["ts"])).sum())

    ts = df["ts"].reset_index(drop=True)
    gaps = ts.diff().dt.total_seconds().div(60).fillna(1) - 1
    rep["longest_gaps"] = [
        {"after": ts[i - 1].isoformat(), "next": ts[i].isoformat(), "missing_minutes": int(v)}
        for i, v in gaps.nlargest(N_EXAMPLES).items() if v > 0]

    rep["first"] = df["ts"].iloc[0].isoformat()
    rep["last"] = df["ts"].iloc[-1].isoformat()
    rep["ticks"] = int(df["tick_count"].sum())
    return rep


# ------------------------------------------------------------------ driver ---
def months(start: date, end: date):
    y, m = start.year, start.month
    while (y, m) <= (end.year, end.month):
        yield y, m
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)


def utc_today() -> date:
    return datetime.now(timezone.utc).date()


def month_hours(y: int, m: int) -> list[datetime]:
    t = datetime(y, m, 1, tzinfo=timezone.utc)
    end = datetime(y + (m == 12), m % 12 + 1, 1, tzinfo=timezone.utc)
    out = []
    while t < end:
        out.append(t)
        t += timedelta(hours=1)
    return out


def build_month(data_root: Path, symbol: str, y: int, m: int,
                workers: int = 4, pause: float = PAUSE_S) -> tuple[pd.DataFrame, dict]:
    """Alle Stunden eines Monats holen (Cache/Download) und zu 1m-Bars verdichten."""
    hours = month_hours(y, m)
    with ThreadPoolExecutor(max_workers=max(1, workers)) as ex:
        results = list(ex.map(lambda h: get_hour(data_root, symbol, h, pause), hours))

    divisor = POINT_DIVISOR[symbol]
    frames, stats = [], {"hours": len(hours), "hours_404": 0, "hours_empty": 0,
                         "hours_downloaded": 0}
    for h, (status, blob) in zip(hours, results):
        if status in ("404", "cached_404"):
            stats["hours_404"] += 1
            continue
        if status == "ok":
            stats["hours_downloaded"] += 1
        try:
            tk = decode_bi5(blob, int(h.timestamp() * 1000), divisor)
        except (ValidationError, lzma.LZMAError) as e:
            raise ValidationError(f"{symbol} {h:%Y-%m-%d %H}h ({raw_path(data_root, symbol, h)}): {e}")
        if tk.empty:
            stats["hours_empty"] += 1
        else:
            frames.append(tk)
    if not frames:
        return aggregate_1m(pd.DataFrame()), stats
    return aggregate_1m(pd.concat(frames, ignore_index=True)), stats


def run(symbol: str, start: date, end: date, data_root: Path | None = None,
        today: date | None = None, workers: int = 4, rebuild: bool = False,
        pause: float = PAUSE_S) -> list[dict]:
    if symbol not in POINT_DIVISOR:
        raise ValueError(f"unbekanntes Symbol {symbol}; bekannt: {sorted(POINT_DIVISOR)}")
    data_root = data_root or default_data_root()
    today = today or utc_today()
    reports = []

    for y, m in months(start, end):
        label = f"{symbol} {y}-{m:02d}"
        # Laufender Monat wird NICHT geladen und NICHT geschrieben: eine
        # Teilmonatsdatei saehe aus wie eine fertige und wuerde nie ersetzt.
        if (y, m) >= (today.year, today.month):
            log.info("skip %s (Monat nicht abgeschlossen)", label)
            continue
        out = store_path(data_root, symbol, y, m)
        if out.exists() and not rebuild:
            continue

        df, stats = build_month(data_root, symbol, y, m, workers, pause)
        if df.empty:
            # z.B. vor Beginn der Historie; nichts geschrieben
            log.warning("skip %s (keine Ticks, %d/%d Stunden 404)",
                        label, stats["hours_404"], stats["hours"])
            continue

        ms = pd.Timestamp(y, m, 1, tz="UTC")
        rep = validate(df, symbol, label, ms, ms + pd.offsets.MonthBegin(1))
        rep.update(stats)
        rep["file"] = str(out.relative_to(data_root))

        out.parent.mkdir(parents=True, exist_ok=True)
        tmp = out.with_name(out.name + ".part")
        df.to_parquet(tmp, index=False, compression="zstd")
        os.replace(tmp, out)

        rp = report_path(data_root, symbol, y, m)
        rp.parent.mkdir(parents=True, exist_ok=True)
        rp.write_text(json.dumps(rep, indent=2, ensure_ascii=False) + "\n")
        reports.append(rep)

        log.info("%-15s %6d Bars %8d Ticks  404h=%-3d gekreuzt=%-3d spread>20x=%-4d "
                 "spruenge=%-3d MoFr-ohne-Ticks=%d",
                 label, rep["rows"], rep["ticks"], rep["hours_404"], rep["crossed"],
                 rep["spread_outliers"], rep["price_jumps"],
                 rep["weekday_minutes_without_ticks"])
        if rep["spread_outliers"] or rep["price_jumps"]:
            log.warning("%s: Auffaelligkeiten, siehe %s", label, rp)

    log.info("FERTIG %s: %d Monatsdateien geschrieben", symbol, len(reports))
    return reports


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--symbol", required=True, type=str.upper, choices=sorted(POINT_DIVISOR))
    p.add_argument("--start", required=True, help="YYYY-MM")
    p.add_argument("--end", default=None, help="YYYY-MM (default: heute)")
    p.add_argument("--workers", type=int, default=4, help="parallele Downloads (default 4)")
    p.add_argument("--pause", type=float, default=PAUSE_S,
                   help=f"Pause je Worker nach jedem Request in s (default {PAUSE_S})")
    p.add_argument("--rebuild", action="store_true",
                   help="vorhandene Monatsdateien neu bauen (aus dem Roh-Cache)")
    p.add_argument("-v", "--verbose", action="store_true")
    a = p.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if a.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
    )

    sy, sm = map(int, a.start.split("-"))
    if a.end:
        ey, em = map(int, a.end.split("-"))
    else:
        ey, em = utc_today().year, utc_today().month

    try:
        run(a.symbol, date(sy, sm, 1), date(ey, em, 1), workers=a.workers,
            rebuild=a.rebuild, pause=a.pause)
    except ValidationError as e:
        log.error("VALIDIERUNG FEHLGESCHLAGEN: %s", e)
        return 2
    except DownloadError as e:
        log.error("DOWNLOAD FEHLGESCHLAGEN: %s (Cache bleibt, naechster Lauf setzt fort)", e)
        return 3
    return 0


if __name__ == "__main__":
    sys.exit(main())
