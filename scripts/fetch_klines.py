#!/usr/bin/env python3
"""
Bulk-Download historischer Binance-Klines von data.binance.vision.

- laedt nur Monats-Archive abgeschlossener UTC-Monate; der laufende Monat
  wird gar nicht geschrieben (siehe run)
- verifiziert jede Datei gegen die mitgelieferte .CHECKSUM (SHA256)
- parst robust (Header optional, Timestamps ms ODER us)
- validiert hart, bevor irgendetwas geschrieben wird
- schreibt hive-partitioniertes Parquet

Usage:
    python fetch_klines.py --symbol BTCUSDT --interval 1m --start 2019-01
    python fetch_klines.py --symbol ETHUSDT --interval 1m --start 2019-01 --end 2026-07
"""
from __future__ import annotations

import argparse
import hashlib
import io
import logging
import sys
import zipfile
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
import requests

BASE = "https://data.binance.vision/data/spot"
ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"
STORE = ROOT / "data" / "klines"

COLS = [
    "open_time", "open", "high", "low", "close", "volume", "close_time",
    "quote_volume", "trades", "taker_buy_base", "taker_buy_quote", "ignore",
]
NUMERIC = ["open", "high", "low", "close", "volume", "quote_volume",
           "trades", "taker_buy_base", "taker_buy_quote"]

INTERVAL_MS = {"1m": 60_000, "5m": 300_000, "15m": 900_000,
               "1h": 3_600_000, "4h": 14_400_000, "1d": 86_400_000}

log = logging.getLogger("fetch")


# ---------------------------------------------------------------- download ---
def _get(url: str) -> bytes | None:
    r = requests.get(url, timeout=60)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    return r.content


def download_verified(url: str, dest: Path) -> Path | None:
    """Laedt Archiv + .CHECKSUM und prueft SHA256. None wenn 404."""
    if dest.exists():
        log.debug("cached %s", dest.name)
        return dest

    blob = _get(url)
    if blob is None:
        return None

    chk = _get(url + ".CHECKSUM")
    if chk is None:
        raise RuntimeError(f"Archiv da, aber keine CHECKSUM: {url}")

    expected = chk.decode().split()[0].lower()
    actual = hashlib.sha256(blob).hexdigest()
    if actual != expected:
        raise RuntimeError(f"CHECKSUM-Mismatch {url}\n  exp {expected}\n  got {actual}")

    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(blob)
    log.info("ok   %s (%.1f MB)", dest.name, len(blob) / 1e6)
    return dest


# ------------------------------------------------------------------ parsing ---
def _has_header(first_line: bytes) -> bool:
    """Aeltere Binance-Dumps haben keine Kopfzeile, neuere schon."""
    field = first_line.split(b",")[0].strip().strip(b'"')
    try:
        int(field)
        return False
    except ValueError:
        return True


def _to_utc(s: pd.Series) -> pd.Series:
    """Binance hat irgendwann von ms auf us umgestellt. Einheit messen, nicht raten."""
    probe = int(pd.to_numeric(s, errors="coerce").dropna().abs().max())
    if probe > 1e17:
        unit = "ns"
    elif probe > 1e14:
        unit = "us"
    elif probe > 1e11:
        unit = "ms"
    else:
        raise ValueError(f"Timestamp-Groessenordnung unplausibel: {probe}")
    # einheitliche Aufloesung im Store, sonst kollidieren ms- und us-Dateien beim Join
    return pd.to_datetime(pd.to_numeric(s), unit=unit, utc=True).dt.as_unit("ms")


def parse_zip(path: Path) -> pd.DataFrame:
    with zipfile.ZipFile(path) as z:
        name = z.namelist()[0]
        raw = z.read(name)

    hdr = _has_header(raw.split(b"\n", 1)[0])
    # names= gesetzt -> header IMMER None, Kopfzeile stattdessen ueberspringen.
    # header=0 UND skiprows=1 wuerde die erste Datenzeile fressen.
    df = pd.read_csv(io.BytesIO(raw), header=None, names=COLS, skiprows=1 if hdr else 0)

    df["open_time"] = _to_utc(df["open_time"])
    df["close_time"] = _to_utc(df["close_time"])
    for c in NUMERIC:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df.drop(columns=["ignore"]).sort_values("open_time").reset_index(drop=True)


# --------------------------------------------------------------- validation ---
class ValidationError(Exception):
    pass


def validate(df: pd.DataFrame, interval: str, label: str) -> dict:
    """Harte Checks. Wirft, statt still kaputte Daten durchzulassen."""
    if df.empty:
        raise ValidationError(f"{label}: leer")

    rep = {"label": label, "rows": len(df)}

    dupes = int(df["open_time"].duplicated().sum())
    if dupes:
        raise ValidationError(f"{label}: {dupes} doppelte open_time")

    if not df["open_time"].is_monotonic_increasing:
        raise ValidationError(f"{label}: open_time nicht monoton")

    bad_ohlc = (
        (df["low"] > df[["open", "close"]].min(axis=1))
        | (df["high"] < df[["open", "close"]].max(axis=1))
        | (df["high"] < df["low"])
    ).sum()
    if bad_ohlc:
        raise ValidationError(f"{label}: {bad_ohlc} Zeilen mit inkonsistentem OHLC")

    if (df[["open", "high", "low", "close"]] <= 0).any().any():
        raise ValidationError(f"{label}: Preise <= 0")
    if (df["volume"] < 0).any():
        raise ValidationError(f"{label}: negatives Volumen")

    nan = int(df[NUMERIC].isna().sum().sum())
    if nan:
        raise ValidationError(f"{label}: {nan} NaN in numerischen Spalten")

    # Luecken gegen ein vollstaendiges Zeitraster
    step = pd.Timedelta(milliseconds=INTERVAL_MS[interval])
    grid = pd.date_range(df["open_time"].iloc[0], df["open_time"].iloc[-1],
                         freq=step, tz="UTC")
    missing = len(grid) - len(df)
    rep["expected"] = len(grid)
    rep["missing"] = int(missing)
    rep["zero_volume"] = int((df["volume"] == 0).sum())
    rep["first"] = str(df["open_time"].iloc[0])
    rep["last"] = str(df["open_time"].iloc[-1])

    if missing < 0:
        raise ValidationError(f"{label}: mehr Zeilen als Raster erlaubt")
    if len(grid) and missing / len(grid) > 0.02:
        raise ValidationError(f"{label}: {missing}/{len(grid)} Bars fehlen (>2%)")
    return rep


# ------------------------------------------------------------------- driver ---
def months(start: date, end: date):
    y, m = start.year, start.month
    while (y, m) <= (end.year, end.month):
        yield y, m
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)


def utc_today() -> date:
    return datetime.now(timezone.utc).date()


def run(symbol: str, interval: str, start: date, end: date,
        today: date | None = None) -> None:
    STORE.mkdir(parents=True, exist_ok=True)
    today = today or utc_today()
    reports = []

    for y, m in months(start, end):
        label = f"{symbol} {y}-{m:02d}"
        # Laufender Monat wird NICHT geschrieben. Eine Teilmonatsdatei saehe
        # aus wie eine fertige und wuerde wegen out.exists() nie ersetzt.
        # Er kommt beim ersten Lauf nach Monatsende aus dem Monatsarchiv.
        if (y, m) >= (today.year, today.month):
            log.info("skip %s (Monat nicht abgeschlossen)", label)
            continue

        out = STORE / f"symbol={symbol}" / f"year={y}" / f"{symbol}-{interval}-{y}-{m:02d}.parquet"
        if out.exists():
            continue

        fn = f"{symbol}-{interval}-{y}-{m:02d}.zip"
        url = f"{BASE}/monthly/klines/{symbol}/{interval}/{fn}"
        z = download_verified(url, RAW / symbol / fn)
        if z is None:
            # Monatsarchiv erscheint erst ein paar Tage nach Monatsende;
            # nichts geschrieben -> naechster Lauf versucht es erneut
            log.warning("skip %s (kein Monatsarchiv)", label)
            continue
        df = parse_zip(z)

        rep = validate(df, interval, label)
        out.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(out, index=False, compression="zstd")
        reports.append(rep)
        log.info("%-18s %7d Bars  fehlend=%-4d  vol0=%-5d  %s",
                 label, rep["rows"], rep["missing"], rep["zero_volume"], out.name)

    if reports:
        tot = sum(r["rows"] for r in reports)
        miss = sum(r["missing"] for r in reports)
        log.info("FERTIG %s: %d neue Bars, %d fehlend, %d Dateien",
                 symbol, tot, miss, len(reports))
    else:
        log.info("FERTIG %s: nichts Neues", symbol)


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--symbol", required=True)
    p.add_argument("--interval", default="1m", choices=sorted(INTERVAL_MS))
    p.add_argument("--start", required=True, help="YYYY-MM")
    p.add_argument("--end", default=None, help="YYYY-MM (default: heute)")
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
        run(a.symbol.upper(), a.interval, date(sy, sm, 1), date(ey, em, 1))
    except ValidationError as e:
        log.error("VALIDIERUNG FEHLGESCHLAGEN: %s", e)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
