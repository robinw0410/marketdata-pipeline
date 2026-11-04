#!/usr/bin/env python3
"""
Kleinen Ausschnitt aus dem Kline-Store als Test-Fixture exportieren.

Gedacht zum einmaligen Ausfuehren dort, wo die Daten liegen (LXC), danach
wird die Datei committet. Ausgabe ist gzip-CSV mit festen Spalten:
    open_time (ISO 8601 UTC, z.B. 2024-01-01T00:00:00Z), open, high, low, close, volume
Die gzip-Datei wird ohne Zeitstempel im Header geschrieben (mtime=0), damit
ein erneuter Export bei gleichen Daten byte-identisch ist.

Usage:
    python export_fixture.py
    python export_fixture.py --symbol ETHUSDT --start 2024-03-01 --end 2024-03-03
    python export_fixture.py --store /pfad/zu/klines --out /tmp/x.csv.gz
"""
from __future__ import annotations

import argparse
import gzip
import logging
import sys
from datetime import date, timedelta
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
STORE = ROOT / "data" / "klines"
FIXTURES = ROOT / "tests" / "fixtures"
OUT_COLS = ["open_time", "open", "high", "low", "close", "volume"]

log = logging.getLogger("export_fixture")


def default_out(symbol: str, interval: str, start: date, end: date) -> Path:
    # BTCUSDT-1m-2024-01-01_07.csv.gz; ueber Monatsgrenzen das volle Enddatum
    tail = f"{end.day:02d}" if (start.year, start.month) == (end.year, end.month) else end.isoformat()
    return FIXTURES / f"{symbol}-{interval}-{start.isoformat()}_{tail}.csv.gz"


def load(store: Path, symbol: str, interval: str, start: date, end: date) -> pd.DataFrame:
    """Bars [start 00:00, end+1 00:00) UTC; end ist inklusive."""
    # Dateiname traegt das Intervall; andere Intervalle im selben Ordner ignorieren
    files = sorted((store / f"symbol={symbol}").glob(f"year=*/{symbol}-{interval}-*.parquet"))
    if not files:
        raise FileNotFoundError(f"keine Dateien unter {store}/symbol={symbol} fuer {interval}")
    lo = pd.Timestamp(start, tz="UTC")
    hi = pd.Timestamp(end + timedelta(days=1), tz="UTC")
    flist = ", ".join("'" + str(f).replace("'", "''") + "'" for f in files)
    df = duckdb.execute(
        f"""SELECT open_time, open, high, low, close, volume
            FROM read_parquet([{flist}], hive_partitioning=0)
            WHERE open_time >= ? AND open_time < ?
            ORDER BY open_time""",
        [lo.to_pydatetime(), hi.to_pydatetime()],
    ).df()
    if df.empty:
        raise ValueError(f"keine Bars fuer {symbol} {interval} {start}..{end}")
    df["open_time"] = pd.to_datetime(df["open_time"], utc=True)
    if df["open_time"].duplicated().any():
        raise ValueError("doppelte open_time im Ausschnitt")
    return df


def export(df: pd.DataFrame, out: Path) -> None:
    df = df.copy()
    df["open_time"] = df["open_time"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "wb") as raw, gzip.GzipFile(fileobj=raw, mode="wb", mtime=0, filename="") as gz:
        gz.write(df[OUT_COLS].to_csv(index=False, lineterminator="\n").encode())


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Kline-Ausschnitt als gzip-CSV-Fixture exportieren")
    p.add_argument("--symbol", default="BTCUSDT", type=str.upper)
    p.add_argument("--interval", default="1m")
    p.add_argument("--start", default="2024-01-01", type=date.fromisoformat, help="YYYY-MM-DD")
    p.add_argument("--end", default="2024-01-07", type=date.fromisoformat,
                   help="YYYY-MM-DD, inklusive")
    p.add_argument("--store", type=Path, default=STORE, help=f"Kline-Store (default {STORE})")
    p.add_argument("--out", type=Path, default=None,
                   help="Zieldatei (default tests/fixtures/<SYM>-<IV>-<start>_<DD>.csv.gz)")
    a = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s")

    if a.end < a.start:
        p.error("--end liegt vor --start")
    out = a.out or default_out(a.symbol, a.interval, a.start, a.end)
    try:
        df = load(a.store, a.symbol, a.interval, a.start, a.end)
    except (FileNotFoundError, ValueError) as e:
        log.error("%s", e)
        return 2

    expected = ((a.end - a.start).days + 1) * 1440 if a.interval == "1m" else None
    export(df, out)
    log.info("%s: %d Bars%s -> %s", a.symbol, len(df),
             f" (erwartet {expected})" if expected else "", out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
