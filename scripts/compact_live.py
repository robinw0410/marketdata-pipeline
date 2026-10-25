#!/usr/bin/env python3
"""
Verdichtet die append-only NDJSON des Collectors zu Parquet.

Die JSONL bleiben liegen. Sie sind die Quelle der Wahrheit; Parquet ist
abgeleitet und jederzeit neu erzeugbar. Nur abgeschlossene Tage werden
verdichtet, der laufende Tag nicht.

Usage:
    python compact_live.py --symbol BTCUSDT --interval 1m
"""
from __future__ import annotations

import argparse
import json
import logging
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd

from fetch_klines import NUMERIC, ValidationError, validate

ROOT = Path(__file__).resolve().parents[1]
LIVE = ROOT / "data" / "live"
STORE = ROOT / "data" / "live_parquet"

log = logging.getLogger("compact")


def utc_today() -> date:
    """Die Tagesdateien des Collectors sind UTC-Tage. date.today() waere Ortszeit."""
    return datetime.now(timezone.utc).date()


def day_of(src: Path) -> date:
    """BTCUSDT-1m-2026-08-21.jsonl -> date(2026, 8, 21)"""
    return date.fromisoformat("-".join(src.stem.rsplit("-", 3)[-3:]))


def compact_day(symbol: str, interval: str, src: Path,
                today: date | None = None) -> dict | None:
    day = day_of(src)
    # Schutz, unabhaengig vom Aufrufer: der laufende UTC-Tag wird nie verdichtet.
    # Ein halber Tag als Parquet wuerde wegen out.exists() nie mehr ersetzt.
    if day >= (today or utc_today()):
        raise ValueError(f"{src.name}: UTC-Tag {day} ist nicht abgeschlossen")
    out = STORE / f"symbol={symbol}" / f"{src.stem}.parquet"
    if out.exists():
        return None

    rows = [json.loads(l) for l in src.read_text(encoding="utf-8").splitlines() if l.strip()]
    if not rows:
        return None

    df = pd.DataFrame(rows)
    # Backfill-Overlap kann Duplikate erzeugen: WS-Version gewinnt
    df["_pref"] = (df["src"] == "ws").astype(int)
    df = (df.sort_values(["open_time", "_pref"])
            .drop_duplicates("open_time", keep="last")
            .drop(columns="_pref"))

    for c in ["open_time", "close_time"]:
        df[c] = pd.to_datetime(df[c], unit="ms", utc=True).dt.as_unit("ms")
    for c in NUMERIC:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.sort_values("open_time").reset_index(drop=True)
    rep = validate(df, interval, f"{symbol} {day.isoformat()}")

    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out, index=False, compression="zstd")
    return rep


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--symbol", required=True)
    p.add_argument("--interval", default="1m")
    a = p.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s")

    sym = a.symbol.upper()
    today = utc_today()                            # laufenden UTC-Tag nicht anfassen
    src_dir = LIVE / f"symbol={sym}"
    if not src_dir.exists():
        log.warning("keine Live-Daten fuer %s", sym)
        return

    for f in sorted(src_dir.glob(f"{sym}-{a.interval}-*.jsonl")):
        if day_of(f) >= today:
            continue
        try:
            rep = compact_day(sym, a.interval, f, today=today)
        except ValidationError as e:
            log.error("ABGELEHNT %s: %s", f.name, e)
            continue
        if rep:
            log.info("%-22s %5d Bars  fehlend=%-3d  vol0=%d",
                     rep["label"], rep["rows"], rep["missing"], rep["zero_volume"])


if __name__ == "__main__":
    main()
