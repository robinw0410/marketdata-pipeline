#!/usr/bin/env python3
"""
Verifiziert den Live-Collector gegen das offizielle Binance-Tagesarchiv.

Das ist der einzige Test, der dir sagt, ob dein Forward-Datensatz brauchbar
ist. Laeuft sinnvollerweise einmal nach dem ersten vollen Tag, danach
gelegentlich stichprobenartig.

Usage:
    python verify_live.py --symbol BTCUSDT --date 2026-08-20
    python verify_live.py --symbol BTCUSDT --date 2026-08-20 --tolerance 0
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date
from pathlib import Path

import pandas as pd

from fetch_klines import BASE, RAW, download_verified, parse_zip

ROOT = Path(__file__).resolve().parents[1]
LIVE = ROOT / "data" / "live"
FIELDS = ["open", "high", "low", "close", "volume"]

log = logging.getLogger("verify")


def load_live(symbol: str, interval: str, d: date) -> pd.DataFrame:
    p = LIVE / f"symbol={symbol}" / f"{symbol}-{interval}-{d.isoformat()}.jsonl"
    if not p.exists():
        raise SystemExit(f"keine Live-Datei: {p}")
    rows = [json.loads(l) for l in p.read_text().splitlines() if l.strip()]
    df = pd.DataFrame(rows)
    df["_pref"] = (df["src"] == "ws").astype(int)
    df = (df.sort_values(["open_time", "_pref"])
            .drop_duplicates("open_time", keep="last").drop(columns="_pref"))
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    for c in FIELDS:
        df[c] = pd.to_numeric(df[c])
    return df.set_index("open_time").sort_index()


def load_official(symbol: str, interval: str, d: date) -> pd.DataFrame:
    fn = f"{symbol}-{interval}-{d.isoformat()}.zip"
    url = f"{BASE}/daily/klines/{symbol}/{interval}/{fn}"
    z = download_verified(url, RAW / symbol / fn)
    if z is None:
        raise SystemExit(f"Tagesarchiv fuer {d} noch nicht verfuegbar "
                         f"(erscheint typischerweise am Folgetag)")
    return parse_zip(z).set_index("open_time").sort_index()


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--symbol", default="BTCUSDT")
    p.add_argument("--interval", default="1m")
    p.add_argument("--date", required=True, help="YYYY-MM-DD")
    p.add_argument("--tolerance", type=float, default=0.0,
                   help="relative Abweichung, die noch als gleich gilt")
    a = p.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    d = date.fromisoformat(a.date)
    sym = a.symbol.upper()
    live = load_live(sym, a.interval, d)
    off = load_official(sym, a.interval, d)

    print(f"\n{sym} {d}  live={len(live)} Bars   offiziell={len(off)} Bars")

    only_off = off.index.difference(live.index)
    only_live = live.index.difference(off.index)
    both = off.index.intersection(live.index)

    print(f"  gemeinsam            {len(both)}")
    print(f"  fehlen im Live-Satz  {len(only_off)}")
    print(f"  nur im Live-Satz     {len(only_live)}")

    if len(only_off):
        print("  erste fehlende:", ", ".join(str(t) for t in only_off[:5]))
    if len(only_live):
        print("  !! Bars, die Binance nicht kennt -- das waere ein Bug:",
              ", ".join(str(t) for t in only_live[:5]))

    bad = {}
    for c in FIELDS:
        lv, ov = live.loc[both, c], off.loc[both, c]
        rel = ((lv - ov).abs() / ov.replace(0, pd.NA)).fillna(0)
        n = int((rel > a.tolerance).sum())
        if n:
            bad[c] = (n, float(rel.max()))

    if bad:
        print("\n  ABWEICHUNGEN:")
        for c, (n, mx) in bad.items():
            print(f"    {c:8s} {n} Bars, max. relative Abweichung {mx:.3e}")
    else:
        print("\n  OHLCV: identisch auf allen gemeinsamen Bars")

    src = live["src"].value_counts().to_dict() if "src" in live else {}
    print(f"  Herkunft: {src}")

    ok = not bad and not len(only_live) and len(only_off) <= 2
    print(f"\n  URTEIL: {'Collector verifiziert' if ok else 'PRUEFEN -- siehe oben'}\n")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
