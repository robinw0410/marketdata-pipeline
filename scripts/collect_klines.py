#!/usr/bin/env python3
"""
Live-Collector fuer Binance-Klines.

Drei Dinge, die hier bewusst so und nicht anders sind:
  1. Es wird NUR die geschlossene Kerze geschrieben (k.x == true).
     Offene Kerzen ins Dataset zu schreiben ist direkter Lookahead-Bias.
  2. Nach jedem Reconnect wird die Luecke per REST nachgeladen.
     Ein Collector, der nach Verbindungsabbruch einfach weitermacht,
     produziert stille Loecher, die du im Backtest nie bemerkst.
  3. Append-only NDJSON. Nichts wird ueberschrieben, nichts wird korrigiert.
     Der gesamte Wert dieser Daten liegt darin, dass sie unangetastet sind.
     Die Verdichtung nach Parquet macht ein separater Job.

Usage:
    python collect_klines.py --symbols BTCUSDT ETHUSDT --interval 1m
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import signal
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
import websockets

WS = "wss://stream.binance.com:9443/stream?streams="
REST = "https://api.binance.com/api/v3/klines"
ROOT = Path(__file__).resolve().parents[1]
LIVE = ROOT / "data" / "live"
STEP_MS = {"1m": 60_000, "5m": 300_000, "15m": 900_000, "1h": 3_600_000}

log = logging.getLogger("collect")
_stop = asyncio.Event()
_last: dict[str, int] = {}          # symbol -> letzte geschriebene open_time (ms)


def _path(symbol: str, interval: str, open_ms: int) -> Path:
    d = datetime.fromtimestamp(open_ms / 1000, tz=timezone.utc).date()
    return LIVE / f"symbol={symbol}" / f"{symbol}-{interval}-{d.isoformat()}.jsonl"


def write_bar(symbol: str, interval: str, bar: dict) -> None:
    """Append-only. fsync, damit ein Host-Reboot die letzte Zeile nicht frisst."""
    ot = bar["open_time"]
    prev = _last.get(symbol)
    if prev is not None and ot <= prev:
        return                                    # Duplikat aus Backfill-Overlap
    p = _path(symbol, interval, ot)
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "a", encoding="utf-8") as f:
        f.write(json.dumps(bar, separators=(",", ":")) + "\n")
        f.flush()
        os.fsync(f.fileno())
    _last[symbol] = ot


def _from_ws(k: dict) -> dict:
    return {
        "open_time": int(k["t"]), "close_time": int(k["T"]),
        "open": k["o"], "high": k["h"], "low": k["l"], "close": k["c"],
        "volume": k["v"], "quote_volume": k["q"], "trades": int(k["n"]),
        "taker_buy_base": k["V"], "taker_buy_quote": k["Q"],
        "src": "ws", "ingested_at": int(time.time() * 1000),
    }


def backfill(symbol: str, interval: str, since_ms: int) -> int:
    """Luecke nach Reconnect per REST schliessen. Gibt Anzahl Bars zurueck."""
    step = STEP_MS[interval]
    start = since_ms + step
    now = int(time.time() * 1000)
    n = 0
    while start < now - step:
        r = requests.get(REST, params={"symbol": symbol, "interval": interval,
                                       "startTime": start, "limit": 1000}, timeout=30)
        r.raise_for_status()
        rows = r.json()
        if not rows:
            break
        for row in rows:
            if int(row[6]) >= now:          # noch offene Kerze -> nicht schreiben
                continue
            write_bar(symbol, interval, {
                "open_time": int(row[0]), "close_time": int(row[6]),
                "open": row[1], "high": row[2], "low": row[3], "close": row[4],
                "volume": row[5], "quote_volume": row[7], "trades": int(row[8]),
                "taker_buy_base": row[9], "taker_buy_quote": row[10],
                "src": "rest_backfill", "ingested_at": int(time.time() * 1000),
            })
            n += 1
        start = int(rows[-1][0]) + step
        time.sleep(0.25)                     # REST-Weight schonen
    if n:
        log.warning("backfill %s: %d Bars nachgeladen", symbol, n)
    return n


def heartbeat() -> None:
    url = os.environ.get("KUMA_PUSH_URL")
    if not url:
        return
    try:
        requests.get(url, params={"status": "up", "msg": "ok"}, timeout=10)
    except requests.RequestException as e:
        log.debug("heartbeat fehlgeschlagen: %s", e)


async def run(symbols: list[str], interval: str) -> None:
    url = WS + "/".join(f"{s.lower()}@kline_{interval}" for s in symbols)
    delay = 1

    while not _stop.is_set():
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                log.info("verbunden: %s", ", ".join(symbols))
                delay = 1
                for s in symbols:
                    if s in _last:
                        await asyncio.to_thread(backfill, s, interval, _last[s])

                last_hb = 0.0
                while not _stop.is_set():
                    msg = await asyncio.wait_for(ws.recv(), timeout=90)
                    k = json.loads(msg).get("data", {}).get("k")
                    if not k or not k.get("x"):
                        continue              # offene Kerze -> ignorieren
                    sym = k["s"]
                    bar = _from_ws(k)
                    prev = _last.get(sym)
                    step = STEP_MS[interval]
                    if prev is not None and bar["open_time"] > prev + step:
                        await asyncio.to_thread(backfill, sym, interval, prev)
                    write_bar(sym, interval, bar)
                    log.debug("%s %s close=%s", sym,
                              datetime.fromtimestamp(bar["open_time"] / 1000,
                                                     tz=timezone.utc), bar["close"])
                    if time.time() - last_hb > 60:
                        heartbeat()
                        last_hb = time.time()

        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.error("Verbindung weg (%s: %s), reconnect in %ds",
                      type(e).__name__, e, delay)
            try:
                await asyncio.wait_for(_stop.wait(), timeout=delay)
            except asyncio.TimeoutError:
                pass
            delay = min(delay * 2, 60)

    log.info("gestoppt")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--symbols", nargs="+", default=["BTCUSDT", "ETHUSDT"])
    p.add_argument("--interval", default="1m", choices=sorted(STEP_MS))
    p.add_argument("-v", "--verbose", action="store_true")
    a = p.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if a.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
    )

    syms = [s.upper() for s in a.symbols]
    # letzten Stand aus den vorhandenen Dateien lesen, damit ein Neustart
    # die Luecke seit dem letzten Lauf per Backfill schliesst
    for s in syms:
        d = LIVE / f"symbol={s}"
        files = sorted(d.glob(f"{s}-{a.interval}-*.jsonl")) if d.exists() else []
        if files:
            lines = files[-1].read_text(encoding="utf-8").strip().splitlines()
            if lines:
                _last[s] = json.loads(lines[-1])["open_time"]
                log.info("%s: letzter Stand %s", s,
                         datetime.fromtimestamp(_last[s] / 1000, tz=timezone.utc))

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _stop.set)
    try:
        loop.run_until_complete(run(syms, a.interval))
    finally:
        loop.close()


if __name__ == "__main__":
    main()
