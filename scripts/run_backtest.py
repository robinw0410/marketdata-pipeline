#!/usr/bin/env python3
"""
Backtest-Runner.

JEDER Lauf wird in logs/experiments.jsonl protokolliert - auch die, die
nichts werden. Ohne diesen Zaehler kannst du spaeter nicht beurteilen, ob
ein Sharpe von 1.4 etwas bedeutet oder der beste von 300 Versuchen ist.
Rueckwirkend rekonstruieren geht nicht. Das Logging ist deshalb nicht
abschaltbar.

Usage:
    python run_backtest.py --strategy sma_cross --tf 15min --param fast=20 --param slow=100
    python run_backtest.py --strategy buy_and_hold --tf 1h --no-baseline
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from bt import baseline, data, engine, metrics, strategies

LOGDIR = Path(__file__).resolve().parents[1] / "logs"
log = logging.getLogger("run")


def _coerce(v: str):
    for f in (int, float):
        try:
            return f(v)
        except ValueError:
            pass
    return v


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--symbol", default="BTCUSDT")
    p.add_argument("--strategy", required=True, choices=sorted(strategies.REGISTRY))
    p.add_argument("--tf", default="15min", help="1min/5min/15min/1h/4h")
    p.add_argument("--split", default="explore", choices=["explore", "holdout", "all"])
    p.add_argument("--param", action="append", default=[], metavar="K=V")
    p.add_argument("--from", dest="date_from", default=None, metavar="YYYY-MM-DD",
                   help="verengt den Split. Kann ihn nie erweitern.")
    p.add_argument("--to", dest="date_to", default=None, metavar="YYYY-MM-DD")
    p.add_argument("--fee-bps", type=float, default=10.0)
    p.add_argument("--slippage-bps", type=float, default=5.0)
    p.add_argument("--baseline-runs", type=int, default=1000)
    p.add_argument("--no-baseline", action="store_true")
    p.add_argument("--note", default="", help="warum dieser Lauf?")
    p.add_argument("--i-am-done", action="store_true",
                   help="entsperrt den Holdout. Einmal. Ueberleg es dir.")
    a = p.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    params = dict(kv.split("=", 1) for kv in a.param)
    params = {k: _coerce(v) for k, v in params.items()}

    df = data.load(a.symbol, a.split, a.tf, unlock_holdout=a.i_am_done,
                   date_from=a.date_from, date_to=a.date_to)
    costs = engine.Costs(fee_bps=a.fee_bps, slippage_bps=a.slippage_bps)
    sig = strategies.REGISTRY[a.strategy](df, **params)
    res = engine.run(df, sig, costs)
    s = metrics.summarize(res)

    span = f" {a.date_from or ''}..{a.date_to or ''}" if (a.date_from or a.date_to) else ""
    print(f"\n{a.strategy}({params}) {a.symbol} {a.tf} [{a.split}{span}]")
    print(metrics.fmt(s))

    null = None
    if not a.no_baseline and s["n_trades"] > 0:
        dist = baseline.null_distribution(df, res, costs, n_runs=a.baseline_runs)
        print(baseline.report(s, dist))
        null = {
            "runs": int(a.baseline_runs),
            "sharpe_pct": baseline.percentile_of(s["sharpe"], dist["sharpe"]),
            "return_pct": baseline.percentile_of(s["total_return"], dist["total_return"]),
            "sharpe_p95": float(dist["sharpe"].quantile(0.95)),
        }

    cfg = {"symbol": a.symbol, "strategy": a.strategy, "tf": a.tf,
           "split": a.split, "params": params,
           "date_from": a.date_from, "date_to": a.date_to,
           "fee_bps": a.fee_bps, "slippage_bps": a.slippage_bps}
    rec = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "config_hash": hashlib.sha256(json.dumps(cfg, sort_keys=True).encode()).hexdigest()[:12],
        "config": cfg, "metrics": s, "null": null, "note": a.note,
    }
    LOGDIR.mkdir(parents=True, exist_ok=True)
    f = LOGDIR / "experiments.jsonl"
    with f.open("a") as fh:
        fh.write(json.dumps(rec) + "\n")
    n = sum(1 for _ in f.open())
    print(f"\n  -> experiments.jsonl: Lauf #{n}. "
          f"Bei {n} getesteten Varianten ist ein Sharpe im 95. Perzentil "
          f"der Nullverteilung {'noch nichts' if n > 20 else 'schwach'}.")


if __name__ == "__main__":
    main()
