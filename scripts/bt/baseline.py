"""
Nullhypothese: Zufallseinstiege mit identischer Trade-Anzahl und
identischer Haltedauer-Verteilung wie die getestete Strategie.

Das ist der einzige Vergleich, der zaehlt. "Schlaegt Buy-and-Hold" ist
schwach, weil Exposure und Timing sich unterscheiden. Hier wird alles
konstant gehalten ausser der Frage, WANN eingestiegen wird.

Liegt deine Strategie nicht deutlich im oberen Rand dieser Verteilung,
hast du nichts gefunden - egal wie gut die absolute Zahl aussieht.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from .engine import Costs, Result, run
from .metrics import summarize


def random_signal(n: int, holds: np.ndarray, n_trades: int,
                  rng: np.random.Generator) -> np.ndarray:
    """Zieht n_trades Einstiege mit Haltedauern aus der empirischen Verteilung."""
    sig = np.zeros(n)
    if n_trades == 0 or len(holds) == 0:
        return sig
    durs = rng.choice(holds, size=n_trades, replace=True).astype(int)
    durs = np.clip(durs, 1, max(n // max(n_trades, 1), 1))
    free = n - durs.sum()
    if free <= 0:
        return sig
    # Luecken zwischen den Trades zufaellig, aber nicht ueberlappend
    gaps = rng.multinomial(free, np.ones(n_trades + 1) / (n_trades + 1))
    pos = 0
    for g, d in zip(gaps[:-1], durs):
        pos += g
        sig[pos:pos + d] = 1.0
        pos += d
    return sig


def null_distribution(df: pd.DataFrame, res: Result, costs: Costs,
                      n_runs: int = 1000, seed: int = 0) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    n = len(df)
    holds = (res.trades["bars"].to_numpy() if not res.trades.empty
             else np.array([], dtype=int))
    n_trades = len(res.trades)

    rows = []
    for i in range(n_runs):
        sig = pd.Series(random_signal(n, holds, n_trades, rng), index=df.index)
        r = run(df, sig, costs, initial=res.config["initial"])
        s = summarize(r)
        rows.append({"sharpe": s["sharpe"], "total_return": s["total_return"],
                     "max_dd": s["max_dd"]})
    return pd.DataFrame(rows)


def percentile_of(value: float, dist: pd.Series) -> float:
    return float((dist < value).mean())


def report(strategy: dict, dist: pd.DataFrame) -> str:
    p_sharpe = percentile_of(strategy["sharpe"], dist["sharpe"])
    p_ret = percentile_of(strategy["total_return"], dist["total_return"])
    verdict = ("KEIN Befund - im Rauschen der Zufallseinstiege"
               if p_sharpe < 0.95 else
               "auffaellig - aber siehe Anzahl getesteter Varianten")
    return "\n".join([
        f"  Zufalls-Baseline ({len(dist)} Laeufe, gleiche Trade-Anzahl & Haltedauern)",
        f"    Sharpe   Median {dist['sharpe'].median():+6.2f}   "
        f"p95 {dist['sharpe'].quantile(0.95):+6.2f}   "
        f"Strategie {strategy['sharpe']:+6.2f}  -> Perzentil {p_sharpe:.1%}",
        f"    Rendite  Median {dist['total_return'].median():+7.1%}   "
        f"p95 {dist['total_return'].quantile(0.95):+7.1%}   "
        f"Strategie {strategy['total_return']:+7.1%}  -> Perzentil {p_ret:.1%}",
        f"    Urteil: {verdict}",
    ])
