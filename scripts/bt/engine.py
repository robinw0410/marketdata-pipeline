"""
Event-driven Backtest-Engine, Long-only Spot.

Bewusst eine Schleife, keine Vektorisierung. In einer Bar-fuer-Bar-Schleife
kannst du physisch nicht auf Daten zugreifen, die es noch nicht gibt.
Bei Pandas-Vektoroperationen ist ein .shift() in die falsche Richtung ein
Einzeichenfehler mit spektakulaeren Ergebnissen.

Kernregeln:
  * Signal aus Bar t  ->  Ausfuehrung zur OPEN von t+1. Niemals Close von t.
  * Kosten bei jeder Positionsaenderung: Fee + Slippage, auf das Notional.
  * Positionen bestehen ueber Boersenluecken fort. Vorher rauszugehen waere
    Lookahead (du weisst nicht, dass gleich der Ausfall kommt). Gap-Trades
    werden im Log markiert, damit du siehst, wieviel P&L daher kommt.
"""
from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
import pandas as pd


@dataclass
class Costs:
    fee_bps: float = 10.0        # Binance Spot Taker ~0.10 %
    slippage_bps: float = 5.0    # halber Spread, konservativ nach oben

    @property
    def total_bps(self) -> float:
        return self.fee_bps + self.slippage_bps


@dataclass
class Result:
    equity: pd.Series
    trades: pd.DataFrame
    exposure: float
    cost_paid: float
    config: dict = field(default_factory=dict)


def run(df: pd.DataFrame, signal: pd.Series, costs: Costs = Costs(),
        initial: float = 10_000.0) -> Result:
    """
    signal: Zielposition in {0, 1}, indiziert wie df.
            signal[t] = Entscheidung NACH Schliessen von Bar t.
    """
    if not signal.index.equals(df.index):
        raise ValueError("signal und df muessen denselben Index haben")

    o = df["open"].to_numpy(float)
    c = df["close"].to_numpy(float)
    sess = df["session"].to_numpy(int)
    ts = df.index
    # Entscheidung aus t wirkt ab t+1 -> hier einmal zentral verschoben,
    # damit keine Strategie das vergessen kann
    target = np.roll(signal.to_numpy(float), 1)
    target[0] = 0.0

    n = len(df)
    cash, units = initial, 0.0
    eq = np.empty(n)
    cost_total = 0.0
    bars_in_market = 0
    trades: list[dict] = []
    entry: dict | None = None

    for i in range(n):
        px_open = o[i]
        pos = 1.0 if units > 0 else 0.0

        if target[i] != pos:
            notional = (units * px_open) if pos else cash
            fee = notional * costs.total_bps / 1e4
            if target[i] > 0:                              # kaufen
                spend = cash - fee
                units = spend / px_open
                cash = 0.0
                entry = {"entry_time": ts[i], "entry_px": px_open,
                         "entry_session": sess[i], "entry_eq": spend + fee,
                         "_i": i}
            else:                                          # verkaufen
                cash = units * px_open - fee
                units = 0.0
                if entry is not None:
                    trades.append({
                        **{k: v for k, v in entry.items() if k != "_i"},
                        "exit_time": ts[i], "exit_px": px_open,
                        "exit_session": sess[i],
                        "bars": i - entry["_i"],
                        "gross_ret": px_open / entry["entry_px"] - 1.0,
                        "net_pnl": cash - entry["entry_eq"],
                        "spans_gap": sess[i] != entry["entry_session"],
                    })
                    entry = None
            cost_total += fee

        if units > 0:
            bars_in_market += 1
        eq[i] = cash + units * c[i]

    equity = pd.Series(eq, index=ts, name="equity")
    tdf = pd.DataFrame(trades)
    if not tdf.empty:
        tdf["net_ret"] = tdf["net_pnl"] / tdf["entry_eq"]

    return Result(
        equity=equity, trades=tdf,
        exposure=bars_in_market / n, cost_paid=cost_total,
        config={"fee_bps": costs.fee_bps, "slippage_bps": costs.slippage_bps,
                "initial": initial, "bars": n},
    )
