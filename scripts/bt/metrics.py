"""
Kennzahlen.

Sharpe wird auf TAGESRENDITEN gerechnet, nicht auf Minutenrenditen.
Minutenrenditen sind stark autokorreliert; sqrt(525600) darauf anzuwenden
produziert Fantasiezahlen. Auch die Tagesvariante ist bei nicht-normalen
Renditen nur eine grobe Orientierung.

Sortino nutzt die Downside Deviation sqrt(mean(min(r, 0)^2)) ueber ALLE
Tagesrenditen (positive zaehlen als 0), nicht die Standardabweichung nur der
negativen Renditen. ACHTUNG: Sortino-Werte in logs/experiments.jsonl, die vor
dem Commit "metrics: Sortino mit Downside Deviation ..." geloggt wurden, sind
mit der alten Formel (r[r < 0].std()) berechnet und nicht direkt mit neueren
Werten vergleichbar. Das Log wird bewusst nicht umgeschrieben (append-only).
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from .engine import Result


def summarize(res: Result, periods_per_year: int = 365) -> dict:
    """
    Kennzahlen eines Backtests.

    periods_per_year: Annualisierungsfaktor fuer Sharpe/Sortino (sqrt davon).
        Default 365 = unveraendertes Verhalten fuer Krypto (24/7, jeder
        Kalendertag ist ein Handelstag). Fuer Assets mit Wochenenden/Pausen
        (Metalle, FX) 252 verwenden, und zwar zusammen mit einer Tagesreihe
        OHNE Wochenenden: Tage ohne Bars fallen beim resample("1D") +
        dropna() ohnehin heraus, sofern die Equity-Kurve dort keine Punkte
        hat. 252 auf eine 7-Tage-Reihe anzuwenden waere inkonsistent.
    CAGR rechnet unabhaengig davon mit Kalenderjahren (365.25 Tage).
    """
    eq = res.equity
    daily = eq.resample("1D").last().dropna()
    r = daily.pct_change().dropna()

    years = max((eq.index[-1] - eq.index[0]).days / 365.25, 1e-9)
    total = eq.iloc[-1] / eq.iloc[0] - 1.0
    cagr = (eq.iloc[-1] / eq.iloc[0]) ** (1 / years) - 1.0

    ann = np.sqrt(periods_per_year)
    sharpe = float(r.mean() / r.std() * ann) if r.std() > 0 else 0.0
    # Downside Deviation ueber alle Beobachtungen (Ziel-Rendite 0).
    # Bis zu diesem Fix: r[r < 0].std() -> siehe Modul-Docstring.
    downside_dev = float(np.sqrt(np.mean(np.minimum(r.to_numpy(float), 0.0) ** 2))) if len(r) else 0.0
    sortino = float(r.mean() / downside_dev * ann) if downside_dev > 0 else 0.0

    dd = eq / eq.cummax() - 1.0
    maxdd = float(dd.min())

    out = {
        "total_return": float(total), "cagr": float(cagr),
        "sharpe": sharpe, "sortino": sortino, "max_dd": maxdd,
        "calmar": float(cagr / abs(maxdd)) if maxdd < 0 else 0.0,
        "exposure": float(res.exposure),
        "cost_paid": float(res.cost_paid),
        "cost_pct_of_initial": float(res.cost_paid / res.config["initial"]),
        "n_trades": int(len(res.trades)),
        "years": float(years),
    }

    t = res.trades
    if not t.empty:
        wins, losses = t[t["net_ret"] > 0], t[t["net_ret"] <= 0]
        gross_win = float(wins["net_pnl"].sum())
        gross_loss = float(-losses["net_pnl"].sum())
        out.update({
            "win_rate": float(len(wins) / len(t)),
            "avg_win": float(wins["net_ret"].mean()) if len(wins) else 0.0,
            "avg_loss": float(losses["net_ret"].mean()) if len(losses) else 0.0,
            "profit_factor": float(gross_win / gross_loss) if gross_loss > 0 else float("inf"),
            "median_bars": float(t["bars"].median()),
            # Konzentration: wenn top5 fast den ganzen Gewinn tragen, hast du
            # keine Strategie, sondern drei Gluecksfaelle
            "top5_share_of_gross_profit": (
                float(t.nlargest(5, "net_pnl")["net_pnl"].sum() / gross_win)
                if gross_win > 0 else float("nan")),
            "gap_trades": int(t["spans_gap"].sum()),
            "gap_pnl_share": (float(t.loc[t["spans_gap"], "net_pnl"].sum() / gross_win)
                              if gross_win > 0 else 0.0),
        })
    return out


def fmt(s: dict) -> str:
    L = [
        f"  Gesamtrendite     {s['total_return']:+8.1%}   CAGR {s['cagr']:+7.1%}",
        f"  Sharpe (taeglich) {s['sharpe']:8.2f}   Sortino {s['sortino']:6.2f}",
        f"  Max Drawdown      {s['max_dd']:+8.1%}   Calmar  {s['calmar']:6.2f}",
        f"  Exposure          {s['exposure']:8.1%}   Trades  {s['n_trades']:6d}",
        f"  Kosten gezahlt    {s['cost_pct_of_initial']:8.1%} des Startkapitals",
    ]
    if s.get("n_trades"):
        L += [
            f"  Trefferquote      {s['win_rate']:8.1%}   PF      {s['profit_factor']:6.2f}",
            f"  Ø Gewinn/Verlust  {s['avg_win']:+8.2%} / {s['avg_loss']:+.2%}",
            f"  Top-5-Anteil am Bruttogewinn  {s['top5_share_of_gross_profit']:6.1%}",
            f"  Trades ueber Luecken {s['gap_trades']:5d}  ({s['gap_pnl_share']:.1%} des Gewinns)",
        ]
    return "\n".join(L)
