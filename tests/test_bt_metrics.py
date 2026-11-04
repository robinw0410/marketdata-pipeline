"""bt.metrics: Sortino mit Downside Deviation, Annualisierung per periods_per_year."""
import numpy as np
import pandas as pd
import pytest

from bt.engine import Result
from bt.metrics import summarize


def make_result(values) -> Result:
    """Equity als Tagesreihe (UTC), keine Trades."""
    idx = pd.date_range("2024-01-01", periods=len(values), freq="1D", tz="UTC")
    return Result(equity=pd.Series(values, index=idx, dtype=float),
                  trades=pd.DataFrame(), exposure=1.0, cost_paid=0.0,
                  config={"initial": float(values[0])})


# Renditen: +10 %, -10 %, 0 %, +10 %
EQ = [100.0, 110.0, 99.0, 99.0, 108.9]
R = np.array([0.1, -0.1, 0.0, 0.1])


def test_sortino_by_hand():
    # mean = 0.025; Downside Deviation = sqrt((0 + 0.01 + 0 + 0) / 4) = 0.05
    s = summarize(make_result(EQ))
    assert s["sortino"] == pytest.approx(0.025 / 0.05 * np.sqrt(365))
    # alte Formel (std der negativen Renditen) haette hier 0.0 geliefert
    assert s["sortino"] != 0.0


def test_sortino_zero_without_losses():
    s = summarize(make_result([100.0, 101.0, 102.0, 103.0]))
    assert s["sortino"] == 0.0


def test_sharpe_annualization():
    s365 = summarize(make_result(EQ))
    s252 = summarize(make_result(EQ), periods_per_year=252)
    expected = R.mean() / R.std(ddof=1)
    assert s365["sharpe"] == pytest.approx(expected * np.sqrt(365))
    assert s252["sharpe"] == pytest.approx(expected * np.sqrt(252))
    assert s252["sortino"] == pytest.approx(s365["sortino"] * np.sqrt(252 / 365))


def test_default_unchanged():
    res = make_result(EQ)
    assert summarize(res) == summarize(res, periods_per_year=365)
    # CAGR haengt nicht vom Annualisierungsfaktor ab
    assert summarize(res, periods_per_year=252)["cagr"] == summarize(res)["cagr"]
