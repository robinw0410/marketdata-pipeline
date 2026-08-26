"""
Ein paar Referenzstrategien. Die ersten beiden sind Kontrollen, keine Ideen:
buy_and_hold als Bezugspunkt, always_flat als Nullfall, oracle NUR zum
Testen der Engine (sie schaut in die Zukunft und muss absurd gut sein -
wenn nicht, ist die Engine kaputt).
"""
from __future__ import annotations
import pandas as pd
from .data import warmup_ok


def buy_and_hold(df: pd.DataFrame, **kw) -> pd.Series:
    return pd.Series(1.0, index=df.index)


def always_flat(df: pd.DataFrame, **kw) -> pd.Series:
    return pd.Series(0.0, index=df.index)


def oracle(df: pd.DataFrame, **kw) -> pd.Series:
    """NUR Engine-Test: long, wenn die naechste Bar steigt. Verboten im Ernst."""
    nxt = df["open"].shift(-2) > df["open"].shift(-1)
    return nxt.fillna(False).astype(float)


def sma_cross(df: pd.DataFrame, fast: int = 20, slow: int = 100, **kw) -> pd.Series:
    f = df.groupby("session")["close"].transform(lambda s: s.rolling(fast).mean())
    s = df.groupby("session")["close"].transform(lambda x: x.rolling(slow).mean())
    sig = (f > s).astype(float)
    return sig.where(warmup_ok(df, slow), 0.0).fillna(0.0)


REGISTRY = {"buy_and_hold": buy_and_hold, "always_flat": always_flat,
            "oracle": oracle, "sma_cross": sma_cross}
