"""
Laden, Resampling, Session-Logik, Split-Waechter.

Die Split-Grenzen stehen HIER und werden nicht per CLI ueberschrieben.
Wenn du sie aendern willst, aenderst du sie im Code, mit Commit, mit Datum.
Das ist Absicht: ein Holdout, den man im Vorbeigehen verschieben kann,
ist kein Holdout.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
STORE = ROOT / "data" / "klines"
LOGDIR = ROOT / "logs"

# --- eingefroren am 2026-08-20 ------------------------------------------------
# 2026-10-25: Holdout-Ende von 2026-12-31 auf 2026-08-20 gesetzt. Vorher lief
# der Holdout in den Forward-Zeitraum (ab 2026-08-21) hinein.
EXPLORE = ("2019-01-01", "2023-12-31")
HOLDOUT = ("2024-01-01", "2026-08-20")
# ------------------------------------------------------------------------------

log = logging.getLogger("bt.data")


class HoldoutViolation(Exception):
    pass


def load(symbol: str, split: str = "explore", timeframe: str = "5min",
         unlock_holdout: bool = False,
         date_from: str | None = None, date_to: str | None = None) -> pd.DataFrame:
    if split == "holdout":
        if not unlock_holdout:
            raise HoldoutViolation(
                "Holdout ist gesperrt. Er wird EINMAL benutzt, fuer EINE Strategie, "
                "am Ende. Wenn du wirklich soweit bist: unlock_holdout=True setzen. "
                "Jede Nutzung wird in logs/holdout_uses.jsonl protokolliert."
            )
        _record_holdout_use(symbol, timeframe)
        lo, hi = HOLDOUT
    elif split == "explore":
        lo, hi = EXPLORE
    elif split == "all":
        lo, hi = "2000-01-01", "2099-01-01"
    else:
        raise ValueError(split)

    q = f"""
        SELECT open_time, open, high, low, close, volume
        FROM read_parquet('{STORE}/**/*.parquet', hive_partitioning=1)
        WHERE symbol = ? AND open_time >= ? AND open_time < ?
        ORDER BY open_time
    """
    # Zeitraum-Einschraenkung: darf den Split nur VERENGEN, nie erweitern.
    # Sonst waere --from 2024-01-01 im explore-Split eine Hintertuer in den Holdout.
    if date_from:
        lo = max(str(pd.Timestamp(lo).date()), str(pd.Timestamp(date_from).date()))
    if date_to:
        hi = min(str(pd.Timestamp(hi).date()), str(pd.Timestamp(date_to).date()))
    if pd.Timestamp(lo) > pd.Timestamp(hi):
        raise ValueError(f"leerer Zeitraum: {lo} .. {hi} (Split {split} = {EXPLORE if split=='explore' else HOLDOUT})")

    df = duckdb.execute(q, [symbol, lo, str(pd.Timestamp(hi) + pd.Timedelta(days=1))]).df()
    if df.empty:
        raise ValueError(f"keine Daten fuer {symbol} / {split}")

    df["open_time"] = pd.to_datetime(df["open_time"], utc=True)
    df = df.set_index("open_time")

    if timeframe != "1min":
        df = resample(df, timeframe)

    df = add_sessions(df, timeframe)
    log.info("%s %s %s: %d Bars, %d Sessions, %s .. %s",
             symbol, split, timeframe, len(df), df["session"].nunique(),
             df.index[0], df.index[-1])
    return df


def resample(df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    """Resampling OHNE Auffuellen. Perioden ohne Bars bleiben weg, nicht 0."""
    out = df.resample(timeframe, label="left", closed="left").agg(
        open=("open", "first"), high=("high", "max"),
        low=("low", "min"), close=("close", "last"), volume=("volume", "sum"),
    )
    return out.dropna(subset=["open", "close"])


def add_sessions(df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    """
    Eine neue Session beginnt, wo eine Luecke im Zeitraster liegt.
    Rolling-Fenster duerfen NIE ueber eine Session-Grenze rechnen: ein
    20-Perioden-Schnitt, der eine 10-Stunden-Boersenpause ueberspannt,
    ist kein 20-Perioden-Schnitt.
    """
    step = pd.Timedelta(timeframe)
    gap = df.index.to_series().diff() > step
    df = df.copy()
    df["session"] = gap.cumsum().astype(int)
    df["bar_in_session"] = df.groupby("session").cumcount()
    return df


def warmup_ok(df: pd.DataFrame, lookback: int) -> pd.Series:
    """True, wenn die Bar genug Historie INNERHALB ihrer Session hat."""
    return df["bar_in_session"] >= lookback - 1


def _record_holdout_use(symbol: str, timeframe: str) -> None:
    LOGDIR.mkdir(parents=True, exist_ok=True)
    p = LOGDIR / "holdout_uses.jsonl"
    prev = sum(1 for _ in p.open()) if p.exists() else 0
    with p.open("a") as f:
        f.write(json.dumps({
            "ts": datetime.now(timezone.utc).isoformat(),
            "symbol": symbol, "timeframe": timeframe, "use_number": prev + 1,
        }) + "\n")
    if prev:
        log.error("!!! HOLDOUT WURDE BEREITS %dx BENUTZT. Jede weitere Nutzung "
                  "macht ihn als unabhaengigen Test wertlos.", prev)
