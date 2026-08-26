#!/usr/bin/env python3
"""
Funding-Raten und Perp-Klines (USDT-M Futures) von data.binance.vision.

Warum das der interessantere Datensatz ist: Funding ist ein MECHANISCHER
Cashflow zwischen Long- und Short-Seite, kein Muster in einer Preisreihe.
Basis (Perp - Spot) und Funding-Rate haben eine oekonomische Begruendung,
und die Effekte sind nicht durch Mustersuche entstanden. Das ist der
einzige Bereich, in dem ein Einzelner realistisch noch Reste findet -
weil die Nischen zu klein sind, als dass sich Fonds dafuer bewegen.

Store:
    data/klines_um/symbol=X/year=Y/...   Perp-Klines (gleiches Schema wie Spot)
    data/funding/symbol=X/...            Funding-Raten, 8h-Takt

Usage:
    python fetch_funding.py --symbol BTCUSDT --start 2020-01
    python fetch_funding.py --symbol BTCUSDT --start 2020-01 --klines
"""
from __future__ import annotations

import argparse
import io
import logging
import zipfile
from datetime import date
from pathlib import Path

import pandas as pd

from fetch_klines import (INTERVAL_MS, RAW, ValidationError, _to_utc,
                          download_verified, months, parse_zip, validate)

FBASE = "https://data.binance.vision/data/futures/um"
ROOT = Path(__file__).resolve().parents[1]
UM_STORE = ROOT / "data" / "klines_um"
FUND_STORE = ROOT / "data" / "funding"

log = logging.getLogger("funding")


def parse_funding(path: Path) -> pd.DataFrame:
    """
    Spalten je nach Jahrgang: calc_time, funding_interval_hours, last_funding_rate.
    Wird ueber die Kopfzeile gemappt, wo vorhanden - Positionsannahmen sind
    bei diesem Datensatz nicht sicher.
    """
    with zipfile.ZipFile(path) as z:
        raw = z.read(z.namelist()[0])

    first = raw.split(b"\n", 1)[0].decode(errors="replace").lower()
    has_header = not first.split(",")[0].strip().strip('"').isdigit()

    if has_header:
        df = pd.read_csv(io.BytesIO(raw))
        df.columns = [c.strip().lower() for c in df.columns]
        tcol = next((c for c in df.columns if "time" in c), df.columns[0])
        rcol = next((c for c in df.columns if "rate" in c), df.columns[-1])
        df = df.rename(columns={tcol: "calc_time", rcol: "funding_rate"})
    else:
        df = pd.read_csv(io.BytesIO(raw), header=None)
        if df.shape[1] < 2:
            raise ValidationError(f"{path.name}: unerwartetes Format")
        df.columns = (["calc_time", "funding_interval_hours", "funding_rate"]
                      if df.shape[1] >= 3 else ["calc_time", "funding_rate"])

    df["calc_time"] = _to_utc(df["calc_time"])
    df["funding_rate"] = pd.to_numeric(df["funding_rate"], errors="coerce")
    return df[["calc_time", "funding_rate"]].sort_values("calc_time").reset_index(drop=True)


def validate_funding(df: pd.DataFrame, label: str) -> dict:
    if df.empty:
        raise ValidationError(f"{label}: leer")
    if df["calc_time"].duplicated().any():
        raise ValidationError(f"{label}: doppelte calc_time")
    if df["funding_rate"].isna().any():
        raise ValidationError(f"{label}: NaN in funding_rate")
    # Binance-Cap liegt bei +/-0.75 % pro Periode; alles darueber ist ein Parsefehler
    if df["funding_rate"].abs().max() > 0.02:
        raise ValidationError(f"{label}: |rate| > 2 % - vermutlich falsche Spalte "
                              f"(max {df['funding_rate'].abs().max():.4f})")
    return {"label": label, "rows": len(df),
            "mean_bps": float(df["funding_rate"].mean() * 1e4),
            "min": float(df["funding_rate"].min()),
            "max": float(df["funding_rate"].max())}


def run_funding(symbol: str, start: date, end: date) -> None:
    FUND_STORE.mkdir(parents=True, exist_ok=True)
    today = date.today()
    for y, m in months(start, end):
        if (y, m) >= (today.year, today.month):
            continue
        out = FUND_STORE / f"symbol={symbol}" / f"{symbol}-fundingRate-{y}-{m:02d}.parquet"
        if out.exists():
            continue
        fn = f"{symbol}-fundingRate-{y}-{m:02d}.zip"
        url = f"{FBASE}/monthly/fundingRate/{symbol}/{fn}"
        z = download_verified(url, RAW / "um" / symbol / fn)
        if z is None:
            log.warning("skip %s %s-%02d (kein Archiv)", symbol, y, m)
            continue
        df = parse_funding(z)
        rep = validate_funding(df, f"{symbol} {y}-{m:02d}")
        out.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(out, index=False, compression="zstd")
        log.info("%-18s %4d Perioden  Ø %+6.2f bps  [%+.4f .. %+.4f]",
                 rep["label"], rep["rows"], rep["mean_bps"], rep["min"], rep["max"])


def run_um_klines(symbol: str, interval: str, start: date, end: date) -> None:
    UM_STORE.mkdir(parents=True, exist_ok=True)
    today = date.today()
    for y, m in months(start, end):
        if (y, m) >= (today.year, today.month):
            continue
        out = UM_STORE / f"symbol={symbol}" / f"year={y}" / f"{symbol}-{interval}-{y}-{m:02d}.parquet"
        if out.exists():
            continue
        fn = f"{symbol}-{interval}-{y}-{m:02d}.zip"
        url = f"{FBASE}/monthly/klines/{symbol}/{interval}/{fn}"
        z = download_verified(url, RAW / "um" / symbol / fn)
        if z is None:
            continue
        df = parse_zip(z)
        rep = validate(df, interval, f"{symbol}-PERP {y}-{m:02d}")
        out.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(out, index=False, compression="zstd")
        log.info("%-22s %7d Bars  fehlend=%d", rep["label"], rep["rows"], rep["missing"])


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--symbol", required=True)
    p.add_argument("--start", required=True, help="YYYY-MM")
    p.add_argument("--end", default=None)
    p.add_argument("--klines", action="store_true", help="auch Perp-Klines laden")
    p.add_argument("--interval", default="1m", choices=sorted(INTERVAL_MS))
    a = p.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s")

    sy, sm = map(int, a.start.split("-"))
    ey, em = (map(int, a.end.split("-")) if a.end
              else (date.today().year, date.today().month))
    sym = a.symbol.upper()
    run_funding(sym, date(sy, sm, 1), date(ey, em, 1))
    if a.klines:
        run_um_klines(sym, a.interval, date(sy, sm, 1), date(ey, em, 1))


if __name__ == "__main__":
    main()
