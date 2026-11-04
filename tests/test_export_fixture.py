"""export_fixture: Ausschnitt aus synthetischem Kline-Store -> gzip-CSV."""
import gzip
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

import export_fixture


def write_month(store: Path, symbol: str, interval: str, y: int, m: int,
                start: str, end: str, freq: str = "1min", px: float = 100.0) -> None:
    """Parquet im Layout von fetch_klines (open_time datetime64[ms, UTC])."""
    ot = pd.date_range(start, end, freq=freq, tz="UTC", inclusive="left").as_unit("ms")
    df = pd.DataFrame({"open_time": ot, "open": px, "high": px + 1, "low": px - 1,
                       "close": px + 0.5, "volume": 1.25})
    out = store / f"symbol={symbol}" / f"year={y}" / f"{symbol}-{interval}-{y}-{m:02d}.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out, index=False, compression="zstd")


@pytest.fixture
def store(tmp_path):
    s = tmp_path / "klines"
    # Dezember-Rand und Januar, dazu Stoerfaelle, die NICHT im Export landen duerfen
    write_month(s, "BTCUSDT", "1m", 2023, 12, "2023-12-31 23:00", "2024-01-01")
    write_month(s, "BTCUSDT", "1m", 2024, 1, "2024-01-01", "2024-01-09")
    write_month(s, "BTCUSDT", "5m", 2024, 1, "2024-01-01", "2024-01-09", freq="5min", px=999.0)
    write_month(s, "ETHUSDT", "1m", 2024, 1, "2024-01-01", "2024-01-09", px=555.0)
    return s


def test_export_default_range(store, tmp_path):
    out = tmp_path / "fx" / "BTCUSDT-1m-2024-01-01_07.csv.gz"
    assert export_fixture.main(["--store", str(store), "--out", str(out)]) == 0

    df = pd.read_csv(out)
    assert list(df.columns) == ["open_time", "open", "high", "low", "close", "volume"]
    assert len(df) == 7 * 1440
    assert df["open_time"].iloc[0] == "2024-01-01T00:00:00Z"
    assert df["open_time"].iloc[-1] == "2024-01-07T23:59:00Z"
    assert (df["open"] == 100.0).all() and (df["volume"] == 1.25).all()
    t = pd.to_datetime(df["open_time"], utc=True)
    assert t.is_monotonic_increasing and not t.duplicated().any()


def test_export_is_byte_identical_on_rerun(store, tmp_path):
    a, b = tmp_path / "a.csv.gz", tmp_path / "b.csv.gz"
    export_fixture.main(["--store", str(store), "--out", str(a)])
    export_fixture.main(["--store", str(store), "--out", str(b)])
    assert a.read_bytes() == b.read_bytes()
    assert gzip.decompress(a.read_bytes()).startswith(b"open_time,open,high,low,close,volume\n")


def test_default_out_name():
    p = export_fixture.default_out("BTCUSDT", "1m", date(2024, 1, 1), date(2024, 1, 7))
    assert p == export_fixture.ROOT / "tests" / "fixtures" / "BTCUSDT-1m-2024-01-01_07.csv.gz"


def test_missing_store_or_range_fails(tmp_path, store):
    assert export_fixture.main(["--store", str(tmp_path / "nix"), "--out", str(tmp_path / "x.gz")]) == 2
    assert export_fixture.main(["--store", str(store), "--start", "2025-01-01", "--end", "2025-01-02",
                                "--out", str(tmp_path / "y.gz")]) == 2
    assert not (tmp_path / "x.gz").exists() and not (tmp_path / "y.gz").exists()
