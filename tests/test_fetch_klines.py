"""fetch_klines: der laufende (unvollstaendige) Monat wird nie geschrieben."""
import zipfile
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

import fetch_klines

SYM, IV = "BTCUSDT", "1m"


def make_zip(path: Path, y: int, m: int, n: int = 5) -> Path:
    """Synthetisches Binance-Monatsarchiv ohne Kopfzeile, ms-Timestamps."""
    t0 = int(datetime(y, m, 1, tzinfo=timezone.utc).timestamp() * 1000)
    lines = []
    for i in range(n):
        ot = t0 + i * 60_000
        lines.append(f"{ot},100.0,101.0,99.0,100.5,1.0,{ot + 59_999},100.0,3,0.5,50.0,0")
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w") as z:
        z.writestr(path.with_suffix(".csv").name, "\n".join(lines) + "\n")
    return path


@pytest.fixture
def env(tmp_path, monkeypatch):
    """Store im tmp-Verzeichnis, Download durch Fixture-Archive ersetzt."""
    store = tmp_path / "klines"
    requested: list[str] = []

    def fake_download(url: str, dest: Path):
        requested.append(url)
        name = url.rsplit("/", 1)[-1]                # BTCUSDT-1m-2026-07.zip
        y, m = map(int, name.removesuffix(".zip").rsplit("-", 2)[-2:])
        return make_zip(tmp_path / "raw" / name, y, m)

    monkeypatch.setattr(fetch_klines, "STORE", store)
    monkeypatch.setattr(fetch_klines, "download_verified", fake_download)
    return store, requested


def parquet_names(store: Path) -> list[str]:
    return sorted(p.name for p in store.rglob("*.parquet"))


def test_running_month_is_not_written_or_requested(env):
    store, requested = env
    fetch_klines.run(SYM, IV, date(2026, 6, 1), date(2026, 8, 1), today=date(2026, 8, 20))
    assert parquet_names(store) == [f"{SYM}-{IV}-2026-06.parquet", f"{SYM}-{IV}-2026-07.parquet"]
    assert not any("2026-08" in u for u in requested)
    assert not any("/daily/" in u for u in requested)


def test_month_is_fetched_once_it_is_complete(env):
    store, _ = env
    fetch_klines.run(SYM, IV, date(2026, 8, 1), date(2026, 8, 1), today=date(2026, 8, 31))
    assert parquet_names(store) == []
    fetch_klines.run(SYM, IV, date(2026, 8, 1), date(2026, 8, 1), today=date(2026, 9, 3))
    assert parquet_names(store) == [f"{SYM}-{IV}-2026-08.parquet"]
    df = pd.read_parquet(next(store.rglob("*.parquet")))
    assert len(df) == 5 and str(df["open_time"].dt.tz) == "UTC"


def test_missing_monthly_archive_writes_nothing_and_retries(env, monkeypatch):
    store, _ = env
    monkeypatch.setattr(fetch_klines, "download_verified", lambda url, dest: None)
    fetch_klines.run(SYM, IV, date(2026, 8, 1), date(2026, 8, 1), today=date(2026, 9, 1))
    assert parquet_names(store) == []                # kein Platzhalter -> naechster Lauf holt neu


def test_utc_today():
    assert fetch_klines.utc_today() == datetime.now(timezone.utc).date()
