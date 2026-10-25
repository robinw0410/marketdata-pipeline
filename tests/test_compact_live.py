"""compact_live: Stichtag in UTC, laufender UTC-Tag wird nie verdichtet."""
import json
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

import compact_live

SYM, IV = "BTCUSDT", "1m"


def write_day(live_dir: Path, d: date, n: int = 5) -> Path:
    """Synthetische Collector-Tagesdatei mit n geschlossenen 1m-Bars."""
    p = live_dir / f"symbol={SYM}" / f"{SYM}-{IV}-{d.isoformat()}.jsonl"
    p.parent.mkdir(parents=True, exist_ok=True)
    t0 = int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp() * 1000)
    with p.open("w") as f:
        for i in range(n):
            ot = t0 + i * 60_000
            f.write(json.dumps({
                "open_time": ot, "close_time": ot + 59_999,
                "open": "100.0", "high": "101.0", "low": "99.0", "close": "100.5",
                "volume": "1.0", "quote_volume": "100.0", "trades": 3,
                "taker_buy_base": "0.5", "taker_buy_quote": "50.0",
                "src": "ws", "ingested_at": ot + 60_000,
            }) + "\n")
    return p


@pytest.fixture
def store(tmp_path, monkeypatch):
    live, out = tmp_path / "live", tmp_path / "live_parquet"
    monkeypatch.setattr(compact_live, "LIVE", live)
    monkeypatch.setattr(compact_live, "STORE", out)
    return live, out


def test_day_of():
    assert compact_live.day_of(Path("BTCUSDT-1m-2026-08-21.jsonl")) == date(2026, 8, 21)


def test_utc_today_ignores_local_timezone(monkeypatch):
    # UTC+14: lokal ist es meist schon ein anderer Kalendertag als in UTC
    monkeypatch.setenv("TZ", "Etc/GMT-14")
    time.tzset()
    try:
        assert compact_live.utc_today() == datetime.now(timezone.utc).date()
    finally:
        monkeypatch.delenv("TZ")
        time.tzset()


def test_completed_day_is_compacted(store):
    live, out = store
    src = write_day(live, date(2026, 8, 21))
    rep = compact_live.compact_day(SYM, IV, src, today=date(2026, 8, 22))
    assert rep["rows"] == 5 and rep["missing"] == 0
    assert len(pd.read_parquet(out / f"symbol={SYM}" / f"{src.stem}.parquet")) == 5


@pytest.mark.parametrize("today", [date(2026, 8, 21), date(2026, 8, 20)])
def test_running_or_future_day_is_refused(store, today):
    live, out = store
    src = write_day(live, date(2026, 8, 21))
    with pytest.raises(ValueError):
        compact_live.compact_day(SYM, IV, src, today=today)
    assert not out.exists()


def test_main_skips_running_utc_day(store, monkeypatch):
    live, out = store
    write_day(live, date(2026, 8, 21))
    write_day(live, date(2026, 8, 22))
    monkeypatch.setattr(compact_live, "utc_today", lambda: date(2026, 8, 22))
    monkeypatch.setattr(sys, "argv", ["compact_live.py", "--symbol", SYM])
    compact_live.main()
    written = sorted(p.name for p in (out / f"symbol={SYM}").iterdir())
    assert written == [f"{SYM}-{IV}-2026-08-21.parquet"]
