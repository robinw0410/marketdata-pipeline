"""fetch_dukascopy: bi5-Parsing, 1m-Aggregation, Monatsregel, Validierung, Download.

Alles mit synthetischen bi5-Dateien, kein Netzwerk.
"""
import json
import lzma
import struct
from datetime import date, datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import requests

import fetch_dukascopy as fd

SYM = "XAUUSD"
UTC = timezone.utc


def make_bi5(ticks) -> bytes:
    """ticks: [(ms, ask_int, bid_int, ask_vol, bid_vol), ...] -> LZMA (alone)."""
    raw = b"".join(struct.pack(">3I2f", *t) for t in ticks)
    return lzma.compress(raw, format=lzma.FORMAT_ALONE)


def hour_ms(*args) -> int:
    return int(datetime(*args, tzinfo=UTC).timestamp() * 1000)


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    slept: list[float] = []
    monkeypatch.setattr(fd, "_sleep", slept.append)
    return slept


# ------------------------------------------------------------------- pfade ---
def test_url_month_is_zero_based_cache_month_one_based(tmp_path):
    h = datetime(2024, 1, 5, 13, tzinfo=UTC)
    assert fd.url_for(SYM, h) == f"{fd.BASE}/XAUUSD/2024/00/05/13h_ticks.bi5"
    assert fd.url_for(SYM, datetime(2024, 12, 31, 0, tzinfo=UTC)).endswith("/2024/11/31/00h_ticks.bi5")
    assert fd.raw_path(tmp_path, SYM, h) == \
        tmp_path / "raw" / "dukascopy" / "XAUUSD" / "2024" / "01" / "05" / "13h_ticks.bi5"
    assert fd.store_path(tmp_path, SYM, 2024, 1) == (
        tmp_path / "metals" / "source=dukascopy" / "symbol=XAUUSD" / "year=2024"
        / "XAUUSD-1m-2024-01.parquet")


def test_default_data_root(monkeypatch, tmp_path):
    monkeypatch.delenv("MARKETDATA_DATA_ROOT", raising=False)
    assert fd.default_data_root() == fd.ROOT / "data"
    monkeypatch.setenv("MARKETDATA_DATA_ROOT", str(tmp_path))
    assert fd.default_data_root() == tmp_path


# ----------------------------------------------------------------- parsing ---
def test_decode_divisor_and_ms_offset():
    h0 = hour_ms(2024, 1, 2, 10)
    blob = make_bi5([(1500, 2050123, 2049987, 1.5, 2.25), (3_599_999, 2050200, 2050100, 0.5, 0.75)])
    df = fd.decode_bi5(blob, h0, fd.POINT_DIVISOR[SYM])
    assert len(df) == 2
    assert df["ts"].iloc[0] == pd.Timestamp("2024-01-02 10:00:01.500", tz="UTC")
    assert df["ts"].iloc[1] == pd.Timestamp("2024-01-02 10:59:59.999", tz="UTC")
    assert str(df["ts"].dt.tz) == "UTC"
    assert df["ask"].iloc[0] == pytest.approx(2050.123)
    assert df["bid"].iloc[0] == pytest.approx(2049.987)
    assert df["ask_volume"].iloc[0] == pytest.approx(1.5)
    assert df["bid_volume"].iloc[0] == pytest.approx(2.25)


def test_decode_eurusd_divisor():
    df = fd.decode_bi5(make_bi5([(0, 110012, 110005, 1.0, 1.0)]), 0, fd.POINT_DIVISOR["EURUSD"])
    assert df["ask"].iloc[0] == pytest.approx(1.10012)
    assert df["bid"].iloc[0] == pytest.approx(1.10005)


@pytest.mark.parametrize("blob", [b"", lzma.compress(b"", format=lzma.FORMAT_ALONE), None])
def test_decode_empty(blob):
    df = fd.decode_bi5(blob, 0, 1000)
    assert df.empty and list(df.columns) == ["ts", "bid", "ask", "bid_volume", "ask_volume"]


def test_decode_rejects_bad_offset_and_length():
    with pytest.raises(fd.ValidationError):
        fd.decode_bi5(make_bi5([(3_600_000, 2050000, 2049000, 1.0, 1.0)]), 0, 1000)
    with pytest.raises(fd.ValidationError):
        fd.decode_bi5(lzma.compress(b"\x00" * 19, format=lzma.FORMAT_ALONE), 0, 1000)


# ------------------------------------------------------------- aggregation ---
def test_aggregate_ohlc_and_missing_minutes():
    h0 = hour_ms(2024, 1, 2, 10)
    blob = make_bi5([
        # Minute 10:00: drei Ticks
        (1_000, 2000500, 2000000, 1.0, 2.0),
        (20_000, 2003500, 2003000, 1.0, 2.0),
        (59_999, 2001500, 2001000, 1.0, 2.0),
        # Minute 10:01: keine Ticks
        # Minute 10:02: ein Tick, genau auf der Minutengrenze -> gehoert zu 10:02
        (120_000, 1999500, 1999000, 0.5, 0.25),
    ])
    bars = fd.aggregate_1m(fd.decode_bi5(blob, h0, 1000))
    assert list(bars.columns) == fd.COLUMNS
    assert list(bars["ts"]) == [pd.Timestamp("2024-01-02 10:00", tz="UTC"),
                                pd.Timestamp("2024-01-02 10:02", tz="UTC")]
    b = bars.iloc[0]
    assert (b.bid_open, b.bid_high, b.bid_low, b.bid_close) == pytest.approx((2000.0, 2003.0, 2000.0, 2001.0))
    assert (b.ask_open, b.ask_high, b.ask_low, b.ask_close) == pytest.approx((2000.5, 2003.5, 2000.5, 2001.5))
    assert b.tick_count == 3 and b.ask_volume == pytest.approx(3.0) and b.bid_volume == pytest.approx(6.0)
    b = bars.iloc[1]
    assert b.tick_count == 1 and b.bid_open == b.bid_close == pytest.approx(1999.0)
    assert bars["source"].eq("dukascopy").all()
    assert bars["ts"].dtype == "datetime64[ms, UTC]"
    assert bars["tick_count"].dtype == "int64"


# -------------------------------------------------------- monatsregel / run ---
@pytest.fixture
def fake_net(monkeypatch):
    """Ersetzt download_bi5: je Tag um 10:00 ein paar Ticks, sonst leere Stunden."""
    requested: list[str] = []

    def fake(url: str, **kw):
        requested.append(url)
        if url.endswith("/10h_ticks.bi5"):
            return make_bi5([(0, 2000500, 2000000, 1.0, 1.0), (61_000, 2001500, 2001000, 1.0, 1.0)])
        return b""

    monkeypatch.setattr(fd, "download_bi5", fake)
    return requested


def parquet_names(root: Path) -> list[str]:
    return sorted(p.name for p in (root / "metals").rglob("*.parquet"))


def test_running_month_is_not_written_or_requested(tmp_path, fake_net):
    fd.run(SYM, date(2026, 7, 1), date(2026, 8, 1), data_root=tmp_path,
           today=date(2026, 8, 20), pause=0)
    assert parquet_names(tmp_path) == ["XAUUSD-1m-2026-07.parquet"]
    # Juli = 06 in der URL (0-basiert), August = 07
    assert fake_net and all("/2026/06/" in u for u in fake_net)
    assert not any("/2026/07/" in u for u in fake_net)

    df = pd.read_parquet(fd.store_path(tmp_path, SYM, 2026, 7))
    assert list(df.columns) == fd.COLUMNS
    assert len(df) == 31 * 2 and str(df["ts"].dt.tz) == "UTC"
    rep = json.loads(fd.report_path(tmp_path, SYM, 2026, 7).read_text())
    assert rep["rows"] == 62 and rep["crossed"] == 0 and rep["hours"] == 31 * 24
    assert rep["weekday_minutes_without_ticks"] > 0          # nur Report, kein Abbruch


def test_month_written_once_complete_and_not_overwritten(tmp_path, fake_net):
    fd.run(SYM, date(2026, 8, 1), date(2026, 8, 1), data_root=tmp_path, today=date(2026, 8, 31), pause=0)
    assert parquet_names(tmp_path) == [] and fake_net == []
    fd.run(SYM, date(2026, 8, 1), date(2026, 8, 1), data_root=tmp_path, today=date(2026, 9, 1), pause=0)
    assert parquet_names(tmp_path) == ["XAUUSD-1m-2026-08.parquet"]
    n = len(fake_net)
    fd.run(SYM, date(2026, 8, 1), date(2026, 8, 1), data_root=tmp_path, today=date(2026, 9, 2), pause=0)
    assert len(fake_net) == n                                # existiert -> nichts angefragt


def test_rebuild_uses_raw_cache_without_download(tmp_path, fake_net, monkeypatch):
    fd.run(SYM, date(2026, 7, 1), date(2026, 7, 1), data_root=tmp_path, today=date(2026, 8, 1), pause=0)
    out = fd.store_path(tmp_path, SYM, 2026, 7)
    before = pd.read_parquet(out)

    def offline(url, **kw):
        raise AssertionError(f"Download trotz Cache: {url}")

    monkeypatch.setattr(fd, "download_bi5", offline)
    fd.run(SYM, date(2026, 7, 1), date(2026, 7, 1), data_root=tmp_path, today=date(2026, 8, 1),
           pause=0, rebuild=True)
    pd.testing.assert_frame_equal(pd.read_parquet(out), before)


def test_wrong_divisor_aborts_before_writing(tmp_path, fake_net, monkeypatch):
    monkeypatch.setitem(fd.POINT_DIVISOR, SYM, 100)          # Faktor 10 daneben
    with pytest.raises(fd.ValidationError, match="Plausibilitaetsbereich"):
        fd.run(SYM, date(2026, 7, 1), date(2026, 7, 1), data_root=tmp_path,
               today=date(2026, 8, 1), pause=0)
    assert parquet_names(tmp_path) == []


def test_unknown_symbol_is_refused(tmp_path):
    with pytest.raises(ValueError):
        fd.run("BTCUSD", date(2026, 7, 1), date(2026, 7, 1), data_root=tmp_path, today=date(2026, 8, 1))


# --------------------------------------------------------------- validation ---
MS = pd.Timestamp("2024-01-01", tz="UTC")
ME = pd.Timestamp("2024-02-01", tz="UTC")


def make_bars(n: int = 1000, bid: float = 2000.0, ask: float = 2000.5) -> pd.DataFrame:
    ts = pd.date_range("2024-01-02 00:00", periods=n, freq="1min", tz="UTC").as_unit("ms")
    df = pd.DataFrame({"ts": ts})
    for c in fd.BID:
        df[c] = bid
    for c in fd.ASK:
        df[c] = ask
    df["tick_count"] = 3
    df["bid_volume"] = 1.0
    df["ask_volume"] = 1.0
    df["source"] = "dukascopy"
    return df


def cross(df: pd.DataFrame, rows) -> pd.DataFrame:
    for c in fd.ASK:
        df.loc[rows, c] = 1999.9                            # ask unter bid, OHLC konsistent
    return df


def test_validate_ok():
    rep = fd.validate(make_bars(), SYM, "t", MS, ME)
    assert rep["rows"] == 1000 and rep["crossed"] == 0
    assert rep["spread_outliers"] == 0 and rep["price_jumps"] == 0


def test_crossed_quotes_counted_below_threshold():
    rep = fd.validate(cross(make_bars(), [10]), SYM, "t", MS, ME)   # 0.1 % -> noch ok
    assert rep["crossed"] == 1


def test_crossed_quotes_abort_above_threshold():
    with pytest.raises(fd.ValidationError, match="ask_close < bid_close"):
        fd.validate(cross(make_bars(), [10, 20]), SYM, "t", MS, ME)  # 0.2 %


def test_duplicates_abort():
    df = make_bars()
    df.loc[5, "ts"] = df.loc[4, "ts"]
    with pytest.raises(fd.ValidationError, match="doppelte"):
        fd.validate(df, SYM, "t", MS, ME)


def test_inconsistent_ohlc_and_plausibility_abort():
    df = make_bars()
    df.loc[3, "ask_high"] = 1990.0
    with pytest.raises(fd.ValidationError, match="ask-OHLC"):
        fd.validate(df, SYM, "t", MS, ME)
    with pytest.raises(fd.ValidationError, match="Plausibilitaetsbereich"):
        fd.validate(make_bars(bid=20.0, ask=20.01), SYM, "t", MS, ME)


def test_soft_checks_only_report():
    df = make_bars()
    for c in fd.BID:                                        # Spread 0.5 -> 15.0 (30x)
        df.loc[100, c] = 1985.5
    for side in (fd.BID, fd.ASK):                           # +5 % Sprung
        for c in side:
            df.loc[500, c] = df.loc[500, c] * 1.05
    rep = fd.validate(df, SYM, "t", MS, ME)
    assert rep["spread_outliers"] == 1
    assert rep["spread_outlier_examples"][0]["ts"].startswith("2024-01-02T01:40")
    assert rep["price_jumps"] >= 2                          # hin und zurueck
    assert rep["weekday_minutes_without_ticks"] == rep["weekday_grid_minutes"] - 1000


# ----------------------------------------------------------------- download ---
class FakeResp:
    def __init__(self, status: int, content: bytes = b""):
        self.status_code, self.content = status, content


@pytest.fixture
def fake_get(monkeypatch):
    """Antworten der Reihe nach; Exceptions in der Liste werden geworfen."""
    calls: list[dict] = []
    answers: list = []

    def get(url, headers=None, timeout=None):
        calls.append({"url": url, "headers": headers, "timeout": timeout})
        a = answers.pop(0)
        if isinstance(a, Exception):
            raise a
        return a

    monkeypatch.setattr(fd.requests, "get", get)
    return calls, answers


def test_download_404_returns_none(fake_get):
    calls, answers = fake_get
    answers.append(FakeResp(404))
    assert fd.download_bi5("u") is None
    assert len(calls) == 1
    assert calls[0]["headers"]["User-Agent"] == "marketdata-pipeline/1.0"


def test_download_empty_file(fake_get):
    _, answers = fake_get
    answers.append(FakeResp(200, b""))
    assert fd.download_bi5("u") == b""


def test_download_retries_with_backoff(fake_get, no_sleep):
    calls, answers = fake_get
    good = make_bi5([(0, 2000500, 2000000, 1.0, 1.0)])
    answers += [FakeResp(503), requests.ConnectionError("weg"), FakeResp(200, b"kaputt"),
                FakeResp(200, good)]
    assert fd.download_bi5("u", backoff=1.0) == good
    assert len(calls) == 4
    assert no_sleep == [1.0, 2.0, 4.0]


def test_download_gives_up_after_retries(fake_get, no_sleep):
    calls, answers = fake_get
    answers += [FakeResp(500)] * 3
    with pytest.raises(fd.DownloadError):
        fd.download_bi5("u", retries=2, backoff=0.5)
    assert len(calls) == 3 and no_sleep == [0.5, 1.0]


def test_download_other_http_error_no_retry(fake_get):
    calls, answers = fake_get
    answers.append(FakeResp(403))
    with pytest.raises(fd.DownloadError, match="403"):
        fd.download_bi5("u")
    assert len(calls) == 1


def test_get_hour_caches_blob_and_404(tmp_path, fake_get):
    calls, answers = fake_get
    h_ok = datetime(2024, 1, 5, 13, tzinfo=UTC)
    h_404 = datetime(2024, 1, 5, 14, tzinfo=UTC)
    blob = make_bi5([(0, 2000500, 2000000, 1.0, 1.0)])
    answers += [FakeResp(200, blob), FakeResp(404)]

    assert fd.get_hour(tmp_path, SYM, h_ok, pause=0) == ("ok", blob)
    assert fd.get_hour(tmp_path, SYM, h_404, pause=0) == ("404", None)
    assert calls[0]["url"].endswith("/XAUUSD/2024/00/05/13h_ticks.bi5")
    assert fd.raw_path(tmp_path, SYM, h_ok).read_bytes() == blob
    # zweiter Durchlauf: alles aus dem Cache, kein Request
    assert fd.get_hour(tmp_path, SYM, h_ok, pause=0) == ("cached", blob)
    assert fd.get_hour(tmp_path, SYM, h_404, pause=0) == ("cached_404", None)
    assert len(calls) == 2
