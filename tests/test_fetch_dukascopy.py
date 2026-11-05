"""fetch_dukascopy: bi5-Parsing, 1m-Aggregation, Monatsregel, Validierung, Download,
Kerzenmodus, Fehlertoleranz/Lueckenliste, Drosselung, 429-Sperre, verify-day
(auch --candles-only mit Stichprobe), SIGTERM.

Alles mit synthetischen bi5-Dateien, kein Netzwerk, keine echten Wartezeiten
(Sleep/Uhr injiziert). Bestehende Tick-Tests laufen explizit mit mode="ticks".
"""
import json
import lzma
import os
import random
import signal
import struct
import threading
from datetime import date, datetime, timedelta, timezone
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


NOW = datetime(2026, 11, 5, 12, 0, tzinfo=UTC)


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    slept: list[float] = []
    monkeypatch.setattr(fd, "_sleep", slept.append)
    return slept


@pytest.fixture(autouse=True)
def fixed_now(monkeypatch):
    """Feste Wanduhr (unabhaengig von der Systemzeit); Zeitreisen per now=."""
    monkeypatch.setattr(fd, "utc_now", lambda: NOW)


class FakeSession:
    """requests.Session-Ersatz: delegiert an (das ggf. gepatchte) fd.requests.get."""

    def __init__(self):
        self.headers = {}

    def get(self, *a, **kw):
        return fd.requests.get(*a, **kw)


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def offline(*a, **kw):
        raise AssertionError("Netzwerkzugriff im Test")
    monkeypatch.setattr(fd.requests, "get", offline)
    monkeypatch.setattr(fd.requests, "Session", FakeSession)


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
           today=date(2026, 8, 20), pause=0, mode="ticks")
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
    fd.run(SYM, date(2026, 8, 1), date(2026, 8, 1), data_root=tmp_path, today=date(2026, 8, 31), pause=0,
           mode="ticks")
    assert parquet_names(tmp_path) == [] and fake_net == []
    fd.run(SYM, date(2026, 8, 1), date(2026, 8, 1), data_root=tmp_path, today=date(2026, 9, 1), pause=0,
           mode="ticks")
    assert parquet_names(tmp_path) == ["XAUUSD-1m-2026-08.parquet"]
    n = len(fake_net)
    fd.run(SYM, date(2026, 8, 1), date(2026, 8, 1), data_root=tmp_path, today=date(2026, 9, 2), pause=0,
           mode="ticks")
    assert len(fake_net) == n                                # existiert -> nichts angefragt


def test_rebuild_uses_raw_cache_without_download(tmp_path, fake_net, monkeypatch):
    fd.run(SYM, date(2026, 7, 1), date(2026, 7, 1), data_root=tmp_path, today=date(2026, 8, 1), pause=0,
           mode="ticks")
    out = fd.store_path(tmp_path, SYM, 2026, 7)
    before = pd.read_parquet(out)

    def offline(url, **kw):
        raise AssertionError(f"Download trotz Cache: {url}")

    monkeypatch.setattr(fd, "download_bi5", offline)
    fd.run(SYM, date(2026, 7, 1), date(2026, 7, 1), data_root=tmp_path, today=date(2026, 8, 1),
           pause=0, rebuild=True, mode="ticks")
    pd.testing.assert_frame_equal(pd.read_parquet(out), before)


def test_wrong_divisor_aborts_before_writing(tmp_path, fake_net, monkeypatch):
    monkeypatch.setitem(fd.POINT_DIVISOR, SYM, 100)          # Faktor 10 daneben
    rep = fd.run(SYM, date(2026, 7, 1), date(2026, 7, 1), data_root=tmp_path,
                 today=date(2026, 8, 1), pause=0, mode="ticks")
    assert rep["exit_code"] == 2 and rep["status"] == "validation_error"
    assert "Plausibilitaetsbereich" in rep["validation_errors"][0]["error"]
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
    def __init__(self, status: int, content: bytes = b"", headers: dict | None = None):
        self.status_code, self.content, self.headers = status, content, headers or {}


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
    assert fd.download_bi5("u", backoff_base=1.0, jitter=0) == good
    assert len(calls) == 4
    assert no_sleep == [1.0, 2.0, 4.0]


def test_download_gives_up_after_retries(fake_get, no_sleep):
    calls, answers = fake_get
    answers += [FakeResp(500)] * 3
    with pytest.raises(fd.DownloadError):
        fd.download_bi5("u", attempts=3, backoff_base=0.5, jitter=0)
    assert len(calls) == 3 and no_sleep == [0.5, 1.0]


def test_download_other_http_error_no_retry(fake_get):
    calls, answers = fake_get
    answers.append(FakeResp(401))          # 403 wird inzwischen wiederholt (WAF?)
    with pytest.raises(fd.DownloadError, match="401"):
        fd.download_bi5("u")
    assert len(calls) == 1


def test_get_hour_caches_blob_and_404(tmp_path, fake_get):
    calls, answers = fake_get
    h_ok = datetime(2024, 1, 5, 13, tzinfo=UTC)
    h_404 = datetime(2024, 1, 6, 14, tzinfo=UTC)         # Samstag: 404 sofort endgueltig
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


def test_divisor_factor_ten_too_large_is_caught_by_anchor():
    """Gold mit Divisor 10000 statt 1000 laege bei ~200 und damit noch im groben
    Plausibilitaetsbereich; der Jahresanker (2024 ~2390) muss abbrechen."""
    with pytest.raises(fd.ValidationError, match="Jahresanker"):
        fd.validate(make_bars(bid=205.0, ask=205.05), SYM, "t", MS, ME)
    rep = fd.validate(make_bars(), SYM, "t", MS, ME)
    assert 0.4 < rep["anchor_ratio"] < 2.5


# ===================================================================== neu ===
DAY0 = datetime(2026, 7, 1, tzinfo=UTC)


class FakeClock:
    """Uhr + Schlaf: Schlaf schiebt die Uhr vor, nichts wartet wirklich."""

    def __init__(self):
        self.t = 0.0
        self.slept: list[float] = []

    def __call__(self) -> float:
        return self.t

    def sleep(self, s: float) -> None:
        self.slept.append(s)
        self.t += s


def pack_candles(rows, order: str = "OCLH") -> bytes:
    """rows: [(sec, open, high, low, close, volume)] (Ganzzahl-Preise) -> LZMA-bi5."""
    fields = fd.FIELD_ORDERS[order]
    raw = b""
    for sec, o, h, l, c, v in rows:
        val = {"open": o, "high": h, "low": l, "close": c}
        raw += struct.pack(">5I", sec, *(val[f] for f in fields)) + struct.pack(">f", v)
    return lzma.compress(raw, format=lzma.FORMAT_ALONE)


def day_rows(side: str, base: int = 4_200_000):
    """Ein Tag: 10:00 und 10:02 aktiv, 10:01 Fuellkerze (flach, volume 0)."""
    off = 300 if side == "ASK" else 0
    b = base + off
    return [(600 * 60, b, b + 1000, b - 500, b + 500, 1.5),
            (601 * 60, b + 500, b + 500, b + 500, b + 500, 0.0),
            (602 * 60, b + 500, b + 700, b + 100, b + 200, 2.0)]


def candle_url_side(url: str) -> str:
    return "BID" if url.endswith("BID_candles_min_1.bi5") else "ASK"


@pytest.fixture
def candle_net(monkeypatch):
    """Ersetzt download_bi5 im Kerzenmodus. fail: {url-Teil: Anzahl Fehlschlaege}."""
    state = {"requested": [], "fail": {}, "order": "OCLH", "special": {}}

    def fake(url: str, **kw):
        state["requested"].append(url)
        for part, n in list(state["fail"].items()):
            if part in url and n:
                state["fail"][part] = n - 1 if n > 0 else n      # -1 = immer
                raise fd.DownloadError(f"HTTP 503: {url}", status=503, attempts=kw.get("attempts", 1))
        for part, blob in state["special"].items():
            if part in url:
                return blob
        return pack_candles(day_rows(candle_url_side(url)), state["order"])

    monkeypatch.setattr(fd, "download_bi5", fake)
    return state


def run_c(tmp_path, clock=None, **kw):
    kw.setdefault("today", date(2026, 8, 20))
    kw.setdefault("now", lambda: datetime.combine(kw["today"], datetime.min.time(), UTC)
                  + timedelta(hours=12))
    kw.setdefault("pause", 0)
    clock = clock or FakeClock()
    kw.setdefault("wall", clock)              # Wanduhr der 429-Sperre laeuft mit dem Schlaf
    return fd.run(SYM, kw.pop("start", date(2026, 7, 1)), kw.pop("end", date(2026, 7, 1)),
                  data_root=tmp_path, sleep=clock.sleep, clock=clock, **kw)


JULY_BID_01 = "/XAUUSD/2026/06/01/BID_candles_min_1.bi5"


# ------------------------------------------------------------ kerzen-format ---
def test_candle_url_and_cache_path(tmp_path):
    assert fd.candle_url_for(SYM, date(2024, 1, 5), "BID") == \
        f"{fd.BASE}/XAUUSD/2024/00/05/BID_candles_min_1.bi5"
    assert fd.candle_raw_path(tmp_path, SYM, date(2024, 12, 31), "ASK") == \
        tmp_path / "raw" / "dukascopy" / "XAUUSD" / "2024" / "12" / "31" / "ASK_candles_min_1.bi5"


@pytest.mark.parametrize("order", ["OCLH", "OHLC"])
def test_decode_candles_both_field_orders(order):
    d0 = hour_ms(2024, 3, 6, 0)
    df, found = fd.decode_candles(pack_candles(day_rows("BID", 2_150_000), order), d0, 1000)
    assert found == order
    assert df["ts"].iloc[0] == pd.Timestamp("2024-03-06 10:00", tz="UTC")   # Sekunden, nicht ms
    assert df["ts"].iloc[2] == pd.Timestamp("2024-03-06 10:02", tz="UTC")
    r = df.iloc[0]
    assert (r.open, r.high, r.low, r.close, r.volume) == pytest.approx((2150.0, 2151.0, 2149.5, 2150.5, 1.5))


def test_decode_candles_rejects_bad_offsets_and_length():
    with pytest.raises(fd.ValidationError, match="1 Tag"):
        fd.decode_candles(pack_candles([(86_400_000, 1, 2, 1, 1, 1.0)]), 0, 1000)  # ms statt s
    with pytest.raises(fd.ValidationError, match="Minutengrenze"):
        fd.decode_candles(pack_candles([(61, 1, 2, 1, 1, 1.0)]), 0, 1000)
    with pytest.raises(fd.ValidationError, match="Vielfaches"):
        fd.decode_candles(lzma.compress(b"\0" * 23, format=lzma.FORMAT_ALONE), 0, 1000)


def test_merge_candles_drops_fill_minutes():
    d0 = int(DAY0.timestamp() * 1000)
    bid, _ = fd.decode_candles(pack_candles(day_rows("BID")), d0, 1000)
    ask_rows = day_rows("ASK") + [(700 * 60, 4_201_000, 4_201_000, 4_201_000, 4_201_000, 0.0)]
    ask, _ = fd.decode_candles(pack_candles(ask_rows), d0, 1000)
    bars, st = fd.merge_candles(bid, ask)
    assert list(bars.columns) == fd.COLUMNS
    assert list(bars["ts"]) == [pd.Timestamp("2026-07-01 10:00", tz="UTC"),
                                pd.Timestamp("2026-07-01 10:02", tz="UTC")]
    assert st == {"fill_minutes_dropped": 1, "one_side_minutes_dropped": 1,
                  "zero_volume_nonflat_minutes": 0}
    b = bars.iloc[0]
    assert (b.bid_open, b.bid_high, b.bid_low, b.bid_close) == pytest.approx((4200.0, 4201.0, 4199.5, 4200.5))
    assert (b.ask_open, b.ask_close) == pytest.approx((4200.3, 4200.8))
    assert (bars["tick_count"] == 1).all() and bars["tick_count"].dtype == "int64"
    assert b.bid_volume == pytest.approx(1.5) and bars["source"].eq("dukascopy").all()


def test_candles_month_written_with_mode(tmp_path, candle_net):
    rep = run_c(tmp_path)
    assert rep["exit_code"] == 0 and rep["mode"] == "candles" and rep["gaps"] == []
    # 31 Tage x 2 Seiten, keine Tick-Stunden
    assert len(candle_net["requested"]) == 62
    assert all(u.endswith("_candles_min_1.bi5") and "/2026/06/" in u for u in candle_net["requested"])
    out = fd.store_path(tmp_path, SYM, 2026, 7)
    df = pd.read_parquet(out)
    assert list(df.columns) == fd.COLUMNS and len(df) == 31 * 2
    import pyarrow.parquet as pq
    assert pq.read_schema(out).metadata[b"dukascopy_mode"] == b"candles"
    mrep = json.loads(fd.report_path(tmp_path, SYM, 2026, 7).read_text())
    assert mrep["mode"] == "candles" and mrep["fill_minutes_dropped"] == 31
    assert mrep["field_order"] == {"OCLH": 62}
    assert mrep["continuity"]["bid"]["pairs"] == 0      # 10:00 -> 10:02 nicht direkt aufeinander
    assert fd.candle_raw_path(tmp_path, SYM, DAY0, "BID").exists()


def test_candles_ohlc_order_gives_same_store(tmp_path, candle_net):
    run_c(tmp_path / "a")
    candle_net["order"] = "OHLC"
    run_c(tmp_path / "b")
    pd.testing.assert_frame_equal(pd.read_parquet(fd.store_path(tmp_path / "a", SYM, 2026, 7)),
                                  pd.read_parquet(fd.store_path(tmp_path / "b", SYM, 2026, 7)))


def test_day_incomplete_if_one_side_fails(tmp_path, candle_net):
    candle_net["fail"]["/2026/06/15/ASK_candles"] = -1
    rep = run_c(tmp_path, final_retries=0)
    assert rep["exit_code"] == 3
    assert parquet_names(tmp_path) == []
    assert [g["file"] for g in rep["gaps"]] == ["XAUUSD 2026-07-15 ASK candles"]
    assert fd.candle_raw_path(tmp_path, SYM, datetime(2026, 7, 15, tzinfo=UTC), "BID").exists()


def test_day_with_one_side_404_needs_confirmation_then_dropped(tmp_path, candle_net, monkeypatch):
    inner = fd.download_bi5
    monkeypatch.setattr(fd, "download_bi5",
                        lambda url, **kw: None if "/2026/06/15/ASK" in url else inner(url, **kw))
    rep = run_c(tmp_path)                                     # Mi 15.07.: Werktag
    assert rep["exit_code"] == 3 and parquet_names(tmp_path) == []
    assert [u["file"] for u in rep["unconfirmed"]] == ["XAUUSD 2026-07-15 ASK candles"]
    t2 = datetime(2026, 8, 21, 13, tzinfo=UTC)                # > 24 h spaeter: erneut 404
    rep = run_c(tmp_path, today=date(2026, 8, 21), now=lambda: t2)
    assert rep["exit_code"] == 0
    mrep = json.loads(fd.report_path(tmp_path, SYM, 2026, 7).read_text())
    assert mrep["days_one_side_only"] == ["2026-07-15 nur BID"] and mrep["rows"] == 30 * 2


def test_running_day_candle_file_never_requested(tmp_path, fake_get):
    calls, _ = fake_get
    f = fd.RawFile(SYM, "candles", datetime(2026, 8, 20, tzinfo=UTC), "BID")
    with pytest.raises(ValueError, match="laufenden"):
        fd.Fetcher(tmp_path, today=date(2026, 8, 20), pause=0).get(f)
    assert calls == [] and not f.path(tmp_path).exists()


# -------------------------------------------------------- fehlertoleranz ---
def test_failure_in_first_hour_does_not_abort_ticks(tmp_path, fake_net, monkeypatch):
    inner = fd.download_bi5

    def flaky(url, **kw):
        if url.endswith("/2026/06/01/00h_ticks.bi5"):
            fake_net.append(url)
            raise fd.DownloadError("HTTP 503", status=503, attempts=kw["attempts"])
        return inner(url, **kw)

    monkeypatch.setattr(fd, "download_bi5", flaky)
    clock = FakeClock()
    rep = run_c(tmp_path, clock, mode="ticks", final_retries=2, final_wait=300)
    assert len(set(fake_net)) == 31 * 24                     # alle uebrigen Stunden versucht
    assert fake_net.count(f"{fd.BASE}/XAUUSD/2026/06/01/00h_ticks.bi5") == 3   # 1 + 2 Runden
    assert clock.slept.count(300) == 2
    assert rep["exit_code"] == 3 and rep["status"] == "gaps"
    assert rep["summary"]["months_incomplete"] == ["2026-07"]
    (g,) = rep["gaps"]
    assert (g["symbol"], g["kind"], g["date"], g["hour"], g["http_status"]) == \
        (SYM, "ticks", "2026-07-01", 0, 503)
    assert g["attempts"] == 3 * 5 and "503" in g["last_error"]
    assert parquet_names(tmp_path) == []
    # Report auch auf Platte
    assert json.loads(fd.run_report_path(tmp_path, SYM).read_text())["gaps"] == rep["gaps"]


def test_collected_retry_at_end_recovers_and_writes(tmp_path, candle_net):
    candle_net["fail"][JULY_BID_01] = 1                      # erster Versuch scheitert
    clock = FakeClock()
    rep = run_c(tmp_path, clock, final_wait=300)
    assert rep["exit_code"] == 0 and rep["gaps"] == []
    assert rep["summary"]["recovered_in_final_retries"] == 1
    assert rep["summary"]["final_retry_rounds"] == 1 and clock.slept == [300]
    # der Fehlschlag wurde erst am Ende wiederholt, nicht sofort
    req = candle_net["requested"]
    assert req[0].endswith(JULY_BID_01) and req[-1].endswith(JULY_BID_01) and len(req) == 63
    assert parquet_names(tmp_path) == ["XAUUSD-1m-2026-07.parquet"]


def test_gappy_month_not_written_followup_fetches_only_missing(tmp_path, candle_net):
    candle_net["fail"][JULY_BID_01] = -1
    rep = run_c(tmp_path, end=date(2026, 7, 1), final_retries=1, final_wait=10)
    assert rep["exit_code"] == 3 and parquet_names(tmp_path) == []
    assert rep["gaps"][0]["file"] == "XAUUSD 2026-07-01 BID candles"

    candle_net["fail"].clear()
    candle_net["requested"].clear()
    rep = run_c(tmp_path, end=date(2026, 7, 1))
    assert candle_net["requested"] == [f"{fd.BASE}{JULY_BID_01}"]    # nur die fehlende Datei
    assert rep["exit_code"] == 0 and rep["gaps"] == []
    assert rep["summary"]["files_pending_at_start"] == 1 and rep["summary"]["files_downloaded"] == 1
    assert parquet_names(tmp_path) == ["XAUUSD-1m-2026-07.parquet"]


def test_previous_gaps_are_tried_first(tmp_path, candle_net):
    candle_net["fail"]["/2026/07/31/ASK"] = -1               # August, letzter Tag
    rep = run_c(tmp_path, end=date(2026, 8, 1), today=date(2026, 9, 2), final_retries=0)
    assert parquet_names(tmp_path) == ["XAUUSD-1m-2026-07.parquet"]
    assert rep["summary"]["months_incomplete"] == ["2026-08"]
    # Juli kuenstlich wieder offen machen (Monatsdatei + eine Rohdatei weg)
    fd.store_path(tmp_path, SYM, 2026, 7).unlink()
    fd.candle_raw_path(tmp_path, SYM, DAY0, "BID").unlink()
    candle_net["fail"].clear()
    candle_net["requested"].clear()
    rep = run_c(tmp_path, end=date(2026, 8, 1), today=date(2026, 9, 2))
    # Luecke des letzten Laufs zuerst, dann der normale Monatsablauf
    assert candle_net["requested"] == [f"{fd.BASE}/XAUUSD/2026/07/31/ASK_candles_min_1.bi5",
                                       f"{fd.BASE}{JULY_BID_01}"]
    assert rep["exit_code"] == 0 and rep["summary"]["months_written"] == ["2026-07", "2026-08"]


def test_corrupt_run_report_is_ignored(tmp_path, candle_net):
    p = fd.run_report_path(tmp_path, SYM)
    p.parent.mkdir(parents=True)
    p.write_text("{kaputt")
    assert run_c(tmp_path)["exit_code"] == 0


def test_validation_error_still_writes_run_report(tmp_path, candle_net, monkeypatch):
    monkeypatch.setitem(fd.POINT_DIVISOR, SYM, 100)
    assert run_c(tmp_path)["exit_code"] == 2
    rep = json.loads(fd.run_report_path(tmp_path, SYM).read_text())
    assert rep["status"] == "validation_error" and rep["exit_code"] == 2
    assert rep["validation_errors"][0]["month"] == "2026-07"


# ------------------------------------------------------- backoff/drossel ---
def test_backoff_exponential_capped_and_retry_after():
    seq = [fd.backoff_delay(n, 2.0, 600.0, jitter=0) for n in range(12)]
    assert seq[:6] == [2, 4, 8, 16, 32, 64] and seq[-3:] == [600, 600, 600]
    assert fd.backoff_delay(0, 2.0, 600.0, jitter=0, retry_after=30) == 30
    assert fd.backoff_delay(5, 2.0, 600.0, jitter=0, retry_after=1) == 64
    assert fd.backoff_delay(0, 2.0, 600.0, jitter=0, retry_after=10_000) == 600
    rng = random.Random(1)
    for n in range(12):
        d = fd.backoff_delay(n, 2.0, 600.0, jitter=0.5, rng=rng)
        assert min(600, 2 * 2 ** n) * 0.5 <= d <= min(600, 2 * 2 ** n)
    assert fd.parse_retry_after("12") == 12.0 and fd.parse_retry_after("x") is None
    assert fd.parse_retry_after("Wed, 21 Oct 2015 07:28:10 GMT",
                                now=datetime(2015, 10, 21, 7, 28, 0, tzinfo=UTC)) == 10.0


def test_download_respects_retry_after(fake_get, no_sleep):
    calls, answers = fake_get
    good = make_bi5([(0, 2000500, 2000000, 1.0, 1.0)])
    answers += [FakeResp(500, headers={"Retry-After": "45"}), FakeResp(200, good)]
    assert fd.download_bi5("u", backoff_base=2, jitter=0) == good
    assert no_sleep == [45.0]
    assert calls[0]["timeout"] == (fd.CONNECT_TIMEOUT_S, fd.READ_TIMEOUT_S) == (10.0, 60.0)


def test_503_sets_global_cooldown_for_all_workers(fake_get):
    calls, answers = fake_get
    clock = FakeClock()
    th = fd.Throttle(clock, clock.sleep)
    good = make_bi5([(0, 2000500, 2000000, 1.0, 1.0)])
    answers += [FakeResp(503, headers={"Retry-After": "30"}), FakeResp(200, good)]
    # Worker A: ein Versuch, 503 -> gibt auf, setzt aber die globale Abkuehlung
    with pytest.raises(fd.DownloadError):
        fd.download_bi5("a", attempts=1, jitter=0, throttle=th, sleep=clock.sleep)
    assert th.not_before == 30 and th.cooldowns == 1 and clock.slept == []
    # Worker B wartet vor seinem Request bis zum gemeinsamen Zeitpunkt
    assert fd.download_bi5("b", jitter=0, throttle=th, sleep=clock.sleep) == good
    assert clock.slept == [30] and len(calls) == 2


def test_403_is_retried_with_cooldown_not_404(fake_get):
    calls, answers = fake_get
    clock = FakeClock()
    th = fd.Throttle(clock, clock.sleep, jitter=0)
    good = make_bi5([(0, 2000500, 2000000, 1.0, 1.0)])
    answers += [FakeResp(403), FakeResp(403), FakeResp(200, good)]
    assert fd.download_bi5("u", backoff_base=2, jitter=0, throttle=th, sleep=clock.sleep) == good
    assert len(calls) == 3 and clock.slept == [2, 4] and th.cooldowns == 2


def test_mostly_403_run_reports_waf_hint(tmp_path, fake_get, monkeypatch):
    calls, answers = fake_get
    monkeypatch.setattr(fd.requests, "get", lambda url, headers=None, timeout=None:
                        (calls.append(url), FakeResp(403))[1])
    clock = FakeClock()
    rep = run_c(tmp_path, clock, attempts=2, final_retries=0, max_consecutive_failures=0)
    assert rep["exit_code"] == 3 and len(rep["gaps"]) == 62
    s = rep["summary"]
    assert s["http_403_waf_suspected"] == 124 == s["requests"] and s["http_status_counts"] == {"403": 124}
    assert "WAF" in rep["hint"] and rep["gaps"][0]["http_status"] == 403
    assert "(WAF?)" in rep["gaps"][0]["last_error"]
    assert not list((tmp_path / "raw").rglob("*.404"))       # 403 ist nicht "keine Daten"


def test_pause_between_requests_and_cache_hits_are_free(tmp_path, candle_net):
    clock = FakeClock()
    rep = run_c(tmp_path, clock, pause=1.0)
    assert clock.slept == [1.0] * 62
    clock2 = FakeClock()
    candle_net["requested"].clear()
    rep = run_c(tmp_path, clock2, pause=1.0, rebuild=True)    # alles im Cache
    assert candle_net["requested"] == [] and clock2.slept == [] and rep["summary"]["requests"] == 0


def test_workers_parallel_same_result(tmp_path, candle_net):
    run_c(tmp_path / "w1", workers=1)
    n = len(candle_net["requested"])
    run_c(tmp_path / "w4", workers=4)
    assert len(candle_net["requested"]) == 2 * n
    pd.testing.assert_frame_equal(pd.read_parquet(fd.store_path(tmp_path / "w1", SYM, 2026, 7)),
                                  pd.read_parquet(fd.store_path(tmp_path / "w4", SYM, 2026, 7)))


def test_progress_logged(tmp_path, candle_net, caplog):
    with caplog.at_level("INFO", logger="dukascopy"):
        run_c(tmp_path, progress_every=20)
    lines = [r.getMessage() for r in caplog.records if r.getMessage().startswith("FORTSCHRITT")]
    assert [ln.split()[1] for ln in lines] == ["20/62", "40/62", "60/62", "62/62"]
    assert "ETA" in lines[0]


def test_cli_defaults(monkeypatch):
    seen = {}
    monkeypatch.setattr(fd, "install_stop_handler", lambda stop: None)
    monkeypatch.setattr(fd, "run", lambda *a, **kw: seen.update(kw) or {"exit_code": 0})
    assert fd.main(["--symbol", "xauusd", "--start", "2024-01", "--end", "2024-02"]) == 0
    assert seen["mode"] == "candles" and seen["workers"] == 1 and seen["pause"] == 1.0
    assert seen["attempts"] == 5 and seen["backoff_base"] == 2.0 and seen["max_backoff"] == 600.0
    assert seen["final_retries"] == 2 and seen["final_wait"] == 300.0
    assert seen["timeout"] == (10.0, 60.0) and seen["max_consecutive_failures"] == 20
    assert seen["rate_limit_pause"] == 900.0 and seen["max_rate_limit_wait"] == 21600.0
    monkeypatch.setattr(fd, "run", lambda *a, **kw: {"exit_code": 3, "summary": {
        "failed_files": 1, "unconfirmed_files": 0, "months_incomplete": ["2024-01"]}})
    assert fd.main(["--symbol", "XAUUSD", "--start", "2024-01", "--mode", "ticks"]) == 3


# ----------------------------------------------------------------- sigterm ---
def test_stop_writes_report_and_exit_3(tmp_path, candle_net, monkeypatch):
    stop = threading.Event()
    inner = fd.download_bi5

    def stopping(url, **kw):
        if len(candle_net["requested"]) == 10:
            stop.set()                                        # "SIGTERM" waehrend des Laufs
        return inner(url, **kw)

    monkeypatch.setattr(fd, "download_bi5", stopping)
    rep = run_c(tmp_path, end=date(2026, 7, 1), stop=stop)
    assert rep["status"] == "interrupted" and rep["exit_code"] == 3
    assert rep["summary"]["interrupted"] and rep["summary"]["months_incomplete"] == ["2026-07"]
    assert len(candle_net["requested"]) == 11 and parquet_names(tmp_path) == []
    assert json.loads(fd.run_report_path(tmp_path, SYM).read_text())["status"] == "interrupted"
    # Neustart setzt ohne Doppelarbeit fort
    candle_net["requested"].clear()
    monkeypatch.setattr(fd, "download_bi5", inner)
    rep = run_c(tmp_path)
    assert len(candle_net["requested"]) == 62 - 11 and rep["exit_code"] == 0


def test_sigterm_handler_sets_stop():
    stop = threading.Event()
    old = {s: signal.getsignal(s) for s in (signal.SIGTERM, signal.SIGINT)}
    try:
        fd.install_stop_handler(stop)
        os.kill(os.getpid(), signal.SIGTERM)
        assert stop.wait(2)
    finally:
        for s, h in old.items():
            signal.signal(s, h)


# -------------------------------------------------------------- verify-day ---
VDAY = date(2024, 3, 6)


def verify_files(tmp_path, shift_close_points: int = 0):
    """Tick-Stunden (nur 10h mit Ticks) und daraus abgeleitete Kerzen in den Cache."""
    d0 = datetime(2024, 3, 6, tzinfo=UTC)
    ticks = [  # (ms, ask, bid, ask_vol, bid_vol)
        (1_000, 2150500, 2150000, 1.0, 2.0), (20_000, 2153500, 2153000, 1.0, 2.0),
        (59_999, 2151500, 2151000, 1.0, 2.0), (120_000, 2149500, 2149000, 0.5, 0.25)]
    for h in range(24):
        fd._atomic_write(fd.raw_path(tmp_path, SYM, d0 + timedelta(hours=h)),
                         make_bi5(ticks) if h == 10 else b"")
    for side, i, vi in (("BID", 2, 4), ("ASK", 1, 3)):
        rows = []
        for minute in range(1440):
            sel = [t for t in ticks if (10 * 3600_000 + t[0]) // 60_000 == minute]
            if sel:
                p = [t[i] for t in sel]
                rows.append((minute * 60, p[0], max(p), min(p), p[-1], sum(t[vi] for t in sel)))
            else:
                last = rows[-1][4] if rows else 2150000
                rows.append((minute * 60, last, last, last, last, 0.0))
        if side == "BID" and shift_close_points:
            r = rows[600]
            rows[600] = (r[0], r[1], r[2] + shift_close_points, r[3], r[4] + shift_close_points, r[5])
        fd._atomic_write(fd.candle_raw_path(tmp_path, SYM, d0, side), pack_candles(rows))


def test_verify_day_match(tmp_path, fake_get):
    calls, _ = fake_get
    verify_files(tmp_path)
    rep = fd.verify_day(SYM, VDAY, data_root=tmp_path, today=date(2024, 3, 7), pause=0)
    assert calls == []                                       # alles aus dem Cache
    assert rep["ok"] and rep["exit_code"] == 0
    assert rep["field_order"] == {"BID": "OCLH", "ASK": "OCLH"}
    assert rep["minutes_common"] == 2 and rep["minutes_only_ticks"] == rep["minutes_only_candles"] == 0
    assert max(rep["max_abs_dev_points"].values()) == pytest.approx(0, abs=1e-6)
    assert rep["fill_minutes_dropped"] == 1438
    assert fd.verify_report_path(tmp_path, SYM, VDAY).exists()
    # auch der Tickvergleich schreibt die Kerzen-Minuten als Stichprobe
    sp = fd.sample_path(tmp_path, SYM, VDAY)
    assert rep["sample_path"] == str(sp) and len(pd.read_parquet(sp)) == 2
    assert rep["rate_limit"]["hits_429"] == 0 and rep["variant"] == "ticks_vs_candles"


def test_verify_day_mismatch(tmp_path, fake_get):
    verify_files(tmp_path, shift_close_points=5)
    rep = fd.verify_day(SYM, VDAY, data_root=tmp_path, today=date(2024, 3, 7), pause=0)
    assert not rep["ok"] and rep["exit_code"] == 4
    assert rep["minutes_deviating"] == 1
    assert rep["max_abs_dev_points"]["bid_close"] == pytest.approx(5)
    assert rep["deviating_examples"][0]["ts"].startswith("2024-03-06T10:00")


def test_verify_day_refuses_running_day(tmp_path):
    with pytest.raises(ValueError):
        fd.verify_day(SYM, VDAY, data_root=tmp_path, today=VDAY)
    with pytest.raises(ValueError):                         # Tagesende erst 3 h her
        fd.verify_day(SYM, VDAY, data_root=tmp_path, today=date(2024, 3, 7),
                      now=lambda: datetime(2024, 3, 7, 3, tzinfo=UTC))


# ========================================================== review-funde ===
def none_fit_rows():
    # weder OCLH noch OHLC: Feld 2 und Feld 4 jeweils unter open
    return [(600 * 60, 4_200_000, 4_190_000, 4_195_000, 4_180_000, 1.0)]


def amb_rows():
    # close == high in jeder aktiven Kerze -> beide Reihenfolgen passen
    return [(600 * 60, 4_199_000, 4_200_000, 4_198_500, 4_200_000, 1.0)]


def raw_none_fit() -> bytes:
    raw = b"".join(struct.pack(">5If", *r) for r in none_fit_rows())
    return lzma.compress(raw, format=lzma.FORMAT_ALONE)


# H1 ---------------------------------------------------------------------
def test_field_order_ambiguous_flat_and_none():
    rec = fd.candle_records(pack_candles(amb_rows(), "OCLH"))
    assert fd.detect_field_order(rec) == "ambiguous"
    b = pack_candles(amb_rows(), "OCLH")
    d_amb, order = fd.decode_candles(b, 0, 1000)
    assert order == "ambiguous"
    for o in ("OCLH", "OHLC"):                               # beide Lesarten identisch
        pd.testing.assert_frame_equal(d_amb, fd.decode_candles(b, 0, 1000, o)[0])
    assert fd.detect_field_order(fd.candle_records(pack_candles([(0, 5, 5, 5, 5, 0.0)]))) == "flat"
    with pytest.raises(fd.FieldOrderError, match="unklar"):
        fd.detect_field_order(fd.candle_records(raw_none_fit()))


def test_ambiguous_day_is_written(tmp_path, candle_net):
    candle_net["special"]["/2026/06/03/BID"] = pack_candles(amb_rows())
    rep = run_c(tmp_path)
    assert rep["exit_code"] == 0 and rep["quarantine"] == []
    mrep = json.loads(fd.report_path(tmp_path, SYM, 2026, 7).read_text())
    assert mrep["field_order"] == {"OCLH": 61, "ambiguous": 1}


def test_no_fitting_order_goes_to_quarantine_exit_4(tmp_path, candle_net):
    url = f"{fd.BASE}/XAUUSD/2026/06/03/BID_candles_min_1.bi5"
    candle_net["special"]["/2026/06/03/BID"] = raw_none_fit()
    rep = run_c(tmp_path, final_retries=2)
    assert rep["exit_code"] == 4 and rep["status"] == "quarantine"
    assert rep["gaps"] == [] and rep["summary"]["final_retry_rounds"] == 0   # kein Endlos-Retry
    (q,) = rep["quarantine"]
    assert q["file"] == "XAUUSD 2026-07-03 BID candles" and "unklar" in q["reason"]
    assert rep["summary"]["months_quarantine"] == ["2026-07"] and parquet_names(tmp_path) == []
    f = fd.RawFile(SYM, "candles", datetime(2026, 7, 3, tzinfo=UTC), "BID")
    assert not f.path(tmp_path).exists() and f.marker(tmp_path, "quarantine").exists()
    assert candle_net["requested"].count(url) == 1
    # Folgelauf: Quarantaene ist klebrig, kein erneuter Request, weiter Exit 4
    candle_net["requested"].clear()
    rep = run_c(tmp_path)
    assert candle_net["requested"] == [] and rep["exit_code"] == 4
    # manuell freigeben (Quarantaene loeschen) -> neu laden -> fertig
    del candle_net["special"]["/2026/06/03/BID"]
    f.marker(tmp_path, "quarantine").unlink()
    assert run_c(tmp_path)["exit_code"] == 0


def test_cached_file_without_fitting_order_moved_to_quarantine(tmp_path, candle_net):
    f = fd.RawFile(SYM, "candles", datetime(2026, 7, 3, tzinfo=UTC), "BID")
    fd._atomic_write(f.path(tmp_path), raw_none_fit())
    rep = run_c(tmp_path)
    assert rep["exit_code"] == 4 and not f.path(tmp_path).exists()
    assert f.marker(tmp_path, "quarantine").exists()
    assert f.url() not in candle_net["requested"]


# H2 / N4 ----------------------------------------------------------------
def test_throttle_escalates_across_files_and_resets_on_success():
    clock = FakeClock()
    th = fd.Throttle(clock, clock.sleep, base=2, cap=600, jitter=0, breaker=0)
    assert [th.failure("x") for _ in range(4)] == [2, 4, 8, 16]
    th.success()
    assert th.failure("x") == 2
    for _ in range(20):
        th.failure("x")
    assert th.failure("x") == 600 and not th.tripped.is_set()


def test_persistent_503_escalates_to_minutes_and_breaker_stops(tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr(fd.requests, "get", lambda url, headers=None, timeout=None:
                        (calls.append(url), FakeResp(503))[1])
    clock = FakeClock()
    rep = run_c(tmp_path, clock, jitter=0, final_retries=2)
    assert len(calls) == 20                                   # Schutzschalter nach 20
    assert rep["status"] == "circuit_breaker" and rep["exit_code"] == 3
    assert rep["summary"]["circuit_breaker"] and rep["summary"]["final_retry_rounds"] == 0
    assert "Schutzschalter" in rep["hint"]
    # globale Abkuehlung eskaliert ueber Dateigrenzen (5 Versuche je Datei) bis 600 s
    assert clock.slept[:10] == [2, 4, 8, 16, 32, 64, 128, 256, 512, 600]
    assert max(clock.slept) == 600 and 300 not in clock.slept   # keine Endrunde
    assert len(rep["gaps"]) == 4


def test_connect_timeout_and_5xx_trigger_global_cooldown(fake_get):
    calls, answers = fake_get
    clock = FakeClock()
    th = fd.Throttle(clock, clock.sleep, jitter=0)
    good = make_bi5([(0, 2000500, 2000000, 1.0, 1.0)])
    answers += [requests.ConnectTimeout("weg"), FakeResp(502), FakeResp(200, good)]
    assert fd.download_bi5("u", throttle=th, sleep=clock.sleep) == good
    assert th.cooldowns == 2 and clock.slept == [2, 4] and th.consecutive == 0


def test_session_per_worker_thread(tmp_path):
    fe = fd.Fetcher(tmp_path)
    s1 = fe.session()
    assert fe.session() is s1 and s1.headers["User-Agent"] == fd.USER_AGENT
    other = []
    t = threading.Thread(target=lambda: other.append(fe.session()))
    t.start()
    t.join()
    assert other[0] is not s1


# M1 ---------------------------------------------------------------------
def test_needs_confirmation_rules():
    def f(kind, d):
        return fd.RawFile(SYM, kind, datetime(*d, tzinfo=UTC), "BID" if kind == "candles" else "")
    assert fd.needs_confirmation(f("candles", (2026, 7, 8)), "404")          # Mittwoch
    assert fd.needs_confirmation(f("candles", (2026, 7, 8)), "empty")
    assert not fd.needs_confirmation(f("candles", (2026, 7, 4)), "404")      # Samstag
    assert not fd.needs_confirmation(f("candles", (2025, 12, 25)), "empty")  # Weihnachten (Do)
    assert not fd.needs_confirmation(f("candles", (2026, 1, 1)), "404")      # Neujahr (Do)
    assert fd.needs_confirmation(f("ticks", (2026, 7, 8, 21)), "404")
    assert not fd.needs_confirmation(f("ticks", (2026, 7, 8, 21)), "empty")  # Tagespause normal


def test_weekday_404_confirmed_only_after_24h(tmp_path, candle_net):
    for s in ("BID", "ASK"):
        candle_net["special"][f"/2026/06/08/{s}"] = None       # Mittwoch: beide 404
    rep = run_c(tmp_path)
    assert rep["exit_code"] == 3 and parquet_names(tmp_path) == []
    assert len(rep["unconfirmed"]) == 2 and rep["summary"]["unconfirmed_files"] == 2
    f = fd.RawFile(SYM, "candles", datetime(2026, 7, 8, tzinfo=UTC), "BID")
    assert json.loads(f.marker(tmp_path, "404").read_text())["confirmed"] is None
    # < 24 h spaeter: kein Request, weiter Luecke
    candle_net["requested"].clear()
    rep = run_c(tmp_path, today=date(2026, 8, 21), now=lambda: datetime(2026, 8, 21, 6, tzinfo=UTC))
    assert candle_net["requested"] == [] and rep["exit_code"] == 3
    # > 24 h spaeter: erneut 404 -> endgueltig, Monat wird geschrieben
    rep = run_c(tmp_path, today=date(2026, 8, 21), now=lambda: datetime(2026, 8, 21, 13, tzinfo=UTC))
    assert len(candle_net["requested"]) == 2 and rep["exit_code"] == 0
    assert json.loads(f.marker(tmp_path, "404").read_text())["confirmed"] == "repeat"
    assert json.loads(fd.report_path(tmp_path, SYM, 2026, 7).read_text())["days_404"] == 1


def test_weekday_no_data_then_data_clears_marker(tmp_path, candle_net):
    candle_net["special"]["/2026/06/08/BID"] = b""             # leere 200 an Werktag
    assert run_c(tmp_path)["exit_code"] == 3
    f = fd.RawFile(SYM, "candles", datetime(2026, 7, 8, tzinfo=UTC), "BID")
    assert f.marker(tmp_path, "empty").exists() and not f.path(tmp_path).exists()
    del candle_net["special"]["/2026/06/08/BID"]
    rep = run_c(tmp_path, today=date(2026, 8, 22), now=lambda: datetime(2026, 8, 22, 12, tzinfo=UTC))
    assert rep["exit_code"] == 0 and f.path(tmp_path).exists()
    assert not f.marker(tmp_path, "empty").exists()


def test_empty_weekend_response_is_marker_not_raw_file(tmp_path, candle_net):
    for s in ("BID", "ASK"):
        candle_net["special"][f"/2026/06/04/{s}"] = b""        # Samstag
    rep = run_c(tmp_path)
    assert rep["exit_code"] == 0
    f = fd.RawFile(SYM, "candles", datetime(2026, 7, 4, tzinfo=UTC), "BID")
    assert not f.path(tmp_path).exists()
    assert json.loads(f.marker(tmp_path, "empty").read_text())["confirmed"] == "auto"
    assert not [p for p in (tmp_path / "raw").rglob("*.bi5") if p.stat().st_size == 0]


def test_one_sided_weekend_day_becomes_gap_until_confirmed(tmp_path, candle_net):
    candle_net["special"]["/2026/06/05/ASK"] = b""            # Sonntag: nur BID
    rep = run_c(tmp_path)
    assert rep["exit_code"] == 3 and parquet_names(tmp_path) == []
    assert rep["unconfirmed"][0]["file"] == "XAUUSD 2026-07-05 ASK candles"
    assert "einseitig" in rep["unconfirmed"][0]["reason"]
    rep = run_c(tmp_path, today=date(2026, 8, 22), now=lambda: datetime(2026, 8, 22, 12, tzinfo=UTC))
    assert rep["exit_code"] == 0
    mrep = json.loads(fd.report_path(tmp_path, SYM, 2026, 7).read_text())
    assert mrep["days_one_side_only"] == ["2026-07-05 nur BID"]


def test_legacy_zero_byte_marker_counts_as_confirmed(tmp_path):
    f = fd.RawFile(SYM, "candles", datetime(2026, 7, 8, tzinfo=UTC), "BID")
    fd._atomic_write(f.marker(tmp_path, "404"), b"")
    assert fd.Fetcher(tmp_path).state(f)[0] == "404"


def test_month_end_less_than_6h_ago_not_requested(tmp_path, candle_net, fake_get):
    rep = run_c(tmp_path, today=date(2026, 8, 1), now=lambda: datetime(2026, 8, 1, 3, tzinfo=UTC))
    assert candle_net["requested"] == [] and rep["summary"]["months_considered"] == 0
    f = fd.RawFile(SYM, "candles", datetime(2026, 7, 31, tzinfo=UTC), "BID")
    with pytest.raises(ValueError, match="6 h"):
        fd.Fetcher(tmp_path, today=date(2026, 8, 2),
                   now=lambda: datetime(2026, 8, 1, 5, tzinfo=UTC)).get(f)
    assert fake_get[0] == []


# M2 ---------------------------------------------------------------------
def test_corrupt_cache_files_moved_aside_and_redownloaded(tmp_path, candle_net):
    f1 = fd.RawFile(SYM, "candles", datetime(2026, 7, 15, tzinfo=UTC), "BID")
    f2 = fd.RawFile(SYM, "candles", datetime(2026, 7, 16, tzinfo=UTC), "ASK")
    fd._atomic_write(f1.path(tmp_path), pack_candles(day_rows("BID"))[:30])   # abgeschnitten
    fd._atomic_write(f2.path(tmp_path), b"")                                  # 0 Byte
    rep = run_c(tmp_path, final_retries=1)
    assert rep["exit_code"] == 0 and parquet_names(tmp_path) == ["XAUUSD-1m-2026-07.parquet"]
    assert f1.marker(tmp_path, "corrupt").exists() and f2.marker(tmp_path, "corrupt").exists()
    assert candle_net["requested"].count(f1.url()) == 1 and candle_net["requested"].count(f2.url()) == 1
    assert rep["summary"]["recovered_in_final_retries"] == 2


def test_validation_errors_collected_per_month(tmp_path, candle_net, monkeypatch):
    inner = fd.download_bi5

    def july_x10(url, **kw):
        if "/2026/06/" in url:                                 # Juli: Gold bei 15000 -> Jahresanker
            candle_net["requested"].append(url)
            return pack_candles(day_rows(candle_url_side(url), 15_000_000))
        return inner(url, **kw)

    monkeypatch.setattr(fd, "download_bi5", july_x10)
    rep = run_c(tmp_path, end=date(2026, 8, 1), today=date(2026, 9, 2))
    assert rep["exit_code"] == 2 and rep["status"] == "validation_error"
    assert rep["summary"]["months_validation_error"] == ["2026-07"]
    assert "Jahresanker" in rep["validation_errors"][0]["error"]
    assert parquet_names(tmp_path) == ["XAUUSD-1m-2026-08.parquet"]    # August trotzdem


# M3 / N3 ----------------------------------------------------------------
def test_temp_names_hidden_and_orphans_removed(tmp_path, candle_net):
    t = fd._tmp_name(Path("x/XAUUSD-1m-2026-07.parquet"))
    assert t.name.startswith(".XAUUSD-1m-2026-07.parquet.") and t.name.endswith(".part")
    assert f".{os.getpid()}." in t.name
    part = fd.store_path(tmp_path, SYM, 2026, 7).parent
    part.mkdir(parents=True)
    orphans = [part / ".XAUUSD-1m-2026-06.parquet.999.1.part", part / "XAUUSD-1m-2026-06.parquet.part",
               fd.reports_dir(tmp_path) / ".XAUUSD-dukascopy-run.json.9.9.part"]
    orphans[2].parent.mkdir(parents=True)
    for p in orphans:
        p.write_bytes(b"x")
    run_c(tmp_path)
    assert not any(p.exists() for p in orphans)
    assert sorted(p.name for p in part.iterdir()) == ["XAUUSD-1m-2026-07.parquet"]


def test_atomic_writes_fsync_file_and_dir(tmp_path, monkeypatch):
    synced = []
    real = os.fsync
    monkeypatch.setattr(fd.os, "fsync", lambda fd_: (synced.append(fd_), real(fd_))[1])
    fd._atomic_write(tmp_path / "a" / "b.bi5", b"x")
    assert len(synced) == 2
    fd.write_parquet(tmp_path / "p" / "x.parquet", make_bars(5), "candles")
    assert len(synced) == 4


# N1 ---------------------------------------------------------------------
def test_retry_after_http_date_without_zone_and_garbage():
    now = datetime(2015, 10, 21, 7, 28, 0, tzinfo=UTC)
    assert fd.parse_retry_after("Wed, 21 Oct 2015 07:28:10 -0000", now=now) == 10.0
    for bad in ("nan", "inf", "Wed, 99 Foo", "", None):
        assert fd.parse_retry_after(bad, now=now) is None


# N2 ---------------------------------------------------------------------
def test_symbol_lock_blocks_second_process(tmp_path, candle_net):
    held = fd.acquire_lock(tmp_path, SYM)
    try:
        rep = run_c(tmp_path)
        assert rep["status"] == "locked" and rep["exit_code"] == 3
        assert candle_net["requested"] == [] and not fd.run_report_path(tmp_path, SYM).exists()
        other = fd.acquire_lock(tmp_path, "EURUSD")                      # anderes Symbol frei
        assert other is not None
        other.close()
    finally:
        held.close()
    assert run_c(tmp_path)["exit_code"] == 0


# ===================================================== 429 (Rate-Limit/IP) ===
GOOD = make_bi5([(0, 2000500, 2000000, 1.0, 1.0)])


def gate_for(clock, **kw):
    kw.setdefault("min_pause", 900)
    return fd.RateLimitGate(kw.pop("state_path", None), wall=clock, sleep=clock.sleep, **kw)


def test_429_does_not_consume_attempt_nor_escalate(fake_get):
    calls, answers = fake_get
    clock = FakeClock()
    th = fd.Throttle(clock, clock.sleep, jitter=0, breaker=1)   # 1 Fehlschlag wuerde ausloesen
    gate = gate_for(clock)
    answers += [FakeResp(429), FakeResp(429, headers={"Retry-After": "60"}), FakeResp(200, GOOD)]
    assert fd.download_bi5("u", attempts=1, throttle=th, gate=gate, sleep=clock.sleep) == GOOD
    assert len(calls) == 3 and gate.hits == 2
    assert sum(clock.slept) == pytest.approx(2 * 900)        # kurzer Retry-After -> trotzdem 15 min
    assert th.consecutive == 0 and th.cooldowns == 0 and not th.tripped.is_set()


def test_429_long_retry_after_respected(fake_get, monkeypatch):
    calls, answers = fake_get
    clock = FakeClock()
    gate = gate_for(clock)
    t_req = []
    answers += [FakeResp(429, headers={"Retry-After": "3600"}), FakeResp(200, GOOD)]
    inner = fd.requests.get
    monkeypatch.setattr(fd.requests, "get",
                        lambda *a, **kw: (t_req.append(clock.t), inner(*a, **kw))[1])
    assert fd.download_bi5("u", attempts=1, gate=gate, sleep=clock.sleep) == GOOD
    assert t_req == [0, 3600] and max(clock.slept) <= 30      # in Scheiben (Stop-Signal)


def test_429_block_shared_by_other_worker(fake_get, monkeypatch):
    """Sperre eines Workers gilt sofort fuer alle: der naechste wartet VOR dem Request."""
    calls, answers = fake_get
    clock = FakeClock()
    gate = gate_for(clock)
    answers += [FakeResp(200, GOOD)]
    t_req = []
    inner = fd.requests.get
    monkeypatch.setattr(fd.requests, "get",
                        lambda *a, **kw: (t_req.append(clock.t), inner(*a, **kw))[1])
    gate.hit(None, "a")                       # Worker A bekommt 429
    assert fd.download_bi5("b", gate=gate, sleep=clock.sleep) == GOOD   # Worker B
    assert t_req == [900]


def answer_429_once(calls, clock=None):
    """Jede URL: erst 429, dann gueltige Kerzendatei."""
    seen = set()
    lock = threading.Lock()

    def get(url, headers=None, timeout=None):
        with lock:
            calls.append((url, clock.t if clock else None))
            first = url not in seen
            seen.add(url)
        if first:
            return FakeResp(429)
        return FakeResp(200, pack_candles(day_rows(candle_url_side(url))))
    return get


def test_many_429_never_trip_breaker_or_escalate(tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr(fd.requests, "get", answer_429_once(calls))
    clock = FakeClock()
    rep = run_c(tmp_path, clock, attempts=1, max_rate_limit_wait=1e9, final_retries=0)
    assert rep["exit_code"] == 0 and rep["gaps"] == [] and rep["pending"] == []
    s = rep["summary"]
    assert s["http_status_counts"] == {"200": 62, "429": 62}  # 62 x 429 > Schutzschalter (20)
    assert not s["circuit_breaker"] and s["global_cooldowns"] == 0
    assert s["rate_limit"]["hits_429"] == 62 and not s["rate_limited"]
    assert sum(clock.slept) == pytest.approx(62 * 900)
    assert parquet_names(tmp_path) == ["XAUUSD-1m-2026-07.parquet"]


def test_many_429_with_workers_same_result(tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr(fd.requests, "get", answer_429_once(calls))
    rep = run_c(tmp_path, workers=4, attempts=1, max_rate_limit_wait=1e9, final_retries=0)
    assert rep["exit_code"] == 0 and rep["summary"]["rate_limit"]["hits_429"] == 62
    assert rep["summary"]["global_cooldowns"] == 0 and not rep["summary"]["circuit_breaker"]


def test_429_budget_exhausted_exit_3_pending_and_persisted(tmp_path, monkeypatch, caplog):
    calls = []
    clock = FakeClock()
    monkeypatch.setattr(fd.requests, "get", answer_429_once(calls, clock))
    rep = run_c(tmp_path, clock, max_rate_limit_wait=600)    # 15-min-Pause passt nicht
    assert rep["exit_code"] == 3 and rep["status"] == "rate_limited"
    assert len(calls) == 1                                    # danach KEIN Request mehr
    assert rep["gaps"] == [] and len(rep["pending"]) == 62
    assert {p["reason"] for p in rep["pending"]} == {"rate_limit"}
    assert "XAUUSD 2026-07-01 BID candles" in {p["file"] for p in rep["pending"]}
    s = rep["summary"]
    assert s["rate_limited"] and s["pending_files"] == 62 and s["final_retry_rounds"] == 0
    assert s["rate_limit"]["hits_429"] == 1 and s["rate_limit"]["exhausted"]
    assert "429" in rep["hint"] and parquet_names(tmp_path) == []
    assert json.loads(fd.run_report_path(tmp_path, SYM).read_text())["status"] == "rate_limited"
    state = json.loads(fd.ratelimit_state_path(tmp_path).read_text())
    assert state["blocked_until"] == pytest.approx(900)

    # neuer Lauf (z.B. systemd-Neustart): wartet ZUERST die persistierte Sperre ab
    calls.clear()
    with caplog.at_level("WARNING", logger="dukascopy"):
        rep = run_c(tmp_path, clock, max_rate_limit_wait=1e9)
    assert any("429-Sperre aktiv" in r.getMessage() for r in caplog.records)
    assert calls[0][1] >= 900                                 # erster Request erst nach Ablauf
    assert rep["exit_code"] == 0


def test_429_persisted_block_seen_by_other_process(tmp_path, fake_get, monkeypatch):
    """Parallele Prozesse (anderes Symbol, Cross-Check) teilen die Sperre ueber die Datei."""
    calls, answers = fake_get
    clock = FakeClock()
    other = gate_for(clock, state_path=fd.ratelimit_state_path(tmp_path))
    fe = fd.Fetcher(tmp_path, today=date(2024, 3, 7), pause=0, sleep=clock.sleep, wall=clock)
    other.hit(1200, "anderer Prozess")        # nach Start des Fetchers
    answers.append(FakeResp(200, GOOD))
    t_req = []
    inner = fd.requests.get
    monkeypatch.setattr(fd.requests, "get",
                        lambda *a, **kw: (t_req.append(clock.t), inner(*a, **kw))[1])
    o = fe.get(fd.RawFile(SYM, "ticks", datetime(2024, 3, 6, 10, tzinfo=UTC)))
    assert o.status == "ok" and t_req == [1200]


def test_cli_rate_limit_options(monkeypatch, capsys):
    monkeypatch.setattr(fd, "install_stop_handler", lambda stop: None)
    with pytest.raises(SystemExit) as e:
        fd.main(["--symbol", "XAUUSD", "--start", "2024-01", "--rate-limit-pause", "60"])
    assert e.value.code == 2 and "--rate-limit-pause" in capsys.readouterr().err
    with pytest.raises(SystemExit):
        fd.main(["--symbol", "XAUUSD", "--start", "2024-01", "--candles-only"])
    seen = {}
    monkeypatch.setattr(fd, "verify_day", lambda *a, **kw: seen.update(kw) or {"exit_code": 0})
    assert fd.main(["--symbol", "XAUUSD", "--verify-day", "2024-03-06", "--candles-only",
                    "--rate-limit-pause", "1800", "--max-rate-limit-wait", "3600"]) == 0
    assert seen["candles_only"] and seen["rate_limit_pause"] == 1800
    assert seen["max_rate_limit_wait"] == 3600
    with pytest.raises(ValueError):
        fd.Fetcher(Path("/nonexistent"), rate_limit_pause=60)


def test_429_not_in_escalating_status():
    assert 429 not in fd.RETRY_STATUS and {403, 408, 425} <= fd.RETRY_STATUS


# ========================================= verify-day --candles-only ===
def full_day(side: str, base: int = 2_150_000, minutes: int = 1380) -> bytes:
    """Werktag mit `minutes` aktiven Minuten ab 00:00, Spread 0.3 (ASK = BID + 300)."""
    b = base + (300 if side == "ASK" else 0)
    rows = []
    for m in range(minutes):
        o, c = b + (m % 7) * 10, b + ((m + 3) % 7) * 10
        rows.append((m * 60, o, max(o, c) + 20, min(o, c) - 20, c, 1.0))
    return pack_candles(rows)


def test_verify_candles_only_two_requests_and_sample(tmp_path, fake_get, caplog):
    calls, answers = fake_get
    answers += [FakeResp(200, full_day("BID")), FakeResp(200, full_day("ASK"))]
    with caplog.at_level("WARNING", logger="dukascopy"):
        rep = fd.verify_day(SYM, VDAY, data_root=tmp_path, today=date(2024, 3, 7), pause=0,
                            candles_only=True)
    assert len(calls) == 2 and rep["requests"] == 2
    assert [c["url"].rsplit("/", 1)[1] for c in calls] == ["BID_candles_min_1.bi5",
                                                           "ASK_candles_min_1.bi5"]
    assert not any("26 Requests" in r.getMessage() for r in caplog.records)
    assert rep["ok"] and rep["exit_code"] == 0 and rep["status"] == "ok"
    assert rep["variant"] == "candles_only" and rep["field_order"] == {"BID": "OCLH", "ASK": "OCLH"}
    pl = rep["plausibility"]
    assert pl["minutes"] == 1380 and pl["crossed"] == 0
    assert pl["spread_median"] == pytest.approx(0.3) and rep["plausibility_problems"] == []
    assert "minutes_ticks" not in rep                       # kein Tickvergleich
    # Stichprobe: ausserhalb des Monatsstores, Store-Schema
    sp = fd.sample_path(tmp_path, SYM, VDAY)
    assert sp == (tmp_path / "metals" / "samples" / "source=dukascopy" / "symbol=XAUUSD"
                  / "XAUUSD-1m-2024-03-06.parquet")
    assert rep["sample_path"] == str(sp)
    assert not list((tmp_path / "metals" / "source=dukascopy").rglob("*.parquet"))
    df = pd.read_parquet(sp)
    assert list(df.columns) == fd.COLUMNS and len(df) == 1380
    assert df["ts"].dtype == "datetime64[ms, UTC]" and df["tick_count"].dtype == "int64"
    assert df["source"].eq("dukascopy").all()
    assert df["ask_close"].iloc[0] - df["bid_close"].iloc[0] == pytest.approx(0.3)
    saved = json.loads(fd.verify_report_path(tmp_path, SYM, VDAY).read_text())
    assert saved["sample_path"] == str(sp) and saved["rate_limit"]["hits_429"] == 0
    # erneut: alles aus dem Cache, 0 Requests
    rep = fd.verify_day(SYM, VDAY, data_root=tmp_path, today=date(2024, 3, 7), pause=0,
                        candles_only=True)
    assert len(calls) == 2 and rep["requests"] == 0 and rep["exit_code"] == 0


def test_verify_tick_variant_warns_26_requests(tmp_path, fake_get, caplog):
    verify_files(tmp_path)
    with caplog.at_level("WARNING", logger="dukascopy"):
        fd.verify_day(SYM, VDAY, data_root=tmp_path, today=date(2024, 3, 7), pause=0)
    assert any("26 Requests" in r.getMessage() for r in caplog.records)


def test_verify_candles_only_implausible_writes_no_sample(tmp_path, candle_net):
    run_c(tmp_path)
    day = date(2026, 7, 15)
    rep = fd.verify_day(SYM, day, data_root=tmp_path, today=date(2026, 8, 20), pause=0,
                        candles_only=True)
    # nur 2 aktive Minuten an einem Werktag -> unplausibel -> KEINE Stichprobe
    # (der Cross-Check liest Stichproben ohne Report)
    assert rep["exit_code"] == 4 and rep["status"] == "implausible"
    assert "Minuten an einem Werktag" in rep["plausibility_problems"][0]
    assert rep["sample_path"] is None and not fd.sample_path(tmp_path, SYM, day).exists()


def test_verify_candles_only_sample_has_store_schema(tmp_path, fake_get):
    import pyarrow.parquet as pq
    _, answers = fake_get
    answers += [FakeResp(200, full_day("BID")), FakeResp(200, full_day("ASK"))]
    rep = fd.verify_day(SYM, VDAY, data_root=tmp_path, today=date(2024, 3, 7), pause=0,
                        candles_only=True)
    assert rep["exit_code"] == 0 and rep["ok"]
    sp = fd.sample_path(tmp_path, SYM, VDAY)
    assert rep["sample_path"] == str(sp)
    ref = tmp_path / "ref.parquet"
    fd.write_parquet(ref, make_bars(3), "candles")              # wie ein Monats-Store
    assert pq.read_schema(sp).remove_metadata() == pq.read_schema(ref).remove_metadata()
    assert pq.read_schema(sp).metadata[b"dukascopy_mode"] == b"candles"
    got = pd.read_parquet(sp)
    assert len(got) > 600 and (got["source"] == "dukascopy").all()


def test_verify_candles_only_implausible_exit_4_no_sample(tmp_path, fake_get, monkeypatch):
    _, answers = fake_get
    sp = fd.sample_path(tmp_path, SYM, VDAY)
    fd.write_parquet(sp, make_bars(3), "candles")             # veraltete Stichprobe
    monkeypatch.setitem(fd.POINT_DIVISOR, SYM, 100)          # Faktor 10 -> ausserhalb PLAUSIBLE
    answers += [FakeResp(200, full_day("BID")), FakeResp(200, full_day("ASK"))]
    rep = fd.verify_day(SYM, VDAY, data_root=tmp_path, today=date(2024, 3, 7), pause=0,
                        candles_only=True)
    assert rep["exit_code"] == 4 and not rep["ok"]
    assert "Plausibilitaetsbereich" in rep["plausibility_problems"][0]
    assert rep["sample_path"] is None and not sp.exists()


def test_verify_candles_only_crossed_and_wide_spread(tmp_path, fake_get):
    _, answers = fake_get
    # ASK unter BID -> gekreuzt
    answers += [FakeResp(200, full_day("BID")), FakeResp(200, full_day("BID", 2_149_000))]
    rep = fd.verify_day(SYM, VDAY, data_root=tmp_path, today=date(2024, 3, 7), pause=0,
                        candles_only=True)
    assert rep["exit_code"] == 4 and "ask_close < bid_close" in rep["plausibility_problems"][0]
    d2 = date(2024, 3, 5)
    answers += [FakeResp(200, full_day("BID")), FakeResp(200, full_day("BID", 2_200_000))]
    rep = fd.verify_day(SYM, d2, data_root=tmp_path, today=date(2024, 3, 7), pause=0,
                        candles_only=True)                     # Spread 50 = 2.3 %
    assert rep["exit_code"] == 4 and "Median-Spread" in rep["plausibility_problems"][0]


def test_verify_candles_only_weekend_no_data_exit_4(tmp_path, fake_get):
    _, answers = fake_get
    answers += [FakeResp(404), FakeResp(404)]
    rep = fd.verify_day(SYM, date(2024, 3, 9), data_root=tmp_path, today=date(2024, 3, 11),
                        pause=0, candles_only=True)           # Samstag
    assert rep["exit_code"] == 4 and rep["status"] == "no_data" and "Werktag" in rep["note"]


def test_verify_candles_only_rate_limited_exit_3(tmp_path, monkeypatch):
    calls = []
    clock = FakeClock()
    monkeypatch.setattr(fd.requests, "get", answer_429_once(calls, clock))
    rep = fd.verify_day(SYM, VDAY, data_root=tmp_path, today=date(2024, 3, 7), pause=0,
                        candles_only=True, sleep=clock.sleep, wall=clock, max_rate_limit_wait=0)
    assert rep["exit_code"] == 3 and rep["status"] == "rate_limited" and len(calls) == 1
    assert [p["reason"] for p in rep["pending"]] == ["rate_limit", "rate_limit"]
    assert rep["rate_limit"]["hits_429"] == 1 and rep["sample_path"] is None
    # neuer Aufruf mit Budget: wartet erst die persistierte Sperre ab; BID 200, ASK 429
    # (kein Versuch verbraucht, attempts=1) -> Pause -> ASK 200. Tag hat nur 3 Minuten -> 4
    calls.clear()
    rep = fd.verify_day(SYM, VDAY, data_root=tmp_path, today=date(2024, 3, 7), pause=0,
                        candles_only=True, sleep=clock.sleep, wall=clock, attempts=1)
    assert [u.rsplit("/", 1)[1][:3] for u, _ in calls] == ["BID", "ASK", "ASK"]
    assert calls[0][1] >= 900 and calls[2][1] - calls[1][1] >= 900
    assert rep["exit_code"] == 4 and rep["status"] == "implausible"
    assert rep["rate_limit"]["hits_429"] == 1


def test_verify_candles_only_lock_held_exit_3(tmp_path, fake_get):
    held = fd.acquire_lock(tmp_path, SYM)
    try:
        rep = fd.verify_day(SYM, VDAY, data_root=tmp_path, today=date(2024, 3, 7),
                            candles_only=True)
        assert rep["exit_code"] == 3 and rep["status"] == "locked" and fake_get[0] == []
    finally:
        held.close()
