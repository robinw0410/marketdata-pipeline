#!/usr/bin/env python3
"""
Historische Dukascopy-Daten (XAUUSD, XAGUSD, EURUSD) als 1-Minuten-Bars mit
Bid- UND Ask-OHLC laden.

- Modus "candles" (Default): je Tag und Seite eine Datei mit 1-Minuten-
  Kerzen (BID + ASK = 2 Requests je Tag statt 24 Tick-Stunden, ~12x weniger)
- Modus "ticks": stuendliche Tickdateien, selbst zu 1m-Bars verdichtet
- cached Rohdateien (Neubau/Fortsetzen ohne erneuten Download)
- schreibt nur ABGESCHLOSSENE UTC-Monate und nur VOLLSTAENDIGE Monate (keine
  ungeloesten Download-Fehlschlaege); der laufende Monat wird weder geladen
  noch geschrieben (analog fetch_klines)
- fehlertolerant: gescheiterte Dateien werden gesammelt, der Lauf geht
  weiter, am Ende gesammelte Wiederholungsrunden mit langem Backoff; ein
  Laufreport mit Lueckenliste wird IMMER geschrieben
- validiert hart, bevor irgendetwas geschrieben wird; weiche Auffaelligkeiten
  (Spread-Ausreisser, Preisspruenge, Minuten ohne Ticks) nur als Report
- schreibt hive-partitioniertes Parquet (zstd), alle Zeiten UTC

Quelle / Format (Dukascopy datafeed, nicht offiziell dokumentiert):
  Ticks   {BASE}/{SYM}/{YYYY}/{MM0:02d}/{DD:02d}/{HH:02d}h_ticks.bi5
          Records a 20 Byte big-endian ('>3I2f'): uint32 ms seit Stundenbeginn,
          uint32 ask, uint32 bid, float32 ask_volume, float32 bid_volume
  Kerzen  {BASE}/{SYM}/{YYYY}/{MM0:02d}/{DD:02d}/{BID|ASK}_candles_min_1.bi5
          Records a 24 Byte big-endian: uint32 Sekunden seit Tagesbeginn
          (UTC), 4x uint32 Preise, float32 volume
  MONAT IN DER URL 0-BASIERT (Januar = 00). LZMA-komprimiert; 0 Bytes =
  keine Daten; 404 = fehlt. Preis = int / POINT_DIVISOR[symbol].

Belege Kerzenformat (nur gelesen, nichts ausgefuehrt; Details DATA.md):
  npm dukascopy-node 1.46.4 (letzte Version mit datafeed/bi5; ab 1.49 nutzt
  das Paket eine andere JSON-API mit AWS-WAF-Behandlung), dist/index.js:
    Z. 16459-16468  URL_ROOT datafeed, ".../{MM}/{DD}/{SIDE}_candles_min_1.bi5"
    Z. 16348-16351  Monat aus getUTCMonth() -> 0-basiert
    Z. 16678-16679  Recordgroesse 24 (Kerzen) bzw. 20 (Ticks)
    Z. 16690-16695  5x readInt32BE + readFloatBE(20)
    Z. 16743-16745  [sec, open, close, low, high, volume]; ts = sec*1e3 + Tagesbeginn
    Z. 16899, 1716  ignoreFlats (Default true): verwirft Kerzen mit volume == 0
    Z. 16486        laufender Tag -> Rueckfall auf Stunden-Ticks (Tagesdatei
                    gilt erst nach Tagesende als verfuegbar)
    Z. 12935/12926/6149  decimalFactor XAUUSD 1e3, XAGUSD 1e3, EURUSD 1e5
  UNSICHER: Feldreihenfolge (dukascopy-node: open, close, low, high; eine
  StackOverflow-Quelle: open, high, low, close). Deshalb wird die Reihenfolge
  JE DATEI aus den Daten bestimmt (detect_field_order): nur genau eine der
  beiden Reihenfolgen darf zu allen nicht flachen Kerzen passen, sonst gilt
  die Datei als Fehlschlag ("Feldreihenfolge unklar") statt zu raten.
  Pruefen gegen Ticks: --verify-day YYYY-MM-DD.

Fuellkerzen (Annahme): Minuten ohne Ticks sind flache Kerzen mit volume 0.
Eine Minute wird verworfen, wenn BEIDE Seiten volume == 0 UND flache Preise
(O=H=L=C) haben. tick_count ist im Kerzenmodus KEIN echter Tick-Zaehler:
1 fuer jede behaltene Minute (erfuellt tick_count >= 1); der Modus steht im
Monats- und Laufreport und in den Parquet-Metadaten (dukascopy_mode).

Ablage (DATA_ROOT = $MARKETDATA_DATA_ROOT, sonst <repo>/data):
    raw/dukascopy/{SYM}/{YYYY}/{MM}/{DD}/{HH}h_ticks.bi5
    raw/dukascopy/{SYM}/{YYYY}/{MM}/{DD}/{BID|ASK}_candles_min_1.bi5
        Rohdateien, Monat hier 1-BASIERT. 404 -> leere Markerdatei
        <name>.404 daneben (Marker loeschen = erneut anfragen). Kerzendatei
        mit unklarer Feldreihenfolge -> <name>.unclear (nur zur Ansicht).
    metals/source=dukascopy/symbol={SYM}/year={YYYY}/{SYM}-1m-{YYYY}-{MM}.parquet
    metals/reports/{SYM}-1m-{YYYY}-{MM}.json     Validierungsreport je Monat
    metals/reports/{SYM}-dukascopy-run.json      Laufreport inkl. Lueckenliste
                                                 (wird beim Start gelesen)
    metals/reports/{SYM}-verify-{YYYY-MM-DD}.json  Ergebnis --verify-day

Exit-Codes: 0 vollstaendig, 2 Validierungsfehler, 3 Luecken (Downloads nach
allen Wiederholungen gescheitert oder Lauf per SIGTERM unterbrochen),
4 --verify-day: Abweichung Kerzen vs. Ticks ueber Toleranz.

Lizenz: Dukascopy-Nutzungsbedingungen selbst pruefen. Daten (roh wie
abgeleitet) nicht weitergeben und nicht committen (data/ ist git-ignored).

Usage:
    python fetch_dukascopy.py --symbol XAUUSD --start 2019-01
    python fetch_dukascopy.py --symbol EURUSD --start 2024-01 --end 2024-06 --mode ticks
    python fetch_dukascopy.py --symbol XAUUSD --start 2024-01 --end 2024-01 --rebuild
    python fetch_dukascopy.py --symbol XAUUSD --verify-day 2024-03-06
"""
from __future__ import annotations

import argparse
import json
import logging
import lzma
import os
import random
import signal
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

BASE = "https://datafeed.dukascopy.com/datafeed"
ROOT = Path(__file__).resolve().parents[1]
SOURCE = "dukascopy"
USER_AGENT = "marketdata-pipeline/1.0"

# [Annahme, zu verifizieren] Divisor int -> Preis je Symbol, fuer Ticks und
# Kerzen gleich (dukascopy-node 1.46.4 decimalFactor, s. Docstring).
# Abgesichert durch PLAUSIBLE und ANCHORS: ein falscher Divisor verschiebt
# Preise um Zehnerpotenzen und fuehrt dort zum harten Abbruch.
POINT_DIVISOR = {"XAUUSD": 1000, "XAGUSD": 1000, "EURUSD": 100000}

# Plausibilitaetsbereich je Symbol (alle OHLC-Werte, Bid und Ask). Bewusst
# grob: faengt Divisor-Fehler (Faktor 10+), nicht echte Marktbewegungen.
PLAUSIBLE = {"XAUUSD": (200.0, 20000.0), "XAGUSD": (3.0, 500.0), "EURUSD": (0.5, 2.0)}

# Zweite, engere Absicherung gegen Zehnerpotenz-Fehler: grobe Jahres-
# Durchschnittspreise aus allgemeinem Vorwissen (NICHT aus diesen Daten,
# gerundet, 2025/2026 unsicher). Der Monatsmedian des Mid muss innerhalb
# Faktor ANCHOR_BAND davon liegen.
ANCHOR_BAND = 2.5
ANCHORS = {
    "XAUUSD": {2003: 360, 2004: 410, 2005: 445, 2006: 605, 2007: 695, 2008: 870,
               2009: 970, 2010: 1225, 2011: 1570, 2012: 1670, 2013: 1410, 2014: 1265,
               2015: 1160, 2016: 1250, 2017: 1255, 2018: 1270, 2019: 1390, 2020: 1770,
               2021: 1800, 2022: 1800, 2023: 1940, 2024: 2390, 2025: 3400, 2026: 4200},
    "XAGUSD": {2003: 4.9, 2004: 6.7, 2005: 7.3, 2006: 11.5, 2007: 13.4, 2008: 15.0,
               2009: 14.7, 2010: 20.2, 2011: 35.1, 2012: 31.2, 2013: 23.8, 2014: 19.1,
               2015: 15.7, 2016: 17.1, 2017: 17.0, 2018: 15.7, 2019: 16.2, 2020: 20.5,
               2021: 25.1, 2022: 21.8, 2023: 23.4, 2024: 28.3, 2025: 40.0, 2026: 70.0},
    "EURUSD": {},   # PLAUSIBLE (0.5..2.0) ist fuer EURUSD bereits eng genug
}

RECORD = np.dtype([("ms", ">u4"), ("ask", ">u4"), ("bid", ">u4"),
                   ("ask_volume", ">f4"), ("bid_volume", ">f4")])   # == '>3I2f'
assert RECORD.itemsize == 20
# Kerze: Sekunden-Offset, vier Preisfelder (Reihenfolge je Datei bestimmt), Volumen
CANDLE = np.dtype([("sec", ">u4"), ("p1", ">u4"), ("p2", ">u4"), ("p3", ">u4"),
                   ("p4", ">u4"), ("volume", ">f4")])
assert CANDLE.itemsize == 24
HOUR_MS = 3_600_000
DAY_S = 86_400

# Moegliche Feldreihenfolgen der vier Preisfelder (Offsets 4, 8, 12, 16).
FIELD_ORDERS = {
    "OCLH": ("open", "close", "low", "high"),   # dukascopy-node 1.46.4, Z. 16743
    "OHLC": ("open", "high", "low", "close"),   # StackOverflow-Quelle (Gegenstimme)
}

MODES = ("candles", "ticks")
SIDES = ("BID", "ASK")

# ---- Drosselung / Wiederholung (Defaults, alle per CLI aenderbar) ----------
WORKERS = 1
PAUSE_S = 1.0              # Pause je Worker nach jedem echten Request
ATTEMPTS = 5               # Versuche je Datei und Durchgang
BACKOFF_BASE_S = 2.0       # Wartezeit vor Versuch n+1: base * 2**n (mit Jitter)
MAX_BACKOFF_S = 600.0      # Deckel fuer Backoff und Retry-After
JITTER = 0.5               # Wartezeit * (1 - JITTER * U[0,1))
FINAL_RETRIES = 2          # gesammelte Wiederholungsrunden am Ende
FINAL_WAIT_S = 300.0       # Wartezeit vor jeder Wiederholungsrunde
CONNECT_TIMEOUT_S = 10.0
READ_TIMEOUT_S = 60.0
PROGRESS_EVERY = 50
# Wiederholbare HTTP-Status. 403 ist beim Datafeed vermutlich WAF/Bot-Schutz
# (neuere Pakete sind deshalb auf eine andere API umgestiegen) -> wie 429/503
# behandeln, nicht als "keine Daten" und nicht als sofortigen Abbruch.
RETRY_STATUS = {403, 408, 425, 429}          # zusaetzlich alle 5xx
COOLDOWN_STATUS = {403, 429, 503}            # -> globale Abkuehlung aller Worker
WAF_SHARE = 0.5            # >= 50 % der Requests mit 403 -> deutlicher Hinweis

EXIT_OK, EXIT_VALIDATION, EXIT_GAPS, EXIT_VERIFY = 0, 2, 3, 4

MAX_CROSSED_SHARE = 0.001  # > 0.1 % gekreuzte Bars (ask_close < bid_close) -> Abbruch
SPREAD_OUTLIER_X = 20.0    # weich: Spread > 20x Monatsmedian
JUMP_THRESHOLD = 0.02      # weich: |Mid-Rendite 1m| > 2 %
N_EXAMPLES = 10

BID = ["bid_open", "bid_high", "bid_low", "bid_close"]
ASK = ["ask_open", "ask_high", "ask_low", "ask_close"]
COLUMNS = ["ts", *BID, *ASK, "tick_count", "bid_volume", "ask_volume", "source"]
NUMERIC = [*BID, *ASK, "tick_count", "bid_volume", "ask_volume"]

log = logging.getLogger("dukascopy")
_sleep = time.sleep        # in Tests ersetzbar


def default_data_root() -> Path:
    env = os.environ.get("MARKETDATA_DATA_ROOT")
    return Path(env) if env else ROOT / "data"


class ValidationError(Exception):
    pass


class FieldOrderError(ValidationError):
    """Feldreihenfolge einer Kerzendatei nicht eindeutig -> Datei = Fehlschlag."""


class DownloadError(Exception):
    def __init__(self, msg: str, status: int | None = None, attempts: int = 1):
        super().__init__(msg)
        self.status, self.attempts = status, attempts


class Interrupted(Exception):
    """Stop-Signal (SIGTERM) waehrend eines Downloads."""


# ------------------------------------------------------------------ pfade ---
def url_for(symbol: str, hour: datetime) -> str:
    # Monat in der URL 0-basiert (Januar = 00)
    return (f"{BASE}/{symbol}/{hour.year:04d}/{hour.month - 1:02d}/"
            f"{hour.day:02d}/{hour.hour:02d}h_ticks.bi5")


def candle_url_for(symbol: str, day: datetime | date, side: str) -> str:
    return (f"{BASE}/{symbol}/{day.year:04d}/{day.month - 1:02d}/"
            f"{day.day:02d}/{side}_candles_min_1.bi5")


def _raw_dir(data_root: Path, symbol: str, t: datetime | date) -> Path:
    # Monat im Cache 1-basiert (Januar = 01), damit Verzeichnisse lesbar sind
    return (data_root / "raw" / "dukascopy" / symbol / f"{t.year:04d}" /
            f"{t.month:02d}" / f"{t.day:02d}")


def raw_path(data_root: Path, symbol: str, hour: datetime) -> Path:
    return _raw_dir(data_root, symbol, hour) / f"{hour.hour:02d}h_ticks.bi5"


def candle_raw_path(data_root: Path, symbol: str, day: datetime | date, side: str) -> Path:
    return _raw_dir(data_root, symbol, day) / f"{side}_candles_min_1.bi5"


def store_path(data_root: Path, symbol: str, y: int, m: int) -> Path:
    return (data_root / "metals" / f"source={SOURCE}" / f"symbol={symbol}" /
            f"year={y}" / f"{symbol}-1m-{y}-{m:02d}.parquet")


def report_path(data_root: Path, symbol: str, y: int, m: int) -> Path:
    return data_root / "metals" / "reports" / f"{symbol}-1m-{y}-{m:02d}.json"


def run_report_path(data_root: Path, symbol: str) -> Path:
    return data_root / "metals" / "reports" / f"{symbol}-dukascopy-run.json"


def verify_report_path(data_root: Path, symbol: str, day: date) -> Path:
    return data_root / "metals" / "reports" / f"{symbol}-verify-{day:%Y-%m-%d}.json"


@dataclass(frozen=True)
class RawFile:
    """Eine Rohdatei: Tick-Stunde (kind=ticks) oder Kerzen-Tag einer Seite."""
    symbol: str
    kind: str          # "ticks" | "candles"
    t: datetime        # Stundenbeginn bzw. Tagesbeginn, UTC
    side: str = ""     # "BID" | "ASK" (nur candles)

    @property
    def name(self) -> str:
        return (f"{self.t.hour:02d}h_ticks" if self.kind == "ticks"
                else f"{self.side}_candles_min_1")

    @property
    def key(self) -> str:
        what = f"{self.t:%H}h ticks" if self.kind == "ticks" else f"{self.side} candles"
        return f"{self.symbol} {self.t:%Y-%m-%d} {what}"

    @property
    def month(self) -> tuple[int, int]:
        return self.t.year, self.t.month

    def url(self) -> str:
        return (url_for(self.symbol, self.t) if self.kind == "ticks"
                else candle_url_for(self.symbol, self.t, self.side))

    def path(self, data_root: Path) -> Path:
        return (raw_path(data_root, self.symbol, self.t) if self.kind == "ticks"
                else candle_raw_path(data_root, self.symbol, self.t, self.side))

    def marker(self, data_root: Path) -> Path:
        return self.path(data_root).with_name(self.name + ".404")

    def resolved(self, data_root: Path) -> bool:
        """Datei liegt im Cache oder ist als 404 markiert."""
        return self.path(data_root).exists() or self.marker(data_root).exists()

    def to_json(self) -> dict:
        return {"kind": self.kind, "date": f"{self.t:%Y-%m-%d}",
                "hour": self.t.hour if self.kind == "ticks" else None,
                "side": self.side or None}

    @classmethod
    def from_json(cls, symbol: str, d: dict) -> "RawFile":
        y, m, dd = map(int, d["date"].split("-"))
        return cls(symbol, d["kind"], datetime(y, m, dd, d.get("hour") or 0, tzinfo=timezone.utc),
                   d.get("side") or "")


def month_files(symbol: str, mode: str, y: int, m: int) -> list[RawFile]:
    if mode == "ticks":
        return [RawFile(symbol, "ticks", h) for h in month_hours(y, m)]
    return [RawFile(symbol, "candles", d, s) for d in month_days(y, m) for s in SIDES]


# ------------------------------------------------------- drosselung/backoff ---
def parse_retry_after(value: str | None, now: datetime | None = None) -> float | None:
    """Retry-After: Sekunden oder HTTP-Datum -> Sekunden (>= 0); None wenn unlesbar."""
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        pass
    try:
        dt = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    now = now or datetime.now(timezone.utc)
    return max(0.0, (dt - now).total_seconds())


def backoff_delay(n: int, base: float = BACKOFF_BASE_S, cap: float = MAX_BACKOFF_S,
                  jitter: float = JITTER, rng=None, retry_after: float | None = None) -> float:
    """
    Wartezeit nach dem n-ten Fehlversuch (n = 0, 1, ...): base * 2**n, mit
    Jitter nach unten (bis zu jitter-Anteil), gedeckelt auf cap. Ein
    Retry-After des Servers wird respektiert (Minimum), ebenfalls bis cap.
    """
    d = min(cap, base * 2 ** n)
    if jitter:
        d *= 1.0 - jitter * (rng or random).random()
    if retry_after is not None:
        d = max(d, min(retry_after, cap))
    return d


class Throttle:
    """
    Gemeinsamer Zeitpunkt "nicht vor t" fuer ALLE Worker. Nach einem 503
    (bzw. 429/403) wird er nach hinten geschoben, jeder Worker wartet vor
    seinem naechsten Request darauf -> parallele Worker fluten den Server
    nicht weiter.
    """

    def __init__(self, clock=time.monotonic, sleep=None):
        self.clock, self._sleep = clock, sleep
        self.lock = threading.Lock()
        self.not_before = float("-inf")
        self.cooldowns = 0

    def sleep(self, s: float) -> None:
        (self._sleep or _sleep)(s)

    def cooldown(self, seconds: float, reason: str = "") -> None:
        with self.lock:
            nb = self.clock() + seconds
            if nb > self.not_before:
                self.not_before = nb
                self.cooldowns += 1
                log.warning("globale Abkuehlung %.0fs fuer alle Worker (%s)", seconds, reason)

    def wait(self) -> None:
        slept_for = None
        while True:
            with self.lock:
                target = self.not_before
            dt = target - self.clock()
            # target == slept_for: schon bis dahin geschlafen (auch bei
            # injiziertem Schlaf ohne echte Uhr keine Endlosschleife)
            if dt <= 0 or target == slept_for:
                return
            self.sleep(dt)
            slept_for = target


# --------------------------------------------------------------- download ---
def download_bi5(url: str, *, attempts: int = ATTEMPTS, backoff_base: float = BACKOFF_BASE_S,
                 max_backoff: float = MAX_BACKOFF_S, jitter: float = JITTER,
                 timeout: tuple[float, float] = (CONNECT_TIMEOUT_S, READ_TIMEOUT_S),
                 sleep=None, throttle: Throttle | None = None, rng=None,
                 stop: threading.Event | None = None, on_response=None) -> bytes | None:
    """
    Laedt eine .bi5-Datei. None bei 404, b"" bei leerer Datei (keine Daten).
    Bis zu `attempts` Versuche mit exponentiellem Backoff (Jitter, Deckel,
    Retry-After) bei Netzfehlern/Timeouts, 403/408/425/429/5xx und bei nicht
    dekomprimierbarem Inhalt (abgeschnittene Uebertragung). Bei 403/429/503
    globale Abkuehlung ueber `throttle`. Andere HTTP-Fehler: kein Retry.
    Wirft DownloadError (mit .status, .attempts) bzw. Interrupted.
    """
    sleep = sleep if sleep is not None else _sleep
    err, status = None, None
    for n in range(attempts):
        if stop is not None and stop.is_set():
            raise Interrupted(url)
        if throttle is not None:
            throttle.wait()
            if stop is not None and stop.is_set():
                raise Interrupted(url)
        retry_after = None
        try:
            r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=timeout)
        except requests.RequestException as e:
            err, status = f"{type(e).__name__}: {e}", None
            if on_response:
                on_response(None)
        else:
            status = r.status_code
            if on_response:
                on_response(status)
            if status == 404:
                return None
            if status == 200:
                blob = r.content
                if not blob:
                    return b""
                try:
                    lzma.decompress(blob, format=lzma.FORMAT_AUTO)
                    return blob
                except lzma.LZMAError as e:
                    err = f"LZMA: {e}"
            elif status in RETRY_STATUS or status >= 500:
                err = f"HTTP {status}" + (" (WAF?)" if status == 403 else "")
                retry_after = parse_retry_after((getattr(r, "headers", None) or {}).get("Retry-After"))
            else:
                raise DownloadError(f"HTTP {status}: {url}", status=status, attempts=n + 1)
        wait = backoff_delay(n, backoff_base, max_backoff, jitter, rng, retry_after)
        shared = status in COOLDOWN_STATUS and throttle is not None
        if shared:
            # auch nach dem letzten Versuch: die anderen Worker sollen abkuehlen
            throttle.cooldown(wait, err)
        if n + 1 < attempts:
            log.info("retry %d/%d in %.1fs %s (%s)", n + 1, attempts - 1, wait, url, err)
            if shared:
                throttle.wait()
            else:
                sleep(wait)
    raise DownloadError(f"{url}: {attempts} Versuche fehlgeschlagen ({err})",
                        status=status, attempts=attempts)


def _atomic_write(dest: Path, blob: bytes) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.name + ".part")
    tmp.write_bytes(blob)
    os.replace(tmp, dest)


def write_json_atomic(dest: Path, obj: dict) -> None:
    _atomic_write(dest, (json.dumps(obj, indent=2, ensure_ascii=False) + "\n").encode())


@dataclass
class Outcome:
    status: str                 # cached | cached_404 | ok | 404 | failed | skipped
    error: str | None = None
    attempts: int = 0
    http_status: int | None = None


class Fetcher:
    """
    Holt Rohdateien aus dem Cache oder per Download, gedrosselt (Pause je
    Worker, Backoff, globale Abkuehlung). Cache-Treffer sind keine Requests.
    Kerzendateien des laufenden UTC-Tages werden nie angefordert.
    """

    def __init__(self, data_root: Path, *, workers: int = WORKERS, pause: float = PAUSE_S,
                 attempts: int = ATTEMPTS, backoff_base: float = BACKOFF_BASE_S,
                 max_backoff: float = MAX_BACKOFF_S, jitter: float = JITTER,
                 timeout: tuple[float, float] = (CONNECT_TIMEOUT_S, READ_TIMEOUT_S),
                 sleep=None, clock=None, rng=None, stop: threading.Event | None = None,
                 today: date | None = None):
        self.data_root = data_root
        self.workers, self.pause, self.attempts = max(1, workers), pause, attempts
        self.backoff_base, self.max_backoff, self.jitter = backoff_base, max_backoff, jitter
        self.timeout, self.rng = timeout, rng
        self._sleep = sleep
        self.stop = stop or threading.Event()
        self.today = today or utc_today()
        self.throttle = Throttle(clock or time.monotonic, sleep)
        self.lock = threading.Lock()
        self.requests = 0
        self.status_counts: dict[str, int] = {}

    def sleep(self, s: float) -> None:
        (self._sleep or _sleep)(s)

    def _count(self, status: int | None) -> None:
        with self.lock:
            self.requests += 1
            k = str(status) if status is not None else "netzfehler"
            self.status_counts[k] = self.status_counts.get(k, 0) + 1

    def get(self, f: RawFile) -> Outcome:
        if f.kind == "candles" and f.t.date() >= self.today:
            # Tagesdatei erst nach Tagesende vollstaendig -> nie anfordern/cachen
            raise ValueError(f"{f.key}: Kerzendatei des laufenden/zukuenftigen UTC-Tages")
        dest, marker = f.path(self.data_root), f.marker(self.data_root)
        if dest.exists():
            return Outcome("cached")
        if marker.exists():
            return Outcome("cached_404")
        if self.stop.is_set():
            return Outcome("skipped")

        t0 = time.monotonic()
        try:
            blob = download_bi5(
                f.url(), attempts=self.attempts, backoff_base=self.backoff_base,
                max_backoff=self.max_backoff, jitter=self.jitter, timeout=self.timeout,
                sleep=self.sleep, throttle=self.throttle, rng=self.rng, stop=self.stop,
                on_response=self._count)
        except Interrupted:
            return Outcome("skipped")
        except DownloadError as e:
            log.warning("FEHLSCHLAG %s: %s (%d Versuche, spaeter erneut)", f.key, e, e.attempts)
            out = Outcome("failed", str(e), e.attempts, e.status)
        else:
            out = self._store(f, blob, time.monotonic() - t0)
        if self.pause and not self.stop.is_set():
            self.sleep(self.pause)
        return out

    def _store(self, f: RawFile, blob: bytes | None, dt: float) -> Outcome:
        if blob is None:
            _atomic_write(f.marker(self.data_root), b"")
            log.info("404 %s (keine Daten, %.1fs)", f.key, dt)
            return Outcome("404", attempts=1, http_status=404)
        if f.kind == "candles" and blob:
            try:
                order = detect_field_order(candle_records(blob))
            except FieldOrderError as e:
                # nicht cachen (sonst gaelte die Datei als erledigt), nur zur Ansicht ablegen
                _atomic_write(f.path(self.data_root).with_name(f.name + ".unclear"), blob)
                log.warning("FEHLSCHLAG %s: %s", f.key, e)
                return Outcome("failed", str(e), 1, 200)
            except ValidationError:
                order = "?"            # Formatfehler -> harter Abbruch beim Monatsbau
            log.info("ok  %s (%d B, %.1fs, Reihenfolge %s)", f.key, len(blob), dt, order)
        else:
            log.info("ok  %s (%d B, %.1fs)", f.key, len(blob), dt)
        _atomic_write(f.path(self.data_root), blob)
        return Outcome("ok", attempts=1, http_status=200)

    def fetch(self, files: list[RawFile], progress: "Progress | None" = None) -> dict[RawFile, Outcome]:
        def one(f: RawFile) -> Outcome:
            o = self.get(f)
            if progress is not None and o.status not in ("cached", "cached_404", "skipped"):
                progress.tick()
            return o

        if self.workers <= 1 or len(files) <= 1:
            return {f: one(f) for f in files}
        with ThreadPoolExecutor(max_workers=self.workers) as ex:
            return dict(zip(files, ex.map(one, files)))


class Progress:
    """Fortschritt (erledigt/gesamt, Rate, ETA) alle `every` Dateien."""

    def __init__(self, total: int, every: int = PROGRESS_EVERY, clock=time.monotonic,
                 label: str = ""):
        self.total, self.every, self.clock, self.label = total, max(1, every), clock, label
        self.done = 0
        self.t0 = clock()
        self.lock = threading.Lock()

    def tick(self) -> None:
        with self.lock:
            self.done += 1
            if self.done % self.every and self.done != self.total:
                return
            el = max(1e-9, self.clock() - self.t0)
            rate = self.done / el
            eta = (self.total - self.done) / rate if rate > 0 else float("inf")
            log.info("FORTSCHRITT %s%d/%d Dateien, %.1f/min, ETA %s", self.label, self.done,
                     self.total, rate * 60, _fmt_dur(eta))


def _fmt_dur(s: float) -> str:
    if s == float("inf"):
        return "?"
    s = int(s)
    return f"{s // 3600}h{s % 3600 // 60:02d}m" if s >= 3600 else f"{s // 60}m{s % 60:02d}s"


def get_hour(data_root: Path, symbol: str, hour: datetime,
             pause: float = PAUSE_S) -> tuple[str, bytes | None]:
    """
    Rohdatei einer Tick-Stunde aus dem Cache oder per Download (Kompatibilitaet).
    Rueckgabe (status, blob): status in {cached, cached_404, ok, 404}.
    """
    f = RawFile(symbol, "ticks", hour)
    o = Fetcher(data_root, pause=pause, today=date.max).get(f)
    if o.status == "failed":
        raise DownloadError(o.error or f.key, o.http_status, o.attempts)
    if o.status in ("404", "cached_404"):
        return o.status, None
    return o.status, f.path(data_root).read_bytes()


# ---------------------------------------------------------------- parsing ---
def decode_bi5(blob: bytes | None, hour_start_ms: int, divisor: int) -> pd.DataFrame:
    """Rohdatei einer Stunde -> Ticks (ts UTC ms, bid, ask, Volumina)."""
    raw = lzma.decompress(blob, format=lzma.FORMAT_AUTO) if blob else b""
    if len(raw) % RECORD.itemsize:
        raise ValidationError(f"bi5-Laenge {len(raw)} kein Vielfaches von {RECORD.itemsize}")
    rec = np.frombuffer(raw, dtype=RECORD)
    if len(rec) and int(rec["ms"].max()) >= HOUR_MS:
        raise ValidationError(f"ms-Offset {int(rec['ms'].max())} >= 1h (Stunde {hour_start_ms})")

    ts = hour_start_ms + rec["ms"].astype(np.int64)
    return pd.DataFrame({
        "ts": pd.to_datetime(ts, unit="ms", utc=True).as_unit("ms"),
        "bid": rec["bid"].astype(np.float64) / divisor,
        "ask": rec["ask"].astype(np.float64) / divisor,
        "bid_volume": rec["bid_volume"].astype(np.float64),
        "ask_volume": rec["ask_volume"].astype(np.float64),
    })


def candle_records(blob: bytes | None) -> np.ndarray:
    """Kerzen-bi5 -> Records (strukturiertes Array), mit Formatpruefung."""
    raw = lzma.decompress(blob, format=lzma.FORMAT_AUTO) if blob else b""
    if len(raw) % CANDLE.itemsize:
        raise ValidationError(f"Kerzen-bi5-Laenge {len(raw)} kein Vielfaches von {CANDLE.itemsize}")
    rec = np.frombuffer(raw, dtype=CANDLE)
    if len(rec):
        sec = rec["sec"].astype(np.int64)
        if int(sec.max()) >= DAY_S:
            # z.B. Millisekunden statt Sekunden -> hart
            raise ValidationError(f"Sekunden-Offset {int(sec.max())} >= 1 Tag")
        if (sec % 60).any():
            raise ValidationError("Sekunden-Offset nicht auf Minutengrenze")
        if len(sec) > 1 and (np.diff(sec) <= 0).any():
            raise ValidationError("Sekunden-Offsets nicht streng steigend")
    return rec


def detect_field_order(rec: np.ndarray) -> str:
    """
    Feldreihenfolge der vier Preisfelder aus den Daten: "OCLH" oder "OHLC".
    Genau eine Reihenfolge muss in ALLEN nicht flachen Kerzen high >=
    max(open, close) und low <= min(open, close) erfuellen. "flat", wenn
    alle Kerzen flach sind (O=H=L=C, dann ist die Reihenfolge egal: alle
    Felder gleich). Sonst FieldOrderError ("Feldreihenfolge unklar").
    """
    if not len(rec):
        return "flat"
    v = np.stack([rec[k].astype(np.int64) for k in ("p1", "p2", "p3", "p4")], axis=1)
    nonflat = ~((v[:, 0] == v[:, 1]) & (v[:, 1] == v[:, 2]) & (v[:, 2] == v[:, 3]))
    if not nonflat.any():
        return "flat"
    w = v[nonflat]
    fits = []
    for name, fields in FIELD_ORDERS.items():
        o, h, l, c = (w[:, fields.index(k)] for k in ("open", "high", "low", "close"))
        if ((h >= np.maximum(o, c)) & (l <= np.minimum(o, c))).all():
            fits.append(name)
    if len(fits) == 1:
        return fits[0]
    if not fits:
        raise FieldOrderError("Feldreihenfolge unklar: weder OCLH noch OHLC passt zu allen "
                              f"{int(nonflat.sum())} nicht flachen Kerzen")
    raise FieldOrderError("Feldreihenfolge unklar: OCLH und OHLC gleich moeglich "
                          f"({int(nonflat.sum())} nicht flache Kerzen)")


def decode_candles(blob: bytes | None, day_start_ms: int, divisor: int,
                   order: str | None = None) -> tuple[pd.DataFrame, str]:
    """Kerzendatei eines Tages (eine Seite) -> (ts, open, high, low, close, volume), Reihenfolge."""
    rec = candle_records(blob)
    order = order or detect_field_order(rec)
    fields = FIELD_ORDERS.get(order, FIELD_ORDERS["OCLH"])     # "flat": alle Felder gleich
    cols = {name: rec[f"p{i + 1}"].astype(np.float64) / divisor for i, name in enumerate(fields)}
    ts = day_start_ms + rec["sec"].astype(np.int64) * 1000
    df = pd.DataFrame({
        "ts": pd.to_datetime(ts, unit="ms", utc=True).as_unit("ms"),
        "open": cols["open"], "high": cols["high"], "low": cols["low"], "close": cols["close"],
        "volume": rec["volume"].astype(np.float64),
    })
    return df, order


def merge_candles(bid: pd.DataFrame, ask: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """
    BID- und ASK-Kerzen eines Tages -> Store-Schema. Minuten, in denen BEIDE
    Seiten volume == 0 und flache Preise haben, sind Fuellkerzen und werden
    verworfen. Minuten nur auf einer Seite werden verworfen und gezaehlt.
    tick_count = 1 (kein echter Tick-Zaehler im Kerzenmodus).
    """
    m = bid.merge(ask, on="ts", how="outer", suffixes=("_bid", "_ask"), indicator=True)
    both = m[m["_merge"] == "both"]
    one_side = int((m["_merge"] != "both").sum())

    def flat(s: str) -> pd.Series:
        return ((both[f"open_{s}"] == both[f"high_{s}"]) & (both[f"high_{s}"] == both[f"low_{s}"])
                & (both[f"low_{s}"] == both[f"close_{s}"]))

    zero = (both["volume_bid"] == 0) & (both["volume_ask"] == 0)
    fill = zero & flat("bid") & flat("ask")
    keep = both[~fill].sort_values("ts")
    bars = pd.DataFrame({"ts": keep["ts"].dt.as_unit("ms")})
    for s, side in (("bid", "bid"), ("ask", "ask")):
        for k in ("open", "high", "low", "close"):
            bars[f"{side}_{k}"] = keep[f"{k}_{s}"].to_numpy()
    bars["tick_count"] = np.ones(len(bars), dtype="int64")
    bars["bid_volume"] = keep["volume_bid"].to_numpy()
    bars["ask_volume"] = keep["volume_ask"].to_numpy()
    bars["source"] = SOURCE
    stats = {"fill_minutes_dropped": int(fill.sum()), "one_side_minutes_dropped": one_side,
             "zero_volume_nonflat_minutes": int((zero & ~fill).sum())}
    return bars[COLUMNS].reset_index(drop=True), stats


# ------------------------------------------------------------- aggregation ---
def aggregate_1m(ticks: pd.DataFrame) -> pd.DataFrame:
    """
    Ticks -> 1m-Bars, Intervall [t, t+1min), Label links. Nur Minuten mit
    mindestens einem Tick; es wird nichts aufgefuellt.
    """
    if ticks.empty:
        return pd.DataFrame(columns=COLUMNS)

    # stabil sortieren: gleiche Zeitstempel behalten ihre Dateireihenfolge
    t = ticks.sort_values("ts", kind="mergesort")
    g = t.groupby(t["ts"].dt.floor("1min"), sort=True)
    bars = pd.DataFrame({
        "bid_open": g["bid"].first(), "bid_high": g["bid"].max(),
        "bid_low": g["bid"].min(), "bid_close": g["bid"].last(),
        "ask_open": g["ask"].first(), "ask_high": g["ask"].max(),
        "ask_low": g["ask"].min(), "ask_close": g["ask"].last(),
        "tick_count": g.size().astype("int64"),
        "bid_volume": g["bid_volume"].sum(), "ask_volume": g["ask_volume"].sum(),
    })
    bars.index.name = "ts"
    bars = bars.reset_index()
    bars["ts"] = bars["ts"].dt.as_unit("ms")
    bars["source"] = SOURCE
    return bars[COLUMNS]


# -------------------------------------------------------------- validation ---
def _ohlc_bad(df: pd.DataFrame, side: str) -> int:
    o, h, l, c = (df[f"{side}_{k}"] for k in ("open", "high", "low", "close"))
    return int(((l > np.minimum(o, c)) | (h < np.maximum(o, c)) | (h < l)).sum())


def continuity(df: pd.DataFrame, divisor: int) -> dict:
    """
    Plausibilitaet (nur Report): |open[t+1] - close[t]| in Punkten fuer
    direkt aufeinanderfolgende Minuten, je Seite. Bei falscher Feld-
    reihenfolge waere der Median deutlich groesser.
    """
    out = {}
    nxt = df["ts"].diff().shift(-1) == pd.Timedelta(minutes=1)
    for side in ("bid", "ask"):
        d = ((df[f"{side}_open"].shift(-1) - df[f"{side}_close"]).abs() * divisor)[nxt]
        out[side] = {"pairs": int(len(d)),
                     "median_points": float(d.median()) if len(d) else None,
                     "share_within_1_point": float((d <= 1.0 + 1e-6).mean()) if len(d) else None}
    return out


def validate(df: pd.DataFrame, symbol: str, label: str,
             month_start: pd.Timestamp, month_end: pd.Timestamp) -> dict:
    """
    Harte Checks werfen ValidationError. Weiche Auffaelligkeiten landen nur im
    Report. Bewusst KEINE harte Lueckengrenze wie bei Krypto: Metalle/FX
    haben Tagespausen und Wochenenden; die Einordnung geplant/ungeplant
    macht der Bot mit seinem Handelskalender.
    """
    if df.empty:
        raise ValidationError(f"{label}: leer")
    rep: dict = {"label": label, "symbol": symbol, "rows": len(df)}

    dupes = int(df["ts"].duplicated().sum())
    if dupes:
        raise ValidationError(f"{label}: {dupes} doppelte ts")
    if not df["ts"].is_monotonic_increasing:
        raise ValidationError(f"{label}: ts nicht monoton")
    if df["ts"].iloc[0] < month_start or df["ts"].iloc[-1] >= month_end:
        raise ValidationError(f"{label}: Bars ausserhalb des Monats")

    nan = int(df[NUMERIC].isna().sum().sum())
    if nan:
        raise ValidationError(f"{label}: {nan} NaN in numerischen Spalten")
    if (df[BID + ASK] <= 0).any().any():
        raise ValidationError(f"{label}: Preise <= 0")
    if (df[["bid_volume", "ask_volume"]] < 0).any().any():
        raise ValidationError(f"{label}: negatives Volumen")
    if (df["tick_count"] < 1).any():
        raise ValidationError(f"{label}: Bar ohne Ticks")
    for side in ("bid", "ask"):
        bad = _ohlc_bad(df, side)
        if bad:
            raise ValidationError(f"{label}: {bad} Zeilen mit inkonsistentem {side}-OHLC")

    lo, hi = PLAUSIBLE[symbol]
    pmin, pmax = float(df[BID + ASK].min().min()), float(df[BID + ASK].max().max())
    if pmin < lo or pmax > hi:
        raise ValidationError(
            f"{label}: Preise {pmin:g}..{pmax:g} ausserhalb Plausibilitaetsbereich "
            f"{lo:g}..{hi:g} - POINT_DIVISOR[{symbol}]={POINT_DIVISOR[symbol]} pruefen")

    anchor = ANCHORS.get(symbol, {}).get(month_start.year)
    if anchor:
        med = float(((df["bid_close"] + df["ask_close"]) / 2).median())
        if not (anchor / ANCHOR_BAND <= med <= anchor * ANCHOR_BAND):
            raise ValidationError(
                f"{label}: Monatsmedian {med:g} weicht mehr als Faktor {ANCHOR_BAND:g} vom "
                f"Jahresanker {anchor:g} ab - POINT_DIVISOR[{symbol}]={POINT_DIVISOR[symbol]} pruefen")
        rep["anchor_ratio"] = med / anchor

    crossed = int((df["ask_close"] < df["bid_close"]).sum())
    rep["crossed"] = crossed
    if crossed / len(df) > MAX_CROSSED_SHARE:
        raise ValidationError(
            f"{label}: {crossed}/{len(df)} Bars mit ask_close < bid_close "
            f"(> {MAX_CROSSED_SHARE:.1%})")

    # ----------------------------------------------- weich: nur Report ---
    def examples(mask: pd.Series, col: pd.Series) -> list[dict]:
        sel = col[mask].abs().nlargest(N_EXAMPLES).index
        return [{"ts": df.at[i, "ts"].isoformat(), "value": float(col.at[i])} for i in sel]

    spread = df["ask_close"] - df["bid_close"]
    med = float(spread.median())
    rep["spread_median"] = med
    out_mask = spread > SPREAD_OUTLIER_X * med if med > 0 else pd.Series(False, index=df.index)
    rep["spread_outliers"] = int(out_mask.sum())
    rep["spread_outlier_examples"] = examples(out_mask, spread)

    mid = (df["ask_close"] + df["bid_close"]) / 2
    ret = mid.pct_change()
    jump_mask = ret.abs() > JUMP_THRESHOLD
    rep["price_jumps"] = int(jump_mask.sum())
    rep["price_jump_examples"] = examples(jump_mask, ret)

    # grobes Raster: alle Minuten Mo-Fr (UTC). Enthaelt Tagespausen und den
    # Freitagabend/Sonntagabend-Rand -> nur Orientierung, kein Fehler.
    grid = pd.date_range(month_start, month_end, freq="1min", inclusive="left")
    grid = grid[grid.dayofweek < 5]
    rep["weekday_grid_minutes"] = int(len(grid))
    rep["weekday_minutes_without_ticks"] = int((~grid.isin(df["ts"])).sum())

    ts = df["ts"].reset_index(drop=True)
    gaps = ts.diff().dt.total_seconds().div(60).fillna(1) - 1
    rep["longest_gaps"] = [
        {"after": ts[i - 1].isoformat(), "next": ts[i].isoformat(), "missing_minutes": int(v)}
        for i, v in gaps.nlargest(N_EXAMPLES).items() if v > 0]
    rep["continuity"] = continuity(df.reset_index(drop=True), POINT_DIVISOR[symbol])

    rep["first"] = df["ts"].iloc[0].isoformat()
    rep["last"] = df["ts"].iloc[-1].isoformat()
    rep["ticks"] = int(df["tick_count"].sum())
    return rep


# ------------------------------------------------------------- monatsbau ---
def months(start: date, end: date):
    y, m = start.year, start.month
    while (y, m) <= (end.year, end.month):
        yield y, m
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)


def utc_today() -> date:
    return datetime.now(timezone.utc).date()


def _month_range(y: int, m: int) -> tuple[datetime, datetime]:
    return (datetime(y, m, 1, tzinfo=timezone.utc),
            datetime(y + (m == 12), m % 12 + 1, 1, tzinfo=timezone.utc))


def month_hours(y: int, m: int) -> list[datetime]:
    t, end = _month_range(y, m)
    out = []
    while t < end:
        out.append(t)
        t += timedelta(hours=1)
    return out


def month_days(y: int, m: int) -> list[datetime]:
    t, end = _month_range(y, m)
    out = []
    while t < end:
        out.append(t)
        t += timedelta(days=1)
    return out


def _read_cached(data_root: Path, f: RawFile) -> bytes | None:
    """Blob aus dem Cache; None bei 404-Marker. Fehlt beides: Programmfehler."""
    p = f.path(data_root)
    if p.exists():
        return p.read_bytes()
    if f.marker(data_root).exists():
        return None
    raise RuntimeError(f"{f.key}: weder im Cache noch als 404 markiert")


def build_month_ticks(data_root: Path, symbol: str, y: int, m: int) -> tuple[pd.DataFrame, dict]:
    """Alle Tick-Stunden eines Monats aus dem Cache zu 1m-Bars verdichten."""
    divisor = POINT_DIVISOR[symbol]
    hours = month_hours(y, m)
    frames, stats = [], {"hours": len(hours), "hours_404": 0, "hours_empty": 0}
    for h in hours:
        f = RawFile(symbol, "ticks", h)
        blob = _read_cached(data_root, f)
        if blob is None:
            stats["hours_404"] += 1
            continue
        try:
            tk = decode_bi5(blob, int(h.timestamp() * 1000), divisor)
        except (ValidationError, lzma.LZMAError) as e:
            raise ValidationError(f"{symbol} {h:%Y-%m-%d %H}h ({f.path(data_root)}): {e}")
        if tk.empty:
            stats["hours_empty"] += 1
        else:
            frames.append(tk)
    if not frames:
        return aggregate_1m(pd.DataFrame()), stats
    return aggregate_1m(pd.concat(frames, ignore_index=True)), stats


class FileFieldOrderError(FieldOrderError):
    def __init__(self, f: RawFile, msg: str):
        super().__init__(msg)
        self.file = f


def build_month_candles(data_root: Path, symbol: str, y: int, m: int) -> tuple[pd.DataFrame, dict]:
    """
    Alle Kerzen-Tage eines Monats aus dem Cache: je Tag BID + ASK decodieren
    und zusammenfuehren. Ein Tag mit Daten nur auf einer Seite wird
    verworfen und im Report gefuehrt.
    """
    divisor = POINT_DIVISOR[symbol]
    days = month_days(y, m)
    frames = []
    stats: dict = {"days": len(days), "files": 2 * len(days), "files_404": 0, "days_404": 0,
                   "days_empty": 0, "days_one_side_only": [], "fill_minutes_dropped": 0,
                   "one_side_minutes_dropped": 0, "zero_volume_nonflat_minutes": 0,
                   "field_order": {}}
    for d in days:
        sides: dict[str, pd.DataFrame | None] = {}
        for s in SIDES:
            f = RawFile(symbol, "candles", d, s)
            blob = _read_cached(data_root, f)
            if blob is None:
                stats["files_404"] += 1
                sides[s] = None
                continue
            try:
                df, order = decode_candles(blob, int(d.timestamp() * 1000), divisor)
            except FieldOrderError as e:
                raise FileFieldOrderError(f, str(e))
            except (ValidationError, lzma.LZMAError) as e:
                raise ValidationError(f"{f.key} ({f.path(data_root)}): {e}")
            stats["field_order"][order] = stats["field_order"].get(order, 0) + 1
            sides[s] = df
        present = [s for s in SIDES if sides[s] is not None and not sides[s].empty]
        if not present:
            stats["days_404" if all(sides[s] is None for s in SIDES) else "days_empty"] += 1
            continue
        if len(present) == 1:
            stats["days_one_side_only"].append(f"{d:%Y-%m-%d} nur {present[0]}")
            log.warning("%s %s: Daten nur auf %s-Seite, Tag verworfen", symbol, f"{d:%Y-%m-%d}",
                        present[0])
            continue
        bars, st = merge_candles(sides["BID"], sides["ASK"])
        for k, v in st.items():
            stats[k] += v
        if not bars.empty:
            frames.append(bars)
    if not frames:
        return pd.DataFrame(columns=COLUMNS), stats
    return pd.concat(frames, ignore_index=True), stats


def write_parquet(out: Path, df: pd.DataFrame, mode: str) -> None:
    """Atomar (.part + os.replace); Modus als Schema-Metadatum dukascopy_mode."""
    out.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(df, preserve_index=False)
    meta = dict(table.schema.metadata or {})
    meta[b"dukascopy_mode"] = mode.encode()
    table = table.replace_schema_metadata(meta)
    tmp = out.with_name(out.name + ".part")
    pq.write_table(table, tmp, compression="zstd")
    os.replace(tmp, out)


# ------------------------------------------------------------------ driver ---
def load_previous_gaps(data_root: Path, symbol: str, mode: str) -> list[RawFile]:
    """Lueckenliste des letzten Laufs (gleicher Modus); defekter Report -> leer."""
    p = run_report_path(data_root, symbol)
    if not p.exists():
        return []
    try:
        rep = json.loads(p.read_text())
        return [RawFile.from_json(symbol, g) for g in rep.get("gaps", [])
                if g.get("mode", mode) == mode and g.get("symbol", symbol) == symbol]
    except (ValueError, KeyError, TypeError) as e:
        log.warning("Laufreport %s unlesbar (%s), ignoriert", p, e)
        return []


def run(symbol: str, start: date, end: date, data_root: Path | None = None,
        today: date | None = None, *, mode: str = "candles", workers: int = WORKERS,
        rebuild: bool = False, pause: float = PAUSE_S, attempts: int = ATTEMPTS,
        backoff_base: float = BACKOFF_BASE_S, max_backoff: float = MAX_BACKOFF_S,
        jitter: float = JITTER, final_retries: int = FINAL_RETRIES,
        final_wait: float = FINAL_WAIT_S,
        timeout: tuple[float, float] = (CONNECT_TIMEOUT_S, READ_TIMEOUT_S),
        progress_every: int = PROGRESS_EVERY, sleep=None, clock=None, rng=None,
        stop: threading.Event | None = None) -> dict:
    """
    Laedt alle abgeschlossenen Monate [start, end] und gibt den Laufreport
    zurueck (Feld exit_code: 0 vollstaendig, 3 Luecken). Wirft
    ValidationError (Exit 2) - der Laufreport wird auch dann geschrieben.
    """
    if symbol not in POINT_DIVISOR:
        raise ValueError(f"unbekanntes Symbol {symbol}; bekannt: {sorted(POINT_DIVISOR)}")
    if mode not in MODES:
        raise ValueError(f"unbekannter Modus {mode}; bekannt: {MODES}")
    data_root = data_root or default_data_root()
    today = today or utc_today()
    stop = stop or threading.Event()
    clock = clock or time.monotonic
    fetcher = Fetcher(data_root, workers=workers, pause=pause, attempts=attempts,
                      backoff_base=backoff_base, max_backoff=max_backoff, jitter=jitter,
                      timeout=timeout, sleep=sleep, clock=clock, rng=rng, stop=stop, today=today)
    build = build_month_candles if mode == "candles" else build_month_ticks

    started = datetime.now(timezone.utc)
    months_todo: list[tuple[int, int]] = []
    skipped_existing = 0
    for y, m in months(start, end):
        # Laufender Monat wird NICHT geladen und NICHT geschrieben: eine
        # Teilmonatsdatei saehe aus wie eine fertige und wuerde nie ersetzt.
        # Damit wird auch nie eine Kerzendatei des laufenden Tages angefragt.
        if (y, m) >= (today.year, today.month):
            log.info("skip %s %d-%02d (Monat nicht abgeschlossen)", symbol, y, m)
            continue
        if store_path(data_root, symbol, y, m).exists() and not rebuild:
            skipped_existing += 1
            continue
        months_todo.append((y, m))
    files_by_month = {ym: month_files(symbol, mode, *ym) for ym in months_todo}
    pending_total = sum(1 for fs in files_by_month.values() for f in fs if not f.resolved(data_root))
    log.info("START %s mode=%s: %d Monate offen (%d fertig uebersprungen), %d von %d Dateien "
             "zu laden", symbol, mode, len(months_todo), skipped_existing, pending_total,
             sum(len(v) for v in files_by_month.values()))

    gaps: dict[RawFile, dict] = {}        # ungeloeste Fehlschlaege
    counts = {"downloaded": 0, "404": 0, "recovered_in_final_retries": 0}
    finished: dict[tuple[int, int], str] = {}   # Monat -> written | empty
    month_reports: list[dict] = []
    state = {"status": None, "error": None, "final_rounds": 0}

    def record(outs: dict[RawFile, Outcome], final: bool = False) -> None:
        for f, o in outs.items():
            if o.status == "failed":
                prev = gaps.get(f)
                gaps[f] = {"error": o.error, "http_status": o.http_status,
                           "attempts": (prev["attempts"] if prev else 0) + o.attempts}
            elif o.status in ("ok", "404"):
                counts["downloaded" if o.status == "ok" else "404"] += 1
                if gaps.pop(f, None) is not None and final:
                    counts["recovered_in_final_retries"] += 1

    def finish(ym: tuple[int, int]) -> None:
        y, m = ym
        label = f"{symbol} {y}-{m:02d}"
        try:
            df, stats = build(data_root, symbol, y, m)
        except FileFieldOrderError as e:
            # nicht raten: Datei beiseitelegen (nicht mehr Cache) -> Luecke, erneut laden
            p = e.file.path(data_root)
            os.replace(p, p.with_name(e.file.name + ".unclear"))
            gaps[e.file] = {"error": str(e), "http_status": None,
                            "attempts": gaps.get(e.file, {}).get("attempts", 0)}
            log.warning("%s: %s - Monat nicht geschrieben", label, e)
            return
        if df.empty:
            log.warning("skip %s (keine Daten; z.B. vor Beginn der Historie)", label)
            finished[ym] = "empty"
            return
        ms = pd.Timestamp(y, m, 1, tz="UTC")
        rep = validate(df, symbol, label, ms, ms + pd.offsets.MonthBegin(1))
        rep.update(stats)
        rep["mode"] = mode
        out = store_path(data_root, symbol, y, m)
        rep["file"] = str(out.relative_to(data_root))
        write_parquet(out, df, mode)
        write_json_atomic(report_path(data_root, symbol, y, m), rep)
        finished[ym] = "written"
        month_reports.append(rep)
        log.info("%-15s %6d Bars  gekreuzt=%-3d spread>20x=%-4d spruenge=%-3d "
                 "MoFr-ohne-Ticks=%d  mode=%s", label, rep["rows"], rep["crossed"],
                 rep["spread_outliers"], rep["price_jumps"],
                 rep["weekday_minutes_without_ticks"], mode)
        if rep["spread_outliers"] or rep["price_jumps"]:
            log.warning("%s: Auffaelligkeiten, siehe %s", label, report_path(data_root, symbol, y, m))

    def complete(ym) -> bool:
        return all(f.resolved(data_root) for f in files_by_month[ym]) and \
            not any(f.month == ym for f in gaps)

    try:
        progress = Progress(pending_total, progress_every, clock)
        prev = [f for f in load_previous_gaps(data_root, symbol, mode)
                if f.month in files_by_month and not f.resolved(data_root)]
        if prev:
            log.info("%d Dateien aus der Lueckenliste des letzten Laufs zuerst", len(prev))
            record(fetcher.fetch(prev, progress))
        for ym in months_todo:
            if stop.is_set():
                break
            todo = [f for f in files_by_month[ym] if f not in gaps]   # Fehlschlaege: am Ende
            record(fetcher.fetch(todo, progress))
            if complete(ym):
                finish(ym)
            elif not stop.is_set():
                n = sum(1 for f in gaps if f.month == ym)
                log.warning("%s %d-%02d unvollstaendig (%d Fehlschlaege) - nicht geschrieben, "
                            "Wiederholung am Ende", symbol, *ym, n)

        for r in range(1, final_retries + 1):
            if not gaps or stop.is_set():
                break
            state["final_rounds"] = r
            log.warning("WIEDERHOLUNG %d/%d: %d Fehlschlaege nach %.0fs Pause", r, final_retries,
                        len(gaps), final_wait)
            fetcher.sleep(final_wait)
            if stop.is_set():
                break
            retry = sorted(gaps, key=lambda f: (f.t, f.side))
            # Datei muss aus dem Cache verschwunden sein, sonst waere sie keine Luecke
            record(fetcher.fetch(retry, Progress(len(retry), progress_every, clock,
                                                 f"Runde {r}: ")), final=True)
            for ym in months_todo:
                if ym not in finished and complete(ym):
                    finish(ym)
    except ValidationError as e:
        state["status"], state["error"] = "validation_error", str(e)
        raise
    finally:
        incomplete = [f"{y}-{m:02d}" for (y, m) in months_todo if (y, m) not in finished]
        if state["status"] is None:
            state["status"] = ("complete" if not incomplete and not gaps
                               else "interrupted" if stop.is_set() else "gaps")
        exit_code = (EXIT_VALIDATION if state["status"] == "validation_error"
                     else EXIT_OK if state["status"] == "complete" else EXIT_GAPS)
        n403 = fetcher.status_counts.get("403", 0)
        waf = fetcher.requests > 0 and n403 >= max(1, WAF_SHARE * fetcher.requests)
        hint = None
        if waf:
            hint = (f"ueberwiegend HTTP 403 ({n403}/{fetcher.requests} Requests): vermutlich "
                    "WAF/Bot-Schutz des Datafeeds. --pause erhoehen, --workers 1, spaeter erneut "
                    "starten (Cache bleibt).")
            log.error(hint)
        rep = {
            "symbol": symbol, "mode": mode, "status": state["status"], "exit_code": exit_code,
            "started": started.isoformat(), "finished": datetime.now(timezone.utc).isoformat(),
            "range": {"start": f"{start:%Y-%m}", "end": f"{end:%Y-%m}"},
            "options": {"workers": workers, "pause": pause, "attempts": attempts,
                        "backoff_base": backoff_base, "max_backoff": max_backoff,
                        "final_retries": final_retries, "final_wait": final_wait,
                        "connect_timeout": timeout[0], "read_timeout": timeout[1],
                        "rebuild": rebuild},
            "summary": {
                "months_considered": len(months_todo),
                "months_skipped_existing": skipped_existing,
                "months_written": sorted(f"{y}-{m:02d}" for (y, m), v in finished.items()
                                         if v == "written"),
                "months_empty": sorted(f"{y}-{m:02d}" for (y, m), v in finished.items()
                                       if v == "empty"),
                "months_incomplete": incomplete,
                "files_total": sum(len(v) for v in files_by_month.values()),
                "files_pending_at_start": pending_total,
                "files_downloaded": counts["downloaded"],
                "files_404": counts["404"],
                "requests": fetcher.requests,
                "http_status_counts": dict(sorted(fetcher.status_counts.items())),
                "http_403_waf_suspected": n403,
                "global_cooldowns": fetcher.throttle.cooldowns,
                "final_retry_rounds": state["final_rounds"],
                "recovered_in_final_retries": counts["recovered_in_final_retries"],
                "failed_files": len(gaps),
                "interrupted": stop.is_set(),
            },
            "hint": hint,
            "error": state["error"],
            "gaps": [
                {"symbol": symbol, "mode": mode, "month": f"{f.t:%Y-%m}", "file": f.key,
                 **f.to_json(), "url": f.url(), "http_status": g["http_status"],
                 "last_error": g["error"], "attempts": g["attempts"]}
                for f, g in sorted(gaps.items(), key=lambda kv: (kv[0].t, kv[0].side))],
        }
        write_json_atomic(run_report_path(data_root, symbol), rep)
        log.info("FERTIG %s: status=%s, %d Monate geschrieben, %d Luecken, %d Requests "
                 "(Report %s)", symbol, state["status"], len(rep["summary"]["months_written"]),
                 len(gaps), fetcher.requests, run_report_path(data_root, symbol))
    return rep


# ------------------------------------------------------------- verify-day ---
def compare_bars(ticks: pd.DataFrame, candles: pd.DataFrame, divisor: int,
                 tol_points: float = 0.5) -> dict:
    """Aus Ticks gebildete Minuten vs. Kerzen-Minuten: OHLC je Seite."""
    ticks, candles = (df.assign(ts=pd.to_datetime(df["ts"], utc=True).astype("datetime64[ms, UTC]"))
                      for df in (ticks, candles))
    m = ticks.merge(candles, on="ts", how="outer", suffixes=("_t", "_c"), indicator=True)
    common = m[m["_merge"] == "both"]
    only_t = m[m["_merge"] == "left_only"]["ts"]
    only_c = m[m["_merge"] == "right_only"]["ts"]
    dev = pd.DataFrame({c: (common[f"{c}_t"] - common[f"{c}_c"]).abs() * divisor
                        for c in BID + ASK})
    bad = (dev > tol_points).any(axis=1)
    union = len(m)
    rep = {
        "minutes_ticks": int(len(ticks)), "minutes_candles": int(len(candles)),
        "minutes_common": int(len(common)),
        "minutes_only_ticks": int(len(only_t)), "minutes_only_candles": int(len(only_c)),
        "only_ticks_examples": [t.isoformat() for t in only_t.head(N_EXAMPLES)],
        "only_candles_examples": [t.isoformat() for t in only_c.head(N_EXAMPLES)],
        "max_abs_dev_points": {c: float(dev[c].max()) if len(dev) else 0.0 for c in dev},
        "minutes_deviating": int(bad.sum()),
        "deviating_examples": [
            {"ts": common.loc[i, "ts"].isoformat(),
             **{c: [float(common.loc[i, f"{c}_t"]), float(common.loc[i, f"{c}_c"])]
                for c in BID + ASK if dev.loc[i, c] > tol_points}}
            for i in bad[bad].index[:N_EXAMPLES]],
        "volume_rel_dev_max": {
            s: float(((common[f"{s}_t"] - common[f"{s}_c"]).abs()
                      / common[f"{s}_t"].abs().clip(lower=1e-12)).max()) if len(common) else 0.0
            for s in ("bid_volume", "ask_volume")},
        "tol_points": tol_points,
    }
    rep["mismatch_share"] = (int(bad.sum()) + len(only_t) + len(only_c)) / union if union else 1.0
    return rep


def verify_day(symbol: str, day: date, data_root: Path | None = None, today: date | None = None,
               *, tol_points: float = 0.5, max_share: float = 0.0, **fetch_kw) -> dict:
    """
    Laedt fuer einen abgeschlossenen Tag 24 Tick-Stunden und BID/ASK-Kerzen
    (ueber den Cache), aggregiert die Ticks zu Minuten und vergleicht OHLC je
    Seite. exit_code: 0 ok, 3 Download gescheitert, 4 Abweichung/keine Daten.
    """
    data_root = data_root or default_data_root()
    today = today or utc_today()
    if day >= today:
        raise ValueError(f"--verify-day {day}: nur abgeschlossene UTC-Tage (heute {today})")
    divisor = POINT_DIVISOR[symbol]
    d0 = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    tick_files = [RawFile(symbol, "ticks", d0 + timedelta(hours=h)) for h in range(24)]
    candle_files = [RawFile(symbol, "candles", d0, s) for s in SIDES]
    fetcher = Fetcher(data_root, today=today, **fetch_kw)
    outs = fetcher.fetch(tick_files + candle_files)
    rep: dict = {"symbol": symbol, "day": f"{day}", "requests": fetcher.requests}
    failed = {f.key: o.error for f, o in outs.items() if o.status in ("failed", "skipped")}
    if failed:
        rep.update(ok=False, exit_code=EXIT_GAPS, failed=failed)
    else:
        frames = []
        for f in tick_files:
            blob = _read_cached(data_root, f)
            if blob is not None:
                frames.append(decode_bi5(blob, int(f.t.timestamp() * 1000), divisor))
        tick_bars = aggregate_1m(pd.concat(frames, ignore_index=True) if frames else pd.DataFrame())
        sides, orders = {}, {}
        for f in candle_files:
            blob = _read_cached(data_root, f)
            sides[f.side], orders[f.side] = decode_candles(blob, int(d0.timestamp() * 1000),
                                                           divisor)
            if blob is None:
                orders[f.side] = "404"
        candle_bars, st = merge_candles(sides["BID"], sides["ASK"])
        rep["field_order"] = orders
        rep.update(st)
        rep.update(compare_bars(tick_bars, candle_bars, divisor, tol_points))
        if len(candle_bars):
            rep["continuity_candles"] = continuity(candle_bars, divisor)
        if len(tick_bars):
            rep["continuity_ticks"] = continuity(tick_bars.reset_index(drop=True), divisor)
        no_data = rep["minutes_ticks"] == 0 and rep["minutes_candles"] == 0
        rep["ok"] = (not no_data) and rep["mismatch_share"] <= max_share
        rep["max_share"] = max_share
        if no_data:
            rep["note"] = "keine Daten an diesem Tag (Wochenende/Feiertag?) - Werktag waehlen"
        rep["exit_code"] = EXIT_OK if rep["ok"] else EXIT_VERIFY
    write_json_atomic(verify_report_path(data_root, symbol, day), rep)
    log.info("VERIFY %s %s: ok=%s Reihenfolge=%s Minuten ticks=%s kerzen=%s gemeinsam=%s "
             "abweichend=%s nur-ticks=%s nur-kerzen=%s max-dev(Punkte)=%s",
             symbol, day, rep.get("ok"), rep.get("field_order"), rep.get("minutes_ticks"),
             rep.get("minutes_candles"), rep.get("minutes_common"), rep.get("minutes_deviating"),
             rep.get("minutes_only_ticks"), rep.get("minutes_only_candles"),
             rep.get("max_abs_dev_points"))
    return rep


# -------------------------------------------------------------------- cli ---
def install_stop_handler(stop: threading.Event) -> None:
    """SIGTERM/SIGINT: keine neuen Downloads, laufende enden, Report wird geschrieben."""
    def handler(signum, frame):
        log.warning("Signal %d: beende nach laufenden Downloads, schreibe Report", signum)
        stop.set()
    signal.signal(signal.SIGTERM, handler)
    signal.signal(signal.SIGINT, handler)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--symbol", required=True, type=str.upper, choices=sorted(POINT_DIVISOR))
    p.add_argument("--start", help="YYYY-MM (Pflicht ausser bei --verify-day)")
    p.add_argument("--end", default=None, help="YYYY-MM (default: heute)")
    p.add_argument("--mode", choices=MODES, default="candles",
                   help="candles: 1m-Kerzen je Tag/Seite (default); ticks: stuendliche Ticks")
    p.add_argument("--workers", type=int, default=WORKERS, help=f"parallele Downloads (default {WORKERS})")
    p.add_argument("--pause", type=float, default=PAUSE_S,
                   help=f"Pause je Worker nach jedem Request in s (default {PAUSE_S})")
    p.add_argument("--attempts", type=int, default=ATTEMPTS,
                   help=f"Versuche je Datei und Durchgang (default {ATTEMPTS})")
    p.add_argument("--backoff-base", type=float, default=BACKOFF_BASE_S,
                   help=f"Backoff-Basis in s, Faktor 2 (default {BACKOFF_BASE_S})")
    p.add_argument("--max-backoff", type=float, default=MAX_BACKOFF_S,
                   help=f"Deckel Backoff/Retry-After in s (default {MAX_BACKOFF_S})")
    p.add_argument("--final-retries", type=int, default=FINAL_RETRIES,
                   help=f"gesammelte Wiederholungsrunden am Ende (default {FINAL_RETRIES})")
    p.add_argument("--final-wait", type=float, default=FINAL_WAIT_S,
                   help=f"Wartezeit vor jeder Wiederholungsrunde in s (default {FINAL_WAIT_S})")
    p.add_argument("--connect-timeout", type=float, default=CONNECT_TIMEOUT_S)
    p.add_argument("--read-timeout", type=float, default=READ_TIMEOUT_S)
    p.add_argument("--progress-every", type=int, default=PROGRESS_EVERY,
                   help=f"Fortschritt alle N Dateien (default {PROGRESS_EVERY})")
    p.add_argument("--rebuild", action="store_true",
                   help="vorhandene Monatsdateien neu bauen (aus dem Roh-Cache)")
    p.add_argument("--verify-day", metavar="YYYY-MM-DD",
                   help="Kerzen gegen aus Ticks gebildete Minuten pruefen (ein Tag)")
    p.add_argument("--verify-tol", type=float, default=0.5,
                   help="erlaubte Abweichung je OHLC-Wert in Punkten (default 0.5 = exakt)")
    p.add_argument("--verify-max-share", type=float, default=0.0,
                   help="erlaubter Anteil abweichender Minuten (default 0.0)")
    p.add_argument("-v", "--verbose", action="store_true")
    a = p.parse_args(argv)
    if a.workers < 1 or a.attempts < 1 or a.final_retries < 0 or min(
            a.pause, a.backoff_base, a.max_backoff, a.final_wait) < 0:
        p.error("--workers/--attempts >= 1, uebrige Zahlen >= 0")

    logging.basicConfig(
        level=logging.DEBUG if a.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
    )
    stop = threading.Event()
    install_stop_handler(stop)
    kw = dict(workers=a.workers, pause=a.pause, attempts=a.attempts,
              backoff_base=a.backoff_base, max_backoff=a.max_backoff,
              timeout=(a.connect_timeout, a.read_timeout), stop=stop,
              sleep=lambda s: stop.wait(s))       # Schlaf durch SIGTERM unterbrechbar

    if a.verify_day:
        try:
            day = date.fromisoformat(a.verify_day)
            return verify_day(a.symbol, day, tol_points=a.verify_tol,
                              max_share=a.verify_max_share, **kw)["exit_code"]
        except ValueError as e:
            p.error(str(e))
        except ValidationError as e:
            log.error("VALIDIERUNG FEHLGESCHLAGEN: %s", e)
            return EXIT_VALIDATION

    if not a.start:
        p.error("--start fehlt")
    sy, sm = map(int, a.start.split("-"))
    if a.end:
        ey, em = map(int, a.end.split("-"))
    else:
        ey, em = utc_today().year, utc_today().month
    try:
        rep = run(a.symbol, date(sy, sm, 1), date(ey, em, 1), mode=a.mode, rebuild=a.rebuild,
                  final_retries=a.final_retries, final_wait=a.final_wait,
                  progress_every=a.progress_every, **kw)
    except ValidationError as e:
        log.error("VALIDIERUNG FEHLGESCHLAGEN: %s", e)
        return EXIT_VALIDATION
    if rep["exit_code"] == EXIT_GAPS:
        log.error("LUECKEN: %d Dateien fehlgeschlagen, %d Monate unvollstaendig - Folgelauf "
                  "holt nur fehlende Dateien nach (%s)", rep["summary"]["failed_files"],
                  len(rep["summary"]["months_incomplete"]), run_report_path(default_data_root(),
                                                                            a.symbol))
    return rep["exit_code"]


if __name__ == "__main__":
    sys.exit(main())
