#!/usr/bin/env python3
"""
Historische Dukascopy-Daten (XAUUSD, XAGUSD, EURUSD) als 1-Minuten-Bars mit
Bid- UND Ask-OHLC laden.

- Modus "candles" (Default): je Tag und Seite eine Datei mit 1-Minuten-
  Kerzen (BID + ASK = 2 Requests je Tag statt 24 Tick-Stunden, ~12x weniger)
- Modus "ticks": stuendliche Tickdateien, selbst zu 1m-Bars verdichtet
- cached Rohdateien (Neubau/Fortsetzen ohne erneuten Download)
- schreibt nur ABGESCHLOSSENE UTC-Monate (Monatsende mindestens SETTLE_S
  her) und nur VOLLSTAENDIGE Monate (keine ungeloesten Fehlschlaege,
  keine unbestaetigten "keine Daten"-Antworten, keine Quarantaene)
- fehlertolerant: gescheiterte Dateien werden gesammelt, der Lauf geht
  weiter, am Ende gesammelte Wiederholungsrunden; ein Laufreport mit
  Lueckenliste wird IMMER geschrieben; Validierungsfehler werden je Monat
  gesammelt (uebrige Monate laufen weiter)
- Drosselung: Pause je Worker, eskalierende globale Abkuehlung bei Drossel-/
  Fehlerantworten (403/408/425/5xx, Timeouts), Schutzschalter nach N
  Fehlschlaegen in Folge
- HTTP 429 (Rate-Limit pro Client-IP) gesondert (ratelimit.py): KEIN
  Fehlversuch, keine Eskalation, zaehlt nicht fuer den Schutzschalter.
  Stattdessen Pause max(Retry-After, --rate-limit-pause >= 15 min) fuer ALLE
  Worker, persistiert in metals/state/dukascopy-ratelimit.json (gilt auch
  nach Neustart und fuer parallele Prozesse); danach dieselbe Datei erneut.
  Wartebudget je Lauf --max-rate-limit-wait (default 6 h): danach keine
  weiteren Requests, offene Dateien als "pending" im Laufreport, Exit 3
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
  StackOverflow-Quelle: open, high, low, close). Deshalb wird sie JE DATEI
  aus den Daten bestimmt (detect_field_order):
    - genau eine Reihenfolge passt zu allen nicht flachen Kerzen -> diese
    - beide passen -> "ambiguous": nur moeglich, wenn in jeder nicht flachen
      Kerze close == high; beide Lesarten liefern dann identische Werte
    - keine passt -> Datei in QUARANTAENE (<name>.quarantine), Monat wird
      nicht geschrieben, Exit 4 ("manuelle Pruefung noetig"); die Datei wird
      nicht erneut geladen, bis die Quarantaenedatei geloescht ist
  Pruefen gegen Ticks: --verify-day YYYY-MM-DD (26 Requests: 24 Tick-
  Stunden + 2 Kerzendateien). Unter Rate-Limit: --verify-day YYYY-MM-DD
  --candles-only (nur BID+ASK-Kerzen = 2 Requests, Plausibilitaetspruefung
  statt Tickvergleich). Beide schreiben die Tagesbars zusaetzlich als
  Stichprobe (metals/samples/..., s. Ablage).

Fuellkerzen (Annahme): Minuten ohne Ticks sind flache Kerzen mit volume 0.
Eine Minute wird verworfen, wenn BEIDE Seiten volume == 0 UND flache Preise
(O=H=L=C) haben. tick_count ist im Kerzenmodus KEIN echter Tick-Zaehler:
1 fuer jede behaltene Minute (erfuellt tick_count >= 1); der Modus steht im
Monats- und Laufreport und in den Parquet-Metadaten (dukascopy_mode).

"Keine Daten" (404 oder leere 200-Antwort) wird als Marker <name>.404 bzw.
<name>.empty gecached (JSON: first_seen, confirmed), nie als 0-Byte-Rohdatei.
An Werktagen (Mo-Fr des Datentags, ausser 25.12./1.1.) gilt das zunaechst
als UNBESTAETIGT (Luecke, Monat nicht geschrieben) und wird erst endgueltig,
wenn ein spaeterer Request frühestens CONFIRM_AFTER_S (24 h) danach wieder
keine Daten liefert. Im Tickmodus gilt das nur fuer 404 (leere Werktags-
stunden sind normal: Tagespause). Wochenende/Feiertag: sofort endgueltig.
Ein Kerzentag mit Daten nur auf einer Seite: ist die fehlende Seite nur
automatisch (Wochenende) bestaetigt, wird sie auf unbestaetigt gesetzt
(Luecke); nach Wiederholungsbestaetigung wird der Tag verworfen und im
Monatsreport gefuehrt. Kaputte Cache-Dateien (LZMA, 0 Byte) werden als
<name>.corrupt beiseitegelegt und neu geladen.

Ablage (DATA_ROOT = $MARKETDATA_DATA_ROOT, sonst <repo>/data):
    raw/dukascopy/{SYM}/{YYYY}/{MM}/{DD}/{HH}h_ticks.bi5
    raw/dukascopy/{SYM}/{YYYY}/{MM}/{DD}/{BID|ASK}_candles_min_1.bi5
        Rohdateien, Monat hier 1-BASIERT; daneben Marker .404/.empty,
        .quarantine, .corrupt (Marker loeschen = erneut anfragen).
    metals/source=dukascopy/symbol={SYM}/year={YYYY}/{SYM}-1m-{YYYY}-{MM}.parquet
    metals/reports/{SYM}-1m-{YYYY}-{MM}.json     Validierungsreport je Monat
    metals/reports/{SYM}-dukascopy-run.json      Laufreport inkl. Lueckenliste
                                                 (wird beim Start gelesen)
    metals/reports/{SYM}-verify-{YYYY-MM-DD}.json  Ergebnis --verify-day
    metals/samples/source=dukascopy/symbol={SYM}/{SYM}-1m-{YYYY-MM-DD}.parquet
        Tagesbars aus --verify-day (Store-Schema), bewusst AUSSERHALB von
        metals/source=dukascopy/..., damit der Loader des Bots sie nie mit
        dem Monatsstore mischt (Cross-Check-Stichproben)
    metals/state/dukascopy-ratelimit.json        429-Sperre (blocked_until), flock
    metals/.lock-{SYM}                           Prozess-Lock je Symbol (flock)
  Temporaerdateien heissen .{name}.{pid}.{thread}.part (Punkt vorn: werden
  von pyarrow/hive ignoriert); verwaiste werden beim Start entfernt. Jede
  Datei wird vor os.replace per fsync geschrieben, danach das Verzeichnis.

Exit-Codes (Vorrang in dieser Reihenfolge):
  2  Validierungsfehler (mindestens ein Monat; uebrige Monate liefen weiter)
  3  Luecken: Downloads gescheitert, unbestaetigte "keine Daten", Lauf
     unterbrochen (SIGTERM), Schutzschalter ausgeloest, 429-Wartebudget
     erschoepft oder Symbol-Lock belegt -> spaeter erneut (systemd-Neustart
     sinnvoll)
  4  manuelle Pruefung noetig: Datei(en) in Quarantaene (Feldreihenfolge);
     bei --verify-day: Abweichung Kerzen vs. Ticks ueber Toleranz bzw.
     (--candles-only) Kerzen unplausibel
  0  vollstaendig

Lizenz: Dukascopy-Nutzungsbedingungen selbst pruefen. Daten (roh wie
abgeleitet) nicht weitergeben und nicht committen (data/ ist git-ignored).

Usage:
    python fetch_dukascopy.py --symbol XAUUSD --start 2019-01
    python fetch_dukascopy.py --symbol EURUSD --start 2024-01 --end 2024-06 --mode ticks
    python fetch_dukascopy.py --symbol XAUUSD --start 2024-01 --end 2024-01 --rebuild
    python fetch_dukascopy.py --symbol XAUUSD --verify-day 2024-03-06
    python fetch_dukascopy.py --symbol XAUUSD --verify-day 2024-03-06 --candles-only
"""
from __future__ import annotations

import argparse
import fcntl
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

import ratelimit as rl
from ratelimit import RateLimitExhausted, RateLimitGate

BASE = "https://datafeed.dukascopy.com/datafeed"
ROOT = Path(__file__).resolve().parents[1]
SOURCE = "dukascopy"
USER_AGENT = "marketdata-pipeline/1.0"
UTC = timezone.utc

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
NO_DATA = ("404", "empty")          # Marker-Arten fuer "keine Daten"

# ---- Drosselung / Wiederholung (Defaults, alle per CLI aenderbar) ----------
WORKERS = 1
PAUSE_S = 1.0              # Pause je Worker nach jedem echten Request
ATTEMPTS = 5               # Versuche je Datei und Durchgang
BACKOFF_BASE_S = 2.0       # Abkuehlung nach k Fehlschlaegen in Folge: base * 2**k
MAX_BACKOFF_S = 600.0      # Deckel fuer Backoff und Retry-After
JITTER = 0.5               # Wartezeit * (1 - JITTER * U[0,1))
FINAL_RETRIES = 2          # gesammelte Wiederholungsrunden am Ende
FINAL_WAIT_S = 300.0       # Wartezeit vor jeder Wiederholungsrunde
CONNECT_TIMEOUT_S = 10.0
READ_TIMEOUT_S = 60.0
PROGRESS_EVERY = 50
MAX_CONSECUTIVE_FAILURES = 20   # Schutzschalter: Lauf sauber beenden (Exit 3)
CONFIRM_AFTER_S = 24 * 3600     # Werktags-"keine Daten" erst danach endgueltig
SETTLE_S = 6 * 3600             # Tagesende muss mindestens so lange her sein
# Wiederholbare HTTP-Status: zusaetzlich alle 5xx. 403 ist beim Datafeed
# vermutlich WAF/Bot-Schutz (neuere Pakete sind deshalb auf eine andere API
# umgestiegen) -> wie 503 behandeln, nie als "keine Daten".
# 429 ist NICHT dabei: eigener Pfad ueber RateLimitGate (kein Fehlversuch).
RETRY_STATUS = {403, 408, 425}
RATE_LIMIT_STATUS = 429
RATE_LIMIT_PAUSE_S = rl.MIN_PAUSE_S          # >= 15 min Pause nach 429 (CLI: nicht kleiner)
MAX_RATE_LIMIT_WAIT_S = rl.MAX_TOTAL_WAIT_S  # 429-Wartebudget je Lauf, danach Exit 3
WAF_SHARE = 0.5            # >= 50 % der Requests mit 403 -> deutlicher Hinweis

EXIT_OK, EXIT_VALIDATION, EXIT_GAPS, EXIT_MANUAL = 0, 2, 3, 4
EXIT_VERIFY = EXIT_MANUAL

MAX_CROSSED_SHARE = 0.001  # > 0.1 % gekreuzte Bars (ask_close < bid_close) -> Abbruch
SPREAD_OUTLIER_X = 20.0    # weich: Spread > 20x Monatsmedian
JUMP_THRESHOLD = 0.02      # weich: |Mid-Rendite 1m| > 2 %
N_EXAMPLES = 10
# --verify-day --candles-only (grobe Plausibilitaet eines Tages)
MIN_WEEKDAY_MINUTES = 600  # Werktag mit weniger Minuten -> unplausibel (Handel ~23 h/Tag)
MAX_SPREAD_REL = 0.01      # Median-Spread > 1 % vom Mid -> unplausibel

BID = ["bid_open", "bid_high", "bid_low", "bid_close"]
ASK = ["ask_open", "ask_high", "ask_low", "ask_close"]
COLUMNS = ["ts", *BID, *ASK, "tick_count", "bid_volume", "ask_volume", "source"]
NUMERIC = [*BID, *ASK, "tick_count", "bid_volume", "ask_volume"]

log = logging.getLogger("dukascopy")
_sleep = time.sleep        # in Tests ersetzbar


def default_data_root() -> Path:
    env = os.environ.get("MARKETDATA_DATA_ROOT")
    return Path(env) if env else ROOT / "data"


def utc_now() -> datetime:
    return datetime.now(UTC)


class ValidationError(Exception):
    pass


class FieldOrderError(ValidationError):
    """Keine bekannte Feldreihenfolge passt -> Datei in Quarantaene."""


class DownloadError(Exception):
    def __init__(self, msg: str, status: int | None = None, attempts: int = 1):
        super().__init__(msg)
        self.status, self.attempts = status, attempts


class Interrupted(Exception):
    """Stop-Signal (SIGTERM) oder Schutzschalter waehrend eines Downloads."""


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


def store_dir(data_root: Path, symbol: str) -> Path:
    return data_root / "metals" / f"source={SOURCE}" / f"symbol={symbol}"


def store_path(data_root: Path, symbol: str, y: int, m: int) -> Path:
    return store_dir(data_root, symbol) / f"year={y}" / f"{symbol}-1m-{y}-{m:02d}.parquet"


def reports_dir(data_root: Path) -> Path:
    return data_root / "metals" / "reports"


def report_path(data_root: Path, symbol: str, y: int, m: int) -> Path:
    return reports_dir(data_root) / f"{symbol}-1m-{y}-{m:02d}.json"


def run_report_path(data_root: Path, symbol: str) -> Path:
    return reports_dir(data_root) / f"{symbol}-dukascopy-run.json"


def verify_report_path(data_root: Path, symbol: str, day: date) -> Path:
    return reports_dir(data_root) / f"{symbol}-verify-{day:%Y-%m-%d}.json"


def lock_path(data_root: Path, symbol: str) -> Path:
    return data_root / "metals" / f".lock-{symbol}"


def ratelimit_state_path(data_root: Path) -> Path:
    """429-Sperre, geteilt von allen Dukascopy-Prozessen (alle Symbole: Limit gilt je IP)."""
    return data_root / "metals" / "state" / "dukascopy-ratelimit.json"


def sample_path(data_root: Path, symbol: str, day: date) -> Path:
    """Stichprobe eines Tages (--verify-day); bewusst ausserhalb des Monatsstores."""
    return (data_root / "metals" / "samples" / f"source={SOURCE}" / f"symbol={symbol}"
            / f"{symbol}-1m-{day:%Y-%m-%d}.parquet")


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

    @property
    def end(self) -> datetime:
        return self.t + (timedelta(hours=1) if self.kind == "ticks" else timedelta(days=1))

    def url(self) -> str:
        return (url_for(self.symbol, self.t) if self.kind == "ticks"
                else candle_url_for(self.symbol, self.t, self.side))

    def path(self, data_root: Path) -> Path:
        return (raw_path(data_root, self.symbol, self.t) if self.kind == "ticks"
                else candle_raw_path(data_root, self.symbol, self.t, self.side))

    def marker(self, data_root: Path, kind: str = "404") -> Path:
        """Nebendatei: 404 | empty | quarantine | corrupt."""
        return self.path(data_root).with_name(f"{self.name}.{kind}")

    def to_json(self) -> dict:
        return {"kind": self.kind, "date": f"{self.t:%Y-%m-%d}",
                "hour": self.t.hour if self.kind == "ticks" else None,
                "side": self.side or None}

    @classmethod
    def from_json(cls, symbol: str, d: dict) -> "RawFile":
        y, m, dd = map(int, d["date"].split("-"))
        return cls(symbol, d["kind"], datetime(y, m, dd, d.get("hour") or 0, tzinfo=UTC),
                   d.get("side") or "")


def month_files(symbol: str, mode: str, y: int, m: int) -> list[RawFile]:
    if mode == "ticks":
        return [RawFile(symbol, "ticks", h) for h in month_hours(y, m)]
    return [RawFile(symbol, "candles", d, s) for d in month_days(y, m) for s in SIDES]


def needs_confirmation(f: RawFile, kind: str) -> bool:
    """
    "Keine Daten" an einem Werktag (Mo-Fr des Datentags, ausser ganztaegigen
    Feiertagen 25.12., 1.1., Karfreitag) ist verdaechtig (Drosselung, noch
    nicht erzeugt) und muss in einem spaeteren Request bestaetigt werden.
    Tickmodus: nur 404 (leere Stunden sind werktags normal, z.B. Tagespause).
    """
    d = f.t.date()
    if d.weekday() >= 5 or full_holiday(d):
        return False
    return f.kind == "candles" or kind == "404"


# ------------------------------------------------------- atomar schreiben ---
def _tmp_name(dest: Path) -> Path:
    # Punkt vorn: von pyarrow/hive ignoriert; pid+thread: eindeutig je Schreiber
    return dest.with_name(f".{dest.name}.{os.getpid()}.{threading.get_ident()}.part")


def _fsync_dir(d: Path) -> None:
    fd = os.open(d, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _atomic_write(dest: Path, blob: bytes) -> None:
    """Temporaerdatei + fsync + os.replace + fsync des Verzeichnisses."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = _tmp_name(dest)
    with open(tmp, "wb") as fh:
        fh.write(blob)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, dest)
    _fsync_dir(dest.parent)


def write_json_atomic(dest: Path, obj: dict) -> None:
    _atomic_write(dest, (json.dumps(obj, indent=2, ensure_ascii=False) + "\n").encode())


def write_parquet(out: Path, df: pd.DataFrame, mode: str) -> None:
    """Atomar (versteckte Temp-Datei, fsync); Modus als Schema-Metadatum."""
    out.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(df, preserve_index=False)
    meta = dict(table.schema.metadata or {})
    meta[b"dukascopy_mode"] = mode.encode()
    table = table.replace_schema_metadata(meta)
    tmp = _tmp_name(out)
    pq.write_table(table, tmp, compression="zstd")
    with open(tmp, "rb") as fh:
        os.fsync(fh.fileno())
    os.replace(tmp, out)
    _fsync_dir(out.parent)


def cleanup_orphans(data_root: Path, symbol: str) -> int:
    """Verwaiste Temp-Dateien (abgebrochene Laeufe) in Partition und Reports entfernen."""
    n = 0
    sd = store_dir(data_root, symbol)
    cands = list(sd.rglob(".*.part")) + list(sd.rglob("*.parquet.part")) if sd.exists() else []
    rd = reports_dir(data_root)
    if rd.exists():
        # nur eigene Report-Namen: ".{SYM}-*" traefe auch die Temp-Dateien eines
        # gleichzeitig laufenden HistData-Laufs (.{SYM}-histdata-...)
        for pat in (f".{symbol}-1m-*.part", f".{symbol}-dukascopy-*.part", f".{symbol}-verify-*.part"):
            cands += list(rd.glob(pat))
    for p in cands:
        p.unlink(missing_ok=True)
        n += 1
    if n:
        log.warning("%d verwaiste Temp-Dateien entfernt", n)
    return n


def acquire_lock(data_root: Path, symbol: str):
    """Exklusiver flock je Symbol; None, wenn ein anderer Prozess ihn haelt."""
    p = lock_path(data_root, symbol)
    p.parent.mkdir(parents=True, exist_ok=True)
    fh = open(p, "a+")
    try:
        fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        fh.close()
        return None
    return fh


# ------------------------------------------------------- drosselung/backoff ---
def parse_retry_after(value: str | None, now: datetime | None = None) -> float | None:
    """Retry-After: Sekunden oder HTTP-Datum (ohne Zone = UTC) -> s >= 0; None wenn unlesbar."""
    if not value:
        return None
    try:
        v = float(value)
        return max(0.0, v) if v == v and v != float("inf") else None
    except ValueError:
        pass
    try:
        dt = parsedate_to_datetime(value)
        if dt is None:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return max(0.0, (dt - (now or utc_now())).total_seconds())
    except (TypeError, ValueError, IndexError, OverflowError):
        return None


def backoff_delay(n: int, base: float = BACKOFF_BASE_S, cap: float = MAX_BACKOFF_S,
                  jitter: float = JITTER, rng=None, retry_after: float | None = None) -> float:
    """
    Wartezeit nach dem n-ten Fehlschlag (n = 0, 1, ...): base * 2**n, mit
    Jitter nach unten (bis zu jitter-Anteil), gedeckelt auf cap. Ein
    Retry-After des Servers wird respektiert (Minimum), ebenfalls bis cap.
    """
    d = min(cap, base * 2 ** min(n, 60))
    if jitter:
        d *= 1.0 - jitter * (rng or random).random()
    if retry_after is not None:
        d = max(d, min(retry_after, cap))
    return d


class Throttle:
    """
    Gemeinsamer Zustand ALLER Worker:
    - "nicht vor t": vor jedem Request wird darauf gewartet
    - Zaehler aufeinanderfolgender Drossel-/Fehlerantworten (403/408/425/5xx,
      Timeouts, Verbindungsfehler; NICHT 429, s. RateLimitGate), ueber
      Dateigrenzen hinweg; jede setzt
      eine globale Abkuehlung base * 2**k (bis cap); Reset erst bei 200/404
    - Schutzschalter: nach `breaker` Fehlschlaegen in Folge wird `tripped`
      gesetzt -> keine neuen Requests, Lauf endet sauber (Exit 3)
    """

    def __init__(self, clock=time.monotonic, sleep=None, base: float = BACKOFF_BASE_S,
                 cap: float = MAX_BACKOFF_S, jitter: float = JITTER, rng=None,
                 breaker: int = MAX_CONSECUTIVE_FAILURES):
        self.clock, self._sleep = clock, sleep
        self.base, self.cap, self.jitter, self.rng, self.breaker = base, cap, jitter, rng, breaker
        self.lock = threading.Lock()
        self.not_before = float("-inf")
        self.consecutive = 0
        self.cooldowns = 0
        self.tripped = threading.Event()

    def sleep(self, s: float) -> None:
        (self._sleep or _sleep)(s)

    def success(self) -> None:
        with self.lock:
            self.consecutive = 0

    def failure(self, reason: str = "", retry_after: float | None = None) -> float:
        """Fehlschlag melden -> eskalierende globale Abkuehlung; gibt deren Dauer zurueck."""
        with self.lock:
            k = self.consecutive
            self.consecutive += 1
            d = backoff_delay(k, self.base, self.cap, self.jitter, self.rng, retry_after)
            nb = self.clock() + d
            if nb > self.not_before:
                self.not_before = nb
            self.cooldowns += 1
            if self.breaker and self.consecutive >= self.breaker:
                self.tripped.set()
        log.warning("globale Abkuehlung %.0fs fuer alle Worker (%s, %d in Folge)",
                    d, reason, k + 1)
        return d

    def cooldown(self, seconds: float, reason: str = "") -> None:
        with self.lock:
            nb = self.clock() + seconds
            if nb > self.not_before:
                self.not_before = nb
                self.cooldowns += 1

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
                 stop: threading.Event | None = None, on_response=None,
                 session=None, gate: RateLimitGate | None = None) -> bytes | None:
    """
    Laedt eine .bi5-Datei. None bei 404, b"" bei leerer Antwort.
    Bis zu `attempts` Versuche bei Netzfehlern/Timeouts, 403/408/425/5xx
    und nicht dekomprimierbarem Inhalt (abgeschnittene Uebertragung).
    Mit `throttle`: jede Drossel-/Fehlerantwort setzt eine eskalierende
    globale Abkuehlung (s. Throttle), 200/404 setzt sie zurueck; ohne
    `throttle`: lokaler Backoff base * 2**n. Andere HTTP-Fehler: kein Retry.

    HTTP 429: KEIN Versuch verbraucht, kein Throttle-Fehlschlag. `gate.hit`
    setzt die gemeinsame Sperre (max(Retry-After, min_pause)), danach wird
    dieselbe Datei erneut angefragt. Vor JEDEM Request `gate.wait()`. Ohne
    `gate` wird bei Bedarf eine lokale Sperre (ohne Persistenz) angelegt.
    Wirft DownloadError (mit .status, .attempts), Interrupted bzw.
    RateLimitExhausted (429-Wartebudget erschoepft).
    """
    sleep = sleep if sleep is not None else _sleep
    get = session.get if session is not None else requests.get
    err, status = None, None

    def halted() -> bool:
        return ((stop is not None and stop.is_set())
                or (throttle is not None and throttle.tripped.is_set()))

    n = 0                                   # verbrauchte Versuche (429 zaehlt nicht)
    while n < attempts:
        if halted():
            raise Interrupted(url)
        if throttle is not None:
            throttle.wait()
            if halted():
                raise Interrupted(url)
        # direkt vor dem Request: 429-Sperre (auch eines anderen Workers/Prozesses)
        if gate is not None and not gate.wait():
            raise Interrupted(url)
        retry_after, overload = None, False
        try:
            r = get(url, headers={"User-Agent": USER_AGENT}, timeout=timeout)
        except requests.RequestException as e:
            err, status, overload = f"{type(e).__name__}: {e}", None, True
            if gate is not None:
                gate.release()          # Antwort ohne 429: Probe nach Sperre beendet
            if on_response:
                on_response(None)
        else:
            status = r.status_code
            if on_response:
                on_response(status)
            if status == RATE_LIMIT_STATUS:
                ra = parse_retry_after((getattr(r, "headers", None) or {}).get("Retry-After"))
                if gate is None:
                    gate = RateLimitGate(None, sleep=sleep, stop=stop)
                pause = gate.hit(ra, url)
                log.info("429 %s: gleiche Datei nach %.0f min Pause erneut (Versuch %d/%d "
                         "nicht verbraucht)", url, pause / 60, n + 1, attempts)
                continue
            if gate is not None:
                # nach einer Sperre laesst gate.wait() nur EINEN Worker durch
                # (Probe); kommt die ohne 429 zurueck, duerfen alle wieder
                gate.release()
            if status == 404:
                if throttle is not None:
                    throttle.success()
                return None
            if status == 200:
                if throttle is not None:
                    throttle.success()
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
                overload = True
                retry_after = parse_retry_after((getattr(r, "headers", None) or {}).get("Retry-After"))
            else:
                raise DownloadError(f"HTTP {status}: {url}", status=status, attempts=n + 1)
        n += 1
        if overload and throttle is not None:
            # auch nach dem letzten Versuch: die anderen Worker sollen abkuehlen
            wait = throttle.failure(err, retry_after)
            if n < attempts:
                log.info("retry %d/%d nach globaler Abkuehlung %.1fs %s (%s)",
                         n, attempts - 1, wait, url, err)
                throttle.wait()
        elif n < attempts:
            wait = backoff_delay(n - 1, backoff_base, max_backoff, jitter, rng, retry_after)
            log.info("retry %d/%d in %.1fs %s (%s)", n, attempts - 1, wait, url, err)
            sleep(wait)
    raise DownloadError(f"{url}: {attempts} Versuche fehlgeschlagen ({err})",
                        status=status, attempts=attempts)


@dataclass
class Outcome:
    # cached | cached_404 | cached_empty | ok | 404 | empty | unconfirmed |
    # quarantined | failed | skipped | rate_limited
    status: str
    error: str | None = None
    attempts: int = 0
    http_status: int | None = None
    first_seen: str | None = None
    requested: bool = False     # echter Request (zaehlt fuer den Fortschritt)


def read_marker(p: Path) -> dict | None:
    """Marker lesen; alter 0-Byte-Marker (Vorversion) gilt als bestaetigt."""
    if not p.exists():
        return None
    try:
        txt = p.read_text().strip()
        return json.loads(txt) if txt else {"confirmed": "legacy", "first_seen": None}
    except (OSError, ValueError):
        return {"confirmed": "legacy", "first_seen": None}


def quarantine_reason(p: Path) -> str:
    try:
        detect_field_order(candle_records(p.read_bytes()))
        return "Quarantaene (Grund nicht reproduzierbar)"
    except (ValidationError, lzma.LZMAError, OSError) as e:
        return str(e)


class Fetcher:
    """
    Holt Rohdateien aus dem Cache oder per Download, gedrosselt (Pause je
    Worker, eskalierende globale Abkuehlung, Schutzschalter). Cache-Treffer
    sind keine Requests. Kerzendateien werden erst SETTLE_S nach Tagesende
    angefordert. Eine requests.Session (Keep-Alive) je Worker-Thread.
    Eine RateLimitGate (429) fuer alle Worker, persistiert unter
    ratelimit_state_path(data_root); `wall` = Wanduhr der Sperre (Tests).
    429-Wartebudget erschoepft -> `rate_limited` gesetzt, keine weiteren
    Requests (wie Schutzschalter, aber eigener Status).
    """

    def __init__(self, data_root: Path, *, workers: int = WORKERS, pause: float = PAUSE_S,
                 attempts: int = ATTEMPTS, backoff_base: float = BACKOFF_BASE_S,
                 max_backoff: float = MAX_BACKOFF_S, jitter: float = JITTER,
                 timeout: tuple[float, float] = (CONNECT_TIMEOUT_S, READ_TIMEOUT_S),
                 sleep=None, clock=None, rng=None, stop: threading.Event | None = None,
                 today: date | None = None, now=None,
                 max_consecutive_failures: int = MAX_CONSECUTIVE_FAILURES,
                 rate_limit_pause: float = RATE_LIMIT_PAUSE_S,
                 max_rate_limit_wait: float = MAX_RATE_LIMIT_WAIT_S,
                 wall=None, gate: RateLimitGate | None = None):
        if gate is None and rate_limit_pause < rl.MIN_PAUSE_S:
            raise ValueError(f"rate_limit_pause {rate_limit_pause:g}s < {rl.MIN_PAUSE_S:g}s")
        self.data_root = data_root
        self.workers, self.pause, self.attempts = max(1, workers), pause, attempts
        self.backoff_base, self.max_backoff, self.jitter = backoff_base, max_backoff, jitter
        self.timeout, self.rng = timeout, rng
        self._sleep = sleep
        self.stop = stop or threading.Event()
        self.today = today or utc_today()
        self.now = now or utc_now
        self.throttle = Throttle(clock or time.monotonic, sleep, backoff_base, max_backoff,
                                 jitter, rng, max_consecutive_failures)
        # Schlaf der Sperre ueber self.sleep: per CLI durch SIGTERM unterbrechbar,
        # in Tests injiziert (Wanduhr `wall` muss dann mitlaufen)
        self.gate = gate or RateLimitGate(
            ratelimit_state_path(data_root), min_pause=rate_limit_pause,
            max_total_wait=max_rate_limit_wait, wall=wall or time.time,
            sleep=self.sleep, stop=self.stop)
        self.rate_limited = threading.Event()
        self.rate_limit_error: str | None = None
        self.lock = threading.Lock()
        self.local = threading.local()
        self.requests = 0
        self.status_counts: dict[str, int] = {}
        rem = self.gate.remaining()
        if rem > 0:
            log.warning("429-Sperre aktiv (persistiert in %s): noch %.0f min bis %s UTC - "
                        "erster Request wartet so lange (Wartebudget %.1f h)",
                        self.gate.state_path, rem / 60,
                        self.gate.summary()["blocked_until_utc"], self.gate.max_total_wait / 3600)

    @property
    def halted(self) -> bool:
        return (self.stop.is_set() or self.throttle.tripped.is_set()
                or self.rate_limited.is_set())

    def sleep(self, s: float) -> None:
        (self._sleep or _sleep)(s)

    def session(self):
        s = getattr(self.local, "session", None)
        if s is None:
            s = self.local.session = requests.Session()
            s.headers["User-Agent"] = USER_AGENT
        return s

    def _count(self, status: int | None) -> None:
        with self.lock:
            self.requests += 1
            k = str(status) if status is not None else "netzfehler"
            self.status_counts[k] = self.status_counts.get(k, 0) + 1

    # ----------------------------------------------------------- zustand ---
    def state(self, f: RawFile) -> tuple[str, dict | None]:
        """
        data | 404 | empty (endgueltig) | pending (unbestaetigt, < 24 h) |
        recheck (unbestaetigt, >= 24 h) | quarantine | missing
        """
        if f.path(self.data_root).exists():
            return "data", None
        if f.marker(self.data_root, "quarantine").exists():
            return "quarantine", None
        for kind in NO_DATA:
            info = read_marker(f.marker(self.data_root, kind))
            if info is None:
                continue
            if info.get("confirmed"):
                return kind, info
            first = datetime.fromisoformat(info["first_seen"])
            if (self.now() - first).total_seconds() < CONFIRM_AFTER_S:
                return "pending", info
            return "recheck", info
        return "missing", None

    def get(self, f: RawFile) -> Outcome:
        if f.t.date() >= self.today or (
                f.kind == "candles" and self.now() < f.end + timedelta(seconds=SETTLE_S)):
            # Tagesdatei erst SETTLE_S nach Tagesende -> nie frueher anfordern/cachen
            raise ValueError(f"{f.key}: Datei des laufenden Tages / Tagesende < "
                             f"{SETTLE_S / 3600:g} h her")
        st, info = self.state(f)
        if st == "data":
            return Outcome("cached")
        if st in NO_DATA:
            return Outcome("cached_" + st)
        if st == "quarantine":
            return Outcome("quarantined", quarantine_reason(f.marker(self.data_root, "quarantine")))
        if st == "pending":
            return self._unconfirmed(f, info)
        if self.halted:
            return Outcome("skipped")

        t0 = time.monotonic()
        try:
            blob = download_bi5(
                f.url(), attempts=self.attempts, backoff_base=self.backoff_base,
                max_backoff=self.max_backoff, jitter=self.jitter, timeout=self.timeout,
                sleep=self.sleep, throttle=self.throttle, rng=self.rng, stop=self.stop,
                on_response=self._count, session=self.session(), gate=self.gate)
        except Interrupted:
            return Outcome("skipped")
        except RateLimitExhausted as e:
            with self.lock:
                first = not self.rate_limited.is_set()
                self.rate_limited.set()
                self.rate_limit_error = self.rate_limit_error or str(e)
            if first:
                log.error("429-WARTEBUDGET ERSCHOEPFT bei %s: %s - keine weiteren Requests, "
                          "Lauf endet mit Luecken (Exit 3)", f.key, e)
            return Outcome("rate_limited", str(e))
        except DownloadError as e:
            log.warning("FEHLSCHLAG %s: %s (%d Versuche, spaeter erneut)", f.key, e, e.attempts)
            out = Outcome("failed", str(e), e.attempts, e.status)
        else:
            out = self._store(f, blob, info if st == "recheck" else None, time.monotonic() - t0)
        out.requested = True
        if self.pause and not self.halted:
            self.sleep(self.pause)
        return out

    def _unconfirmed(self, f: RawFile, info: dict) -> Outcome:
        first = info["first_seen"]
        after = datetime.fromisoformat(first) + timedelta(seconds=CONFIRM_AFTER_S)
        return Outcome("unconfirmed", f"keine Daten an Werktag/einseitig, unbestaetigt; erneute "
                       f"Pruefung ab {after:%Y-%m-%d %H:%M} UTC", first_seen=first)

    def _clear_markers(self, f: RawFile) -> None:
        for kind in NO_DATA:
            f.marker(self.data_root, kind).unlink(missing_ok=True)

    def _store(self, f: RawFile, blob: bytes | None, prev: dict | None, dt: float) -> Outcome:
        if not blob:
            kind = "404" if blob is None else "empty"
            now = self.now().isoformat()
            if prev is not None:
                info = {"first_seen": prev.get("first_seen") or now, "confirmed": "repeat",
                        "confirmed_at": now}
            elif needs_confirmation(f, kind):
                info = {"first_seen": now, "confirmed": None}
            else:
                info = {"first_seen": now, "confirmed": "auto"}
            self._clear_markers(f)
            write_json_atomic(f.marker(self.data_root, kind), info)
            log.info("%-5s %s (keine Daten%s, %.1fs)", kind, f.key,
                     "" if info["confirmed"] else ", UNBESTAETIGT", dt)
            if not info["confirmed"]:
                return self._unconfirmed(f, info)
            return Outcome(kind, attempts=1, http_status=404 if blob is None else 200)
        if f.kind == "candles":
            try:
                order = detect_field_order(candle_records(blob))
            except FieldOrderError as e:
                # nicht raten, nicht endlos neu laden: Quarantaene, manuelle Pruefung
                _atomic_write(f.marker(self.data_root, "quarantine"), blob)
                log.error("QUARANTAENE %s: %s", f.key, e)
                return Outcome("quarantined", str(e), 1, 200)
            except ValidationError:
                order = "?"            # Formatfehler -> Validierungsfehler beim Monatsbau
            log.info("ok    %s (%d B, %.1fs, Reihenfolge %s)", f.key, len(blob), dt, order)
        else:
            log.info("ok    %s (%d B, %.1fs)", f.key, len(blob), dt)
        _atomic_write(f.path(self.data_root), blob)
        self._clear_markers(f)
        return Outcome("ok", attempts=1, http_status=200)

    def fetch(self, files: list[RawFile], progress: "Progress | None" = None) -> dict[RawFile, Outcome]:
        def one(f: RawFile) -> Outcome:
            o = self.get(f)
            if progress is not None and o.requested:
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
    Rueckgabe (status, blob): status in {cached, cached_404, cached_empty, ok,
    404, empty, unconfirmed}; blob None ausser bei Daten.
    """
    f = RawFile(symbol, "ticks", hour)
    o = Fetcher(data_root, pause=pause, today=date.max).get(f)
    if o.status == "failed":
        raise DownloadError(o.error or f.key, o.http_status, o.attempts)
    if o.status in ("cached", "ok"):
        return o.status, f.path(data_root).read_bytes()
    return o.status, None


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
    Feldreihenfolge der vier Preisfelder aus den Daten. Pruefung je
    Reihenfolge: high >= max(open, close) und low <= min(open, close) in
    ALLEN nicht flachen Kerzen.
      "OCLH" / "OHLC"  genau eine passt
      "ambiguous"      beide passen; das geht nur, wenn in jeder nicht
                       flachen Kerze close == high - dann liefern beide
                       Lesarten identische Werte (kein Raten noetig)
      "flat"           nur flache Kerzen (O=H=L=C), Reihenfolge egal
    Passt keine: FieldOrderError (Datei -> Quarantaene).
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
    if fits:
        return "ambiguous"
    raise FieldOrderError("Feldreihenfolge unklar: weder OCLH noch OHLC passt zu allen "
                          f"{int(nonflat.sum())} nicht flachen Kerzen")


def decode_candles(blob: bytes | None, day_start_ms: int, divisor: int,
                   order: str | None = None) -> tuple[pd.DataFrame, str]:
    """Kerzendatei eines Tages (eine Seite) -> (ts, open, high, low, close, volume), Reihenfolge."""
    rec = candle_records(blob)
    order = order or detect_field_order(rec)
    # "flat": alle Felder gleich; "ambiguous": close == high, beide Lesarten identisch
    fields = FIELD_ORDERS.get(order, FIELD_ORDERS["OCLH"])
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


class BuildIssues(Exception):
    """Probleme einzelner Rohdateien beim Monatsbau: [(art, RawFile, text)],
    art in corrupt | quarantine | one_sided. Monat wird nicht geschrieben."""

    def __init__(self, issues: list[tuple[str, RawFile, str]]):
        super().__init__(f"{len(issues)} Dateiprobleme")
        self.issues = issues


def _load(fetcher: "Fetcher", f: RawFile) -> tuple[str, bytes | None, dict | None]:
    """(zustand, blob, marker-info) einer erledigten Datei aus dem Cache."""
    st, info = fetcher.state(f)
    if st == "data":
        return st, f.path(fetcher.data_root).read_bytes(), None
    if st in NO_DATA:
        return st, None, info
    raise RuntimeError(f"{f.key}: nicht erledigt ({st})")


def build_month_ticks(fetcher: "Fetcher", symbol: str, y: int, m: int) -> tuple[pd.DataFrame, dict]:
    """Alle Tick-Stunden eines Monats aus dem Cache zu 1m-Bars verdichten."""
    divisor = POINT_DIVISOR[symbol]
    hours = month_hours(y, m)
    frames, issues = [], []
    stats = {"hours": len(hours), "hours_404": 0, "hours_empty": 0}
    for h in hours:
        f = RawFile(symbol, "ticks", h)
        st, blob, _ = _load(fetcher, f)
        if st != "data":
            stats["hours_404" if st == "404" else "hours_empty"] += 1
            continue
        if not blob:
            issues.append(("corrupt", f, "0-Byte-Rohdatei"))
            continue
        try:
            tk = decode_bi5(blob, int(h.timestamp() * 1000), divisor)
        except (lzma.LZMAError, EOFError) as e:
            issues.append(("corrupt", f, f"LZMA: {e}"))
            continue
        except ValidationError as e:
            raise ValidationError(f"{f.key} ({f.path(fetcher.data_root)}): {e}")
        if tk.empty:
            stats["hours_empty"] += 1
        else:
            frames.append(tk)
    if issues:
        raise BuildIssues(issues)
    if not frames:
        return aggregate_1m(pd.DataFrame()), stats
    return aggregate_1m(pd.concat(frames, ignore_index=True)), stats


def build_month_candles(fetcher: "Fetcher", symbol: str, y: int, m: int) -> tuple[pd.DataFrame, dict]:
    """
    Alle Kerzen-Tage eines Monats aus dem Cache: je Tag BID + ASK decodieren
    und zusammenfuehren. Tag mit Daten nur auf einer Seite: ist die fehlende
    Seite nur automatisch (Wochenende/Feiertag) als "keine Daten" bestaetigt
    -> Problem one_sided (wird unbestaetigt, Luecke); nach Wiederholungs-
    bestaetigung -> Tag verworfen und im Report gefuehrt.
    """
    divisor = POINT_DIVISOR[symbol]
    days = month_days(y, m)
    frames, issues = [], []
    stats: dict = {"days": len(days), "files": 2 * len(days), "files_404": 0, "files_empty": 0,
                   "days_404": 0, "days_empty": 0, "days_one_side_only": [],
                   "fill_minutes_dropped": 0, "one_side_minutes_dropped": 0,
                   "zero_volume_nonflat_minutes": 0, "field_order": {}}
    for d in days:
        sides: dict[str, pd.DataFrame | None] = {}
        nodata: dict[str, tuple[RawFile, dict | None]] = {}
        n_issues = len(issues)
        for s in SIDES:
            f = RawFile(symbol, "candles", d, s)
            st, blob, info = _load(fetcher, f)
            if st != "data":
                stats["files_404" if st == "404" else "files_empty"] += 1
                sides[s], nodata[s] = None, (f, info)
                continue
            if not blob:
                issues.append(("corrupt", f, "0-Byte-Rohdatei"))
                continue
            try:
                df, order = decode_candles(blob, int(d.timestamp() * 1000), divisor)
            except FieldOrderError as e:
                issues.append(("quarantine", f, str(e)))
                continue
            except (lzma.LZMAError, EOFError) as e:
                issues.append(("corrupt", f, f"LZMA: {e}"))
                continue
            except ValidationError as e:
                raise ValidationError(f"{f.key} ({f.path(fetcher.data_root)}): {e}")
            stats["field_order"][order] = stats["field_order"].get(order, 0) + 1
            sides[s] = df
        if len(issues) > n_issues:
            continue
        present = [s for s in SIDES if sides[s] is not None and not sides[s].empty]
        if not present:
            stats["days_404" if len(nodata) == 2 else "days_empty"] += 1
            continue
        if len(present) == 1:
            other = SIDES[1 - SIDES.index(present[0])]
            f2, info = nodata.get(other, (None, None))
            if f2 is not None and (info or {}).get("confirmed") == "auto":
                issues.append(("one_sided", f2, f"einseitiger Tag (nur {present[0]})"))
                continue
            stats["days_one_side_only"].append(f"{d:%Y-%m-%d} nur {present[0]}")
            log.warning("%s %s: Daten nur auf %s-Seite (bestaetigt), Tag verworfen",
                        symbol, f"{d:%Y-%m-%d}", present[0])
            continue
        bars, st = merge_candles(sides["BID"], sides["ASK"])
        for k, v in st.items():
            stats[k] += v
        if not bars.empty:
            frames.append(bars)
    if issues:
        raise BuildIssues(issues)
    if not frames:
        return pd.DataFrame(columns=COLUMNS), stats
    return pd.concat(frames, ignore_index=True), stats


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
    except (ValueError, KeyError, TypeError, AttributeError) as e:
        log.warning("Laufreport %s unlesbar (%s), ignoriert", p, e)
        return []


def run(symbol: str, start: date, end: date, data_root: Path | None = None,
        today: date | None = None, *, mode: str = "candles", workers: int = WORKERS,
        rebuild: bool = False, pause: float = PAUSE_S, attempts: int = ATTEMPTS,
        backoff_base: float = BACKOFF_BASE_S, max_backoff: float = MAX_BACKOFF_S,
        jitter: float = JITTER, final_retries: int = FINAL_RETRIES,
        final_wait: float = FINAL_WAIT_S,
        timeout: tuple[float, float] = (CONNECT_TIMEOUT_S, READ_TIMEOUT_S),
        progress_every: int = PROGRESS_EVERY,
        max_consecutive_failures: int = MAX_CONSECUTIVE_FAILURES,
        rate_limit_pause: float = RATE_LIMIT_PAUSE_S,
        max_rate_limit_wait: float = MAX_RATE_LIMIT_WAIT_S,
        sleep=None, clock=None, rng=None, now=None, wall=None,
        stop: threading.Event | None = None) -> dict:
    """
    Laedt alle abgeschlossenen Monate [start, end] und gibt den Laufreport
    zurueck (Feld exit_code, s. Docstring oben). Validierungsfehler werden je
    Monat gesammelt (Exit 2 am Ende). Der Laufreport wird immer geschrieben,
    ausser wenn ein anderer Prozess den Symbol-Lock haelt (Exit 3).
    """
    if symbol not in POINT_DIVISOR:
        raise ValueError(f"unbekanntes Symbol {symbol}; bekannt: {sorted(POINT_DIVISOR)}")
    if mode not in MODES:
        raise ValueError(f"unbekannter Modus {mode}; bekannt: {MODES}")
    data_root = data_root or default_data_root()
    today = today or utc_today()
    now = now or utc_now
    stop = stop or threading.Event()
    clock = clock or time.monotonic

    lock = acquire_lock(data_root, symbol)
    if lock is None:
        log.error("%s: Lock %s belegt (anderer Lauf aktiv) - spaeter erneut",
                  symbol, lock_path(data_root, symbol))
        return {"symbol": symbol, "mode": mode, "status": "locked", "exit_code": EXIT_GAPS}
    try:
        return _run_locked(symbol, start, end, data_root, today, now, stop, clock, mode=mode,
                           workers=workers, rebuild=rebuild, pause=pause, attempts=attempts,
                           backoff_base=backoff_base, max_backoff=max_backoff, jitter=jitter,
                           final_retries=final_retries, final_wait=final_wait, timeout=timeout,
                           progress_every=progress_every,
                           max_consecutive_failures=max_consecutive_failures,
                           rate_limit_pause=rate_limit_pause,
                           max_rate_limit_wait=max_rate_limit_wait,
                           sleep=sleep, rng=rng, wall=wall)
    finally:
        lock.close()


def _run_locked(symbol, start, end, data_root, today, now, stop, clock, *, mode, workers,
                rebuild, pause, attempts, backoff_base, max_backoff, jitter, final_retries,
                final_wait, timeout, progress_every, max_consecutive_failures,
                rate_limit_pause, max_rate_limit_wait, sleep, rng, wall) -> dict:
    cleanup_orphans(data_root, symbol)
    fetcher = Fetcher(data_root, workers=workers, pause=pause, attempts=attempts,
                      backoff_base=backoff_base, max_backoff=max_backoff, jitter=jitter,
                      timeout=timeout, sleep=sleep, clock=clock, rng=rng, stop=stop,
                      today=today, now=now, max_consecutive_failures=max_consecutive_failures,
                      rate_limit_pause=rate_limit_pause,
                      max_rate_limit_wait=max_rate_limit_wait, wall=wall)
    build = build_month_candles if mode == "candles" else build_month_ticks

    started = utc_now()
    months_todo: list[tuple[int, int]] = []
    skipped_existing = 0
    for y, m in months(start, end):
        # Laufender Monat wird NICHT geladen und NICHT geschrieben: eine
        # Teilmonatsdatei saehe aus wie eine fertige und wuerde nie ersetzt.
        # Dazu muss das Monatsende SETTLE_S her sein (letzte Tagesdatei).
        if (y, m) >= (today.year, today.month) or \
                now() < _month_range(y, m)[1] + timedelta(seconds=SETTLE_S):
            log.info("skip %s %d-%02d (Monat nicht abgeschlossen bzw. Ende < %g h her)",
                     symbol, y, m, SETTLE_S / 3600)
            continue
        if store_path(data_root, symbol, y, m).exists() and not rebuild:
            skipped_existing += 1
            continue
        months_todo.append((y, m))
    files_by_month = {ym: month_files(symbol, mode, *ym) for ym in months_todo}

    def done(f: RawFile) -> bool:
        return fetcher.state(f)[0] in ("data", *NO_DATA)

    pending_total = sum(1 for fs in files_by_month.values() for f in fs
                        if fetcher.state(f)[0] in ("missing", "recheck"))
    log.info("START %s mode=%s: %d Monate offen (%d fertig uebersprungen), %d von %d Dateien "
             "zu laden", symbol, mode, len(months_todo), skipped_existing, pending_total,
             sum(len(v) for v in files_by_month.values()))

    gaps: dict[RawFile, dict] = {}          # ungeloeste Download-Fehlschlaege
    unconfirmed: dict[RawFile, dict] = {}   # "keine Daten" noch nicht bestaetigt
    quarantine: dict[RawFile, str] = {}     # Feldreihenfolge: manuelle Pruefung
    validation_errors: dict[tuple[int, int], str] = {}
    counts = {"downloaded": 0, "404": 0, "empty": 0, "recovered_in_final_retries": 0}
    finished: dict[tuple[int, int], str] = {}   # Monat -> written | empty
    state = {"status": None, "error": None, "final_rounds": 0}

    def record(outs: dict[RawFile, Outcome], final: bool = False) -> None:
        for f, o in outs.items():
            if o.status == "failed":
                prev = gaps.get(f)
                gaps[f] = {"error": o.error, "http_status": o.http_status,
                           "attempts": (prev["attempts"] if prev else 0) + o.attempts}
                continue
            if o.status in ("skipped", "rate_limited"):
                continue
            if o.status in ("ok", "404", "empty"):
                counts["downloaded" if o.status == "ok" else o.status] += 1
            if gaps.pop(f, None) is not None and final:
                counts["recovered_in_final_retries"] += 1
            if o.status == "unconfirmed":
                unconfirmed[f] = {"reason": o.error, "first_seen": o.first_seen}
            else:
                unconfirmed.pop(f, None)
            if o.status == "quarantined":
                quarantine[f] = o.error or "Feldreihenfolge unklar"

    def handle_issues(ym, issues) -> None:
        for kind, f, msg in issues:
            p = f.path(data_root)
            if kind == "corrupt":
                os.replace(p, f.marker(data_root, "corrupt"))
                gaps[f] = {"error": f"Cache-Datei defekt ({msg}), beiseitegelegt "
                           f"({f.name}.corrupt), wird neu geladen", "http_status": None,
                           "attempts": gaps.get(f, {}).get("attempts", 0)}
            elif kind == "quarantine":
                os.replace(p, f.marker(data_root, "quarantine"))
                quarantine[f] = msg
                log.error("QUARANTAENE %s: %s", f.key, msg)
            else:   # one_sided: "keine Daten" der Gegenseite muss bestaetigt werden
                first = now().isoformat()
                for k in NO_DATA:
                    mp = f.marker(data_root, k)
                    if mp.exists():
                        write_json_atomic(mp, {"first_seen": first, "confirmed": None,
                                               "reason": msg})
                unconfirmed[f] = {"reason": f"{msg}: fehlende Seite unbestaetigt",
                                  "first_seen": first}
        log.warning("%s %d-%02d nicht geschrieben: %s", symbol, *ym,
                    "; ".join(f"{k} {f.key}" for k, f, _ in issues))

    def finish(ym: tuple[int, int]) -> None:
        y, m = ym
        label = f"{symbol} {y}-{m:02d}"
        try:
            df, stats = build(fetcher, symbol, y, m)
            if df.empty:
                log.warning("skip %s (keine Daten; z.B. vor Beginn der Historie)", label)
                finished[ym] = "empty"
                return
            ms = pd.Timestamp(y, m, 1, tz="UTC")
            rep = validate(df, symbol, label, ms, ms + pd.offsets.MonthBegin(1))
        except BuildIssues as e:
            handle_issues(ym, e.issues)
            return
        except ValidationError as e:
            # gesammelt; uebrige Monate laufen weiter, Exit 2 am Ende
            validation_errors[ym] = str(e)
            log.error("VALIDIERUNG FEHLGESCHLAGEN %s: %s", label, e)
            return
        rep.update(stats)
        rep["mode"] = mode
        out = store_path(data_root, symbol, y, m)
        rep["file"] = str(out.relative_to(data_root))
        write_parquet(out, df, mode)
        write_json_atomic(report_path(data_root, symbol, y, m), rep)
        finished[ym] = "written"
        log.info("%-15s %6d Bars  gekreuzt=%-3d spread>20x=%-4d spruenge=%-3d "
                 "MoFr-ohne-Ticks=%d  mode=%s", label, rep["rows"], rep["crossed"],
                 rep["spread_outliers"], rep["price_jumps"],
                 rep["weekday_minutes_without_ticks"], mode)
        if rep["spread_outliers"] or rep["price_jumps"]:
            log.warning("%s: Auffaelligkeiten, siehe %s", label, report_path(data_root, symbol, y, m))

    def complete(ym) -> bool:
        return (ym not in finished and ym not in validation_errors
                and all(done(f) for f in files_by_month[ym])
                and not any(f.month == ym for f in gaps))

    try:
        progress = Progress(pending_total, progress_every, clock)
        prev = [f for f in load_previous_gaps(data_root, symbol, mode)
                if f.month in files_by_month and not done(f)]
        if prev:
            log.info("%d Dateien aus der Lueckenliste des letzten Laufs zuerst", len(prev))
            record(fetcher.fetch(prev, progress))
        for ym in months_todo:
            if fetcher.halted:
                break
            todo = [f for f in files_by_month[ym] if f not in gaps]   # Fehlschlaege: am Ende
            record(fetcher.fetch(todo, progress))
            if complete(ym):
                finish(ym)
            elif not fetcher.halted:
                log.warning("%s %d-%02d unvollstaendig - nicht geschrieben", symbol, *ym)

        for r in range(1, final_retries + 1):
            if not gaps or fetcher.halted:
                break
            state["final_rounds"] = r
            log.warning("WIEDERHOLUNG %d/%d: %d Fehlschlaege nach %.0fs Pause", r, final_retries,
                        len(gaps), final_wait)
            fetcher.sleep(final_wait)
            if fetcher.halted:
                break
            retry = sorted(gaps, key=lambda f: (f.t, f.side))
            record(fetcher.fetch(retry, Progress(len(retry), progress_every, clock,
                                                 f"Runde {r}: ")), final=True)
            for ym in months_todo:
                if complete(ym):
                    finish(ym)
    except Exception as e:
        state["status"], state["error"] = "error", f"{type(e).__name__}: {e}"
        raise
    finally:
        rep = _run_report(symbol, mode, start, end, started, data_root, fetcher, months_todo,
                          files_by_month, finished, validation_errors, gaps, unconfirmed,
                          quarantine, counts, state, skipped_existing, pending_total, dict(
                              workers=workers, pause=pause, attempts=attempts,
                              backoff_base=backoff_base, max_backoff=max_backoff,
                              final_retries=final_retries, final_wait=final_wait,
                              connect_timeout=timeout[0], read_timeout=timeout[1],
                              max_consecutive_failures=max_consecutive_failures,
                              rate_limit_pause=rate_limit_pause,
                              max_rate_limit_wait=max_rate_limit_wait, rebuild=rebuild))
        write_json_atomic(run_report_path(data_root, symbol), rep)
        rls = rep["summary"]["rate_limit"]
        log.info("FERTIG %s: status=%s exit=%d, %d Monate geschrieben, %d Luecken, "
                 "%d unbestaetigt, %d Quarantaene, %d offen, %d Requests, 429: %d (%.0f min "
                 "gewartet) (Report %s)", symbol, rep["status"], rep["exit_code"],
                 len(rep["summary"]["months_written"]), len(gaps), len(unconfirmed),
                 len(quarantine), len(rep["pending"]), fetcher.requests, rls["hits_429"],
                 rls["waited_s"] / 60, run_report_path(data_root, symbol))
    return rep


def _run_report(symbol, mode, start, end, started, data_root, fetcher, months_todo,
                files_by_month, finished, validation_errors, gaps, unconfirmed, quarantine,
                counts, state, skipped_existing, pending_total, options) -> dict:
    def ymstr(ym) -> str:
        return f"{ym[0]}-{ym[1]:02d}"

    q_months = {f.month for f in quarantine}
    blocked_q = [ym for ym in months_todo if ym not in finished and ym not in validation_errors
                 and ym in q_months and not any(f.month == ym for f in (*gaps, *unconfirmed))
                 and all(fetcher.state(f)[0] in ("data", *NO_DATA, "quarantine")
                         for f in files_by_month[ym])]
    incomplete = [ym for ym in months_todo
                  if ym not in finished and ym not in validation_errors and ym not in blocked_q]
    tripped = fetcher.throttle.tripped.is_set()
    rate_limited = fetcher.rate_limited.is_set()
    halt_reason = ("circuit_breaker" if tripped else "rate_limit" if rate_limited
                   else "interrupted" if fetcher.stop.is_set() else None)
    # nicht mehr angefragte Dateien (Lauf vorzeitig beendet): fuer den Report
    pending = [] if halt_reason is None else [
        f for ym in months_todo for f in files_by_month[ym]
        if f not in gaps and f not in unconfirmed
        and fetcher.state(f)[0] in ("missing", "recheck")]
    # Vorrang 3 vor 4 BEWUSST: die systemd-Units starten bei 3 neu, bei 4
    # nicht - ein Exit 4 bei offenen Luecken wuerde den Backfill anhalten.
    # Damit die Quarantaene trotzdem nicht untergeht: status "...+quarantine",
    # Liste "quarantined" oben im Report und eine ERROR-Zeile in JEDEM Lauf.
    quarantined = sorted({ymstr(f.month) for f in quarantine})
    if state["status"] == "error":
        status, code = "error", 1
    elif validation_errors:
        status, code = "validation_error", EXIT_VALIDATION
    elif gaps or unconfirmed or incomplete:
        status = ("circuit_breaker" if tripped else "rate_limited" if rate_limited
                  else "interrupted" if fetcher.stop.is_set() else "gaps")
        code = EXIT_GAPS
    elif blocked_q:
        status, code = "quarantine", EXIT_MANUAL
    else:
        status, code = "complete", EXIT_OK
    if quarantined and code != EXIT_MANUAL:
        status += "+quarantine"
    if quarantined:
        log.error("QUARANTAENE %s (%s): Monate %s - %d Datei(en), manuelle Pruefung noetig "
                  "(Report: quarantine)", symbol, mode, ", ".join(quarantined), len(quarantine))

    hints = []
    n403 = fetcher.status_counts.get("403", 0)
    if fetcher.requests > 0 and n403 >= max(1, WAF_SHARE * fetcher.requests):
        hints.append(f"ueberwiegend HTTP 403 ({n403}/{fetcher.requests} Requests): vermutlich "
                     "WAF/Bot-Schutz des Datafeeds. --pause erhoehen, --workers 1, spaeter "
                     "erneut starten (Cache bleibt).")
    if tripped:
        hints.append(f"Schutzschalter: {fetcher.throttle.consecutive} Fehlschlaege in Folge - "
                     "Lauf beendet, Neustart spaeter (systemd) setzt fort.")
    if rate_limited:
        hints.append(f"HTTP 429 (Rate-Limit pro IP), Wartebudget erschoepft "
                     f"({fetcher.rate_limit_error}): {len(pending)} Dateien offen (pending). "
                     f"Sperre persistiert in {fetcher.gate.state_path}; Neustart spaeter "
                     "(systemd) wartet sie ab und setzt fort.")
    if quarantine:
        hints.append("Dateien in Quarantaene (Feldreihenfolge): manuell pruefen, z.B. "
                     "--verify-day; Quarantaenedatei loeschen = erneut laden.")
    for h in hints:
        log.error(h)

    return {
        "symbol": symbol, "mode": mode, "status": status, "exit_code": code,
        "quarantined": quarantined,
        "started": started.isoformat(), "finished": utc_now().isoformat(),
        "range": {"start": f"{start:%Y-%m}", "end": f"{end:%Y-%m}"},
        "options": options,
        "summary": {
            "months_considered": len(months_todo),
            "months_skipped_existing": skipped_existing,
            "months_written": sorted(ymstr(k) for k, v in finished.items() if v == "written"),
            "months_empty": sorted(ymstr(k) for k, v in finished.items() if v == "empty"),
            "months_incomplete": [ymstr(k) for k in incomplete],
            "months_quarantine": [ymstr(k) for k in blocked_q],
            "months_validation_error": [ymstr(k) for k in sorted(validation_errors)],
            "files_total": sum(len(v) for v in files_by_month.values()),
            "files_pending_at_start": pending_total,
            "files_downloaded": counts["downloaded"],
            "files_404": counts["404"],
            "files_empty": counts["empty"],
            "requests": fetcher.requests,
            "http_status_counts": dict(sorted(fetcher.status_counts.items())),
            "http_403_waf_suspected": n403,
            "global_cooldowns": fetcher.throttle.cooldowns,
            "circuit_breaker": tripped,
            "rate_limited": rate_limited,
            "rate_limit": fetcher.gate.summary(),
            "pending_files": len(pending),
            "final_retry_rounds": state["final_rounds"],
            "recovered_in_final_retries": counts["recovered_in_final_retries"],
            "failed_files": len(gaps),
            "unconfirmed_files": len(unconfirmed),
            "quarantine_files": len(quarantine),
            "interrupted": fetcher.stop.is_set(),
        },
        "hint": " | ".join(hints) or None,
        "error": state["error"],
        "validation_errors": [{"month": ymstr(k), "error": v}
                              for k, v in sorted(validation_errors.items())],
        "gaps": [
            {"symbol": symbol, "mode": mode, "month": f"{f.t:%Y-%m}", "file": f.key,
             **f.to_json(), "url": f.url(), "http_status": g["http_status"],
             "last_error": g["error"], "attempts": g["attempts"]}
            for f, g in sorted(gaps.items(), key=lambda kv: (kv[0].t, kv[0].side))],
        "unconfirmed": [
            {"symbol": symbol, "mode": mode, "month": f"{f.t:%Y-%m}", "file": f.key,
             **f.to_json(), "first_seen": u["first_seen"], "reason": u["reason"]}
            for f, u in sorted(unconfirmed.items(), key=lambda kv: (kv[0].t, kv[0].side))],
        "quarantine": [
            {"symbol": symbol, "mode": mode, "month": f"{f.t:%Y-%m}", "file": f.key,
             **f.to_json(), "path": str(f.marker(data_root, "quarantine")), "reason": r}
            for f, r in sorted(quarantine.items(), key=lambda kv: (kv[0].t, kv[0].side))],
        # vorzeitig beendet (429-Budget, Schutzschalter, SIGTERM): nicht angefragt
        "pending": [
            {"symbol": symbol, "mode": mode, "month": f"{f.t:%Y-%m}", "file": f.key,
             **f.to_json(), "url": f.url(), "reason": halt_reason}
            for f in sorted(pending, key=lambda f: (f.t, f.side))],
    }


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
               *, tol_points: float = 0.5, max_share: float = 0.0, candles_only: bool = False,
               now=None, **fetch_kw) -> dict:
    """
    Prueft einen abgeschlossenen Tag und schreibt den Verify-Report.

    Standard: laedt 24 Tick-Stunden und BID/ASK-Kerzen (ueber den Cache, bis
    zu 26 Requests), aggregiert die Ticks zu Minuten und vergleicht OHLC je
    Seite. exit_code: 0 ok, 3 Download gescheitert/429-Budget/Lock belegt,
    4 Abweichung, keine Daten oder Kerzendatei in Quarantaene.

    candles_only: NUR BID- und ASK-Kerzen (2 Requests, wenn nicht im Cache),
    Feldreihenfolge bestimmen, decodieren, Plausibilitaet pruefen (harte
    Monatschecks aus validate() inkl. PLAUSIBLE/Jahresanker/gekreuzte Anteile,
    Median-Spread, genug Minuten an Werktagen). exit_code: 0 ok, 3 Download
    gescheitert/429-Budget/Lock belegt/"keine Daten" unbestaetigt, 4
    Quarantaene oder unplausibel.

    Beide Varianten schreiben die zusammengefuehrten Kerzen-Minuten des Tages
    (Store-Schema) nach sample_path(), NUR wenn die Pruefung bestanden ist
    (ok, harte Store-Checks); Pfad im Report (sample_path, sonst None).
    """
    data_root = data_root or default_data_root()
    today = today or utc_today()
    now = now or utc_now
    d0 = datetime(day.year, day.month, day.day, tzinfo=UTC)
    if day >= today or now() < d0 + timedelta(days=1, seconds=SETTLE_S):
        raise ValueError(f"--verify-day {day}: nur abgeschlossene UTC-Tage, Tagesende mind. "
                         f"{SETTLE_S / 3600:g} h her (heute {today})")
    lock = acquire_lock(data_root, symbol)
    if lock is None:
        log.error("%s: Lock belegt (anderer Lauf aktiv) - spaeter erneut", symbol)
        return {"symbol": symbol, "day": f"{day}", "status": "locked", "ok": False,
                "exit_code": EXIT_GAPS}
    try:
        return _verify_locked(symbol, day, d0, data_root, today, now, tol_points, max_share,
                              candles_only, fetch_kw)
    finally:
        lock.close()


def _good_friday(y: int) -> date:
    """Karfreitag (gregorianische Osterformel, Meeus/Jones/Butcher)."""
    a, b, c = y % 19, y // 100, y % 100
    d, e = b // 4, b % 4
    g = (8 * b + 13) // 25
    h = (19 * a + b - d - g + 15) % 30
    i, k = c // 4, c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month, day = divmod(h + l - 7 * m + 114, 31)
    return date(y, month, day + 1) - timedelta(days=2)


def full_holiday(d: datetime | date) -> bool:
    """Ganztaegige Handelsfeiertage der Metalle: 25.12., 1.1., Karfreitag."""
    dd = d.date() if isinstance(d, datetime) else d
    return (dd.month, dd.day) in ((12, 25), (1, 1)) or dd == _good_friday(dd.year)


def _is_trading_weekday(d: datetime) -> bool:
    """Mo-Fr ohne ganztaegige Feiertage (dort keine Mindest-Minutenzahl)."""
    return d.weekday() < 5 and not full_holiday(d)


def _day_checks(symbol: str, d0: datetime, bars: pd.DataFrame) -> tuple[dict, list[str], bool]:
    """
    Plausibilitaet der Kerzen-Minuten eines Tages -> (info, probleme, store_ok).
    store_ok: harte Store-Checks (validate) bestanden -> als Stichprobe nutzbar.
    """
    info: dict = {"minutes": int(len(bars)), "trading_weekday": _is_trading_weekday(d0),
                  "min_weekday_minutes": MIN_WEEKDAY_MINUTES, "max_spread_rel": MAX_SPREAD_REL,
                  "plausible_range": list(PLAUSIBLE[symbol])}
    if bars.empty:
        return info, ["keine Minuten"], False
    ds = pd.Timestamp(d0)
    try:
        v = validate(bars, symbol, f"{symbol} {d0:%Y-%m-%d}", ds, ds + pd.Timedelta(days=1))
    except ValidationError as e:
        return info, [str(e)], False
    info.update({k: x for k, x in v.items() if k not in ("label", "symbol", "rows")})
    info["price_min"] = float(bars[BID + ASK].min().min())
    info["price_max"] = float(bars[BID + ASK].max().max())
    info["crossed_share"] = v["crossed"] / len(bars)
    mid = float(((bars["bid_close"] + bars["ask_close"]) / 2).median())
    info["spread_median_rel"] = v["spread_median"] / mid if mid > 0 else None
    problems = []
    if v["spread_median"] <= 0:
        problems.append(f"Median-Spread {v['spread_median']:g} <= 0")
    elif info["spread_median_rel"] is None or info["spread_median_rel"] > MAX_SPREAD_REL:
        problems.append(f"Median-Spread {v['spread_median']:g} = {info['spread_median_rel']:.2%} "
                        f"vom Mid (> {MAX_SPREAD_REL:.0%})")
    if info["trading_weekday"] and len(bars) < MIN_WEEKDAY_MINUTES:
        problems.append(f"nur {len(bars)} Minuten an einem Werktag (< {MIN_WEEKDAY_MINUTES})")
    return info, problems, True


def _verify_locked(symbol, day, d0, data_root, today, now, tol_points, max_share, candles_only,
                   fetch_kw):
    divisor = POINT_DIVISOR[symbol]
    tick_files = [] if candles_only else [RawFile(symbol, "ticks", d0 + timedelta(hours=h))
                                          for h in range(24)]
    candle_files = [RawFile(symbol, "candles", d0, s) for s in SIDES]
    files = tick_files + candle_files
    fetcher = Fetcher(data_root, today=today, now=now, **fetch_kw)
    if not candles_only:
        todo = sum(1 for f in files if fetcher.state(f)[0] in ("missing", "recheck"))
        log.warning("--verify-day mit Tickvergleich kostet bis zu 26 Requests (24 Tick-Stunden + "
                    "2 Kerzendateien), davon %d nicht im Cache. Unter Rate-Limit (429): "
                    "--candles-only (2 Requests)", todo)
    outs = fetcher.fetch(files)
    rep: dict = {"symbol": symbol, "day": f"{day}",
                 "variant": "candles_only" if candles_only else "ticks_vs_candles",
                 "sample_path": None}
    failed = {f.key: o.error for f, o in outs.items()
              if o.status in ("failed", "skipped", "rate_limited")}
    quarantined = {f.key: o.error for f, o in outs.items() if o.status == "quarantined"}
    sp = sample_path(data_root, symbol, day)

    def blob_of(f: RawFile) -> bytes | None:
        p = f.path(data_root)
        return p.read_bytes() if p.exists() else None    # 404/leer (auch unbestaetigt)

    if failed:
        rate_limited = fetcher.rate_limited.is_set()
        rep.update(ok=False, exit_code=EXIT_GAPS, status="rate_limited" if rate_limited else "failed",
                   failed=failed,
                   pending=[{"file": f.key, "url": f.url(),
                             "reason": "rate_limit" if rate_limited else "failed"}
                            for f in files if fetcher.state(f)[0] in ("missing", "recheck")])
    elif quarantined:
        rep.update(ok=False, exit_code=EXIT_MANUAL, status="quarantine", quarantine=quarantined,
                   field_order={f.side: "keine passt (Quarantaene)" for f in candle_files
                                if f.key in quarantined})
        sp.unlink(missing_ok=True)
    else:
        sides, orders = {}, {}
        for f in candle_files:
            blob = blob_of(f)
            sides[f.side], orders[f.side] = decode_candles(blob, int(d0.timestamp() * 1000),
                                                           divisor)
            if blob is None:
                orders[f.side] = "keine Daten"
        candle_bars, st = merge_candles(sides["BID"], sides["ASK"])
        rep["field_order"] = orders
        rep.update(st)
        info, problems, store_ok = _day_checks(symbol, d0, candle_bars)
        present = [s for s in SIDES if not sides[s].empty]
        one_side_unconfirmed = False
        if len(present) == 1:
            problems.insert(0, f"Daten nur auf {present[0]}-Seite")
            store_ok = False
            other = next(f for f in candle_files if f.side != present[0])
            # fehlende Seite nur unbestaetigt leer -> Luecke (spaeter erneut), keine Abweichung
            one_side_unconfirmed = outs[other].status == "unconfirmed"
        rep["plausibility"] = info
        rep["plausibility_problems"] = problems
        if candles_only:
            rep["minutes_candles"] = int(len(candle_bars))
            if not present:
                unconf = any(outs[f].status == "unconfirmed" for f in candle_files)
                rep.update(ok=False, status="no_data",
                           exit_code=EXIT_GAPS if unconf else EXIT_VERIFY,
                           note=("keine Daten an einem Werktag, unbestaetigt - spaeter erneut"
                                 if unconf else
                                 "keine Daten: ganztaegiger Feiertag - Handelstag waehlen"
                                 if full_holiday(d0) else "keine Daten an diesem Tag (Wochenende/"
                                 "Feiertag?) - Werktag waehlen"))
            elif one_side_unconfirmed:
                rep.update(ok=False, status="one_side_unconfirmed", exit_code=EXIT_GAPS,
                           note=f"nur {present[0]}-Seite, Gegenseite unbestaetigt leer - "
                                "spaeter erneut (>= 24 h)")
            else:
                rep["ok"] = not problems
                rep["status"] = "ok" if rep["ok"] else "implausible"
                rep["exit_code"] = EXIT_OK if rep["ok"] else EXIT_VERIFY
        else:
            frames = []
            for f in tick_files:
                blob = blob_of(f)
                if blob:
                    frames.append(decode_bi5(blob, int(f.t.timestamp() * 1000), divisor))
            tick_bars = aggregate_1m(pd.concat(frames, ignore_index=True) if frames
                                     else pd.DataFrame())
            rep.update(compare_bars(tick_bars, candle_bars, divisor, tol_points))
            if len(candle_bars):
                rep["continuity_candles"] = continuity(candle_bars, divisor)
            if len(tick_bars):
                rep["continuity_ticks"] = continuity(tick_bars.reset_index(drop=True), divisor)
            no_data = rep["minutes_ticks"] == 0 and rep["minutes_candles"] == 0
            rep["ok"] = (not no_data) and rep["mismatch_share"] <= max_share
            rep["max_share"] = max_share
            rep["status"] = "ok" if rep["ok"] else "no_data" if no_data else "mismatch"
            if no_data:
                rep["note"] = "keine Daten an diesem Tag (Wochenende/Feiertag?) - Werktag waehlen"
            rep["exit_code"] = EXIT_OK if rep["ok"] else EXIT_VERIFY
        # Stichprobe NUR bei bestandener Pruefung (ok): der Cross-Check liest die
        # Datei ohne den Report; eine Datei aus einem gescheiterten Lauf waere
        # nicht von einer guten zu unterscheiden. Sonst keine veraltete stehen lassen.
        if rep.get("ok") and store_ok:
            write_parquet(sp, candle_bars, "candles")
            rep["sample_path"] = str(sp)
        else:
            sp.unlink(missing_ok=True)
    rep["requests"] = fetcher.requests
    rep["http_status_counts"] = dict(sorted(fetcher.status_counts.items()))
    rep["rate_limit"] = fetcher.gate.summary()
    write_json_atomic(verify_report_path(data_root, symbol, day), rep)
    if candles_only:
        pl = rep.get("plausibility") or {}
        log.info("VERIFY %s %s (nur Kerzen): ok=%s status=%s Reihenfolge=%s Minuten=%s "
                 "Spread-Median=%s Probleme=%s Requests=%d Stichprobe=%s", symbol, day,
                 rep.get("ok"), rep.get("status"), rep.get("field_order"), pl.get("minutes"),
                 pl.get("spread_median"), rep.get("plausibility_problems"), fetcher.requests,
                 rep["sample_path"])
    else:
        log.info("VERIFY %s %s: ok=%s Reihenfolge=%s Minuten ticks=%s kerzen=%s gemeinsam=%s "
                 "abweichend=%s nur-ticks=%s nur-kerzen=%s max-dev(Punkte)=%s Stichprobe=%s",
                 symbol, day, rep.get("ok"), rep.get("field_order"), rep.get("minutes_ticks"),
                 rep.get("minutes_candles"), rep.get("minutes_common"),
                 rep.get("minutes_deviating"), rep.get("minutes_only_ticks"),
                 rep.get("minutes_only_candles"), rep.get("max_abs_dev_points"),
                 rep["sample_path"])
    if rep["rate_limit"]["hits_429"]:
        log.warning("429: %d Treffer, %.0f min gewartet", rep["rate_limit"]["hits_429"],
                    rep["rate_limit"]["waited_s"] / 60)
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
                   help=f"Backoff-/Abkuehlungs-Basis in s, Faktor 2 (default {BACKOFF_BASE_S})")
    p.add_argument("--max-backoff", type=float, default=MAX_BACKOFF_S,
                   help=f"Deckel Backoff/Abkuehlung/Retry-After in s (default {MAX_BACKOFF_S})")
    p.add_argument("--max-consecutive-failures", type=int, default=MAX_CONSECUTIVE_FAILURES,
                   help="Schutzschalter: nach N Fehlantworten in Folge Lauf beenden, Exit 3 "
                        f"(default {MAX_CONSECUTIVE_FAILURES}, 0 = aus)")
    p.add_argument("--rate-limit-pause", type=float, default=RATE_LIMIT_PAUSE_S,
                   help="Pause nach HTTP 429 fuer alle Worker/Prozesse in s, mind. "
                        f"{RATE_LIMIT_PAUSE_S:g} (default {RATE_LIMIT_PAUSE_S:g}); ein laengerer "
                        "Retry-After gilt; 429 verbraucht keinen Versuch")
    p.add_argument("--max-rate-limit-wait", type=float, default=MAX_RATE_LIMIT_WAIT_S,
                   help="429-Wartebudget je Lauf in s; danach keine Requests mehr, offene "
                        f"Dateien als pending, Exit 3 (default {MAX_RATE_LIMIT_WAIT_S:g})")
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
                   help="Kerzen gegen aus Ticks gebildete Minuten pruefen (ein Tag, bis zu "
                        "26 Requests); schreibt zusaetzlich die Tagesbars als Stichprobe")
    p.add_argument("--candles-only", action="store_true",
                   help="mit --verify-day: nur BID+ASK-Kerzen laden (2 Requests) und auf "
                        "Plausibilitaet pruefen statt gegen Ticks")
    p.add_argument("--verify-tol", type=float, default=0.5,
                   help="erlaubte Abweichung je OHLC-Wert in Punkten (default 0.5 = exakt)")
    p.add_argument("--verify-max-share", type=float, default=0.0,
                   help="erlaubter Anteil abweichender Minuten (default 0.0)")
    p.add_argument("-v", "--verbose", action="store_true")
    a = p.parse_args(argv)
    if a.workers < 1 or a.attempts < 1 or a.final_retries < 0 or a.max_consecutive_failures < 0 \
            or min(a.pause, a.backoff_base, a.max_backoff, a.final_wait,
                   a.max_rate_limit_wait) < 0:
        p.error("--workers/--attempts >= 1, uebrige Zahlen >= 0")
    if not a.rate_limit_pause >= RATE_LIMIT_PAUSE_S:
        p.error(f"--rate-limit-pause muss >= {RATE_LIMIT_PAUSE_S:g} s sein (Rate-Limit pro IP: "
                "kuerzere Pausen verlaengern die Sperre nur)")
    if a.candles_only and not a.verify_day:
        p.error("--candles-only nur zusammen mit --verify-day")

    logging.basicConfig(
        level=logging.DEBUG if a.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
    )
    stop = threading.Event()
    install_stop_handler(stop)
    kw = dict(workers=a.workers, pause=a.pause, attempts=a.attempts,
              backoff_base=a.backoff_base, max_backoff=a.max_backoff,
              max_consecutive_failures=a.max_consecutive_failures,
              rate_limit_pause=a.rate_limit_pause, max_rate_limit_wait=a.max_rate_limit_wait,
              timeout=(a.connect_timeout, a.read_timeout), stop=stop,
              sleep=lambda s: stop.wait(s))       # Schlaf durch SIGTERM unterbrechbar

    if a.verify_day:
        try:
            day = date.fromisoformat(a.verify_day)
            return verify_day(a.symbol, day, tol_points=a.verify_tol,
                              max_share=a.verify_max_share, candles_only=a.candles_only,
                              **kw)["exit_code"]
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
    rep = run(a.symbol, date(sy, sm, 1), date(ey, em, 1), mode=a.mode, rebuild=a.rebuild,
              final_retries=a.final_retries, final_wait=a.final_wait,
              progress_every=a.progress_every, **kw)
    if rep["exit_code"] == EXIT_GAPS and rep.get("summary"):
        log.error("LUECKEN: %d fehlgeschlagen, %d unbestaetigt, %d offen (pending), %d Monate "
                  "unvollstaendig - Folgelauf holt nur fehlende Dateien nach",
                  rep["summary"]["failed_files"], rep["summary"]["unconfirmed_files"],
                  rep["summary"].get("pending_files", 0), len(rep["summary"]["months_incomplete"]))
    return rep["exit_code"]


if __name__ == "__main__":
    sys.exit(main())
