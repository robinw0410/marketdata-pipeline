#!/usr/bin/env python3
"""
Historische HistData.com-Daten (XAUUSD, XAGUSD, EURUSD) als 1-Minuten-Bars
mit Bid- UND Ask-OHLC laden. Default-Quelle fuer den Gold/Silber-Bot
(Dukascopy drosselt per IP mit HTTP 429, s. fetch_dukascopy.py).

Gleiches Ausgabeformat wie fetch_dukascopy.py (Spalten, dtypes, atomares
Schreiben, Exit-Codes); der Bot unterscheidet die Quellen nur an der
Partition source=histdata.

- Modus "tick" (Default): Generic-ASCII-Ticks je Monat
  ("YYYYMMDD HHMMSSmmm,bid,ask,volume"), selbst zu 1m-Bid/Ask-OHLC verdichtet
  (fetch_dukascopy.aggregate_1m), tick_count = echte Tickzahl. Spread bleibt
  erhalten. Partition source=histdata.
- Modus "m1": Generic-ASCII-Minutenbars ("YYYYMMDD HHMMSS;o;h;l;c;v"), NUR
  Bid. ask_* = bid_* (Spread 0), tick_count = 1; Parquet-Metadatum
  spread_available = false - den Spread liefert dann das Kostenmodell des
  Bots. EIGENE Partition source=histdata_m1 (der Bot liest nur
  source=histdata/dukascopy), damit Bid-only-Monate nie mit Tick-Monaten
  gemischt werden. Vergangene Jahre nur als Jahres-ZIP, laufendes Jahr monatlich.
  Eine vorhandene Monatsdatei mit anderem histdata_mode wird nie ueberschrieben.

Download (keine offizielle API; Ablauf wie PyPI histdata 1.1, histdatacom,
histdata-fetcher, s. docs/histdata_research.md): GET der Periodenseite
  https://www.histdata.com/download-free-forex-historical-data/?/ascii/
      {tick-data-quotes|1-minute-bar-quotes}/{sym}/{YYYY}[/{M}]
-> verstecktes Formular id="file_down" (tk, date, datemonth, platform,
timeframe, fxpair) -> POST https://www.histdata.com/get.php mit Referer =
Periodenseite -> ZIP (CSV + Status-.txt mit Lueckenliste). Leerer Body =
keine Daten. Je Datei 2 Requests, immer nur einer gleichzeitig. 429 auf den
POST -> nach der Pause wieder ab dem Seiten-GET (frisches Token).

Zeitzone (RAW_TZ, je Modus; Rohzeit = Wanduhr-Ziffern der Datei):
- tick: America/New_York MIT Sommerzeit (EST -5 h / EDT -4 h) - entgegen der
  HistData-Angabe "EST WITHOUT Day Light Savings". Gemessen an echten
  XAUUSD-Ticks (2024-07: Tagespause 16:59:58 -> 18:00:01, Wochenschluss Fr
  16:59:58, Wochenstart So 18:00:01 = New-York-Sessionzeiten, alle 38 Anker
  bei -4 h; 2024-01 bei -5 h), s. docs/histdata_research.md.
- m1: Etc/GMT+5 = fest UTC-5 laut HistData-Angabe, fuer M1 NICHT verifiziert:
  mit --mode m1 --verify-month 2024-07 (Sommermonat) pruefen.
Umrechnung raw_to_utc_ms (vektorisiert, exakt auf die Millisekunde). Ortszeiten
in der Sommerzeit-Luecke (z.B. 2024-03-10 02:00-02:59:59.999) oder der
doppelten Stunde (z.B. 2024-11-03 01:00-01:59:59.999) werden NIE geraten:
TimezoneError -> Quarantaene (Exit 4); der Markt ist dann ohnehin zu. Folge:
UTC-Monat M braucht die Rohmonate M-1 (dessen letzte 4 h in EDT bzw. 5 h in
EST; aus RAW_TZ berechnet) und M; er gilt nur als vollstaendig, wenn beide da
sind (oder M-1 vor Historienbeginn bzw. bestaetigt leer ist).
Die Zeitzone wird JE ROHMONAT aus den Daten geprueft (check_timezone):
Wochenschluss Freitag 17:00 New York, Wochenstart Sonntag 18:00 (Metalle)
bzw. 17:00 (EURUSD), bei Metallen zusaetzlich die Tagespause 17:00-18:00
New York (zoneinfo America/New_York; nur Luecken, die hoechstens 3 h vom
erwarteten Pausenbeginn entfernt liegen - duenne Maerkte haben nachts
weitere Luecken). Daraus der implizite Offset (Rohzeit - UTC) je Ankerpunkt,
verglichen mit dem in diesem Zeitpunkt erwarteten Offset von RAW_TZ[mode]
(tick: -4 h in EDT, -5 h in EST; m1: immer -5 h). Abweichung = implizit -
erwartet: Median 0, Anteil 0 und Laengster-Abweichungs-Lauf muessen passen,
in den ersten/letzten 72 h des Rohmonats darf KEIN Anker um genau 1 h
abweichen (Uhrumstellung am Monatsrand), und kein Zeitstempel darf in
Dateireihenfolge um >= 30 min zurueckspringen. Zu wenige Anker -> gemeinsam
mit dem Nachbar-Rohmonat pruefen. Beginnt der (gecachte) Folge-Rohmonat
bereits mit 1 h Abweichung, wird auch Monat M nicht geschrieben (Umstellung
nach dem letzten Anker von M). Sonst QUARANTAENE (Exit 4), es wird nichts
verschoben geschrieben.
Parquet-Metadatum source_timezone = SOURCE_TZ_META[mode]. Vorhandene
Monatsdateien desselben Modus mit anderem source_timezone (alte Version: fest
UTC-5 auch fuer Ticks, Sommermonate um 1 h verschoben) werden im naechsten
Lauf automatisch aus dem Roh-Cache neu gebaut (WARNING, Laufreport
"rebuilt_timezone"). Quarantaene-Marker tragen raw_tz und tz_check_version;
Marker einer aelteren Pruefversion (ohne diese Felder) werden entfernt und der
Rohmonat ohne Download neu geprueft - ausser sie sind "accepted": true.

Ablage (DATA_ROOT = $MARKETDATA_DATA_ROOT, sonst <repo>/data), SRC =
histdata (tick) bzw. histdata_m1 (m1):
    metals/source={SRC}/symbol={SYM}/year={YYYY}/{SYM}-1m-{YYYY}-{MM}.parquet
    metals/raw/source=histdata/symbol={SYM}/mode={tick|m1}/HISTDATA_COM_ASCII_{SYM}_{T|M1}{YYYY[MM]}.zip
        Roh-Cache; daneben Marker (JSON):
        <zip>.empty       keine Daten (first_seen, last_checked, confirmed,
                          confirmed_at; "accepted": true = Luecke innerhalb der
                          Historie nach manueller Pruefung akzeptiert)
        <zip>[.YYYY-MM].quarantine  Zeitzone/Format; loeschen = erneut pruefen
                          (ohne Download); "accepted": true = nach manueller
                          Pruefung freigeben; ein neuer Download (ZIP loeschen)
                          entfernt den Marker und prueft neu; Marker aelterer
                          tz_check_version werden automatisch neu geprueft
        <zip>.failed      dauerhafter Inhaltsfehler (kein Formular, HTML statt
                          ZIP, falsche CSV); erneuter Versuch fruehestens 24 h spaeter
        <zip>.corrupt     beiseitegelegtes kaputtes ZIP
    metals/reports/{SYM}-{SRC}-1m-{YYYY}-{MM}.json   Validierungsreport je Monat
    metals/reports/{SYM}-{SRC}-run.json              Laufreport (immer)
    metals/reports/{SYM}-{SRC}-verify-{YYYY-MM}.json Ergebnis --verify-month
    metals/state/histdata-ratelimit.json             429-Sperre (alle Prozesse)
    metals/.lock-histdata-{SYM}                      Prozess-Lock je Symbol

Exit-Codes (Vorrang in dieser Reihenfolge, wie fetch_dukascopy.py):
  2  Validierungsfehler (mindestens ein Monat; uebrige Monate liefen weiter)
  3  Luecken: Download gescheitert, Inhaltsfehler (24 h zurueckgestellt),
     Monat noch nicht veroeffentlicht (pending), unbestaetigte "keine
     Daten", 3 leere Antworten in Folge innerhalb der Historie, 429-Budget
     erschoepft, Schutzschalter, SIGTERM, Symbol-Lock belegt -> spaeter erneut
  4  manuelle Pruefung noetig: Quarantaene (Zeitzone, Format), bestaetigt
     leerer Monat INNERHALB der Historie, Monatsdatei mit anderem Modus
  0  vollstaendig
  3 geht bewusst vor 4: die systemd-Units starten bei 3 neu, bei 4 nicht -
  ein Exit 4 bei offenen Luecken wuerde den Backfill anhalten. Quarantaene
  ist trotzdem nicht zu uebersehen: ERROR-Zeile in jedem Lauf, Laufreport
  status "...+quarantine" und Liste "quarantined" auf oberster Ebene.

Lizenz: HistData-Nutzungsbedingungen selbst pruefen. Daten (roh wie
abgeleitet) nicht weitergeben und nicht committen (data/ ist git-ignored).

Usage:
    python fetch_histdata.py --symbol XAUUSD --start 2009-03
    python fetch_histdata.py --symbol EURUSD --start 2024-01 --end 2024-06 --mode m1
    python fetch_histdata.py --symbol XAUUSD --verify-month 2024-03
    python fetch_histdata.py --symbol XAUUSD --mode m1 --verify-month 2024-07   # M1-Zeitzone pruefen
"""
from __future__ import annotations

import argparse
import fcntl
import functools
import io
import logging
import math
import os
import re
import sys
import threading
import time
import zipfile
from collections import Counter, OrderedDict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
import requests

import fetch_dukascopy as dk
import ratelimit as rl

SOURCE = "histdata"
SOURCE_BY_MODE = {"tick": "histdata", "m1": "histdata_m1"}   # Partition + Spalte source
BASE = "https://www.histdata.com"
PAGE_URL = BASE + "/download-free-forex-historical-data/?/ascii/{tf}/{pair}/{year}"
GET_URL = BASE + "/get.php"
# Generischer Browser-artiger User-Agent (nie eine E-Mail-Adresse). Ob
# HistData Nicht-Browser-Agenten abweist, ist NICHT verifiziert.
USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) marketdata-pipeline/1.0"
UTC = timezone.utc
NY = ZoneInfo("America/New_York")

SYMBOLS = ("XAUUSD", "XAGUSD", "EURUSD")
METALS = ("XAUUSD", "XAGUSD")
MODES = ("tick", "m1")
TF_PATH = {"tick": "tick-data-quotes", "m1": "1-minute-bar-quotes"}
TF_CODE = {"tick": "T", "m1": "M1"}
PARQUET_MODE = {"tick": "histdata_tick", "m1": "histdata_m1"}

# Erster Monat laut HistData-Auswahlliste (ASCII M1; Quellen: histdatacom
# data/.repo, ai-trader QA). Fuer TICKS nicht verifiziert - fehlen dort
# fruehe Monate, werden sie als "keine Daten" bestaetigt (24 h) und gelistet.
HISTORY_START = {"XAUUSD": (2009, 3), "XAGUSD": (2009, 5), "EURUSD": (2000, 5)}

HOUR_MS, MIN_MS, DAY_MS = 3_600_000, 60_000, 86_400_000

# Zeitzone der Rohzeitstempel je Modus (zoneinfo-Namen). Tick: New-York-
# Ortszeit MIT Sommerzeit (gemessen an echten Daten, s. docs/histdata_research.md).
# M1: weiter fest UTC-5 ("Etc/GMT+5" = UTC-5, Vorzeichen POSIX-verdreht) laut
# HistData-Angabe - fuer M1 NICHT verifiziert, mit --mode m1 --verify-month pruefen.
RAW_TZ = {"tick": "America/New_York", "m1": "Etc/GMT+5"}
SOURCE_TZ_META = {"tick": "America/New_York (mit Sommerzeit) -> UTC, je Monat geprueft",
                  "m1": "Etc/GMT+5 (fest UTC-5) -> UTC, je Monat geprueft"}
# Version der Zeitzonen-Hypothese/Pruefung; steht in jedem Quarantaene-Marker.
# Marker einer aelteren Version (oder anderer Zeitzone) werden ignoriert und
# der Rohmonat neu geprueft (v1 = fest UTC-5 fuer beide Modi).
TZ_CHECK_VERSION = 2

# ---- Drosselung / Wiederholung (Defaults, alle per CLI aenderbar) ----------
PAUSE_S = 5.0              # Mindestabstand zwischen zwei Requests
ATTEMPTS = 5               # Versuche je Datei (429 zaehlt NICHT)
BACKOFF_BASE_S = 10.0      # Abkuehlung nach k Fehlschlaegen in Folge: base * 2**k
MAX_BACKOFF_S = 600.0
JITTER = 0.5
CONNECT_TIMEOUT_S = 10.0
READ_TIMEOUT_S = 120.0     # Tick-ZIPs sind einige 10 MB gross
MAX_CONSECUTIVE_FAILURES = 10
RETRY_STATUS = {403, 408, 425}   # + alle 5xx; 403 vermutlich WAF
SETTLE_S = dk.SETTLE_S           # Rohmonatsende muss so lange her sein
CONFIRM_AFTER_S = dk.CONFIRM_AFTER_S
PUBLICATION_LAG_DAYS = 45        # bis dahin: leer = "noch nicht veroeffentlicht"
CONTENT_RETRY_S = 24 * 3600      # dauerhafter Inhaltsfehler: erst danach erneut
MAX_CONTENT_ERRORS_IN_ROW = 3    # so viele Inhaltsfehler in Folge -> Lauf beenden
MAX_EMPTY_IN_ROW = 3             # so viele leere Antworten in Folge (in der Historie) -> Lauf beenden
EMPTY_RECHECK_DAYS = 30          # bestaetigt leere Monate in der Historie danach erneut pruefen

# ---- Zeitzonen-Pruefung -----------------------------------------------------
# Offset = Rohzeit - UTC in Stunden; erwartet je Anker aus RAW_TZ[mode],
# Abweichung = impliziter - erwarteter Offset (soll 0 sein)
TZ_MIN_ANCHORS = 3
TZ_MIN_SHARE = 0.6         # Anteil der Ankerpunkte mit Abweichung 0
TZ_MAX_RUN = 3             # so viele aufeinanderfolgende gleich abweichende Anker = Regimewechsel
TZ_EDGE_MS = 72 * HOUR_MS  # Monatsrand: hier ist schon EIN Anker mit +-1 h fatal
TZ_BREAK_WINDOW_H = 3      # Tagespausen-Anker nur hoechstens so weit vom erwarteten Beginn
REGRESSION_MAX_MS = 30 * MIN_MS  # Ruecksprung in Dateireihenfolge ab dem -> Uhrumstellung
WEEKEND_GAP_MS = 24 * HOUR_MS
BREAK_GAP_MS = (45 * MIN_MS, 3 * HOUR_MS)
CLOSE_H_NY = 17
OPEN_H_NY = {"XAUUSD": 18, "XAGUSD": 18, "EURUSD": 17}

EXIT_OK, EXIT_VALIDATION, EXIT_GAPS, EXIT_MANUAL = dk.EXIT_OK, dk.EXIT_VALIDATION, dk.EXIT_GAPS, dk.EXIT_MANUAL
N_EXAMPLES = 10

log = logging.getLogger("histdata")


class FormatError(Exception):
    """CSV/ZIP-Inhalt nicht lesbar (Format geaendert?) -> Quarantaene."""


class TimezoneError(FormatError):
    """Rohzeit in der Quell-Zeitzone nicht eindeutig (Sommerzeit-Luecke oder
    doppelte Stunde) -> nie raten, Rohmonat in Quarantaene."""


class CorruptZip(Exception):
    """Kaputtes ZIP (BadZipFile, CRC) -> beiseitelegen, neu laden."""


class WrongContent(CorruptZip):
    """ZIP ohne die erwartete CSV (fehlt, mehrere, falscher Name) - dauerhaft."""


class ContentError(Exception):
    """Antwort ist kein brauchbares ZIP (HTML-Fehlerseite, Captcha, WAF,
    fehlendes/falsches Formular) - nie Daten, dauerhafter Fehler."""


class ContentFailure(dk.DownloadError):
    """Dauerhafter Inhaltsfehler einer Datei: 1 Versuch je Lauf, kein Schutzschalter."""


class RetryableError(Exception):
    pass


class Restart(Exception):
    """429 auf den POST: Token womoeglich abgelaufen -> ab Seiten-GET neu (kein Versuch)."""


class Quarantined(Exception):
    def __init__(self, unit: "Unit", ym: tuple[int, int], reason: str, check: dict | None = None):
        super().__init__(reason)
        self.unit, self.ym, self.reason, self.check = unit, ym, reason, check


# ------------------------------------------------------------------ pfade ---
def store_dir(data_root: Path, symbol: str, mode: str = "tick") -> Path:
    return data_root / "metals" / f"source={SOURCE_BY_MODE[mode]}" / f"symbol={symbol}"


def retire_stale_store_file(data_root: Path, out: Path) -> Path:
    """Store-Datei mit alter Zeitzonen-Annahme nach metals/stale/... verschieben
    (ausserhalb jeder source=-Partition, der Bot liest sie nie). Nichts wird
    geloescht; der Zeitstempel im Namen verhindert Ueberschreiben."""
    rel = out.relative_to(data_root / "metals")
    dest = data_root / "metals" / "stale" / rel.parent / (
        f"{out.name}.{datetime.now(UTC):%Y%m%dT%H%M%S%f}.stale")
    dest.parent.mkdir(parents=True, exist_ok=True)
    os.replace(out, dest)
    dk._fsync_dir(out.parent)
    return dest


def store_path(data_root: Path, symbol: str, y: int, m: int, mode: str = "tick") -> Path:
    return store_dir(data_root, symbol, mode) / f"year={y}" / f"{symbol}-1m-{y}-{m:02d}.parquet"


def raw_dir(data_root: Path, symbol: str, mode: str) -> Path:
    return data_root / "metals" / "raw" / f"source={SOURCE}" / f"symbol={symbol}" / f"mode={mode}"


def report_path(data_root: Path, symbol: str, y: int, m: int, mode: str = "tick") -> Path:
    return dk.reports_dir(data_root) / f"{symbol}-{SOURCE_BY_MODE[mode]}-1m-{y}-{m:02d}.json"


def run_report_path(data_root: Path, symbol: str, mode: str = "tick") -> Path:
    return dk.reports_dir(data_root) / f"{symbol}-{SOURCE_BY_MODE[mode]}-run.json"


def verify_report_path(data_root: Path, symbol: str, y: int, m: int, mode: str = "tick") -> Path:
    return dk.reports_dir(data_root) / f"{symbol}-{SOURCE_BY_MODE[mode]}-verify-{y}-{m:02d}.json"


def lock_path(data_root: Path, symbol: str) -> Path:
    return data_root / "metals" / f".lock-histdata-{symbol}"


def ratelimit_state_path(data_root: Path) -> Path:
    return data_root / "metals" / "state" / "histdata-ratelimit.json"


def _ym_add(ym: tuple[int, int], k: int) -> tuple[int, int]:
    i = ym[0] * 12 + ym[1] - 1 + k
    return i // 12, i % 12 + 1


def _naive_ms(y: int, m: int, d: int = 1) -> int:
    return int(datetime(y, m, d, tzinfo=UTC).timestamp() * 1000)


def raw_month_bounds_ms(ym: tuple[int, int]) -> tuple[int, int]:
    """Rohmonat [start, ende) in Rohzeit-ms (Ziffern wie in der Datei, als UTC gelesen)."""
    return _naive_ms(*ym), _naive_ms(*_ym_add(ym, 1))


# ----------------------------------------------------- zeitzonen-umrechnung ---
def _utcoff_ms(tz: ZoneInfo, s: int) -> int:
    return int(datetime.fromtimestamp(s, tz).utcoffset().total_seconds() * 1000)


@functools.lru_cache(maxsize=None)
def _year_transitions(tzname: str, year: int) -> tuple[tuple[int, int], ...]:
    """Offset-Wechsel im UTC-Jahr: ((utc_ms ab dem gilt, offset_ms), ...).
    Tagesweise abgetastet, dann sekundengenau halbiert (hoechstens ein
    Wechsel je Tag; Wechsel liegen auf ganzen Sekunden)."""
    tz = ZoneInfo(tzname)
    s0 = int(datetime(year, 1, 1, tzinfo=UTC).timestamp())
    s1 = int(datetime(year + 1, 1, 1, tzinfo=UTC).timestamp())
    out, prev = [], _utcoff_ms(tz, s0)
    for a in range(s0, s1, 86400):
        b = min(a + 86400, s1)
        if _utcoff_ms(tz, b) == prev:
            continue
        lo, hi = a, b                           # off(lo) == prev != off(hi)
        while hi - lo > 1:
            mid = (lo + hi) // 2
            if _utcoff_ms(tz, mid) == prev:
                lo = mid
            else:
                hi = mid
        prev = _utcoff_ms(tz, hi)
        out.append((hi * 1000, prev))
    return tuple(out)


@functools.lru_cache(maxsize=64)
def _tz_table(tzname: str, y0: int, y1: int) -> tuple[np.ndarray, np.ndarray]:
    """(T, O): ab UTC-ms T[i] gilt Offset O[i] (T[0] = -inf) fuer die UTC-Jahre y0..y1."""
    base = _utcoff_ms(ZoneInfo(tzname), int(datetime(y0, 1, 1, tzinfo=UTC).timestamp()))
    tr = [t for y in range(y0, y1 + 1) for t in _year_transitions(tzname, y)]
    T = np.array([np.iinfo(np.int64).min] + [t for t, _ in tr], dtype=np.int64)
    O = np.array([base] + [o for _, o in tr], dtype=np.int64)
    return T, O


def _ms_year(ms: int) -> int:
    return int(np.datetime64(int(ms), "ms").astype("datetime64[Y]").astype(np.int64)) + 1970


def _table_for(tzname: str, ms: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    lo, hi = int(ms.min()) - 2 * DAY_MS, int(ms.max()) + 2 * DAY_MS   # Offsets < 1 Tag
    return _tz_table(tzname, _ms_year(lo), _ms_year(hi))


def utc_to_raw_ms(utc_ms: np.ndarray, mode: str) -> np.ndarray:
    """UTC-ms -> Rohzeit-ms (Ortszeit RAW_TZ[mode], Ziffern als UTC gelesen); immer eindeutig."""
    utc_ms = np.asarray(utc_ms, dtype=np.int64)
    if not len(utc_ms):
        return utc_ms.copy()
    T, O = _table_for(RAW_TZ[mode], utc_ms)
    return utc_ms + O[np.searchsorted(T, utc_ms, side="right") - 1]


def raw_to_utc_ms(raw_ms: np.ndarray, mode: str) -> np.ndarray:
    """
    Rohzeit-ms (Wanduhr-Ziffern in RAW_TZ[mode], als UTC gelesen) -> UTC-ms
    (int64, gleiche Reihenfolge/Laenge). Vektorisiert: je moeglichem Offset
    der Zone (America/New_York: -5 h, -4 h) Kandidat u = roh - offset bilden;
    gueltig, wenn die Zone im Zeitpunkt u genau diesen Offset hat
    (searchsorted in der Wechseltabelle, exakt auf die Millisekunde).
    Kein gueltiger Kandidat = Ortszeit existiert nicht (Sommerzeitbeginn,
    z.B. 2024-03-10 02:00-02:59:59.999), zwei = mehrdeutig (Sommerzeitende,
    z.B. 2024-11-03 01:00-01:59:59.999) -> TimezoneError mit Beispielen; nie
    raten (der Markt ist dann zu, ein Tick dort heisst: etwas stimmt nicht).
    """
    raw_ms = np.asarray(raw_ms, dtype=np.int64)
    if not len(raw_ms):
        return raw_ms.copy()
    tzname = RAW_TZ[mode]
    T, O = _table_for(tzname, raw_ms)
    cands = np.unique(O)
    if len(cands) == 1:                         # feste Zone (Etc/GMT+5)
        return raw_ms - cands[0]
    out = np.zeros(len(raw_ms), dtype=np.int64)
    n_ok = np.zeros(len(raw_ms), dtype=np.int8)
    for off in cands:
        u = raw_ms - off
        ok = O[np.searchsorted(T, u, side="right") - 1] == off
        out[ok] = u[ok]
        n_ok += ok
    if (n_ok != 1).any():
        msg = []
        for mask, what in ((n_ok == 0, "nicht existierender Ortszeit (Sommerzeitbeginn)"),
                           (n_ok > 1, "mehrdeutiger Ortszeit (Sommerzeitende)")):
            idx = np.nonzero(mask)[0]
            if len(idx):
                ex = ", ".join(_raw_iso(int(raw_ms[i]), ms_digits=True) for i in idx[:5])
                msg.append(f"{len(idx)} Zeitstempel in {what} {tzname}, z.B. {ex}")
        raise TimezoneError("; ".join(msg) + " - Markt ist dann geschlossen, nicht umgerechnet")
    return out


def tz_offset_h(utc_ms: int, mode: str) -> float:
    """Offset (Ortszeit - UTC) von RAW_TZ[mode] im UTC-Zeitpunkt utc_ms, in Stunden."""
    return _utcoff_ms(ZoneInfo(RAW_TZ[mode]), utc_ms // 1000) / HOUR_MS


def raw_month_end_utc(ym: tuple[int, int], mode: str = "tick") -> datetime:
    """Ende des Rohmonats ym in UTC (Monatsende Ortszeit; nie in einer Umstellung)."""
    end = raw_to_utc_ms(np.array([raw_month_bounds_ms(ym)[1]]), mode)[0]
    return datetime.fromtimestamp(int(end) / 1000, UTC)


def before_history(symbol: str, ym: tuple[int, int]) -> bool:
    return ym < HISTORY_START[symbol]


@dataclass(frozen=True)
class Unit:
    """Eine HistData-Datei: Monat (tick, m1 laufendes Jahr) oder Jahr (m1, month=None)."""
    symbol: str
    mode: str
    year: int
    month: int | None = None

    @property
    def period(self) -> str:
        return f"{self.year}{self.month:02d}" if self.month else f"{self.year}"

    @property
    def code(self) -> str:
        return TF_CODE[self.mode]

    @property
    def name(self) -> str:
        return f"HISTDATA_COM_ASCII_{self.symbol}_{self.code}{self.period}.zip"

    @property
    def member(self) -> str:
        return f"DAT_ASCII_{self.symbol}_{self.code}_{self.period}.csv"

    @property
    def key(self) -> str:
        return f"{self.symbol} {self.mode} {self.period}"

    @property
    def first_month(self) -> tuple[int, int]:
        return self.year, self.month or 1

    def raw_bounds_ms(self) -> tuple[int, int]:
        if self.month:
            return raw_month_bounds_ms((self.year, self.month))
        return _naive_ms(self.year, 1), _naive_ms(self.year + 1, 1)

    @property
    def end_utc(self) -> datetime:
        """Ende der Rohperiode in UTC (Ortszeit RAW_TZ[mode] -> UTC)."""
        end = raw_to_utc_ms(np.array([self.raw_bounds_ms()[1]]), self.mode)[0]
        return datetime.fromtimestamp(int(end) / 1000, UTC)

    def page_url(self) -> str:
        url = PAGE_URL.format(tf=TF_PATH[self.mode], pair=self.symbol.lower(), year=self.year)
        return url + (f"/{self.month}" if self.month else "")

    def path(self, data_root: Path) -> Path:
        return raw_dir(data_root, self.symbol, self.mode) / self.name

    def marker(self, data_root: Path, kind: str) -> Path:
        return self.path(data_root).with_name(f"{self.name}.{kind}")

    def qmarker(self, data_root: Path, ym: tuple[int, int]) -> Path:
        """Quarantaene je Rohmonat (bei Jahresdateien mit Monatssuffix)."""
        suffix = "" if self.month else f".{ym[0]}-{ym[1]:02d}"
        return self.path(data_root).with_name(f"{self.name}{suffix}.quarantine")

    def to_json(self) -> dict:
        return {"unit": self.key, "file": self.name, "url": self.page_url()}


def unit_for(symbol: str, mode: str, ym: tuple[int, int], current_raw_year: int,
             data_root: Path | None = None) -> Unit:
    """Tick: immer Monat. M1: Monat im laufenden Jahr (oder wenn schon gecached), sonst Jahr."""
    monthly = Unit(symbol, mode, ym[0], ym[1])
    if mode == "tick" or ym[0] >= current_raw_year:
        return monthly
    if data_root is not None and monthly.path(data_root).exists():
        return monthly
    return Unit(symbol, mode, ym[0], None)


# ------------------------------------------------------- atomar schreiben ---
def write_parquet(out: Path, df: pd.DataFrame, mode: str) -> None:
    """Atomar wie fetch_dukascopy.write_parquet; Metadaten: Modus, Spread, Zeitzone."""
    out.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(df, preserve_index=False)
    meta = dict(table.schema.metadata or {})
    meta[b"histdata_mode"] = PARQUET_MODE[mode].encode()
    meta[b"spread_available"] = b"true" if mode == "tick" else b"false"
    meta[b"source_timezone"] = SOURCE_TZ_META[mode].encode()
    table = table.replace_schema_metadata(meta)
    tmp = dk._tmp_name(out)
    pq.write_table(table, tmp, compression="zstd")
    with open(tmp, "rb") as fh:
        os.fsync(fh.fileno())
    os.replace(tmp, out)
    dk._fsync_dir(out.parent)


def _stored_meta(path: Path, key: bytes) -> str | None:
    try:
        meta = pq.read_schema(path).metadata or {}
    except (OSError, pa.ArrowException):
        return None
    v = meta.get(key)
    return v.decode("utf-8", "replace") if v else None


def stored_mode(path: Path) -> str | None:
    """histdata_mode einer vorhandenen Monatsdatei (None: fehlt/unlesbar)."""
    return _stored_meta(path, b"histdata_mode")


def stored_timezone(path: Path) -> str | None:
    """source_timezone einer vorhandenen Monatsdatei (None: fehlt/unlesbar)."""
    return _stored_meta(path, b"source_timezone")


def cleanup_orphans(data_root: Path, symbol: str) -> int:
    """Verwaiste Temp-Dateien (.*.part) in Partitionen, Roh-Cache und eigenen Reports."""
    cands: list[Path] = []
    for d in (*(store_dir(data_root, symbol, m) for m in MODES),
              *(raw_dir(data_root, symbol, m) for m in MODES)):
        if d.exists():
            cands += list(d.rglob(".*.part"))
    rd = dk.reports_dir(data_root)
    if rd.exists():
        cands += list(rd.glob(f".{symbol}-histdata*-*.part"))
    for p in cands:
        p.unlink(missing_ok=True)
    if cands:
        log.warning("%d verwaiste Temp-Dateien entfernt", len(cands))
    return len(cands)


def acquire_lock(data_root: Path, symbol: str):
    """Exklusiver flock je Symbol (eigener Name, unabhaengig von Dukascopy)."""
    p = lock_path(data_root, symbol)
    p.parent.mkdir(parents=True, exist_ok=True)
    fh = open(p, "a+")
    try:
        fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        fh.close()
        return None
    return fh


# ------------------------------------------------------------------- http ---
class _FormParser:
    """Felder des Formulars id="file_down" (die Seite hat ein zweites Formular
    file_status mit gleichnamigem tk -> nur innerhalb file_down suchen)."""
    FIELDS = ("tk", "date", "datemonth", "platform", "timeframe", "fxpair")

    @classmethod
    def parse(cls, html: str) -> dict | None:
        form = re.search(r"<form\b[^>]*\bid=[\"']file_down[\"'][^>]*>(.*?)</form>", html,
                         re.DOTALL | re.IGNORECASE)
        if not form:
            return None
        out = {}
        for tag in re.findall(r"<input\b[^>]*>", form.group(1), re.IGNORECASE):
            key = re.search(r"\b(?:id|name)=[\"']([^\"']+)[\"']", tag)
            val = re.search(r"\bvalue=[\"']([^\"']*)[\"']", tag)
            if key and key.group(1) in cls.FIELDS:
                out.setdefault(key.group(1), val.group(1) if val else "")
        return out


class Client:
    """
    Ein Request zur Zeit (kein Worker-Pool): vor JEDEM Request gate.wait()
    (429-Sperre, persistiert), Throttle.wait() (eskalierende Abkuehlung nach
    5xx/Timeout/Verbindungsfehler/403) und Mindestabstand `pause`.
    429: gate.hit(Retry-After) - kein Versuch verbraucht, keine Eskalation,
    kein Schutzschalter; beim POST danach wieder ab Seiten-GET (Token).
    Dauerhafte Inhaltsfehler: EIN Versuch, ContentFailure, kein Schutzschalter.
    """

    def __init__(self, data_root: Path, *, pause: float = PAUSE_S, attempts: int = ATTEMPTS,
                 backoff_base: float = BACKOFF_BASE_S, max_backoff: float = MAX_BACKOFF_S,
                 jitter: float = JITTER,
                 timeout: tuple[float, float] = (CONNECT_TIMEOUT_S, READ_TIMEOUT_S),
                 rate_limit_pause: float = rl.MIN_PAUSE_S,
                 max_rate_limit_wait: float = rl.MAX_TOTAL_WAIT_S,
                 max_consecutive_failures: int = MAX_CONSECUTIVE_FAILURES,
                 sleep=None, clock=None, wall=None, rng=None,
                 stop: threading.Event | None = None, session=None):
        self.pause, self.attempts, self.timeout = pause, attempts, timeout
        self.stop = stop or threading.Event()
        self._sleep = sleep or time.sleep
        self.clock = clock or time.monotonic
        self.throttle = dk.Throttle(self.clock, self._sleep, backoff_base, max_backoff, jitter,
                                    rng, max_consecutive_failures)
        self.gate = rl.RateLimitGate(ratelimit_state_path(data_root), min_pause=rate_limit_pause,
                                     max_total_wait=max_rate_limit_wait, wall=wall or time.time,
                                     sleep=sleep, stop=self.stop)
        self.session = session
        self.requests = 0
        self.status_counts: Counter = Counter()
        self.abort: str | None = None       # Lauf vorzeitig beenden (Grund)
        self._last = float("-inf")

    @property
    def halted(self) -> bool:
        return self.stop.is_set() or self.throttle.tripped.is_set() or self.abort is not None

    def _session(self):
        if self.session is None:
            self.session = requests.Session()
            self.session.headers["User-Agent"] = USER_AGENT
        return self.session

    def _request(self, method: str, url: str, **kw):
        """Ein HTTP-Request; 429 wird hier abgewartet (GET: gleicher Request
        erneut; POST: Restart, damit ein frisches Token geholt wird)."""
        while True:
            if self.halted:
                raise dk.Interrupted(url)
            self.throttle.wait()
            if not self.gate.wait():            # Stop-Signal; RateLimitExhausted geht durch
                raise dk.Interrupted(url)
            dt = self._last + self.pause - self.clock()
            if dt > 0:
                self._sleep(dt)
            if self.halted:
                raise dk.Interrupted(url)
            try:
                r = getattr(self._session(), method)(url, timeout=self.timeout, **kw)
            except requests.RequestException as e:
                self.gate.release()
                self._last = self.clock()
                self.requests += 1
                self.status_counts["netzfehler"] += 1
                raise RetryableError(f"{type(e).__name__}: {e}")
            self._last = self.clock()
            self.requests += 1
            self.status_counts[str(r.status_code)] += 1
            if r.status_code == 429:
                self.gate.hit(rl.parse_retry_after((r.headers or {}).get("Retry-After")), url)
                if method == "post":
                    raise Restart(url)
                continue
            self.gate.release()
            if r.status_code == 200:
                return r
            if r.status_code in RETRY_STATUS or r.status_code >= 500:
                raise RetryableError(f"HTTP {r.status_code}" + (" (WAF?)" if r.status_code == 403 else ""))
            raise dk.DownloadError(f"HTTP {r.status_code}: {url}", status=r.status_code)

    def fetch(self, unit: Unit) -> bytes:
        """Periodenseite + POST. Rueckgabe: ZIP-Bytes (geprueft) oder b"" (keine Daten)."""
        err, n = None, 0
        while n < self.attempts:
            try:
                page = self._request("get", unit.page_url())
                fields = _FormParser.parse(page.text or "")
                if not fields or not fields.get("tk"):
                    raise ContentError("Periodenseite ohne Download-Formular file_down/tk "
                                       "(Captcha, WAF oder Layout geaendert?)")
                want = {"fxpair": unit.symbol, "timeframe": unit.code,
                        "datemonth": unit.period if unit.month else ""}
                bad = {k: fields.get(k) for k, v in want.items()
                       if v and fields.get(k, "").upper() != v.upper()}
                if bad:
                    raise ContentError(f"Formular passt nicht zur Anfrage: {bad}")
                r = self._request("post", GET_URL, data=fields,
                                  headers={"Referer": unit.page_url(), "Origin": BASE})
                blob = r.content or b""
                if blob:
                    if not blob.startswith(b"PK"):
                        head = blob[:120].decode("utf-8", "replace").replace("\n", " ")
                        raise ContentError(f"HTML/Text statt ZIP ({len(blob)} B, "
                                           f"Fehlerseite/Captcha/WAF?): {head!r}")
                    try:
                        read_zip(blob, unit)
                    except WrongContent as e:
                        raise ContentError(f"ZIP mit falschem Inhalt: {e}")
                    except CorruptZip as e:          # abgeschnittene Uebertragung: wiederholbar
                        raise RetryableError(f"ZIP defekt: {e}")
                self.throttle.success()
                return blob
            except Restart:
                log.info("429 auf POST %s: nach der Pause neu ab Seiten-GET (frisches Token, "
                         "kein Versuch verbraucht)", unit.key)
                continue
            except ContentError as e:
                # dauerhaft: kein Wiederholen in diesem Lauf, kein Schutzschalter,
                # nur eine kurze Abkuehlung (nicht eskalierend)
                self.throttle.cooldown(self.throttle.base, str(e))
                raise ContentFailure(f"{unit.key}: {e}", attempts=n + 1)
            except RetryableError as e:
                err = str(e)
                n += 1
                wait = self.throttle.failure(err)
                if n < self.attempts:
                    log.info("retry %d/%d nach %.0fs %s (%s)", n, self.attempts - 1, wait,
                             unit.key, err)
        raise dk.DownloadError(f"{unit.key}: {self.attempts} Versuche fehlgeschlagen ({err})",
                               attempts=self.attempts)


# ---------------------------------------------------------------- parsing ---
def read_zip(blob: bytes, unit: Unit) -> tuple[bytes, str | None]:
    """ZIP -> (CSV-Bytes, Status-Text). Genau eine CSV mit erwartetem Namen."""
    try:
        with zipfile.ZipFile(io.BytesIO(blob)) as zf:
            bad = zf.testzip()
            if bad is not None:
                raise CorruptZip(f"CRC-Fehler in {bad}")
            names = [n for n in zf.namelist() if not n.endswith("/")]
            csvs = [n for n in names if n.lower().endswith(".csv")]
            if len(csvs) != 1:
                raise WrongContent(f"erwartet genau eine CSV, gefunden {names}")
            if Path(csvs[0]).name.upper() != unit.member.upper():
                raise WrongContent(f"CSV {csvs[0]} statt {unit.member}")
            txts = [n for n in names if n.lower().endswith(".txt")]
            txt = zf.read(txts[0]).decode("latin-1") if txts else None
            return zf.read(csvs[0]), txt
    except (zipfile.BadZipFile, EOFError, OSError) as e:
        raise CorruptZip(f"{type(e).__name__}: {e}")


_GAP_RE = re.compile(r"gap of\s+(\d+)\s*s\w*\s+found between\s+(\d{8})\s?(\d{6})\d*\s+and\s+"
                     r"(\d{8})\s?(\d{6})", re.IGNORECASE)


def parse_status(txt: str | None, mode: str = "tick") -> dict:
    """
    Status-.txt von HistData (Lueckenliste). Format NICHT verifiziert (keine
    echte Datei gesehen): erwartet Zeilen wie "Gap of 62s found between
    20120201000106 and 20120201000208" (Rohzeit, RAW_TZ[mode] -> UTC; nicht
    eindeutige Ortszeit -> *_utc None, *_raw bleibt). Unbekanntes Format ->
    nur die ersten Zeilen im Report.
    """
    if txt is None:
        return {"present": False}

    def utc(digits: str) -> tuple[str, str | None]:
        r = datetime.strptime(digits, "%Y%m%d%H%M%S").replace(tzinfo=UTC)
        try:
            u = int(raw_to_utc_ms(np.array([int(r.timestamp() * 1000)]), mode)[0])
        except TimezoneError:
            return r.strftime("%Y-%m-%d %H:%M:%S"), None
        return r.strftime("%Y-%m-%d %H:%M:%S"), datetime.fromtimestamp(u / 1000, UTC).isoformat()

    gaps = []
    for m in _GAP_RE.finditer(txt):
        a_raw, a = utc(m.group(2) + m.group(3))
        b_raw, b = utc(m.group(4) + m.group(5))
        g = {"from_utc": a, "to_utc": b, "seconds": int(m.group(1))}
        if a is None or b is None:
            g.update(from_raw=a_raw, to_raw=b_raw)
        gaps.append(g)
    out: dict = {"present": True, "gaps_reported": len(gaps),
                 "gap_seconds_total": sum(g["seconds"] for g in gaps),
                 "largest_gaps": sorted(gaps, key=lambda g: -g["seconds"])[:N_EXAMPLES]}
    if not gaps:
        out["head"] = [ln.strip() for ln in txt.splitlines() if ln.strip()][:8]
    return out


def parse_csv(csv: bytes, mode: str) -> pd.DataFrame:
    """
    CSV -> DataFrame mit raw_ms (Rohzeit in ms, Dateireihenfolge) und Preisen.
      tick: "YYYYMMDD HHMMSSmmm,bid,ask,volume"
      m1:   "YYYYMMDD HHMMSS;open;high;low;close;volume" (Bid)
    Strikt (pyarrow.csv, speicherschonend bei Millionen Ticks): falsche
    Spaltenzahl, falscher Trenner, Zeitstempel anderer Laenge oder nicht
    numerische Preise -> FormatError (Quarantaene, nie raten).
    """
    if mode == "tick":
        names, sep, n = ["dt", "bid", "ask", "volume"], ",", 18
    else:
        names, sep, n = ["dt", "open", "high", "low", "close", "volume"], ";", 15
    num = names[1:]
    try:
        tbl = pacsv.read_csv(
            io.BytesIO(csv), read_options=pacsv.ReadOptions(column_names=names),
            parse_options=pacsv.ParseOptions(delimiter=sep),
            convert_options=pacsv.ConvertOptions(
                column_types={"dt": pa.string(), **{c: pa.float64() for c in num}}))
        dt = pc.utf8_trim_whitespace(tbl["dt"])
        if tbl.num_rows and not pc.all(pc.equal(pc.utf8_length(dt), n)).as_py():
            bad = pc.filter(dt, pc.not_equal(pc.utf8_length(dt), n))[0].as_py()
            raise FormatError(f"Zeitstempel {bad!r} hat nicht {n} Zeichen ({mode})")
        sec = pc.strptime(pc.utf8_slice_codeunits(dt, 0, 15), format="%Y%m%d %H%M%S", unit="ms")
        raw = sec.cast(pa.int64()).to_numpy(zero_copy_only=False)
        if mode == "tick":
            raw = raw + pc.cast(pc.utf8_slice_codeunits(dt, 15, 18), pa.int64()).to_numpy(
                zero_copy_only=False)
    except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError) as e:
        raise FormatError(f"CSV nicht lesbar ({mode}, Trenner {sep!r}, {len(names)} Felder): "
                          f"{str(e).splitlines()[0][:200]}")
    out = pd.DataFrame({"raw_ms": np.asarray(raw, dtype=np.int64)})
    for c in num:
        col = tbl[c]
        if col.null_count:
            raise FormatError(f"{col.null_count} leere Werte in Spalte {c}")
        out[c] = col.to_numpy(zero_copy_only=False).astype(np.float64)
    return out


# ------------------------------------------------------- zeitzonen-pruefung ---
def _expected_ms(r_ms: int, hour_ny: int) -> int:
    """hour_ny:00 New York am (Roh-)Kalendertag von r_ms, in UTC-ms."""
    d = datetime.fromtimestamp(r_ms / 1000, UTC).date()
    return int(datetime(d.year, d.month, d.day, hour_ny, tzinfo=NY).timestamp() * 1000)


def _offsets_h(r_ms: int, hour_ny: int, mode: str) -> tuple[int, int]:
    """(impliziter, erwarteter) Offset in ganzen Stunden fuer einen Anker:
    implizit = Rohzeit - UTC des Marktereignisses hour_ny:00 New York,
    erwartet = Offset von RAW_TZ[mode] in genau diesem UTC-Zeitpunkt."""
    e = _expected_ms(r_ms, hour_ny)
    return (math.floor((r_ms - e) / HOUR_MS + 0.5), int(round(tz_offset_h(e, mode))))


def check_timezone(raw_ms: np.ndarray, symbol: str,
                   bounds: tuple[int, int] | None = None, mode: str = "tick") -> dict:
    """
    Impliziten Offset (Rohzeit - UTC, ganze Stunden) aus Marktzeiten schaetzen
    und je Anker mit dem erwarteten Offset von RAW_TZ[mode] vergleichen
    (tick America/New_York: -4 h in EDT, -5 h in EST; m1 Etc/GMT+5: immer -5 h).
    Anker (Marktzeiten New York, zoneinfo America/New_York):
      Wochenschluss  letzter Tick vor einer Luecke >= 24 h ~ 17:00 New York
      Wochenstart    erster Tick danach ~ 18:00 (Metalle) / 17:00 (EURUSD) New York
      Tagespause     (nur Metalle, Mo-Do) Luecke 45 min..3 h, deren Beginn
                     hoechstens TZ_BREAK_WINDOW_H vom erwarteten Beginn (17:00
                     New York, umgerechnet mit dem erwarteten Offset dieses
                     Ankers) liegt; Ende ~ 18:00 New York. Andere Luecken
                     (duenner Markt nachts) sind keine Anker.
    Entschieden wird ueber die Abweichung = implizit - erwartet. status:
      ok             genug Anker, Median-Abweichung 0, Anteil Abweichung 0 >=
                     TZ_MIN_SHARE, kein Lauf von TZ_MAX_RUN gleich abweichenden
                     Ankern, kein Anker mit Abweichung genau +-1 h in den
                     ersten/letzten 72 h von `bounds` (Uhrumstellung am
                     Monatsrand; Feiertage weichen um >= 2 h ab und bleiben
                     erlaubt), kein Ruecksprung >= 30 min in Dateireihenfolge
      failed         eine dieser Bedingungen verletzt
      not_checkable  zu wenige Anker (und sonst kein Fehler)
    Report: raw_tz, offsets (implizit), expected_offsets, deviations (Counter),
    median_offset_h (implizit, Anzeige), median_deviation_h, share_expected
    (Anteil Abweichung 0), edge_violations, deviating_examples, reversals, ...
    """
    raw_ms = np.asarray(raw_ms, dtype=np.int64)
    if bounds is None and len(raw_ms):        # Rohmonat des Medians als Monatsrand
        mid = datetime.fromtimestamp(float(np.median(raw_ms)) / 1000, UTC)
        bounds = raw_month_bounds_ms((mid.year, mid.month))
    rep: dict = {"raw_tz": RAW_TZ[mode], "mode": mode, "check_version": TZ_CHECK_VERSION,
                 "rows": int(len(raw_ms))}
    d0 = np.diff(raw_ms)
    rep["reversals"] = int((d0 < 0).sum())
    rep["max_reversal_s"] = float(max(0, -int(d0.min())) / 1000) if len(d0) else 0.0
    large = np.nonzero(d0 <= -REGRESSION_MAX_MS)[0]
    rep["large_reversals"] = [
        {"at_raw": _raw_iso(int(raw_ms[i])), "back_to_raw": _raw_iso(int(raw_ms[i + 1])),
         "minutes": round(-int(d0[i]) / MIN_MS, 1)} for i in large[:N_EXAMPLES]]

    t = np.unique(raw_ms)
    d = np.diff(t)
    anchors: list[tuple[int, str, int, int, int]] = []     # roh, art, stunde NY, implizit, erwartet
    for i in np.nonzero(d >= WEEKEND_GAP_MS)[0]:
        for r, k, h in ((int(t[i]), "wochenschluss", CLOSE_H_NY),
                        (int(t[i + 1]), "wochenstart", OPEN_H_NY[symbol])):
            anchors.append((r, k, h, *_offsets_h(r, h, mode)))
    ignored_breaks = 0
    if symbol in METALS:
        lo, hi = BREAK_GAP_MS
        for i in np.nonzero((d >= lo) & (d <= hi))[0]:
            if (int(t[i]) // DAY_MS + 3) % 7 >= 4:       # nur Mo-Do (1.1.1970 = Do)
                continue
            o, x = _offsets_h(int(t[i]), CLOSE_H_NY, mode)
            if abs(o - x) > TZ_BREAK_WINDOW_H:
                ignored_breaks += 1                       # z.B. naechtliche Luecke
                continue
            anchors.append((int(t[i]), "pause_beginn", CLOSE_H_NY, o, x))
            r = int(t[i + 1])
            anchors.append((r, "pause_ende", 18, *_offsets_h(r, 18, mode)))
    anchors.sort()
    offs = [o for *_, o, _ in anchors]
    exps = [x for *_, x in anchors]
    devs = [o - x for o, x in zip(offs, exps)]

    run, best = 0, 0                     # laengster Lauf gleich abweichender Anker
    for k, v in enumerate(devs):
        if v == 0:
            run = 0
        else:
            run = run + 1 if k and devs[k - 1] == v else 1
        best = max(best, run)
    edge = []
    if bounds is not None:
        blo, bhi = bounds
        edge = [(a, v) for a, v in zip(anchors, devs)
                if abs(v) == 1 and (a[0] < blo + TZ_EDGE_MS or a[0] >= bhi - TZ_EDGE_MS)]
    n = len(devs)
    share = devs.count(0) / n if n else 0.0
    med = float(np.median(offs)) if n else None
    med_dev = float(np.median(devs)) if n else None

    def row(a, v) -> dict:
        r, k, h, o, x = a
        return {"raw": _raw_iso(r), "kind": k, "expected_ny": f"{h}:00", "implied_offset_h": o,
                "expected_offset_h": x, "deviation_h": v}

    rep.update(anchors=n, ignored_break_gaps=ignored_breaks, share_expected=round(share, 3),
               median_offset_h=med, median_deviation_h=med_dev,
               offsets=Counter(dict(sorted(Counter(offs).items()))),
               expected_offsets=Counter(dict(sorted(Counter(exps).items()))),
               deviations=Counter(dict(sorted(Counter(devs).items()))),
               first_offset_h=offs[0] if n else None, last_offset_h=offs[-1] if n else None,
               first_deviation_h=devs[0] if n else None, last_deviation_h=devs[-1] if n else None,
               longest_deviating_run=best,
               edge_violations=[row(a, v) for a, v in edge],
               deviating_examples=[row(a, v) for a, v in zip(anchors, devs) if v][:N_EXAMPLES])
    reasons = []
    if len(large):
        reasons.append(f"{len(large)} Ruecksprung/Rueckspruenge >= {REGRESSION_MAX_MS // MIN_MS} min "
                       f"in Dateireihenfolge (max {rep['max_reversal_s'] / 60:.0f} min, Uhrumstellung?)")
    if edge:
        (r, k, _, o, x), v = edge[0]
        reasons.append(f"{len(edge)} Anker am Monatsrand (erste/letzte 72 h) mit genau 1 h "
                       f"Abweichung, z.B. {_raw_iso(r)} {k} -> {o} h statt {x} h "
                       f"(Uhrumstellung am Monatsrand?)")
    if n >= TZ_MIN_ANCHORS:
        if med_dev != 0:
            reasons.append(f"Median-Abweichung {med_dev:+g} h (Median-Offset {med:g} h, erwartet "
                           f"{_fmt_counter(rep['expected_offsets'])} h aus {RAW_TZ[mode]})")
        if share < TZ_MIN_SHARE:
            reasons.append(f"nur {share:.0%} der Anker ohne Abweichung von {RAW_TZ[mode]}")
        if best >= TZ_MAX_RUN:
            reasons.append(f"{best} aufeinanderfolgende Anker mit gleicher Abweichung "
                           "(Wechsel mitten im Monat?)")
    rep["status"] = "failed" if reasons else ("ok" if n >= TZ_MIN_ANCHORS else "not_checkable")
    if rep["status"] == "not_checkable":
        reasons.append(f"nur {n} Ankerpunkte (< {TZ_MIN_ANCHORS}), Zeitzone nicht pruefbar")
    rep["ok"] = rep["status"] == "ok"
    rep["reason"] = "; ".join(reasons) or None
    return rep


def _fmt_counter(c: Counter) -> str:
    return "/".join(f"{k:g}" for k in c)


def _raw_iso(ms: int, *, ms_digits: bool = False) -> str:
    dt = datetime.fromtimestamp(ms / 1000, UTC)
    return dt.strftime("%Y-%m-%d %H:%M:%S") + (f".{ms % 1000:03d}" if ms_digits else "")


# ------------------------------------------------------------- bars bauen ---
def _utc_of(df: pd.DataFrame, mode: str) -> np.ndarray:
    """UTC-ms einer Rohtabelle: vorberechnete Spalte utc_ms, sonst aus raw_ms (TimezoneError)."""
    if "utc_ms" in df:
        return df["utc_ms"].to_numpy(dtype=np.int64)
    return raw_to_utc_ms(df["raw_ms"].to_numpy(), mode)


def ticks_to_bars(ticks: pd.DataFrame) -> pd.DataFrame:
    """Ticks (raw_ms, bid, ask, volume) -> 1m-Bars im Store-Schema (UTC; Rohzeit =
    New-York-Ortszeit, RAW_TZ["tick"]; nicht eindeutige Ortszeit -> TimezoneError)."""
    t = pd.DataFrame({
        "ts": pd.to_datetime(_utc_of(ticks, "tick"), unit="ms", utc=True).as_unit("ms"),
        "bid": ticks["bid"].to_numpy(), "ask": ticks["ask"].to_numpy(),
        "bid_volume": ticks["volume"].to_numpy(), "ask_volume": ticks["volume"].to_numpy(),
    })
    bars = dk.aggregate_1m(t)
    bars["source"] = SOURCE_BY_MODE["tick"]
    return bars


def m1_to_bars(m1: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """
    M1-Bid-Bars -> Store-Schema (Rohzeit RAW_TZ["m1"] = fest UTC-5, NICHT
    verifiziert); ask = bid (kein Spread). Doppelte Minuten
    mit abweichenden Werten (von HistData beobachtet) werden zusammengefasst:
    open erste, high max, low min, close letzte Zeile in Dateireihenfolge.
    """
    ts = pd.to_datetime(_utc_of(m1, "m1"), unit="ms", utc=True).as_unit("ms")
    df = m1.assign(ts=ts)
    g = df.groupby("ts", sort=True)
    merged = int((g.size() > 1).sum())
    b = pd.DataFrame({"bid_open": g["open"].first(), "bid_high": g["high"].max(),
                      "bid_low": g["low"].min(), "bid_close": g["close"].last(),
                      "vol": g["volume"].sum()})
    b.index.name = "ts"
    b = b.reset_index()
    for k in ("open", "high", "low", "close"):
        b[f"ask_{k}"] = b[f"bid_{k}"]
    b["tick_count"] = np.ones(len(b), dtype="int64")
    b["bid_volume"] = b["vol"].astype(np.float64)
    b["ask_volume"] = b["vol"].astype(np.float64)
    b["source"] = SOURCE_BY_MODE["m1"]
    b["ts"] = b["ts"].dt.as_unit("ms")
    return b[dk.COLUMNS].reset_index(drop=True), merged


# --------------------------------------------------------------- kontext ---
@dataclass
class Outcome:
    # cached | ok | empty (bestaetigt) | pending | unconfirmed | failed |
    # content_error (dauerhaft, 24 h zurueckgestellt) | deferred | skipped
    status: str
    error: str | None = None
    first_seen: str | None = None
    attempts: int = 0
    http_status: int | None = None
    accepted: bool = False


@dataclass
class Decoded:
    df: pd.DataFrame              # raw_ms + Preise, auf die Rohperiode begrenzt
    status: dict                  # Status-.txt
    stats: dict


UNRESOLVED = ("failed", "skipped", "pending", "unconfirmed", "content_error", "deferred")


class Ctx:
    """Cache, Client und Zustand je Datei fuer einen Lauf (ein Symbol, ein Modus)."""

    def __init__(self, symbol: str, mode: str, data_root: Path, client: Client, now,
                 rebuild: bool = False):
        self.symbol, self.mode, self.data_root, self.client, self.now = symbol, mode, data_root, client, now
        self.rebuild = rebuild
        self.decoded: OrderedDict[Unit, Decoded] = OrderedDict()
        self.tz: dict[tuple[Unit, tuple[int, int]], dict] = {}
        self.downloaded = 0
        self.empty_streak = self.content_streak = 0
        self.first_data = self._first_cached_month()

    def _first_cached_month(self) -> tuple[int, int] | None:
        """Fruehester Rohmonat mit gecachten Daten = Beginn der bekannten Historie."""
        d = raw_dir(self.data_root, self.symbol, self.mode)
        pat = re.compile(rf"HISTDATA_COM_ASCII_{self.symbol}_{TF_CODE[self.mode]}(\d{{4}})(\d{{2}})?\.zip$")
        found = []
        for p in d.glob("*.zip") if d.exists() else []:
            m = pat.match(p.name)
            if m:
                found.append((int(m.group(1)), int(m.group(2) or 1)))
        return min(found) if found else None

    def inside_history(self, u: Unit) -> bool:
        """
        Hier ist ein bestaetigtes "keine Daten" verdaechtig (Exit 4, Neupruefung
        nach 30 Tagen): nach dem ersten Monat mit Daten - oder ueberall ab
        HISTORY_START, solange noch NIE Daten kamen (sonst wuerde eine
        systematisch leere Antwort, z.B. WAF/Token, still als "nicht
        verfuegbar" mit Exit 0 enden). Leere Monate VOR dem ersten Monat mit
        Daten gelten als vor Beginn der Tick-Historie.
        """
        if before_history(self.symbol, u.first_month) and u.month:
            return False
        return self.first_data is None or u.first_month > self.first_data

    def _streak_zone(self, u: Unit) -> bool:
        """Leere Antworten zaehlen fuer den Abbruch erst nach bekannten Daten
        (sonst stockt der erste Backfill vor Beginn der Tick-Historie)."""
        return self.first_data is not None and u.first_month > self.first_data

    def unit(self, ym: tuple[int, int]) -> Unit:
        # Jahresdatei erst, wenn das Rohjahr (Ortszeit RAW_TZ) samt SETTLE_S vorbei ist
        t = int((self.now() - timedelta(seconds=SETTLE_S)).timestamp() * 1000)
        cur = datetime.fromtimestamp(int(utc_to_raw_ms(np.array([t]), self.mode)[0]) / 1000, UTC).year
        return unit_for(self.symbol, self.mode, ym, cur, self.data_root)

    def _since(self, iso: str | None) -> float:
        return (self.now() - datetime.fromisoformat(iso)).total_seconds() if iso else float("inf")

    def state(self, u: Unit) -> tuple[str, dict | None]:
        """data | deferred (Inhaltsfehler < 24 h) | empty (bestaetigt) |
        wait (< 24 h seit letzter Pruefung) | recheck | missing"""
        if u.path(self.data_root).exists():
            return "data", None
        failed = dk.read_marker(u.marker(self.data_root, "failed"))
        if failed is not None and self._since(failed.get("last_checked")) < CONTENT_RETRY_S:
            return "deferred", failed
        info = dk.read_marker(u.marker(self.data_root, "empty"))
        if info is None:
            return "missing", None
        if info.get("confirmed"):
            if info.get("accepted") or not self.inside_history(u):
                return "empty", info
            if self.rebuild or self._since(info.get("confirmed_at") or info.get("last_checked")) \
                    >= EMPTY_RECHECK_DAYS * 86400:
                return "recheck", info          # Luecke in der Historie: periodisch erneut
            return "empty", info
        if self._since(info.get("last_checked") or info["first_seen"]) < CONFIRM_AFTER_S:
            return "wait", info
        return "recheck", info

    def recent(self, u: Unit) -> bool:
        return self.now() < u.end_utc + timedelta(days=PUBLICATION_LAG_DAYS)

    def _unresolved(self, u: Unit, info: dict) -> Outcome:
        after = datetime.fromisoformat(info.get("last_checked") or info["first_seen"]) + \
            timedelta(seconds=CONFIRM_AFTER_S)
        if self.recent(u):
            return Outcome("pending", f"HistData liefert (noch) keine Daten - Veroeffentlichung "
                           f"abwarten (bis {PUBLICATION_LAG_DAYS} Tage nach Monatsende); naechste "
                           f"Pruefung ab {after:%Y-%m-%d %H:%M} UTC", info["first_seen"])
        return Outcome("unconfirmed", f"keine Daten, unbestaetigt; erneute Pruefung ab "
                       f"{after:%Y-%m-%d %H:%M} UTC", info["first_seen"])

    def _clear_markers(self, u: Unit) -> None:
        """Neue Rohdatei: alte Marker (auch Quarantaene) gelten nicht mehr."""
        for kind in ("empty", "failed"):
            u.marker(self.data_root, kind).unlink(missing_ok=True)
        for q in u.path(self.data_root).parent.glob(f"{u.name}*.quarantine"):
            q.unlink(missing_ok=True)
        self.tz = {k: v for k, v in self.tz.items() if k[0] != u}

    def get(self, u: Unit) -> Outcome:
        if self.now() < u.end_utc + timedelta(seconds=SETTLE_S):
            raise ValueError(f"{u.key}: Periode nicht abgeschlossen (Ende + {SETTLE_S / 3600:g} h)")
        st, info = self.state(u)
        if st == "data":
            return Outcome("cached")
        if st == "empty":
            return Outcome("empty", accepted=bool(info.get("accepted")))
        if st == "wait":
            return self._unresolved(u, info)
        if st == "deferred":
            after = datetime.fromisoformat(info["last_checked"]) + timedelta(seconds=CONTENT_RETRY_S)
            return Outcome("deferred", f"{info.get('error')} - erneuter Versuch ab "
                           f"{after:%Y-%m-%d %H:%M} UTC", info.get("first_seen"))
        if self.client.halted:
            return Outcome("skipped")
        t0 = time.monotonic()
        now = self.now().isoformat()
        try:
            blob = self.client.fetch(u)
        except dk.Interrupted:
            return Outcome("skipped")
        except ContentFailure as e:
            old = dk.read_marker(u.marker(self.data_root, "failed")) or {}
            dk.write_json_atomic(u.marker(self.data_root, "failed"), {
                "first_seen": old.get("first_seen") or now, "last_checked": now,
                "error": str(e), "count": int(old.get("count", 0)) + 1})
            log.error("INHALTSFEHLER %s: %s (naechster Versuch fruehestens in %g h)", u.key, e,
                      CONTENT_RETRY_S / 3600)
            self.content_streak += 1
            if self.content_streak >= MAX_CONTENT_ERRORS_IN_ROW:
                self.client.abort = (f"{self.content_streak} Inhaltsfehler in Folge (Formular/HTML/"
                                     f"falsche CSV) - HistData-Seite geaendert oder blockiert?")
            return Outcome("content_error", str(e), now, attempts=e.attempts)
        except dk.DownloadError as e:
            log.warning("FEHLSCHLAG %s: %s (spaeter erneut)", u.key, e)
            return Outcome("failed", str(e), attempts=e.attempts, http_status=e.status)
        self.content_streak = 0
        if not blob:
            first = (info or {}).get("first_seen") or now
            done = (st == "recheck" and not self.recent(u)
                    and self._since(first) >= CONFIRM_AFTER_S)
            new = {"first_seen": first, "last_checked": now, "confirmed": "repeat" if done else None,
                   "confirmed_at": now if done else None,
                   "checks": int((info or {}).get("checks", 0)) + 1}
            if (info or {}).get("accepted"):
                new["accepted"] = info["accepted"]
            dk.write_json_atomic(u.marker(self.data_root, "empty"), new)
            u.marker(self.data_root, "failed").unlink(missing_ok=True)
            log.info("leer  %s (keine Daten%s)", u.key, ", bestaetigt" if done else ", unbestaetigt")
            if self._streak_zone(u) and not self.recent(u):
                self.empty_streak += 1
                if self.empty_streak >= MAX_EMPTY_IN_ROW:
                    self.client.abort = (
                        f"{self.empty_streak} leere Antworten in Folge innerhalb der bekannten "
                        f"Historie (ab {self.first_data[0]}-{self.first_data[1]:02d}) - Token-/WAF-/"
                        "Serverproblem? Lauf beendet, nichts als 'keine Daten' gewertet ausser "
                        "den schon markierten Dateien")
            return Outcome("empty", accepted=bool(new.get("accepted"))) if done \
                else self._unresolved(u, new)
        dk._atomic_write(u.path(self.data_root), blob)
        self._clear_markers(u)
        self.empty_streak = 0
        self.downloaded += 1
        if self.first_data is None or u.first_month < self.first_data:
            self.first_data = u.first_month
        log.info("ok    %s (%d B, %.1fs)", u.key, len(blob), time.monotonic() - t0)
        return Outcome("ok")

    def set_aside(self, u: Unit, why: str) -> None:
        p = u.path(self.data_root)
        if p.exists():
            os.replace(p, u.marker(self.data_root, "corrupt"))
        self.decoded.pop(u, None)
        log.warning("Cache-Datei %s defekt (%s), beiseitegelegt (.corrupt), wird neu geladen", u.name, why)

    def decode(self, u: Unit) -> Decoded:
        """ZIP aus dem Cache -> Decoded (gecached, max. 3 Dateien im Speicher: M-1, M, M+1)."""
        if u in self.decoded:
            self.decoded.move_to_end(u)
            return self.decoded[u]
        csv, txt = read_zip(u.path(self.data_root).read_bytes(), u)     # CorruptZip
        df = parse_csv(csv, self.mode)                                  # FormatError
        lo, hi = u.raw_bounds_ms()
        inside = (df["raw_ms"] >= lo) & (df["raw_ms"] < hi)
        stats = {"rows": int(len(df)), "rows_outside_period": int((~inside).sum())}
        df = df[inside.to_numpy()].reset_index(drop=True)
        if self.mode == "m1":
            dup = df.duplicated()
            stats["m1_exact_duplicates_dropped"] = int(dup.sum())
            df = df[~dup.to_numpy()].reset_index(drop=True)
        dec = Decoded(df, parse_status(txt, self.mode), stats)
        self.decoded[u] = dec
        while len(self.decoded) > 3:
            self.decoded.popitem(last=False)
        return dec

    def _month_rows(self, u: Unit, ym: tuple[int, int]) -> pd.DataFrame:
        dec = self.decode(u)
        lo, hi = raw_month_bounds_ms(ym)
        return dec.df[(dec.df["raw_ms"] >= lo).to_numpy() & (dec.df["raw_ms"] < hi).to_numpy()]

    def _neighbour_ms(self, ym: tuple[int, int]) -> np.ndarray | None:
        """raw_ms eines gecachten Nachbar-Rohmonats (ohne eigene Pruefung) oder None."""
        if before_history(self.symbol, ym):
            return None
        u = self.unit(ym)
        if not u.path(self.data_root).exists():
            return None
        try:
            return self._month_rows(u, ym)["raw_ms"].to_numpy()
        except (CorruptZip, FormatError):
            return None

    def _marker_current(self, info: dict) -> bool:
        """Quarantaene-Marker der aktuellen Zeitzonen-Hypothese? Marker aelterer
        Pruefversionen (v1: fest UTC-5 fuer beide Modi, ohne tz_check_version)
        oder anderer Zeitzone gelten nicht mehr, ausser manuell freigegeben."""
        return info.get("accepted") or (info.get("tz_check_version") == TZ_CHECK_VERSION
                                        and info.get("raw_tz") == RAW_TZ[self.mode])

    def raw_slice(self, u: Unit, ym: tuple[int, int]) -> tuple[pd.DataFrame, dict]:
        """Rohmonat ym aus Datei u, zeitzonengeprueft, mit Spalte utc_ms; Quarantined wenn nicht."""
        q = u.qmarker(self.data_root, ym)
        accepted = False
        if q.exists():
            info = dk.read_marker(q) or {}
            if not self._marker_current(info):
                log.warning("Quarantaene-Marker %s stammt aus aelterer Zeitzonen-Hypothese "
                            "(Version %s, %s) - entfernt, Rohmonat wird neu geprueft (ohne Download)",
                            q.name, info.get("tz_check_version", 1),
                            info.get("raw_tz") or "fest UTC-5")
                q.unlink(missing_ok=True)
            elif not info.get("accepted"):
                raise Quarantined(u, ym, info.get("reason") or "Quarantaene", info.get("check"))
            else:
                accepted = True
        try:
            sel = self._month_rows(u, ym)
        except FormatError as e:
            if accepted:
                raise Quarantined(u, ym, f"Format nicht lesbar: {e}", None)
            self._quarantine(u, ym, f"Format nicht lesbar: {e}", None)
        try:
            sel = sel.assign(utc_ms=raw_to_utc_ms(sel["raw_ms"].to_numpy(), self.mode))
        except TimezoneError as e:
            # auch bei "accepted": nicht eindeutige Ortszeit wird nie geraten
            reason = f"Rohzeit nicht eindeutig nach UTC umrechenbar: {e}"
            if accepted:
                raise Quarantined(u, ym, reason, None)
            self._quarantine(u, ym, reason, None)
        key = (u, ym)
        if accepted:
            return sel, {"accepted_manually": True, "marker": str(q)}
        if key not in self.tz and len(sel):
            bounds = raw_month_bounds_ms(ym)
            chk = check_timezone(sel["raw_ms"].to_numpy(), self.symbol, bounds, self.mode)
            if chk["status"] == "not_checkable":
                # zu wenige Anker (kurzer erster Historienmonat, duenner Monat):
                # gemeinsam mit gecachten Nachbar-Rohmonaten pruefen
                parts = [self._neighbour_ms(_ym_add(ym, -1)), sel["raw_ms"].to_numpy(),
                         self._neighbour_ms(_ym_add(ym, 1))]
                used = [_ymstr(_ym_add(ym, k)) for k, p in zip((-1, 0, 1), parts) if p is not None]
                if len(used) > 1:
                    joint = check_timezone(np.concatenate([p for p in parts if p is not None]),
                                           self.symbol, bounds, self.mode)
                    joint["joint_with"] = used
                    joint["alone"] = {k: chk[k] for k in ("anchors", "reason")}
                    chk = joint
                if chk["status"] == "not_checkable":
                    chk["reason"] = (f"Zeitzone NICHT PRUEFBAR: {chk['reason']} (auch gemeinsam mit "
                                     f"Nachbarmonaten {used}) - manuell pruefen, dann im Marker "
                                     "\"accepted\": true setzen")
            self.tz[key] = chk
            if not chk["ok"]:
                self._quarantine(u, ym, ("" if chk["status"] == "not_checkable" else
                                         f"Zeitzone passt nicht zu {RAW_TZ[self.mode]}: ")
                                 + chk["reason"], chk)
        return sel, self.tz.get(key, {})

    def check_following(self, ym: tuple[int, int]) -> None:
        """
        Folge-Rohmonat ym+1 (falls gecached) beginnt mit 1 h Abweichung ->
        Umstellung vermutlich nach dem letzten Anker von ym: UTC-Monat ym
        nicht schreiben (Quarantined, Marker liegt am Folgemonat).
        """
        nxt = _ym_add(ym, 1)
        u = self.unit(nxt)
        if not u.path(self.data_root).exists():
            return
        try:
            self.raw_slice(u, nxt)
        except Quarantined as q:
            fd = (q.check or {}).get("first_deviation_h")
            if fd is not None and abs(fd) == 1:
                fo = q.check.get("first_offset_h")
                raise Quarantined(q.unit, q.ym, (
                    f"Folge-Rohmonat {_ymstr(nxt)} beginnt mit Offset {fo} h ({fd:+d} h Abweichung "
                    f"von {RAW_TZ[self.mode]}): Uhrumstellung vermutlich am Ende von {_ymstr(ym)} "
                    f"(nach dessen letztem Anker) - {_ymstr(ym)} nicht geschrieben. "
                    f"Folgemonat: {q.reason}"), q.check)
        except (CorruptZip, FormatError):
            return

    def _quarantine(self, u: Unit, ym, reason: str, check: dict | None):
        dk.write_json_atomic(u.qmarker(self.data_root, ym),
                             {"reason": reason, "check": check, "since": self.now().isoformat(),
                              "accepted": False, "raw_tz": RAW_TZ[self.mode],
                              "tz_check_version": TZ_CHECK_VERSION})
        log.error("QUARANTAENE %s Rohmonat %d-%02d: %s", u.key, *ym, reason)
        raise Quarantined(u, ym, reason, check)


# ------------------------------------------------------------- monatsbau ---
def needed(symbol: str, ym: tuple[int, int], mode: str = "tick") -> list[tuple[int, int]]:
    """Rohmonate, die UTC-Monat ym ueberdecken (aus RAW_TZ[mode] berechnet, nicht
    fest verdrahtet): fuer New York/UTC-5 M-1 (dessen letzte 4 h in EDT bzw.
    5 h in EST) und M; vor Historienbeginn entfaellt."""
    lo = int(datetime(*ym, 1, tzinfo=UTC).timestamp() * 1000)
    hi = int(datetime(*_ym_add(ym, 1), 1, tzinfo=UTC).timestamp() * 1000) - 1
    a, b = (datetime.fromtimestamp(int(x) / 1000, UTC)
            for x in utc_to_raw_ms(np.array([lo, hi]), mode))
    out, r = [], (a.year, a.month)
    while r <= (b.year, b.month):
        out.append(r)
        r = _ym_add(r, 1)
    return [r for r in out if not before_history(symbol, r)]


def build_month(ctx: Ctx, ym: tuple[int, int]) -> tuple[pd.DataFrame, dict]:
    """UTC-Monat aus den Rohmonaten M-1 und M (beide erledigt) bauen."""
    ms, me = (pd.Timestamp(datetime(*ym, 1, tzinfo=UTC)),
              pd.Timestamp(datetime(*_ym_add(ym, 1), 1, tzinfo=UTC)))
    parts, stats = [], {"raw_months": {}, "histdata_status": {}}
    for r in needed(ctx.symbol, ym, ctx.mode):
        u = ctx.unit(r)
        info = {"file": u.name, "state": ctx.state(u)[0]}
        stats["raw_months"][_ymstr(r)] = info
        if info["state"] != "data":
            continue
        sel, tz = ctx.raw_slice(u, r)                 # mit utc_ms (RAW_TZ[mode] -> UTC)
        utc_ms = sel["utc_ms"].to_numpy()
        sel = sel[(utc_ms >= ms.value // 1_000_000) & (utc_ms < me.value // 1_000_000)]
        info.update(file_rows=ctx.decode(u).stats, rows_in_month=int(len(sel)), timezone=tz)
        stats["histdata_status"][u.name] = ctx.decode(u).status
        parts.append(sel)
    ctx.check_following(ym)
    raw = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
    if raw.empty:
        return pd.DataFrame(columns=dk.COLUMNS), stats
    if ctx.mode == "tick":
        bars = ticks_to_bars(raw)
    else:
        bars, stats["m1_conflicting_minutes_merged"] = m1_to_bars(raw)
    bars = bars[(bars["ts"] >= ms) & (bars["ts"] < me)].reset_index(drop=True)
    return bars, stats


def month_report(df: pd.DataFrame, symbol: str, ym, mode: str, stats: dict) -> dict:
    """fetch_dukascopy.validate + HistData-Angaben (ValidationError bei harten Fehlern)."""
    ms = pd.Timestamp(datetime(*ym, 1, tzinfo=UTC))
    rep = dk.validate(df, symbol, f"{symbol} {ym[0]}-{ym[1]:02d}", ms, ms + pd.offsets.MonthBegin(1))
    rep.update(stats)
    rep.update(source=SOURCE_BY_MODE[mode], mode=PARQUET_MODE[mode], spread_available=mode == "tick")
    if mode == "m1":
        rep["note"] = ("M1 enthaelt nur Bid: ask = bid, Spread 0 - Spread kommt aus dem "
                       "Kostenmodell; tick_count = 1 je Bar (kein Tick-Zaehler)")
    return rep


# ------------------------------------------------------------------ driver ---
def run(symbol: str, start: date, end: date, data_root: Path | None = None, *,
        mode: str = "tick", rebuild: bool = False, now=None,
        stop: threading.Event | None = None, **client_kw) -> dict:
    """
    Laedt alle abgeschlossenen UTC-Monate [start, end] und gibt den Laufreport
    zurueck (exit_code, s. Docstring oben). Der Laufreport wird immer
    geschrieben, ausser wenn ein anderer Prozess den Symbol-Lock haelt.
    """
    if symbol not in SYMBOLS:
        raise ValueError(f"unbekanntes Symbol {symbol}; bekannt: {SYMBOLS}")
    if mode not in MODES:
        raise ValueError(f"unbekannter Modus {mode}; bekannt: {MODES}")
    data_root = data_root or dk.default_data_root()
    now = now or dk.utc_now
    stop = stop or threading.Event()
    lock = acquire_lock(data_root, symbol)
    if lock is None:
        log.error("%s: Lock %s belegt (anderer Lauf aktiv) - spaeter erneut", symbol,
                  lock_path(data_root, symbol))
        return {"symbol": symbol, "source": SOURCE_BY_MODE[mode], "mode": mode, "status": "locked",
                "exit_code": EXIT_GAPS}
    try:
        cleanup_orphans(data_root, symbol)
        ctx = Ctx(symbol, mode, data_root, Client(data_root, stop=stop, **client_kw), now, rebuild)
        return _run_locked(ctx, start, end, rebuild)
    finally:
        lock.close()


def _month_closed(now: datetime, ym: tuple[int, int], mode: str = "tick") -> bool:
    """Rohmonat ym (und damit UTC-Monat ym) abgeschlossen: Ende in UTC + SETTLE_S vorbei."""
    return now >= raw_month_end_utc(ym, mode) + timedelta(seconds=SETTLE_S)


def _ymstr(ym) -> str:
    return f"{ym[0]}-{ym[1]:02d}"


def _run_locked(ctx: Ctx, start: date, end: date, rebuild: bool) -> dict:
    sym, data_root, client, mode = ctx.symbol, ctx.data_root, ctx.client, ctx.mode
    started = dk.utc_now()
    todo, skipped_existing, unavailable, conflicts = [], 0, {}, {}
    stale_tz: dict[tuple[int, int], str | None] = {}     # alte source_timezone -> neu bauen

    def mode_conflict(ym) -> str | None:
        out = store_path(data_root, sym, *ym, mode)
        if not out.exists():
            return None
        have = stored_mode(out)
        if have == PARQUET_MODE[mode]:
            return None
        return (f"{out} hat histdata_mode={have!r}, Lauf ist {PARQUET_MODE[mode]!r} - "
                "nicht ueberschrieben (Datei pruefen/verschieben)")

    for ym in dk.months(start, end):
        if not _month_closed(ctx.now(), ym, mode):
            log.info("skip %s %s (Monat nicht abgeschlossen: Rohmonatsende %s in UTC + %g h)", sym,
                     _ymstr(ym), RAW_TZ[mode], SETTLE_S / 3600)
            continue
        if before_history(sym, ym):
            unavailable[ym] = f"vor Historienbeginn {HISTORY_START[sym][0]}-{HISTORY_START[sym][1]:02d}"
            continue
        c = mode_conflict(ym)
        if c:
            conflicts[ym] = c
            continue
        out = store_path(data_root, sym, *ym, mode)
        if out.exists():
            old_tz = stored_timezone(out)
            if old_tz != SOURCE_TZ_META[mode]:
                # mit alter Zeitzonen-Annahme geschrieben (z.B. fest UTC-5 fuer Ticks:
                # Sommermonate um 1 h verschoben) -> aus dem Roh-Cache neu bauen
                stale_tz[ym] = old_tz
                # SOFORT aus dem Store nehmen (nicht erst nach erfolgreichem Neubau):
                # scheitert der Neubau (Quarantaene, Luecke), soll der Bot eine
                # Luecke sehen statt eines um 1 h verschobenen Monats.
                moved = retire_stale_store_file(data_root, out)
                log.warning("%s: source_timezone %r statt %r - aus dem Store genommen (%s), wird "
                            "aus dem Roh-Cache neu gebaut", out.name, old_tz, SOURCE_TZ_META[mode],
                            moved)
            elif not rebuild:
                skipped_existing += 1
                continue
        todo.append(ym)
    log.info("START %s mode=%s: %d Monate offen (%d fertig uebersprungen, %d vor Historienbeginn, "
             "%d mit alter Zeitzone neu zu bauen)", sym, mode, len(todo), skipped_existing,
             len(unavailable), len(stale_tz))

    outcomes: dict[Unit, Outcome] = {}
    written, validation_errors, quarantine, pending, missing = [], {}, {}, {}, {}
    state = {"status": None, "error": None}
    reported_gaps: dict[str, dict] = {}

    def resolve(u: Unit) -> Outcome:
        if u not in outcomes or outcomes[u].status == "skipped":
            outcomes[u] = ctx.get(u)
        return outcomes[u]

    def finish(ym) -> None:
        label = f"{sym} {_ymstr(ym)}"
        try:
            df, stats = build_month(ctx, ym)
        except Quarantined as q:
            quarantine[ym] = {"file": q.unit.name, "raw_month": _ymstr(q.ym), "reason": q.reason,
                              "path": str(q.unit.qmarker(data_root, q.ym)), "check": q.check}
            return
        reported_gaps.update(stats["histdata_status"])
        if df.empty:
            unavailable[ym] = "HistData-Datei ohne Zeilen in diesem Monat"
            return
        try:
            rep = month_report(df, sym, ym, mode, stats)
        except dk.ValidationError as e:
            validation_errors[ym] = str(e)
            log.error("VALIDIERUNG FEHLGESCHLAGEN %s: %s", label, e)
            return
        c = mode_conflict(ym)
        if c:
            conflicts[ym] = c
            return
        out = store_path(data_root, sym, *ym, mode)
        rep["file"] = str(out.relative_to(data_root))
        write_parquet(out, df, mode)
        dk.write_json_atomic(report_path(data_root, sym, *ym, mode), rep)
        written.append(ym)
        log.info("%-15s %6d Bars  gekreuzt=%-3d spread_median=%g  mode=%s", label, rep["rows"],
                 rep["crossed"], rep["spread_median"], mode)

    todo_set = set(todo)
    try:
        for ym in todo:
            if client.halted:
                break
            raws = needed(sym, ym, mode)
            for attempt in range(2):            # 2. Durchgang nur nach beiseitegelegtem ZIP
                outs = {r: resolve(ctx.unit(r)) for r in raws}
                # Rohmonat M+1 zuerst holen, wenn er ohnehin gebraucht wird (UTC-Monat
                # M+1 offen): check_following prueft dann vor dem Schreiben von M,
                # ob M+1 schon mit 1 h Abweichung beginnt. Keine Extra-Requests.
                nxt = _ym_add(ym, 1)
                if nxt in todo_set and not client.halted:
                    resolve(ctx.unit(nxt))
                if client.halted and any(o.status == "skipped" for o in outs.values()):
                    break
                bad = [o for o in outs.values() if o.status in UNRESOLVED]
                if bad:
                    if any(o.status == "pending" for o in bad):
                        pending[ym] = "; ".join(o.error or "" for o in bad if o.status == "pending")
                    break
                cur = outs[ym]
                if cur.status == "empty":
                    if ctx.inside_history(ctx.unit(ym)) and not cur.accepted:
                        missing[ym] = ("HistData liefert wiederholt keine Daten, obwohl der Monat "
                                       "innerhalb der bekannten Historie liegt - manuell pruefen; "
                                       "akzeptieren: \"accepted\": true in "
                                       f"{ctx.unit(ym).marker(data_root, 'empty')}")
                    else:
                        unavailable[ym] = "HistData hat keine Daten (bestaetigt, wiederholt leer)"
                    break
                try:
                    finish(ym)
                except CorruptZip as e:
                    if attempt:
                        raise
                    for r in raws:
                        u = ctx.unit(r)
                        if u.path(data_root).exists():
                            try:
                                read_zip(u.path(data_root).read_bytes(), u)
                            except CorruptZip:
                                ctx.set_aside(u, str(e))
                                outcomes.pop(u, None)
                    continue
                break
    except rl.RateLimitExhausted as e:
        state["status"] = "rate_limited"
        state["error"] = f"429-Wartebudget erschoepft: {e}"
        log.error("%s - Lauf endet, systemd setzt spaeter fort", state["error"])
    except Exception as e:
        state["status"], state["error"] = "error", f"{type(e).__name__}: {e}"
        raise
    finally:
        rep = _run_report(ctx, start, end, started, todo, outcomes, written, validation_errors,
                          quarantine, pending, unavailable, missing, conflicts, skipped_existing,
                          state, reported_gaps, rebuild, stale_tz)
        dk.write_json_atomic(run_report_path(data_root, sym, mode), rep)
        log.info("FERTIG %s: status=%s exit=%d, %d Monate geschrieben, %d Requests (Report %s)", sym,
                 rep["status"], rep["exit_code"], len(written), client.requests,
                 run_report_path(data_root, sym, mode))
    return rep


def _run_report(ctx, start, end, started, todo, outcomes, written, validation_errors, quarantine,
                pending, unavailable, missing, conflicts, skipped_existing, state, reported_gaps,
                rebuild, stale_tz=None) -> dict:
    stale_tz = stale_tz or {}
    client = ctx.client
    manual = [{"month": _ymstr(k), "kind": "quarantine", **{x: v[x] for x in ("reason", "path")}}
              for k, v in quarantine.items()]
    manual += [{"month": _ymstr(k), "kind": "missing_in_history", "reason": v}
               for k, v in missing.items()]
    manual += [{"month": _ymstr(k), "kind": "store_mode_conflict", "reason": v}
               for k, v in conflicts.items()]
    manual.sort(key=lambda r: r["month"])
    done = set(written) | set(validation_errors) | set(quarantine) | set(unavailable) | \
        set(missing) | set(conflicts)
    incomplete = [ym for ym in todo if ym not in done]
    gaps = {u: o for u, o in outcomes.items() if o.status in ("failed", "skipped")}
    content = {u: o for u, o in outcomes.items() if o.status in ("content_error", "deferred")}
    unconfirmed = {u: o for u, o in outcomes.items() if o.status == "unconfirmed"}
    pend_units = {u: o for u, o in outcomes.items() if o.status == "pending"}
    tripped = client.throttle.tripped.is_set()
    # Vorrang 3 vor 4 BEWUSST: systemd startet bei 3 neu, bei 4 nicht. Ein
    # Exit 4 bei offenen Luecken wuerde den Backfill anhalten. Damit die
    # Quarantaene trotzdem nicht untergeht: status "...+quarantine",
    # Liste "quarantined" oben im Report, ERROR-Zeile in jedem Lauf.
    if state["status"] == "error":
        status, code = "error", 1
    elif validation_errors:
        status, code = "validation_error", EXIT_VALIDATION
    elif (state["status"] == "rate_limited" or client.abort or gaps or content or unconfirmed
          or pend_units or incomplete):
        status = ("rate_limited" if state["status"] == "rate_limited" else
                  "aborted" if client.abort else
                  "circuit_breaker" if tripped else "interrupted" if client.stop.is_set() else
                  "pending" if pend_units and not (gaps or unconfirmed or content) else "gaps")
        code = EXIT_GAPS
    elif manual:
        status, code = "quarantine", EXIT_MANUAL
    else:
        status, code = "complete", EXIT_OK
    if manual and code != EXIT_MANUAL:
        status += "+quarantine"
    hints = []
    if manual:
        log.error("MANUELLE PRUEFUNG NOETIG %s (%s): %s", ctx.symbol, ctx.mode,
                  "; ".join(f"{r['month']} {r['kind']}" for r in manual))
        hints.append("Quarantaene/manuelle Pruefung: Zeitzonen-Pruefung im Report ansehen, "
                     "--verify-month fuer den Monat. <zip>.quarantine loeschen = erneut pruefen "
                     "(ohne Download); ZIP loeschen = neu laden und neu pruefen (Marker wird "
                     "dabei entfernt); nach manueller Pruefung \"accepted\": true im Marker "
                     "(.quarantine bzw. .empty) = freigeben.")
    if client.abort:
        hints.append(f"Lauf abgebrochen: {client.abort}")
    if content:
        hints.append(f"{len(content)} Datei(en) mit Inhaltsfehler (kein Formular/HTML statt ZIP/"
                     f"falsche CSV): je Lauf 1 Versuch, erneut fruehestens nach "
                     f"{CONTENT_RETRY_S / 3600:g} h; Marker <zip>.failed.")
    if tripped:
        hints.append(f"Schutzschalter: {client.throttle.consecutive} Fehlschlaege in Folge - Lauf "
                     "beendet, Neustart spaeter (systemd) setzt fort.")
    n403 = client.status_counts.get("403", 0)
    if n403 and n403 >= 0.5 * client.requests:
        hints.append(f"ueberwiegend HTTP 403 ({n403}/{client.requests}): vermutlich WAF/Bot-Schutz.")
    for h in hints:
        log.error(h)

    def unit_rows(d: dict) -> list[dict]:
        return [{"symbol": ctx.symbol, "mode": ctx.mode, **u.to_json(), "status": o.status,
                 "error": o.error, "first_seen": o.first_seen, "attempts": o.attempts,
                 "http_status": o.http_status}
                for u, o in sorted(d.items(), key=lambda kv: kv[0].period)]

    return {
        "symbol": ctx.symbol, "source": SOURCE_BY_MODE[ctx.mode], "mode": ctx.mode,
        "status": status, "exit_code": code, "quarantined": manual,
        # Monatsdateien mit alter source_timezone, in diesem Lauf zum Neubau
        # eingeplant (ohne Requests, aus dem Roh-Cache); Ergebnis s. summary
        "rebuilt_timezone": [_ymstr(k) for k in sorted(stale_tz)],
        "started": started.isoformat(), "finished": dk.utc_now().isoformat(),
        "range": {"start": f"{start:%Y-%m}", "end": f"{end:%Y-%m}"},
        "options": {"pause": client.pause, "attempts": client.attempts,
                    "connect_timeout": client.timeout[0], "read_timeout": client.timeout[1],
                    "backoff_base": client.throttle.base, "max_backoff": client.throttle.cap,
                    "max_consecutive_failures": client.throttle.breaker,
                    "rate_limit_pause": client.gate.min_pause,
                    "max_rate_limit_wait": client.gate.max_total_wait, "rebuild": rebuild,
                    "publication_lag_days": PUBLICATION_LAG_DAYS},
        "summary": {
            "months_considered": len(todo),
            "months_skipped_existing": skipped_existing,
            "months_written": [_ymstr(k) for k in sorted(written)],
            "months_unavailable": {_ymstr(k): v for k, v in sorted(unavailable.items())},
            "months_missing_in_history": {_ymstr(k): v for k, v in sorted(missing.items())},
            "months_pending": {_ymstr(k): v for k, v in sorted(pending.items())},
            "months_incomplete": [_ymstr(k) for k in incomplete],
            "months_quarantine": [_ymstr(k) for k in sorted(quarantine)],
            "months_store_conflict": [_ymstr(k) for k in sorted(conflicts)],
            "months_validation_error": [_ymstr(k) for k in sorted(validation_errors)],
            "months_rebuilt_timezone": [_ymstr(k) for k in sorted(stale_tz) if k in written],
            "months_stale_timezone_not_rebuilt": {
                _ymstr(k): v for k, v in sorted(stale_tz.items()) if k not in written},
            "history_known_from": _ymstr(ctx.first_data) if ctx.first_data else None,
            "files_downloaded": ctx.downloaded,
            "requests": client.requests,
            "http_status_counts": dict(sorted(client.status_counts.items())),
            "global_cooldowns": client.throttle.cooldowns,
            "circuit_breaker": tripped,
            "aborted": client.abort,
            "rate_limit": client.gate.summary(),
            "interrupted": client.stop.is_set(),
        },
        "hint": " | ".join(hints) or None,
        "error": state["error"],
        "validation_errors": [{"month": _ymstr(k), "error": v} for k, v in sorted(validation_errors.items())],
        "gaps": unit_rows(gaps),
        "content_errors": unit_rows(content),
        "pending": unit_rows(pend_units),
        "unconfirmed": unit_rows(unconfirmed),
        "quarantine": [{"month": _ymstr(k), **v} for k, v in sorted(quarantine.items())],
        "histdata_reported_gaps": reported_gaps,
    }


# ----------------------------------------------------------- verify-month ---
def verify_month(symbol: str, ym: tuple[int, int], data_root: Path | None = None, *,
                 mode: str = "tick", now=None, stop: threading.Event | None = None,
                 **client_kw) -> dict:
    """
    Ein UTC-Monat: Rohmonate M-1 und M laden (max. 4 Requests, Cache wird
    genutzt), decodieren, Zeitzone pruefen, Bars bauen, validieren. Schreibt
    nur den Verify-Report (kein Parquet). exit_code: 0 ok, 2 Validierung,
    3 Download/pending/429-Budget/Lock, 4 Zeitzone/Format (Quarantaene).
    """
    data_root = data_root or dk.default_data_root()
    now = now or dk.utc_now
    if not _month_closed(now(), ym, mode):
        raise ValueError(f"--verify-month {_ymstr(ym)}: Monat nicht abgeschlossen")
    if before_history(symbol, ym):
        raise ValueError(f"--verify-month {_ymstr(ym)}: vor Historienbeginn")
    lock = acquire_lock(data_root, symbol)
    if lock is None:
        return {"symbol": symbol, "month": _ymstr(ym), "status": "locked", "ok": False,
                "exit_code": EXIT_GAPS}
    try:
        ctx = Ctx(symbol, mode, data_root, Client(data_root, stop=stop, **client_kw), now)
        rep: dict = {"symbol": symbol, "month": _ymstr(ym), "source": SOURCE_BY_MODE[mode],
                     "mode": mode}
        try:
            outs = {_ymstr(r): ctx.get(ctx.unit(r)) for r in needed(symbol, ym, mode)}
            rep["files"] = {k: {"status": o.status, "error": o.error} for k, o in outs.items()}
            if any(o.status != "cached" and o.status != "ok" for o in outs.values()):
                rep.update(ok=False, exit_code=EXIT_GAPS,
                           note="Rohdatei(en) fehlen/leer/pending/Inhaltsfehler - nichts zu pruefen")
            else:
                df, stats = build_month(ctx, ym)
                rep.update(stats)
                rep.update(month_report(df, symbol, ym, mode, {}))
                rep.update(ok=True, exit_code=EXIT_OK)
        except Quarantined as q:
            rep.update(ok=False, exit_code=EXIT_MANUAL, reason=q.reason, timezone=q.check,
                       quarantine=str(q.unit.qmarker(data_root, q.ym)))
        except dk.ValidationError as e:
            rep.update(ok=False, exit_code=EXIT_VALIDATION, reason=str(e))
        except CorruptZip as e:
            rep.update(ok=False, exit_code=EXIT_GAPS, reason=f"ZIP defekt: {e}")
        except rl.RateLimitExhausted as e:
            rep.update(ok=False, exit_code=EXIT_GAPS, reason=f"429-Wartebudget erschoepft: {e}")
        rep["requests"] = ctx.client.requests
        rep["rate_limit"] = ctx.client.gate.summary()
        dk.write_json_atomic(verify_report_path(data_root, symbol, *ym, mode), rep)
        tzs = {k: ((v.get("timezone") or {}).get("median_offset_h"),
                   (v.get("timezone") or {}).get("median_deviation_h"))
               for k, v in rep.get("raw_months", {}).items()}
        log.info("VERIFY %s %s (%s, Rohzeit %s): ok=%s exit=%d Bars=%s Spread-Median=%s "
                 "(Offset, Abweichung) Median h=%s Requests=%d", symbol, _ymstr(ym), mode,
                 RAW_TZ[mode], rep["ok"], rep["exit_code"], rep.get("rows"),
                 rep.get("spread_median"), tzs, rep["requests"])
        return rep
    finally:
        lock.close()


# -------------------------------------------------------------------- cli ---
def _ym(s: str) -> tuple[int, int]:
    y, m = map(int, s.split("-"))
    if not 1 <= m <= 12:
        raise ValueError(s)
    return y, m


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--symbol", required=True, type=str.upper, choices=SYMBOLS)
    p.add_argument("--start", help="YYYY-MM (default: Historienbeginn des Symbols)")
    p.add_argument("--end", default=None, help="YYYY-MM (default: laufender Monat)")
    p.add_argument("--mode", choices=MODES, default="tick",
                   help="tick: Bid+Ask aus Ticks (default, source=histdata); m1: nur Bid, "
                        "ask = bid (eigene Partition source=histdata_m1)")
    p.add_argument("--pause", type=float, default=PAUSE_S,
                   help=f"Mindestabstand zwischen Requests in s (default {PAUSE_S})")
    p.add_argument("--attempts", type=int, default=ATTEMPTS,
                   help=f"Versuche je Datei, 429 zaehlt nicht (default {ATTEMPTS})")
    p.add_argument("--backoff-base", type=float, default=BACKOFF_BASE_S,
                   help=f"Abkuehlung nach Fehlern: base * 2^k s (default {BACKOFF_BASE_S})")
    p.add_argument("--max-backoff", type=float, default=MAX_BACKOFF_S,
                   help=f"Deckel der Abkuehlung in s (default {MAX_BACKOFF_S})")
    p.add_argument("--rate-limit-pause", type=float, default=rl.MIN_PAUSE_S,
                   help=f"Pause nach HTTP 429 in s, mind. {rl.MIN_PAUSE_S:g} (default {rl.MIN_PAUSE_S:g})")
    p.add_argument("--max-rate-limit-wait", type=float, default=rl.MAX_TOTAL_WAIT_S,
                   help=f"429-Wartebudget je Lauf in s, danach Exit 3 (default {rl.MAX_TOTAL_WAIT_S:g})")
    p.add_argument("--max-consecutive-failures", type=int, default=MAX_CONSECUTIVE_FAILURES,
                   help=f"Schutzschalter (default {MAX_CONSECUTIVE_FAILURES}, 0 = aus)")
    p.add_argument("--connect-timeout", type=float, default=CONNECT_TIMEOUT_S)
    p.add_argument("--read-timeout", type=float, default=READ_TIMEOUT_S)
    p.add_argument("--rebuild", action="store_true",
                   help="vorhandene Monatsdateien neu bauen (aus dem Roh-Cache); bestaetigt "
                        "leere Monate innerhalb der Historie erneut anfragen")
    p.add_argument("--verify-month", metavar="YYYY-MM",
                   help="einen Monat laden, decodieren, Zeitzone pruefen, validieren (Report)")
    p.add_argument("-v", "--verbose", action="store_true")
    a = p.parse_args(argv)
    if a.attempts < 1 or a.max_consecutive_failures < 0 or \
            min(a.pause, a.backoff_base, a.max_backoff, a.max_rate_limit_wait) < 0:
        p.error("--attempts >= 1, uebrige Zahlen >= 0")
    if a.rate_limit_pause < rl.MIN_PAUSE_S:
        p.error(f"--rate-limit-pause muss >= {rl.MIN_PAUSE_S:g} s sein (15 min)")
    try:
        start = _ym(a.start) if a.start else HISTORY_START[a.symbol]
        end = _ym(a.end) if a.end else (dk.utc_now().year, dk.utc_now().month)
        verify = _ym(a.verify_month) if a.verify_month else None
    except ValueError:
        p.error("Monate als YYYY-MM angeben")

    logging.basicConfig(level=logging.DEBUG if a.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)-7s %(message)s")
    stop = threading.Event()
    dk.install_stop_handler(stop)
    kw = dict(pause=a.pause, attempts=a.attempts, backoff_base=a.backoff_base,
              max_backoff=a.max_backoff, rate_limit_pause=a.rate_limit_pause,
              max_rate_limit_wait=a.max_rate_limit_wait,
              max_consecutive_failures=a.max_consecutive_failures,
              timeout=(a.connect_timeout, a.read_timeout), stop=stop,
              sleep=lambda s: stop.wait(s))          # Schlaf durch SIGTERM unterbrechbar
    if verify:
        try:
            return verify_month(a.symbol, verify, mode=a.mode, **kw)["exit_code"]
        except ValueError as e:
            p.error(str(e))
    rep = run(a.symbol, date(*start, 1), date(*end, 1), mode=a.mode, rebuild=a.rebuild, **kw)
    if rep["exit_code"] == EXIT_GAPS and rep.get("summary"):
        s = rep["summary"]
        log.error("LUECKEN: %d Dateien gescheitert, %d Inhaltsfehler, %d pending, %d unbestaetigt, "
                  "%d Monate unvollstaendig - Folgelauf holt nur Fehlendes nach", len(rep["gaps"]),
                  len(rep["content_errors"]), len(rep["pending"]), len(rep["unconfirmed"]),
                  len(s["months_incomplete"]))
    return rep["exit_code"]


if __name__ == "__main__":
    sys.exit(main())
