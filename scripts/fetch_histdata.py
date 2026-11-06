#!/usr/bin/env python3
"""
Historische HistData.com-Daten (XAUUSD, XAGUSD) als 1-Minuten-Bars mit Bid-
UND Ask-OHLC laden. Default-Quelle fuer den Gold/Silber-Bot (Dukascopy
drosselt per IP mit HTTP 429, s. fetch_dukascopy.py). EURUSD kommt nicht
mehr von hier (keine Tagespause, zu wenige Zeitanker, --verify-month 2024-11
bestand faelschlich): der Bot nutzt einen Tages-Referenzkurs (EZB/FRED);
--symbol EURUSD wird mit diesem Hinweis abgelehnt.

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

Zeitzone = Stempelungsregel (TzRule: Rohzeit = Ortszeit in `zone` + shift_h)
je ROHmonat nach Fahrplan in config/histdata_timezone.yaml (load_tz_config,
rule_for; andere Datei per $HISTDATA_TZ_CONFIG; Datei muss existieren und ab
spaetestens 2009-02 gelten; Fehler -> Abbruch beim Laden, nie still ein
Default). Wechsel nur an Rohmonatsgrenzen.
- tick (VORLAEUFIGER Fahrplan nach Survey, s. Kommentar in der YAML und
  docs/histdata_research.md): ny_local (America/New_York) bis 2018-11 -
  Survey 2010/2012/2015/2018 eindeutig; "undecided" (Kandidaten ny_local,
  berlin_minus_6) 2018-12 .. 2021-02 - Wechsel liegt zwischen 2018-11 und
  2021-03; berlin_minus_6 (Europe/Berlin - 6 h: UTC-5 im EU-Winter, UTC-4 in
  der EU-Sommerzeit) ab 2021-03 - 2021-03/2024-03 eindeutig, 2021-10/2024-10
  bis auf einen Ruecksprung im geschlossenen Wochenendmarkt. Beide Regeln
  rechnen nur in den EU/US-Lueckenwochen verschieden (Maerz: US-Beginn 2. So
  07:00 UTC bis EU-Beginn letzter So 01:00 UTC; Okt/Nov: EU-Ende letzter So
  Okt 01:00 UTC bis US-Ende 1. So Nov 06:00 UTC). Die Marktzeiten (Anker)
  bleiben New York/CME: 17:00/18:00 America/New_York.
  "undecided" (UndecidedRule, rule_for): raw_to_utc_ms rechnet alle
  Kandidaten; identisch -> Ergebnis, sonst RuleUndecided. Ein UTC-Monat, in
  den ein verschieden umgerechneter Tick (unter irgendeinem Kandidaten) fallen
  kann, wird NICHT geschrieben: Laufreport months_rule_undecided (Rohbereiche),
  manueller Fall (Exit 4, 3 hat Vorrang), kein Marker, keine Requests, jeder
  Lauf prueft neu aus dem Cache. Pruefung sonst unter dem ersten Kandidaten
  (mit abweichenden Ticks: bestanden, wenn ein Kandidat passt).
- m1: fixed_est = Etc/GMT+5 (fest UTC-5) laut HistData-Angabe, fuer M1 NICHT
  verifiziert: mit --mode m1 --verify-month 2024-07 (Sommermonat) pruefen.
Andere Regeln offline gegen den Roh-Cache abgleichen: evaluate_raw_month
(scripts/histdata_tz_survey.py).
Umrechnung raw_to_utc_ms(raw, rule) (vektorisiert, exakt auf die
Millisekunde). Ortszeiten in der Sommerzeit-Luecke oder der doppelten Stunde
der Regel-Zone (berlin_minus_6: roh Samstag 20:00-20:59:59.999 in den
Umstellungsnaechten) werden NIE geraten: TimezoneError -> Quarantaene (Exit
4); der Markt ist dann ohnehin zu. Folge: UTC-Monat M braucht die Rohmonate
M-1 (dessen letzte 4 bzw. 5 h, aus dem Fahrplan berechnet) und M; er gilt nur
als vollstaendig, wenn beide da sind (oder M-1 vor Historienbeginn bzw.
bestaetigt leer ist).
Die Zeitzone wird JE ROHMONAT aus den Daten geprueft (check_timezone, v6):
je Marktluecke ein Schluss- und ein Oeffnungsanker - Wochenende (>= 24 h,
Schluss Do/Fr, Oeffnung So/Mo: Fr 17:00 -> So 18:00 New York; andere Luecken >= 24 h sind Ausfaelle, nur Bericht), bei Metallen
die Tagespause Mo-Do (17:00-18:00 New York; nur wenn alle pausenartigen
Luecken des Tages dieselbe eindeutige Abweichung <= 1 h ergeben). Die
Abweichung einer Luecke wird aus beiden Seiten gemeinsam bestimmt (Schluss
nicht vor dem letzten Tick, Toleranz 15 min; Oeffnung nicht nach dem ersten,
5 min) - auch bei duennen Daten richtig. Erwartet je Anker: Offset von
der Stempelungsregel im Ereigniszeitpunkt (berlin_minus_6: -5 h im
EU-Winter, -4 h in der EU-Sommerzeit; fixed_est: immer -5 h). Abweichung =
implizit - erwartet.
HART (Quarantaene, Exit 4) nur Systematisches oder Bestaetigtes: Median-
Abweichung != 0, Anteil 0 < 60 %, >= 2 UNABHAENGIGE Luecken in Folge mit
derselben Abweichung != 0 ohne 0 dazwischen (auch ueber den Monatsrand: die
Ticks der ersten/letzten 8 Tage der gecachten Nachbar-Rohmonate sind
Kontext), Ruecksprung >= 30 min, nicht eindeutige Ortszeit. Nur WARNUNG:
einzelne Ausreisser (verirrter Tick, Feed-Kante genau 1 h daneben,
Feiertag, Ausfall), widerspruechliche/mehrdeutige Luecken.
Reihenfolge: Ergebnisse ohne einen Nachbar-Rohmonat, der existieren sollte
(abgeschlossen, in der Historie, nicht im Cache), sind VORLAEUFIG: weder
gemerkt noch als Marker gespeichert. Ein einzelner abweichender Eintrag am
Monatsrand ohne Kontext dahinter haelt den Monat zurueck, bis der Nachbar
da ist. Fehlt der rechte Nachbar (neuester Monat), wird mit
tz_right_padding=false geschrieben und nachgeprueft, sobald Rohmonat M+1 im
Cache ist (besteht -> Flag true, sonst aus dem Store + Quarantaene). Reicht
ein bestaetigter Wechsel im Folge-Rohmonat in Monat M zurueck, wird M nicht
geschrieben (check_following). Zu wenige Anker -> gemeinsam mit dem
Nachbar-Rohmonat pruefen.
Parquet-Metadatum source_timezone = store_tz_label(mode, ym): Regel(n) der
Rohmonate mit Zone/Verschiebung und TZ_CHECK_VERSION (6), z.B.
"berlin_minus_6 (Europe/Berlin -6 h) -> UTC, je Rohmonat geprueft (tz_check v6)".
v5-Dateien mit berlin_minus_6 fuer Monate, die jetzt ny_local/undecided sind:
in den Lueckenwochen verschoben -> metals/stale/ und neu gebaut (bzw. als
rule_undecided zurueckgehalten), sonst nur neu gekennzeichnet.
Zu Beginn JEDES Laufs werden alle Monatsdateien
des Symbols/Modus geprueft (classify_store_file):
- andere oder aeltere Kennzeichnung, im UTC-Monat aber dieselben UTC-Zeiten
  (aeltere Pruefversion, oder z.B. ny_local-Dateien von Januar/Juli): Roh-
  monate aus dem Cache neu pruefen (kein Download);
  besteht -> nur Metadaten erneuern, faellt durch -> nach metals/stale/ +
  Quarantaene, Roh-Cache fehlt -> Datei bleibt (unverified_after_version_bump);
- tz_right_padding=false und Rohmonat M+1 jetzt im Cache: ebenso nachpruefen
  (auch am Laufende);
- andere Umrechnung mit anderen UTC-Zeiten im Monat (ny_local: Maerz,
  Oktober, November = die EU/US-Lueckenwochen; fest UTC-5: alle Monate mit
  EU-Sommerzeit): sofort nach metals/stale/ verschieben, auch ausserhalb
  von --start/--end (Laufreport stale_retired_out_of_range).
Monate in [start, end] werden aus dem Roh-Cache neu gebaut (WARNING, Laufreport
"rebuilt_timezone"); fehlt der Roh-Cache, wird normal geladen (Requests).
--verify-month ist nur Bericht: Zustand der Monatsdatei (store_file), keine
Quarantaene-Marker schreiben oder loeschen.
Quarantaene-Marker tragen tz_rule, raw_tz (Zone), tz_shift_h und
tz_check_version. Nicht freigegebene Marker aelterer Version/anderer Regel
(z.B. v4 ny_local): neu pruefen (ohne Download). "accepted": true gilt nur
fuer dieselbe Regel (Zone + Verschiebung); eine Freigabe unter anderer
Regel wird neu geprueft - besteht: Marker weg, sonst neuer Marker accepted
false mit Hinweis. Unlesbarer Marker: ERROR, bleibt, sperrt weiter.
Ruecksprung >= 30 min (large_reversals mit at_raw, back_to_raw, minutes,
n_ticks, first_raw, last_raw, weekend_closed; betroffen = alle Ticks mit
Rohzeit in [back_to_raw, at_raw]) und nicht eindeutige Ortszeit sperren per
Default. Liegen ALLE betroffenen Ticks (Ruecksprung und verbotene Stunden) im
geschlossenen Wochenendmarkt nach Rohuhr (WEEKEND_CLOSED_RAW: Fr ab 18:00,
ganzer Sa, So vor 16:00 - konservativ ueber alle Regeln/Offsets), traegt
Bericht und Marker releasable_weekend (reversal_ticks, impossible_hour_ticks,
raw_ranges). Freigabe mit --accept-weekend-ticks YYYY-MM (ROHmonat, mit
--symbol/--mode; gibt die Ticks aus, kein Download; sonst Exit 2): genau
diese Ticks werden VERWORFEN (nie umgerechnet), der Rest neu geprueft und
bei Erfolg geschrieben; Monatsreport dropped_ticks, dropped_raw, warning.
Ausserhalb des geschlossenen Wochenendmarkts nie freigebbar.
EURUSD-Storeverzeichnisse aus Pruefversion 4 verschiebt jeder Lauf nach
metals/stale/ (retired_removed_symbols).

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
     leerer Monat INNERHALB der Historie, Monatsdatei mit anderem Modus,
     Fahrplan "undecided" mit verschieden umgerechneten Ticks
     (months_rule_undecided; status "rule_undecided", wenn nur das)
  0  vollstaendig
  3 geht bewusst vor 4: die systemd-Units starten bei 3 neu, bei 4 nicht -
  ein Exit 4 bei offenen Luecken wuerde den Backfill anhalten. Quarantaene
  ist trotzdem nicht zu uebersehen: ERROR-Zeile in jedem Lauf, Laufreport
  status "...+quarantine" und Liste "quarantined" auf oberster Ebene.

Lizenz: HistData-Nutzungsbedingungen selbst pruefen. Daten (roh wie
abgeleitet) nicht weitergeben und nicht committen (data/ ist git-ignored).

Usage:
    python fetch_histdata.py --symbol XAUUSD --start 2009-03
    python fetch_histdata.py --symbol XAGUSD --start 2024-01 --end 2024-06 --mode m1
    python fetch_histdata.py --symbol XAUUSD --verify-month 2024-03
    python fetch_histdata.py --symbol XAUUSD --accept-weekend-ticks 2024-10   # Exit 0/2
    python fetch_histdata.py --symbol XAUUSD --mode m1 --verify-month 2024-07   # M1-Zeitzone pruefen
"""
from __future__ import annotations

import argparse
import fcntl
import functools
import io
import json
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

SYMBOLS = ("XAUUSD", "XAGUSD")
# EURUSD kommt NICHT mehr von HistData: keine Tagespause, zu wenige Zeitanker
# (2024-11 bestand --verify-month faelschlich). Der Bot nutzt einen Tages-
# Referenzkurs (EZB/FRED); fetch_dukascopy.py bleibt unveraendert.
REMOVED_SYMBOLS = {"EURUSD": "EURUSD wird nicht mehr ueber HistData geladen (keine Tagespause, "
                             "zu wenige Zeitanker, --verify-month 2024-11 bestand faelschlich) - "
                             "der Bot nutzt dafuer einen Tages-Referenzkurs (EZB/FRED)"}
METALS = ("XAUUSD", "XAGUSD")
MODES = ("tick", "m1")
TF_PATH = {"tick": "tick-data-quotes", "m1": "1-minute-bar-quotes"}
TF_CODE = {"tick": "T", "m1": "M1"}
PARQUET_MODE = {"tick": "histdata_tick", "m1": "histdata_m1"}

# Erster Monat laut HistData-Auswahlliste (ASCII M1; Quellen: histdatacom
# data/.repo, ai-trader QA). Fuer TICKS nicht verifiziert - fehlen dort
# fruehe Monate, werden sie als "keine Daten" bestaetigt (24 h) und gelistet.
HISTORY_START = {"XAUUSD": (2009, 3), "XAGUSD": (2009, 5)}

HOUR_MS, MIN_MS, DAY_MS = 3_600_000, 60_000, 86_400_000

# Version der Zeitzonen-Pruefung; steht in jedem Quarantaene-Marker und im
# Parquet-Metadatum. v1 = fest UTC-5 fuer beide Modi (ohne Versionsfeld),
# v2 = Zone je Modus, v3 = Anker je Luecke (duenne Daten, Nachbar-Randticks,
# Umstellungsfenster), v4 = hart nur Systematisches/Bestaetigtes (>= 2 unabhaengige
# Luecken), Einzelausreisser nur Warnung, vorlaeufige Ergebnisse nie gespeichert,
# v5 = Stempelungsregel je Rohmonat (TzRule, config/histdata_timezone.yaml;
# Ticks: Europe/Berlin - 6 h statt America/New_York, im Homelab gemessen),
# v6 = vorlaeufiger Fahrplan nach Survey (ny_local bis 2018-11, "undecided"
# 2018-12..2021-02, berlin_minus_6 ab 2021-03), Rueckspruenge mit betroffenen
# Ticks, verwerfbare Wochenend-Ticks (releasable_weekend). v5-Dateien mit
# berlin_minus_6 fuer heute ny_local/undecided-Monate: verschoben -> metals/stale/
# und neu gebaut, gleich -> nur neu gekennzeichnet.
# Nicht freigegebene Marker und Store-Dateien aelterer Version werden neu geprueft.
TZ_CHECK_VERSION = 6
REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_TZ_CONFIG = REPO_ROOT / "config" / "histdata_timezone.yaml"
TZ_CONFIG_ENV = "HISTDATA_TZ_CONFIG"
# Fruehere Metadaten -> damalige Umrechnung (Zone, Verschiebung) fuer "gleiche Umrechnung?"
LEGACY_TZ_META = {"EST fixed UTC-5, converted to UTC, checked per month": ("Etc/GMT+5", 0)}
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
# Offset = Rohzeit - UTC in Stunden; erwartet je Anker aus der Stempelungsregel,
# Abweichung = impliziter - erwarteter Offset (soll 0 sein)
TZ_MIN_ANCHORS = 3         # so viele eindeutige Anker (eigene Seite) mindestens, sonst nicht pruefbar
TZ_MIN_SHARE = 0.6         # Anteil der eindeutigen Anker mit Abweichung 0
TZ_MAX_RUN = 2             # so viele UNABHAENGIGE Luecken mit gleicher Abweichung != 0 in Folge = Wechsel
TZ_EDGE_MS = 72 * HOUR_MS  # Monatsrand (nur Bericht)
TZ_BREAK_MAX_DEV_H = 1     # Tagespausen mit groesserer Abweichung sind Ausfaelle, keine Anker
TZ_TOL_MS = 15 * MIN_MS    # Ticks so weit NACH dem Schluss gelten noch als Rand (verirrte Ticks, 17:15-Schluss)
TZ_TOL_OPEN_MS = 5 * MIN_MS  # ... so weit VOR der Oeffnung (enger: sonst wirkt eine duenne verschobene Oeffnung richtig)
TZ_TIGHT_MS = 10 * MIN_MS  # Einzelseite: Tick so nah an einer vollen Stunde ...
TZ_ACTIVE_MS = 5 * MIN_MS  # ... und Markt dort aktiv (Nachbartick so nah)
TZ_TRANSITION_MS = 7 * DAY_MS   # Umstellungsfenster (nur Bericht)
TZ_PAD_MS = 8 * DAY_MS     # so viele Tage der Nachbar-Rohmonate als Kontext (Laeufe ueber den Monatsrand)
REGRESSION_MAX_MS = 30 * MIN_MS  # Ruecksprung in Dateireihenfolge ab dem -> Uhrumstellung
WEEKEND_GAP_MS = 24 * HOUR_MS
BREAK_GAP_MS = (45 * MIN_MS, 3 * HOUR_MS)
CLOSE_H_NY = 17
OPEN_H_NY = {"XAUUSD": 18, "XAGUSD": 18}

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


def retire_removed_symbol_stores(data_root: Path) -> list[dict]:
    """Store-Verzeichnisse nicht mehr unterstuetzter Symbole (REMOVED_SYMBOLS,
    z.B. EURUSD aus Pruefversion 4 mit falscher Stempelung) nach metals/stale/
    verschieben - der Bot soll sie nicht mehr lesen. Roh-Cache und Marker
    bleiben unberuehrt. Rueckgabe: [{symbol, mode, from, to}]."""
    out = []
    for sym in REMOVED_SYMBOLS:
        for mode in MODES:
            d = store_dir(data_root, sym, mode)
            if not d.is_dir():
                continue
            rel = d.relative_to(data_root / "metals")
            dest = data_root / "metals" / "stale" / rel.parent / (
                f"{d.name}.{datetime.now(UTC):%Y%m%dT%H%M%S%f}.stale")
            dest.parent.mkdir(parents=True, exist_ok=True)
            os.replace(d, dest)
            dk._fsync_dir(d.parent)
            log.warning("%s wird nicht mehr ueber HistData geladen: Store %s nach %s verschoben "
                        "(Roh-Cache bleibt)", sym, d, dest)
            out.append({"symbol": sym, "mode": mode, "from": str(d), "to": str(dest)})
    return out


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


# ------------------------------------------------ stempelungsregeln (v5) ---
@dataclass(frozen=True)
class TzRule:
    """Stempelungsregel: Rohzeit (Wanduhr-Ziffern der Datei) = Ortszeit in `zone`
    + shift_h Stunden. berlin_minus_6: Europe/Berlin - 6 h = UTC-5 im EU-Winter,
    UTC-4 in der EU-Sommerzeit (Umstellung letzter So Maerz/Oktober 01:00 UTC)."""
    name: str
    zone: str
    shift_h: int

    def describe(self) -> str:
        return f"{self.name} ({self.zone} {self.shift_h:+d} h)"


@dataclass(frozen=True)
class UndecidedRule:
    """Fahrplan-Eintrag "undecided": welche der Kandidatenregeln gilt, ist noch
    nicht belegt (z.B. Wechsel ny_local -> berlin_minus_6 irgendwann zwischen
    2018-11 und 2021-03). Umrechnung nur, wo ALLE Kandidaten dasselbe ergeben
    (raw_to_utc_ms), sonst RuleUndecided; Pruefung unter dem ersten Kandidaten."""
    name: str = "undecided"
    candidates: tuple = ()

    @property
    def first(self) -> TzRule:
        return self.candidates[0]

    def describe(self) -> str:
        # Label wie die erste Regel (gleiche Umrechnung, wo geschrieben wird) plus Kandidaten
        return (f"{self.first.describe()} [undecided: "
                + "|".join(c.name for c in self.candidates) + ", hier identisch]")


class RuleUndecided(Exception):
    """Kandidatenregeln eines "undecided"-Zeitraums rechnen Ticks verschieden um:
    nicht schreiben, nicht raten (kein Marker; jeder Lauf prueft neu aus dem Cache).
    differing_raw: Rohzeit-ms der abweichenden Ticks; ranges: [(von, bis)] roh."""

    def __init__(self, msg: str, differing_raw=None, ranges=None):
        super().__init__(msg)
        self.differing_raw = differing_raw if differing_raw is not None else np.array([], dtype=np.int64)
        self.ranges = ranges or []
        self.raw_month = None


def concrete_rule(rule) -> TzRule:
    """Konkrete Regel fuer Pruefung/Offsets: bei "undecided" der erste Kandidat."""
    return rule.first if isinstance(rule, UndecidedRule) else rule


def rule_candidates(rule) -> tuple:
    return rule.candidates if isinstance(rule, UndecidedRule) else (rule,)


def rule_identity(rule) -> tuple[str, int]:
    """(Zone, Verschiebung) bzw. fuer "undecided" ein eigener Schluessel (Marker)."""
    if isinstance(rule, UndecidedRule):
        return "undecided:" + "|".join(c.name for c in rule.candidates), 0
    return rule.zone, rule.shift_h


TZ_RULES = {"berlin_minus_6": TzRule("berlin_minus_6", "Europe/Berlin", -6),
            "ny_local": TzRule("ny_local", "America/New_York", 0),
            "fixed_est": TzRule("fixed_est", "Etc/GMT+5", 0)}
# Default-Fahrplan, falls die Konfiguration keinen liefert (identisch mit der
# ausgelieferten config/histdata_timezone.yaml). M1 ist NICHT verifiziert.
DEFAULT_TZ_SCHEDULE = {"tick": [((2000, 1), "berlin_minus_6")], "m1": [((2000, 1), "fixed_est")]}
# (nur fuer build_tz_config(None) ohne Datei; load_tz_config verlangt immer eine Datei)


@dataclass(frozen=True)
class TzConfig:
    rules: dict                    # name -> TzRule (eingebaute + Konfiguration)
    schedule: dict                 # mode -> tuple[((y, m), TzRule), ...], aufsteigend
    path: str | None = None


def _check_zone(zone: str, where: str) -> None:
    try:
        ZoneInfo(str(zone))
    except Exception as e:                       # ZoneInfoNotFoundError, ValueError
        raise ValueError(f"{where}: unbekannte Zeitzone {zone!r} ({e})")


def _parse_ym_str(v, where: str) -> tuple[int, int]:
    m = re.fullmatch(r"(\d{4})-(\d{2})", str(v))
    if not m or not 1 <= int(m.group(2)) <= 12:
        raise ValueError(f"{where}: 'from' muss YYYY-MM sein, nicht {v!r}")
    return int(m.group(1)), int(m.group(2))


def build_tz_config(data: dict | None, path: str | None = None) -> TzConfig:
    """Konfiguration pruefen und aufbauen; jeder Fehler -> ValueError (nie still ein Default)."""
    data = data or {}
    if not isinstance(data, dict):
        raise ValueError(f"{path}: oberste Ebene muss ein Mapping sein")
    unknown = set(data) - {"rules", "schedule"}
    if unknown:
        raise ValueError(f"{path}: unbekannte Schluessel {sorted(unknown)}")
    rules = dict(TZ_RULES)
    for i, r in enumerate(data.get("rules") or []):
        where = f"{path}: rules[{i}]"
        if not isinstance(r, dict) or set(r) != {"name", "zone", "shift_h"}:
            raise ValueError(f"{where}: genau name, zone, shift_h erwartet, nicht {r!r}")
        name = str(r["name"])
        if not re.fullmatch(r"[a-z0-9_]+", name):
            raise ValueError(f"{where}: Name {name!r} nur a-z, 0-9, _")
        if name in rules:
            raise ValueError(f"{where}: Regelname {name!r} doppelt (auch eingebaute Namen sind belegt)")
        _check_zone(r["zone"], where)
        if not isinstance(r["shift_h"], int) or isinstance(r["shift_h"], bool) or abs(r["shift_h"]) > 14:
            raise ValueError(f"{where}: shift_h muss ganze Stunden (-14..14) sein, nicht {r['shift_h']!r}")
        rules[name] = TzRule(name, str(r["zone"]), int(r["shift_h"]))
    sched_in = data.get("schedule")
    if sched_in is None:
        if path is not None:
            raise ValueError(f"{path}: Abschnitt schedule fehlt")
        sched_in = {m: [{"from": f"{y}-{mm:02d}", "rule": n} for (y, mm), n in v]
                    for m, v in DEFAULT_TZ_SCHEDULE.items()}
    if not isinstance(sched_in, dict) or set(sched_in) != set(MODES):
        raise ValueError(f"{path}: schedule braucht genau die Modi {list(MODES)}")
    schedule = {}
    for mode in MODES:
        entries = sched_in[mode]
        if not entries:
            raise ValueError(f"{path}: schedule.{mode} ist leer")
        out = []
        for i, e in enumerate(entries):
            where = f"{path}: schedule.{mode}[{i}]"
            if not isinstance(e, dict) or "from" not in e or "rule" not in e:
                raise ValueError(f"{where}: from, rule erwartet, nicht {e!r}")
            ym = _parse_ym_str(e["from"], where)
            if e["rule"] == "undecided":
                if set(e) != {"from", "rule", "candidates"}:
                    raise ValueError(f"{where}: undecided braucht genau from, rule, candidates")
                cands = e["candidates"]
                if not isinstance(cands, list) or len(cands) < 2 or len(set(map(str, cands))) != len(cands):
                    raise ValueError(f"{where}: candidates = Liste von >= 2 verschiedenen Regeln, "
                                     f"nicht {cands!r}")
                bad = [c for c in cands if c not in rules]
                if bad:
                    raise ValueError(f"{where}: unbekannte Kandidaten {bad}; bekannt: {sorted(rules)}")
                rule = UndecidedRule(candidates=tuple(rules[c] for c in cands))
            else:
                if set(e) != {"from", "rule"}:
                    raise ValueError(f"{where}: genau from, rule erwartet (candidates nur bei "
                                     f"undecided), nicht {e!r}")
                if e["rule"] not in rules:
                    raise ValueError(f"{where}: unbekannte Regel {e['rule']!r}; bekannt: "
                                     f"{sorted(rules)} oder undecided")
                rule = rules[e["rule"]]
            if out and ym <= out[-1][0]:
                raise ValueError(f"{where}: 'from' {e['from']} nicht aufsteigend/doppelt")
            out.append((ym, rule))
        earliest = _ym_add(min(HISTORY_START.values()), -1)   # Rohmonat vor dem ersten UTC-Monat
        if out[0][0] > earliest:
            raise ValueError(f"{path}: schedule.{mode} beginnt erst {out[0][0][0]}-{out[0][0][1]:02d}; "
                             f"er muss spaetestens ab {earliest[0]}-{earliest[1]:02d} gelten "
                             f"(fruehester benoetigter Rohmonat: Historienbeginn "
                             f"{min(HISTORY_START.values())} minus 1 Monat)")
        schedule[mode] = tuple(out)
    return TzConfig(rules, schedule, path)


_TZ_CFG_CACHE: dict = {}


def load_tz_config(path: str | Path | None = None) -> TzConfig:
    """config/histdata_timezone.yaml (oder $HISTDATA_TZ_CONFIG, oder `path`) laden
    und pruefen. Die Datei muss existieren (auch die Default-Datei im Repo: kein
    stiller Rueckfall auf eingebaute Werte) und einen Abschnitt schedule haben.
    Fehler -> ValueError."""
    p = Path(path or os.environ.get(TZ_CONFIG_ENV) or DEFAULT_TZ_CONFIG)
    if not p.exists():
        raise ValueError(f"Zeitzonen-Konfiguration {p} fehlt (Default: {DEFAULT_TZ_CONFIG}, "
                         f"andere Datei per ${TZ_CONFIG_ENV}) - ohne sie wird nichts umgerechnet")
    key = (str(p.resolve()), p.stat().st_mtime_ns)
    if key not in _TZ_CFG_CACHE:
        import yaml
        try:
            data = yaml.safe_load(p.read_text())
        except yaml.YAMLError as e:
            raise ValueError(f"{p}: kein gueltiges YAML ({e})")
        _TZ_CFG_CACHE[key] = build_tz_config(data, str(p))
    return _TZ_CFG_CACHE[key]


def rule_for(mode: str, ym: tuple[int, int], cfg: TzConfig | None = None) -> "TzRule | UndecidedRule":
    """Stempelungsregel fuer den ROHmonat ym (Wechsel nur an Rohmonatsgrenzen);
    UndecidedRule fuer Zeitraeume mit "undecided"."""
    cfg = cfg or load_tz_config()
    if mode not in cfg.schedule:
        raise ValueError(f"unbekannter Modus {mode}")
    found = None
    for start, rule in cfg.schedule[mode]:
        if tuple(ym) >= start:
            found = rule
    if found is None:
        raise ValueError(f"keine Stempelungsregel fuer {mode} {ym[0]}-{ym[1]:02d} "
                         f"(Fahrplan beginnt {cfg.schedule[mode][0][0]})")
    return found


def as_rule(rule: "TzRule | str", cfg: TzConfig | None = None) -> TzRule:
    if isinstance(rule, (TzRule, UndecidedRule)):
        return rule
    cfg = cfg or load_tz_config()
    if rule not in cfg.rules:
        raise ValueError(f"unbekannte Stempelungsregel {rule!r}; bekannt: {sorted(cfg.rules)}")
    return cfg.rules[rule]


def source_tz_label(rules) -> str:
    """Parquet-Metadatum source_timezone: Regel(n) mit Zone/Verschiebung und
    Pruefversion (abgeleitet, nie von Hand gepflegt)."""
    rules = [rules] if isinstance(rules, (TzRule, UndecidedRule)) else list(dict.fromkeys(rules))
    return " + ".join(r.describe() for r in rules) + \
        f" -> UTC, je Rohmonat geprueft (tz_check v{TZ_CHECK_VERSION})"


def store_tz_label(mode: str, ym: tuple[int, int]) -> str:
    """Erwartetes source_timezone fuer UTC-Monat ym (Regeln seiner Rohmonate)."""
    return source_tz_label([rule_for(mode, r) for r in _raw_months_for(ym, mode)])


class _SourceTzMeta(dict):
    """SOURCE_TZ_META[mode]: Label der juengsten Regel des Fahrplans (Bequemlichkeit;
    massgeblich je Monat ist store_tz_label)."""

    def __getitem__(self, mode):
        return source_tz_label(load_tz_config().schedule[mode][-1][1])

    def __contains__(self, mode):
        return mode in MODES

    def keys(self):
        return list(MODES)

    def __iter__(self):
        return iter(MODES)

    def __len__(self):
        return len(MODES)


SOURCE_TZ_META = _SourceTzMeta()


def _zone_offset_ms(zone: str, utc_ms: np.ndarray) -> np.ndarray:
    T, O = _table_for(zone, utc_ms)
    return O[np.searchsorted(T, utc_ms, side="right") - 1]


def utc_to_raw_ms(utc_ms: np.ndarray, rule: TzRule) -> np.ndarray:
    """UTC-ms -> Rohzeit-ms nach `rule` (Ortszeit in rule.zone + shift_h); immer eindeutig.
    "undecided": nach dem ersten Kandidaten (nur fuer Monatsgrenzen/Planung)."""
    rule = concrete_rule(rule)
    utc_ms = np.asarray(utc_ms, dtype=np.int64)
    if not len(utc_ms):
        return utc_ms.copy()
    return utc_ms + _zone_offset_ms(rule.zone, utc_ms) + rule.shift_h * HOUR_MS


def _ranges_of(raw_ms: np.ndarray, gap_ms: int = HOUR_MS) -> list[tuple[str, str]]:
    """Zusammenhaengende Rohzeit-Bereiche (Luecke > gap_ms trennt) als ISO-Paare."""
    u = np.unique(np.asarray(raw_ms, dtype=np.int64))
    if not len(u):
        return []
    cuts = np.nonzero(np.diff(u) > gap_ms)[0]
    starts = np.concatenate([[0], cuts + 1])
    ends = np.concatenate([cuts, [len(u) - 1]])
    return [(_raw_iso(int(u[a]), ms_digits=True), _raw_iso(int(u[b]), ms_digits=True))
            for a, b in zip(starts, ends)]


def _raw_to_utc_undecided(raw_ms: np.ndarray, rule: "UndecidedRule") -> np.ndarray:
    """Alle Kandidaten rechnen; nur bei identischem Ergebnis zurueck, sonst RuleUndecided."""
    outs, errs = [], []
    for c in rule.candidates:
        try:
            outs.append(raw_to_utc_ms(raw_ms, c))
            errs.append(None)
        except TimezoneError as e:
            outs.append(None)
            errs.append(e)
    if all(e is not None for e in errs):
        raise errs[0]                            # ueberall nicht eindeutig: normale Behandlung
    diff = np.zeros(len(raw_ms), dtype=bool)
    ref = next(o for o in outs if o is not None)
    for o, e in zip(outs, errs):
        diff |= getattr(e, "bad_mask", np.ones(len(raw_ms), bool)) if o is None else (o != ref)
    if diff.any():
        ranges = _ranges_of(raw_ms[diff])
        raise RuleUndecided(
            f"{int(diff.sum())} Tick(s), die die Kandidaten "
            f"{', '.join(c.name for c in rule.candidates)} verschieden umrechnen (Zeitraum "
            f"'undecided'), roh z.B. {', '.join(f'{a}..{b}' for a, b in ranges[:5])} - nicht "
            "geschrieben, bis der Fahrplan entschieden ist", raw_ms[diff], ranges)
    return ref


def raw_to_utc_ms(raw_ms: np.ndarray, rule: TzRule) -> np.ndarray:
    """
    Rohzeit-ms (Wanduhr-Ziffern, als UTC gelesen) -> UTC-ms nach `rule`
    (int64, gleiche Reihenfolge/Laenge): roh - shift_h ist Ortszeit in
    rule.zone. Vektorisiert: je moeglichem Offset der Zone Kandidat
    u = ortszeit - offset; gueltig, wenn die Zone in u genau diesen Offset hat
    (searchsorted in der Wechseltabelle, exakt auf die Millisekunde).
    Kein gueltiger Kandidat = Ortszeit existiert nicht (Sommerzeitbeginn), zwei
    = mehrdeutig (Sommerzeitende) -> TimezoneError mit Beispielen (Rohzeit);
    nie raten. berlin_minus_6: roh Samstag 20:00-20:59:59.999 in den
    Umstellungsnaechten (Markt zu); ny_local: 02:00-02:59 bzw. 01:00-01:59.
    """
    raw_ms = np.asarray(raw_ms, dtype=np.int64)
    if not len(raw_ms):
        return raw_ms.copy()
    if isinstance(rule, UndecidedRule):
        return _raw_to_utc_undecided(raw_ms, rule)
    local = raw_ms - rule.shift_h * HOUR_MS
    T, O = _table_for(rule.zone, local)
    cands = np.unique(O)
    if len(cands) == 1:                         # feste Zone (Etc/GMT+5)
        return local - cands[0]
    out = np.zeros(len(raw_ms), dtype=np.int64)
    n_ok = np.zeros(len(raw_ms), dtype=np.int8)
    for off in cands:
        u = local - off
        ok = O[np.searchsorted(T, u, side="right") - 1] == off
        out[ok] = u[ok]
        n_ok += ok
    if (n_ok != 1).any():
        bad = n_ok != 1
        msg = []
        for mask, what in ((n_ok == 0, "nicht existierender Ortszeit (Sommerzeitbeginn)"),
                           (n_ok > 1, "mehrdeutiger Ortszeit (Sommerzeitende)")):
            idx = np.nonzero(mask)[0]
            if len(idx):
                ex = ", ".join(_raw_iso(int(raw_ms[i]), ms_digits=True) for i in idx[:5])
                msg.append(f"{len(idx)} Zeitstempel (roh) in {what} nach {rule.describe()}, z.B. {ex}")
        err = TimezoneError("; ".join(msg) + " - Markt ist dann geschlossen, nicht umgerechnet")
        err.bad_mask = bad
        raise err
    return out


# Geschlossener Wochenendmarkt nach ROHuhr, konservativ ueber alle Regeln und
# Offsets (Schluss Fr 17:00 New York = roh 16:00-18:00, Oeffnung So 18:00 New
# York = roh 17:00-19:00 je nach Regel/Woche): Freitag ab 18:00, ganzer Samstag,
# Sonntag vor 16:00. (Wochentag Mo = 0, Stunde) von inklusive bis exklusive.
# Nur Ticks hier duerfen nach menschlicher Freigabe verworfen werden.
WEEKEND_CLOSED_RAW = ((4, 18), (6, 16))


def weekend_closed_raw(raw_ms: np.ndarray) -> np.ndarray:
    """Rohzeit im geschlossenen Wochenendmarkt (WEEKEND_CLOSED_RAW: Fr >= 18:00,
    Sa, So < 16:00) - dort liegen die verbotenen Stunden der Regeln
    (berlin_minus_6: Sa 20:00-20:59, ny_local: So 01:00-02:59) und verirrte
    Ticks der Umstellungsnaechte."""
    raw_ms = np.asarray(raw_ms, dtype=np.int64)
    (wa, ha), (wb, hb) = WEEKEND_CLOSED_RAW
    pos = (raw_ms + 3 * DAY_MS) % (7 * DAY_MS)          # ms seit Montag 00:00 (roh)
    return (pos >= wa * DAY_MS + ha * HOUR_MS) & (pos < wb * DAY_MS + hb * HOUR_MS)


def _reversal_mask(raw_ms: np.ndarray) -> tuple[np.ndarray, list[dict]]:
    """Ruecksprung >= REGRESSION_MAX_MS in Dateireihenfolge: betroffen sind alle
    Ticks mit Rohzeit in [zurueck_auf, bei] -> (Maske, Details je Ruecksprung)."""
    raw_ms = np.asarray(raw_ms, dtype=np.int64)
    mask = np.zeros(len(raw_ms), dtype=bool)
    out = []
    d0 = np.diff(raw_ms)
    for i in np.nonzero(d0 <= -REGRESSION_MAX_MS)[0]:
        at, back = int(raw_ms[i]), int(raw_ms[i + 1])
        m = (raw_ms >= back) & (raw_ms <= at)
        mask |= m
        idx = np.nonzero(m)[0]
        out.append({"at_raw": _raw_iso(at), "back_to_raw": _raw_iso(back),
                    "minutes": round((at - back) / MIN_MS, 1), "n_ticks": int(len(idx)),
                    "first_raw": _raw_iso(int(raw_ms[idx[0]]), ms_digits=True),
                    "last_raw": _raw_iso(int(raw_ms[idx[-1]]), ms_digits=True),
                    "weekend_closed": bool(weekend_closed_raw(raw_ms[m]).all())})
    return mask, out


def _impossible_mask(raw_ms: np.ndarray, mode: str = "tick", rule=None) -> np.ndarray:
    """Ticks in nicht eindeutiger Ortszeit (verbotene Stunden) unter `rule` bzw.
    der Regel ihres Rohmonats ("undecided": erster Kandidat)."""
    raw_ms = np.asarray(raw_ms, dtype=np.int64)
    bad = np.zeros(len(raw_ms), dtype=bool)
    if not len(raw_ms):
        return bad
    idx = _raw_month_index(raw_ms)
    for k in np.unique(idx):
        sel = idx == k
        r = rule if rule is not None else rule_for(mode, (int(k) // 12, int(k) % 12 + 1))
        try:
            raw_to_utc_ms(raw_ms[sel], concrete_rule(r))
        except TimezoneError as e:
            m = getattr(e, "bad_mask", None)
            sub = np.zeros(int(sel.sum()), dtype=bool) if m is None else m
            bad[np.nonzero(sel)[0][sub]] = True
    return bad


def weekend_release(raw_ms: np.ndarray, mode: str = "tick", rule=None) -> tuple[np.ndarray, dict | None]:
    """
    Verwerfbare Ticks eines Rohmonats -> (Maske, releasable_weekend oder None).
    Kandidaten: alle von Rueckspruengen >= 30 min betroffenen Ticks (Rohzeit in
    [zurueck_auf, bei]) und alle Ticks in verbotenen Stunden. Freigebbar
    (dict) nur, wenn es solche gibt und ALLE im geschlossenen Wochenendmarkt
    liegen (WEEKEND_CLOSED_RAW); sonst None (nie freigebbar).
    """
    raw_ms = np.asarray(raw_ms, dtype=np.int64)
    rev, _ = _reversal_mask(raw_ms)
    imp = _impossible_mask(raw_ms, mode, rule)
    drop = rev | imp
    if not drop.any() or not bool(weekend_closed_raw(raw_ms[drop]).all()):
        return drop, None
    return drop, {"reversal_ticks": int(rev.sum()), "impossible_hour_ticks": int(imp.sum()),
                  "raw_ranges": [[a, b] for a, b in _ranges_of(raw_ms[drop])]}


def _raw_month_index(ms: np.ndarray) -> np.ndarray:
    """Monatsindex y*12+m-1 je Rohzeit-ms."""
    m = np.asarray(ms, dtype=np.int64).astype("datetime64[ms]").astype("datetime64[M]").astype(np.int64)
    return m + 1970 * 12


def raw_to_utc_ms_mode(raw_ms: np.ndarray, mode: str, ym: tuple[int, int] | None = None,
                       cfg: TzConfig | None = None) -> np.ndarray:
    """Wie raw_to_utc_ms, Regel je ROHmonat aus dem Fahrplan (ym fest oder je Wert)."""
    raw_ms = np.asarray(raw_ms, dtype=np.int64)
    if ym is not None:
        return raw_to_utc_ms(raw_ms, rule_for(mode, ym, cfg))
    if not len(raw_ms):
        return raw_ms.copy()
    idx = _raw_month_index(raw_ms)
    out = np.empty(len(raw_ms), dtype=np.int64)
    for k in np.unique(idx):
        sel = idx == k
        out[sel] = raw_to_utc_ms(raw_ms[sel], rule_for(mode, (int(k) // 12, int(k) % 12 + 1), cfg))
    return out


def utc_to_raw_ms_mode(utc_ms: np.ndarray, mode: str, cfg: TzConfig | None = None,
                       pick: int = 0) -> np.ndarray:
    """UTC-ms -> Rohzeit-ms, Regel des resultierenden Rohmonats aus dem Fahrplan
    ("undecided": Kandidat Nr. pick, ueber die Zahl der Kandidaten hinaus der letzte)."""
    def rf(k):
        r = rule_for(mode, (int(k) // 12, int(k) % 12 + 1), cfg)
        c = rule_candidates(r)
        return c[min(pick, len(c) - 1)]
    utc_ms = np.asarray(utc_ms, dtype=np.int64)
    if not len(utc_ms):
        return utc_ms.copy()
    out = np.empty(len(utc_ms), dtype=np.int64)
    idx = _raw_month_index(utc_ms)
    for k in np.unique(idx):
        sel = idx == k
        out[sel] = utc_to_raw_ms(utc_ms[sel], rf(k))
    # Rohmonat weicht vom UTC-Monat ab und hat eine andere Regel: damit neu rechnen
    ridx = _raw_month_index(out)
    for k in np.unique(ridx[ridx != idx]):
        sel = ridx == k
        out[sel] = utc_to_raw_ms(utc_ms[sel], rf(k))
    return out


def tz_offset_h(utc_ms: int, rule: TzRule) -> float:
    """Offset (Rohzeit - UTC) nach `rule` im UTC-Zeitpunkt utc_ms, in Stunden
    ("undecided": erster Kandidat)."""
    rule = concrete_rule(rule)
    return (_utcoff_ms(ZoneInfo(rule.zone), utc_ms // 1000) + rule.shift_h * HOUR_MS) / HOUR_MS


def raw_month_end_utc(ym: tuple[int, int], mode: str = "tick") -> datetime:
    """Ende des Rohmonats ym in UTC (Regel des Rohmonats ym; nie in einer Umstellung)."""
    end = max(int(raw_to_utc_ms(np.array([raw_month_bounds_ms(ym)[1]]), c)[0])
              for c in rule_candidates(rule_for(mode, ym)))          # "undecided": spaetestes Ende
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
        """Ende der Rohperiode in UTC (Regel ihres letzten Rohmonats)."""
        end = max(int(raw_to_utc_ms(np.array([self.raw_bounds_ms()[1]]), c)[0])
                  for c in rule_candidates(rule_for(self.mode, (self.year, self.month or 12))))
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
def write_parquet(out: Path, df: pd.DataFrame, mode: str, right_padding: bool = True,
                  label: str | None = None) -> None:
    """Atomar wie fetch_dukascopy.write_parquet; Metadaten: Modus, Spread, Zeitzone,
    tz_right_padding (false: Rohmonat M ohne Randticks von M+1 geprueft -> nachpruefen)."""
    out.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(df, preserve_index=False)
    meta = dict(table.schema.metadata or {})
    meta[b"histdata_mode"] = PARQUET_MODE[mode].encode()
    meta[b"spread_available"] = b"true" if mode == "tick" else b"false"
    meta[b"source_timezone"] = (label or SOURCE_TZ_META[mode]).encode()
    meta[b"tz_right_padding"] = b"true" if right_padding else b"false"
    _write_table_atomic(out, table.replace_schema_metadata(meta))


def relabel_store_file(out: Path, mode: str, right_padding: bool = True,
                       label: str | None = None) -> None:
    """Nur die Zeitzonen-Metadaten aktualisieren (Inhalt unveraendert, atomar)."""
    table = pq.read_table(out)
    meta = dict(table.schema.metadata or {})
    meta[b"source_timezone"] = (label or SOURCE_TZ_META[mode]).encode()
    meta[b"tz_right_padding"] = b"true" if right_padding else b"false"
    _write_table_atomic(out, table.replace_schema_metadata(meta))


def label_version(label: str | None) -> int:
    """Pruefversion aus source_timezone (0: unbekannt/aelter als v3)."""
    m = re.search(r"tz_check v(\d+)", label or "")
    return int(m.group(1)) if m else 0


def label_rules(label: str | None) -> list[tuple[str, int]] | None:
    """Umrechnung(en) (Zone, Verschiebung) aus einem source_timezone-Metadatum
    (v5: "name (Zone +-N h) [+ ...] -> UTC ...", aeltere: "Zone ... -> UTC ...",
    Vorversion fest UTC-5); None: unbekannt."""
    if not label:
        return None
    if label in LEGACY_TZ_META:
        return [LEGACY_TZ_META[label]]
    if "->" not in label:
        return None
    found = re.findall(r"\S+ \((\S+) ([+-]\d+) h\)", label.split("->")[0])
    out = [(z, int(h)) for z, h in found] if found else [(label.split()[0], 0)]
    for z, _ in out:
        try:
            ZoneInfo(z)
        except Exception:
            return None
    return out


def _zones_differ_in(old: list[tuple[str, int]] | None, mode: str, ym: tuple[int, int]) -> bool:
    """Liefert die alte Umrechnung (Zone, Verschiebung) im UTC-Monat ym irgendwo
    eine andere Rohzeit als der aktuelle Fahrplan?
    (Viertelstunden-Raster; mehrere alte Regeln: nur gleich, wenn identisch)."""
    if not old:
        return True
    # "undecided": alle Kandidaten muessen hier dieselbe Rohzeit liefern wie die alte Regel
    rules = [rule_for(mode, m) for m in _raw_months_for(ym, mode)]
    cands = [c for r in rules for c in rule_candidates(r)]
    new_rules = [(r.zone, r.shift_h) for r in dict.fromkeys(cands)]
    if old == new_rules:
        return False
    if len(set(old)) != 1:
        return True
    # nur Zeitpunkte IM UTC-Monat zaehlen (nur die stehen in der Datei)
    lo = int(datetime(*ym, 1, tzinfo=UTC).timestamp() * 1000)
    hi = int(datetime(*_ym_add(ym, 1), 1, tzinfo=UTC).timestamp() * 1000)
    grid = np.arange(lo, hi, HOUR_MS // 4, dtype=np.int64)
    zone, shift = old[0]
    ref = utc_to_raw_ms(grid, TzRule("alt", zone, shift))
    npick = max(len(rule_candidates(r)) for r in rules)
    return any(bool((utc_to_raw_ms_mode(grid, mode, pick=k) != ref).any()) for k in range(npick))


def classify_store_file(out: Path, mode: str, ym: tuple[int, int]) -> tuple[str, str | None]:
    """
    Zeitzonen-Zustand einer vorhandenen Monatsdatei desselben Modus ->
    (zustand, altes_metadatum):
      current     Metadatum = store_tz_label(mode, ym) (und rechter Nachbar geprueft)
      recheck_padding  aktuelles Metadatum, aber tz_right_padding=false: nachpruefen,
                  sobald Rohmonat M+1 im Cache ist (besteht -> Flag setzen, sonst
                  aus dem Store nehmen)
      recheck     andere/aeltere Kennzeichnung, aber in diesem Monat dieselben
                  UTC-Zeiten (gleiche Umrechnung, aeltere Pruefversion, oder andere
                  Regel ohne Unterschied hier, z.B. Jan/Jul ny_local vs.
                  berlin_minus_6): Rohmonate aus dem Cache neu pruefen; besteht ->
                  nur Metadaten neu, sonst aus dem Store nehmen + Quarantaene; ohne
                  Roh-Cache bleibt die Datei (unverified_after_version_bump)
      stale_shifted  andere Umrechnung mit anderen UTC-Zeiten in diesem Monat (z.B.
                  die EU/US-Luecken-Wochen im Maerz und Okt/Nov unter ny_local):
                  sofort nach metals/stale/ verschieben, dann neu bauen
    """
    old = stored_timezone(out)
    if old == store_tz_label(mode, ym):
        if _stored_meta(out, b"tz_right_padding") == "false":
            return "recheck_padding", old
        return "current", old
    return ("stale_shifted" if _zones_differ_in(label_rules(old), mode, ym) else "recheck"), old


def _store_months(data_root: Path, symbol: str, mode: str) -> list[tuple[tuple[int, int], Path]]:
    pat = re.compile(rf"{symbol}-1m-(\d{{4}})-(\d{{2}})\.parquet$")
    out = []
    d = store_dir(data_root, symbol, mode)
    for p in sorted(d.glob("year=*/*.parquet")) if d.exists() else []:
        m = pat.match(p.name)
        if m:
            out.append(((int(m.group(1)), int(m.group(2))), p))
    return out


def _write_table_atomic(out: Path, table: pa.Table) -> None:
    out.parent.mkdir(parents=True, exist_ok=True)
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
    20120201000106 and 20120201000208" (Rohzeit, Regel des Rohmonats -> UTC; nicht
    eindeutige Ortszeit -> *_utc None, *_raw bleibt). Unbekanntes Format ->
    nur die ersten Zeilen im Report.
    """
    if txt is None:
        return {"present": False}

    def utc(digits: str) -> tuple[str, str | None]:
        r = datetime.strptime(digits, "%Y%m%d%H%M%S").replace(tzinfo=UTC)
        try:
            u = int(raw_to_utc_ms_mode(np.array([int(r.timestamp() * 1000)]), mode)[0])
        except (TimezoneError, RuleUndecided):
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


def _gap_fit(a: int, b: int, hours: tuple[int, int], rule_at) -> dict:
    """
    Luecke [a, b] (a = letzter Tick davor, b = erster danach) gegen erwarteten
    Schluss hours[0]:00 (Rohdatum von a) und Oeffnung hours[1]:00 New York
    (Rohdatum von b). F = alle ganzen Abweichungen v (vom erwarteten Offset der
    jeweiligen Seite), mit denen der Schluss nicht vor dem letzten (Toleranz
    TZ_TOL_MS) und die Oeffnung nicht nach dem ersten Tick laege (Toleranz
    TZ_TOL_OPEN_MS). Richtige Daten
    jeder Dichte haben 0 in F (auch am Umstellungssonntag).
    """
    hc, ho = hours
    ec, eo = _expected_ms(a, hc), _expected_ms(b, ho)
    # erwartet: Offset der Stempelungsregel (des jeweiligen Rohmonats) im Ereigniszeitpunkt
    xc, xo = int(round(tz_offset_h(ec, rule_at(a)))), int(round(tz_offset_h(eo, rule_at(b))))
    lo = math.ceil((a - ec - xc * HOUR_MS - TZ_TOL_MS) / HOUR_MS)
    hi = math.floor((b - eo - xo * HOUR_MS + TZ_TOL_OPEN_MS) / HOUR_MS)
    vc = math.floor((a - ec - xc * HOUR_MS) / HOUR_MS + 0.5)        # naechste volle Stunde je Seite
    vo = math.floor((b - eo - xo * HOUR_MS) / HOUR_MS + 0.5)
    return {"a": a, "b": b, "ec": ec, "eo": eo, "xc": xc, "xo": xo, "lo": lo, "hi": hi,
            "vc": vc, "vo": vo, "rc": a - ec - (xc + vc) * HOUR_MS, "ro": b - eo - (xo + vo) * HOUR_MS}


def _transitions_ms(rule: TzRule, lo_ms: int, hi_ms: int) -> np.ndarray:
    """UTC-ms der Uhrumstellungen der Regel-Zone in [lo_ms, hi_ms] (leer bei fester Zone)."""
    T, _ = _table_for(concrete_rule(rule).zone, np.array([lo_ms, hi_ms], dtype=np.int64))
    T = T[1:]
    return T[(T >= lo_ms) & (T <= hi_ms)]


def _ambig_before_run(run: list, entries: list, ambig: list, own_lo: int) -> bool:
    """Liegt direkt vor dem Lauf (ohne Eintrag mit 0 dazwischen) ein mehrdeutiges
    Wochenende im Vormonat, das zur Abweichung des Laufs passt? Dann kann der
    Wechsel schon dort begonnen haben (duenne Daten: Oeffnung nicht eindeutig)."""
    start, dev = run[0][0], run[0][1]
    for a, lo, hi in ambig:
        # Null-Eintraege DESSELBEN mehrdeutigen Wochenendes (z.B. sein Freitags-
        # schluss) trennen nicht: der Wechsel kann in genau dieser Luecke liegen
        zeros = [e[0] for e in entries if e[1] == 0 and e[0] < start and e[2] != a]
        after = max(zeros) if zeros else -1
        if after < a < start and a < own_lo and lo <= dev <= hi:
            return True
    return False


def _wd(ms: int) -> int:
    """Wochentag der Rohzeit (Mo = 0)."""
    return (ms // DAY_MS + 3) % 7


def check_timezone(raw_ms: np.ndarray, symbol: str,
                   bounds: tuple[int, int] | None = None, mode: str = "tick",
                   rule: TzRule | None = None, neighbours: tuple = (None, None)) -> dict:
    """
    Impliziten Offset (Rohzeit - UTC, ganze Stunden) aus Marktluecken schaetzen
    (Marktzeiten bleiben New York/CME) und mit dem erwarteten Offset der
    Stempelungsregel im Ereigniszeitpunkt vergleichen (tick berlin_minus_6:
    -5 h im EU-Winter, -4 h in der EU-Sommerzeit; m1 fixed_est: immer -5 h).
    rule: fest vorgegeben (z.B. Kandidat beim Abgleich) oder None = je Rohmonat
    aus dem Fahrplan (rule_for). Abweichung = implizit - erwartet.

    Luecken (Marktzeiten New York):
      Wochenende   >= 24 h, Schluss Do/Fr (Fr oder Feiertagsvorabend), Oeffnung
                   So/Mo: Schluss 17:00, Oeffnung 18:00 New York.
                   Andere Luecken >= 24 h sind Ausfaelle (Bericht "outages").
      Tagespause   (nur Metalle, Mo-Do) 45 min..3 h, nur eindeutig und
                   |Abweichung| <= 1 h; je Rohtag die Luecke mit Beginn am
                   naechsten an 17:00, aber nur wenn alle solchen Kandidaten
                   des Tages (auch mehrdeutige, die zu +-1 h passen) dieselbe
                   eindeutige Abweichung ergeben.
    neighbours = (Ticks vom Ende des Vor-, vom Anfang des Folge-Rohmonats,
    z.B. je TZ_PAD_MS), falls gecached: Luecken dort liefern Kontext-Eintraege
    (Laeufe ueber den Monatsrand), aber keine Anker fuer Median/Anteil.

    Je Luecke F = vertraegliche Abweichungen (_gap_fit). Genau eine: EINDEUTIGER
    Eintrag (auch bei duennen Daten richtig) - nur diese zaehlen fuer Median und
    Anteil. F leer (Seiten widersprechen sich; bei richtigen Daten unmoeglich,
    z.B. Wechsel ueber das Wochenende, verirrter Tick): je Seite ein Eintrag
    (Schluss = kleinste, Oeffnung = groesste vertraegliche Abweichung). F
    mehrdeutig (nur Wochenende): je Seite ein schwacher Eintrag mit der
    naechsten vollen Stunde, aber nur wenn der Tick hoechstens TZ_TIGHT_MS
    daneben liegt und der Markt dort aktiv ist (Nachbartick <= TZ_ACTIVE_MS).

    HARTE Fehler (status failed):
      - Median-Abweichung der eindeutigen Anker != 0
      - Anteil Abweichung 0 < TZ_MIN_SHARE
      - >= TZ_MAX_RUN unabhaengige Luecken in Folge (Eintraege in Zeitfolge,
        Nachbar-Randticks eingeschlossen, kein Eintrag mit 0 dazwischen) mit
        derselben Abweichung != 0, mindestens einer im eigenen Monat
        (Stempelungswechsel; auch eine Woche mit anderer Sommerzeitregel)
      - Ruecksprung >= 30 min in Dateireihenfolge
    Nur WARNUNG (warnings, kein Fehler): einzelne abweichende Eintraege
    (Ausfall am Freitag, verspaetete Oeffnung, verirrter Tick, Feiertag),
    widerspruechliche oder mehrdeutige Luecken, Ausfaelle >= 24 h unter der
    Woche. not_checkable: weniger als TZ_MIN_ANCHORS eindeutige Anker.
    switch_reaches_prev_month: ein Wechsel-Lauf beginnt schon im Vormonat
    (check_following sperrt dann den Vormonat).
    """
    raw_ms = np.asarray(raw_ms, dtype=np.int64)
    if bounds is None and len(raw_ms):        # Rohmonat des Medians als Monatsrand
        mid = datetime.fromtimestamp(float(np.median(raw_ms)) / 1000, UTC)
        bounds = raw_month_bounds_ms((mid.year, mid.month))
    own_ym = None
    if bounds is not None:
        b0 = datetime.fromtimestamp(bounds[0] / 1000, UTC)
        own_ym = (b0.year, b0.month)
    if rule is None:
        rule = rule_for(mode, own_ym) if own_ym else load_tz_config().schedule[mode][-1][1]
        fixed = None
    else:
        fixed = rule

    @functools.lru_cache(maxsize=None)
    def _rule_of_month(k: int) -> TzRule:
        return rule_for(mode, (k // 12, k % 12 + 1))

    def rule_at(r_ms: int) -> TzRule:
        """Regel fuer einen Tick: fest vorgegeben, sonst die seines Rohmonats."""
        if fixed is not None:
            return fixed
        return _rule_of_month(int(_raw_month_index(np.array([r_ms]))[0]))

    rep: dict = {"rule": rule.name, "rule_detail": rule.describe(), "raw_tz": rule.describe(),
                 "mode": mode, "check_version": TZ_CHECK_VERSION, "rows": int(len(raw_ms))}
    if isinstance(rule, UndecidedRule):
        # Pruefung unter dem ersten Kandidaten (wo geschrieben wird, rechnen alle gleich)
        rep.update(rule_checked=rule.first.name, candidates=[c.name for c in rule.candidates])
    d0 = np.diff(raw_ms)
    rep["reversals"] = int((d0 < 0).sum())
    rep["max_reversal_s"] = float(max(0, -int(d0.min())) / 1000) if len(d0) else 0.0
    large = np.nonzero(d0 <= -REGRESSION_MAX_MS)[0]
    # betroffene Ticks je Ruecksprung: Rohzeit in [zurueck_auf, bei]
    rev_mask, rev_rows = _reversal_mask(raw_ms)
    rep["large_reversals"] = rev_rows[:N_EXAMPLES]
    rep["reversal_ticks_total"] = int(rev_mask.sum())
    _, rel = weekend_release(raw_ms, mode, fixed)
    if rel is not None:
        # alle Ruecksprung- und Verbotene-Stunden-Ticks im geschlossenen Wochenendmarkt:
        # nach menschlicher Pruefung verwerfbar (--accept-weekend-ticks)
        rep["releasable_weekend"] = rel

    own_lo = int(raw_ms.min()) if len(raw_ms) else 0
    own_hi = int(raw_ms.max()) if len(raw_ms) else -1
    pads = [int(x) for nb in neighbours if nb is not None for x in np.atleast_1d(nb)
            if not len(raw_ms) or x < own_lo or x > own_hi]
    t = np.unique(np.concatenate([raw_ms, np.array(pads, dtype=np.int64)]))
    d = np.diff(t)

    def own(x: int) -> bool:
        return own_lo <= x <= own_hi

    def active(i: int, side: str) -> bool:
        if side == "c":
            return i > 0 and int(t[i] - t[i - 1]) <= TZ_ACTIVE_MS
        return i + 2 < len(t) and int(t[i + 2] - t[i + 1]) <= TZ_ACTIVE_MS

    anchors: list[tuple] = []      # eindeutig, eigene Seite: roh, art, stunde, implizit, erwartet
    entries: list[tuple] = []      # zeit, abweichung, luecke, eigen, art, eindeutig
    outages, contradictions, warnings = [], [], []
    ambig_gaps: list[tuple[int, int, int]] = []    # (schluss-tick, lo, hi) mehrdeutiger Wochenenden
    ambiguous = 0

    def add_unique(f: dict, gid: int, kinds: tuple[str, str], hours: tuple[int, int]) -> None:
        v = f["lo"]
        for r, k, h, x in ((f["a"], kinds[0], hours[0], f["xc"]), (f["b"], kinds[1], hours[1], f["xo"])):
            if own(r):
                anchors.append((r, k, h, x + v, x))
        entries.append((f["a"], v, gid, own(f["a"]) or own(f["b"]), kinds[0], True))

    for i in np.nonzero(d >= WEEKEND_GAP_MS)[0]:
        a, b = int(t[i]), int(t[i + 1])
        if _wd(a) not in (3, 4) or _wd(b) not in (6, 0) or b - a > 5 * DAY_MS:
            if not (own(a) or own(b)):
                continue
            outages.append({"from_raw": _raw_iso(a), "to_raw": _raw_iso(b),
                            "hours": round((b - a) / HOUR_MS, 1)})
            continue
        hours = (CLOSE_H_NY, OPEN_H_NY[symbol])
        f = _gap_fit(a, b, hours, rule_at)
        if f["lo"] == f["hi"]:
            add_unique(f, a, ("wochenschluss", "wochenstart"), hours)
            continue
        if f["lo"] <= f["hi"]:
            ambig_gaps.append((a, f["lo"], f["hi"]))
        if f["lo"] > f["hi"]:
            # Widerspruch (bei richtigen Daten jeder Dichte unmoeglich): beide Seiten
            # als Eintraege (Schluss = kleinste, Oeffnung = groesste vertraegliche
            # Abweichung); hart erst mit Bestaetigung durch eine weitere Luecke
            contradictions.append({"from_raw": _raw_iso(a), "to_raw": _raw_iso(b),
                                   "close_deviation_h": f["lo"], "open_deviation_h": f["hi"]})
            entries.append((a, f["lo"], a, own(a), "wochenschluss", False))
            entries.append((b, f["hi"], a, own(b), "wochenstart", False))
            continue
        ambiguous += 1
        # schwache Einzelseiten: Tick an einer vollen Stunde bei aktivem Markt
        for r, v, res, side, k in ((a, f["vc"], f["rc"], "c", "wochenschluss"),
                                   (b, f["vo"], f["ro"], "o", "wochenstart")):
            if abs(res) <= TZ_TIGHT_MS and active(int(i), side):
                entries.append((r, v, a, own(r), k, False))

    ignored_breaks = 0
    if symbol in METALS:
        lo, hi = BREAK_GAP_MS
        cands: dict[int, list[tuple[int, dict]]] = {}     # rohtag -> [(abstand zu 17:00, fit)]
        for i in np.nonzero((d >= lo) & (d <= hi))[0]:
            a = int(t[i])
            if _wd(a) >= 4:
                continue
            f = _gap_fit(a, int(t[i + 1]), (CLOSE_H_NY, 18), rule_at)
            f["rng"] = (max(f["lo"], -TZ_BREAK_MAX_DEV_H), min(f["hi"], TZ_BREAK_MAX_DEV_H))
            if f["rng"][0] > f["rng"][1]:
                ignored_breaks += 1               # keine Pause (Ausfall, naechtliche Luecke)
                continue
            cands.setdefault(a // DAY_MS, []).append(
                (abs(a - (a // DAY_MS * DAY_MS + CLOSE_H_NY * HOUR_MS)), f))
        for day, cs in cands.items():
            # Anker nur, wenn ALLE Kandidaten des Tages (auch mehrdeutige) genau
            # dieselbe Abweichung ergeben: bei richtigen Daten passt die echte Pause
            # immer zu 0, eine zufaellige Luecke kann sie also nie allein ersetzen
            if len({f["rng"] for _, f in cs}) > 1 or cs[0][1]["rng"][0] != cs[0][1]["rng"][1]:
                ignored_breaks += len(cs)
                continue
            ignored_breaks += len(cs) - 1
            f = dict(min(cs, key=lambda c: c[0])[1])   # Beginn am naechsten an 17:00
            f["lo"] = f["hi"] = f["rng"][0]            # auf +-1 h begrenzt eindeutig
            add_unique(f, f["a"], ("pause_beginn", "pause_ende"), (CLOSE_H_NY, 18))
    anchors.sort()
    entries.sort()
    offs = [a[3] for a in anchors]
    exps = [a[4] for a in anchors]
    devs = [o - x for o, x in zip(offs, exps)]

    # Laeufe unabhaengiger Luecken mit gleicher Abweichung != 0
    runs, cur = [], []
    for e in entries:
        if e[1] == 0:
            cur = []
            continue
        if cur and cur[-1][1] != e[1]:
            cur = []
        if not cur or cur[-1][2] != e[2]:
            cur.append(e)
        else:
            cur[-1] = e if e[3] else cur[-1]
        if len({x[2] for x in cur}) >= TZ_MAX_RUN and any(x[3] for x in cur):
            if runs and runs[-1] is cur:
                continue
            runs.append(cur)
    best_run = 0
    cnt, prev = 0, None
    for e in entries:
        cnt = 0 if e[1] == 0 else (cnt + 1 if prev is not None and prev[1] == e[1] and prev[2] != e[2] else 1)
        prev = e
        best_run = max(best_run, cnt)

    n = len(devs)
    share = devs.count(0) / n if n else 0.0
    med = float(np.median(offs)) if n else None
    med_dev = float(np.median(devs)) if n else None
    blo, bhi = bounds if bounds is not None else (own_lo, own_hi + 1)
    nonzero = [e for e in entries if e[1] and e[3]]
    tr = (_transitions_ms(rule, own_lo - TZ_TRANSITION_MS, own_hi + TZ_TRANSITION_MS)
          if len(raw_ms) else np.array([], dtype=np.int64))

    def erow(e) -> dict:
        return {"raw": _raw_iso(e[0]), "kind": e[4], "deviation_h": e[1], "unique": e[5],
                "own_month": e[3]}

    def arow(a) -> dict:
        r, k, h, o, x = a
        return {"raw": _raw_iso(r), "kind": k, "expected_ny": f"{h}:00", "implied_offset_h": o,
                "expected_offset_h": x, "deviation_h": o - x}

    first = entries[0] if entries else None
    for e in nonzero:
        if not any(e in r for r in runs):
            warnings.append(f"einzelner abweichender Eintrag {_raw_iso(e[0])} {e[4]} {e[1]:+d} h "
                            f"({'eindeutig' if e[5] else 'Einzelseite'}, nicht bestaetigt)")
    for c in contradictions:
        warnings.append(f"widerspruechliches Wochenende {c['from_raw']} -> {c['to_raw']}")
    for o in outages:
        warnings.append(f"Ausfall {o['from_raw']} -> {o['to_raw']} ({o['hours']} h, kein Wochenende)")
    rep.update(anchors=n, entries=len(entries), unique_entries=sum(1 for e in entries if e[5]),
               ignored_break_gaps=ignored_breaks, ambiguous_gaps=ambiguous,
               neighbour_ticks_used=len(pads), share_expected=round(share, 3),
               median_offset_h=med, median_deviation_h=med_dev,
               offsets=Counter(dict(sorted(Counter(offs).items()))),
               expected_offsets=Counter(dict(sorted(Counter(exps).items()))),
               deviations=Counter(dict(sorted(Counter(devs).items()))),
               first_offset_h=offs[0] if n else None, last_offset_h=offs[-1] if n else None,
               first_deviation_h=devs[0] if n else None, last_deviation_h=devs[-1] if n else None,
               first_entry_deviation_h=first[1] if first else None,
               first_entry_in_prev_month=bool(first and first[0] < own_lo),
               # ein Wechsel-Lauf reicht in den Vormonat (Schluss-Seite dort):
               # check_following sperrt dann den Vormonat
               switch_reaches_prev_month=any(e[0] < own_lo for r in runs for e in r)
               or any(_ambig_before_run(r, entries, ambig_gaps, own_lo) for r in runs),
               # erster/letzter Eintrag weicht ab und ist (noch) nicht bestaetigt: ohne
               # Nachbar-Kontext auf dieser Seite haelt raw_slice den Monat zurueck
               leading_suspect=bool(first and first[1] and first[3] and not any(first in r for r in runs)),
               trailing_suspect=bool(entries and entries[-1][1] and entries[-1][3]
                                     and not any(entries[-1] in r for r in runs)),
               longest_deviating_run=best_run,
               switch_runs=[[erow(e) for e in r] for r in runs][:N_EXAMPLES],
               edge_violations=[erow(e) for e in nonzero
                                if abs(e[1]) == 1 and (e[0] < blo + TZ_EDGE_MS or e[0] >= bhi - TZ_EDGE_MS)],
               transition_violations=[erow(e) for e in nonzero
                                      if len(tr) and np.abs(tr - e[0]).min() <= TZ_TRANSITION_MS],
               contradictions=contradictions[:N_EXAMPLES], outages=outages[:N_EXAMPLES],
               warnings=warnings[:N_EXAMPLES],
               deviating_examples=[arow(a) for a in anchors if a[3] != a[4]][:N_EXAMPLES])
    reasons = []
    if len(large):
        reasons.append(f"{len(large)} Ruecksprung/Rueckspruenge >= {REGRESSION_MAX_MS // MIN_MS} min "
                       f"in Dateireihenfolge (max {rep['max_reversal_s'] / 60:.0f} min, Uhrumstellung?; "
                       f"{rep['reversal_ticks_total']} Tick(s) betroffen"
                       + (", alle im geschlossenen Wochenendmarkt: nach Pruefung freigebbar "
                          "(--accept-weekend-ticks), diese Ticks werden dann verworfen)"
                          if rel is not None else ", nicht freigebbar)"))
    for r in runs[:1]:
        reasons.append(f"{len(runs)} Stempelungswechsel: {len({x[2] for x in r})} unabhaengige Luecken in "
                       f"Folge mit {r[0][1]:+d} h Abweichung von {rule.describe()}, z.B. "
                       + ", ".join(f"{_raw_iso(x[0])} {x[4]}" for x in r[:3])
                       + " (andere Sommerzeitregel/Verschiebung?)")
    if n >= TZ_MIN_ANCHORS:
        if med_dev != 0:
            reasons.append(f"Median-Abweichung {med_dev:+g} h (Median-Offset {med:g} h, erwartet "
                           f"{_fmt_counter(rep['expected_offsets'])} h aus {rule.describe()})")
        if share < TZ_MIN_SHARE:
            reasons.append(f"nur {share:.0%} der Anker ohne Abweichung von {rule.describe()}")
    rep["status"] = "failed" if reasons else ("ok" if n >= TZ_MIN_ANCHORS else "not_checkable")
    if rep["status"] == "not_checkable":
        reasons.append(f"nur {n} eindeutige Ankerpunkte (< {TZ_MIN_ANCHORS}), Zeitzone nicht pruefbar")
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
    return raw_to_utc_ms_mode(df["raw_ms"].to_numpy(), mode)


def ticks_to_bars(ticks: pd.DataFrame) -> pd.DataFrame:
    """Ticks (raw_ms, bid, ask, volume) -> 1m-Bars im Store-Schema (UTC; Rohzeit =
    Regel des Rohmonats aus dem Fahrplan; nicht eindeutige Ortszeit -> TimezoneError)."""
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
    M1-Bid-Bars -> Store-Schema (Rohzeit nach Fahrplan, Default fixed_est = fest UTC-5, NICHT
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
                 rebuild: bool = False, report_only: bool = False):
        self.symbol, self.mode, self.data_root, self.client, self.now = symbol, mode, data_root, client, now
        self.rebuild = rebuild
        self.report_only = report_only          # --verify-month: Marker weder schreiben noch loeschen
        self.decoded: OrderedDict[Unit, Decoded] = OrderedDict()
        self.tz: dict[tuple, dict] = {}         # (unit, ym, links da, rechts da) -> endgueltige Pruefung
        self.edges: dict[tuple[tuple[int, int], bool], np.ndarray] = {}
        # (unit, ym) -> (Rohzeit, [UTC je Kandidat]) der Ticks, die die Kandidaten einer
        # "undecided"-Regel verschieden umrechnen (build_month entscheidet je UTC-Monat)
        self.undecided: dict[tuple, tuple] = {}
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
        # Jahresdatei erst, wenn das Rohjahr (Stempelungsregel) samt SETTLE_S vorbei ist
        t = int((self.now() - timedelta(seconds=SETTLE_S)).timestamp() * 1000)
        cur = datetime.fromtimestamp(int(utc_to_raw_ms_mode(np.array([t]), self.mode)[0]) / 1000, UTC).year
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
        self.undecided = {k: v for k, v in self.undecided.items() if k[0] != u}
        self.edges.clear()

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
        self.edges.clear()
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

    def _edge_ms(self, ym: tuple[int, int], last: bool) -> np.ndarray | None:
        """Ticks der letzten (last) bzw. ersten TZ_PAD_MS eines gecachten Nachbar-
        Rohmonats (gemerkt): Kontext fuer Luecken und Laeufe ueber den Monatsrand."""
        key = (ym, last)
        if key not in self.edges:
            ms = self._neighbour_ms(ym)
            if ms is None or not len(ms):
                return None                    # nicht merken: kann spaeter geladen werden
            u = np.unique(ms)
            self.edges[key] = u[u >= u[-1] - TZ_PAD_MS] if last else u[u <= u[0] + TZ_PAD_MS]
        return self.edges[key]

    @staticmethod
    def read_qmarker(q: Path) -> dict | None:
        """Quarantaene-Marker lesen; None = unlesbar (kein JSON-Objekt)."""
        try:
            info = json.loads(q.read_text())
        except (OSError, ValueError):
            return None
        return info if isinstance(info, dict) else None

    def _marker_decision(self, u: Unit, ym, q: Path) -> tuple[bool, dict | None]:
        """
        Vorhandenen Quarantaene-Marker bewerten -> (accepted, alter_marker_zum_neupruefen).
        - unlesbar: ERROR, Marker bleibt, Monat bleibt gesperrt (ein "accepted"
          mit Tippfehler geht nicht still verloren)
        - "accepted": true mit derselben Stempelungsregel (raw_tz + tz_shift_h ==
          Regel des Rohmonats): gilt (gleiche Umrechnung)
        - "accepted": true unter einer anderen Regel (v1 fest UTC-5, v2-v4
          ny_local, ...): neu pruefen; besteht -> Marker weg, sonst neuer Marker
          accepted false mit Hinweis
        - nicht freigegeben, aktuelle tz_check_version und Regel: sperrt
        - nicht freigegeben, aeltere Version/andere Regel: neu pruefen
        """
        if not q.exists():
            return False, None
        info = self.read_qmarker(q)
        if info is None:
            log.error("Quarantaene-Marker %s unlesbar (kein gueltiges JSON) - bleibt gesperrt; "
                      "Marker reparieren (z.B. \"accepted\": true) oder loeschen", q)
            raise Quarantined(u, ym, f"Quarantaene-Marker {q.name} unlesbar (kein gueltiges JSON) - "
                              "reparieren oder loeschen (= neu pruefen)", None)
        same_tz = (info.get("raw_tz"), info.get("tz_shift_h", 0)) == rule_identity(rule_for(self.mode, ym))
        if info.get("accepted") and same_tz:
            return True, None
        if not info.get("accepted") and same_tz and info.get("tz_check_version") == TZ_CHECK_VERSION:
            raise Quarantined(u, ym, info.get("reason") or "Quarantaene", info.get("check"))
        log.warning("Quarantaene-Marker %s stammt aus aelterer Zeitzonen-Pruefung (Version %s, %s%s) - "
                    "Rohmonat wird neu geprueft (ohne Download)", q.name,
                    info.get("tz_check_version", 1), info.get("tz_rule") or info.get("raw_tz") or "fest UTC-5",
                    ", war freigegeben" if info.get("accepted") else "")
        return False, info

    def _neighbour_expected(self, ym: tuple[int, int]) -> bool:
        """Nachbar-Rohmonat sollte existieren, ist aber nicht im Cache: abgeschlossen,
        innerhalb der Historie, nicht bestaetigt leer, ZIP fehlt."""
        if before_history(self.symbol, ym) or not _month_closed(self.now(), ym, self.mode):
            return False
        u = self.unit(ym)
        if u.path(self.data_root).exists():
            return False
        return self.state(u)[0] != "empty"

    def _neighbours(self, ym: tuple[int, int]) -> tuple[tuple, dict]:
        prev, nxt = _ym_add(ym, -1), _ym_add(ym, 1)
        left, right = self._edge_ms(prev, last=True), self._edge_ms(nxt, last=False)
        pad = {"left": left is not None, "right": right is not None,
               "left_missing": left is None and self._neighbour_expected(prev),
               "right_missing": right is None and self._neighbour_expected(nxt)}
        # rechter Rand endgueltig geprueft: Randticks da oder M+1 bestaetigt leer
        pad["right_final"] = pad["right"] or self.state(self.unit(nxt))[0] == "empty"
        pad["provisional"] = pad["left_missing"] or pad["right_missing"]
        return (left, right), pad

    def _convert(self, raw: np.ndarray, rule, u: Unit, ym: tuple[int, int]) -> np.ndarray:
        """Rohzeit -> UTC nach der Regel des Rohmonats (TimezoneError wie raw_to_utc_ms).
        "undecided": erster rechnender Kandidat; Ticks, die die Kandidaten verschieden
        umrechnen, werden mit allen Kandidaten-UTC gemerkt (ctx.undecided) -
        build_month schreibt einen UTC-Monat nicht, in den einer davon fallen kann."""
        if not isinstance(rule, UndecidedRule):
            return raw_to_utc_ms(raw, rule)
        outs, errs = [], []
        for c in rule.candidates:
            try:
                outs.append(raw_to_utc_ms(raw, c))
                errs.append(None)
            except TimezoneError as e:
                outs.append(None)
                errs.append(e)
        if all(e is not None for e in errs):
            raise errs[0]                        # ueberall nicht eindeutig: normale Behandlung
        ref = next(o for o in outs if o is not None)
        diff = np.zeros(len(raw), dtype=bool)
        for o, e in zip(outs, errs):
            diff |= getattr(e, "bad_mask", np.ones(len(raw), dtype=bool)) if o is None else (o != ref)
        if diff.any():
            self.undecided[(u, ym)] = (raw[diff], [(o if o is not None else ref)[diff] for o in outs])
        else:
            self.undecided.pop((u, ym), None)
        return ref

    def _check(self, raw: np.ndarray, ym, bounds, nb, rule=None) -> dict:
        """check_timezone fuer Rohmonat ym (rule None = Regel je Rohmonat), bei zu
        wenigen Ankern gemeinsam mit den gecachten Nachbar-Rohmonaten."""
        chk = check_timezone(raw, self.symbol, bounds, self.mode, rule=rule, neighbours=nb)
        if chk["status"] == "not_checkable":
            # zu wenige Anker (kurzer erster Historienmonat, duenner Monat):
            # gemeinsam mit gecachten Nachbar-Rohmonaten pruefen
            parts = [self._neighbour_ms(_ym_add(ym, -1)), raw, self._neighbour_ms(_ym_add(ym, 1))]
            used = [_ymstr(_ym_add(ym, k)) for k, p in zip((-1, 0, 1), parts) if p is not None]
            if len(used) > 1:
                joint = check_timezone(np.concatenate([p for p in parts if p is not None]),
                                       self.symbol, bounds, self.mode, rule=rule)
                joint["joint_with"] = used
                joint["alone"] = {k: chk[k] for k in ("anchors", "reason")}
                chk = joint
            if chk["status"] == "not_checkable":
                chk["reason"] = (f"Zeitzone NICHT PRUEFBAR: {chk['reason']} (auch gemeinsam mit "
                                 f"Nachbarmonaten {used}) - manuell pruefen, dann im Marker "
                                 "\"accepted\": true setzen")
        return chk

    def raw_slice(self, u: Unit, ym: tuple[int, int]) -> tuple[pd.DataFrame, dict]:
        """
        Rohmonat ym aus Datei u, zeitzonengeprueft, mit Spalte utc_ms; Quarantined wenn nicht.
        Geprueft mit den Randticks der gecachten Nachbar-Rohmonate. Fehlt ein
        Nachbar, der existieren sollte (abgeschlossen, in der Historie), ist das
        Ergebnis VORLAEUFIG: weder gemerkt noch als Marker gespeichert (der
        naechste Aufruf mit Nachbar prueft neu); tz["padding"] sagt, was fehlte.
        Freigegebener Marker mit releasable_weekend: genau die verwerfbaren
        Wochenend-Ticks (weekend_release) werden VERWORFEN (nie umgerechnet), der
        Rest normal geprueft; tz meldet dropped_ticks/dropped_raw.
        """
        rule = rule_for(self.mode, ym)
        q = u.qmarker(self.data_root, ym)
        accepted, old = self._marker_decision(u, ym, q)
        note = None
        if old is not None and old.get("accepted"):
            note = (f"Fruehere Freigabe (\"accepted\": true, Pruefversion "
                    f"{old.get('tz_check_version', 1)}, {old.get('raw_tz') or 'fest UTC-5'}) galt "
                    f"einer anderen Umrechnung; unter {rule.describe()} neu geprueft und "
                    "durchgefallen - erneut pruefen und ggf. neu freigeben")
        try:
            sel = self._month_rows(u, ym)
        except FormatError as e:
            if accepted:
                raise Quarantined(u, ym, f"Format nicht lesbar: {e}", None)
            self._quarantine(u, ym, f"Format nicht lesbar: {e}", None, note)
        raw_all = sel["raw_ms"].to_numpy()
        marker = (self.read_qmarker(q) or {}) if accepted else {}
        released = marker.get("releasable_weekend")
        dropped = None
        if isinstance(released, dict) or marker.get("tz_error_releasable"):
            # Freigabe verwerfbarer Wochenend-Ticks (v5: tz_error_releasable): genau
            # diese Ticks verwerfen - nur wenn sie weiterhin ALLE im geschlossenen
            # Wochenendmarkt liegen und (v6) dieselben Rohzeit-Bereiche sind
            drop, rel = weekend_release(raw_all, self.mode, rule)
            if rel is None or (isinstance(released, dict)
                               and rel["raw_ranges"] != released.get("raw_ranges")):
                raise Quarantined(u, ym, f"Freigabe im Marker {q.name} (releasable_weekend) passt nicht "
                                  "mehr zu den Rohdaten (andere/zusaetzliche Ticks oder nicht mehr alle im "
                                  "geschlossenen Wochenendmarkt) - Marker loeschen (= neu pruefen)", None)
            dropped = {"dropped_ticks": int(drop.sum()),
                       "dropped_raw": [_raw_iso(int(x), ms_digits=True) for x in raw_all[drop][:N_EXAMPLES]]}
            log.warning("%s Rohmonat %s: %d Tick(s) im geschlossenen Wochenendmarkt nach Freigabe "
                        "VERWORFEN (Ruecksprung/nicht eindeutige Ortszeit), z.B. %s", u.key, _ymstr(ym),
                        dropped["dropped_ticks"], ", ".join(dropped["dropped_raw"][:3]))
            sel = sel[~drop].reset_index(drop=True)
            raw_all = raw_all[~drop]
        try:
            sel = sel.assign(utc_ms=self._convert(raw_all, rule, u, ym))
        except TimezoneError as e:
            # Default: nie raten -> Quarantaene. Liegen ALLE betroffenen Ticks (auch die
            # von Rueckspruengen) im geschlossenen Wochenendmarkt, darf ein Mensch
            # freigeben (--accept-weekend-ticks): diese Ticks werden dann verworfen.
            drop, rel = weekend_release(raw_all, self.mode, rule)
            reason = (f"Rohzeit nicht eindeutig nach UTC umrechenbar: {e}" + (
                f" [nur Ticks im geschlossenen Wochenendmarkt: nach Pruefung freigebbar mit "
                f"--accept-weekend-ticks {_ymstr(ym)}, diese {int(drop.sum())} Tick(s) werden dann "
                "verworfen]" if rel is not None else
                " [nicht freigebbar: Ticks ausserhalb des geschlossenen Wochenendmarkts]"))
            if accepted:
                raise Quarantined(u, ym, reason, None)
            self._quarantine(u, ym, reason, None, note, releasable=rel)
        if accepted and dropped is None:
            return sel, {"accepted_manually": True, "marker": str(q)}
        if not len(sel):
            return sel, dict(dropped or {})
        nb, pad = self._neighbours(ym)
        key = (u, ym, pad["left"], pad["right"], dropped is not None)
        chk = self.tz.get(key)
        if chk is None:
            bounds = raw_month_bounds_ms(ym)
            # Fahrplan "undecided" und Kandidaten rechnen hier verschieden: unter jedem
            # Kandidaten pruefen, bestanden wenn einer passt (welcher gilt, entscheidet
            # der Mensch im Fahrplan; build_month haelt betroffene UTC-Monate zurueck).
            # Sonst (auch "undecided" mit identischer Umrechnung): Regel je Rohmonat
            # (undecided -> erster Kandidat)
            cands = (list(rule.candidates) if isinstance(rule, UndecidedRule)
                     and (u, ym) in self.undecided else [None])
            results = [self._check(sel["raw_ms"].to_numpy(), ym, bounds, nb, c) for c in cands]
            chk = next((c for c in results if c["ok"]), results[0])
            if cands != [None]:
                chk = {**chk, "candidates_checked": {c.name: r["status"] for c, r in zip(cands, results)}}
            chk["padding"] = pad
            if not pad["provisional"]:
                self.tz[key] = chk
        hold = [_ymstr(_ym_add(ym, k)) for k, cond in (
            (-1, pad["left_missing"] and chk.get("leading_suspect")),
            (1, not pad["right_final"] and chk.get("trailing_suspect"))) if cond]
        if chk["ok"] and hold:
            # einzelne Abweichung am Rand ohne Kontext dahinter: koennte der Beginn
            # eines Wechsels sein -> nicht schreiben, bis der Nachbarmonat da ist
            ex = (chk.get("warnings") or ["?"])[0]
            raise Quarantined(u, ym, f"VORLAEUFIG zurueckgehalten (nicht gespeichert): abweichender "
                              f"Eintrag am Monatsrand ohne Bestaetigung ({ex}) - wartet auf Rohmonat "
                              f"{', '.join(hold)}", chk)
        if not chk["ok"]:
            reason = ("" if chk["status"] == "not_checkable" else
                      f"Zeitzone passt nicht zu {rule.describe()}: ") + chk["reason"]
            if pad["provisional"]:
                miss = [_ymstr(_ym_add(ym, k)) for k, s_ in ((-1, "left_missing"), (1, "right_missing"))
                        if pad[s_]]
                raise Quarantined(u, ym, f"VORLAEUFIG (Nachbar-Rohmonat {', '.join(miss)} fehlt im "
                                  f"Cache, nicht gespeichert): {reason}", chk)
            if dropped is not None:
                # freigegebener Marker bleibt; der Rest besteht trotzdem nicht
                raise Quarantined(u, ym, f"nach Verwerfen von {dropped['dropped_ticks']} freigegebenen "
                                  f"Wochenend-Tick(s) weiterhin: {reason}", chk)
            self._quarantine(u, ym, reason, chk, note, releasable=chk.get("releasable_weekend"))
        if old is not None and q.exists() and not self.report_only and not pad["provisional"]:
            q.unlink(missing_ok=True)                  # neu geprueft und bestanden
            log.warning("Quarantaene-Marker %s (aeltere Pruefung) entfernt: %s besteht die Pruefung "
                        "unter %s", q.name, _ymstr(ym), rule.describe())
        if dropped is not None:
            chk = {**chk, "accepted_manually": True, "marker": str(q), **dropped}
        return sel, chk

    def check_undecided(self, u: Unit, r: tuple[int, int], ym: tuple[int, int]) -> None:
        """Fahrplan "undecided": Ticks des Rohmonats r, die die Kandidaten verschieden
        umrechnen und unter IRGENDEINEM Kandidaten in den UTC-Monat ym fallen ->
        RuleUndecided (UTC-Monat nicht schreiben; kein Marker)."""
        und = self.undecided.get((u, r))
        if und is None:
            return
        lo_ms = int(datetime(*ym, 1, tzinfo=UTC).timestamp() * 1000)
        hi_ms = int(datetime(*_ym_add(ym, 1), 1, tzinfo=UTC).timestamp() * 1000)
        d_raw, d_utcs = und
        hit = np.zeros(len(d_raw), dtype=bool)
        for x in d_utcs:
            hit |= (x >= lo_ms) & (x < hi_ms)
        if not hit.any():
            return
        ranges = _ranges_of(d_raw[hit])
        e = RuleUndecided(
            f"UTC-Monat {_ymstr(ym)}: {int(hit.sum())} Tick(s) aus Rohmonat {_ymstr(r)} rechnen "
            f"die Kandidaten {', '.join(c.name for c in rule_candidates(rule_for(self.mode, r)))} "
            f"verschieden um (Fahrplan 'undecided'), roh "
            + ", ".join(f"{a}..{b}" for a, b in ranges[:5])
            + " - nicht geschrieben (kein Marker; jeder Lauf prueft neu aus dem Cache, "
            "bis der Fahrplan entschieden ist)", d_raw[hit], ranges)
        e.raw_month, e.unit = r, u
        raise e

    def check_following(self, ym: tuple[int, int]) -> None:
        """
        Folge-Rohmonat ym+1 (falls gecached) faellt mit einem Stempelungswechsel
        durch, der schon in ym beginnt (Lauf mit Eintrag aus dem ym-Kontext, z.B.
        Sonntagsoeffnung/Freitagsschluss am Ende von ym): UTC-Monat ym nicht
        schreiben (Quarantined, Marker liegt am Folgemonat).
        """
        nxt = _ym_add(ym, 1)
        u = self.unit(nxt)
        if not u.path(self.data_root).exists():
            return
        try:
            self.raw_slice(u, nxt)
        except Quarantined as q:
            if (q.check or {}).get("switch_reaches_prev_month"):
                runs = q.check.get("switch_runs") or [[]]
                first = runs[0][0] if runs[0] else {}
                raise Quarantined(q.unit, q.ym, (
                    f"Folge-Rohmonat {_ymstr(nxt)}: Stempelungswechsel beginnt schon in {_ymstr(ym)} "
                    f"({first.get('raw')} {first.get('kind')} {first.get('deviation_h', 0):+d} h) - "
                    f"{_ymstr(ym)} nicht geschrieben. Folgemonat: {q.reason}"), q.check)
        except (CorruptZip, FormatError):
            return

    def reverify(self, ym: tuple[int, int]) -> bool | None:
        """
        Rohmonate fuer UTC-Monat ym aus dem CACHE neu pruefen (kein Download).
        None: Roh-Cache fehlt/kaputt (nicht pruefbar); sonst tz_right_padding.
        Quarantined bei Fehler (Marker wie ueblich, ausser vorlaeufig/report_only).
        """
        pad = True
        for r in needed(self.symbol, ym, self.mode):
            u = self.unit(r)
            st = self.state(u)[0]
            if st == "empty":
                continue
            if st != "data":
                return None
            try:
                _, tz = self.raw_slice(u, r)
            except CorruptZip:
                return None
            self.check_undecided(u, r, ym)                 # RuleUndecided
            if r == ym:
                pad = bool((tz.get("padding") or {}).get("right_final", True))
        self.check_following(ym)
        return pad

    def _quarantine(self, u: Unit, ym, reason: str, check: dict | None, note: str | None = None,
                    releasable: dict | None = None):
        rule = rule_for(self.mode, ym)
        zone, shift = rule_identity(rule)
        info = {"reason": reason, "check": check, "since": self.now().isoformat(),
                "accepted": False, "tz_rule": rule.name, "raw_tz": zone,
                "tz_shift_h": shift, "tz_check_version": TZ_CHECK_VERSION}
        if isinstance(rule, UndecidedRule):
            info["tz_candidates"] = [c.name for c in rule.candidates]
        if releasable:
            # alle Ruecksprung-/Verbotene-Stunden-Ticks im geschlossenen Wochenendmarkt:
            # "accepted": true (--accept-weekend-ticks) verwirft genau diese Ticks
            info["releasable_weekend"] = releasable
        if note:
            info["note"] = note
            reason = f"{reason} [{note}]"
            info["reason"] = reason
        if self.report_only:
            log.error("QUARANTAENE (nur Bericht, kein Marker) %s Rohmonat %d-%02d: %s", u.key, *ym, reason)
        else:
            dk.write_json_atomic(u.qmarker(self.data_root, ym), info)
            log.error("QUARANTAENE %s Rohmonat %d-%02d: %s", u.key, *ym, reason)
        raise Quarantined(u, ym, reason, check)


# ------------------------------------------------------------- monatsbau ---
def _raw_months_for(ym: tuple[int, int], mode: str) -> list[tuple[int, int]]:
    lo = int(datetime(*ym, 1, tzinfo=UTC).timestamp() * 1000)
    hi = int(datetime(*_ym_add(ym, 1), 1, tzinfo=UTC).timestamp() * 1000) - 1
    # "undecided": alle Kandidaten (fruehester Anfang, spaetestes Ende)
    los, his = [], []
    for k in range(4):
        los.append(int(utc_to_raw_ms_mode(np.array([lo]), mode, pick=k)[0]))
        his.append(int(utc_to_raw_ms_mode(np.array([hi]), mode, pick=k)[0]))
    a, b = (datetime.fromtimestamp(x / 1000, UTC) for x in (min(los), max(his)))
    out, r = [], (a.year, a.month)
    while r <= (b.year, b.month):
        out.append(r)
        r = _ym_add(r, 1)
    return out


def needed(symbol: str, ym: tuple[int, int], mode: str = "tick") -> list[tuple[int, int]]:
    """Rohmonate, die UTC-Monat ym ueberdecken (aus dem Stempelungsfahrplan
    berechnet, nicht fest verdrahtet): fuer UTC-5/UTC-4 M-1 (dessen letzte
    4 bzw. 5 h) und M; vor Historienbeginn entfaellt."""
    return [r for r in _raw_months_for(ym, mode) if not before_history(symbol, r)]


def build_month(ctx: Ctx, ym: tuple[int, int]) -> tuple[pd.DataFrame, dict]:
    """UTC-Monat aus den Rohmonaten M-1 und M (beide erledigt) bauen."""
    ms, me = (pd.Timestamp(datetime(*ym, 1, tzinfo=UTC)),
              pd.Timestamp(datetime(*_ym_add(ym, 1), 1, tzinfo=UTC)))
    parts, stats = [], {"raw_months": {}, "histdata_status": {}, "tz_right_padding": True}
    for r in needed(ctx.symbol, ym, ctx.mode):
        u = ctx.unit(r)
        info = {"file": u.name, "state": ctx.state(u)[0]}
        stats["raw_months"][_ymstr(r)] = info
        if info["state"] != "data":
            continue
        sel, tz = ctx.raw_slice(u, r)                 # mit utc_ms (Regel des Rohmonats -> UTC)
        if r == ym:
            # rechter Nachbar (Rohmonat M+1) fehlte bei der Pruefung von M: Datei
            # wird mit tz_right_padding=false geschrieben und spaeter nachgeprueft
            stats["tz_right_padding"] = bool((tz.get("padding") or {}).get("right_final", True))
        lo_ms, hi_ms = ms.value // 1_000_000, me.value // 1_000_000
        ctx.check_undecided(u, r, ym)
        utc_ms = sel["utc_ms"].to_numpy()
        sel = sel[(utc_ms >= lo_ms) & (utc_ms < hi_ms)]
        if tz.get("dropped_ticks"):
            stats["dropped_ticks"] = stats.get("dropped_ticks", 0) + tz["dropped_ticks"]
            stats["dropped_raw"] = (stats.get("dropped_raw", []) + tz["dropped_raw"])[:N_EXAMPLES]
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
    if stats.get("dropped_ticks"):
        rep["warning"] = (f"{stats['dropped_ticks']} Tick(s) im geschlossenen Wochenendmarkt nach "
                          "manueller Freigabe (releasable_weekend) VERWORFEN, nicht umgerechnet: "
                          + ", ".join(stats["dropped_raw"][:5]))
        log.warning("%s %s: %s", symbol, _ymstr(ym), rep["warning"])
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
    _check_symbol(symbol)
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

    removed_stores = retire_removed_symbol_stores(data_root)
    in_range = set(dk.months(start, end))
    stale_out: dict[str, dict] = {}
    relabelled: list[tuple[int, int]] = []
    retired_recheck: dict[str, dict] = {}
    unverified: list[tuple[int, int]] = []
    # ALLE Monatsdateien des Symbols/Modus pruefen, nicht nur [start, end]:
    # verschobene Altdateien nie im Store lassen. only_padding: zweiter Durchgang
    # am Laufende (Rohmonat M+1 evtl. erst in diesem Lauf geladen)
    def scan_store(only_padding: bool = False) -> None:
        for ym, out in _store_months(data_root, sym, mode):
            if stored_mode(out) != PARQUET_MODE[mode]:
                continue                           # store_mode_conflict, unten bzw. nicht unsere Datei
            st, old_tz = classify_store_file(out, mode, ym)
            if st == "current" or (only_padding and st != "recheck_padding"):
                continue
            if st == "recheck_padding":
                nu = ctx.unit(_ym_add(ym, 1))
                if not nu.path(data_root).exists() and ctx.state(nu)[0] != "empty":
                    continue                       # Rohmonat M+1 noch nicht da: spaeter nachpruefen
            if st in ("recheck", "recheck_padding"):
                # gleiche Umrechnung: aus dem Roh-Cache neu pruefen (kein Download)
                try:
                    pad = ctx.reverify(ym)
                except (Quarantined, RuleUndecided) as q:
                    moved = retire_stale_store_file(data_root, out)
                    retired_recheck[_ymstr(ym)] = {"reason": getattr(q, "reason", str(q)),
                                                   "retired_to": str(moved)}
                    log.error("%s: Nachpruefung (%s) durchgefallen - aus dem Store genommen (%s): %s",
                              out.name, st, moved, q.reason)
                    if ym in in_range:
                        stale_tz[ym] = old_tz
                    continue
                if pad is None:
                    if st == "recheck":
                        unverified.append(ym)
                        log.warning("%s: aeltere Pruefversion (%r), Roh-Cache fehlt - nicht nachgeprueft, "
                                    "Datei bleibt", out.name, old_tz)
                    continue
                if st == "recheck_padding" and not pad:
                    continue
                relabel_store_file(out, mode, pad, store_tz_label(mode, ym))
                relabelled.append(ym)
                log.info("%s: nachgeprueft (%s) und bestanden -> source_timezone %r, tz_right_padding=%s",
                         out.name, st, store_tz_label(mode, ym), pad)
                continue
            moved = None
            if st == "stale_shifted":
                # SOFORT aus dem Store nehmen (nicht erst nach erfolgreichem Neubau):
                # scheitert der Neubau, sieht der Bot eine Luecke statt eines
                # verschobenen Monats
                moved = retire_stale_store_file(data_root, out)
            log.warning("%s: source_timezone %r statt %r (andere Umrechnung%s) - %s", out.name,
                        old_tz, store_tz_label(mode, ym),
                        ", UTC-Zeiten verschoben" if moved else ", in diesem Monat gleiche UTC-Zeiten",
                        (f"aus dem Store genommen ({moved}); " if moved else "bleibt bis zum Neubau; ")
                        + ("wird aus dem Roh-Cache neu gebaut" if ym in in_range else
                           "ausserhalb von --start/--end, nicht neu gebaut"))
            if ym in in_range:
                stale_tz[ym] = old_tz
            else:
                stale_out[_ymstr(ym)] = {"source_timezone": old_tz,
                                         "retired_to": str(moved) if moved else None}


    scan_store()

    for ym in dk.months(start, end):
        if not _month_closed(ctx.now(), ym, mode):
            log.info("skip %s %s (Monat nicht abgeschlossen: Rohmonatsende %s in UTC + %g h)", sym,
                     _ymstr(ym), rule_for(mode, ym).describe(), SETTLE_S / 3600)
            continue
        if before_history(sym, ym):
            unavailable[ym] = f"vor Historienbeginn {HISTORY_START[sym][0]}-{HISTORY_START[sym][1]:02d}"
            continue
        c = mode_conflict(ym)
        if c:
            conflicts[ym] = c
            continue
        out = store_path(data_root, sym, *ym, mode)
        if ym in stale_tz:
            todo.append(ym)                    # neu bauen (Datei verschoben oder noch da)
            continue
        if out.exists() and not rebuild:
            skipped_existing += 1
            continue
        todo.append(ym)
    log.info("START %s mode=%s: %d Monate offen (%d fertig uebersprungen, %d vor Historienbeginn, "
             "%d mit alter Zeitzone neu zu bauen)", sym, mode, len(todo), skipped_existing,
             len(unavailable), len(stale_tz))

    outcomes: dict[Unit, Outcome] = {}
    written, validation_errors, quarantine, pending, missing = [], {}, {}, {}, {}
    undecided: dict[tuple[int, int], dict] = {}      # Fahrplan "undecided", Kandidaten verschieden
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
        except RuleUndecided as e:
            undecided[ym] = undecided_entry(e)
            log.error("REGEL OFFEN %s: %s", label, e)
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
        write_parquet(out, df, mode, right_padding=stats.get("tz_right_padding", True),
                      label=store_tz_label(mode, ym))
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
        if not client.halted:
            scan_store(only_padding=True)
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
                          state, reported_gaps, rebuild, stale_tz, stale_out, relabelled,
                          retired_recheck, unverified, removed_stores, undecided)
        dk.write_json_atomic(run_report_path(data_root, sym, mode), rep)
        log.info("FERTIG %s: status=%s exit=%d, %d Monate geschrieben, %d Requests (Report %s)", sym,
                 rep["status"], rep["exit_code"], len(written), client.requests,
                 run_report_path(data_root, sym, mode))
    return rep


def _run_report(ctx, start, end, started, todo, outcomes, written, validation_errors, quarantine,
                pending, unavailable, missing, conflicts, skipped_existing, state, reported_gaps,
                rebuild, stale_tz=None, stale_out=None, relabelled=None, retired_recheck=None,
                unverified=None, removed_stores=None, undecided=None) -> dict:
    stale_tz, stale_out, relabelled = stale_tz or {}, stale_out or {}, relabelled or []
    undecided = undecided or {}
    client = ctx.client
    manual = [{"month": _ymstr(k), "kind": "quarantine", **{x: v[x] for x in ("reason", "path")}}
              for k, v in quarantine.items()]
    manual += [{"month": _ymstr(k), "kind": "missing_in_history", "reason": v}
               for k, v in missing.items()]
    manual += [{"month": _ymstr(k), "kind": "store_mode_conflict", "reason": v}
               for k, v in conflicts.items()]
    # Fahrplan "undecided" und Kandidaten rechnen hier verschieden: manueller Fall
    # (Fahrplan entscheiden), kein Marker
    manual += [{"month": _ymstr(k), "kind": "rule_undecided", "reason": v["reason"]}
               for k, v in undecided.items()]
    manual.sort(key=lambda r: r["month"])
    done = set(written) | set(validation_errors) | set(quarantine) | set(unavailable) | \
        set(missing) | set(conflicts) | set(undecided)
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
        status = "rule_undecided" if all(r["kind"] == "rule_undecided" for r in manual) else "quarantine"
        code = EXIT_MANUAL
    else:
        status, code = "complete", EXIT_OK
    if manual and code != EXIT_MANUAL:
        status += "+quarantine"
    hints = []
    if manual:
        log.error("MANUELLE PRUEFUNG NOETIG %s (%s): %s", ctx.symbol, ctx.mode,
                  "; ".join(f"{r['month']} {r['kind']}" for r in manual))
        if undecided:
            hints.append(f"{len(undecided)} Monat(e) im Fahrplan-Zeitraum 'undecided' mit Ticks, die die "
                         "Kandidatenregeln verschieden umrechnen (months_rule_undecided): nicht "
                         "geschrieben; Regel in config/histdata_timezone.yaml entscheiden (Survey).")
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
        # veraltete Dateien ausserhalb von [start, end]: verschobene sind schon aus
        # dem Store genommen (retired_to), unverschobene bleiben bis zum Neubau
        "stale_retired_out_of_range": [k for k, v in sorted(stale_out.items()) if v["retired_to"]],
        "stale_out_of_range": stale_out,
        # gleiche Umrechnung, aus dem Roh-Cache nachgeprueft und bestanden: nur
        # Metadaten erneuert (kein Neubau, keine Requests)
        "relabelled_timezone": [_ymstr(k) for k in sorted(relabelled)],
        # Nachpruefung (neue Pruefversion / rechter Nachbar jetzt da) durchgefallen:
        # aus dem Store genommen, Quarantaene
        "retired_after_recheck": retired_recheck or {},
        # aeltere Pruefversion, Roh-Cache fehlt: Datei bleibt ungeprueft
        "unverified_after_version_bump": [_ymstr(k) for k in sorted(unverified or [])],
        # Store-Verzeichnisse nicht mehr unterstuetzter Symbole (EURUSD) nach metals/stale/
        "retired_removed_symbols": removed_stores or [],
        # Fahrplan "undecided": UTC-Monate mit verschieden umgerechneten Ticks (roh)
        "months_rule_undecided": {_ymstr(k): v for k, v in sorted(undecided.items())},
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
            "months_rule_undecided": {_ymstr(k): v for k, v in sorted(undecided.items())},
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


def undecided_entry(e: RuleUndecided) -> dict:
    """Berichtszeile fuer einen wegen "undecided" nicht geschriebenen UTC-Monat."""
    return {"raw_month": _ymstr(e.raw_month) if e.raw_month else None,
            "raw_ranges": [list(r) for r in e.ranges],
            "differing_ticks": int(len(e.differing_raw)), "reason": str(e)}


def _check_symbol(symbol: str) -> None:
    if symbol in REMOVED_SYMBOLS:
        raise ValueError(REMOVED_SYMBOLS[symbol])
    if symbol not in SYMBOLS:
        raise ValueError(f"unbekanntes Symbol {symbol}; bekannt: {', '.join(SYMBOLS)}")


# ----------------------------------------------------------- verify-month ---
def verify_month(symbol: str, ym: tuple[int, int], data_root: Path | None = None, *,
                 mode: str = "tick", now=None, stop: threading.Event | None = None,
                 **client_kw) -> dict:
    """
    Ein UTC-Monat: Rohmonate M-1 und M laden (max. 4 Requests, Cache wird
    genutzt), decodieren, Zeitzone pruefen, Bars bauen, validieren. Schreibt
    nur den Verify-Report (kein Parquet, keine Quarantaene-Marker; vorhandene
    Marker werden nur ausgewertet). exit_code: 0 ok, 2 Validierung,
    3 Download/pending/429-Budget/Lock, 4 Zeitzone/Format (Quarantaene).
    """
    _check_symbol(symbol)
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
        ctx = Ctx(symbol, mode, data_root, Client(data_root, stop=stop, **client_kw), now,
                  report_only=True)               # nur Bericht: keine Quarantaene-Marker schreiben/loeschen
        rep: dict = {"symbol": symbol, "month": _ymstr(ym), "source": SOURCE_BY_MODE[mode],
                     "mode": mode}
        out = store_path(data_root, symbol, *ym, mode)
        if out.exists():
            # nur berichten, nichts aendern (das macht der naechste normale Lauf)
            if stored_mode(out) != PARQUET_MODE[mode]:
                rep["store_file"] = {"path": str(out), "state": "mode_conflict",
                                     "histdata_mode": stored_mode(out)}
            else:
                st, old_tz = classify_store_file(out, mode, ym)
                rep["store_file"] = {"path": str(out), "state": st, "source_timezone": old_tz,
                                     "expected": store_tz_label(mode, ym)}
                if st != "current":
                    log.warning("VERIFY %s: Store-Datei %s hat source_timezone %r (%s) - der "
                                "naechste normale Lauf %s", _ymstr(ym), out.name, old_tz, st,
                                "prueft sie aus dem Roh-Cache nach" if st.startswith("recheck") else
                                "baut sie neu" + (" (vorher aus dem Store genommen)"
                                                  if st == "stale_shifted" else ""))
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
        except RuleUndecided as e:
            rep.update(ok=False, exit_code=EXIT_MANUAL, status="rule_undecided", reason=str(e),
                       rule_undecided=undecided_entry(e))
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
                 rule_for(mode, ym).describe(), rep["ok"], rep["exit_code"], rep.get("rows"),
                 rep.get("spread_median"), tzs, rep["requests"])
        return rep
    finally:
        lock.close()


# ------------------------------------------------- regel offline abgleichen ---
def evaluate_raw_month(data_root: Path, symbol: str, ym: tuple[int, int], mode: str = "tick",
                       rule: "TzRule | str | None" = None, drop_releasable_weekend: bool = False) -> dict:
    """
    Einen Rohmonat NUR aus dem Roh-Cache gegen eine Stempelungsregel pruefen
    (Default: Regel aus dem Fahrplan; z.B. fuer den Abgleich von Kandidaten).
    Wie im echten Bau mit den Ticks der gecachten Nachbar-Rohmonate als Kontext.
    Kein Download, keine Marker, keine Store-Aenderung. Rueckgabe: Bericht von
    check_timezone plus cached, neighbours (prev/next vorhanden, *_missing =
    sollte existieren, fehlt im Cache) und convert_error (nicht eindeutige Ortszeit).
    releasable_weekend: verwerfbare Wochenend-Ticks unter dieser Regel (weekend_release,
    vor dem Verwerfen) oder None. drop_releasable_weekend=True: diese Ticks vor der
    Pruefung verwerfen (wie nach --accept-weekend-ticks); dropped_weekend_ticks = Anzahl.
    "undecided": rule_undecided = verschieden umgerechnete Ticks (oder None).
    """
    _check_symbol(symbol)
    data_root = Path(data_root)
    rule = as_rule(rule) if rule is not None else rule_for(mode, ym)
    ctx = Ctx(symbol, mode, data_root, None, dk.utc_now, report_only=True)
    base = {"symbol": symbol, "mode": mode, "month": _ymstr(tuple(ym)), "rule": rule.name,
            "rule_detail": rule.describe()}
    u = ctx.unit(tuple(ym))
    if not u.path(data_root).exists():
        return {**base, "cached": False, "status": "not_cached", "ok": False,
                "reason": f"Rohdatei {u.name} nicht im Cache", "neighbours": {}}
    try:
        raw = ctx._month_rows(u, tuple(ym))["raw_ms"].to_numpy()
    except (CorruptZip, FormatError) as e:
        return {**base, "cached": True, "status": "unreadable", "ok": False,
                "reason": f"Rohdatei nicht lesbar: {e}", "neighbours": {}}
    nb, pad = ctx._neighbours(tuple(ym))
    drop, rel = weekend_release(raw, mode, rule)
    dropped = 0
    if drop_releasable_weekend and rel is not None:
        dropped = int(drop.sum())
        raw = raw[~drop]
    rep = check_timezone(raw, symbol, raw_month_bounds_ms(tuple(ym)), mode, rule=rule, neighbours=nb)
    rep.update(convert_error=None, releasable_weekend=rel, dropped_weekend_ticks=dropped,
               rule_undecided=None)
    try:
        raw_to_utc_ms(raw, rule)
    except RuleUndecided as e:
        rep["rule_undecided"] = {"raw_ranges": [list(r) for r in e.ranges],
                                 "differing_ticks": int(len(e.differing_raw))}
    except TimezoneError as e:
        rep.update(convert_error=str(e), status="failed", ok=False,
                   reason="; ".join(x for x in (rep.get("reason"), f"nicht eindeutige Ortszeit: {e}") if x))
    rep.update(base)
    rep.update(cached=True, neighbours={"prev": pad["left"], "next": pad["right"],
                                        "prev_missing": pad["left_missing"],
                                        "next_missing": pad["right_missing"]})
    return rep


# ------------------------------------------------- wochenend-ticks freigeben ---
def accept_weekend_ticks(symbol: str, ym: tuple[int, int], data_root: Path | None = None, *,
                         mode: str = "tick", out=None) -> int:
    """
    --accept-weekend-ticks: Quarantaene-Marker des ROHmonats ym freigeben
    ("accepted": true), aber NUR wenn er releasable_weekend traegt (alle
    betroffenen Ticks im geschlossenen Wochenendmarkt, WEEKEND_CLOSED_RAW) und
    zur aktuellen Regel gehoert. Gibt die zu verwerfenden Ticks aus (aus dem
    Roh-Cache neu berechnet und mit dem Marker abgeglichen). Kein Download.
    Exit 0 freigegeben, 2 nicht freigebbar (Grund auf stderr), 3 Lock belegt.
    """
    out = out or sys.stdout
    _check_symbol(symbol)
    data_root = Path(data_root or dk.default_data_root())
    ym = tuple(ym)

    def refuse(msg: str) -> int:
        log.error("--accept-weekend-ticks %s %s: %s", symbol, _ymstr(ym), msg)
        print(f"NICHT freigegeben: {msg}", file=sys.stderr)
        return EXIT_VALIDATION

    lock = acquire_lock(data_root, symbol)
    if lock is None:
        log.error("%s: Lock belegt (anderer Lauf aktiv) - spaeter erneut", symbol)
        return EXIT_GAPS
    try:
        ctx = Ctx(symbol, mode, data_root, None, dk.utc_now, report_only=True)
        u = ctx.unit(ym)
        q = u.qmarker(data_root, ym)
        if not q.exists():
            return refuse(f"kein Quarantaene-Marker {q}")
        info = ctx.read_qmarker(q)
        if info is None:
            return refuse(f"Marker {q} unlesbar (kein gueltiges JSON)")
        rel = info.get("releasable_weekend")
        if not isinstance(rel, dict):
            return refuse(f"Marker {q.name} ist nicht freigebbar (Ticks ausserhalb des geschlossenen "
                          f"Wochenendmarkts oder andere Ursache): {info.get('reason')}")
        rule = rule_for(mode, ym)
        if (info.get("raw_tz"), info.get("tz_shift_h", 0)) != rule_identity(rule) or \
                info.get("tz_check_version") != TZ_CHECK_VERSION:
            return refuse(f"Marker {q.name} stammt aus einer anderen Regel/Pruefversion "
                          f"({info.get('tz_rule')}, v{info.get('tz_check_version')}) - erst einen "
                          "normalen Lauf neu pruefen lassen")
        ticks = None
        if u.path(data_root).exists():
            try:
                raw = ctx._month_rows(u, ym)["raw_ms"].to_numpy()
            except (CorruptZip, FormatError) as e:
                return refuse(f"Rohdatei {u.name} nicht lesbar: {e}")
            drop, now_rel = weekend_release(raw, mode, rule)
            if now_rel is None or now_rel["raw_ranges"] != rel.get("raw_ranges"):
                return refuse("verwerfbare Ticks im Roh-Cache passen nicht (mehr) zum Marker - "
                              "Marker loeschen (= neu pruefen)")
            ticks = [_raw_iso(int(x), ms_digits=True) for x in raw[drop]]
        print(f"{symbol} {mode} Rohmonat {_ymstr(ym)}: folgende Ticks (roh, geschlossener "
              f"Wochenendmarkt) werden beim naechsten Lauf VERWORFEN, nicht umgerechnet:", file=out)
        print(f"  Ruecksprung-Ticks: {rel.get('reversal_ticks')}, Ticks in verbotenen Stunden: "
              f"{rel.get('impossible_hour_ticks')}", file=out)
        for a, b in rel.get("raw_ranges") or []:
            print(f"  Bereich {a} .. {b}", file=out)
        for t in (ticks or [])[:500]:
            print(f"  {t}", file=out)
        if ticks and len(ticks) > 500:
            print(f"  ... ({len(ticks)} insgesamt)", file=out)
        info.update(accepted=True, accepted_via="--accept-weekend-ticks",
                    accepted_at=dk.utc_now().isoformat())
        dk.write_json_atomic(q, info)
        log.warning("%s Rohmonat %s: Wochenend-Ticks freigegeben (%s) - werden verworfen", symbol,
                    _ymstr(ym), q)
        return EXIT_OK
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
    p.add_argument("--symbol", required=True, type=str.upper, help=f"eines von {', '.join(SYMBOLS)}")
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
    p.add_argument("--accept-weekend-ticks", metavar="YYYY-MM",
                   help="Quarantaene des ROHmonats freigeben, wenn alle betroffenen Ticks im "
                        "geschlossenen Wochenendmarkt liegen (releasable_weekend): diese Ticks "
                        "werden dann verworfen; sonst Exit 2. Kein Download")
    p.add_argument("-v", "--verbose", action="store_true")
    a = p.parse_args(argv)
    try:
        _check_symbol(a.symbol)
    except ValueError as e:
        p.error(str(e))
    if a.attempts < 1 or a.max_consecutive_failures < 0 or \
            min(a.pause, a.backoff_base, a.max_backoff, a.max_rate_limit_wait) < 0:
        p.error("--attempts >= 1, uebrige Zahlen >= 0")
    if a.rate_limit_pause < rl.MIN_PAUSE_S:
        p.error(f"--rate-limit-pause muss >= {rl.MIN_PAUSE_S:g} s sein (15 min)")
    try:
        start = _ym(a.start) if a.start else HISTORY_START[a.symbol]
        end = _ym(a.end) if a.end else (dk.utc_now().year, dk.utc_now().month)
        verify = _ym(a.verify_month) if a.verify_month else None
        accept = _ym(a.accept_weekend_ticks) if a.accept_weekend_ticks else None
    except ValueError:
        p.error("Monate als YYYY-MM angeben")
    if verify and accept:
        p.error("--verify-month und --accept-weekend-ticks nicht zusammen")

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
    if accept:
        return accept_weekend_ticks(a.symbol, accept, mode=a.mode)
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
