#!/usr/bin/env python3
"""
Wirtschaftskalender (US/Eurozone) fuer die News-Sperre eines Gold/Silber-Bots
laden, historisch und laufend.

- Quellen (kostenlos, je eigene Funktion + eigener Parser):
    fred   FRED API fred/release/dates -> nur Datum, Uhrzeit per Regel
           (config/calendar.yaml), Release-IDs per Namens-Check abgesichert
    fomc   federalreserve.gov: fomccalendars.htm (aktuelle Jahre) und
           fomchistorical{YYYY}.htm (aeltere Jahre, roh gecached)
    ecb    ecb.europa.eu: Sitzungskalender des EZB-Rats
    adhoc  config/calendar_adhoc.csv (Notfallsitzungen, status=adhoc)
    ff     nfs.faireconomy.media Wochenkalender: NUR Kreuzcheck kommender
           Termine (Abweichungen > tolerance -> Report), nichts uebernommen,
           Fehler nicht fatal
- Uhrzeiten immer Lokalzeit + zoneinfo -> UTC (Sommerzeit!), nie fester Offset
- Merge mit dem letzten Stand: gleiche event_id -> neueste Quelle gewinnt;
  ts_utc geaendert -> status rescheduled; Event einer Quelle verschwindet
  innerhalb des neu abgerufenen Zeitraums -> status cancelled (Zeile bleibt)
- harte Validierung vor dem Schreiben; schlaegt eine Quelle fehl, wird NICHTS
  geschrieben (alter Stand bleibt), Report + Exit-Code != 0

Ablage (DATA_ROOT = $MARKETDATA_DATA_ROOT, sonst <repo>/data):
    calendar/events.parquet                               aktueller Stand
    calendar/snapshots/events-YYYYMMDDTHHMMSSZ.parquet    Stand je Abruf
    calendar/reports/calendar-YYYYMMDDTHHMMSSZ.json       Report je Lauf
    raw/fomc/fomchistorical{YYYY}.htm                     Roh-Cache (aendert
        sich nicht mehr; Datei loeschen = beim naechsten Lauf neu laden)

Schema events.parquet (verbindlich, wird von anderen Komponenten gelesen):
    event_id    str   f"{event_type}:{Datum lokal YYYY-MM-DD}" (stabil)
    ts_utc      datetime64[ns, UTC]
    event_type  str   z.B. NFP, CPI, FOMC_STATEMENT, ECB_RATE_DECISION
    country     str   "US" | "EA"
    impact      str   "high" | "medium" | "low"
    source      str   "fred" | "fomc" | "ecb" | "adhoc"
    status      str   "scheduled" | "rescheduled" | "cancelled" | "adhoc"
    fetched_at  datetime64[ns, UTC]  Abrufzeit des Laufs, der die Zeile
                                     zuletzt geliefert/geaendert hat

Exit-Codes: 0 ok, 2 Validierung, 3 Quelle fehlgeschlagen, 4 Konfiguration
(z.B. FRED-Namens-Check).

Usage:
    FRED_API_KEY=... python fetch_calendar.py
    python fetch_calendar.py --start-year 2015 --sources fomc,ecb,adhoc --dry-run -v
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from html.parser import HTMLParser
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import requests
import yaml

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = ROOT / "config" / "calendar.yaml"
USER_AGENT = "marketdata-pipeline/1.0"

TIMEOUT_S = 30
MAX_RETRIES = 4            # Versuche insgesamt = MAX_RETRIES + 1
BACKOFF_S = 2.0            # Wartezeit vor Retry n: BACKOFF_S * 2**n
RETRY_STATUS = {429, 500, 502, 503, 504}
PAUSE_S = 1.0              # Pause nach jedem echten Request (hoeflich)

SOURCES = ("fred", "fomc", "ecb", "adhoc", "ff")
COLUMNS = ["event_id", "ts_utc", "event_type", "country", "impact", "source",
           "status", "fetched_at"]
IMPACTS = {"high", "medium", "low"}
STATUSES = {"scheduled", "rescheduled", "cancelled", "adhoc"}
COUNTRIES = {"US", "EA"}
TS_DTYPE = "datetime64[ns, UTC]"

log = logging.getLogger("calendar")
_sleep = time.sleep        # in Tests ersetzbar


def default_data_root() -> Path:
    env = os.environ.get("MARKETDATA_DATA_ROOT")
    return Path(env) if env else ROOT / "data"


def calendar_dir(data_root: Path) -> Path:
    return data_root / "calendar"


def events_path(data_root: Path) -> Path:
    return calendar_dir(data_root) / "events.parquet"


def stamp(now: datetime) -> str:
    return now.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


class ValidationError(Exception):
    pass


class DownloadError(Exception):
    pass


class ParseError(Exception):
    pass


class ConfigError(Exception):
    pass


# ----------------------------------------------------------------- config ---
def load_config(path: Path = DEFAULT_CONFIG) -> dict:
    cfg = yaml.safe_load(Path(path).read_text())
    imp: dict[str, str] = {}
    for level in ("high", "medium", "low"):
        for et in cfg["impact"].get(level) or []:
            if et in imp:
                raise ConfigError(f"event_type {et} mehrfach in impact ({imp[et]}, {level})")
            imp[et] = level
    cfg["_impact_of"] = imp
    produced = {r["event_type"] for r in cfg["fred"]["releases"]} | {
        "FOMC_STATEMENT", "FOMC_PRESS_CONF", "FOMC_MINUTES",
        "ECB_RATE_DECISION", "ECB_PRESS_CONF"}
    missing = produced - set(imp)
    if missing:
        raise ConfigError(f"event_type ohne impact-Zuordnung: {sorted(missing)}")
    ids = [r["release_id"] for r in cfg["fred"]["releases"]]
    if len(ids) != len(set(ids)):
        raise ConfigError("doppelte FRED release_id")
    for c in cfg["timezones"].values():
        ZoneInfo(c)        # wirft, wenn die Zone fehlt
    return cfg


# ------------------------------------------------------------------- zeit ---
def parse_hhmm(s: str) -> tuple[int, int]:
    m = re.fullmatch(r"(\d{1,2}):(\d{2})", str(s).strip())
    if not m or int(m[1]) > 23 or int(m[2]) > 59:
        raise ConfigError(f"ungueltige Uhrzeit {s!r}")
    return int(m[1]), int(m[2])


def local_to_utc(d: date, hhmm: str, tz: str) -> pd.Timestamp:
    """Lokales Datum + Uhrzeit in Zone tz -> UTC. Immer zoneinfo, nie Offset."""
    h, mi = parse_hhmm(hhmm)
    local = datetime(d.year, d.month, d.day, h, mi, tzinfo=ZoneInfo(tz))
    # Nicht existierende Lokalzeit (Uhr springt vor) waere ein Konfigfehler
    back = local.astimezone(timezone.utc).astimezone(ZoneInfo(tz))
    if (back.hour, back.minute) != (h, mi):
        raise ConfigError(f"{d} {hhmm} existiert in {tz} nicht (Zeitumstellung)")
    return pd.Timestamp(local).tz_convert("UTC").as_unit("ns")


def make_event(cfg: dict, event_type: str, d: date, hhmm: str, country: str,
               source: str, fetched_at: pd.Timestamp, status: str = "scheduled",
               impact: str | None = None) -> dict:
    tz = cfg["timezones"][country]
    return {
        "event_id": f"{event_type}:{d.isoformat()}",
        "ts_utc": local_to_utc(d, hhmm, tz),
        "event_type": event_type,
        "country": country,
        "impact": impact or cfg["_impact_of"][event_type],
        "source": source,
        "status": status,
        "fetched_at": fetched_at,
    }


def to_frame(rows: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(rows, columns=COLUMNS)
    for c in ("ts_utc", "fetched_at"):
        df[c] = pd.to_datetime(df[c], utc=True).astype(TS_DTYPE)
    for c in COLUMNS:
        if c not in ("ts_utc", "fetched_at"):
            df[c] = df[c].astype(object)
    return df


# ------------------------------------------------------------------- http ---
def _redact(s: str, secrets: list[str]) -> str:
    for x in secrets:
        if x:
            s = s.replace(x, "***")
    return s


def http_get(url: str, params: dict | None = None, retries: int = MAX_RETRIES,
             backoff: float = BACKOFF_S, pause: float = PAUSE_S,
             secrets: list[str] | None = None) -> requests.Response:
    """
    GET mit Retries + Exponential-Backoff bei Netzfehlern und 429/5xx, danach
    Pause. Andere HTTP-Fehler brechen sofort ab. Geheimnisse (API-Key) werden
    aus allen Meldungen entfernt.
    """
    secrets = secrets or []
    err: str = ""
    for attempt in range(retries + 1):
        try:
            r = requests.get(url, params=params, headers={"User-Agent": USER_AGENT},
                             timeout=TIMEOUT_S)
        except requests.RequestException as e:
            err = _redact(f"{type(e).__name__}: {e}", secrets)
        else:
            if r.status_code == 200:
                if pause:
                    _sleep(pause)
                return r
            detail = ""
            try:
                body = r.json()
                if isinstance(body, dict) and body.get("error_message"):
                    detail = f" ({body['error_message']})"     # FRED-Fehlerformat
            except ValueError:
                pass
            msg = _redact(f"HTTP {r.status_code}{detail}: {url}", secrets)
            if r.status_code not in RETRY_STATUS:
                raise DownloadError(msg)
            err = msg
        if attempt < retries:
            wait = backoff * 2 ** attempt
            log.debug("retry %d/%d in %.1fs %s (%s)", attempt + 1, retries, wait, url, err)
            _sleep(wait)
    raise DownloadError(f"{url}: {retries + 1} Versuche fehlgeschlagen ({err})")


# -------------------------------------------------------------- html text ---
_BLOCK_TAGS = {"div", "p", "br", "h1", "h2", "h3", "h4", "h5", "h6", "li", "ul", "ol",
               "tr", "td", "th", "dt", "dd", "dl", "table", "section", "article",
               "header", "footer", "main", "nav", "hr", "tbody", "thead"}


class _TextLines(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []
        self._skip = 0

    def handle_starttag(self, tag, attrs):
        if tag in ("script", "style"):
            self._skip += 1
        if tag in _BLOCK_TAGS:
            self.parts.append("\n")

    def handle_endtag(self, tag):
        if tag in ("script", "style") and self._skip:
            self._skip -= 1
        if tag in _BLOCK_TAGS:
            self.parts.append("\n")

    def handle_data(self, data):
        if not self._skip:
            self.parts.append(re.sub(r"\s+", " ", data.replace("\xa0", " ")))


def html_lines(html: str) -> list[str]:
    """HTML -> nicht-leere Textzeilen (Zeilenumbruch an Block-Elementen)."""
    p = _TextLines()
    p.feed(html)
    p.close()
    lines = (re.sub(r"\s+", " ", ln).strip() for ln in "".join(p.parts).split("\n"))
    return [ln for ln in lines if ln]


MONTHS = {m: i for i, m in enumerate(
    ["january", "february", "march", "april", "may", "june", "july", "august",
     "september", "october", "november", "december"], start=1)}
for _name, _i in list(MONTHS.items()):
    MONTHS[_name[:3]] = _i
MONTHS["sept"] = 9
_MON = r"(?:" + "|".join(sorted(MONTHS, key=len, reverse=True)) + r")"


def month_no(s: str) -> int:
    return MONTHS[s.lower().rstrip(".")]


# ------------------------------------------------------------------- FRED ---
def fred_check_release_name(payload: dict, release_id: int, expected: str) -> str:
    """
    Antwort von fred/release pruefen: Name muss expected enthalten. Die IDs
    stammen aus dem Gedaechtnis, eine falsche ID wuerde still falsche Termine
    liefern -> harter Abbruch (ConfigError).
    """
    rel = payload.get("releases") if isinstance(payload, dict) else None
    if not rel or not isinstance(rel, list):
        raise ConfigError(f"FRED release_id={release_id}: keine Release-Angabe in Antwort")
    r0 = rel[0]
    if int(r0.get("id", -1)) != int(release_id):
        raise ConfigError(f"FRED release_id={release_id}: Antwort fuer id={r0.get('id')}")
    name = str(r0.get("name", ""))
    if expected.lower() not in name.lower():
        raise ConfigError(
            f"FRED release_id={release_id}: Name {name!r} enthaelt nicht {expected!r} - "
            f"release_id in config/calendar.yaml pruefen")
    return name


def parse_fred_release_dates(payload: dict, release_id: int) -> tuple[list[date], list[str]]:
    """
    fred/release/dates (file_type=json) -> sortierte, eindeutige Daten.
    Wochenend-Daten werden verworfen (zurueckgegeben als Warnungen), alles
    andere Unerwartete ist ein ParseError.
    """
    if not isinstance(payload, dict) or not isinstance(payload.get("release_dates"), list):
        raise ParseError(f"FRED release_id={release_id}: 'release_dates' fehlt")
    out, warnings = set(), []
    for item in payload["release_dates"]:
        try:
            rid = int(item["release_id"])
            d = date.fromisoformat(str(item["date"]))
        except (KeyError, TypeError, ValueError) as e:
            raise ParseError(f"FRED release_id={release_id}: ungueltiger Eintrag {item!r} ({e})")
        if rid != int(release_id):
            raise ParseError(f"FRED release_id={release_id}: Eintrag mit release_id={rid}")
        if d.weekday() >= 5:
            warnings.append(f"release_id={release_id}: Wochenend-Datum {d} verworfen")
            continue
        out.add(d)
    return sorted(out), warnings


def fetch_fred(cfg: dict, start_year: int, fetched_at: pd.Timestamp, ctx: dict,
               api_key: str | None = None) -> pd.DataFrame:
    api_key = api_key if api_key is not None else os.environ.get("FRED_API_KEY", "")
    if not api_key:
        raise ConfigError("FRED_API_KEY nicht gesetzt (EnvironmentFile /etc/marketdata.env)")
    fc = cfg["fred"]
    base = fc["base_url"].rstrip("/")
    rows, meta = [], {"releases": {}, "warnings": []}
    for rel in fc["releases"]:
        rid, et = int(rel["release_id"]), rel["event_type"]
        r = http_get(f"{base}/release", {"release_id": rid, "api_key": api_key,
                                          "file_type": "json"}, secrets=[api_key])
        name = fred_check_release_name(r.json(), rid, rel["expected_name"])

        dates: list[date] = []
        offset = 0
        while True:
            params = {
                "release_id": rid, "api_key": api_key, "file_type": "json",
                "include_release_dates_with_no_data": "true",
                "realtime_start": fc["realtime_start"], "realtime_end": fc["realtime_end"],
                "sort_order": "asc", "limit": int(fc["page_limit"]), "offset": offset,
            }
            payload = http_get(f"{base}/release/dates", params, secrets=[api_key]).json()
            ds, warns = parse_fred_release_dates(payload, rid)
            dates += ds
            meta["warnings"] += warns
            n_page = len(payload["release_dates"])
            count = int(payload.get("count", n_page))
            offset += n_page
            if n_page == 0 or offset >= count:
                break
        dates = sorted(set(d for d in dates if d.year >= start_year))
        for d in dates:
            rows.append(make_event(cfg, et, d, rel["time_local"], "US", "fred", fetched_at))
        meta["releases"][et] = {"release_id": rid, "name": name, "dates": len(dates),
                                "first": dates[0].isoformat() if dates else None,
                                "last": dates[-1].isoformat() if dates else None}
        log.info("fred %-15s id=%-3d %-55s %5d Termine", et, rid, repr(name), len(dates))
    ctx["meta"] = meta
    return to_frame(rows)


# ------------------------------------------------------------------- FOMC ---
@dataclass
class Meeting:
    year: int
    start: date
    end: date               # letzter Sitzungstag = Tag des Statements
    sep: bool               # Summary of Economic Projections
    kind: str               # regular | unscheduled | notation_vote | conference_call
    minutes_released: date | None = None
    raw: str = ""
    block: list[str] = field(default_factory=list)


_DAYS = r"(?P<d1>\d{1,2})(?:\s*[-–—]\s*(?:(?P<m2b>" + _MON + r")\.?\s+)?(?P<d2>\d{1,2}))?"
_REST = r"(?P<rest>(?:\s|\*|\([^)]*\)|(?:FOMC\s+)?Meetings?|Conference\s+Calls?|[-–,]\s*\d{4})*)"
_RE_HEADER = re.compile(
    r"^(?P<m1>" + _MON + r")\.?(?:/(?P<m2>" + _MON + r")\.?)?\s+" + _DAYS + _REST + r"$", re.I)
_RE_MONTH_ONLY = re.compile(r"^(?P<m1>" + _MON + r")\.?(?:/(?P<m2>" + _MON + r")\.?)?$", re.I)
_RE_DAYS_ONLY = re.compile(r"^" + _DAYS + _REST + r"$", re.I)
_RE_YEAR_HEADING = re.compile(r"^(\d{4})\s+FOMC\s+Meetings$", re.I)
_RE_MINUTES = re.compile(
    r"Minutes\b[^()]*?\(\s*Released\s+(" + _MON + r")\.?\s+(\d{1,2}),\s*(\d{4})\s*\)", re.I)
_RE_PROJ = re.compile(r"Projections?\s+Materials|Summary\s+of\s+Economic\s+Projections", re.I)


def _classify_rest(rest: str, line: str) -> tuple[str, bool, int | None] | None:
    """rest -> (kind, sep_marker, explicit_year) oder None (keine Sitzung)."""
    low = rest.lower()
    kind = "regular"
    for paren in re.findall(r"\(([^)]*)\)", rest):
        p = paren.strip().lower()
        if "unscheduled" in p:
            kind = "unscheduled"
        elif "notation" in p:
            kind = "notation_vote"
        else:
            raise ParseError(f"FOMC: unbekannte Markierung ({paren}) in {line!r}")
    if re.search(r"conference\s+call", low):
        kind = "conference_call"
    ym = re.search(r"[-–,]\s*(\d{4})", rest)
    year = int(ym[1]) if ym else None
    named = bool(re.search(r"meeting|conference\s+call", low))
    # "April 8, 2015" o.ae. ist ein Datum im Fliesstext, keine Sitzung
    if year is not None and not named:
        return None
    return kind, "*" in rest, year


def _meeting_from(m1: str, m2: str | None, d1: str, m2b: str | None, d2: str | None,
                  rest: str, year: int, line: str) -> Meeting | None:
    cls = _classify_rest(rest or "", line)
    if cls is None:
        return None
    kind, sep, explicit_year = cls
    if explicit_year is not None and explicit_year != year:
        raise ParseError(f"FOMC: Jahr {explicit_year} in {line!r} passt nicht zu {year}")
    mo1 = month_no(m1)
    mo2 = month_no(m2 or m2b) if (m2 or m2b) else mo1
    start = date(year, mo1, int(d1))
    y2 = year + 1 if mo2 < mo1 else year
    end = date(y2, mo2, int(d2)) if d2 else start
    if mo2 != mo1 and not d2:
        raise ParseError(f"FOMC: Monatswechsel ohne Endtag in {line!r}")
    if not (timedelta(0) <= end - start <= timedelta(days=3)):
        raise ParseError(f"FOMC: unplausible Sitzungsdauer in {line!r}")
    return Meeting(year=year, start=start, end=end, sep=sep, kind=kind, raw=line)


def parse_fomc_page(html: str, year: int | None = None) -> list[Meeting]:
    """
    Sitzungen aus fomccalendars.htm (year=None: Jahr aus "YYYY FOMC Meetings")
    oder fomchistorical{YYYY}.htm (year gesetzt). Robust auf Textmuster:
      "January 28-29", "Apr/May 30-1", "July 31-August 1 Meeting",
      Monat und Tage in getrennten Zeilen (aktuelles Layout),
      "*" = SEP-Sitzung, "(unscheduled)", "(notation vote)", "Conference Call".
    Je Sitzung: Minutes-Datum nur, wenn "Minutes ... (Released <Datum>)" im
    Block steht; SEP auch ueber "Projection(s) Materials" im Block.
    """
    lines = html_lines(html)
    cur_year = year
    meetings: list[Meeting] = []
    i = 0
    while i < len(lines):
        ln = lines[i]
        ym = _RE_YEAR_HEADING.match(ln)
        if ym and year is None:
            cur_year = int(ym[1])
            if meetings:
                meetings[-1].block.append("\x00")     # Blockende markieren
            i += 1
            continue
        mt = None
        if cur_year is not None:
            h = _RE_HEADER.match(ln)
            if h:
                mt = _meeting_from(h["m1"], h["m2"], h["d1"], h["m2b"], h["d2"],
                                   h["rest"], cur_year, ln)
            elif _RE_MONTH_ONLY.match(ln) and i + 1 < len(lines):
                d = _RE_DAYS_ONLY.match(lines[i + 1])
                if d:
                    mo = _RE_MONTH_ONLY.match(ln)
                    mt = _meeting_from(mo["m1"], mo["m2"], d["d1"], d["m2b"], d["d2"],
                                       d["rest"], cur_year, f"{ln} {lines[i + 1]}")
                    if mt:
                        i += 1
        if mt:
            meetings.append(mt)
        elif meetings and (not meetings[-1].block or meetings[-1].block[-1] != "\x00"):
            meetings[-1].block.append(ln)
        i += 1

    for mt in meetings:
        text = " ".join(x for x in mt.block if x != "\x00")
        mm = _RE_MINUTES.search(text)
        if mm:
            mt.minutes_released = date(int(mm[3]), month_no(mm[1]), int(mm[2]))
        if _RE_PROJ.search(text):
            mt.sep = True
    return meetings


def check_fomc_meetings(meetings: list[Meeting], years: list[int], cfg: dict) -> dict:
    """Plausibilitaet je Jahr; Fehler -> ParseError."""
    lo, hi = cfg["fomc"]["meetings_per_year"]
    per_year: dict[int, int] = {}
    for y in years:
        reg = [m for m in meetings if m.year == y and m.kind == "regular"]
        if not lo <= len(reg) <= hi:
            raise ParseError(f"FOMC {y}: {len(reg)} regulaere Sitzungen gefunden, "
                             f"erwartet {lo}-{hi} (Seitenstruktur geaendert?)")
        ends = [m.end for m in reg]
        if len(set(ends)) != len(ends):
            raise ParseError(f"FOMC {y}: doppelte Sitzungen")
        for m in reg:
            if m.end.weekday() >= 5:
                raise ParseError(f"FOMC {y}: Sitzungsende {m.end} am Wochenende ({m.raw!r})")
            if m.minutes_released is not None:
                lag = (m.minutes_released - m.end).days
                if not 1 <= lag <= 70 or m.minutes_released.weekday() >= 5:
                    raise ParseError(f"FOMC {y}: Minutes-Datum {m.minutes_released} "
                                     f"unplausibel fuer Sitzung {m.end}")
        n_sep = sum(m.sep for m in reg)
        if y >= 2008 and n_sep > cfg["fomc"]["max_sep_per_year"]:
            raise ParseError(f"FOMC {y}: {n_sep} SEP-Sitzungen (> {cfg['fomc']['max_sep_per_year']})")
        per_year[y] = len(reg)
    return per_year


def has_press_conf(m: Meeting, cfg: dict) -> bool:
    """
    [Annahme] Regel: ab 2019 bei jeder regulaeren Sitzung, davor nur bei
    SEP-Sitzungen ab April 2011. Nicht-regulaere Sitzungen: nie.
    """
    if m.kind != "regular":
        return False
    every = date.fromisoformat(cfg["fomc"]["press_conf_every_meeting_from"])
    sep_from = date.fromisoformat(cfg["fomc"]["press_conf_sep_from"])
    return m.end >= every or (m.sep and m.end >= sep_from)


def fomc_events(meetings: list[Meeting], cfg: dict, fetched_at: pd.Timestamp) -> pd.DataFrame:
    fc, rows = cfg["fomc"], []
    for m in meetings:
        # unscheduled / notation vote / conference call: Zeitpunkt nicht regelbar,
        # bekannte Faelle stehen in calendar_adhoc.csv
        if m.kind != "regular":
            continue
        rows.append(make_event(cfg, "FOMC_STATEMENT", m.end, fc["statement_time"], "US",
                               "fomc", fetched_at))
        if has_press_conf(m, cfg):
            rows.append(make_event(cfg, "FOMC_PRESS_CONF", m.end, fc["press_conf_time"], "US",
                                   "fomc", fetched_at))
        if m.minutes_released:
            rows.append(make_event(cfg, "FOMC_MINUTES", m.minutes_released, fc["minutes_time"],
                                   "US", "fomc", fetched_at))
    return to_frame(rows)


def fetch_fomc(cfg: dict, start_year: int, fetched_at: pd.Timestamp, ctx: dict,
               data_root: Path) -> pd.DataFrame:
    fc = cfg["fomc"]
    cur = parse_fomc_page(http_get(fc["current_url"]).text)
    cur_years = sorted({m.year for m in cur})
    if not cur_years:
        raise ParseError("FOMC: keine Sitzungen auf fomccalendars.htm gefunden")
    meetings = [m for m in cur if m.year >= start_year]
    years = [y for y in cur_years if y >= start_year]
    hist_years = list(range(start_year, cur_years[0]))
    cache = data_root / "raw" / "fomc"
    for y in hist_years:
        p = cache / f"fomchistorical{y}.htm"
        if p.exists():
            html = p.read_text(encoding="utf-8")
        else:
            html = http_get(fc["historical_url"].format(year=y)).text
        ms = parse_fomc_page(html, year=y)
        check_fomc_meetings(ms, [y], cfg)       # erst pruefen, dann cachen
        if not p.exists():
            p.parent.mkdir(parents=True, exist_ok=True)
            tmp = p.with_name(p.name + ".part")
            tmp.write_text(html, encoding="utf-8")
            os.replace(tmp, p)
        meetings += ms
        years.append(y)
    per_year = check_fomc_meetings(meetings, sorted(years), cfg)
    skipped = [{"date": m.end.isoformat(), "kind": m.kind, "raw": m.raw}
               for m in meetings if m.kind != "regular"]
    ctx["meta"] = {"meetings_per_year": per_year, "current_page_years": cur_years,
                   "historical_years": hist_years, "skipped_non_regular": skipped}
    log.info("fomc %d regulaere Sitzungen %s, %d nicht-regulaere uebersprungen",
             sum(per_year.values()), f"{min(years)}-{max(years)}", len(skipped))
    return fomc_events(meetings, cfg, fetched_at)


# -------------------------------------------------------------------- EZB ---
_RE_ECB_DATE = re.compile(
    r"^(?:(?P<d>\d{1,2})/(?P<m>\d{1,2})/(?P<y>\d{4})|(?P<d2>\d{1,2})\s+(?P<mn>" + _MON +
    r")\.?\s+(?P<y2>\d{4}))\b\s*[:\-–]?\s*(?P<rest>.*)$", re.I)
_RE_ECB_MOPO = re.compile(r"(?<!non-)(?<!non )monetary\s+policy\s+meeting", re.I)
_RE_ECB_DAY1 = re.compile(r"\bday\s*(?:1|one)\b", re.I)
_ECB_DESC_LINES = 3


def parse_ecb_calendar(html: str) -> list[date]:
    """
    EZB-Ratskalender -> Tage mit geldpolitischer ENTSCHEIDUNG. Eintraege:
    Datum (dd/mm/yyyy oder "24 January 2024") + Beschreibung (gleiche oder
    folgende Zeile). Gezaehlt wird "monetary policy meeting" (nicht
    "non-monetary"); bei zweitaegigen Sitzungen nur der Tag ohne "Day 1".
    """
    lines = html_lines(html)
    entries: list[tuple[date, list[str]]] = []
    for ln in lines:
        m = _RE_ECB_DATE.match(ln)
        if m:
            try:
                if m["d"]:
                    d = date(int(m["y"]), int(m["m"]), int(m["d"]))
                else:
                    d = date(int(m["y2"]), month_no(m["mn"]), int(m["d2"]))
            except ValueError as e:
                raise ParseError(f"EZB: ungueltiges Datum in {ln!r} ({e})")
            entries.append((d, [m["rest"]] if m["rest"] else []))
        elif entries and len(entries[-1][1]) < _ECB_DESC_LINES:
            entries[-1][1].append(ln)
    out = set()
    for d, desc in entries:
        text = " ".join(desc)
        if _RE_ECB_MOPO.search(text) and not _RE_ECB_DAY1.search(text):
            if d.weekday() >= 5:
                raise ParseError(f"EZB: geldpolitische Sitzung {d} am Wochenende")
            out.add(d)
    return sorted(out)


def check_ecb_dates(dates: list[date], cfg: dict, today: date) -> dict:
    lo, hi = cfg["ecb"]["meetings_per_year"]
    per_year: dict[int, int] = {}
    for d in dates:
        per_year[d.year] = per_year.get(d.year, 0) + 1
    if not per_year:
        raise ParseError("EZB: keine geldpolitischen Sitzungen gefunden (Seitenstruktur geaendert?)")
    for y, n in sorted(per_year.items()):
        if n > hi or (n < lo and y <= today.year):
            raise ParseError(f"EZB {y}: {n} geldpolitische Sitzungen, erwartet {lo}-{hi}")
    for a, b in zip(dates, dates[1:]):
        if (b - a).days < 7:
            raise ParseError(f"EZB: Entscheidungen {a} und {b} zu dicht (Day 1/2 falsch erkannt?)")
    return per_year


def ecb_times(d: date, cfg: dict) -> tuple[str, str]:
    """(Entscheidung, Pressekonferenz) Europe/Berlin fuer das Datum."""
    rules = sorted(cfg["ecb"]["time_rules"], key=lambda r: r["from"])
    cur = None
    for r in rules:
        if d >= date.fromisoformat(str(r["from"])):
            cur = r
    if cur is None:
        raise ConfigError(f"EZB: keine Uhrzeit-Regel fuer {d}")
    return cur["decision"], cur["press_conf"]


def ecb_events(dates: list[date], cfg: dict, fetched_at: pd.Timestamp) -> pd.DataFrame:
    rows = []
    for d in dates:
        dec, pc = ecb_times(d, cfg)
        rows.append(make_event(cfg, "ECB_RATE_DECISION", d, dec, "EA", "ecb", fetched_at))
        rows.append(make_event(cfg, "ECB_PRESS_CONF", d, pc, "EA", "ecb", fetched_at))
    return to_frame(rows)


def fetch_ecb(cfg: dict, start_year: int, fetched_at: pd.Timestamp, ctx: dict,
              today: date) -> pd.DataFrame:
    dates = parse_ecb_calendar(http_get(cfg["ecb"]["url"]).text)
    per_year = check_ecb_dates(dates, cfg, today)
    dates = [d for d in dates if d.year >= start_year]
    ctx["meta"] = {"decisions_per_year": per_year}
    log.info("ecb  %d Entscheidungstermine %s", len(dates), per_year)
    return ecb_events(dates, cfg, fetched_at)


# ------------------------------------------------------------------ adhoc ---
def parse_adhoc_csv(text: str, cfg: dict, fetched_at: pd.Timestamp) -> pd.DataFrame:
    body = "\n".join(ln for ln in text.splitlines() if ln.strip() and not ln.lstrip().startswith("#"))
    rows = []
    need = {"date_local", "time_local", "tz", "event_type", "country", "impact"}
    reader = csv.DictReader(io.StringIO(body))
    if not reader.fieldnames or not need <= set(reader.fieldnames):
        raise ParseError(f"adhoc: Spalten {sorted(need)} erwartet, gefunden {reader.fieldnames}")
    for n, r in enumerate(reader, start=1):
        try:
            d = date.fromisoformat(r["date_local"].strip())
        except ValueError as e:
            raise ParseError(f"adhoc Zeile {n}: {e}")
        country, impact = r["country"].strip(), r["impact"].strip()
        if country not in COUNTRIES or impact not in IMPACTS:
            raise ParseError(f"adhoc Zeile {n}: country/impact ungueltig ({country}, {impact})")
        tz = r["tz"].strip()
        if tz != cfg["timezones"][country]:
            raise ParseError(f"adhoc Zeile {n}: tz {tz} passt nicht zu {country}")
        try:
            rows.append(make_event(cfg, r["event_type"].strip(), d, r["time_local"].strip(),
                                   country, "adhoc", fetched_at, status="adhoc", impact=impact))
        except ConfigError as e:
            raise ParseError(f"adhoc Zeile {n}: {e}")
    df = to_frame(rows)
    if df["event_id"].duplicated().any():
        raise ParseError("adhoc: doppelte event_id")
    return df


def fetch_adhoc(cfg: dict, start_year: int, fetched_at: pd.Timestamp, ctx: dict) -> pd.DataFrame:
    p = Path(cfg["adhoc"]["file"])
    p = p if p.is_absolute() else ROOT / p
    df = parse_adhoc_csv(p.read_text(encoding="utf-8"), cfg, fetched_at)
    df = df[df["ts_utc"].dt.year >= start_year].reset_index(drop=True)
    ctx["meta"] = {"file": str(p), "events": len(df)}
    return df


# --------------------------------------------------------------- ff check ---
def parse_ff(payload, cfg: dict) -> tuple[list[dict], int]:
    """
    ff_calendar_thisweek.json -> [{event_type, country, ts_utc, title}] fuer
    bekannte Titel; zweiter Wert = Anzahl ignorierter Eintraege.
    """
    if not isinstance(payload, list):
        raise ParseError("ff: Liste erwartet")
    cmap = {"USD": "US", "EUR": "EA"}
    pats = {c: [(re.compile(p, re.I), et) for p, et in (m or {}).items()]
            for c, m in cfg["ff"]["titles"].items()}
    out, ignored = [], 0
    for it in payload:
        if not isinstance(it, dict):
            ignored += 1
            continue
        cur, title = str(it.get("country", "")), str(it.get("title", "")).strip()
        et = next((e for rx, e in pats.get(cur, []) if rx.search(title)), None)
        if et is None:
            ignored += 1
            continue
        try:
            ts = pd.Timestamp(datetime.fromisoformat(str(it["date"])))
        except (KeyError, ValueError) as e:
            raise ParseError(f"ff: ungueltiges Datum in {it!r} ({e})")
        if ts.tzinfo is None:
            raise ParseError(f"ff: Datum ohne Offset in {it!r}")
        out.append({"event_type": et, "country": cmap[cur], "title": title,
                    "ts_utc": ts.tz_convert("UTC"), "impact": it.get("impact")})
    return out, ignored


def crosscheck_ff(ff_events: list[dict], table: pd.DataFrame, cfg: dict) -> dict:
    """Vergleich mit unserer Tabelle. Ergebnis NUR fuer den Report."""
    tol = float(cfg["ff"]["tolerance_minutes"])
    by_id = {r.event_id: r for r in table.itertuples(index=False)}
    seen, deviations, missing, ok = set(), [], [], 0
    for e in ff_events:
        local = e["ts_utc"].tz_convert(cfg["timezones"][e["country"]]).date()
        eid = f"{e['event_type']}:{local.isoformat()}"
        if eid in seen:
            continue                    # z.B. "CPI m/m" + weitere Titel desselben Termins
        seen.add(eid)
        ours = by_id.get(eid)
        if ours is None or ours.status == "cancelled":
            missing.append({"event_id": eid, "ff_title": e["title"], "ff_ts_utc": e["ts_utc"].isoformat()})
            continue
        diff = abs((e["ts_utc"] - ours.ts_utc).total_seconds()) / 60
        if diff > tol:
            deviations.append({"event_id": eid, "ours_ts_utc": ours.ts_utc.isoformat(),
                               "ff_ts_utc": e["ts_utc"].isoformat(), "diff_minutes": diff,
                               "ff_title": e["title"]})
        else:
            ok += 1
    return {"checked": len(seen), "ok": ok, "deviations": deviations, "missing_in_ours": missing}


def fetch_ff(cfg: dict) -> list[dict]:
    r = http_get(cfg["ff"]["url"], retries=2)
    try:
        payload = r.json()
    except ValueError as e:
        raise ParseError(f"ff: kein JSON ({e})")
    events, ignored = parse_ff(payload, cfg)
    log.info("ff   %d bekannte Eintraege, %d ignoriert", len(events), ignored)
    return events


# ------------------------------------------------------------------ merge ---
def _local_date(event_id: str) -> date:
    return date.fromisoformat(event_id.rsplit(":", 1)[1])


def combine_new(frames: list[pd.DataFrame]) -> pd.DataFrame:
    """Frames in Abrufreihenfolge; gleiche event_id -> zuletzt abgerufene Quelle gewinnt."""
    frames = [f for f in frames if len(f)]
    if not frames:
        return to_frame([])
    df = pd.concat(frames, ignore_index=True)
    df = df.sort_values("fetched_at", kind="mergesort")
    return df.drop_duplicates("event_id", keep="last").reset_index(drop=True)


def merge_with_previous(prev: pd.DataFrame | None, new: pd.DataFrame,
                        now: pd.Timestamp) -> tuple[pd.DataFrame, dict]:
    """
    Neuer Stand = alter Stand, ueberschrieben durch neu abgerufene Events.
      - gleiche event_id, ts_utc anders            -> rescheduled
      - gleiche event_id, ts_utc gleich, alt rescheduled -> bleibt rescheduled
      - adhoc bleibt adhoc
      - alte Zeile (source, event_type) innerhalb des neu abgerufenen
        Datumsbereichs dieser Kombination, aber nicht mehr geliefert -> cancelled
      - alles ausserhalb bleibt unveraendert (z.B. EZB-Termine, die von der
        Seite gefallen sind, oder Jahre vor --start-year)
    """
    changes = {"added": [], "rescheduled": [], "cancelled": []}
    new = new.copy()
    if prev is None or prev.empty:
        changes["added"] = sorted(new["event_id"])
        return new.sort_values(["ts_utc", "event_id"]).reset_index(drop=True), changes

    prev_by_id = prev.set_index("event_id")
    for i, r in new.iterrows():
        if r.event_id not in prev_by_id.index:
            changes["added"].append(r.event_id)
            continue
        old = prev_by_id.loc[r.event_id]
        if r.status == "adhoc":
            continue
        if old.ts_utc != r.ts_utc:
            new.at[i, "status"] = "rescheduled"
            changes["rescheduled"].append({"event_id": r.event_id, "old_ts_utc": old.ts_utc.isoformat(),
                                           "new_ts_utc": r.ts_utc.isoformat(), "source": r.source})
        elif old.status == "rescheduled":
            new.at[i, "status"] = "rescheduled"

    cover: dict[tuple[str, str], tuple[date, date]] = {}
    if len(new):
        tmp = new.assign(_d=new["event_id"].map(_local_date))
        for (src, et), g in tmp.groupby(["source", "event_type"]):
            cover[(src, et)] = (g["_d"].min(), g["_d"].max())

    new_ids = set(new["event_id"])
    keep = prev[~prev["event_id"].isin(new_ids)].copy()
    for i, r in keep.iterrows():
        rng = cover.get((r.source, r.event_type))
        if rng and rng[0] <= _local_date(r.event_id) <= rng[1] and r.status != "cancelled":
            keep.at[i, "status"] = "cancelled"
            keep.at[i, "fetched_at"] = now
            changes["cancelled"].append({"event_id": r.event_id, "impact": r.impact,
                                         "ts_utc": r.ts_utc.isoformat(), "source": r.source})
    out = pd.concat([keep, new], ignore_index=True) if len(keep) else new
    out = to_frame(out.to_dict("records"))
    return out.sort_values(["ts_utc", "event_id"]).reset_index(drop=True), changes


# ------------------------------------------------------------- validation ---
def validate(df: pd.DataFrame, cfg: dict, start_year: int, today: date,
             prev: pd.DataFrame | None = None, changes: dict | None = None) -> dict:
    """Harte Checks werfen ValidationError. Rueckgabe: Kennzahlen fuer den Report."""
    if list(df.columns) != COLUMNS:
        raise ValidationError(f"Spalten {list(df.columns)} != {COLUMNS}")
    if df.empty:
        raise ValidationError("Tabelle leer")
    for c in ("ts_utc", "fetched_at"):
        if str(df[c].dtype) != TS_DTYPE:
            raise ValidationError(f"{c}: dtype {df[c].dtype}, erwartet {TS_DTYPE}")
        if df[c].isna().any():
            raise ValidationError(f"{c}: {int(df[c].isna().sum())} NaT")
    for c in ("event_id", "event_type", "country", "impact", "source", "status"):
        bad = ~df[c].map(lambda v: isinstance(v, str) and v != "")
        if bad.any():
            raise ValidationError(f"{c}: {int(bad.sum())} leere/nicht-str Werte")
    if df["event_id"].duplicated().any():
        raise ValidationError(f"doppelte event_id: {sorted(df.loc[df['event_id'].duplicated(), 'event_id'])[:5]}")
    for c, allowed in (("country", COUNTRIES), ("impact", IMPACTS), ("status", STATUSES)):
        bad = sorted(set(df[c]) - allowed)
        if bad:
            raise ValidationError(f"{c}: unzulaessige Werte {bad}")

    lo_h, hi_h = cfg["validation"]["plausible_local_hours"]
    imp = cfg["_impact_of"]
    for r in df.itertuples(index=False):
        local = r.ts_utc.tz_convert(cfg["timezones"][r.country])
        if r.event_id != f"{r.event_type}:{local.date().isoformat()}":
            raise ValidationError(f"{r.event_id}: event_id passt nicht zu ts_utc ({local})")
        if not 0 <= local.hour <= 23:
            raise ValidationError(f"{r.event_id}: Stunde {local.hour}")
        if r.status != "adhoc" and r.source != "adhoc":
            if not lo_h <= local.hour <= hi_h:
                raise ValidationError(f"{r.event_id}: Lokalzeit {local:%H:%M} ausserhalb "
                                      f"{lo_h}-{hi_h} Uhr")
            if imp.get(r.event_type) != r.impact:
                raise ValidationError(f"{r.event_id}: impact {r.impact} != Konfiguration "
                                      f"{imp.get(r.event_type)}")

    active = df[df["status"] != "cancelled"]
    high = active[active["impact"] == "high"]
    per_year = {int(y): int(n) for y, n in high.groupby(high["ts_utc"].dt.year).size().items()}
    need = int(cfg["validation"]["min_high_per_year"])
    first = max(start_year, int(cfg["validation"]["min_high_from_year"]))
    short = {y: per_year.get(y, 0) for y in range(first, today.year) if per_year.get(y, 0) < need}
    if short:
        raise ValidationError(
            f"zu wenige High-Impact-Events (< {need}) in vollstaendigen Jahren: {short} - "
            f"Quelle unvollstaendig? Nichts geschrieben")

    if prev is not None and len(prev):
        lost = set(prev.loc[prev["impact"] == "high", "event_id"]) - set(df["event_id"])
        if lost:
            raise ValidationError(f"High-Impact-Events wuerden verloren gehen: {sorted(lost)[:10]}")
    if changes:
        n_canc = sum(1 for c in changes["cancelled"] if c["impact"] == "high")
        mx = int(cfg["validation"]["max_cancelled_high_per_run"])
        if n_canc > mx:
            raise ValidationError(f"{n_canc} High-Impact-Events in einem Lauf gestrichen "
                                  f"(> {mx}) - Quelle/Parser pruefen, nichts geschrieben")
    return {"rows": len(df), "high_per_year": per_year,
            "by_status": {k: int(v) for k, v in df["status"].value_counts().items()},
            "by_type": {k: int(v) for k, v in active["event_type"].value_counts().items()}}


# ------------------------------------------------------------------ io -----
def read_events(path: Path) -> pd.DataFrame | None:
    if not path.exists():
        return None
    df = pd.read_parquet(path)
    return to_frame(df[COLUMNS].to_dict("records"))


def write_parquet_atomic(df: pd.DataFrame, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.name + ".part")
    df.to_parquet(tmp, index=False, compression="zstd")
    os.replace(tmp, dest)


def write_json_atomic(obj: dict, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.name + ".part")
    tmp.write_text(json.dumps(obj, indent=2, ensure_ascii=False, default=str) + "\n")
    os.replace(tmp, dest)


# ----------------------------------------------------------------- driver ---
def _call_fetcher(name: str, cfg: dict, start_year: int, fetched_at: pd.Timestamp,
                  ctx: dict, data_root: Path, today: date) -> pd.DataFrame:
    if name == "fred":
        return FETCHERS["fred"](cfg, start_year, fetched_at, ctx)
    if name == "fomc":
        return FETCHERS["fomc"](cfg, start_year, fetched_at, ctx, data_root)
    if name == "ecb":
        return FETCHERS["ecb"](cfg, start_year, fetched_at, ctx, today)
    return FETCHERS[name](cfg, start_year, fetched_at, ctx)


FETCHERS = {"fred": fetch_fred, "fomc": fetch_fomc, "ecb": fetch_ecb, "adhoc": fetch_adhoc}


def run(sources: list[str], start_year: int = 2003, data_root: Path | None = None,
        dry_run: bool = False, now: datetime | None = None,
        cfg: dict | None = None) -> tuple[int, dict]:
    """Rueckgabe (exit_code, report). Schreibt nur, wenn alles gruen ist."""
    data_root = data_root or default_data_root()
    cfg = cfg or load_config()
    now_ts = pd.Timestamp(now or datetime.now(timezone.utc)).tz_convert("UTC").as_unit("ns")
    today = now_ts.date()
    unknown = sorted(set(sources) - set(SOURCES))
    if unknown:
        raise ConfigError(f"unbekannte Quellen {unknown}; bekannt: {list(SOURCES)}")

    report: dict = {"run_at": now_ts.isoformat(), "start_year": start_year,
                    "sources_requested": list(sources), "dry_run": dry_run,
                    "sources": {}, "ff": None, "written": None}
    rp = calendar_dir(data_root) / "reports" / f"calendar-{stamp(now_ts)}.json"

    def finish(code: int) -> tuple[int, dict]:
        report["exit_code"] = code
        if not dry_run:
            write_json_atomic(report, rp)
            report["report_file"] = str(rp)
        return code, report

    frames, failed, config_failed = [], [], False
    for name in [s for s in SOURCES if s in sources and s != "ff"]:
        # je Quelle eigene Abrufzeit -> "neueste Quelle gewinnt" ist eindeutig
        fetched_at = pd.Timestamp(now_ts) + pd.Timedelta(microseconds=len(frames) + len(failed))
        ctx: dict = {}
        try:
            df = _call_fetcher(name, cfg, start_year, fetched_at, ctx, data_root, today)
        except (DownloadError, ParseError, ConfigError, OSError, ValueError, KeyError) as e:
            config_failed |= isinstance(e, ConfigError)
            failed.append(name)
            report["sources"][name] = {"status": "error", "error": f"{type(e).__name__}: {e}"}
            log.error("Quelle %s FEHLGESCHLAGEN: %s: %s", name, type(e).__name__, e)
            continue
        frames.append(df)
        report["sources"][name] = {"status": "ok", "events": len(df), **ctx}

    prev = read_events(events_path(data_root))
    if failed:
        log.error("Quellen fehlgeschlagen: %s - nichts geschrieben, alter Stand bleibt", failed)
        return finish(4 if config_failed else 3)

    new = combine_new(frames)
    table, changes = merge_with_previous(prev, new, now_ts)
    report["changes"] = changes
    try:
        report["validation"] = validate(table, cfg, start_year, today, prev, changes)
    except ValidationError as e:
        report["validation_error"] = str(e)
        log.error("VALIDIERUNG FEHLGESCHLAGEN: %s", e)
        return finish(2)

    if "ff" in sources:
        try:
            report["ff"] = {"status": "ok", **crosscheck_ff(fetch_ff(cfg), table, cfg)}
            if report["ff"]["deviations"]:
                log.warning("ff-Kreuzcheck: %d Abweichungen > %s min (nur Report)",
                            len(report["ff"]["deviations"]), cfg["ff"]["tolerance_minutes"])
        except Exception as e:          # Kreuzcheck ist nie fatal
            report["ff"] = {"status": "error", "error": f"{type(e).__name__}: {e}"}
            log.warning("ff-Kreuzcheck fehlgeschlagen (nicht fatal): %s", e)

    if not dry_run:
        snap = calendar_dir(data_root) / "snapshots" / f"events-{stamp(now_ts)}.parquet"
        write_parquet_atomic(table, snap)
        write_parquet_atomic(table, events_path(data_root))
        report["written"] = {"events": str(events_path(data_root)), "snapshot": str(snap)}
    log.info("FERTIG: %d Events (neu %d, verschoben %d, gestrichen %d)%s", len(table),
             len(changes["added"]), len(changes["rescheduled"]), len(changes["cancelled"]),
             " [dry-run, nichts geschrieben]" if dry_run else "")
    return finish(0)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--start-year", type=int, default=2003)
    p.add_argument("--sources", default=",".join(SOURCES),
                   help=f"kommagetrennt aus {','.join(SOURCES)} (default: alle)")
    p.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    p.add_argument("--dry-run", action="store_true", help="abrufen + validieren, nichts schreiben")
    p.add_argument("-v", "--verbose", action="store_true")
    a = p.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if a.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
    )
    # urllib3 loggt auf DEBUG volle URLs inkl. api_key -> stumm halten
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    sources = [s.strip() for s in a.sources.split(",") if s.strip()]
    try:
        code, report = run(sources, a.start_year, dry_run=a.dry_run, cfg=load_config(a.config))
    except ConfigError as e:
        log.error("KONFIGURATION: %s", e)
        return 4
    if a.dry_run:
        print(json.dumps(report, indent=2, ensure_ascii=False, default=str))
    return code


if __name__ == "__main__":
    sys.exit(main())
