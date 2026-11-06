#!/usr/bin/env python3
"""
Zeitzonen-Survey fuer HistData-Ticks: welche Stempelregel galt wann, und wo
liegt der Wechsel? Kandidaten "ny_local" (America/New_York-Ortszeit) und
"berlin_minus_6" (Europe/Berlin-Ortszeit minus 6 h), dazu fixed_est und
eigene Regeln aus config/histdata_timezone.yaml.

Ein Befehl:
1. laedt NUR die Rohmonate, die fuer den Survey fehlen (Roh-Cache von
   fetch_histdata.py, gleicher Client: ein Request zur Zeit, Pause, 429-Sperre
   ueber die gemeinsame Zustandsdatei; je Rohmonat 2 Requests). Vor dem
   ersten Download stehen Plan, Request-Zahl und Rest (naechster Aufruf) auf
   der Konsole. --max-downloads begrenzt je Aufruf; Exit 3, bis alles da ist
   (der Cache bleibt, ein erneuter Aufruf setzt fort).
2. prueft jeden Rohmonat mit jeder Kandidatenregel ueber
   fh.evaluate_raw_month(..., drop_releasable_weekend=True) (nur Cache +
   Nachbar-Kontext, schreibt nichts). Freigebbare Wochenend-Ticks
   (geschlossener Markt, z.B. roh Sa 20:00-20:59 in der EU-Umstellungsnacht
   unter berlin_minus_6) werden dabei verworfen, nie umgerechnet; die Tabelle
   nennt sie ("ok (3 Wochenend-Ticks verworfen)"), grosse Ruecksprunge in der
   Dateireihenfolge stehen mit Zeit und Tick-Zahl darunter.
3. wertet je Umstellungsperiode aus und schlaegt einen Fahrplan vor
   (Abschnitt `schedule` in config/histdata_timezone.yaml; die Config selbst
   wird NIE geschrieben).

Umstellungsperioden: je Jahr "Maerz" (Rohmonat 03) und "Okt/Nov" (Rohmonate
10 + 11). Nur dort unterscheiden sich ny_local und berlin_minus_6: Maerz vom
US-Beginn (2. So Maerz) bis zum EU-Beginn (letzter So Maerz), Okt/Nov vom
EU-Ende (letzter So Oktober) bis zum US-Ende (1. So November). In allen
anderen Monaten stempeln beide gleich.
- Rohmonat entscheidend: GENAU EINE Regel besteht, alle anderen fallen durch.
- Urteil einer Periode je Symbol: die eine Regel, die in ihren Monaten
  entscheidend besteht. Nicht entscheidende Monate (alle/keine bestehen,
  nicht pruefbar, fehlt) widersprechen nicht. Zwei Monate mit verschiedenen
  Gewinnern = Konflikt.
- Urteil der Periode ueber XAUUSD/XAGUSD: wo beide entscheiden, muessen sie
  uebereinstimmen, sonst Konflikt. Bestaetigt = fuer mindestens ein Symbol
  entscheidend und ohne Widerspruch.
- Wechselanalyse ueber die bestaetigten Perioden (chronologisch): Zahl der
  Wechsel, Konflikte, letzte bestaetigte Periode der alten und erste der
  neuen Regel, offene Perioden dazwischen. Verlangt: hoechstens EIN Wechsel
  und kein Konflikt; sonst Exit 4 mit Erklaerung und ohne Vorschlag.
- Vorschlag (YAML mit Evidenztabelle und Begruendung als Kommentar):
    {from: <Beginn des Fahrplans>, rule: <alt>}
    {from: <Monat nach der letzten alten Periode>, rule: undecided,
     candidates: [<alt>, <neu>]}           (nur bei offenen Perioden dazwischen)
    {from: <erster Monat der ersten neuen Periode>, rule: <neu>}
  Begruendung: Rohmonate ohne Umstellungsfenster sind unter beiden Regeln
  identisch (stuendlich nachgerechnet), jede Grenze zwischen den beiden
  Perioden ist daher gleichwertig. Ein Block (gleiche Regel) braucht
  mindestens 2 entscheidende Monate ODER beide Symbole im selben Monat, sonst
  "zu wenig Evidenz" (JSON "weak_evidence") und kein Vorschlag.
  --markdown PATH schreibt dasselbe als Markdown (fuer die Doku).

Monate: Default Jahre 2010 2012 2015 2018 2021 2024, Umstellungsmonate 3 4 10
11, Kontrollmonate 1 7 (dort haben alle Sommerzeit-Regeln dieselben Offsets;
sie muessen alle bestehen, sonst Warnung "Kontrolle gescheitert").
--preset full: alle Jahre ab dem fruehesten Historienbeginn (fh.HISTORY_START)
bis 2025, Monate 3 10 11, keine Kontrollmonate. --periods YYYY-MM ...:
einzelne Rohmonate, kombinierbar mit --preset/--years (allein: nur diese).

Nur Tick-Modus, nur XAUUSD und XAGUSD.

Exit-Codes:
  0  sonst (alles ausgewertet, konfigurierte Regel faellt nirgends durch,
     hoechstens ein Wechsel, kein Konflikt)
  4  die konfigurierte Regel (fh.rule_for) hat in einem Monat Status
     "failed" (Liste "KONFIGURIERTE REGEL FAELLT DURCH", JSON
     "configured_failed"; entscheidend anders = zusaetzlich "WIDERSPRUCH",
     JSON "contradictions"), ODER mehr als ein Wechsel, ODER ein Konflikt.
     Monate, die der Fahrplan als "undecided" fuehrt, stehen als "offen" da
     und zaehlen nie als Fehler.
  3  unvollstaendig: Download gescheitert, 429 (Sperre aktiv oder neu),
     --max-downloads erreicht, Symbol-Lock belegt, im --offline-Modus nicht
     im Cache. Teilergebnisse werden trotzdem ausgewertet und gedruckt.
  4 geht vor 3: ein Widerspruch ist ein Befund, auch wenn noch Monate fehlen.

Usage:
    python histdata_tz_survey.py
    python histdata_tz_survey.py --offline
    python histdata_tz_survey.py --preset full --periods 2019-03 2019-10 \\
        --json /tmp/tz.json --markdown /tmp/tz.md
    python histdata_tz_survey.py --symbols XAUUSD --years 2013 2014 --months 3 10 11
"""
from __future__ import annotations

import argparse
import logging
import re
import sys
import textwrap
import threading
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np

import fetch_dukascopy as dk
import fetch_histdata as fh
import ratelimit as rl

UTC = timezone.utc
MODE = "tick"
SYMBOLS = ("XAUUSD", "XAGUSD")
DEFAULT_YEARS = (2010, 2012, 2015, 2018, 2021, 2024)
DEFAULT_MONTHS = (3, 4, 10, 11)      # Umstellungsmonate EU/US
DEFAULT_CONTROLS = (1, 7)            # Kontrollmonate (alle Sommerzeit-Regeln gleich)
PRESET_LAST_YEAR = 2025
PRESET_MONTHS = (3, 10, 11)          # --preset full: nur die Monate der Umstellungsperioden
PRESETS = ("full",)
MAX_DOWNLOADS = 80                   # Rohmonate je Aufruf (Default-Plan: 72)
PAUSE_S = 5.0
REQUESTS_PER_FILE = 2                # Seiten-GET + POST
CONFIG_PATH = "config/histdata_timezone.yaml"
UNDECIDED = "undecided"              # Fahrplan-Eintrag ohne belegte Regel (fh.UndecidedRule)
# Umstellungsperioden je Jahr: Index 0 = Maerz (Rohmonat 03), 1 = Okt/Nov (10 + 11)
PERIOD_MONTHS = ((3,), (10, 11))
PERIOD_NAMES = ("Maerz", "Okt/Nov")
MIN_BLOCK_MONTHS = 2   # so viele entscheidende Monate je Block, sonst beide Symbole im selben Monat

EXIT_OK, EXIT_INCOMPLETE, EXIT_CONTRADICTION = 0, 3, 4

log = logging.getLogger("histdata_tz_survey")


# ----------------------------------------------------------------- helfer ---
def ymstr(ym: tuple[int, int]) -> str:
    return f"{ym[0]}-{ym[1]:02d}"


def parse_ym(s: str) -> tuple[int, int]:
    """"YYYY-MM" -> (y, m); sonst ValueError."""
    m = re.fullmatch(r"(\d{4})-(\d{2})", s.strip())
    if not m or not 1 <= int(m.group(2)) <= 12:
        raise ValueError(f"Rohmonat als YYYY-MM angeben, nicht {s!r}")
    return int(m.group(1)), int(m.group(2))


def ym_add(ym: tuple[int, int], k: int) -> tuple[int, int]:
    n = ym[0] * 12 + ym[1] - 1 + k
    return n // 12, n % 12 + 1


def rule_name(rule) -> str | None:
    """fh.rule_for kann eine TzRule, eine UndecidedRule oder einen Namen liefern."""
    if rule is None:
        return None
    return getattr(rule, "name", rule)


def rule_candidates(rule) -> list[str]:
    """Kandidatennamen einer "undecided"-Regel (sonst leer)."""
    return [rule_name(c) for c in getattr(rule, "candidates", None) or ()]


def load_config():
    """Fahrplan laden; kaputte Config -> None (kein Abbruch: der Survey hilft ja
    beim Reparieren, Widersprueche koennen dann aber nicht geprueft werden)."""
    try:
        return fh.load_tz_config()
    except ValueError as e:
        log.error("Zeitzonen-Konfiguration ungueltig: %s - Abgleich mit dem Fahrplan entfaellt", e)
        return None


def configured(ym: tuple[int, int], cfg) -> tuple[str | None, list[str]]:
    """(Regelname laut Fahrplan, Kandidaten bei "undecided")."""
    if cfg is None:
        return None, []
    try:
        rule = fh.rule_for(MODE, ym, cfg)
    except ValueError as e:                  # Fahrplan beginnt spaeter
        log.warning("kein konfigurierter Fahrplan fuer %s: %s", ymstr(ym), e)
        return None, []
    return rule_name(rule), rule_candidates(rule)


def rule_offset_h(rule, when: datetime) -> float:
    """Offset der Rohzeit gegen UTC in h (Zone der Regel + Verschiebung)."""
    off = ZoneInfo(rule.zone).utcoffset(when.replace(tzinfo=None))
    return off.total_seconds() / 3600 + float(rule.shift_h)


def dst_rules(rules: dict, year: int) -> list[str]:
    """Regeln mit Sommerzeit (Offset Januar != Juli). In Januar und Juli haben
    sie dieselben Offsets (berlin_minus_6 = ny_local) - Kontrollmonate."""
    jan, jul = datetime(year, 1, 15, 12), datetime(year, 7, 15, 12)
    return [n for n, r in rules.items() if rule_offset_h(r, jan) != rule_offset_h(r, jul)]


def plan_months(years, months, controls, periods=()) -> list[tuple[tuple[int, int], str]]:
    """[(ym, "transition"|"control")] chronologisch; Kontrolle geht vor.
    periods: einzelne Rohmonate (YYYY-MM) zusaetzlich, als Umstellungsmonat."""
    kinds: dict[tuple[int, int], str] = {}
    for ym in periods:
        kinds[tuple(ym)] = "transition"
    for y in years:
        for m in months:
            kinds[(y, m)] = "transition"
        for m in controls:
            kinds[(y, m)] = "control"
    return sorted(kinds.items())


def preset_plan(name: str) -> tuple[list[int], list[int], list[int]]:
    """(Jahre, Monate, Kontrollmonate) eines Presets. full: alle Jahre ab dem
    fruehesten Historienbeginn bis PRESET_LAST_YEAR, Maerz/Oktober/November."""
    if name != "full":
        raise ValueError(f"unbekanntes Preset {name!r}; bekannt: {', '.join(PRESETS)}")
    first = min(ym[0] for ym in fh.HISTORY_START.values())
    return list(range(first, PRESET_LAST_YEAR + 1)), list(PRESET_MONTHS), []


def month_closed(sym: str, ym: tuple[int, int], now: datetime) -> bool:
    try:
        end = fh.Unit(sym, MODE, *ym).end_utc
    except ValueError:                       # kein Fahrplan: Rohzeit liegt hoechstens 14 h vor UTC
        end = datetime(*ym_add(ym, 1), 1, tzinfo=UTC) + timedelta(hours=14)
    return now >= end + timedelta(seconds=fh.SETTLE_S)


def cache_state(data_root: Path, sym: str, ym: tuple[int, int], now: datetime) -> str:
    """cached | empty (bestaetigt keine Daten) | deferred (Inhaltsfehler < 24 h) | missing"""
    u = fh.Unit(sym, MODE, *ym)
    if u.path(data_root).exists():
        return "cached"
    empty = dk.read_marker(u.marker(data_root, "empty"))
    if empty and empty.get("confirmed"):
        return "empty"
    failed = dk.read_marker(u.marker(data_root, "failed"))
    if failed and failed.get("last_checked"):
        age = (now - datetime.fromisoformat(failed["last_checked"])).total_seconds()
        if age < fh.CONTENT_RETRY_S:
            return "deferred"
    return "missing"


def planned_requests(n_missing: int, max_downloads: int, offline: bool = False) -> int:
    """Je fehlendem Rohmonat 2 Requests, hoechstens max_downloads Rohmonate."""
    return 0 if offline else REQUESTS_PER_FILE * min(n_missing, max(0, max_downloads))


# -------------------------------------------------------------- download ---
def download(todo: list[tuple[str, tuple[int, int]]], data_root: Path, now, client: fh.Client
             ) -> tuple[dict, str | None]:
    """
    Fehlende Rohmonate nacheinander laden (fh.Ctx.get: Cache, Marker, Pause,
    429-Sperre wie im Fetcher). Rueckgabe: ({(sym, ym): status}, Abbruchgrund).
    status: cached | empty | failed | skipped | locked | <Outcome-Status>.
    429: Wartebudget 0 -> RateLimitExhausted sofort -> Abbruch, Rest "skipped".
    """
    out: dict = {}
    stop_reason = None
    locks, ctxs = {}, {}
    try:
        for sym in dict.fromkeys(s for s, _ in todo):
            lk = fh.acquire_lock(data_root, sym)
            if lk is None:
                log.error("%s: Symbol-Lock belegt (laeuft fetch_histdata.py?) - keine Downloads "
                          "fuer dieses Symbol", sym)
                continue
            locks[sym] = lk
            ctxs[sym] = fh.Ctx(sym, MODE, data_root, client, now, report_only=True)
        for i, (sym, ym) in enumerate(todo):
            key = (sym, ym)
            if stop_reason is not None:
                out[key] = "skipped"
                continue
            if sym not in ctxs:
                out[key] = "locked"
                continue
            ctx = ctxs[sym]
            try:
                o = ctx.get(ctx.unit(ym))
            except rl.RateLimitExhausted as e:
                stop_reason = f"rate_limited: {e}"
                log.error("429-Sperre (HistData drosselt): %s - Survey bricht die Downloads ab, "
                          "spaeter erneut starten", e)
                out[key] = "skipped"
                continue
            st = {"ok": "cached", "cached": "cached", "empty": "empty"}.get(o.status, o.status)
            if st not in ("cached", "empty"):
                log.warning("%s %s: %s %s", sym, ymstr(ym), o.status, o.error or "")
            out[key] = st
            log.info("Download %d/%d %s %s: %s", i + 1, len(todo), sym, ymstr(ym), st)
            if client.halted:
                stop_reason = client.abort or ("Schutzschalter" if client.throttle.tripped.is_set()
                                               else "Stop-Signal")
                log.error("Downloads abgebrochen: %s", stop_reason)
    finally:
        for lk in locks.values():
            lk.close()
    return out, stop_reason


# ------------------------------------------------------------ auswertung ---
def evaluate(data_root: Path, sym: str, ym: tuple[int, int], rules: dict) -> dict:
    """{regel: {"status", "reason", "median_deviation_h", "anchors", "cached",
    "dropped_weekend_ticks", "releasable_weekend", "large_reversals"}}.
    Freigebbare Wochenend-Ticks (geschlossener Markt) verwirft der Fetcher vor
    der Pruefung (drop_releasable_weekend=True) - wie nach einer Freigabe im
    echten Bau; sie werden nie umgerechnet."""
    res = {}
    for name, rule in rules.items():
        try:
            rep = fh.evaluate_raw_month(data_root, sym, ym, mode=MODE, rule=rule,
                                        drop_releasable_weekend=True)
        except Exception as e:      # defektes ZIP, Formatfehler: nicht pruefbar, Survey laeuft weiter
            log.warning("%s %s %s: Auswertung gescheitert: %s", sym, ymstr(ym), name, e)
            rep = {"status": "not_checkable", "reason": f"{type(e).__name__}: {e}", "cached": True}
        anchors = rep.get("anchors")
        res[name] = {"status": rep.get("status"), "reason": rep.get("reason"),
                     "median_deviation_h": rep.get("median_deviation_h"),
                     "anchors": len(anchors) if isinstance(anchors, (list, tuple)) else anchors,
                     "cached": bool(rep.get("cached", True)),
                     "dropped_weekend_ticks": int(rep.get("dropped_weekend_ticks") or 0),
                     "releasable_weekend": rep.get("releasable_weekend"),
                     "large_reversals": list(rep.get("large_reversals") or [])}
    # Verworfene Ruecksprung-Ticks fehlen im Bericht (geprueft wird ohne sie): fuer
    # die Liste "grosse Ruecksprunge" einmal ohne Verwerfen nachlesen (Rohdaten,
    # regelunabhaengig) und als verworfen markieren.
    need = [n for n, r in res.items() if r["dropped_weekend_ticks"]
            and (r["releasable_weekend"] or {}).get("reversal_ticks")]
    if need:
        try:
            raw_rep = fh.evaluate_raw_month(data_root, sym, ym, mode=MODE, rule=rules[need[0]],
                                            drop_releasable_weekend=False)
            revs = list(raw_rep.get("large_reversals") or [])
        except Exception as e:
            log.warning("%s %s: Ruecksprunge nicht lesbar: %s", sym, ymstr(ym), e)
            revs = []
        for n in need:
            known = {(x.get("at_raw"), x.get("back_to_raw")) for x in res[n]["large_reversals"]}
            res[n]["large_reversals"] += [dict(x, dropped=True) for x in revs
                                          if (x.get("at_raw"), x.get("back_to_raw")) not in known]
    return res


def classify(results: dict, kind: str, dst: list[str]) -> dict:
    """Ergebnis eines Rohmonats: decisive (genau eine Regel ok, alle anderen
    failed) + winner; sonst Text. Kontrollmonat: alle Sommerzeit-Regeln ok?"""
    passing = [n for n, r in results.items() if r["status"] == "ok"]
    unclear = [n for n, r in results.items() if r["status"] not in ("ok", "failed")]
    out: dict = {"passing": passing, "decisive": False, "winner": None}
    if len(passing) == 1 and not unclear:
        out.update(decisive=True, winner=passing[0], text=f"entscheidend: {passing[0]}")
    elif not passing and not unclear:
        out["text"] = "keine Regel passt"
        out["none_pass"] = True
    elif not passing:
        out["text"] = "nicht pruefbar"
    elif len(passing) > 1:
        out["text"] = "mehrere passen: " + ", ".join(passing)
    else:
        out["text"] = f"nicht entscheidbar ({passing[0]} ok, {', '.join(unclear)} nicht pruefbar)"
    if kind == "control" and dst:
        bad = [n for n in dst if n in results and results[n]["status"] != "ok"]
        out["control_ok"] = not bad
        if bad:
            out["text"] += " | Kontrolle gescheitert: " + ", ".join(bad)
        elif not out["decisive"]:
            out["text"] = "Kontrolle ok: " + ", ".join(passing)
    return out


def _runs(items: list[tuple[tuple[int, int], str]]) -> list[dict]:
    """Chronologische Folge (ym, regel) -> Bloecke gleicher Regel."""
    runs: list[dict] = []
    for ym, w in sorted(items):
        if runs and runs[-1]["rule"] == w:
            runs[-1]["last"] = ym
        else:
            runs.append({"rule": w, "first": ym, "last": ym})
    return runs


def decisive_by_month(rows: list[dict], support: dict | None = None) -> tuple[dict, list[str]]:
    """{ym: regel} aus allen Symbolen; Monate mit verschiedenen Gewinnern je
    Symbol sind Konflikte (nicht im Fahrplan). support (optional, wird
    gefuellt): {ym: Menge der Symbole, die den Gewinner stuetzen}."""
    winners: dict[tuple[int, int], set] = {}
    syms: dict[tuple[int, int], set] = {}
    for r in rows:
        if r["decisive"]:
            winners.setdefault(r["ym"], set()).add(r["winner"])
            syms.setdefault(r["ym"], set()).add(r["symbol"])
    good = {ym: next(iter(w)) for ym, w in winners.items() if len(w) == 1}
    if support is not None:
        support.update({ym: syms[ym] for ym in good})
    conflicts = [f"{ymstr(ym)}: {', '.join(sorted(w))}" for ym, w in sorted(winners.items())
                 if len(w) > 1]
    return good, conflicts


def year_verdicts(rows: list[dict]) -> dict[int, dict]:
    """Urteil je Jahr ueber alle Symbole."""
    out = {}
    for y in sorted({r["ym"][0] for r in rows}):
        yr = [r for r in rows if r["ym"][0] == y]
        dec = [r for r in yr if r["decisive"]]
        winners = Counter(r["winner"] for r in dec)
        none_pass = [r for r in yr if r.get("none_pass")]
        notes = []
        if not winners:
            if none_pass and not any(r["passing"] for r in yr if r["evaluated"]):
                text = "keine Regel passt"
            else:
                text = "nicht entscheidbar"
        elif len(winners) == 1:
            w = next(iter(winners))
            agrees = all(r["configured"] == w for r in dec)
            text = f"{w} passt" if agrees else f"nur {w} passt"
        else:
            runs = _runs([(r["ym"], r["winner"]) for r in dec])
            if len(runs) == len(winners):
                text = "Wechsel im Jahr: " + " -> ".join(
                    f"{x['rule']} ({ymstr(x['first'])}..{ymstr(x['last'])})" for x in runs)
            else:
                text = "widerspruechlich: " + ", ".join(f"{w} {n}x" for w, n in winners.items())
        if winners:
            by_sym = Counter(r["symbol"] for r in dec)
            notes.append("entscheidend: " + ", ".join(f"{s} {n}" for s, n in sorted(by_sym.items())))
        if none_pass and winners:
            notes.append("keine Regel passt in " + ", ".join(
                f"{r['symbol']} {ymstr(r['ym'])}" for r in none_pass))
        ctrl_bad = [r for r in yr if r.get("control_ok") is False]
        if ctrl_bad:
            notes.append("Kontrolle gescheitert: " + ", ".join(
                f"{r['symbol']} {ymstr(r['ym'])}" for r in ctrl_bad))
        missing = [r for r in yr if not r["evaluated"]]
        if missing:
            notes.append(f"{len(missing)} Rohmonate nicht ausgewertet")
        out[y] = {"verdict": text, "winners": dict(winners), "notes": notes}
    return out


# ------------------------------------------------- umstellungsperioden ---
def period_key(ym: tuple[int, int]) -> tuple[int, int] | None:
    """(Jahr, 0=Maerz|1=Okt/Nov) des Rohmonats, None ausserhalb der Perioden."""
    for k, months in enumerate(PERIOD_MONTHS):
        if ym[1] in months:
            return ym[0], k
    return None


def period_label(key: tuple[int, int]) -> str:
    return f"{key[0]} {PERIOD_NAMES[key[1]]}"


def period_months(key: tuple[int, int]) -> list[tuple[int, int]]:
    return [(key[0], m) for m in PERIOD_MONTHS[key[1]]]


def period_next(key: tuple[int, int]) -> tuple[int, int]:
    return (key[0], key[1] + 1) if key[1] + 1 < len(PERIOD_MONTHS) else (key[0] + 1, 0)


def periods_between(a: tuple[int, int], b: tuple[int, int]) -> list[tuple[int, int]]:
    """Alle Umstellungsperioden echt zwischen a und b (auch nicht untersuchte)."""
    out, k = [], period_next(a)
    while k < b:
        out.append(k)
        k = period_next(k)
    return out


_STATE_SHORT = {"before_history": "vor Historienbeginn", "not_closed": "nicht abgeschlossen",
                "empty": "keine Daten"}


def _month_note(r: dict) -> str:
    """Kurztext eines nicht entscheidenden Rohmonats (fuer offene Perioden)."""
    if r["evaluated"]:
        return r.get("text", "nicht entscheidend").split(" | ")[0]
    return _STATE_SHORT.get(r.get("state"), "fehlt")


def symbol_period(rows: list[dict]) -> dict:
    """Urteil EINES Symbols in EINER Periode aus seinen Rohmonaten:
    status "rule" (genau eine Regel entscheidend, Feld rule), "conflict" (zwei
    Monate mit verschiedenen Gewinnern) oder "open" (kein entscheidender Monat;
    nicht entscheidende/fehlende Monate widersprechen nicht)."""
    rows = sorted(rows, key=lambda r: r["ym"])
    winners = {r["month"]: r["winner"] for r in rows if r["decisive"]}
    dropped = {}
    for r in rows:
        if r["decisive"]:
            n = (r["results"].get(r["winner"]) or {}).get("dropped_weekend_ticks") or 0
            if n:
                dropped[r["month"]] = int(n)
    out = {"months": {r["month"]: (r["winner"] if r["decisive"] else None) for r in rows},
           "decisive_months": sorted(winners), "dropped_weekend_ticks": dropped,
           "notes": {r["month"]: _month_note(r) for r in rows if not r["decisive"]}}
    distinct = sorted(set(winners.values()))
    mm = lambda ms: "+".join(m[-2:] for m in ms)
    if len(distinct) > 1:
        out.update(status="conflict", rule=None, cell="KONFLIKT (" + ", ".join(
            f"{m[-2:]}: {w}" for m, w in sorted(winners.items())) + ")")
    elif distinct:
        cell = f"{distinct[0]} ({mm(sorted(winners))})"
        if dropped:
            cell += " [" + ", ".join(f"{m[-2:]}: {n} Wochenend-Ticks verworfen"
                                     for m, n in sorted(dropped.items())) + "]"
        out.update(status="rule", rule=distinct[0], cell=cell)
    else:
        out.update(status="open", rule=None, cell="offen (" + ", ".join(
            f"{m[-2:]}: {t}" for m, t in sorted(out["notes"].items())) + ")")
    return out


def build_periods(rows: list[dict], symbols) -> list[dict]:
    """Umstellungsperioden (chronologisch) mit Urteil je Symbol und gesamt:
    status "confirmed" (rule), "open" oder "conflict" (conflict = "month" |
    "symbol"). Bestaetigt = fuer mindestens ein Symbol entscheidend und ohne
    Widerspruch (Monate eines Symbols bzw. die Symbole untereinander)."""
    by_key: dict[tuple[int, int], dict[str, list]] = {}
    for r in rows:
        k = period_key(r["ym"])
        if k is not None:
            by_key.setdefault(k, {}).setdefault(r["symbol"], []).append(r)
    out = []
    for k in sorted(by_key):
        per = {s: symbol_period(by_key[k][s]) for s in symbols if s in by_key[k]}
        rules = {s: v["rule"] for s, v in per.items() if v["status"] == "rule"}
        month_conf = [s for s, v in per.items() if v["status"] == "conflict"]
        p = {"key": k, "label": period_label(k), "months": [ymstr(m) for m in period_months(k)],
             "symbols": per, "rule": None, "conflict": None}
        if month_conf:
            p.update(status="conflict", conflict="month", text="KONFLIKT (Monate: " + "; ".join(
                f"{s} {per[s]['cell'][len('KONFLIKT ('):-1]}" for s in month_conf) + ")")
        elif len(set(rules.values())) > 1:
            p.update(status="conflict", conflict="symbol", text="KONFLIKT (Symbole: " + ", ".join(
                f"{s} {w}" for s, w in sorted(rules.items())) + ")")
        elif rules:
            p.update(status="confirmed", rule=next(iter(rules.values())),
                     text=next(iter(rules.values())) + f" ({', '.join(rules)})")
        else:
            p.update(status="open", text="offen")
        # Evidenz: entscheidende Rohmonate -> Symbole (fuer die Mindest-Evidenz je Block)
        support: dict[str, list] = {}
        for s, v in per.items():
            for m in v["decisive_months"]:
                support.setdefault(m, []).append(s)
        p["support"] = {m: sorted(v) for m, v in sorted(support.items())}
        out.append(p)
    return out


def _block_weak(b: dict) -> bool:
    """Zu wenig Evidenz: weniger als MIN_BLOCK_MONTHS entscheidende Monate und
    kein Monat, in dem beide Symbole entscheiden."""
    return len(b["support"]) < MIN_BLOCK_MONTHS and not any(len(v) >= 2 for v in b["support"].values())


def analyze_switches(periods: list[dict]) -> dict:
    """
    Wechselanalyse ueber die bestaetigten Perioden (chronologisch). Bloecke =
    Folgen bestaetigter Perioden mit gleicher Regel (offene Perioden
    unterbrechen nicht). switches = Bloecke - 1. ok: hoechstens ein Wechsel und
    kein Konflikt. Bei genau einem Wechsel: letzte bestaetigte Periode der alten
    Regel, erste der neuen und alle Perioden dazwischen (auch nicht untersuchte).
    """
    by_key = {p["key"]: p for p in periods}
    blocks: list[dict] = []
    for p in periods:
        if p["status"] != "confirmed":
            continue
        if blocks and blocks[-1]["rule"] == p["rule"]:
            b = blocks[-1]
            b["last"] = p["key"]
            b["periods"].append(p["label"])
        else:
            b = {"rule": p["rule"], "first": p["key"], "last": p["key"], "periods": [p["label"]],
                 "support": {}}
            blocks.append(b)
        for m, syms in p["support"].items():
            b["support"][m] = syms
    for b in blocks:
        b["weak"] = _block_weak(b)
    conflicts = [{"period": p["label"], "kind": p["conflict"], "text": p["text"]}
                 for p in periods if p["status"] == "conflict"]
    switches = max(0, len(blocks) - 1)
    an: dict = {"switches": switches, "conflicts": conflicts, "blocks": blocks,
                "ok": switches <= 1 and not conflicts, "between": [], "last_old": None,
                "first_new": None, "explanation": []}

    def span(b):
        return period_label(b["first"]) + ("" if b["first"] == b["last"] else
                                           f" .. {period_label(b['last'])}")
    if conflicts:
        an["explanation"].append("Konflikt in " + "; ".join(
            f"{c['period']}: {c['text']}" for c in conflicts))
    if switches > 1:
        an["explanation"].append(f"{switches} Wechsel statt hoechstens einem: " + " -> ".join(
            f"{b['rule']} ({span(b)})" for b in blocks))
    if switches == 1:
        old, new = blocks
        an["last_old"], an["first_new"] = period_label(old["last"]), period_label(new["first"])
        for k in periods_between(old["last"], new["first"]):
            p = by_key.get(k)
            if p is None:
                st = "nicht untersucht"
            elif p["status"] == "open":
                st = "offen: " + "; ".join(f"{s} " + ", ".join(
                    f"{m[-2:]} {t}" for m, t in sorted(v["notes"].items()))
                    for s, v in p["symbols"].items())
            else:
                st = p["text"]
            an["between"].append({"period": period_label(k), "key": k, "state": st})
    # Raender: offene Perioden vor dem ersten / nach dem letzten Beleg (Regel fortgeschrieben)
    if blocks:
        an["leading_open"] = [p["label"] for p in periods if p["key"] < blocks[0]["first"]]
        an["trailing_open"] = [p["label"] for p in periods if p["key"] > blocks[-1]["last"]]
    else:
        an["leading_open"], an["trailing_open"] = [p["label"] for p in periods], []
    an["weak"] = [{"rule": b["rule"], "periods": b["periods"], "months": sorted(b["support"]),
                   "symbols": sorted({s for v in b["support"].values() for s in v})}
                  for b in blocks if b["weak"]]
    return an


def differing_windows(a, b, lo: tuple[int, int], hi: tuple[int, int]) -> list[tuple[int, int]]:
    """Zeitraeume [von, bis) in UTC-ms (stuendlich), in denen die Regeln a und b
    verschiedene Rohzeiten liefern, ueber die Rohmonate lo..hi (inklusive)."""
    t0 = int(datetime(*lo, 1, tzinfo=UTC).timestamp() * 1000)
    t1 = int(datetime(*ym_add(hi, 1), 1, tzinfo=UTC).timestamp() * 1000)
    hours = np.arange(t0, t1, fh.HOUR_MS, dtype=np.int64)
    diff = fh.utc_to_raw_ms(hours, a) != fh.utc_to_raw_ms(hours, b)
    out: list[tuple[int, int]] = []
    for i in np.nonzero(diff)[0]:
        h = int(hours[i])
        if out and out[-1][1] == h:
            out[-1] = (out[-1][0], h + fh.HOUR_MS)
        else:
            out.append((h, h + fh.HOUR_MS))
    return out


def _utc_str(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, UTC).strftime("%d.%m.%Y %H:%M")


def justify(an: dict, rules: dict) -> dict:
    """
    Begruendung der Grenze zwischen der letzten alten und der ersten neuen
    Periode: stuendlich nachgerechnet, wo die beiden Regeln verschieden
    stempeln. Gilt (equivalent=True), wenn alle Abweichungen im Bereich
    zwischen beiden Perioden in Umstellungsperioden liegen - Rohmonate ohne
    Umstellungsfenster sind dann unter beiden Regeln identisch, jede Grenze
    dazwischen ist gleichwertig.
    """
    old, new = an["blocks"][0], an["blocks"][1]
    ra, rb = rules[old["rule"]], rules[new["rule"]]
    lo = period_months(old["last"])[0]
    hi = period_months(new["first"])[-1]
    wins = differing_windows(ra, rb, lo, hi)
    by_period: dict[str, list[str]] = {}
    outside: list[str] = []
    for a, b in wins:
        ym = datetime.fromtimestamp(a / 1000, UTC)
        k = period_key((ym.year, ym.month))
        txt = f"{_utc_str(a)}-{_utc_str(b)} UTC"
        if k is None:
            outside.append(txt)
        else:
            by_period.setdefault(period_label(k), []).append(txt)
    gap_lo = ym_add(period_months(old["last"])[-1], 1)
    gap_hi = ym_add(period_months(new["first"])[0], -1)
    plain = [ymstr(m) for m in (ym_add(gap_lo, i) for i in range(
        (gap_hi[0] - gap_lo[0]) * 12 + gap_hi[1] - gap_lo[1] + 1)) if period_key(m) is None]
    names = (old["rule"], new["rule"])
    lines = [f"{names[0]} und {names[1]} stempeln nur in den Umstellungsfenstern verschieden"
             + (" (Maerz: US-Beginn 2. So Maerz bis EU-Beginn letzter So Maerz; Okt/Nov: "
                "EU-Ende letzter So Oktober bis US-Ende 1. So November)"
                if set(names) == {"ny_local", "berlin_minus_6"} else "")
             + f". Stuendlich nachgerechnet fuer {ymstr(lo)}..{ymstr(hi)}: "
             + "; ".join(f"{p} {', '.join(w)}" for p, w in by_period.items()) + "."]
    if outside:
        lines.append("ACHTUNG: die Regeln unterscheiden sich auch ausserhalb der "
                     "Umstellungsperioden (" + "; ".join(outside) + ") - die Grenze ist NICHT "
                     "gleichwertig waehlbar, kein Vorschlag.")
    else:
        lines.append(
            f"Rohmonate ohne Umstellungsfenster dazwischen ({_month_span(plain)}) liefern unter "
            f"beiden Regeln dieselben UTC-Zeiten; jede Grenze zwischen {an['last_old']} und "
            f"{an['first_new']} ist fuer sie gleichwertig. Der Vorschlag legt "
            f"{names[0]} bis einschliesslich {an['last_old']} fest und {names[1]} ab "
            f"{an['first_new']} ({ymstr(period_months(new['first'])[0])}).")
        if an["between"]:
            lines.append("Nicht belegte Umstellungsperioden dazwischen (" + ", ".join(
                b["period"] for b in an["between"]) + f") stehen als rule: {UNDECIDED} mit den "
                f"Kandidaten {names[0]}, {names[1]} im Fahrplan: dort wird nur umgerechnet, "
                "wo beide Regeln dasselbe ergeben.")
    return {"equivalent": not outside, "windows": by_period, "outside": outside,
            "plain_months": plain, "text": lines}


def _month_span(months: list[str]) -> str:
    """Monatsliste kompakt: zusammenhaengende Folgen als von..bis."""
    if not months:
        return "keine"
    yms = [parse_ym(m) for m in months]
    runs, start, prev = [], yms[0], yms[0]
    for ym in yms[1:]:
        if ym != ym_add(prev, 1):
            runs.append((start, prev))
            start = ym
        prev = ym
    runs.append((start, prev))
    return ", ".join(ymstr(a) if a == b else f"{ymstr(a)}..{ymstr(b)}" for a, b in runs)


def propose_schedule(an: dict, first_from: tuple[int, int], rules: dict) -> tuple[list[dict], dict | None]:
    """
    Fahrplan nach der Regel des Eigentuemers (nur bei an["ok"], ohne schwache
    Bloecke und mit gleichwertiger Grenze; sonst []):
      {from: first_from, rule: alt}
      {from: Monat nach der letzten alten Periode, rule: undecided,
       candidates: [alt, neu]}                 (nur bei Perioden dazwischen)
      {from: erster Monat der ersten neuen Periode, rule: neu}
    Rueckgabe: (Eintraege, Begruendung oder None).
    """
    blocks = an["blocks"]
    if not an["ok"] or not blocks or an["weak"]:
        return [], None
    old = blocks[0]
    sched = [{"from": ymstr(min(first_from, period_months(old["first"])[0])), "rule": old["rule"],
              "note": "belegt " + (old["periods"][0] if len(old["periods"]) == 1 else
                                   f"{old['periods'][0]} .. {old['periods'][-1]}")}]
    if len(blocks) == 1:
        return sched, None
    new = blocks[1]
    just = justify(an, rules)
    if not just["equivalent"]:
        return [], just
    if an["between"]:
        sched.append({"from": ymstr(ym_add(period_months(old["last"])[-1], 1)), "rule": UNDECIDED,
                      "candidates": [old["rule"], new["rule"]],
                      "note": "offen: " + ", ".join(b["period"] for b in an["between"])})
    sched.append({"from": ymstr(period_months(new["first"])[0]), "rule": new["rule"],
                  "note": "belegt " + (new["periods"][0] if len(new["periods"]) == 1 else
                                       f"{new['periods'][0]} .. {new['periods'][-1]}")})
    return sched, just


def _yaml_entry(frm: str, rule: str, candidates=None) -> str:
    if rule == UNDECIDED:
        return f"{{from: \"{frm}\", rule: {UNDECIDED}, candidates: [{', '.join(candidates)}]}}"
    return f"{{from: \"{frm}\", rule: {rule}}}"


def evidence_rows(periods: list[dict], symbols) -> tuple[list[str], list[list[str]]]:
    """Evidenztabelle Periode x Symbol (+ Gesamt)."""
    head = ["Periode"] + list(symbols) + ["Gesamt"]
    rows = [[p["label"]] + [p["symbols"][s]["cell"] if s in p["symbols"] else "-" for s in symbols]
            + [p["text"]] for p in periods]
    return head, rows


def text_table(head: list[str], rows: list[list[str]], prefix: str = "") -> list[str]:
    w = [max(len(x) for x in col) for col in zip(head, *rows)]
    fmt = lambda r: (prefix + "  ".join(c.ljust(n) for c, n in zip(r, w))).rstrip()
    return [fmt(head), prefix + "  ".join("-" * n for n in w)] + [fmt(r) for r in rows]


def md_table(head: list[str], rows: list[list[str]]) -> list[str]:
    esc = lambda c: c.replace("|", "\\|")
    return ["| " + " | ".join(map(esc, head)) + " |", "|" + "|".join("---" for _ in head) + "|"] + \
        ["| " + " | ".join(map(esc, r)) + " |" for r in rows]


def analysis_lines(an: dict) -> list[str]:
    """Wechselanalyse als Textzeilen (Konsole, YAML-Kommentar, Markdown)."""
    blocks = an["blocks"]
    out = [f"Wechsel: {an['switches']}; Konflikte: {len(an['conflicts'])}"]
    if not blocks:
        out.append("keine bestaetigte Umstellungsperiode")
    elif an["switches"] == 0:
        b = blocks[0]
        out.append(f"durchgehend {b['rule']}: {b['periods'][0]} .. {b['periods'][-1]} "
                   f"({len(b['periods'])} bestaetigte Perioden)")
    elif an["switches"] == 1:
        old, new = blocks
        out.append(f"{old['rule']} bis {an['last_old']} (letzte bestaetigte {old['rule']}-Periode; "
                   f"{len(old['periods'])} bestaetigt)")
        out.append(f"{new['rule']} ab {an['first_new']} (erste bestaetigte {new['rule']}-Periode; "
                   f"{len(new['periods'])} bestaetigt)")
        out.append("offen dazwischen: " + ("; ".join(f"{b['period']} ({b['state']})"
                                                     for b in an["between"]) or "keine"))
    out += an["explanation"]
    if an.get("leading_open") and blocks:
        out.append("vor dem ersten Beleg offen (Regel nach vorn fortgeschrieben): "
                   + ", ".join(an["leading_open"]))
    if an.get("trailing_open") and blocks:
        out.append("nach dem letzten Beleg offen (Regel fortgeschrieben): "
                   + ", ".join(an["trailing_open"]))
    for w in an["weak"]:
        out.append(f"zu wenig Evidenz: {w['rule']} nur in {', '.join(w['months'])} "
                   f"({', '.join(w['symbols'])}) - mindestens {MIN_BLOCK_MONTHS} entscheidende "
                   "Monate oder beide Symbole noetig")
    return out


def schedule_yaml(sched: list[dict], cfg=None, comment: list[str] = ()) -> str:
    """Abschnitt `schedule` fuer die Config: tick aus dem Survey, uebrige Modi
    unveraendert aus der aktuellen Config (die Config braucht alle Modi).
    comment: Zeilen fuer den Kommentarblock (Evidenz, Begruendung)."""
    lines = [f"# Vorschlag fuer {CONFIG_PATH} (der Survey schreibt die Datei NICHT)"]
    lines += [("# " + c).rstrip() for c in comment]
    lines += ["schedule:", f"  {MODE}:"]
    for e in sched:
        lines.append(f"    - {_yaml_entry(e['from'], e['rule'], e.get('candidates'))}"
                     + (f"   # {e['note']}" if e.get("note") else ""))
    for mode, entries in (cfg.schedule.items() if cfg is not None else ()):
        if mode == MODE:
            continue
        lines.append(f"  {mode}:   # unveraendert")
        lines += [f"    - {_yaml_entry(ymstr(ym), rule_name(r), rule_candidates(r))}"
                  for ym, r in entries]
    return "\n".join(lines)


def wrap(text: str, width: int = 96) -> list[str]:
    return textwrap.wrap(text, width) or [""]


def proposal_comment(periods, an, just, symbols, generated: str) -> list[str]:
    """Kommentarblock ueber dem YAML-Vorschlag: Evidenztabelle + Begruendung."""
    head, rows = evidence_rows(periods, symbols)
    out = [f"Evidenz (Zeitzonen-Survey {generated}; Umstellungsperiode x Symbol, Zelle = Regel, die",
           "in den Rohmonaten der Periode als einzige besteht; offen = nicht entscheidend/fehlt):"]
    out += text_table(head, rows, "  ")
    for line in analysis_lines(an):
        out += wrap(line)
    if just:
        out.append("Begruendung:")
        for t in just["text"]:
            out += wrap(t)
    return out


# ---------------------------------------------------------------- ausgabe ---
def _cell(r: dict | None) -> str:
    if r is None:
        return "-"
    dev = r.get("median_deviation_h")
    s = f"{r['status']} {dev:+.1f}" if isinstance(dev, (int, float)) else str(r["status"])
    if r.get("dropped_weekend_ticks"):
        s += f" ({r['dropped_weekend_ticks']} Wochenend-Ticks verworfen)"
    return s


def _conf_text(r: dict) -> str:
    if r["configured"] == UNDECIDED:
        return "offen"
    return r["configured"] or "?"


def format_table(sym: str, rows: list[dict], rule_names: list[str]) -> str:
    cells = [[_cell(r["results"].get(n)) if r["evaluated"] else "-" for n in rule_names] for r in rows]
    w = [max([len(n) + 2] + [len(c[i]) + 2 for c in cells] + [10]) for i, n in enumerate(rule_names)]
    head = f"{'Rohmonat':<9} {'Art':<4}" + "".join(f"{n:<{w[i]}}" for i, n in enumerate(rule_names)) + \
        f"{'konfiguriert':<16}Ergebnis"
    lines = [f"{sym} (Tick-Rohmonate; Zelle = Status + Median-Abweichung in h; "
             "Art T = Umstellung, K = Kontrolle; konfiguriert offen = undecided)", head, "-" * len(head)]
    for r, cs in zip(rows, cells):
        text = r["text"]
        if r.get("contradiction"):
            text += "  <-- WIDERSPRUCH zum Fahrplan"
        elif r.get("configured_failed"):
            text += "  <-- konfigurierte Regel faellt durch"
        lines.append(f"{ymstr(r['ym']):<9} {'K' if r['kind'] == 'control' else 'T':<4}"
                     + "".join(f"{c:<{w[i]}}" for i, c in enumerate(cs))
                     + f"{_conf_text(r):<16}{text}")
    rev = reversal_lines(rows)
    if rev:
        lines.append("Grosse Ruecksprunge in der Dateireihenfolge (roh, Status nach Verwerfen "
                     "freigebbarer Wochenend-Ticks):")
        lines += ["  " + x for x in rev]
    return "\n".join(lines)


def reversal_lines(rows: list[dict]) -> list[str]:
    """Je Rohmonat die grossen Ruecksprunge (Zeit, Minuten, Ticks, Wochenende),
    zusammengefasst ueber die Regeln (Regelnamen nur, wenn nicht alle)."""
    out = []
    for r in rows:
        if not r["evaluated"]:
            continue
        seen: dict[tuple, dict] = {}
        for name, res in r["results"].items():
            for x in res.get("large_reversals") or ():
                key = (x.get("at_raw"), x.get("back_to_raw"))
                v = seen.setdefault(key, {"x": x, "rules": [], "dropped": []})
                v["rules"].append(name)
                if x.get("dropped"):
                    v["dropped"].append(name)
        for (at, back), v in seen.items():
            x = v["x"]
            n = x.get("n_ticks")
            parts = [f"{x.get('minutes', '?')} min"]
            if n is not None:
                parts.append(f"{n} Ticks")
            if x.get("weekend_closed") is not None:
                parts.append("Wochenende (Markt zu)" if x["weekend_closed"] else "NICHT am Wochenende")
            if v["dropped"]:
                parts.append("verworfen" + ("" if len(v["dropped"]) == len(r["results"]) else
                                            f" unter {', '.join(v['dropped'])}"))
            rules_txt = "" if len(v["rules"]) == len(r["results"]) else \
                f" [nur unter {', '.join(v['rules'])}]"
            out.append(f"{r['month']}: {at} -> {back} ({', '.join(parts)}){rules_txt}")
    return out


def render_markdown(res: dict) -> str:
    """Survey-Ergebnis (JSON von survey()) als Markdown fuer die Doku."""
    syms = res["symbols"]
    out = ["# HistData-Zeitzonen-Survey (Tick)", "",
           f"Stand: {res['generated_utc']} - Exit {res['exit_code']} - Symbole {', '.join(syms)} - "
           f"Regeln {', '.join(res['rules'])} - Requests {res['requests']}", ""]
    if res["incomplete"]:
        out += [f"**Unvollstaendig:** {len(res['incomplete'])} Rohmonate fehlen noch "
                f"({REQUESTS_PER_FILE * len(res['incomplete'])} Requests); Ergebnis vorlaeufig.", ""]
    out += ["## Evidenz je Umstellungsperiode", "",
            "Zelle = Regel, die in den Rohmonaten der Periode als einzige besteht (in Klammern die "
            "entscheidenden Rohmonate); offen = kein entscheidender Monat (Grund je Monat); "
            "KONFLIKT = Monate oder Symbole widersprechen sich.", ""]
    out += md_table(res["evidence_table"]["head"], res["evidence_table"]["rows"]) + [""]
    out += ["## Wechselanalyse", ""] + [f"- {x}" for x in res["analysis_lines"]] + [""]
    out += ["## Fahrplan-Vorschlag", ""]
    if res["schedule"]:
        out += ["```yaml", res["schedule_yaml"], "```", ""]
    else:
        out += [res["no_schedule_reason"] or "Kein Vorschlag.", ""]
    if res.get("justification"):
        out += ["## Begruendung", ""] + [x + "\n" for x in res["justification"]["text"]]
    dropped = [(m["symbol"], m["month"], n, d["dropped_weekend_ticks"])
               for m in res["months"] for n, d in m.get("results", {}).items()
               if d.get("dropped_weekend_ticks")]
    if dropped:
        out += ["## Verworfene Wochenend-Ticks", "",
                "Freigebbare Ticks im geschlossenen Wochenendmarkt (z.B. roh Sa 20:00-20:59 in der "
                "EU-Umstellungsnacht unter berlin_minus_6): vor der Pruefung verworfen, nie "
                "umgerechnet.", ""]
        out += md_table(["Symbol", "Rohmonat", "Regel", "Ticks"],
                        [[s, m, n, str(k)] for s, m, n, k in dropped]) + [""]
    if res.get("reversals"):
        out += ["## Grosse Ruecksprunge (Dateireihenfolge)", ""] + \
            [f"- {s} {x}" for s, xs in res["reversals"].items() for x in xs] + [""]
    if res["configured_failed"]:
        out += ["## Konfigurierte Regel faellt durch", ""] + \
            [f"- {c['symbol']} {c['month']}: {c['configured']} ({c['reason'] or 'ohne Grund'})"
             for c in res["configured_failed"]] + [""]
    return "\n".join(out).rstrip() + "\n"


# ---------------------------------------------------------------- survey ---
INCOMPLETE_OK = ("cached", "empty", "before_history", "not_closed")


def survey(symbols=SYMBOLS, years=DEFAULT_YEARS, months=DEFAULT_MONTHS,
           controls=DEFAULT_CONTROLS, rules: list[str] | None = None,
           data_root: Path | None = None, *, periods=(), offline: bool = False,
           max_downloads: int = MAX_DOWNLOADS, pause: float = PAUSE_S, now=None,
           stop: threading.Event | None = None, out=None, **client_kw) -> dict:
    """Ganzer Survey; Rueckgabe = JSON-Ergebnis inkl. exit_code. periods:
    zusaetzliche Rohmonate (y, m). client_kw gehen an fh.Client (Tests:
    session, sleep, clock, wall)."""
    out = out or sys.stdout
    data_root = Path(data_root or dk.default_data_root())
    now = now or dk.utc_now
    t_now = now()
    cfg = load_config()
    all_rules = dict(cfg.rules) if cfg is not None else dict(fh.TZ_RULES)   # eingebaute + eigene
    names = list(rules or all_rules)
    unknown = [n for n in names if n not in all_rules]
    if unknown:
        raise ValueError(f"unbekannte Regel(n) {unknown}; bekannt: {sorted(all_rules)}")
    sel = {n: all_rules[n] for n in names}
    plan = plan_months(years, months, controls, periods)

    # 1) Bestand
    rows: list[dict] = []
    for ym, kind in plan:
        for sym in symbols:
            conf, cands = configured(ym, cfg)
            row = {"symbol": sym, "ym": ym, "month": ymstr(ym), "kind": kind,
                   "configured": conf, "evaluated": False, "results": {},
                   "decisive": False, "winner": None, "passing": []}
            if cands:
                row["configured_candidates"] = cands
            if fh.before_history(sym, ym):
                row.update(state="before_history", text="vor Historienbeginn")
            elif not month_closed(sym, ym, t_now):
                row.update(state="not_closed", text="Rohmonat nicht abgeschlossen")
            else:
                row["state"] = cache_state(data_root, sym, ym, t_now)
            rows.append(row)
    missing = [r for r in rows if r["state"] == "missing"]
    todo = missing[:max(0, max_downloads)] if not offline else []
    n_req = planned_requests(len(missing), max_downloads, offline)
    cached_n = sum(r["state"] == "cached" for r in rows)
    n_periods = len({period_key(ym) for ym, _ in plan} - {None})
    print(f"Plan: {len(plan)} Rohmonate x {len(symbols)} Symbole = {len(rows)} Rohdateien "
          f"({n_periods} Umstellungsperioden), {cached_n} im Cache, {len(missing)} fehlen; "
          f"{len(rows) - cached_n - len(missing)} ohne Download (vor Historienbeginn, nicht "
          "abgeschlossen, leer, Inhaltsfehler < 24 h).", file=out)
    if offline:
        print(f"Offline: {cached_n} Rohmonate im Cache, {len(missing)} fehlen (nicht geladen), "
              "0 Requests.", file=out)
    else:
        rest = len(missing) - len(todo)
        extra = (f"; {rest} weitere ueber --max-downloads {max_downloads} "
                 f"(naechster Aufruf, {REQUESTS_PER_FILE * rest} Requests)") if rest else ""
        print(f"Geplant: {len(todo)} Rohmonate laden = {n_req} Requests (Pause {pause:g} s, "
              f"mind. {n_req * pause / 60:.0f} min){extra}; {cached_n} schon im Cache.", file=out)
    out.flush()

    # 2) Downloads
    stop_reason = None
    requests = 0
    rate = None
    if todo:
        client = fh.Client(data_root, pause=pause, max_rate_limit_wait=0.0, stop=stop,
                           **client_kw)
        got, stop_reason = download([(r["symbol"], r["ym"]) for r in todo], data_root, now, client)
        for r in todo:
            r["state"] = {"cached": "cached", "empty": "empty"}.get(got[(r["symbol"], r["ym"])],
                                                                    got[(r["symbol"], r["ym"])])
        requests, rate = client.requests, client.gate.summary()
    for r in missing:
        if r not in todo:
            r["state"] = "not_cached" if offline else "over_limit"

    # 3) Auswertung (auch nach Abbruch: Teilergebnisse)
    texts = {"empty": "keine Daten (HistData leer)", "deferred": "Inhaltsfehler < 24 h (spaeter)",
             "not_cached": "nicht im Cache (--offline)", "over_limit": "nicht geladen (--max-downloads)",
             "skipped": "nicht geladen (Abbruch)", "locked": "nicht geladen (Symbol-Lock belegt)"}
    for r in rows:
        if r["state"] == "cached":
            r["results"] = evaluate(data_root, r["symbol"], r["ym"], sel)
            if not any(x["cached"] for x in r["results"].values()):
                r["state"], r["text"] = "not_cached", "nicht im Cache"
                continue
            r["evaluated"] = True
            r.update(classify(r["results"], r["kind"], dst_rules(sel, r["ym"][0])))
            conf = r["configured"]
            if conf is None or conf == UNDECIDED:
                # "undecided" im Fahrplan: offen, nie Widerspruch oder Fehler
                r["configured_result"] = None
                continue
            if r["decisive"] and r["winner"] != conf:
                r["contradiction"] = True
            # konfigurierte Regel selbst durchgefallen (auch ohne/mit mehreren Gewinnern)
            if conf not in r["results"] and cfg is not None and conf in cfg.rules:
                r["configured_result"] = evaluate(data_root, r["symbol"], r["ym"],
                                                  {conf: cfg.rules[conf]})[conf]
            else:
                r["configured_result"] = r["results"].get(conf)
            if (r["configured_result"] or {}).get("status") == "failed":
                r["configured_failed"] = True
        elif "text" not in r:
            r["text"] = texts.get(r["state"], f"nicht geladen ({r['state']})")
    incomplete = [r for r in rows if r["state"] not in INCOMPLETE_OK]
    contradictions = [r for r in rows if r.get("contradiction")]
    configured_failed = [r for r in rows if r.get("configured_failed")]

    # 4) Umstellungsperioden, Wechselanalyse, Vorschlag
    periods_ = build_periods(rows, symbols)
    an = analyze_switches(periods_)
    _, month_conflicts = decisive_by_month(rows)
    # Symbol-Konflikte ausserhalb der Umstellungsperioden (in Perioden: schon in an["conflicts"])
    other_conflicts = [c for c in month_conflicts if period_key(parse_ym(c[:7])) is None]
    # erster Eintrag ab dem Beginn des aktuellen Fahrplans (deckt alles davor ab)
    first_from = cfg.schedule[MODE][0][0] if cfg is not None else \
        ym_add(min(fh.HISTORY_START[s] for s in symbols), -1)
    sched, just = propose_schedule(an, first_from, all_rules)
    generated = t_now.strftime("%Y-%m-%d")
    comment = proposal_comment(periods_, an, just, symbols, generated)
    yaml_txt = schedule_yaml(sched, cfg, comment) if sched else ""
    if sched:
        no_reason = None
    elif not an["ok"]:
        no_reason = ("Kein Fahrplan-Vorschlag: " + " ".join(an["explanation"])
                     + " - Rohmonate der genannten Perioden pruefen (Tabellen, Ruecksprunge).")
    elif just is not None and not just["equivalent"]:
        no_reason = "Kein Fahrplan-Vorschlag: " + just["text"][-1]
    elif an["weak"]:
        no_reason = "Kein Fahrplan-Vorschlag: zu wenig Evidenz (siehe Wechselanalyse)."
    else:
        no_reason = "Kein Fahrplan-Vorschlag: keine bestaetigte Umstellungsperiode."

    # 5) Ausgabe
    for sym in symbols:
        print("\n" + format_table(sym, [r for r in rows if r["symbol"] == sym], names), file=out)
    verdicts = year_verdicts(rows)
    print("\nUrteil je Jahr (alle Symbole):", file=out)
    for y, v in verdicts.items():
        print(f"  {y}: {v['verdict']}" + (f"  ({'; '.join(v['notes'])})" if v["notes"] else ""),
              file=out)
    head, erows = evidence_rows(periods_, symbols)
    print("\nUmstellungsperioden (Periode x Symbol):", file=out)
    print("\n".join(text_table(head, erows, "  ")), file=out)
    a_lines = analysis_lines(an)
    print("\nWechselanalyse:", file=out)
    for line in a_lines:
        print(f"  {line}", file=out)
    if other_conflicts:
        print("\nSymbole widersprechen sich (ausserhalb der Umstellungsperioden): "
              + "; ".join(other_conflicts), file=out)
    if sched:
        if incomplete:
            print(f"\n# VORLAEUFIG: {len(incomplete)} Rohmonate fehlen noch - Vorschlag erst nach "
                  "vollstaendigem Survey uebernehmen.", file=out)
        print("\n" + yaml_txt, file=out)
        if cfg is not None and _same_schedule(sched, cfg):
            print("# Vorschlag = konfigurierter Fahrplan (tick).", file=out)
    else:
        print("\n" + no_reason, file=out)
    if contradictions:
        print("\nWIDERSPRUCH zum konfigurierten Fahrplan (fh.rule_for):", file=out)
        for r in contradictions:
            print(f"  {r['symbol']} {r['month']}: Daten -> {r['winner']}, konfiguriert "
                  f"{r['configured']}", file=out)
    if configured_failed:
        print("\nKONFIGURIERTE REGEL FAELLT DURCH (fh.rule_for):", file=out)
        for r in configured_failed:
            res_ = r["configured_result"]
            print(f"  {r['symbol']} {r['month']}: {r['configured']} failed "
                  f"({res_.get('reason') or 'ohne Grund'}); Ergebnis: {r['text']}", file=out)
    undecided_rows = [r for r in rows if r["configured"] == UNDECIDED and r["evaluated"]]
    if undecided_rows:
        print(f"\nOffen laut Fahrplan (undecided, kein Fehler): {len(undecided_rows)} Rohmonate "
              f"({', '.join(sorted({r['month'] for r in undecided_rows}))})", file=out)
    if incomplete:
        print(f"\nUNVOLLSTAENDIG: noch {len(incomplete)} Rohmonate offen "
              f"(= {REQUESTS_PER_FILE * len(incomplete)} Requests; " + ", ".join(
                  f"{r['symbol']} {r['month']} {r['state']}" for r in incomplete[:12]) +
              (", ..." if len(incomplete) > 12 else "") + ") - erneut starten setzt fort "
              "(der Cache bleibt).", file=out)
    if stop_reason:
        print(f"Downloads abgebrochen: {stop_reason}", file=out)
    bad = contradictions or configured_failed or not an["ok"] or other_conflicts
    code = EXIT_CONTRADICTION if bad else EXIT_INCOMPLETE if incomplete else EXIT_OK
    print(f"\nExit {code}; Requests: {requests}; noch fehlend: {len(incomplete)} Rohmonate", file=out)

    return {
        "generated_utc": t_now.isoformat(), "mode": MODE, "data_root": str(data_root),
        "symbols": list(symbols), "rules": names, "offline": offline,
        "planned_requests": n_req, "requests": requests, "rate_limit": rate,
        "stopped": stop_reason, "exit_code": code, "remaining_raw_months": len(incomplete),
        "months": [{k: v for k, v in r.items() if k != "ym"} for r in rows],
        "verdicts": {str(y): v for y, v in verdicts.items()},
        "periods": [{k: v for k, v in p.items() if k != "key"} for p in periods_],
        "evidence_table": {"head": head, "rows": erows},
        "switch_analysis": {
            "switches": an["switches"], "conflicts": an["conflicts"], "ok": an["ok"],
            "last_old": an["last_old"], "first_new": an["first_new"],
            "between": [{"period": b["period"], "state": b["state"]} for b in an["between"]],
            "blocks": [{"rule": b["rule"], "periods": b["periods"], "weak": b["weak"],
                        "decisive_months": b["support"]} for b in an["blocks"]],
            "leading_open": an["leading_open"], "trailing_open": an["trailing_open"],
            "explanation": an["explanation"]},
        "analysis_lines": a_lines,
        "schedule": [{k: v for k, v in e.items() if k != "note"} for e in sched],
        "schedule_details": sched, "schedule_yaml": yaml_txt, "no_schedule_reason": no_reason,
        "justification": just,
        "symbol_conflicts": month_conflicts, "weak_evidence": an["weak"],
        "reversals": {s: x for s in symbols
                      if (x := reversal_lines([r for r in rows if r["symbol"] == s]))},
        "configured_failed": [{"symbol": r["symbol"], "month": r["month"],
                               "configured": r["configured"],
                               "reason": (r["configured_result"] or {}).get("reason")}
                              for r in configured_failed],
        "contradictions": [{"symbol": r["symbol"], "month": r["month"], "winner": r["winner"],
                            "configured": r["configured"]} for r in contradictions],
        "incomplete": [{"symbol": r["symbol"], "month": r["month"], "state": r["state"]}
                       for r in incomplete],
    }


def _same_schedule(sched: list[dict], cfg) -> bool:
    cur = [(ymstr(ym), rule_name(r), rule_candidates(r)) for ym, r in cfg.schedule[MODE]]
    return cur == [(e["from"], e["rule"], e.get("candidates") or []) for e in sched]


# -------------------------------------------------------------------- cli ---
def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--symbols", nargs="+", type=str.upper, choices=SYMBOLS, default=list(SYMBOLS))
    p.add_argument("--preset", choices=PRESETS,
                   help=f"full: Jahre ab Historienbeginn bis {PRESET_LAST_YEAR}, Monate "
                        f"{' '.join(map(str, PRESET_MONTHS))}, keine Kontrollmonate "
                        "(--years/--months/--controls ueberschreiben)")
    p.add_argument("--years", nargs="+", type=int, default=None,
                   help=f"default {' '.join(map(str, DEFAULT_YEARS))} (ohne --preset/--periods)")
    p.add_argument("--months", nargs="+", type=int, default=None,
                   help="Umstellungsmonate je Jahr (default 3 4 10 11)")
    p.add_argument("--controls", nargs="*", type=int, default=None,
                   help="Kontrollmonate je Jahr (default 1 7)")
    p.add_argument("--periods", nargs="+", default=[], metavar="YYYY-MM",
                   help="einzelne Rohmonate zusaetzlich (allein: nur diese)")
    p.add_argument("--rules", nargs="+", default=None,
                   help="Kandidatenregeln (default: alle aus fetch_histdata.TZ_RULES "
                        "plus eigene Regeln aus der Config)")
    p.add_argument("--offline", action="store_true", help="nur den Cache auswerten, nie laden")
    p.add_argument("--max-downloads", type=int, default=MAX_DOWNLOADS,
                   help=f"hoechstens so viele Rohmonate je Aufruf laden (je 2 Requests, default "
                        f"{MAX_DOWNLOADS}); Rest beim naechsten Aufruf")
    p.add_argument("--pause", type=float, default=PAUSE_S,
                   help=f"Mindestabstand zwischen Requests in s (default {PAUSE_S:g})")
    p.add_argument("--json", metavar="PATH", help="Ergebnis zusaetzlich als JSON schreiben")
    p.add_argument("--markdown", metavar="PATH",
                   help="Evidenz, Wechselanalyse, Vorschlag und Begruendung als Markdown schreiben")
    p.add_argument("-v", "--verbose", action="store_true")
    a = p.parse_args(argv)
    try:
        periods = [parse_ym(x) for x in a.periods]
    except ValueError as e:
        p.error(str(e))
    years, months, controls = resolve_plan(a.preset, a.years, a.months, a.controls, bool(periods))
    if any(not 1 <= m <= 12 for m in months + controls):
        p.error("Monate 1..12")
    if a.max_downloads < 0 or a.pause < 0:
        p.error("--max-downloads und --pause >= 0")
    if a.rules:
        cfg = load_config()
        known = cfg.rules if cfg is not None else fh.TZ_RULES
        bad = [r for r in a.rules if r not in known]
        if bad:
            p.error(f"unbekannte Regel(n) {bad}; bekannt: {', '.join(known)}")
    logging.basicConfig(level=logging.DEBUG if a.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)-7s %(message)s")
    stop = threading.Event()
    dk.install_stop_handler(stop)
    res = survey(a.symbols, years, months, controls, a.rules, periods=periods, offline=a.offline,
                 max_downloads=a.max_downloads, pause=a.pause, stop=stop,
                 sleep=lambda s: stop.wait(s))      # Schlaf durch SIGTERM unterbrechbar
    if a.json:
        dk.write_json_atomic(Path(a.json), res)
    if a.markdown:
        dk._atomic_write(Path(a.markdown), render_markdown(res).encode())
    return res["exit_code"]


def resolve_plan(preset, years, months, controls, has_periods: bool
                 ) -> tuple[list[int], list[int], list[int]]:
    """Jahre/Monate/Kontrollmonate aus Preset, expliziten Angaben und Defaults.
    Explizit gegeben schlaegt Preset; ohne Preset und Jahre, aber mit --periods,
    nur die einzelnen Rohmonate (keine Default-Jahre)."""
    if preset:
        dy, dm, dc = preset_plan(preset)
    elif has_periods and years is None:
        dy, dm, dc = [], list(DEFAULT_MONTHS), list(DEFAULT_CONTROLS)
    else:
        dy, dm, dc = list(DEFAULT_YEARS), list(DEFAULT_MONTHS), list(DEFAULT_CONTROLS)
    return (list(years) if years is not None else dy, list(months) if months is not None else dm,
            list(controls) if controls is not None else dc)


if __name__ == "__main__":
    sys.exit(main())
