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

Umstellungsfenster: ny_local und berlin_minus_6 rechnen Rohzeiten nur im Maerz
(US-Beginn 2. So Maerz bis EU-Beginn letzter So Maerz) und im Okt/Nov (EU-Ende
letzter So Oktober bis US-Ende 1. So November) verschieden um; welche ROH-
monate Fensterstunden haben, rechnet der Survey je Rohmonat nach
(month_windows: Rohzeit -> UTC unter beiden Regeln). Alle anderen Rohmonate
sind unter beiden Regeln identisch.
- Rohmonat entscheidend (je Symbol): GENAU EINE Regel besteht, alle anderen
  fallen durch. Rohmonats-Urteil ueber XAUUSD/XAGUSD: wo beide entscheiden,
  muessen sie uebereinstimmen, sonst Konflikt. Nicht entscheidende Monate
  (alle/keine bestehen, nicht pruefbar, fehlt) widersprechen nicht.
- Wechselanalyse ueber die ENTSCHEIDENDEN ROHmonate (chronologisch, nicht
  ueber Perioden - ein entscheidender Oktober bestaetigt NICHT den November):
  Zahl der Wechsel, Konflikte, letzter entscheidender Rohmonat der alten und
  erster der neuen Regel, jeder Rohmonat dazwischen mit seinen Fenstern.
  Verlangt: hoechstens EIN Wechsel und kein Konflikt; sonst Exit 4 mit
  Erklaerung und ohne Vorschlag. Oktober alt / November neu ist ein
  gueltiger Wechsel an der Rohmonatsgrenze 10/11.
- Vorschlag (YAML mit Evidenztabelle und Begruendung als Kommentar):
    {from: <Beginn des Fahrplans>, rule: <alt>}
    {from: <Monat nach dem letzten entscheidenden alten Rohmonat>,
     rule: undecided, candidates: [<alt>, <neu>]}
                       (nur wenn dazwischen Rohmonate mit Fenster liegen)
    {from: <erster entscheidender neuer Rohmonat>, rule: <neu>}
  Begruendung (justify): jeder Rohmonat zwischen beiden Belegen mit
  Fensterstunden liegt im undecided-Block, sonst kein Vorschlag (Exit 4);
  Rohmonate ohne Fenster sind identisch, ihre Zuordnung ist beliebig. Ein
  Block (gleiche Regel) braucht mindestens 2 entscheidende Monate ODER beide
  Symbole im selben Monat, sonst "zu wenig Evidenz" (JSON "weak_evidence")
  und kein Vorschlag. Die Periodentabelle (Maerz = 03, Okt/Nov = 10 + 11) ist
  nur Anzeige. --markdown PATH schreibt dasselbe als Markdown (fuer die Doku).

Monate: Default Jahre 2010 2012 2015 2018 2021 2024, Umstellungsmonate 3 4 10
11, Kontrollmonate 1 7 (dort haben alle Sommerzeit-Regeln dieselben Offsets;
sie muessen alle bestehen, sonst Warnung "Kontrolle gescheitert").
--preset full: alle Jahre ab dem fruehesten Historienbeginn (fh.HISTORY_START)
bis 2025, Monate 3 10 11, keine Kontrollmonate. --periods YYYY-MM ...:
einzelne Rohmonate, kombinierbar mit --preset/--years (allein: nur diese).

Nur Tick-Modus, nur XAUUSD und XAGUSD.

Exit-Codes (Vorrang von oben nach unten):
  4  Befund: die konfigurierte Regel (fh.rule_for) hat in einem Monat Status
     "failed" (Liste "KONFIGURIERTE REGEL FAELLT DURCH", JSON
     "configured_failed"; entscheidend anders = zusaetzlich "WIDERSPRUCH",
     JSON "contradictions"), ODER mehr als ein Wechsel, ODER ein Konflikt -
     auch wenn noch Monate fehlen. Monate, die der Fahrplan als "undecided"
     fuehrt, stehen als "offen" da und zaehlen nie als Fehler.
  3  unvollstaendig: Download gescheitert, 429 (Sperre aktiv oder neu),
     --max-downloads erreicht, Symbol-Lock belegt, im --offline-Modus nicht
     im Cache, leerer Monat wartet auf Bestaetigung. Teilergebnisse werden
     trotzdem ausgewertet und gedruckt.
  4  "Wechsel nicht belegt": alles da, aber kein Vorschlag, weil ein Block zu
     wenig Evidenz hat oder die Grenze nicht gleichwertig ist (JSON
     "unproven"). Mit fehlenden Monaten gilt Exit 3 (mehr Daten koennen belegen).
  0  sonst.

Usage:
    python histdata_tz_survey.py
    python histdata_tz_survey.py --offline
    python histdata_tz_survey.py --preset full --periods 2019-03 2019-10 \\
        --json /tmp/tz.json --markdown /tmp/tz.md
    python histdata_tz_survey.py --symbols XAUUSD --years 2013 2014 --months 3 10 11
"""
from __future__ import annotations

import argparse
import functools
import logging
import re
import sys
import textwrap
import threading
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

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
    """cached | empty (bestaetigt keine Daten) | deferred (Inhaltsfehler < 24 h) |
    wait (leer, unbestaetigt, letzte Pruefung < CONFIRM_AFTER_S: fh.Ctx.get fragt
    dann NICHT an - kein Request, nicht im Request-Plan) | missing"""
    u = fh.Unit(sym, MODE, *ym)
    if u.path(data_root).exists():
        return "cached"

    def age(iso):
        return (now - datetime.fromisoformat(iso)).total_seconds() if iso else float("inf")
    failed = dk.read_marker(u.marker(data_root, "failed"))
    if failed and failed.get("last_checked") and age(failed["last_checked"]) < fh.CONTENT_RETRY_S:
        return "deferred"
    empty = dk.read_marker(u.marker(data_root, "empty"))
    if empty and empty.get("confirmed"):
        return "empty"
    if empty and age(empty.get("last_checked") or empty.get("first_seen")) < fh.CONFIRM_AFTER_S:
        return "wait"
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


_STATE_SHORT = {"before_history": "vor Historienbeginn", "not_closed": "nicht abgeschlossen",
                "empty": "keine Daten", "wait": "warten auf Bestaetigung"}


def _month_note(r: dict) -> str:
    """Kurztext eines nicht entscheidenden Rohmonats."""
    if r["evaluated"]:
        return r.get("text", "nicht entscheidend").split(" | ")[0]
    return _STATE_SHORT.get(r.get("state"), "fehlt")


# ---------------------------------------- rohmonate: urteil und wechsel ---
def build_months(rows: list[dict], symbols) -> list[dict]:
    """
    Urteil je ROHmonat ueber die Symbole (chronologisch, alle Monate des Plans):
    status "decisive" (rule; mindestens ein Symbol entscheidend, alle
    entscheidenden Symbole einig), "conflict" (Symbole entscheiden verschieden)
    oder "open" (kein Symbol entscheidend: alle/keine bestehen, nicht
    pruefbar, fehlt - widerspricht nicht).
    """
    by: dict[tuple[int, int], dict[str, dict]] = {}
    for r in rows:
        by.setdefault(r["ym"], {})[r["symbol"]] = r
    out = []
    for ym in sorted(by):
        per = {s: by[ym][s] for s in symbols if s in by[ym]}
        win = {s: r["winner"] for s, r in per.items() if r["decisive"]}
        v = {"ym": ym, "month": ymstr(ym), "winners": win, "rule": None,
             "support": sorted(win, key=list(symbols).index),
             "notes": {s: _month_note(r) for s, r in per.items() if not r["decisive"]}}
        if len(set(win.values())) > 1:
            v.update(status="conflict", text="KONFLIKT (Symbole: " + ", ".join(
                f"{s} {w}" for s, w in win.items()) + ")")
        elif win:
            v.update(status="decisive", rule=next(iter(win.values())))
            v["text"] = f"{v['rule']} ({', '.join(v['support'])})"
        else:
            v.update(status="open", text="offen (" + "; ".join(
                f"{s}: {t}" for s, t in v["notes"].items()) + ")")
        out.append(v)
    return out


def _block_weak(b: dict) -> bool:
    """Zu wenig Evidenz: weniger als MIN_BLOCK_MONTHS entscheidende Monate und
    kein Monat, in dem beide Symbole entscheiden."""
    return len(b["support"]) < MIN_BLOCK_MONTHS and not any(len(v) >= 2 for v in b["support"].values())


def _raw_to_utc_set(rule, raw: datetime) -> frozenset:
    """UTC-Zeitpunkte, auf die eine Rohzeit (naiv) unter `rule` faellt: einer,
    zwei (mehrdeutig, Sommerzeitende) oder keiner (nicht existent)."""
    local = raw - timedelta(hours=rule.shift_h)
    z = ZoneInfo(rule.zone)
    out = set()
    for fold in (0, 1):
        u = local.replace(tzinfo=z, fold=fold).astimezone(UTC)
        if u.astimezone(z).replace(tzinfo=None) == local:      # existiert in der Zone
            out.add(u)
    return frozenset(out)


@functools.lru_cache(maxsize=None)
def month_windows(a, b, ym: tuple[int, int]) -> tuple[tuple[datetime, datetime], ...]:
    """Rohzeit-Bereiche [von, bis) im ROHmonat ym, in denen die Regeln a und b
    eine Rohzeit verschieden auf UTC abbilden (auch: nur unter einer Regel nicht
    existent oder mehrdeutig). Halbstuendliches Raster, Stichprobe in der
    Slotmitte (Umstellungen liegen auf vollen Stunden, Verschiebungen sind ganze
    Stunden). Leer = Rohmonat unter beiden Regeln identisch."""
    t, end, step = datetime(*ym, 1), datetime(*ym_add(ym, 1), 1), timedelta(minutes=30)
    out: list[list[datetime]] = []
    while t < end:
        s = t + timedelta(minutes=15)
        if _raw_to_utc_set(a, s) != _raw_to_utc_set(b, s):
            if out and out[-1][1] == t:
                out[-1][1] = t + step
            else:
                out.append([t, t + step])
        t += step
    return tuple((x, y) for x, y in out)


def _win_text(wins) -> str:
    return ", ".join(f"{a:%d.%m. %H:%M}-{b:%d.%m. %H:%M}" for a, b in wins) + " roh"


def _months_from_to(a: tuple[int, int], b: tuple[int, int]) -> list[tuple[int, int]]:
    """Rohmonate a..b inklusive (leer, wenn b < a)."""
    out, k = [], a
    while k <= b:
        out.append(k)
        k = ym_add(k, 1)
    return out


def analyze_switches(months: list[dict], rules: dict | None = None) -> dict:
    """
    Wechselanalyse ueber die ENTSCHEIDENDEN ROHmonate (chronologisch): Bloecke =
    Folgen entscheidender Monate mit gleicher Regel (offene Monate unterbrechen
    nicht). switches = Bloecke - 1. ok: hoechstens ein Wechsel und kein
    Konflikt (Symbole uneinig im selben Rohmonat). Ein Wechsel zwischen Oktober
    und November derselben Periode ist ein gueltiger Wechsel an der Rohmonats-
    grenze 10/11. Bei genau einem Wechsel: letzter entscheidender Rohmonat der
    alten Regel (last_old), erster der neuen (first_new) und JEDER Rohmonat
    dazwischen (auch nicht untersuchte), mit den Fenstern, in denen beide Regeln
    verschieden umrechnen (rules noetig); Monate mit Fenster ohne Entscheidung
    gehoeren in den undecided-Block (undecided_months).
    """
    by_ym = {m["ym"]: m for m in months}
    blocks: list[dict] = []
    for m in months:
        if m["status"] != "decisive":
            continue
        if blocks and blocks[-1]["rule"] == m["rule"]:
            b = blocks[-1]
            b["last"] = m["ym"]
        else:
            b = {"rule": m["rule"], "first": m["ym"], "last": m["ym"], "support": {}}
            blocks.append(b)
        b["support"][m["month"]] = m["support"]
    for b in blocks:
        b["weak"] = _block_weak(b)
        b["months"] = list(b["support"])
    conflicts = [{"month": m["month"], "text": m["text"]} for m in months if m["status"] == "conflict"]
    switches = max(0, len(blocks) - 1)
    an: dict = {"switches": switches, "conflicts": conflicts, "blocks": blocks,
                "ok": switches <= 1 and not conflicts, "between": [], "undecided_months": [],
                "last_old": None, "first_new": None, "explanation": []}

    def span(b):
        return ymstr(b["first"]) + ("" if b["first"] == b["last"] else f"..{ymstr(b['last'])}")
    if conflicts:
        an["explanation"].append("Konflikt in " + "; ".join(
            f"{c['month']}: {c['text']}" for c in conflicts))
    if switches > 1:
        an["explanation"].append(f"{switches} Wechsel statt hoechstens einem: " + " -> ".join(
            f"{b['rule']} ({span(b)})" for b in blocks))
    if switches == 1:
        old, new = blocks
        an["last_old"], an["first_new"] = ymstr(old["last"]), ymstr(new["first"])
        for ym in _months_from_to(ym_add(old["last"], 1), ym_add(new["first"], -1)):
            m = by_ym.get(ym)
            wins = month_windows(rules[old["rule"]], rules[new["rule"]], ym) if rules else ()
            st = "nicht untersucht" if m is None else m["text"]
            an["between"].append({"month": ymstr(ym), "window": bool(wins),
                                  "windows": _win_text(wins) if wins else "", "state": st})
        an["undecided_months"] = [x["month"] for x in an["between"] if x["window"]]
    # Raender: nicht entscheidende Monate vor dem ersten / nach dem letzten Beleg
    if blocks:
        an["leading_open"] = [m["month"] for m in months if m["ym"] < blocks[0]["first"]]
        an["trailing_open"] = [m["month"] for m in months if m["ym"] > blocks[-1]["last"]]
    else:
        an["leading_open"], an["trailing_open"] = [m["month"] for m in months], []
    an["weak"] = [{"rule": b["rule"], "months": b["months"],
                   "symbols": sorted({s for v in b["support"].values() for s in v})}
                  for b in blocks if b["weak"]]
    return an


def justify(an: dict, rules: dict) -> dict:
    """
    Begruendung der Grenze (genau ein Wechsel), je ROHmonat nachgerechnet: fuer
    jeden Rohmonat zwischen dem letzten entscheidenden alten und dem ersten
    entscheidenden neuen Monat, ob die beiden Regeln irgendeine Rohzeit
    verschieden umrechnen. Jeder solche Monat muss im undecided-Block liegen
    (from = Monat nach dem letzten alten, bis vor den ersten neuen), sonst
    equivalent=False (kein Vorschlag, Exit 4). Monate ohne Fenster sind unter
    beiden Regeln identisch, ihre Zuordnung ist beliebig.
    """
    old, new = an["blocks"][0], an["blocks"][1]
    names = (old["rule"], new["rule"])
    und = an["undecided_months"]
    und_lo = ymstr(ym_add(old["last"], 1)) if und else None
    und_hi = ymstr(ym_add(new["first"], -1)) if und else None
    windowed = [b for b in an["between"] if b["window"]]
    uncovered = [b["month"] for b in windowed
                 if und_lo is None or not und_lo <= b["month"] <= und_hi]
    plain = [b["month"] for b in an["between"] if not b["window"]]
    lines = [f"{names[0]} und {names[1]} rechnen Rohzeiten nur in den Umstellungsfenstern "
             "verschieden nach UTC um"
             + (" (Maerz: US-Beginn 2. So Maerz bis EU-Beginn letzter So Maerz; Okt/Nov: "
                "EU-Ende letzter So Oktober bis US-Ende 1. So November)"
                if set(names) == {"ny_local", "berlin_minus_6"} else "")
             + "; je Rohmonat halbstuendlich nachgerechnet (Rohzeit -> UTC unter beiden Regeln).",
             f"Letzter entscheidender {names[0]}-Rohmonat: {an['last_old']}; erster "
             f"entscheidender {names[1]}-Rohmonat: {an['first_new']}."]
    if windowed:
        lines.append("Rohmonate dazwischen MIT Fenster, ohne entscheidendes Ergebnis: " + "; ".join(
            f"{b['month']} ({b['windows']}; {b['state']})" for b in windowed)
            + f" -> rule: {UNDECIDED} ab {und_lo} (Kandidaten {names[0]}, {names[1]}): dort "
            "wird nur umgerechnet, wo beide Regeln dasselbe ergeben.")
    else:
        lines.append("Dazwischen liegt kein Rohmonat mit Fenster: kein undecided-Block noetig.")
    lines.append(f"Rohmonate dazwischen ohne Fenster ({_month_span(plain)}) sind unter beiden "
                 "Regeln identisch; ihre Zuordnung ist beliebig. Der Vorschlag fuehrt "
                 f"{names[0]} bis einschliesslich {an['last_old']} und {names[1]} ab "
                 f"{an['first_new']}.")
    if uncovered:
        lines.append("ACHTUNG: Rohmonate mit Fenster ausserhalb des undecided-Blocks ("
                     + ", ".join(uncovered) + ") - Grenze NICHT gleichwertig, kein Vorschlag.")
    return {"equivalent": not uncovered, "uncovered": uncovered,
            "window_months": {b["month"]: b["windows"] for b in windowed},
            "undecided": [und_lo, und_hi] if und else None, "plain_months": plain, "text": lines}


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
    Fahrplan nach der Regel des Eigentuemers, auf Rohmonatsebene (nur bei
    an["ok"], ohne schwache Bloecke und mit gleichwertiger Grenze; sonst []):
      {from: first_from, rule: alt}
      {from: Monat nach dem letzten entscheidenden alten Rohmonat, rule:
       undecided, candidates: [alt, neu]}    (nur wenn dazwischen Rohmonate mit
                                              Umstellungsfenster liegen)
      {from: erster entscheidender neuer Rohmonat, rule: neu}
    Rueckgabe: (Eintraege, Begruendung oder None).
    """
    blocks = an["blocks"]
    if not an["ok"] or not blocks or an["weak"]:
        return [], None

    def note(b):
        return f"belegt {ymstr(b['first'])}..{ymstr(b['last'])} ({len(b['months'])} entscheidende Rohmonate)"
    old = blocks[0]
    sched = [{"from": ymstr(min(first_from, old["first"])), "rule": old["rule"], "note": note(old)}]
    if len(blocks) == 1:
        return sched, None
    new = blocks[1]
    just = justify(an, rules)
    if not just["equivalent"]:
        return [], just
    if just["undecided"]:
        sched.append({"from": just["undecided"][0], "rule": UNDECIDED,
                      "candidates": [old["rule"], new["rule"]],
                      "note": "offen (Fenster ohne Entscheidung): " + ", ".join(an["undecided_months"])})
    sched.append({"from": ymstr(new["first"]), "rule": new["rule"], "note": note(new)})
    return sched, just


# ------------------------------------------------- umstellungsperioden ---
def symbol_period(rows: list[dict]) -> dict:
    """Anzeige: EIN Symbol in EINER Umstellungsperiode. status "rule" (eine
    Regel entscheidend), "split" (Monate mit verschiedenen Gewinnern, z.B. 10
    ny_local, 11 berlin_minus_6 - ob das ein gueltiger Wechsel ist, entscheidet
    die Wechselanalyse ueber die Rohmonate) oder "open"."""
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
    drop_txt = (" [" + ", ".join(f"{m[-2:]}: {n} Wochenend-Ticks verworfen"
                                 for m, n in sorted(dropped.items())) + "]") if dropped else ""
    if len(distinct) > 1:
        out.update(status="split", rule=None, cell=" -> ".join(
            f"{m[-2:]}: {w}" for m, w in sorted(winners.items())) + drop_txt)
    elif distinct:
        out.update(status="rule", rule=distinct[0],
                   cell=f"{distinct[0]} ({'+'.join(m[-2:] for m in sorted(winners))})" + drop_txt)
    else:
        out.update(status="open", rule=None, cell="offen (" + ", ".join(
            f"{m[-2:]}: {t}" for m, t in sorted(out["notes"].items())) + ")")
    return out


def build_periods(rows: list[dict], symbols, months: list[dict] | None = None) -> list[dict]:
    """Umstellungsperioden (chronologisch) fuer die Evidenztabelle: Zelle je
    Symbol und Gesamt aus den Rohmonats-Urteilen (months, sonst neu gebaut):
    "confirmed" (eine Regel), "split" (Wechsel innerhalb der Periode, z.B. an
    der Grenze 10/11), "conflict" (Symbole uneinig im selben Rohmonat), "open"."""
    months = months if months is not None else build_months(rows, symbols)
    mv = {m["ym"]: m for m in months}
    by_key: dict[tuple[int, int], dict[str, list]] = {}
    for r in rows:
        k = period_key(r["ym"])
        if k is not None:
            by_key.setdefault(k, {}).setdefault(r["symbol"], []).append(r)
    out = []
    for k in sorted(by_key):
        per = {s: symbol_period(by_key[k][s]) for s in symbols if s in by_key[k]}
        vs = [mv[ym] for ym in period_months(k) if ym in mv]
        p = {"key": k, "label": period_label(k), "months": [ymstr(m) for m in period_months(k)],
             "symbols": per, "rule": None}
        conf = [v for v in vs if v["status"] == "conflict"]
        dec = [v for v in vs if v["status"] == "decisive"]
        if conf:
            p.update(status="conflict", text="; ".join(f"{v['month'][-2:]}: {v['text']}" for v in conf))
        elif len({v["rule"] for v in dec}) > 1:
            p.update(status="split", text="Wechsel: " + " -> ".join(
                f"{v['month'][-2:]} {v['rule']}" for v in dec))
        elif dec:
            syms = [s for s in symbols if any(s in v["support"] for v in dec)]
            p.update(status="confirmed", rule=dec[0]["rule"], text=f"{dec[0]['rule']} ({', '.join(syms)})")
        else:
            p.update(status="open", text="offen")
        out.append(p)
    return out


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
    out = [f"Wechsel: {an['switches']}; Konflikte: {len(an['conflicts'])} "
           "(ueber die entscheidenden Rohmonate, chronologisch)"]
    if not blocks:
        out.append("kein entscheidender Rohmonat")
    elif an["switches"] == 0:
        b = blocks[0]
        out.append(f"durchgehend {b['rule']}: {ymstr(b['first'])}..{ymstr(b['last'])} "
                   f"({len(b['months'])} entscheidende Rohmonate)")
    elif an["switches"] == 1:
        old, new = blocks
        out.append(f"{old['rule']} bis {an['last_old']} (letzter entscheidender {old['rule']}-"
                   f"Rohmonat; {len(old['months'])} entscheidend)")
        out.append(f"{new['rule']} ab {an['first_new']} (erster entscheidender {new['rule']}-"
                   f"Rohmonat; {len(new['months'])} entscheidend)")
        win = [b for b in an["between"] if b["window"]]
        out.append(f"dazwischen {len(an['between'])} Rohmonate, davon mit Umstellungsfenster "
                   "(-> undecided): " + ("; ".join(f"{b['month']} ({b['state']})" for b in win)
                                         or "keine"))
    out += an["explanation"]
    if an.get("leading_open") and blocks:
        out.append("vor dem ersten Beleg nicht entscheidend (Regel nach vorn fortgeschrieben): "
                   + _month_span(an["leading_open"]))
    if an.get("trailing_open") and blocks:
        out.append("nach dem letzten Beleg nicht entscheidend (Regel fortgeschrieben): "
                   + _month_span(an["trailing_open"]))
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
           "in den Rohmonaten als einzige besteht; offen = nicht entscheidend/fehlt):"]
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
    wait_n = sum(r["state"] == "wait" for r in rows)
    print(f"Plan: {len(plan)} Rohmonate x {len(symbols)} Symbole = {len(rows)} Rohdateien "
          f"({n_periods} Umstellungsperioden), {cached_n} im Cache, {len(missing)} fehlen; "
          f"{wait_n} warten auf Bestaetigung (leer, unbestaetigt: kein Request); "
          f"{len(rows) - cached_n - len(missing) - wait_n} ohne Download (vor Historienbeginn, "
          "nicht abgeschlossen, leer, Inhaltsfehler < 24 h).", file=out)
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
             "skipped": "nicht geladen (Abbruch)", "locked": "nicht geladen (Symbol-Lock belegt)",
             "wait": "warten auf Bestaetigung (leer, unbestaetigt; spaeter erneut)"}
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

    # 4) Rohmonats-Urteile, Wechselanalyse (Rohmonate), Perioden (Anzeige), Vorschlag
    month_v = build_months(rows, symbols)
    an = analyze_switches(month_v, all_rules)
    periods_ = build_periods(rows, symbols, month_v)
    _, month_conflicts = decisive_by_month(rows)
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
        no_reason = "Kein Fahrplan-Vorschlag (Wechsel nicht belegt): " + just["text"][-1]
    elif an["weak"]:
        no_reason = ("Kein Fahrplan-Vorschlag (Wechsel nicht belegt): zu wenig Evidenz "
                     "(siehe Wechselanalyse).")
    else:
        no_reason = "Kein Fahrplan-Vorschlag: kein entscheidender Rohmonat."
    # Bloecke vorhanden, aber kein Vorschlag (zu wenig Evidenz / Grenze nicht gleichwertig)
    unproven = bool(an["blocks"]) and an["ok"] and not sched

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
    # Reihenfolge: Widerspruch/Konflikt/mehrere Wechsel (4) vor unvollstaendig (3)
    # vor "Wechsel nicht belegt" (4: koennte mit mehr Daten noch belegt werden)
    bad = contradictions or configured_failed or not an["ok"]
    code = EXIT_CONTRADICTION if bad else EXIT_INCOMPLETE if incomplete else \
        EXIT_CONTRADICTION if unproven else EXIT_OK
    print(f"\nExit {code}; Requests: {requests}; noch fehlend: {len(incomplete)} Rohmonate", file=out)

    return {
        "generated_utc": t_now.isoformat(), "mode": MODE, "data_root": str(data_root),
        "symbols": list(symbols), "rules": names, "offline": offline,
        "planned_requests": n_req, "requests": requests, "rate_limit": rate,
        "stopped": stop_reason, "exit_code": code, "remaining_raw_months": len(incomplete),
        "unproven": unproven,
        "months": [{k: v for k, v in r.items() if k != "ym"} for r in rows],
        "raw_month_verdicts": [{k: v for k, v in m.items() if k != "ym"} for m in month_v],
        "verdicts": {str(y): v for y, v in verdicts.items()},
        "periods": [{k: v for k, v in p.items() if k != "key"} for p in periods_],
        "evidence_table": {"head": head, "rows": erows},
        "switch_analysis": {
            "switches": an["switches"], "conflicts": an["conflicts"], "ok": an["ok"],
            "last_old": an["last_old"], "first_new": an["first_new"],
            "between": an["between"], "undecided_months": an["undecided_months"],
            "blocks": [{"rule": b["rule"], "first": ymstr(b["first"]), "last": ymstr(b["last"]),
                        "weak": b["weak"], "decisive_months": b["support"]}
                       for b in an["blocks"]],
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
