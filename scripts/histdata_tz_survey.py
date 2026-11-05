#!/usr/bin/env python3
"""
Zeitzonen-Survey fuer HistData-Ticks: gilt die Stempelregel (gemessen fuer
2024: "berlin_minus_6" = Europe/Berlin-Ortszeit minus 6 h) ueber die Jahre,
oder hat HistData frueher anders gestempelt?

Ein Befehl:
1. laedt NUR die Rohmonate, die fuer den Survey fehlen (Roh-Cache von
   fetch_histdata.py, gleicher Client: ein Request zur Zeit, Pause, 429-Sperre
   ueber die gemeinsame Zustandsdatei; je Rohmonat 2 Requests). Vor dem
   ersten Download steht die geplante Request-Zahl auf der Konsole.
2. prueft jeden Rohmonat mit jeder Kandidatenregel (fh.TZ_RULES) ueber
   fh.evaluate_raw_month (nur Cache + Nachbar-Kontext, schreibt nichts).
3. druckt je Symbol eine Tabelle (Rohmonat x Regel: Status und Median-
   Abweichung), ein Urteil je Jahr und einen Vorschlag fuer den Abschnitt
   `schedule` in config/histdata_timezone.yaml. Die Config selbst wird NIE
   geschrieben.

Monate: Umstellungsmonate (Default Maerz, April, Oktober, November: dort
unterscheiden sich EU- und US-Sommerzeit bzw. Sommerzeit und feste Zeit) und
Kontrollmonate (Default Januar, Juli). Entscheidend ist nur ein Rohmonat, in
dem GENAU EINE Regel besteht und alle anderen durchfallen (keine "nicht
pruefbar"). In Kontrollmonaten haben alle Regeln mit Sommerzeit dieselben
Offsets; sie muessen dort alle bestehen, sonst Warnung "Kontrolle
gescheitert" (stimmt der Grund-Offset nicht?).

Nur Tick-Modus, nur XAUUSD und XAGUSD (EURUSD laedt der HistData-Fetcher
nicht mehr).

Exit-Codes:
  0  alle entscheidenden Monate passen zum konfigurierten Fahrplan (fh.rule_for)
  4  mindestens ein entscheidender Monat widerspricht dem Fahrplan (Liste auf
     der Konsole und im JSON)
  3  unvollstaendig: Download gescheitert, 429 (Sperre aktiv oder neu),
     --max-downloads erreicht, Symbol-Lock belegt, im --offline-Modus nicht
     im Cache. Teilergebnisse werden trotzdem ausgewertet und gedruckt.
  4 geht vor 3: ein Widerspruch ist ein Befund, auch wenn noch Monate fehlen.
  Ein erneuter Aufruf setzt fort (der Cache bleibt).

Usage:
    python histdata_tz_survey.py
    python histdata_tz_survey.py --offline
    python histdata_tz_survey.py --symbols XAUUSD --years 2013 2014 --months 3 10 11
    python histdata_tz_survey.py --json /tmp/tz_survey.json -v
"""
from __future__ import annotations

import argparse
import logging
import sys
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
MAX_DOWNLOADS = 80                   # Rohmonate je Aufruf (Default-Plan: 72)
PAUSE_S = 5.0
REQUESTS_PER_FILE = 2                # Seiten-GET + POST
CONFIG_PATH = "config/histdata_timezone.yaml"

EXIT_OK, EXIT_INCOMPLETE, EXIT_CONTRADICTION = 0, 3, 4

log = logging.getLogger("histdata_tz_survey")


# ----------------------------------------------------------------- helfer ---
def ymstr(ym: tuple[int, int]) -> str:
    return f"{ym[0]}-{ym[1]:02d}"


def ym_add(ym: tuple[int, int], k: int) -> tuple[int, int]:
    n = ym[0] * 12 + ym[1] - 1 + k
    return n // 12, n % 12 + 1


def rule_name(rule) -> str | None:
    """fh.rule_for kann eine TzRule oder einen Namen liefern."""
    if rule is None:
        return None
    return getattr(rule, "name", rule)


def load_config():
    """Fahrplan laden; kaputte Config -> None (kein Abbruch: der Survey hilft ja
    beim Reparieren, Widersprueche koennen dann aber nicht geprueft werden)."""
    try:
        return fh.load_tz_config()
    except ValueError as e:
        log.error("Zeitzonen-Konfiguration ungueltig: %s - Abgleich mit dem Fahrplan entfaellt", e)
        return None


def configured_rule(ym: tuple[int, int], cfg) -> str | None:
    if cfg is None:
        return None
    try:
        return rule_name(fh.rule_for(MODE, ym, cfg))
    except ValueError as e:                  # Fahrplan beginnt spaeter
        log.warning("kein konfigurierter Fahrplan fuer %s: %s", ymstr(ym), e)
        return None


def rule_offset_h(rule, when: datetime) -> float:
    """Offset der Rohzeit gegen UTC in h (Zone der Regel + Verschiebung)."""
    off = ZoneInfo(rule.zone).utcoffset(when.replace(tzinfo=None))
    return off.total_seconds() / 3600 + float(rule.shift_h)


def dst_rules(rules: dict, year: int) -> list[str]:
    """Regeln mit Sommerzeit (Offset Januar != Juli). In Januar und Juli haben
    sie dieselben Offsets (berlin_minus_6 = ny_local) - Kontrollmonate."""
    jan, jul = datetime(year, 1, 15, 12), datetime(year, 7, 15, 12)
    return [n for n, r in rules.items() if rule_offset_h(r, jan) != rule_offset_h(r, jul)]


def plan_months(years, months, controls) -> list[tuple[tuple[int, int], str]]:
    """[(ym, "transition"|"control")] chronologisch; Kontrolle geht vor."""
    kinds: dict[tuple[int, int], str] = {}
    for y in years:
        for m in months:
            kinds[(y, m)] = "transition"
        for m in controls:
            kinds[(y, m)] = "control"
    return sorted(kinds.items())


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
    """{regel: {"status", "reason", "median_deviation_h", "anchors", "cached"}}"""
    res = {}
    for name, rule in rules.items():
        try:
            rep = fh.evaluate_raw_month(data_root, sym, ym, mode=MODE, rule=rule)
        except Exception as e:      # defektes ZIP, Formatfehler: nicht pruefbar, Survey laeuft weiter
            log.warning("%s %s %s: Auswertung gescheitert: %s", sym, ymstr(ym), name, e)
            rep = {"status": "not_checkable", "reason": f"{type(e).__name__}: {e}", "cached": True}
        anchors = rep.get("anchors")
        res[name] = {"status": rep.get("status"), "reason": rep.get("reason"),
                     "median_deviation_h": rep.get("median_deviation_h"),
                     "anchors": len(anchors) if isinstance(anchors, (list, tuple)) else anchors,
                     "cached": bool(rep.get("cached", True))}
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


def decisive_by_month(rows: list[dict]) -> tuple[dict, list[str]]:
    """{ym: regel} aus allen Symbolen; Monate mit verschiedenen Gewinnern je
    Symbol sind Konflikte (nicht im Fahrplan)."""
    winners: dict[tuple[int, int], set] = {}
    for r in rows:
        if r["decisive"]:
            winners.setdefault(r["ym"], set()).add(r["winner"])
    good = {ym: next(iter(w)) for ym, w in winners.items() if len(w) == 1}
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


def suggest_schedule(decisive: dict, first_from: tuple[int, int]) -> list[dict]:
    """
    Fahrplan aus den entscheidenden Monaten: je Block gleicher Regel ein
    Eintrag. Erster Eintrag ab first_from (Historienbeginn). Ein Wechsel liegt
    irgendwo zwischen dem letzten Beleg der alten und dem ersten Beleg der neuen
    Regel; "from" = fruehester Monat, der noch zum Jahr des ersten Belegs
    gehoert, aber nach dem letzten Beleg der alten Regel liegt.
    """
    runs = _runs(list(decisive.items()))
    sched = []
    for i, r in enumerate(runs):
        if i == 0:
            start = min(first_from, r["first"])
            between = None
        else:
            prev = runs[i - 1]["last"]
            start = max(ym_add(prev, 1), (r["first"][0], 1))
            between = (prev, r["first"])
        sched.append({"from": ymstr(start), "rule": r["rule"], "evidence": [ymstr(r["first"]),
                      ymstr(r["last"])], "change_between": [ymstr(x) for x in between]
                      if between else None})
    return sched


def schedule_yaml(sched: list[dict], cfg=None) -> str:
    """Abschnitt `schedule` fuer die Config: tick aus dem Survey, uebrige Modi
    unveraendert aus der aktuellen Config (die Config braucht alle Modi)."""
    lines = [f"# Vorschlag fuer {CONFIG_PATH} (der Survey schreibt die Datei NICHT)",
             "schedule:", f"  {MODE}:"]
    for e in sched:
        com = f"belegt {e['evidence'][0]}..{e['evidence'][1]}"
        if e["change_between"]:
            com = f"Wechsel zwischen {e['change_between'][0]} und {e['change_between'][1]}; " + com
        lines.append(f"    - {{from: \"{e['from']}\", rule: {e['rule']}}}   # {com}")
    for mode, entries in (cfg.schedule.items() if cfg is not None else ()):
        if mode == MODE:
            continue
        lines.append(f"  {mode}:   # unveraendert")
        lines += [f"    - {{from: \"{ymstr(ym)}\", rule: {rule_name(r)}}}" for ym, r in entries]
    return "\n".join(lines)


# ---------------------------------------------------------------- ausgabe ---
def _cell(r: dict | None) -> str:
    if r is None:
        return "-"
    dev = r.get("median_deviation_h")
    return f"{r['status']} {dev:+.1f}" if isinstance(dev, (int, float)) else str(r["status"])


def format_table(sym: str, rows: list[dict], rule_names: list[str]) -> str:
    w = max([22] + [len(n) + 2 for n in rule_names])
    head = f"{'Rohmonat':<9} {'Art':<4}" + "".join(f"{n:<{w}}" for n in rule_names) + \
        f"{'konfiguriert':<16}Ergebnis"
    lines = [f"{sym} (Tick-Rohmonate; Zelle = Status + Median-Abweichung in h; "
             "Art T = Umstellung, K = Kontrolle)", head, "-" * len(head)]
    for r in rows:
        cells = "".join(f"{_cell(r['results'].get(n)) if r['evaluated'] else '-':<{w}}"
                        for n in rule_names)
        text = r["text"]
        if r.get("contradiction"):
            text += "  <-- WIDERSPRUCH zum Fahrplan"
        lines.append(f"{ymstr(r['ym']):<9} {'K' if r['kind'] == 'control' else 'T':<4}{cells}"
                     f"{(r['configured'] or '?'):<16}{text}")
    return "\n".join(lines)


# ---------------------------------------------------------------- survey ---
def survey(symbols=SYMBOLS, years=DEFAULT_YEARS, months=DEFAULT_MONTHS,
           controls=DEFAULT_CONTROLS, rules: list[str] | None = None,
           data_root: Path | None = None, *, offline: bool = False,
           max_downloads: int = MAX_DOWNLOADS, pause: float = PAUSE_S, now=None,
           stop: threading.Event | None = None, out=None, **client_kw) -> dict:
    """Ganzer Survey; Rueckgabe = JSON-Ergebnis inkl. exit_code. client_kw
    gehen an fh.Client (Tests: session, sleep, clock, wall)."""
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
    plan = plan_months(years, months, controls)

    # 1) Bestand
    rows: list[dict] = []
    for ym, kind in plan:
        for sym in symbols:
            row = {"symbol": sym, "ym": ym, "month": ymstr(ym), "kind": kind,
                   "configured": configured_rule(ym, cfg), "evaluated": False, "results": {},
                   "decisive": False, "winner": None, "passing": []}
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
    if offline:
        print(f"Offline: {cached_n} Rohmonate im Cache, {len(missing)} fehlen (nicht geladen), "
              "0 Requests.", file=out)
    else:
        extra = (f"; {len(missing) - len(todo)} weitere ueber --max-downloads {max_downloads} "
                 "(naechster Aufruf)") if len(missing) > len(todo) else ""
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
            if r["decisive"] and r["configured"] is not None and r["winner"] != r["configured"]:
                r["contradiction"] = True
        elif "text" not in r:
            r["text"] = texts.get(r["state"], f"nicht geladen ({r['state']})")
    incomplete = [r for r in rows if r["state"] not in
                  ("cached", "empty", "before_history", "not_closed")]
    contradictions = [r for r in rows if r.get("contradiction")]

    # 4) Ausgabe
    for sym in symbols:
        print("\n" + format_table(sym, [r for r in rows if r["symbol"] == sym], names), file=out)
    verdicts = year_verdicts(rows)
    print("\nUrteil je Jahr (alle Symbole):", file=out)
    for y, v in verdicts.items():
        print(f"  {y}: {v['verdict']}" + (f"  ({'; '.join(v['notes'])})" if v["notes"] else ""),
              file=out)
    decisive, conflicts = decisive_by_month(rows)
    # erster Eintrag ab dem Beginn des aktuellen Fahrplans (deckt alles davor ab)
    first_from = cfg.schedule[MODE][0][0] if cfg is not None else \
        min(fh.HISTORY_START[s] for s in symbols)
    sched = suggest_schedule(decisive, first_from) if decisive else []
    if conflicts:
        print("\nSymbole widersprechen sich (nicht im Vorschlag): " + "; ".join(conflicts), file=out)
    if sched:
        print("\n" + schedule_yaml(sched, cfg), file=out)
        if any(e["change_between"] for e in sched):
            print("# Wechselzeitpunkt nur eingegrenzt: mit --years/--months genauer pruefen.",
                  file=out)
    else:
        print("\nKein Fahrplan-Vorschlag: kein entscheidender Monat.", file=out)
    if contradictions:
        print("\nWIDERSPRUCH zum konfigurierten Fahrplan (fh.rule_for):", file=out)
        for r in contradictions:
            print(f"  {r['symbol']} {r['month']}: Daten -> {r['winner']}, konfiguriert "
                  f"{r['configured']}", file=out)
    if incomplete:
        print(f"\nUNVOLLSTAENDIG: {len(incomplete)} Rohmonate fehlen (" + ", ".join(
            f"{r['symbol']} {r['month']} {r['state']}" for r in incomplete[:12]) +
            (", ..." if len(incomplete) > 12 else "") + ") - erneut starten setzt fort.", file=out)
    if stop_reason:
        print(f"Downloads abgebrochen: {stop_reason}", file=out)
    code = EXIT_CONTRADICTION if contradictions else EXIT_INCOMPLETE if incomplete else EXIT_OK
    print(f"\nExit {code}; Requests: {requests}", file=out)

    return {
        "generated_utc": t_now.isoformat(), "mode": MODE, "data_root": str(data_root),
        "symbols": list(symbols), "rules": names, "offline": offline,
        "planned_requests": n_req, "requests": requests, "rate_limit": rate,
        "stopped": stop_reason, "exit_code": code,
        "months": [{k: v for k, v in r.items() if k != "ym"} for r in rows],
        "verdicts": {str(y): v for y, v in verdicts.items()},
        "schedule": [{"from": e["from"], "rule": e["rule"]} for e in sched],
        "schedule_details": sched, "symbol_conflicts": conflicts,
        "contradictions": [{"symbol": r["symbol"], "month": r["month"], "winner": r["winner"],
                            "configured": r["configured"]} for r in contradictions],
        "incomplete": [{"symbol": r["symbol"], "month": r["month"], "state": r["state"]}
                       for r in incomplete],
    }


# -------------------------------------------------------------------- cli ---
def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--symbols", nargs="+", type=str.upper, choices=SYMBOLS, default=list(SYMBOLS))
    p.add_argument("--years", nargs="+", type=int, default=list(DEFAULT_YEARS))
    p.add_argument("--months", nargs="+", type=int, default=list(DEFAULT_MONTHS),
                   help="Umstellungsmonate (default 3 4 10 11)")
    p.add_argument("--controls", nargs="*", type=int, default=list(DEFAULT_CONTROLS),
                   help="Kontrollmonate (default 1 7)")
    p.add_argument("--rules", nargs="+", default=None,
                   help="Kandidatenregeln (default: alle aus fetch_histdata.TZ_RULES "
                        "plus eigene Regeln aus der Config)")
    p.add_argument("--offline", action="store_true", help="nur den Cache auswerten, nie laden")
    p.add_argument("--max-downloads", type=int, default=MAX_DOWNLOADS,
                   help=f"hoechstens so viele Rohmonate laden (je 2 Requests, default {MAX_DOWNLOADS})")
    p.add_argument("--pause", type=float, default=PAUSE_S,
                   help=f"Mindestabstand zwischen Requests in s (default {PAUSE_S:g})")
    p.add_argument("--json", metavar="PATH", help="Ergebnis zusaetzlich als JSON schreiben")
    p.add_argument("-v", "--verbose", action="store_true")
    a = p.parse_args(argv)
    if any(not 1 <= m <= 12 for m in a.months + a.controls):
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
    res = survey(a.symbols, a.years, a.months, a.controls, a.rules, offline=a.offline,
                 max_downloads=a.max_downloads, pause=a.pause, stop=stop,
                 sleep=lambda s: stop.wait(s))      # Schlaf durch SIGTERM unterbrechbar
    if a.json:
        dk.write_json_atomic(Path(a.json), res)
    return res["exit_code"]


if __name__ == "__main__":
    sys.exit(main())
