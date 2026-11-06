"""Tests fuer histdata_tz_survey.py - nur synthetische Daten, HTTP und (meist) die
Auswertung gemockt; ein Integrationstest mit synthetischen Roh-ZIPs."""
import io
import json
import re
import zipfile
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import pytest

import fetch_histdata as fh
import histdata_tz_survey as sv

UTC = timezone.utc
NOW = datetime(2025, 6, 1, 12, tzinfo=UTC)
NO_MONTHS = dict(months=[3, 10], controls=[1, 7])


# ------------------------------------------------------------- hilfen ---
@pytest.fixture
def cfg(tmp_path, monkeypatch):
    """Eigene Fahrplan-Datei (unabhaengig von config/histdata_timezone.yaml)."""
    def write(tick, m1=(("2000-01", "fixed_est"),)):
        p = tmp_path / f"tz-{len(list(tmp_path.glob('tz-*.yaml')))}.yaml"
        lines = ["rules: []", "schedule:", "  tick:"]
        lines += [f'    - {{from: "{f}", rule: {r}}}' for f, r in tick]
        lines += ["  m1:"] + [f'    - {{from: "{f}", rule: {r}}}' for f, r in m1]
        p.write_text("\n".join(lines) + "\n")
        monkeypatch.setenv(fh.TZ_CONFIG_ENV, str(p))
        return p
    write((("2000-01", "berlin_minus_6"),))
    return write


def offsets(rule, ym):
    """Stuendliche Offsets (h) einer Regel ueber den Rohmonat (UTC-Raster, ueber
    fh.utc_to_raw_ms - gilt fuer jede Regelart, auch dst_daily_utc)."""
    lo = int(datetime(*ym, 1, tzinfo=UTC).timestamp() * 1000)
    hi = int(datetime(*sv.ym_add(ym, 1), 1, tzinfo=UTC).timestamp() * 1000)
    u = np.arange(lo, hi, 3_600_000, dtype=np.int64)
    return (fh.utc_to_raw_ms(u, rule) - u) / 3_600_000


def fake_evaluator(truth, calls=None, not_checkable=(), dropped=None, drop_flags=None,
                   nights=None):
    """evaluate_raw_month-Ersatz: Regel besteht, wenn sie im ganzen Rohmonat
    dieselben Offsets wie die "wahre" Stempelung truth(ym) hat. dropped:
    {(symbol, ym, regel): n} verworfene Wochenend-Ticks; drop_flags sammelt
    den Parameter drop_releasable_weekend."""
    def ev(data_root, symbol, ym, mode="tick", rule=None, drop_releasable_weekend=False):
        if calls is not None:
            calls.append((symbol, tuple(ym), rule.name))
        if drop_flags is not None:
            drop_flags.append(drop_releasable_weekend)
        if not fh.Unit(symbol, mode, *ym).path(data_root).exists():
            return {"status": "not_cached", "cached": False}
        if (symbol, tuple(ym), rule.name) in not_checkable:
            return {"status": "not_checkable", "reason": "zu wenige Anker", "cached": True,
                    "anchors": 1, "median_deviation_h": None}
        dev = offsets(fh.TZ_RULES[truth(ym)], ym) - offsets(rule, ym)
        n = (dropped or {}).get((symbol, tuple(ym), rule.name), 0)
        return {"status": "ok" if not dev.any() else "failed", "reason": None, "cached": True,
                "anchors": 40, "median_deviation_h": float(np.median(dev)),
                "dropped_weekend_ticks": n, "releasable_weekend": {"ticks": n} if n else None,
                "large_reversals": [],
                "transition_nights": (nights(symbol, tuple(ym), rule.name) if nights else None) or []}
    return ev


def touch_cache(root, sym, ym):
    p = fh.Unit(sym, "tick", *ym).path(root)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(b"PK-dummy")


def small_zip(sym, ym):
    u = fh.Unit(sym, "tick", *ym)
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(u.member, f"{ym[0]}{ym[1]:02d}01 000000000,1.0,1.1,0\n")
        zf.writestr(u.member[:-4] + ".txt", "HistData.com status report\n")
    return buf.getvalue()


PAGE = """<html><form id="file_down" method="POST" action="/get.php">
<input type="hidden" name="tk" value="tok" /><input type="hidden" name="date" value="{y}" />
<input type="hidden" name="datemonth" value="{y}{m:02d}" /><input type="hidden" name="platform" value="ASCII" />
<input type="hidden" name="timeframe" value="T" /><input type="hidden" name="fxpair" value="{pair}" />
</form></html>"""


class Resp:
    def __init__(self, status=200, content=b"", headers=None):
        self.status_code, self.content, self.headers = status, content, headers or {}

    @property
    def text(self):
        return self.content.decode("utf-8", "replace")


class Site:
    """Fake-HistData (Seiten-GET + POST); post_status: {n-ter POST: HTTP-Status}."""

    def __init__(self, post_status=None):
        self.calls, self.post_status, self._last = [], dict(post_status or {}), None

    def get(self, url, timeout=None, **kw):
        self.calls.append(("GET", url))
        pair, y, m = re.search(r"/([a-z]+)/(\d{4})/(\d{1,2})$", url).groups()
        self._last = (pair.upper(), (int(y), int(m)))
        return Resp(200, PAGE.format(y=y, m=int(m), pair=pair.upper()).encode())

    def post(self, url, data=None, headers=None, timeout=None, **kw):
        self.calls.append(("POST", url))
        n = sum(1 for c in self.calls if c[0] == "POST")
        if n in self.post_status:
            return Resp(self.post_status[n])
        return Resp(200, small_zip(*self._last))

    @property
    def requests(self):
        return len(self.calls)


class NoNet:
    def get(self, *a, **k):
        raise AssertionError("offline: kein Request erlaubt")

    post = get


class Clock:
    def __init__(self, t=1_750_000_000.0):
        self.t = t

    def __call__(self):
        return self.t

    def sleep(self, s):
        self.t += s


def run(tmp_path, site=None, clock=None, **kw):
    clock = clock or Clock()
    kw.setdefault("symbols", ["XAUUSD"])
    out = io.StringIO()
    res = sv.survey(data_root=tmp_path / "data", now=lambda: NOW, pause=0.0, out=out,
                    session=site or NoNet(), sleep=clock.sleep, clock=clock, wall=clock, **kw)
    return res, out.getvalue()


def ny_then_berlin(ym):
    return "ny_local" if ym[0] <= 2015 else "berlin_minus_6"


# --------------------------------------------------------------- plan ---
def test_wait_months_are_not_planned_as_requests(tmp_path, cfg, monkeypatch):
    """LOW 5: leer, unbestaetigt, letzte Pruefung < CONFIRM_AFTER_S -> fh.Ctx.get fragt
    nicht an: "warten auf Bestaetigung", nicht im Request-Plan, aber unvollstaendig."""
    monkeypatch.setattr(fh, "evaluate_raw_month", fake_evaluator(lambda ym: "berlin_minus_6"))
    root = tmp_path / "data"
    u = fh.Unit("XAUUSD", "tick", 2016, 3)
    mk = u.marker(root, "empty")
    mk.parent.mkdir(parents=True, exist_ok=True)
    recent = (NOW - timedelta(hours=2)).isoformat()
    mk.write_text(json.dumps({"first_seen": recent, "last_checked": recent, "confirmed": None}))
    assert sv.cache_state(root, "XAUUSD", (2016, 3), NOW) == "wait"
    assert sv.cache_state(root, "XAUUSD", (2016, 3), NOW + timedelta(days=2)) == "missing"
    site = Site()
    res, out = run(tmp_path, site, years=[2016], months=[3, 10], controls=[])
    assert "1 fehlen; 1 warten auf Bestaetigung" in out and "Geplant: 1 Rohmonate laden = 2 Requests" in out
    assert site.requests == 2 and res["planned_requests"] == 2
    st = {m["month"]: m["state"] for m in res["months"]}
    assert st == {"2016-03": "wait", "2016-10": "cached"}
    assert res["exit_code"] == 3 and res["incomplete"] == [
        {"symbol": "XAUUSD", "month": "2016-03", "state": "wait"}]


def test_unproven_switch_is_exit_4_when_complete_and_3_when_incomplete(tmp_path, cfg, monkeypatch):
    """LOW 5: Vorschlag nur wegen zu wenig Evidenz verweigert -> Exit 4 ("Wechsel nicht
    belegt"), solange nichts fehlt; fehlen noch Monate, Exit 3 (mehr Daten koennen belegen)."""
    cfg((("2000-01", "ny_local"), ("2016-03", "berlin_minus_6")))
    monkeypatch.setattr(fh, "evaluate_raw_month", fake_evaluator(ny_then_berlin))
    for ym in [(2015, 3), (2015, 10), (2016, 3)]:
        touch_cache(tmp_path / "data", "XAUUSD", ym)
    res, out = run(tmp_path, years=[2015, 2016], months=[3], controls=[], offline=True,
                   periods=[(2015, 10)])
    assert res["configured_failed"] == [] and res["incomplete"] == []
    assert res["schedule"] == [] and res["unproven"]
    assert "Wechsel nicht belegt" in out and res["exit_code"] == 4
    res, out = run(tmp_path, years=[2015, 2016], months=[3, 10], controls=[], offline=True)
    assert res["unproven"] and res["incomplete"] and res["exit_code"] == 3


def test_planned_request_count():
    assert sv.planned_requests(5, 60) == 10
    assert sv.planned_requests(100, 60) == 120
    assert sv.planned_requests(5, 0) == 0
    assert sv.planned_requests(5, 60, offline=True) == 0


def test_plan_prints_count_and_downloads_only_missing(tmp_path, cfg, monkeypatch):
    monkeypatch.setattr(fh, "evaluate_raw_month", fake_evaluator(ny_then_berlin))
    root = tmp_path / "data"
    for ym in [(2016, 1), (2016, 3)]:
        touch_cache(root, "XAUUSD", ym)
    site = Site()
    res, out = run(tmp_path, site, years=[2016], **NO_MONTHS)
    assert "Geplant: 2 Rohmonate laden = 4 Requests" in out
    assert res["planned_requests"] == 4 and site.requests == 4 and res["requests"] == 4
    got = {c[1].rsplit("/", 2)[-2:][1] for c in site.calls if c[0] == "GET"}
    assert got == {"7", "10"}                                # nur Juli und Oktober fehlten
    assert res["exit_code"] == 0, out


def test_max_downloads_limits_and_reports_incomplete(tmp_path, cfg, monkeypatch):
    monkeypatch.setattr(fh, "evaluate_raw_month", fake_evaluator(lambda ym: "berlin_minus_6"))
    site = Site()
    res, out = run(tmp_path, site, years=[2016], max_downloads=1, **NO_MONTHS)
    assert "= 2 Requests" in out and "3 weitere ueber --max-downloads 1" in out
    assert site.requests == 2
    assert res["exit_code"] == 3
    assert sorted(m["state"] for m in res["months"]) == ["cached", "over_limit", "over_limit",
                                                         "over_limit"]


def test_months_before_history_or_not_closed_are_skipped(tmp_path, cfg, monkeypatch):
    monkeypatch.setattr(fh, "evaluate_raw_month", fake_evaluator(lambda ym: "berlin_minus_6"))
    res, out = run(tmp_path, years=[2009, 2025], months=[3, 10], controls=[], offline=True)
    st = {m["month"]: m["state"] for m in res["months"]}
    # XAUUSD-Historie ab 2009-03; 2025-10 liegt nach NOW
    assert st == {"2009-03": "not_cached", "2009-10": "not_cached", "2025-03": "not_cached",
                  "2025-10": "not_closed"}
    res, _ = run(tmp_path, symbols=["XAGUSD"], years=[2009], months=[3], controls=[], offline=True)
    assert res["months"][0]["state"] == "before_history"     # XAGUSD erst ab 2009-05
    assert res["exit_code"] == 0                             # nichts zu tun = nichts fehlt


# ------------------------------------------------------------ offline ---
def test_offline_makes_no_requests_and_writes_nothing(tmp_path, cfg, monkeypatch):
    calls = []
    monkeypatch.setattr(fh, "evaluate_raw_month", fake_evaluator(ny_then_berlin, calls))
    root = tmp_path / "data"
    touch_cache(root, "XAUUSD", (2016, 3))
    before = sorted(p for p in root.rglob("*"))
    res, out = run(tmp_path, NoNet(), years=[2016], offline=True, **NO_MONTHS)
    assert res["requests"] == 0 and res["planned_requests"] == 0
    assert "Offline: 1 Rohmonate im Cache, 3 fehlen" in out and "0 Requests" in out
    assert sorted(p for p in root.rglob("*")) == before      # keine Marker, kein Ratelimit-Zustand
    assert {c[1] for c in calls} == {(2016, 3)}              # nur Gecachtes ausgewertet
    assert res["exit_code"] == 3
    assert {m["month"] for m in res["incomplete"]} == {"2016-01", "2016-07", "2016-10"}


# ------------------------------------------------------ urteil/fahrplan ---
def test_classify_needs_exactly_one_pass_and_no_unclear():
    ok = {"status": "ok"}
    bad = {"status": "failed"}
    nc = {"status": "not_checkable"}
    dst = ["berlin_minus_6", "ny_local"]
    r = sv.classify({"berlin_minus_6": ok, "ny_local": bad, "fixed_est": bad}, "transition", dst)
    assert r["decisive"] and r["winner"] == "berlin_minus_6"
    r = sv.classify({"berlin_minus_6": ok, "ny_local": nc, "fixed_est": bad}, "transition", dst)
    assert not r["decisive"] and "nicht entscheidbar" in r["text"]
    r = sv.classify({"berlin_minus_6": ok, "ny_local": ok, "fixed_est": bad}, "transition", dst)
    assert not r["decisive"] and r["text"].startswith("mehrere passen")
    r = sv.classify({"berlin_minus_6": bad, "ny_local": bad, "fixed_est": bad}, "transition", dst)
    assert not r["decisive"] and r["text"] == "keine Regel passt"
    # Kontrolle: alle Sommerzeit-Regeln muessen bestehen
    r = sv.classify({"berlin_minus_6": ok, "ny_local": ok, "fixed_est": bad}, "control", dst)
    assert r["control_ok"] and not r["decisive"]
    r = sv.classify({"berlin_minus_6": ok, "ny_local": bad, "fixed_est": ok}, "control", dst)
    assert r["control_ok"] is False and "Kontrolle gescheitert: ny_local" in r["text"]


def test_dst_rules_share_offsets_in_january_and_july():
    assert sorted(sv.dst_rules(fh.TZ_RULES, 2015)) == ["berlin_minus_6",
                                                      "berlin_minus_6_utc_midnight", "ny_local"]
    for ym in [(2015, 1), (2015, 7)]:
        assert (offsets(fh.TZ_RULES["berlin_minus_6"], ym) == offsets(fh.TZ_RULES["ny_local"], ym)).all()


def test_ny_until_2015_then_berlin_gives_two_entry_schedule_and_exit_4(tmp_path, cfg, monkeypatch):
    monkeypatch.setattr(fh, "evaluate_raw_month", fake_evaluator(ny_then_berlin))
    root = tmp_path / "data"
    years = [2012, 2015, 2016, 2018]
    for y in years:
        for m in (1, 3, 7, 10):
            touch_cache(root, "XAUUSD", (y, m))
            touch_cache(root, "XAGUSD", (y, m))
    res, out = run(tmp_path, years=years, symbols=["XAUUSD", "XAGUSD"], offline=True, **NO_MONTHS)
    # Rohmonatsebene: letzter ny-Beleg 2015-10, erster berlin-Beleg 2016-03; 2015-11 hat
    # ein Umstellungsfenster und wurde nicht untersucht -> undecided ab 2015-11
    assert res["schedule"] == [{"from": "2000-01", "rule": "ny_local"},
                               {"from": "2015-11", "rule": "undecided",
                                "candidates": ["ny_local", "berlin_minus_6"]},
                               {"from": "2016-03", "rule": "berlin_minus_6"}]
    an = res["switch_analysis"]
    assert (an["switches"], an["last_old"], an["first_new"], an["undecided_months"]) == \
        (1, "2015-10", "2016-03", ["2015-11"])
    assert [b["month"] for b in an["between"]] == ["2015-11", "2015-12", "2016-01", "2016-02"]
    assert res["verdicts"]["2012"]["verdict"] == "nur ny_local passt"
    assert res["verdicts"]["2015"]["verdict"] == "nur ny_local passt"
    assert res["verdicts"]["2016"]["verdict"] == "berlin_minus_6 passt"
    assert res["verdicts"]["2018"]["verdict"] == "berlin_minus_6 passt"
    assert not any(n.startswith("Kontrolle") for v in res["verdicts"].values() for n in v["notes"])
    # Ausgabe: Tabelle je Symbol, YAML-Vorschlag, Widerspruchsliste
    assert "XAUUSD (Tick-Rohmonate" in out and "XAGUSD (Tick-Rohmonate" in out
    assert '    - {from: "2000-01", rule: ny_local}' in out
    assert '    - {from: "2016-03", rule: berlin_minus_6}' in out
    assert '  m1:   # unveraendert' in out
    assert "WIDERSPRUCH" in out and "XAUUSD 2012-03: Daten -> ny_local" in out
    assert res["exit_code"] == 4
    assert {(c["month"], c["winner"]) for c in res["contradictions"]} == \
        {("2012-03", "ny_local"), ("2012-10", "ny_local"), ("2015-03", "ny_local"),
         ("2015-10", "ny_local")}
    # der Vorschlag ist eine gueltige Config (mit dem Fetcher-Parser geprueft)
    import yaml
    snippet = out[out.index("# Vorschlag"):].split("\n\n")[0]
    parsed = fh.build_tz_config({"rules": [], **yaml.safe_load(snippet)})
    assert fh.rule_for("tick", (2015, 10), parsed).name == "ny_local"
    assert fh.rule_for("tick", (2016, 2), parsed).name == "undecided"
    assert fh.rule_for("tick", (2016, 3), parsed).name == "berlin_minus_6"
    # Config nicht angefasst
    assert "ny_local" not in (tmp_path / "tz-0.yaml").read_text()


def test_exit_0_when_schedule_matches(tmp_path, cfg, monkeypatch):
    cfg((("2000-01", "ny_local"), ("2016-01", "berlin_minus_6")))
    monkeypatch.setattr(fh, "evaluate_raw_month", fake_evaluator(ny_then_berlin))
    root = tmp_path / "data"
    for y in (2015, 2016):
        for m in (1, 3, 7, 10):
            touch_cache(root, "XAUUSD", (y, m))
    res, out = run(tmp_path, years=[2015, 2016], offline=True, **NO_MONTHS)
    assert res["exit_code"] == 0, out
    assert res["contradictions"] == [] and res["incomplete"] == []
    assert res["verdicts"]["2015"]["verdict"] == "ny_local passt"


def test_fixed_est_failing_july_is_excluded_in_same_year(tmp_path, cfg, monkeypatch):
    """Kontroll-Ausschluss: faellt fixed_est im Juli 2012 durch, zaehlt es in den
    Umstellungsmonaten 2012 nicht mit - ein Wechsel fixed_est -> berlin mitten im
    Jahr ist so nicht belegbar (2012-03: keine nicht ausgeschlossene Regel passt)."""
    truth = lambda ym: "fixed_est" if ym < (2012, 6) else "berlin_minus_6"
    monkeypatch.setattr(fh, "evaluate_raw_month", fake_evaluator(truth))
    root = tmp_path / "data"
    for m in (1, 3, 7, 10):
        touch_cache(root, "XAUUSD", (2012, m))
    res, out = run(tmp_path, years=[2012], offline=True, **NO_MONTHS)
    by = {m["month"]: m for m in res["months"]}
    assert not by["2012-03"]["decisive"]
    assert by["2012-03"]["text"] == "keine Regel passt | fixed_est ausgeschlossen (2012-07: +1.0 h)"
    assert by["2012-10"]["winner"] == "berlin_minus_6"
    assert res["exclusions"] == {"2012": {"XAUUSD": {"fixed_est": "2012-07: +1.0 h"}}}
    assert "fixed_est (2012-07: +1.0 h)" in out
    assert by["2012-01"]["text"] == \
        "Kontrolle ok: berlin_minus_6, ny_local, fixed_est, berlin_minus_6_utc_midnight"
    # ein entscheidender Monat eines Symbols: zu wenig Evidenz, kein Vorschlag
    assert res["schedule"] == [] and [(b["rule"], b["months"]) for b in res["weak_evidence"]] == \
        [("berlin_minus_6", ["2012-10"])]
    assert res["exit_code"] == 4                             # berlin faellt 2012-03 durch


def test_no_rule_passes_and_control_failure(tmp_path, cfg, monkeypatch):
    def ev(data_root, symbol, ym, mode="tick", rule=None, **kw):
        return {"status": "failed", "cached": True, "median_deviation_h": 1.0, "anchors": 30}
    monkeypatch.setattr(fh, "evaluate_raw_month", ev)
    touch_cache(tmp_path / "data", "XAUUSD", (2010, 1))
    touch_cache(tmp_path / "data", "XAUUSD", (2010, 3))
    res, out = run(tmp_path, years=[2010], months=[3], controls=[1], offline=True)
    v = res["verdicts"]["2010"]
    assert v["verdict"] == "keine Regel passt"
    assert any("Kontrolle gescheitert" in n for n in v["notes"])
    assert "Kein Fahrplan-Vorschlag" in out
    # die konfigurierte Regel faellt durch, auch wenn keine andere besteht -> Exit 4
    assert res["exit_code"] == 4
    assert {(c["month"], c["configured"]) for c in res["configured_failed"]} == \
        {("2010-01", "berlin_minus_6"), ("2010-03", "berlin_minus_6")}
    assert "KONFIGURIERTE REGEL FAELLT DURCH" in out


def test_symbols_disagree_month_left_out_of_schedule(tmp_path, cfg, monkeypatch):
    base = fake_evaluator(lambda ym: "berlin_minus_6")
    other = fake_evaluator(lambda ym: "ny_local")
    monkeypatch.setattr(fh, "evaluate_raw_month",
                        lambda d, s, ym, mode="tick", rule=None, **kw:
                        (other if s == "XAGUSD" else base)(d, s, ym, mode, rule, **kw))
    for s in ("XAUUSD", "XAGUSD"):
        touch_cache(tmp_path / "data", s, (2018, 3))
    res, out = run(tmp_path, symbols=["XAUUSD", "XAGUSD"], years=[2018], months=[3],
                   controls=[], offline=True)
    assert res["symbol_conflicts"] == [
        "2018-03: KONFLIKT (Symbole: XAUUSD berlin_minus_6, XAGUSD ny_local)"]
    assert res["schedule"] == []
    assert res["exit_code"] == 4                             # XAGUSD widerspricht dem Fahrplan


def test_not_checkable_month_is_not_decisive(tmp_path, cfg, monkeypatch):
    monkeypatch.setattr(fh, "evaluate_raw_month", fake_evaluator(
        lambda ym: "ny_local", not_checkable={("XAUUSD", (2012, 3), "berlin_minus_6")}))
    touch_cache(tmp_path / "data", "XAUUSD", (2012, 3))
    res, out = run(tmp_path, years=[2012], months=[3], controls=[], offline=True)
    m = res["months"][0]
    assert not m["decisive"] and res["exit_code"] == 0 and res["schedule"] == []
    assert "not_checkable" in out


# ---------------------------------------------------------------- 429 ---
def test_429_stops_cleanly_with_exit_3_and_partial_results(tmp_path, cfg, monkeypatch):
    monkeypatch.setattr(fh, "evaluate_raw_month", fake_evaluator(lambda ym: "berlin_minus_6"))
    site = Site(post_status={2: 429})                        # 2. Datei: POST -> 429
    clock = Clock()
    res, out = run(tmp_path, site, clock, years=[2016], **NO_MONTHS)
    assert res["exit_code"] == 3
    assert res["stopped"].startswith("rate_limited")
    # GET+POST (Datei 1), GET+POST 429 (Datei 2), danach nichts mehr
    assert [c[0] for c in site.calls] == ["GET", "POST", "GET", "POST"]
    st = {m["month"]: m["state"] for m in res["months"]}
    assert st == {"2016-01": "cached", "2016-03": "skipped", "2016-07": "skipped",
                  "2016-10": "skipped"}
    jan = next(m for m in res["months"] if m["month"] == "2016-01")
    assert jan["evaluated"] and jan["results"]["berlin_minus_6"]["status"] == "ok"   # Teilergebnis
    assert "Downloads abgebrochen: rate_limited" in out
    state = json.loads(fh.ratelimit_state_path(tmp_path / "data").read_text())
    assert state["blocked_until"] >= clock.t                 # Sperre fuer alle Prozesse gesetzt
    # zweiter Aufruf waehrend der Sperre: kein einziger Request, wieder Exit 3
    site2 = Site()
    res2, _ = run(tmp_path, site2, clock, years=[2016], **NO_MONTHS)
    assert site2.requests == 0 and res2["exit_code"] == 3
    # nach Ablauf der Sperre: Rest wird geladen
    clock.t += 3600
    site3 = Site()
    res3, _ = run(tmp_path, site3, clock, years=[2016], **NO_MONTHS)
    assert site3.requests == 6 and res3["exit_code"] == 0


def test_locked_symbol_is_incomplete(tmp_path, cfg, monkeypatch):
    monkeypatch.setattr(fh, "evaluate_raw_month", fake_evaluator(lambda ym: "berlin_minus_6"))
    lk = fh.acquire_lock(tmp_path / "data", "XAUUSD")
    try:
        site = Site()
        res, out = run(tmp_path, site, years=[2016], months=[3], controls=[])
    finally:
        lk.close()
    assert site.requests == 0 and res["exit_code"] == 3
    assert res["months"][0]["state"] == "locked"


# ---------------------------------------------------------------- cli ---
def test_cli_offline_json(tmp_path, cfg, monkeypatch):
    monkeypatch.setattr(fh, "evaluate_raw_month", fake_evaluator(lambda ym: "berlin_minus_6"))
    monkeypatch.setenv("MARKETDATA_DATA_ROOT", str(tmp_path / "data"))
    touch_cache(tmp_path / "data", "XAUUSD", (2024, 3))
    touch_cache(tmp_path / "data", "XAUUSD", (2024, 10))     # 2 Belege: Block nicht schwach
    out = tmp_path / "survey.json"
    code = sv.main(["--symbols", "XAUUSD", "--years", "2024", "--months", "3", "10", "--controls",
                    "--offline", "--json", str(out)])
    res = json.loads(out.read_text())
    assert code == res["exit_code"] == 0
    assert res["rules"] == list(fh.TZ_RULES)
    with pytest.raises(SystemExit):
        sv.main(["--rules", "gibt_es_nicht", "--offline"])


# --------------------------------------------- integration (echte Pruefung) ---
def write_raw_month(root, sym, ym, rule, step_s=60, extra_utc=()):
    """Synthetische Ticks eines Rohmonats (New-York-Marktzeiten, Tagespause
    17-18 Uhr), gestempelt nach `rule`, als Roh-ZIP in den Cache. extra_utc:
    zusaetzliche Ticks (UTC-Zeitpunkte), in Dateireihenfolge = UTC-Reihenfolge."""
    lo = pd.Timestamp(*ym, 1, tz="UTC") - pd.Timedelta(days=1)
    hi = pd.Timestamp(*ym, 1, tz="UTC") + pd.offsets.MonthBegin(1) + pd.Timedelta(days=1)
    idx = pd.date_range(lo, hi, freq=f"{step_s}s", inclusive="left") + pd.Timedelta(milliseconds=137)
    ny = idx.tz_convert("America/New_York")
    wd, h = ny.dayofweek, ny.hour + ny.minute / 60
    closed = ((wd == 4) & (h >= 17)) | (wd == 5) | ((wd == 6) & (h < 18)) | \
        ((wd <= 3) & (h >= 17) & (h < 18))
    utc = idx[~np.asarray(closed)].as_unit("ms").asi8
    if len(extra_utc):
        utc = np.sort(np.concatenate([utc, pd.DatetimeIndex(extra_utc).as_unit("ms").asi8]))
    raw = fh.utc_to_raw_ms(utc, fh.TZ_RULES[rule])
    a, b = fh.raw_month_bounds_ms(ym)
    raw = raw[(raw >= a) & (raw < b)]
    stamp = pd.to_datetime(raw, unit="ms").strftime("%Y%m%d %H%M%S%f").str[:-3]
    u = fh.Unit(sym, "tick", *ym)
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(u.member, "".join(f"{s},2000.10000,2000.35000,0\n" for s in stamp))
        zf.writestr(u.member[:-4] + ".txt", "HistData.com status report\n")
    p = u.path(root)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(buf.getvalue())


def test_integration_with_synthetic_raw_zips(tmp_path, cfg):
    root = tmp_path / "data"
    write_raw_month(root, "XAUUSD", (2015, 3), "ny_local")
    write_raw_month(root, "XAUUSD", (2015, 7), "ny_local")
    write_raw_month(root, "XAUUSD", (2024, 10), "berlin_minus_6")
    write_raw_month(root, "XAUUSD", (2024, 7), "berlin_minus_6")
    res, out = run(tmp_path, years=[2015, 2024], months=[3, 10], controls=[7], offline=True)
    by = {m["month"]: m for m in res["months"]}
    assert by["2015-03"]["winner"] == "ny_local"
    assert by["2015-03"]["results"]["berlin_minus_6"]["status"] == "failed"
    assert by["2024-10"]["winner"] == "berlin_minus_6"
    assert by["2015-07"]["control_ok"] and by["2024-07"]["control_ok"]
    assert by["2015-07"]["results"]["fixed_est"]["status"] == "failed"
    # je Block nur ein entscheidender Monat eines Symbols -> kein Vorschlag (zu wenig Evidenz)
    assert res["schedule"] == []
    assert {b["rule"] for b in res["weak_evidence"]} == {"ny_local", "berlin_minus_6"}
    assert res["verdicts"]["2015"]["verdict"] == "nur ny_local passt"
    assert res["verdicts"]["2024"]["verdict"] == "berlin_minus_6 passt"
    assert res["exit_code"] == 4                             # 2015-03 widerspricht berlin_minus_6
    assert {m["month"] for m in res["incomplete"]} == {"2015-10", "2024-03"}


def test_integration_weekend_reversal_dropped_and_schedule(tmp_path, cfg):
    """Echte Pruefung: verirrte Ticks in der EU-Umstellungsnacht 2024 (UTC So 27.10.
    00:30-01:30; unter berlin_minus_6 roh Sa 20:30, 20:59, dann zurueck auf 20:00)
    werden verworfen, 2024-10 bleibt entscheidend berlin_minus_6 (vorher "keine
    Regel passt"); Vorschlag mit undecided zwischen 2018 Okt/Nov und 2021 Maerz."""
    root = tmp_path / "data"
    stray = [pd.Timestamp(t, tz="UTC") for t in ("2024-10-27 00:30:01", "2024-10-27 00:59:02",
                                                 "2024-10-27 01:00:05", "2024-10-27 01:30:07")]
    for ym in [(2018, 3), (2018, 10)]:
        write_raw_month(root, "XAUUSD", ym, "ny_local")
    write_raw_month(root, "XAUUSD", (2021, 3), "berlin_minus_6")
    write_raw_month(root, "XAUUSD", (2024, 10), "berlin_minus_6", extra_utc=stray)
    res, out = run(tmp_path, years=[2018, 2021, 2024], months=[3, 10], controls=[], offline=True)
    oct24 = next(m for m in res["months"] if m["month"] == "2024-10")
    assert oct24["winner"] == "berlin_minus_6", out
    assert oct24["results"]["berlin_minus_6"]["dropped_weekend_ticks"] == 4
    assert "ok +0.0 (4 Wochenend-Ticks verworfen)" in out
    rev = res["reversals"]["XAUUSD"]
    assert len(rev) == 1 and rev[0].startswith(
        "2024-10: 2024-10-26 20:59:02 -> 2024-10-26 20:00:05 (59.0 min, 4 Ticks, "
        "Wochenende (Markt zu), verworfen"), rev
    # letzter ny-Beleg 2018-10 (2018-11 nicht untersucht, hat Fenster) -> undecided ab 2018-11
    assert [(e["from"], e["rule"]) for e in res["schedule"]] == [
        ("2000-01", "ny_local"), ("2018-11", "undecided"), ("2021-03", "berlin_minus_6")], out
    assert res["switch_analysis"]["last_old"] == "2018-10"
    assert res["exit_code"] == 4                             # konfiguriert berlin, 2018 faellt durch


# ------------------------------------------------ Review v5: Evidenz, Exit ---
def test_configured_rule_failing_where_two_others_pass_is_exit_4(tmp_path, cfg, monkeypatch):
    """Review v5 (2): konfigurierte Regel faellt durch, zwei andere bestehen
    (kein entscheidender Monat) -> trotzdem Exit 4 mit Liste."""
    cfg((("2000-01", "fixed_est"),))
    monkeypatch.setattr(fh, "evaluate_raw_month", fake_evaluator(lambda ym: "berlin_minus_6"))
    touch_cache(tmp_path / "data", "XAUUSD", (2018, 7))       # Juli: berlin = ny, fixed_est faellt
    res, out = run(tmp_path, years=[2018], months=[], controls=[7], offline=True)
    m = res["months"][0]
    assert not m["decisive"] and set(m["passing"]) == {"berlin_minus_6", "ny_local",
                                                          "berlin_minus_6_utc_midnight"}
    assert res["contradictions"] == [] and res["exit_code"] == 4
    assert [(c["month"], c["configured"]) for c in res["configured_failed"]] == [("2018-07", "fixed_est")]


def test_configured_rule_outside_rules_selection_is_still_checked(tmp_path, cfg, monkeypatch):
    calls = []
    monkeypatch.setattr(fh, "evaluate_raw_month", fake_evaluator(lambda ym: "ny_local", calls))
    touch_cache(tmp_path / "data", "XAUUSD", (2018, 3))
    res, out = run(tmp_path, years=[2018], months=[3], controls=[], offline=True,
                   rules=["ny_local", "fixed_est"])
    assert ("XAUUSD", (2018, 3), "berlin_minus_6") in calls     # konfiguriert, nicht gewaehlt
    assert res["exit_code"] == 4 and res["configured_failed"][0]["configured"] == "berlin_minus_6"


def test_block_needs_two_months_or_both_symbols():
    """Review v5 (3): ein einzelner entscheidender Monat eines Symbols reicht nicht."""
    _, an, sched, _ = analyse([mrow("XAUUSD", (2012, 3), "ny_local")])
    assert sched == [] and an["weak"][0]["months"] == ["2012-03"]
    _, an, sched, _ = analyse([mrow("XAUUSD", (2012, 3), "ny_local"),
                               mrow("XAGUSD", (2012, 3), "ny_local")])
    assert [(e["from"], e["rule"]) for e in sched] == [("2000-01", "ny_local")]
    _, an, sched, _ = analyse([mrow("XAUUSD", (2012, 3), "ny_local"),
                               mrow("XAUUSD", (2012, 10), "ny_local"),
                               mrow("XAUUSD", (2016, 3), "berlin_minus_6")])
    assert an["switches"] == 1 and [w["rule"] for w in an["weak"]] == ["berlin_minus_6"]
    assert sched == []                                           # berlin: 1 Monat


# ------------------------------------- v6: Umstellungsperioden, Wechsel ---
def mrow(sym, ym, winner=None, *, state="cached", text=None, dropped=0):
    """Synthetisches Monatsergebnis wie in survey(): winner = entscheidende Regel;
    ohne winner nicht entscheidend (text) bzw. nicht ausgewertet (state)."""
    r = {"symbol": sym, "ym": ym, "month": sv.ymstr(ym), "kind": "transition", "state": state,
         "evaluated": state == "cached", "decisive": winner is not None, "winner": winner,
         "results": {}, "passing": [winner] if winner else []}
    if winner:
        r["results"] = {n: {"status": "ok" if n == winner else "failed",
                            "dropped_weekend_ticks": dropped if n == winner else 0}
                        for n in ("berlin_minus_6", "ny_local", "fixed_est")}
        r["text"] = f"entscheidend: {winner}"
    elif state == "cached":
        r["text"] = text or "mehrere passen: berlin_minus_6, ny_local"
    else:
        r["text"] = text or "nicht geladen"
    return r


def both(ym, winner=None, **kw):
    return [mrow(s, ym, winner, **kw) for s in sv.SYMBOLS]


def ny_years(*years):
    return [r for y in years for m in (3, 10, 11) for r in both((y, m), "ny_local")]


def berlin_years(*years):
    return [r for y in years for m in (3, 10, 11) for r in both((y, m), "berlin_minus_6")]


def analyse(rows):
    months = sv.build_months(rows, sv.SYMBOLS)
    an = sv.analyze_switches(months, fh.TZ_RULES)
    sched, just = sv.propose_schedule(an, (2000, 1), fh.TZ_RULES)
    return sv.build_periods(rows, sv.SYMBOLS, months), an, sched, just


def entries(sched):
    return [(e["from"], e["rule"]) for e in sched]


def test_one_switch_with_gap_gives_three_entries_with_undecided():
    rows = ny_years(2017, 2018) + berlin_years(2021, 2022)
    rows += both((2019, 3), state="over_limit")                     # fehlt
    rows += both((2019, 10)) + both((2019, 11), text="keine Regel passt")   # nicht entscheidend
    periods, an, sched, just = analyse(rows)
    assert an["ok"] and an["switches"] == 1 and an["conflicts"] == []
    assert (an["last_old"], an["first_new"]) == ("2018-11", "2021-03")
    # JEDER Rohmonat dazwischen, auch nicht untersuchte; Fenster nur in 03/10/11
    assert [b["month"] for b in an["between"]][:3] == ["2018-12", "2019-01", "2019-02"]
    assert an["undecided_months"] == ["2019-03", "2019-10", "2019-11", "2020-03", "2020-10",
                                      "2020-11"]
    by = {b["month"]: b for b in an["between"]}
    assert by["2020-03"]["state"] == "nicht untersucht" and "fehlt" in by["2019-03"]["state"]
    assert not by["2019-07"]["window"] and by["2019-07"]["windows"] == ""
    assert by["2019-03"]["windows"] == "10.03. 02:00-30.03. 21:00 roh", by["2019-03"]
    assert [{k: e.get(k) for k in ("from", "rule", "candidates")} for e in sched] == [
        {"from": "2000-01", "rule": "ny_local", "candidates": None},
        {"from": "2018-12", "rule": "undecided", "candidates": ["ny_local", "berlin_minus_6"]},
        {"from": "2021-03", "rule": "berlin_minus_6", "candidates": None}]
    assert just["equivalent"] and just["uncovered"] == [] and just["undecided"] == ["2018-12", "2021-02"]
    assert set(just["window_months"]) == set(an["undecided_months"])
    assert just["plain_months"][:3] == ["2018-12", "2019-01", "2019-02"]
    # YAML ist eine gueltige Config (Parser des Fetchers, undecided mit Kandidaten)
    import yaml
    txt = sv.schedule_yaml(sched, comment=sv.proposal_comment(periods, an, just, sv.SYMBOLS, "x"))
    parsed = fh.build_tz_config({"rules": [], **{"schedule": {
        **yaml.safe_load(txt)["schedule"], "m1": [{"from": "2000-01", "rule": "fixed_est"}]}}})
    assert fh.rule_for("tick", (2018, 11), parsed).name == "ny_local"
    und = fh.rule_for("tick", (2019, 6), parsed)
    assert isinstance(und, fh.UndecidedRule) and [c.name for c in und.candidates] == \
        ["ny_local", "berlin_minus_6"]
    assert fh.rule_for("tick", (2021, 3), parsed).name == "berlin_minus_6"
    assert "# Begruendung:" in txt and "#   2019 Maerz" in txt


def test_switch_at_raw_november_with_undecisive_november():
    """Review v6 HIGH 1: wahrer Wechsel ab Rohmonat 2019-11, 2019-11 nicht entscheidend
    (nur eine Fenster-Luecke) -> ny bis 2019-10, undecided ab 2019-11, berlin ab 2020-03.
    Vorher: Periode "2019 Okt/Nov" galt als ny_local bestaetigt, Exit 0."""
    rows = ny_years(2018) + both((2019, 3), "ny_local") + both((2019, 10), "ny_local") + \
        both((2019, 11)) + berlin_years(2020, 2021)
    periods, an, sched, just = analyse(rows)
    assert an["ok"] and an["switches"] == 1
    assert (an["last_old"], an["first_new"]) == ("2019-10", "2020-03")
    assert an["undecided_months"] == ["2019-11"]
    assert entries(sched) == [("2000-01", "ny_local"), ("2019-11", "undecided"),
                              ("2020-03", "berlin_minus_6")]
    assert just["equivalent"] and just["undecided"] == ["2019-11", "2020-02"]
    assert next(p for p in periods if p["label"] == "2019 Okt/Nov")["text"] == \
        "ny_local (XAUUSD, XAGUSD)"                               # Anzeige: nur Oktober belegt


def test_switch_mirror_october_not_decisive():
    """Spiegelfall (EU-Ende 30./31.10.): Oktober nicht entscheidend, November allein."""
    rows = ny_years(2020) + both((2021, 3), "ny_local") + both((2021, 10)) + \
        both((2021, 11), "berlin_minus_6") + berlin_years(2022)
    _, an, sched, just = analyse(rows)
    assert (an["last_old"], an["first_new"]) == ("2021-03", "2021-11")
    assert an["undecided_months"] == ["2021-10"]
    assert entries(sched) == [("2000-01", "ny_local"), ("2021-04", "undecided"),
                              ("2021-11", "berlin_minus_6")]
    assert just["plain_months"] == [f"2021-{m:02d}" for m in range(4, 10)]


def test_switch_exactly_at_10_11_boundary():
    """Oktober ny_local, November berlin_minus_6, beide entscheidend: gueltiger
    Wechsel an der Rohmonatsgrenze 10/11, kein undecided, kein Konflikt."""
    rows = ny_years(2018) + both((2019, 3), "ny_local") + both((2019, 10), "ny_local") + \
        both((2019, 11), "berlin_minus_6") + berlin_years(2020)
    periods, an, sched, just = analyse(rows)
    assert an["ok"] and an["switches"] == 1 and an["conflicts"] == [] and an["between"] == []
    assert entries(sched) == [("2000-01", "ny_local"), ("2019-11", "berlin_minus_6")]
    assert just["undecided"] is None and just["equivalent"]
    p = next(p for p in periods if p["label"] == "2019 Okt/Nov")
    assert p["status"] == "split" and p["text"] == "Wechsel: 10 ny_local -> 11 berlin_minus_6"
    assert p["symbols"]["XAUUSD"]["cell"] == "10: ny_local -> 11: berlin_minus_6"


def test_one_switch_without_gap_two_entries():
    # 2020-11 ny, 2021-03 berlin: dazwischen nur Monate ohne Fenster -> kein undecided
    _, an, sched, just = analyse(ny_years(2019, 2020) + berlin_years(2021))
    assert an["undecided_months"] == [] and (an["last_old"], an["first_new"]) == ("2020-11", "2021-03")
    # ohne Fenster dazwischen: neue Regel ab dem Monat nach dem letzten alten Beleg
    assert entries(sched) == [("2000-01", "ny_local"), ("2020-12", "berlin_minus_6")]
    assert just["plain_months"] == ["2020-12", "2021-01", "2021-02"] and just["undecided"] is None
    # Maerz ny, Okt/Nov berlin im selben Jahr
    rows = ny_years(2020) + both((2021, 3), "ny_local") + both((2021, 10), "berlin_minus_6") + \
        both((2021, 11), "berlin_minus_6") + berlin_years(2022)
    _, an, sched, just = analyse(rows)
    assert entries(sched) == [("2000-01", "ny_local"), ("2021-04", "berlin_minus_6")]
    assert just["plain_months"] == [f"2021-{m:02d}" for m in range(4, 10)]


def test_justify_rejects_window_month_outside_undecided_block():
    """justify prueft wirklich: ein Fenster-Monat ausserhalb des undecided-Blocks
    -> nicht gleichwertig (keine Ausnahme mehr fuer Maerz/Oktober/November)."""
    _, an, _, _ = analyse(ny_years(2018) + both((2019, 10)) + berlin_years(2020))
    assert an["undecided_months"] == ["2019-03", "2019-10", "2019-11"]
    an["undecided_months"] = []                    # Block kuenstlich entfernt
    just = sv.justify(an, fh.TZ_RULES)
    assert not just["equivalent"] and just["uncovered"] == ["2019-03", "2019-10", "2019-11"]
    assert sv.month_windows(fh.TZ_RULES["ny_local"], fh.TZ_RULES["berlin_minus_6"], (2019, 7)) == ()
    assert sv.month_windows(fh.TZ_RULES["ny_local"], fh.TZ_RULES["berlin_minus_6"], (2019, 11))


def test_two_switches_no_proposal():
    rows = ny_years(2010) + berlin_years(2012) + ny_years(2015)
    _, an, sched, _ = analyse(rows)
    assert an["switches"] == 2 and not an["ok"] and sched == []
    assert "2 Wechsel statt hoechstens einem" in an["explanation"][0]
    assert "ny_local (2010-03..2010-11) -> berlin_minus_6 (2012-03..2012-11)" in an["explanation"][0]
    # umgekehrter Split in einer Periode (10 berlin, 11 ny) zwischen ny-Jahren: 2 Wechsel
    rows = ny_years(2017) + both((2018, 10), "berlin_minus_6") + both((2018, 11), "ny_local")
    _, an, sched, _ = analyse(rows)
    assert an["switches"] == 2 and sched == []


def test_two_switches_exit_4_even_when_config_matches(tmp_path, cfg, monkeypatch):
    cfg((("2000-01", "ny_local"), ("2012-01", "berlin_minus_6"), ("2015-01", "ny_local")))
    truth = lambda ym: "berlin_minus_6" if 2012 <= ym[0] < 2015 else "ny_local"
    monkeypatch.setattr(fh, "evaluate_raw_month", fake_evaluator(truth))
    for y in (2010, 2012, 2015):
        for m in (3, 10):
            for s in sv.SYMBOLS:
                touch_cache(tmp_path / "data", s, (y, m))
    res, out = run(tmp_path, symbols=list(sv.SYMBOLS), years=[2010, 2012, 2015], months=[3, 10],
                   controls=[], offline=True)
    assert res["configured_failed"] == [] and res["incomplete"] == []
    assert res["exit_code"] == 4 and res["schedule"] == [] and "schedule:" not in out
    assert res["switch_analysis"]["switches"] == 2
    assert "Kein Fahrplan-Vorschlag: 2 Wechsel statt hoechstens einem" in out


def test_symbol_conflict_is_conflict():
    rows = ny_years(2015) + [mrow("XAUUSD", (2018, 3), "ny_local"),
                             mrow("XAGUSD", (2018, 3), "berlin_minus_6")] + berlin_years(2021)
    periods, an, sched, _ = analyse(rows)
    p = next(p for p in periods if p["label"] == "2018 Maerz")
    assert p["status"] == "conflict"
    assert p["text"] == "03: KONFLIKT (Symbole: XAUUSD ny_local, XAGUSD berlin_minus_6)"
    assert not an["ok"] and sched == [] and an["conflicts"][0]["month"] == "2018-03"
    # nur ein Symbol entscheidend, das andere offen: kein Konflikt
    rows = [mrow("XAUUSD", (2018, 3), "ny_local"), mrow("XAGUSD", (2018, 3), state="over_limit")]
    m = sv.build_months(rows, sv.SYMBOLS)[0]
    assert m["status"] == "decisive" and m["rule"] == "ny_local" and m["support"] == ["XAUUSD"]
    # Symbole in VERSCHIEDENEN Rohmonaten einer Periode (Gold 10 ny, Silber 11 berlin): Wechsel 10/11
    rows = ny_years(2018) + [mrow("XAUUSD", (2019, 10), "ny_local"),
                             mrow("XAGUSD", (2019, 11), "berlin_minus_6")] + berlin_years(2020)
    _, an, sched, _ = analyse(rows)
    assert an["ok"] and entries(sched) == [("2000-01", "ny_local"), ("2019-11", "berlin_minus_6")]


def test_undecisive_month_does_not_contradict():
    rows = [mrow("XAUUSD", (2018, 10), "ny_local"),
            mrow("XAUUSD", (2018, 11), text="keine Regel passt")]
    p = sv.build_periods(rows, ["XAUUSD"])[0]
    assert p["status"] == "confirmed" and p["symbols"]["XAUUSD"]["cell"] == "ny_local (10)"
    assert sv.build_months(rows, ["XAUUSD"])[1]["status"] == "open"


def test_weekend_dropped_month_counts_as_decisive(tmp_path, cfg, monkeypatch):
    flags = []
    monkeypatch.setattr(fh, "evaluate_raw_month", fake_evaluator(
        lambda ym: "berlin_minus_6", drop_flags=flags,
        dropped={("XAUUSD", (2021, 10), "berlin_minus_6"): 3}))
    touch_cache(tmp_path / "data", "XAUUSD", (2021, 10))
    touch_cache(tmp_path / "data", "XAUUSD", (2021, 3))
    res, out = run(tmp_path, years=[2021], months=[3, 10], controls=[], offline=True)
    assert flags and all(flags)                                  # immer drop_releasable_weekend=True
    m = res["months"][1]
    assert m["decisive"] and m["winner"] == "berlin_minus_6"
    assert m["results"]["berlin_minus_6"]["dropped_weekend_ticks"] == 3
    assert "ok +0.0 (3 Wochenend-Ticks verworfen)" in out
    assert res["switch_analysis"]["blocks"][0]["decisive_months"] == \
        {"2021-03": ["XAUUSD"], "2021-10": ["XAUUSD"]}
    p = res["periods"][1]
    assert p["status"] == "confirmed"
    assert p["symbols"]["XAUUSD"]["cell"] == \
        "berlin_minus_6 (10) [10: 3 Wochenend-Ticks verworfen]"
    assert res["exit_code"] == 0, out


def test_large_reversals_are_listed(tmp_path, cfg, monkeypatch):
    base = fake_evaluator(lambda ym: "berlin_minus_6")

    def ev(*a, **kw):
        rep = base(*a, **kw)
        rep["large_reversals"] = [{"at_raw": "2021-10-30 21:00:01", "back_to_raw": "2021-10-30 20:00:03",
                                   "minutes": 60.0, "n_ticks": 4, "weekend_closed": True}]
        return rep
    monkeypatch.setattr(fh, "evaluate_raw_month", ev)
    touch_cache(tmp_path / "data", "XAUUSD", (2021, 10))
    res, out = run(tmp_path, years=[2021], months=[10], controls=[], offline=True)
    line = "2021-10: 2021-10-30 21:00:01 -> 2021-10-30 20:00:03 (60.0 min, 4 Ticks, " \
        "Wochenende (Markt zu))"
    assert line in out and res["reversals"]["XAUUSD"] == [line]


def test_undecided_months_are_open_not_failures(tmp_path, cfg, monkeypatch):
    p = cfg((("2000-01", "ny_local"),))
    p.write_text(p.read_text().replace(
        '    - {from: "2000-01", rule: ny_local}\n',
        '    - {from: "2000-01", rule: ny_local}\n'
        '    - {from: "2018-12", rule: undecided, candidates: [ny_local, berlin_minus_6]}\n'
        '    - {from: "2021-03", rule: berlin_minus_6}\n'))
    truth = lambda ym: "ny_local" if ym < (2019, 6) else "berlin_minus_6"
    monkeypatch.setattr(fh, "evaluate_raw_month", fake_evaluator(truth))
    for ym in [(2018, 10), (2019, 3), (2019, 10), (2021, 3)]:
        touch_cache(tmp_path / "data", "XAUUSD", ym)
    res, out = run(tmp_path, years=[2018, 2019, 2021], months=[3, 10], controls=[], offline=True,
                   max_downloads=0)
    by = {m["month"]: m for m in res["months"]}
    assert by["2019-10"]["configured"] == "undecided" and by["2019-10"]["winner"] == "berlin_minus_6"
    assert by["2019-03"]["configured_candidates"] == ["ny_local", "berlin_minus_6"]
    assert res["contradictions"] == [] and res["configured_failed"] == []
    assert "Offen laut Fahrplan (undecided, kein Fehler): 2 Rohmonate (2019-03, 2019-10)" in out
    row = next(x for x in out.splitlines() if x.startswith("2019-10"))
    assert " offen " in row and "WIDERSPRUCH" not in row
    # Vorschlag: ny bis 2019-03, berlin ab 2019-04 (dazwischen kein Fenster: kein undecided)
    assert [(e["from"], e["rule"]) for e in res["schedule"]] == [
        ("2000-01", "ny_local"), ("2019-04", "berlin_minus_6")]
    assert res["exit_code"] == 3                     # 2018-03, 2021-10 fehlen (nicht im Cache)


# ------------------------------------------------------- v6: Planung ---
def test_preset_full_and_periods_resolve():
    years, months, controls = sv.resolve_plan("full", None, None, None, True)
    assert years == list(range(2009, 2026)) and months == [3, 10, 11] and controls == [1, 7]
    # explizite Angaben schlagen das Preset
    assert sv.resolve_plan("full", [2019], None, [], False) == ([2019], [3, 10, 11], [])
    # nur --periods: keine Default-Jahre
    assert sv.resolve_plan(None, None, None, None, True)[0] == []
    assert sv.resolve_plan(None, None, None, None, False) == (
        list(sv.DEFAULT_YEARS), list(sv.DEFAULT_MONTHS), list(sv.DEFAULT_CONTROLS))
    plan = sv.plan_months([], [3], [], periods=[(2019, 10), (2019, 3)])
    assert plan == [((2019, 3), "transition"), ((2019, 10), "transition")]
    # Periode doppelt mit --years: nur einmal; Kontrolle geht vor
    plan = sv.plan_months([2019], [3], [7], periods=[(2019, 3), (2019, 7)])
    assert plan == [((2019, 3), "transition"), ((2019, 7), "control")]
    with pytest.raises(ValueError):
        sv.parse_ym("2019-13")


def test_preset_full_request_count_and_remaining(tmp_path, cfg, monkeypatch):
    """--preset full mit Kontrollmonaten 01/07: Lage nach dem erweiterten Scan (alle
    03/10/11 im Cache) und dem ersten Survey (01/07 der Jahre 2010/12/15/18/21/24)."""
    monkeypatch.setattr(fh, "evaluate_raw_month", fake_evaluator(lambda ym: "berlin_minus_6"))
    root = tmp_path / "data"
    first = (2010, 2012, 2015, 2018, 2021, 2024)
    for y in range(2009, 2026):
        for m in (3, 10, 11) + ((1, 7) if y in first else ()):
            for s in sv.SYMBOLS:
                if not fh.before_history(s, (y, m)):
                    touch_cache(root, s, (y, m))
    years, months, controls = sv.resolve_plan("full", None, None, None, True)
    assert controls == [1, 7]
    site = Site()
    clock = Clock()
    out = io.StringIO()
    res = sv.survey(sv.SYMBOLS, years, months, controls, data_root=root,
                    periods=[(2019, 3), (2019, 10), (2019, 11), (2020, 3), (2020, 10), (2020, 11)],
                    now=lambda: datetime(2026, 11, 6, tzinfo=UTC), pause=0.0, out=out,
                    max_downloads=3, session=site, sleep=clock.sleep, clock=clock, wall=clock)
    txt = out.getvalue()
    # 17 Jahre x 5 Monate x 2 Symbole = 170; vor Historienbeginn: XAUUSD 2009-01, XAGUSD
    # 2009-01/03; im Cache 101 (03/10/11) + 24 (01/07 erster Survey); fehlen 42 = 84 Requests
    assert "Plan: 85 Rohmonate x 2 Symbole = 170 Rohdateien (34 Umstellungsperioden), " \
        "125 im Cache, 42 fehlen" in txt
    assert "Geplant: 3 Rohmonate laden = 6 Requests" in txt
    assert "39 weitere ueber --max-downloads 3 (naechster Aufruf, 78 Requests)" in txt
    assert site.requests == 6 and res["requests"] == 6
    assert res["exit_code"] == 3 and res["remaining_raw_months"] == 39   # nichts widerspricht
    assert "noch fehlend: 39 Rohmonate" in txt
    assert {m["month"][-2:] for m in res["months"] if m["kind"] == "control"} == {"01", "07"}


def test_cli_markdown_and_json(tmp_path, cfg, monkeypatch):
    cfg((("2000-01", "ny_local"), ("2018-12", "berlin_minus_6")))
    monkeypatch.setattr(fh, "evaluate_raw_month", fake_evaluator(
        lambda ym: "ny_local" if ym < (2019, 1) else "berlin_minus_6"))
    monkeypatch.setenv("MARKETDATA_DATA_ROOT", str(tmp_path / "data"))
    for y in (2018, 2021):
        for m in (3, 10, 11):
            for s in sv.SYMBOLS:
                touch_cache(tmp_path / "data", s, (y, m))
    md, js = tmp_path / "rep" / "s.md", tmp_path / "rep" / "s.json"
    code = sv.main(["--preset", "full", "--years", "2018", "2021", "--controls", "--periods",
                    "2019-03", "--offline", "--json", str(js), "--markdown", str(md)])
    res = json.loads(js.read_text())
    assert code == res["exit_code"] == 3                      # 2019-03 nicht im Cache
    text = md.read_text()
    assert text.startswith("# HistData-Zeitzonen-Survey (Tick)")
    assert "**Unvollstaendig:** 2 Rohmonate fehlen noch (4 Requests)" in text
    assert "| Periode | XAUUSD | XAGUSD | Gesamt |" in text
    assert "| 2019 Maerz | offen (03: fehlt) | offen (03: fehlt) | offen |" in text
    assert "| 2018 Okt/Nov | ny_local (10+11) | ny_local (10+11) | ny_local (XAUUSD, XAGUSD) |" in text
    assert "```yaml" in text
    assert '    - {from: "2018-12", rule: undecided, candidates: [ny_local, berlin_minus_6]}' in text
    assert '    - {from: "2021-03", rule: berlin_minus_6}' in text
    assert "## Begruendung" in text
    assert "Der Vorschlag fuehrt ny_local bis einschliesslich 2018-11 und berlin_minus_6 ab 2021-03" in text
    assert res["schedule"][1] == {"from": "2018-12", "rule": "undecided",
                                  "candidates": ["ny_local", "berlin_minus_6"]}
    assert res["switch_analysis"]["between"][0]["month"] == "2018-12"
    assert res["switch_analysis"]["undecided_months"][0] == "2019-03"


# ------------------------- v7: Kontroll-Ausschluss, Regelfamilien, Naechte ---
UM = "berlin_minus_6_utc_midnight"


def night(kind, status, date, **kw):
    """Eigene Umstellungsnacht einer Regel wie im Fetcher-Bericht (transition_nights)."""
    base = {"date": date, "kind": kind, "rule_transition": True, "status": status,
            "expected": "roh 19:59:59 -> 19:00:00" if kind == "fall" else "roh 18:59:59 -> 20:00:00",
            "last_before_raw": None, "first_after_raw": None, "reversal_found": False}
    base.update(kw)
    return base


def res_(status, nights_=()):
    return {"status": status, "transition_nights": list(nights_)}


def test_families_and_windows_of_the_new_rule():
    fam = sv.rule_families(fh.TZ_RULES)
    assert fam["berlin_minus_6"] == fam[UM] == frozenset({"berlin_minus_6", UM})
    assert fam["ny_local"] == frozenset({"ny_local"}) and fam["fixed_est"] == frozenset({"fixed_est"})
    b, u = fh.TZ_RULES["berlin_minus_6"], fh.TZ_RULES[UM]
    # Varianten unterscheiden sich nur an den EU-Sonntagen (bis Mo 00:00 UTC)
    assert sv._win_text(sv.month_windows(b, u, (2019, 10))) == "26.10. 20:00-27.10. 20:00 roh"
    assert sv.month_windows(b, u, (2019, 11)) == () and sv.month_windows(b, u, (2019, 7)) == ()


def test_2019_03_decided_via_july_control_exclusion(tmp_path, cfg, monkeypatch):
    """Eigentuemer-Lauf: 2019-03 fallen ny_local und berlin_minus_6 durch, es bestehen
    utc_midnight und fixed_est; fixed_est faellt im Juli 2019 durch (-4 h statt -5 h)
    -> ausgeschlossen -> 2019-03 entscheidend fuer berlin_minus_6_utc_midnight."""
    monkeypatch.setattr(fh, "evaluate_raw_month", fake_evaluator(lambda ym: UM))
    for ym in [(2019, 3), (2019, 7)]:
        touch_cache(tmp_path / "data", "XAUUSD", ym)
    res, out = run(tmp_path, years=[2019], months=[3], controls=[7], offline=True)
    mar = next(m for m in res["months"] if m["month"] == "2019-03")
    assert {n for n, r in mar["results"].items() if r["status"] == "ok"} == {UM, "fixed_est"}
    assert mar["decisive"] and mar["winner"] == UM and mar["allowed"] == [UM]
    assert mar["text"] == f"entscheidend: {UM} | fixed_est ausgeschlossen (2019-07: +1.0 h)"
    assert "fixed_est ausgeschlossen (2019-07: +1.0 h)" in out
    # ohne Kontrollmonat: nicht entscheidend
    res, _ = run(tmp_path, years=[2019], months=[3], controls=[], offline=True)
    assert not res["months"][0]["decisive"]
    assert res["months"][0]["text"].startswith("mehrere passen")


def test_october_fold_makes_utc_midnight_decisive():
    fam = sv.rule_families(fh.TZ_RULES)
    results = {"ny_local": res_("failed"), "fixed_est": res_("failed"),
               "berlin_minus_6": res_("ok", [night("fall", "no_data", "2019-10-27")]),
               UM: res_("ok", [night("fall", "ok", "2019-10-27", reversal_found=True,
                                     ticks_in_fold=4200)])}
    c = sv.classify(results, "transition", [], None, fam)
    assert c["decisive"] and c["allowed"] == [UM] and c["winner"] == UM
    assert f"{UM}: Nacht bestaetigt" in c["text"]
    # ohne Nachtbericht: nur die Familie ist belegt
    results[UM] = res_("ok")
    c = sv.classify(results, "transition", [], None, fam)
    assert c["decisive"] and c["allowed"] == ["berlin_minus_6", UM]
    assert c["winner"] == f"Familie berlin_minus_6/{UM}"
    # nicht-eigene Beobachtungen (rule_transition False) sind keine Evidenz
    results[UM] = res_("ok", [night("fall", "ok", "2019-10-27", rule_transition=False)])
    assert sv.classify(results, "transition", [], None, fam)["allowed"] == ["berlin_minus_6", UM]


def test_march_gap_ticks_exclude_the_variant():
    fam = sv.rule_families(fh.TZ_RULES)
    results = {"ny_local": res_("failed"), "fixed_est": res_("failed"),
               "berlin_minus_6": res_("ok"),
               UM: res_("ok", [night("spring", "unexpected", "2019-03-31", ticks_in_gap=950)])}
    c = sv.classify(results, "transition", [], None, fam)
    assert c["decisive"] and c["allowed"] == ["berlin_minus_6"]
    assert f"{UM}: Nacht widerspricht" in c["text"]


def test_family_month_and_variant_month_form_one_block():
    """Familie (2019-11) und Variante (2019-03, 2019-10) vertragen sich: ein Block mit
    der spezifischsten Regel; Variante gegen klassische Regel = Wechsel."""
    def fam_row(sym, ym):
        r = mrow(sym, ym, UM)
        r.update(allowed=["berlin_minus_6", UM], winner=f"Familie berlin_minus_6/{UM}")
        return r
    rows = ny_years(2017, 2018) + both((2019, 3), UM) + both((2019, 10), UM) + \
        [fam_row(s, (2019, 11)) for s in sv.SYMBOLS]
    _, an, sched, just = analyse(rows)
    assert an["switches"] == 1 and an["blocks"][1]["rule"] == UM
    assert entries(sched) == [("2000-01", "ny_local"), ("2018-12", UM)]
    rows += both((2020, 3), "berlin_minus_6") + both((2020, 10), "berlin_minus_6")
    _, an, sched, _ = analyse(rows)
    assert an["switches"] == 2 and sched == []


def owner_truth(ym):
    return "ny_local" if ym <= (2018, 11) else UM


def owner_nights(symbol, ym, rule):
    """utc_midnight-Bericht ab 2019: Maerz-Sprung und Oktober-Rueckfall bestaetigt."""
    if rule != UM or ym < (2019, 1) or ym[1] not in (3, 10):
        return None
    if ym[1] == 3:
        return [night("spring", "ok", f"{ym[0]}-03-31", ticks_in_gap=0,
                      last_before_raw=f"{ym[0]}-03-31 18:59:59", first_after_raw=f"{ym[0]}-03-31 20:00:00")]
    return [night("fall", "ok", f"{ym[0]}-10-27", reversal_found=True, ticks_in_fold=4200,
                  last_before_raw=f"{ym[0]}-10-27 19:59:59", first_after_raw=f"{ym[0]}-10-27 19:00:00")]


def test_owner_data_proposes_ny_until_2018_11_then_utc_midnight(tmp_path, cfg, monkeypatch):
    cfg((("2000-01", "ny_local"), ("2018-12", UM)))
    monkeypatch.setattr(fh, "evaluate_raw_month", fake_evaluator(owner_truth, nights=owner_nights))
    for y in (2017, 2018, 2019, 2020):
        for m in (1, 3, 7, 10, 11):
            for s in sv.SYMBOLS:
                touch_cache(tmp_path / "data", s, (y, m))
    md = tmp_path / "s.md"
    res, out = run(tmp_path, symbols=list(sv.SYMBOLS), years=[2017, 2018, 2019, 2020],
                   months=[3, 10, 11], controls=[1, 7], offline=True)
    assert [(e["from"], e["rule"]) for e in res["schedule"]] == [("2000-01", "ny_local"), ("2018-12", UM)]
    an = res["switch_analysis"]
    assert (an["last_old"], an["first_new"], an["undecided_months"]) == ("2018-11", "2019-03", [])
    assert "# Vorschlag = konfigurierter Fahrplan (tick)." in out
    assert res["exit_code"] == 0, out
    # Umstellungsnaechte: Tabelle + Zusammenfassung in Konsole, JSON und Markdown
    assert "Umstellungsnaechte (EU-Sonntage" in out
    line = f"2019 {UM}: Maerz-Sprung bestaetigt (XAUUSD, XAGUSD); Oktober-Rueckfall bestaetigt (XAUUSD, XAGUSD)"
    assert line in out and line in res["transition_nights_summary"]
    assert any(x["status"] == "ok" and x["first_after_raw"] == "2019-10-27 19:00:00"
               for x in res["transition_nights"])
    md.write_text(sv.render_markdown(res))
    text = md.read_text()
    assert "## Umstellungsnaechte" in text and f"- {line}" in text
    assert "| 2019 | XAUUSD | Oktober | berlin_minus_6_utc_midnight | 2019-10-27 |" in text


def test_unexpected_night_under_configured_rule_is_exit_4(tmp_path, cfg, monkeypatch):
    cfg((("2000-01", UM),))
    bad = lambda sym, ym, rule: [night("spring", "unexpected", "2019-03-31", ticks_in_gap=12,
                                       last_before_raw="2019-03-31 19:00:01",
                                       first_after_raw="2019-03-31 20:00:00")] \
        if rule == UM and ym == (2019, 3) else None
    monkeypatch.setattr(fh, "evaluate_raw_month", fake_evaluator(lambda ym: UM, nights=bad))
    for ym in [(2019, 3), (2019, 10)]:
        touch_cache(tmp_path / "data", "XAUUSD", ym)
    res, out = run(tmp_path, years=[2019], months=[3, 10], controls=[], offline=True)
    assert res["exit_code"] == 4
    assert [(c["month"], c["configured"]) for c in res["configured_failed"]] == [("2019-03", UM)]
    assert "unerwartete Umstellungsnacht: 2019-03-31 spring: 2019-03-31 19:00:01 -> " \
        "2019-03-31 20:00:00" in res["configured_failed"][0]["reason"]
    assert f"2019 {UM}: Maerz abweichend (XAUUSD)" in out


def test_integration_utc_midnight_real_zips(tmp_path, cfg):
    """Echte Pruefung (fh.evaluate_raw_month) mit Ticks nach berlin_minus_6_utc_midnight:
    2019-03 entscheidend erst durch den Juli-Ausschluss von fixed_est und den
    Maerz-Sprung (Nacht), 2019-10 durch den Oktober-Rueckfall; Vorschlag ny_local bis
    2018-11, berlin_minus_6_utc_midnight ab 2018-12."""
    cfg((("2000-01", "ny_local"), ("2018-12", UM)))
    root = tmp_path / "data"
    for ym in [(2018, 10), (2018, 11)]:
        write_raw_month(root, "XAUUSD", ym, "ny_local")
    for ym in [(2019, 3), (2019, 7), (2019, 10)]:
        write_raw_month(root, "XAUUSD", ym, UM)
    res, out = run(tmp_path, years=[2018, 2019], months=[3, 10, 11], controls=[7], offline=True,
                   max_downloads=0)
    by = {m["month"]: m for m in res["months"]}
    assert by["2019-03"]["allowed"] == [UM], out
    assert "fixed_est ausgeschlossen (2019-07: +1.0 h)" in by["2019-03"]["text"]
    assert by["2019-10"]["allowed"] == [UM]
    assert [(e["from"], e["rule"]) for e in res["schedule"]] == [("2000-01", "ny_local"), ("2018-12", UM)]
    assert f"2019 {UM}: Maerz-Sprung bestaetigt (XAUUSD); Oktober-Rueckfall bestaetigt (XAUUSD)" \
        in res["transition_nights_summary"]
    fall = next(x for x in res["transition_nights"]
                if x["rule"] == UM and x["kind"] == "fall" and x["year"] == 2019)
    assert (fall["last_before_raw"], fall["first_after_raw"], fall["reversal_found"]) == \
        ("2019-10-27 19:59:00", "2019-10-27 19:00:00", True)
    assert res["configured_failed"] == [] and res["exit_code"] == 3   # 2018-03, 2019-11 fehlen


# ------------------------------- Review v7: nur trennende Kontroll-Ausschluesse ---
def test_control_glitch_where_all_rules_fail_excludes_nothing(tmp_path, cfg, monkeypatch):
    """Review v7 MEDIUM 1: XAGUSD 2019-07 mit einem Werktags-Ruecksprung (Datenfehler):
    alle Regeln failed +0.0 h -> "Kontrolle unbrauchbar", nichts ausgeschlossen; 2019
    entscheidet trotzdem ueber die anderen Belege (XAUUSD-Juli, Oktober)."""
    base = fake_evaluator(lambda ym: UM)

    def ev(d, sym, ym, mode="tick", rule=None, **kw):
        rep = base(d, sym, ym, mode, rule, **kw)
        if sym == "XAGUSD" and tuple(ym) == (2019, 7):
            rep.update(status="failed", median_deviation_h=0.0,
                       reason="1 Ruecksprung >= 30 min an einem Werktag")
        return rep
    monkeypatch.setattr(fh, "evaluate_raw_month", ev)
    for s_ in sv.SYMBOLS:
        for ym in [(2019, 3), (2019, 7), (2019, 10)]:
            touch_cache(tmp_path / "data", s_, ym)
    res, out = run(tmp_path, symbols=list(sv.SYMBOLS), years=[2019], months=[3, 10],
                   controls=[7], offline=True)
    assert res["exclusions"] == {"2019": {"XAUUSD": {"fixed_est": "2019-07: +1.0 h"}}}
    assert res["unusable_controls"] == [{"symbol": "XAGUSD", "month": "2019-07", "reason":
                                         "alle Regeln durchgefallen (berlin_minus_6: 1 Ruecksprung "
                                         ">= 30 min an einem Werktag)"}]
    assert "Kontrolle unbrauchbar" in out
    by = {(m["symbol"], m["month"]): m for m in res["months"]}
    assert "keine Regel passt" not in by[("XAGUSD", "2019-03")]["text"]
    assert by[("XAGUSD", "2019-03")]["text"].startswith("mehrere passen")   # fixed_est nicht ausgeschl.
    assert by[("XAUUSD", "2019-03")]["allowed"] == [UM]
    assert by[("XAGUSD", "2019-10")]["allowed"] == [UM]
    mv = {m["month"]: m for m in res["raw_month_verdicts"]}
    assert mv["2019-03"]["allowed"] == [UM] and mv["2019-03"]["support"] == ["XAUUSD"]


def test_real_case_fixed_est_excluded_by_july_minus_4h(tmp_path, cfg, monkeypatch):
    """fixed_est faellt im Juli mit Abweichung durch, die berlin-Regeln bestehen:
    trennend -> fixed_est ausgeschlossen, sonst nichts."""
    monkeypatch.setattr(fh, "evaluate_raw_month", fake_evaluator(lambda ym: UM))
    touch_cache(tmp_path / "data", "XAUUSD", (2019, 7))
    touch_cache(tmp_path / "data", "XAUUSD", (2019, 3))
    res, _ = run(tmp_path, years=[2019], months=[3], controls=[7], offline=True)
    jul = next(m for m in res["months"] if m["month"] == "2019-07")
    assert {n for n, r in jul["results"].items() if r["status"] == "ok"} == \
        {"berlin_minus_6", UM, "ny_local"}
    assert res["exclusions"] == {"2019": {"XAUUSD": {"fixed_est": "2019-07: +1.0 h"}}}
    assert res["unusable_controls"] == []


def test_exclusion_is_scoped_per_symbol(tmp_path, cfg, monkeypatch):
    """XAUUSD-Juli schliesst fixed_est nur fuer XAUUSD aus; XAGUSD ohne Juli bleibt
    2019-03 nicht entscheidend (fixed_est und utc_midnight bestehen)."""
    monkeypatch.setattr(fh, "evaluate_raw_month", fake_evaluator(lambda ym: UM))
    for ym in [(2019, 3), (2019, 7)]:
        touch_cache(tmp_path / "data", "XAUUSD", ym)
    touch_cache(tmp_path / "data", "XAGUSD", (2019, 3))
    res, _ = run(tmp_path, symbols=list(sv.SYMBOLS), years=[2019], months=[3], controls=[7],
                 offline=True)
    assert res["exclusions"] == {"2019": {"XAUUSD": {"fixed_est": "2019-07: +1.0 h"}}}
    by = {(m["symbol"], m["month"]): m for m in res["months"]}
    assert by[("XAUUSD", "2019-03")]["allowed"] == [UM]
    assert not by[("XAGUSD", "2019-03")]["decisive"]
    assert "ausgeschlossen" not in by[("XAGUSD", "2019-03")]["text"]


def test_zero_deviation_failure_is_not_discriminating():
    def row(results):
        return {"evaluated": True, "control_like": True, "ym": (2019, 7), "month": "2019-07",
                "symbol": "XAUUSD", "results": results}
    ok = {"status": "ok", "median_deviation_h": 0.0}
    r = row({"berlin_minus_6": ok, "fixed_est": {"status": "failed", "median_deviation_h": 0.0},
             "ny_local": {"status": "failed", "median_deviation_h": None}})
    assert sv.year_exclusions([r]) == {} and "control_unusable" not in r
    r = row({"berlin_minus_6": ok, "fixed_est": {"status": "failed", "median_deviation_h": -1.0}})
    assert sv.year_exclusions([r]) == {(2019, "XAUUSD"): {"fixed_est": "2019-07: -1.0 h"}}
