"""Tests fuer histdata_tz_survey.py - nur synthetische Daten, HTTP und (meist) die
Auswertung gemockt; ein Integrationstest mit synthetischen Roh-ZIPs."""
import io
import json
import re
import zipfile
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

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
    """Stuendliche Offsets (h) einer Regel ueber den Rohmonat."""
    lo = datetime(*ym, 1)
    hi = datetime(*sv.ym_add(ym, 1), 1)
    z = ZoneInfo(rule.zone)
    t, out = lo, []
    while t < hi:
        out.append(z.utcoffset(t).total_seconds() / 3600 + rule.shift_h)
        t += timedelta(hours=1)
    return np.array(out)


def fake_evaluator(truth, calls=None, not_checkable=()):
    """evaluate_raw_month-Ersatz: Regel besteht, wenn sie im ganzen Rohmonat
    dieselben Offsets wie die "wahre" Stempelung truth(ym) hat."""
    def ev(data_root, symbol, ym, mode="tick", rule=None):
        if calls is not None:
            calls.append((symbol, tuple(ym), rule.name))
        if not fh.Unit(symbol, mode, *ym).path(data_root).exists():
            return {"status": "not_cached", "cached": False}
        if (symbol, tuple(ym), rule.name) in not_checkable:
            return {"status": "not_checkable", "reason": "zu wenige Anker", "cached": True,
                    "anchors": 1, "median_deviation_h": None}
        dev = offsets(fh.TZ_RULES[truth(ym)], ym) - offsets(rule, ym)
        return {"status": "ok" if not dev.any() else "failed", "reason": None, "cached": True,
                "anchors": 40, "median_deviation_h": float(np.median(dev))}
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
    assert sorted(sv.dst_rules(fh.TZ_RULES, 2015)) == ["berlin_minus_6", "ny_local"]
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
    assert res["schedule"] == [{"from": "2000-01", "rule": "ny_local"},
                               {"from": "2016-01", "rule": "berlin_minus_6"}]
    det = res["schedule_details"][1]
    assert det["change_between"] == ["2015-10", "2016-03"]
    assert res["verdicts"]["2012"]["verdict"] == "nur ny_local passt"
    assert res["verdicts"]["2015"]["verdict"] == "nur ny_local passt"
    assert res["verdicts"]["2016"]["verdict"] == "berlin_minus_6 passt"
    assert res["verdicts"]["2018"]["verdict"] == "berlin_minus_6 passt"
    assert not any(n.startswith("Kontrolle") for v in res["verdicts"].values() for n in v["notes"])
    # Ausgabe: Tabelle je Symbol, YAML-Vorschlag, Widerspruchsliste
    assert "XAUUSD (Tick-Rohmonate" in out and "XAGUSD (Tick-Rohmonate" in out
    assert '    - {from: "2000-01", rule: ny_local}' in out
    assert '    - {from: "2016-01", rule: berlin_minus_6}' in out
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
    assert fh.rule_for("tick", (2015, 12), parsed).name == "ny_local"
    assert fh.rule_for("tick", (2016, 1), parsed).name == "berlin_minus_6"
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


def test_fixed_est_period_is_decisive_in_july_and_change_inside_year(tmp_path, cfg, monkeypatch):
    truth = lambda ym: "fixed_est" if ym < (2012, 6) else "berlin_minus_6"
    monkeypatch.setattr(fh, "evaluate_raw_month", fake_evaluator(truth))
    root = tmp_path / "data"
    for m in (1, 3, 7, 10):
        touch_cache(root, "XAUUSD", (2012, m))
    res, out = run(tmp_path, years=[2012], offline=True, **NO_MONTHS)
    v = res["verdicts"]["2012"]["verdict"]
    assert v.startswith("Wechsel im Jahr: fixed_est (2012-03..2012-03) -> berlin_minus_6"), v
    assert res["schedule"] == [{"from": "2000-01", "rule": "fixed_est"},
                               {"from": "2012-04", "rule": "berlin_minus_6"}]
    jan = next(m for m in res["months"] if m["month"] == "2012-01")
    assert jan["text"] == "Kontrolle ok: berlin_minus_6, ny_local, fixed_est"
    assert res["exit_code"] == 4


def test_no_rule_passes_and_control_failure(tmp_path, cfg, monkeypatch):
    def ev(data_root, symbol, ym, mode="tick", rule=None):
        return {"status": "failed", "cached": True, "median_deviation_h": 1.0, "anchors": 30}
    monkeypatch.setattr(fh, "evaluate_raw_month", ev)
    touch_cache(tmp_path / "data", "XAUUSD", (2010, 1))
    touch_cache(tmp_path / "data", "XAUUSD", (2010, 3))
    res, out = run(tmp_path, years=[2010], months=[3], controls=[1], offline=True)
    v = res["verdicts"]["2010"]
    assert v["verdict"] == "keine Regel passt"
    assert any("Kontrolle gescheitert" in n for n in v["notes"])
    assert "Kein Fahrplan-Vorschlag" in out
    assert res["exit_code"] == 0                             # nichts Entscheidendes, nichts fehlt


def test_symbols_disagree_month_left_out_of_schedule(tmp_path, cfg, monkeypatch):
    base = fake_evaluator(lambda ym: "berlin_minus_6")
    other = fake_evaluator(lambda ym: "ny_local")
    monkeypatch.setattr(fh, "evaluate_raw_month",
                        lambda d, s, ym, mode="tick", rule=None:
                        (other if s == "XAGUSD" else base)(d, s, ym, mode, rule))
    for s in ("XAUUSD", "XAGUSD"):
        touch_cache(tmp_path / "data", s, (2018, 3))
    res, out = run(tmp_path, symbols=["XAUUSD", "XAGUSD"], years=[2018], months=[3],
                   controls=[], offline=True)
    assert res["symbol_conflicts"] == ["2018-03: berlin_minus_6, ny_local"]
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
    out = tmp_path / "survey.json"
    code = sv.main(["--symbols", "XAUUSD", "--years", "2024", "--months", "3", "--controls",
                    "--offline", "--json", str(out)])
    res = json.loads(out.read_text())
    assert code == res["exit_code"] == 0
    assert res["rules"] == list(fh.TZ_RULES)
    with pytest.raises(SystemExit):
        sv.main(["--rules", "gibt_es_nicht", "--offline"])


# --------------------------------------------- integration (echte Pruefung) ---
def write_raw_month(root, sym, ym, rule, step_s=60):
    """Synthetische Ticks eines Rohmonats (New-York-Marktzeiten, Tagespause
    17-18 Uhr), gestempelt nach `rule`, als Roh-ZIP in den Cache."""
    lo = pd.Timestamp(*ym, 1, tz="UTC") - pd.Timedelta(days=1)
    hi = pd.Timestamp(*ym, 1, tz="UTC") + pd.offsets.MonthBegin(1) + pd.Timedelta(days=1)
    idx = pd.date_range(lo, hi, freq=f"{step_s}s", inclusive="left") + pd.Timedelta(milliseconds=137)
    ny = idx.tz_convert("America/New_York")
    wd, h = ny.dayofweek, ny.hour + ny.minute / 60
    closed = ((wd == 4) & (h >= 17)) | (wd == 5) | ((wd == 6) & (h < 18)) | \
        ((wd <= 3) & (h >= 17) & (h < 18))
    utc = idx[~np.asarray(closed)].as_unit("ms").asi8
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
    assert res["schedule"] == [{"from": "2000-01", "rule": "ny_local"},
                               {"from": "2024-01", "rule": "berlin_minus_6"}]
    assert res["verdicts"]["2015"]["verdict"] == "nur ny_local passt"
    assert res["verdicts"]["2024"]["verdict"] == "berlin_minus_6 passt"
    assert res["exit_code"] == 4                             # 2015-03 widerspricht berlin_minus_6
    assert {m["month"] for m in res["incomplete"]} == {"2015-10", "2024-03"}
