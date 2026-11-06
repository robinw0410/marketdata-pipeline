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


def fake_evaluator(truth, calls=None, not_checkable=(), dropped=None, drop_flags=None):
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
                "large_reversals": []}
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
    # Wechsel an den Beginn der ersten berlin-Periode (2016 Maerz); 2015 Okt/Nov -> 2016 Maerz
    # sind benachbarte Perioden: kein undecided-Eintrag noetig
    assert res["schedule"] == [{"from": "2000-01", "rule": "ny_local"},
                               {"from": "2016-03", "rule": "berlin_minus_6"}]
    an = res["switch_analysis"]
    assert (an["switches"], an["last_old"], an["first_new"], an["between"]) == \
        (1, "2015 Okt/Nov", "2016 Maerz", [])
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
    assert fh.rule_for("tick", (2016, 2), parsed).name == "ny_local"
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


def test_fixed_est_period_is_decisive_in_july_and_change_inside_year(tmp_path, cfg, monkeypatch):
    truth = lambda ym: "fixed_est" if ym < (2012, 6) else "berlin_minus_6"
    monkeypatch.setattr(fh, "evaluate_raw_month", fake_evaluator(truth))
    root = tmp_path / "data"
    for m in (1, 3, 7, 10):
        touch_cache(root, "XAUUSD", (2012, m))
    res, out = run(tmp_path, years=[2012], offline=True, **NO_MONTHS)
    v = res["verdicts"]["2012"]["verdict"]
    assert v.startswith("Wechsel im Jahr: fixed_est (2012-03..2012-03) -> berlin_minus_6"), v
    # je Block nur EIN entscheidender Monat eines Symbols: zu wenig Evidenz fuer einen
    # Vorschlag (Review v5) - Belege werden gedruckt, aber nicht vorgeschlagen
    assert res["schedule"] == []
    assert [(b["rule"], b["months"]) for b in res["weak_evidence"]] == \
        [("fixed_est", ["2012-03"]), ("berlin_minus_6", ["2012-10"])]
    assert "zu wenig Evidenz" in out
    jan = next(m for m in res["months"] if m["month"] == "2012-01")
    assert jan["text"] == "Kontrolle ok: berlin_minus_6, ny_local, fixed_est"
    assert res["exit_code"] == 4


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
    assert rev == ["2024-10: 2024-10-26 20:59:02 -> 2024-10-26 20:00:05 (59.0 min, 4 Ticks, "
                   "Wochenende (Markt zu), verworfen)"], rev
    assert [(e["from"], e["rule"]) for e in res["schedule"]] == [
        ("2000-01", "ny_local"), ("2018-12", "undecided"), ("2021-03", "berlin_minus_6")], out
    assert res["switch_analysis"]["last_old"] == "2018 Okt/Nov"


# ------------------------------------------------ Review v5: Evidenz, Exit ---
def test_configured_rule_failing_where_two_others_pass_is_exit_4(tmp_path, cfg, monkeypatch):
    """Review v5 (2): konfigurierte Regel faellt durch, zwei andere bestehen
    (kein entscheidender Monat) -> trotzdem Exit 4 mit Liste."""
    cfg((("2000-01", "fixed_est"),))
    monkeypatch.setattr(fh, "evaluate_raw_month", fake_evaluator(lambda ym: "berlin_minus_6"))
    touch_cache(tmp_path / "data", "XAUUSD", (2018, 7))       # Juli: berlin = ny, fixed_est faellt
    res, out = run(tmp_path, years=[2018], months=[], controls=[7], offline=True)
    m = res["months"][0]
    assert not m["decisive"] and set(m["passing"]) == {"berlin_minus_6", "ny_local"}
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
    one = [mrow("XAUUSD", (2012, 3), "ny_local")]
    an = sv.analyze_switches(sv.build_periods(one, sv.SYMBOLS))
    assert sv.propose_schedule(an, (2000, 1), fh.TZ_RULES)[0] == []
    assert an["weak"][0]["months"] == ["2012-03"]
    both = [mrow("XAUUSD", (2012, 3), "ny_local"), mrow("XAGUSD", (2012, 3), "ny_local")]
    an = sv.analyze_switches(sv.build_periods(both, sv.SYMBOLS))
    sched, _ = sv.propose_schedule(an, (2000, 1), fh.TZ_RULES)
    assert [(e["from"], e["rule"]) for e in sched] == [("2000-01", "ny_local")]
    two = [mrow("XAUUSD", (2012, 3), "ny_local"), mrow("XAUUSD", (2012, 10), "ny_local"),
           mrow("XAUUSD", (2016, 3), "berlin_minus_6")]
    an = sv.analyze_switches(sv.build_periods(two, sv.SYMBOLS))
    assert an["switches"] == 1 and [w["rule"] for w in an["weak"]] == ["berlin_minus_6"]
    assert sv.propose_schedule(an, (2000, 1), fh.TZ_RULES)[0] == []      # berlin: 1 Monat


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
    periods = sv.build_periods(rows, sv.SYMBOLS)
    an = sv.analyze_switches(periods)
    sched, just = sv.propose_schedule(an, (2000, 1), fh.TZ_RULES)
    return periods, an, sched, just


def test_one_switch_with_gap_gives_three_entries_with_undecided():
    rows = ny_years(2017, 2018) + berlin_years(2021, 2022)
    rows += both((2019, 3), state="over_limit")                     # fehlt
    rows += both((2019, 10)) + both((2019, 11), text="keine Regel passt")   # nicht entscheidend
    periods, an, sched, just = analyse(rows)
    assert an["ok"] and an["switches"] == 1 and an["conflicts"] == []
    assert (an["last_old"], an["first_new"]) == ("2018 Okt/Nov", "2021 Maerz")
    # auch nicht untersuchte Perioden (2020) liegen dazwischen
    assert [b["period"] for b in an["between"]] == ["2019 Maerz", "2019 Okt/Nov", "2020 Maerz",
                                                    "2020 Okt/Nov"]
    assert an["between"][2]["state"] == "nicht untersucht"
    assert "03 fehlt" in an["between"][0]["state"]
    assert [{k: e.get(k) for k in ("from", "rule", "candidates")} for e in sched] == [
        {"from": "2000-01", "rule": "ny_local", "candidates": None},
        {"from": "2018-12", "rule": "undecided", "candidates": ["ny_local", "berlin_minus_6"]},
        {"from": "2021-03", "rule": "berlin_minus_6", "candidates": None}]
    # Begruendung: Abweichungen nur in Umstellungsperioden, Monate dazwischen identisch
    assert just["equivalent"] and just["outside"] == []
    assert set(just["windows"]) == {"2018 Okt/Nov", "2019 Maerz", "2019 Okt/Nov", "2020 Maerz",
                                    "2020 Okt/Nov", "2021 Maerz"}
    assert just["windows"]["2019 Maerz"] == ["10.03.2019 07:00-31.03.2019 01:00 UTC"]
    assert just["plain_months"][:3] == ["2018-12", "2019-01", "2019-02"]
    assert "2019-03" not in just["plain_months"] and "2020-07" in just["plain_months"]
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


def test_one_switch_without_gap_two_entries():
    _, an, sched, just = analyse(ny_years(2019, 2020) + berlin_years(2021))
    assert an["between"] == [] and (an["last_old"], an["first_new"]) == ("2020 Okt/Nov", "2021 Maerz")
    assert [(e["from"], e["rule"]) for e in sched] == [("2000-01", "ny_local"),
                                                      ("2021-03", "berlin_minus_6")]
    assert just["plain_months"] == ["2020-12", "2021-01", "2021-02"]
    # Wechsel innerhalb eines Jahres: letzte alte Periode Maerz, erste neue Okt/Nov
    rows = ny_years(2020) + both((2021, 3), "ny_local") + both((2021, 10), "berlin_minus_6") + \
        berlin_years(2022)
    _, an, sched, just = analyse(rows)
    assert [(e["from"], e["rule"]) for e in sched] == [("2000-01", "ny_local"),
                                                      ("2021-10", "berlin_minus_6")]
    assert just["plain_months"] == [f"2021-{m:02d}" for m in range(4, 10)]


def test_two_switches_no_proposal():
    rows = ny_years(2010) + berlin_years(2012) + ny_years(2015)
    _, an, sched, _ = analyse(rows)
    assert an["switches"] == 2 and not an["ok"] and sched == []
    assert "2 Wechsel statt hoechstens einem" in an["explanation"][0]
    assert "ny_local (2010 Maerz .. 2010 Okt/Nov) -> berlin_minus_6" in an["explanation"][0]


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
    assert p["status"] == "conflict" and p["conflict"] == "symbol"
    assert p["text"] == "KONFLIKT (Symbole: XAGUSD berlin_minus_6, XAUUSD ny_local)"
    assert not an["ok"] and sched == [] and an["conflicts"][0]["period"] == "2018 Maerz"
    # nur ein Symbol entscheidend, das andere offen: kein Konflikt, bestaetigt
    rows = [mrow("XAUUSD", (2018, 3), "ny_local"), mrow("XAGUSD", (2018, 3), state="over_limit")]
    p = sv.build_periods(rows, sv.SYMBOLS)[0]
    assert p["status"] == "confirmed" and p["rule"] == "ny_local"


def test_month_conflict_inside_okt_nov():
    rows = ny_years(2017) + [mrow("XAUUSD", (2018, 10), "berlin_minus_6"),
                             mrow("XAUUSD", (2018, 11), "ny_local"),
                             mrow("XAGUSD", (2018, 10), "ny_local")]
    periods, an, sched, _ = analyse(rows)
    p = periods[-1]
    assert p["label"] == "2018 Okt/Nov" and p["status"] == "conflict" and p["conflict"] == "month"
    assert p["symbols"]["XAUUSD"]["cell"] == "KONFLIKT (10: berlin_minus_6, 11: ny_local)"
    assert p["symbols"]["XAGUSD"]["status"] == "rule"
    assert not an["ok"] and sched == [] and "Konflikt in 2018 Okt/Nov" in an["explanation"][0]
    # nicht entscheidender Monat (alle/keine bestehen) widerspricht NICHT
    rows = [mrow("XAUUSD", (2018, 10), "ny_local"),
            mrow("XAUUSD", (2018, 11), text="keine Regel passt")]
    p = sv.build_periods(rows, ["XAUUSD"])[0]
    assert p["status"] == "confirmed" and p["symbols"]["XAUUSD"]["cell"] == "ny_local (10)"


def test_weekend_dropped_month_counts_as_decisive(tmp_path, cfg, monkeypatch):
    flags = []
    monkeypatch.setattr(fh, "evaluate_raw_month", fake_evaluator(
        lambda ym: "berlin_minus_6", drop_flags=flags,
        dropped={("XAUUSD", (2021, 10), "berlin_minus_6"): 3}))
    touch_cache(tmp_path / "data", "XAUUSD", (2021, 10))
    res, out = run(tmp_path, years=[2021], months=[10], controls=[], offline=True)
    assert flags and all(flags)                                  # immer drop_releasable_weekend=True
    m = res["months"][0]
    assert m["decisive"] and m["winner"] == "berlin_minus_6"
    assert m["results"]["berlin_minus_6"]["dropped_weekend_ticks"] == 3
    assert "ok +0.0 (3 Wochenend-Ticks verworfen)" in out
    p = res["periods"][0]
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
    # Vorschlag: ny bis 2019 Maerz, berlin ab 2019 Okt/Nov (benachbart: kein undecided)
    assert [(e["from"], e["rule"]) for e in res["schedule"]] == [
        ("2000-01", "ny_local"), ("2019-10", "berlin_minus_6")]
    assert res["exit_code"] == 3                     # 2018-03, 2021-10 fehlen (nicht im Cache)


# ------------------------------------------------------- v6: Planung ---
def test_preset_full_and_periods_resolve():
    years, months, controls = sv.resolve_plan("full", None, None, None, True)
    assert years == list(range(2009, 2026)) and months == [3, 10, 11] and controls == []
    # explizite Angaben schlagen das Preset
    assert sv.resolve_plan("full", [2019], None, None, False) == ([2019], [3, 10, 11], [])
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
    monkeypatch.setattr(fh, "evaluate_raw_month", fake_evaluator(lambda ym: "berlin_minus_6"))
    root = tmp_path / "data"
    for y in (2010, 2012, 2015, 2018, 2021, 2024):           # erster Survey: schon im Cache
        for m in (3, 10, 11):
            for s in sv.SYMBOLS:
                touch_cache(root, s, (y, m))
    years, months, controls = sv.resolve_plan("full", None, None, None, True)
    site = Site()
    clock = Clock()
    out = io.StringIO()
    res = sv.survey(sv.SYMBOLS, years, months, controls, data_root=root,
                    periods=[(2019, 3), (2019, 10), (2019, 11), (2020, 3), (2020, 10), (2020, 11)],
                    now=lambda: datetime(2026, 11, 6, tzinfo=UTC), pause=0.0, out=out,
                    max_downloads=3, session=site, sleep=clock.sleep, clock=clock, wall=clock)
    txt = out.getvalue()
    # 2009-2025 x 3 Monate x 2 Symbole = 102, XAGUSD 2009-03 vor Historienbeginn; 36 im Cache
    assert "Plan: 51 Rohmonate x 2 Symbole = 102 Rohdateien (34 Umstellungsperioden), " \
        "36 im Cache, 65 fehlen" in txt
    assert "Geplant: 3 Rohmonate laden = 6 Requests" in txt
    assert "62 weitere ueber --max-downloads 3 (naechster Aufruf, 124 Requests)" in txt
    assert site.requests == 6 and res["requests"] == 6
    assert res["exit_code"] in (3, 4) and res["remaining_raw_months"] == 62
    assert "noch fehlend: 62 Rohmonate" in txt
    assert "UNVOLLSTAENDIG: noch 62 Rohmonate offen (= 124 Requests" in txt


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
    code = sv.main(["--preset", "full", "--years", "2018", "2021", "--periods", "2019-03",
                    "--offline", "--json", str(js), "--markdown", str(md)])
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
    assert "## Begruendung" in text and "jede Grenze zwischen 2018 Okt/Nov und 2021 Maerz" in text
    assert res["schedule"][1] == {"from": "2018-12", "rule": "undecided",
                                  "candidates": ["ny_local", "berlin_minus_6"]}
    assert res["switch_analysis"]["between"][0]["period"] == "2019 Maerz"
