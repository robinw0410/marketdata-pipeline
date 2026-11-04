"""fetch_calendar: Parser je Quelle, Zeitzonen/DST, Merge, Validierung, Lauf.

Alles mit selbst gebauten Fixtures (tests/fixtures/calendar/) und einem
requests.get-Mock, kein Netzwerk.
"""
import copy
import json
import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest
import requests

import fetch_calendar as fc

FIX = Path(__file__).resolve().parent / "fixtures" / "calendar"
FOMC_CURRENT = (FIX / "fomc_current.html").read_text()
FOMC_2020 = (FIX / "fomc_historical_2020.html").read_text()
ECB_HTML = (FIX / "ecb_mgcgc.html").read_text()
NOW = datetime(2025, 11, 4, 5, 10, tzinfo=timezone.utc)
API_KEY = "testkey0123456789abcdef"


@pytest.fixture(scope="module")
def cfg():
    return fc.load_config()


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    slept: list[float] = []
    monkeypatch.setattr(fc, "_sleep", slept.append)
    return slept


FA = pd.Timestamp("2026-04-10 05:10", tz="UTC")


# ----------------------------------------------------------------- config ---
def test_config_impact_and_release_ids(cfg):
    imp = cfg["_impact_of"]
    assert {k for k, v in imp.items() if v == "high"} == {
        "FOMC_STATEMENT", "FOMC_PRESS_CONF", "CPI", "NFP", "PCE", "ECB_RATE_DECISION"}
    assert {k for k, v in imp.items() if v == "medium"} == {
        "GDP", "PPI", "RETAIL_SALES", "FOMC_MINUTES", "ECB_PRESS_CONF", "JOBLESS_CLAIMS"}
    ids = {r["event_type"]: r["release_id"] for r in cfg["fred"]["releases"]}
    assert ids == {"NFP": 50, "CPI": 10, "PCE": 54, "GDP": 53, "PPI": 46,
                   "RETAIL_SALES": 9, "JOBLESS_CLAIMS": 180}
    assert all(r["time_local"] == "08:30" for r in cfg["fred"]["releases"])


def test_config_rejects_unmapped_or_double_impact(tmp_path, cfg):
    import yaml
    raw = yaml.safe_load((fc.ROOT / "config" / "calendar.yaml").read_text())
    raw["impact"]["medium"].remove("PPI")
    p = tmp_path / "c.yaml"
    p.write_text(yaml.safe_dump(raw))
    with pytest.raises(fc.ConfigError, match="PPI"):
        fc.load_config(p)
    raw["impact"]["medium"].append("CPI")
    p.write_text(yaml.safe_dump(raw))
    with pytest.raises(fc.ConfigError, match="mehrfach"):
        fc.load_config(p)


def test_systemd_units():
    svc = (fc.ROOT / "systemd" / "marketdata-calendar.service").read_text()
    tmr = (fc.ROOT / "systemd" / "marketdata-calendar.timer").read_text()
    assert "User=mkt" in svc and "EnvironmentFile=-/etc/marketdata.env" in svc
    assert "ReadWritePaths=/opt/marketdata/data" in svc and "ProtectSystem=strict" in svc
    assert "OnCalendar=*-*-* 05:10:00 UTC" in tmr and "Persistent=true" in tmr
    assert "FRED_API_KEY=" in (fc.ROOT / ".env.example").read_text()


# --------------------------------------------------------------- zeit/DST ---
@pytest.mark.parametrize("d, hhmm, tz, expect", [
    (date(2024, 1, 11), "08:30", "America/New_York", "2024-01-11 13:30"),   # Winter EST
    (date(2024, 7, 11), "08:30", "America/New_York", "2024-07-11 12:30"),   # Sommer EDT
    # 10.-30.03.2024: USA schon Sommerzeit, EU noch Winterzeit
    (date(2024, 3, 14), "08:30", "America/New_York", "2024-03-14 12:30"),
    (date(2024, 3, 14), "14:15", "Europe/Berlin", "2024-03-14 13:15"),
    # 27.10.-02.11.2024: EU schon Winterzeit, USA noch Sommerzeit
    (date(2024, 10, 29), "08:30", "America/New_York", "2024-10-29 12:30"),
    (date(2024, 10, 29), "14:15", "Europe/Berlin", "2024-10-29 13:15"),
    (date(2024, 11, 5), "08:30", "America/New_York", "2024-11-05 13:30"),
    (date(2024, 4, 11), "14:15", "Europe/Berlin", "2024-04-11 12:15"),
])
def test_local_to_utc_dst(d, hhmm, tz, expect):
    got = fc.local_to_utc(d, hhmm, tz)
    assert got == pd.Timestamp(expect, tz="UTC")
    assert str(got.tz) == "UTC" and got.unit == "ns"


def test_nonexistent_local_time_is_error():
    with pytest.raises(fc.ConfigError):
        fc.local_to_utc(date(2024, 3, 10), "02:30", "America/New_York")


# ------------------------------------------------------------------- FRED ---
def test_fred_parse_dates_and_rule_time(cfg):
    payload = {"realtime_start": "2000-01-01", "count": 4, "offset": 0, "limit": 10000,
               "release_dates": [
                   {"release_id": 10, "date": "2024-01-11"},
                   {"release_id": 10, "date": "2024-07-11"},
                   {"release_id": 10, "date": "2024-07-11"},      # doppelt
                   {"release_id": 10, "date": "2024-07-13"}]}     # Samstag
    dates, warns = fc.parse_fred_release_dates(payload, 10)
    assert dates == [date(2024, 1, 11), date(2024, 7, 11)]
    assert len(warns) == 1 and "2024-07-13" in warns[0]
    ev = fc.make_event(cfg, "CPI", dates[0], "08:30", "US", "fred", FA)
    assert ev["event_id"] == "CPI:2024-01-11"
    assert ev["ts_utc"] == pd.Timestamp("2024-01-11 13:30", tz="UTC")
    assert fc.make_event(cfg, "CPI", dates[1], "08:30", "US", "fred", FA)["ts_utc"] == \
        pd.Timestamp("2024-07-11 12:30", tz="UTC")


@pytest.mark.parametrize("payload", [
    {}, {"release_dates": [{"release_id": 11, "date": "2024-01-11"}]},
    {"release_dates": [{"release_id": 10, "date": "11.01.2024"}]},
])
def test_fred_parse_rejects_garbage(payload):
    with pytest.raises(fc.ParseError):
        fc.parse_fred_release_dates(payload, 10)


def test_fred_name_check():
    ok = {"releases": [{"id": 50, "name": "Employment Situation", "press_release": True}]}
    assert fc.fred_check_release_name(ok, 50, "Employment Situation") == "Employment Situation"
    wrong = {"releases": [{"id": 50, "name": "H.4.1 Factors Affecting Reserve Balances"}]}
    with pytest.raises(fc.ConfigError, match="release_id"):
        fc.fred_check_release_name(wrong, 50, "Employment Situation")
    with pytest.raises(fc.ConfigError):
        fc.fred_check_release_name({"releases": [{"id": 51, "name": "Employment Situation"}]},
                                   50, "Employment Situation")
    with pytest.raises(fc.ConfigError):
        fc.fred_check_release_name({"error_code": 400}, 50, "Employment Situation")


# ------------------------------------------------------------------- FOMC ---
def test_fomc_current_page(cfg):
    ms = fc.parse_fomc_page(FOMC_CURRENT)
    reg = [m for m in ms if m.kind == "regular"]
    assert fc.check_fomc_meetings(ms, [2024, 2025, 2026], cfg) == {2024: 8, 2025: 8, 2026: 8}
    by_end = {m.end: m for m in reg}
    # "Apr/May 30-1" -> letzter Tag 1. Mai
    m = by_end[date(2024, 5, 1)]
    assert m.start == date(2024, 4, 30) and not m.sep
    assert m.minutes_released == date(2024, 5, 22)
    assert by_end[date(2024, 3, 20)].sep and not by_end[date(2024, 1, 31)].sep
    # Minutes der Dezember-Sitzung erscheinen im Folgejahr
    assert by_end[date(2024, 12, 18)].minutes_released == date(2025, 1, 8)
    # kein Minutes-Datum fuer kommende Sitzungen
    assert by_end[date(2026, 10, 28)].minutes_released is None
    # notation vote zaehlt nicht als regulaere Sitzung
    nv = [m for m in ms if m.kind == "notation_vote"]
    assert [m.end for m in nv] == [date(2025, 8, 22)]
    # Script-Inhalt / Fliesstext ("January 26-27, 2027") ergibt keine Sitzung
    assert {m.year for m in ms} == {2024, 2025, 2026}


def test_fomc_events_times_and_press_conf(cfg):
    ev = fc.fomc_events(fc.parse_fomc_page(FOMC_CURRENT), cfg, FA).set_index("event_id")
    st = ev.loc["FOMC_STATEMENT:2024-05-01"]
    assert st.ts_utc == pd.Timestamp("2024-05-01 18:00", tz="UTC")       # 14:00 EDT
    assert ev.loc["FOMC_STATEMENT:2024-01-31"].ts_utc == pd.Timestamp("2024-01-31 19:00", tz="UTC")
    assert ev.loc["FOMC_PRESS_CONF:2024-01-31"].ts_utc == pd.Timestamp("2024-01-31 19:30", tz="UTC")
    assert ev.loc["FOMC_MINUTES:2024-05-22"].ts_utc == pd.Timestamp("2024-05-22 18:00", tz="UTC")
    n = ev["event_type"].value_counts()
    assert n["FOMC_STATEMENT"] == 24 and n["FOMC_PRESS_CONF"] == 24   # ab 2019 jede Sitzung
    assert n["FOMC_MINUTES"] == 22                                    # 2 ohne Datum weggelassen
    assert "FOMC_STATEMENT:2025-08-22" not in ev.index                 # notation vote
    assert set(ev["impact"][ev["event_type"] == "FOMC_MINUTES"]) == {"medium"}
    assert set(ev["status"]) == {"scheduled"} and set(ev["source"]) == {"fomc"}


def test_fomc_historical_2020_skips_unscheduled(cfg):
    ms = fc.parse_fomc_page(FOMC_2020, year=2020)
    assert fc.check_fomc_meetings(ms, [2020], cfg) == {2020: 7}
    kinds = {m.end: m.kind for m in ms}
    assert kinds[date(2020, 3, 3)] == "unscheduled" and kinds[date(2020, 3, 15)] == "unscheduled"
    assert kinds[date(2020, 3, 23)] == "conference_call"
    assert [m.end.month for m in ms if m.sep] == [6, 9, 12]           # ueber "Projection Materials"
    ev = fc.fomc_events(ms, cfg, FA)
    assert not ev["event_id"].str.contains("2020-03-").any()          # stehen in calendar_adhoc.csv
    assert (ev["event_type"] == "FOMC_PRESS_CONF").sum() == 7
    assert not (ev["event_type"] == "FOMC_MINUTES").any()             # kein Datum auf der Seite


def _historical(year, heads):
    body = "".join(f'<div class="panel"><h5>{h} - {year}</h5><p>Statement: <a href="#">HTML</a></p>'
                   + ('<p>Projection Materials: <a href="#">PDF</a></p>' if "SEP" in h else "")
                   + "</div>\n" for h in heads)
    return f"<html><body><h3>FOMC Meetings {year}</h3>{body.replace(' SEP', '')}</body></html>"


def test_fomc_press_conf_rule_before_2019(cfg):
    y2011 = _historical(2011, ["January 25-26 Meeting SEP", "March 15 Meeting",
                               "April 26-27 Meeting SEP", "June 21-22 Meeting SEP",
                               "August 9 Meeting", "September 20-21 Meeting",
                               "November 1-2 Meeting SEP", "December 13 Meeting"])
    y2012 = _historical(2012, ["January 24-25 Meeting SEP", "March 13 Meeting",
                               "April 24-25 Meeting SEP", "June 19-20 Meeting SEP",
                               "July 31-August 1 Meeting", "September 12-13 Meeting SEP",
                               "October 23-24 Meeting", "December 11-12 Meeting SEP"])
    ms11 = fc.parse_fomc_page(y2011, 2011)
    ms12 = fc.parse_fomc_page(y2012, 2012)
    assert fc.check_fomc_meetings(ms11 + ms12, [2011, 2012], cfg) == {2011: 8, 2012: 8}
    assert any(m.start == date(2012, 7, 31) and m.end == date(2012, 8, 1) for m in ms12)
    pc = {m.end for m in ms11 + ms12 if fc.has_press_conf(m, cfg)}
    # Januar 2011 SEP, aber vor April 2011 -> keine PK
    assert pc == {date(2011, 4, 27), date(2011, 6, 22), date(2011, 11, 2), date(2012, 1, 25),
                  date(2012, 4, 25), date(2012, 6, 20), date(2012, 9, 13), date(2012, 12, 12)}
    m2019 = fc.Meeting(2019, date(2019, 1, 29), date(2019, 1, 30), sep=False, kind="regular")
    assert fc.has_press_conf(m2019, cfg)


def _m(end, sep=False):
    return fc.Meeting(end.year, end - timedelta(days=1), end, sep=sep, kind="regular")


@pytest.mark.parametrize("end, sep, st_utc, pc_utc", [
    # bis 2012-12-31 ohne PK: 14:15 ET
    (date(2010, 1, 27), True, "2010-01-27 19:15", None),        # EST, SEP aber vor 2011-04
    (date(2010, 6, 23), True, "2010-06-23 18:15", None),        # EDT
    (date(2011, 1, 26), True, "2011-01-26 19:15", None),        # SEP, PK erst ab 2011-04-27
    (date(2012, 3, 13), False, "2012-03-13 18:15", None),       # EDT seit 11.03.2012
    (date(2012, 12, 31), False, "2012-12-31 19:15", None),      # letzter Tag, valid_to inklusive
    # 2011-04-27 .. 2012-12-31 mit PK: Statement 12:30, PK 14:15 ET
    (date(2011, 4, 27), True, "2011-04-27 16:30", "2011-04-27 18:15"),   # EDT, erste PK
    (date(2011, 11, 2), True, "2011-11-02 16:30", "2011-11-02 18:15"),   # EDT (bis 06.11.)
    (date(2012, 1, 25), True, "2012-01-25 17:30", "2012-01-25 19:15"),   # EST
    (date(2012, 12, 12), True, "2012-12-12 17:30", "2012-12-12 19:15"),
    # ab 2013-01-01: 14:00 / 14:30 ET
    (date(2013, 1, 30), False, "2013-01-30 19:00", None),
    (date(2013, 3, 20), True, "2013-03-20 18:00", "2013-03-20 18:30"),   # EDT seit 10.03.2013
    (date(2018, 12, 19), True, "2018-12-19 19:00", "2018-12-19 19:30"),
    (date(2019, 1, 30), False, "2019-01-30 19:00", "2019-01-30 19:30"),  # ab 2019 jede Sitzung
    (date(2024, 7, 31), False, "2024-07-31 18:00", "2024-07-31 18:30"),
])
def test_fomc_time_periods(cfg, end, sep, st_utc, pc_utc):
    ev = fc.fomc_events([_m(end, sep)], cfg, FA).set_index("event_type")
    assert ev.loc["FOMC_STATEMENT"].ts_utc == pd.Timestamp(st_utc, tz="UTC")
    assert ev.loc["FOMC_STATEMENT"].event_id == f"FOMC_STATEMENT:{end.isoformat()}"
    if pc_utc is None:
        assert "FOMC_PRESS_CONF" not in ev.index
    else:
        assert ev.loc["FOMC_PRESS_CONF"].ts_utc == pd.Timestamp(pc_utc, tz="UTC")


def test_fomc_time_rules_config_errors(cfg):
    c = copy.deepcopy(cfg)
    c["fomc"]["time_rules"][2]["valid_from"] = "2012-12-01"     # Ueberlappung
    with pytest.raises(fc.ConfigError, match="2 passende"):
        fc.fomc_times(date(2012, 12, 12), False, c)
    c = copy.deepcopy(cfg)
    c["fomc"]["time_rules"] = c["fomc"]["time_rules"][2:]        # Luecke vor 2013
    with pytest.raises(fc.ConfigError, match="0 passende"):
        fc.fomc_events([_m(date(2012, 1, 25), True)], c, FA)
    with pytest.raises(fc.ConfigError):                         # PK vor 2011-04-27 ohne Regel
        fc.fomc_times(date(2011, 1, 26), True, cfg)


def test_fomc_historical_pages_use_period_times(cfg):
    y2012 = _historical(2012, ["January 24-25 Meeting SEP", "March 13 Meeting",
                               "April 24-25 Meeting SEP", "June 19-20 Meeting SEP",
                               "July 31-August 1 Meeting", "September 12-13 Meeting SEP",
                               "October 23-24 Meeting", "December 11-12 Meeting SEP"])
    ev = fc.fomc_events(fc.parse_fomc_page(y2012, 2012), cfg, FA).set_index("event_id")
    assert ev.loc["FOMC_STATEMENT:2012-08-01"].ts_utc == pd.Timestamp("2012-08-01 18:15", tz="UTC")
    assert ev.loc["FOMC_STATEMENT:2012-09-13"].ts_utc == pd.Timestamp("2012-09-13 16:30", tz="UTC")
    assert ev.loc["FOMC_PRESS_CONF:2012-09-13"].ts_utc == pd.Timestamp("2012-09-13 18:15", tz="UTC")
    fc.validate(ev.reset_index()[fc.COLUMNS], cfg, 2030, date(2026, 1, 1))   # Lokalzeiten plausibel


def test_fomc_plausibility_too_few_meetings(cfg):
    html = FOMC_CURRENT
    for days in ("27-28", "28-29"):            # zwei 2026er-Zeilen entfernen
        start = html.index(f">{days}<")
        a = html.rindex('<div class="row fomc-meeting', 0, start)
        b = html.index('<div class="row fomc-meeting', start)
        html = html[:a] + html[b:]
    ms = fc.parse_fomc_page(html)
    with pytest.raises(fc.ParseError, match="2026: 6 regulaere"):
        fc.check_fomc_meetings(ms, [2024, 2025, 2026], cfg)


def test_fomc_unknown_marker_and_weekend_are_errors(cfg):
    with pytest.raises(fc.ParseError, match="Markierung"):
        fc.parse_fomc_page("<div>March 17-18 (postponed) Meeting</div>", 2020)
    html = _historical(2024, ["January 27-28 Meeting"] + [f"{m} 6-7 Meeting" for m in
                       ("March", "May", "June", "August", "September", "November")])
    with pytest.raises(fc.ParseError, match="Wochenende"):
        fc.check_fomc_meetings(fc.parse_fomc_page(html, 2024), [2024], cfg)


# -------------------------------------------------------------------- EZB ---
def test_ecb_parse_fixture(cfg):
    dates = fc.parse_ecb_calendar(ECB_HTML)
    assert dates[:2] == [date(2025, 1, 30), date(2025, 3, 6)]      # Day 2, nicht Day 1
    assert date(2025, 2, 12) not in dates                           # non-monetary
    assert date(2025, 6, 19) not in dates                           # General Council
    assert date(2025, 10, 30) in dates                              # externe Sitzung
    per = fc.check_ecb_dates(dates, cfg, date(2025, 11, 1))
    assert per == {2025: 8, 2026: 2}          # 2026 noch unvollstaendig: erlaubt (Zukunft)


def test_ecb_plausibility(cfg):
    dates = fc.parse_ecb_calendar(ECB_HTML)
    with pytest.raises(fc.ParseError, match="EZB 2026: 2"):
        fc.check_ecb_dates(dates, cfg, date(2026, 4, 10))         # laufendes Jahr: Fehler
    with pytest.raises(fc.ParseError, match="keine"):
        fc.check_ecb_dates(fc.parse_ecb_calendar("<p>Wartung</p>"), cfg, date(2026, 1, 1))
    many = [date(2025, 1, 2) + timedelta(weeks=3 * i) for i in range(14)]
    with pytest.raises(fc.ParseError, match="2025: 1[34]"):
        fc.check_ecb_dates(many, cfg, date(2025, 12, 31))


def test_ecb_time_switch_2022_07_21(cfg):
    ev = fc.ecb_events([date(2022, 7, 20), date(2022, 7, 21)], cfg, FA).set_index("event_id")
    # 13:45 / 14:30 MESZ bis 20.07.2022, ab 21.07.2022 14:15 / 14:45 MESZ
    assert ev.loc["ECB_RATE_DECISION:2022-07-20"].ts_utc == pd.Timestamp("2022-07-20 11:45", tz="UTC")
    assert ev.loc["ECB_PRESS_CONF:2022-07-20"].ts_utc == pd.Timestamp("2022-07-20 12:30", tz="UTC")
    assert ev.loc["ECB_RATE_DECISION:2022-07-21"].ts_utc == pd.Timestamp("2022-07-21 12:15", tz="UTC")
    assert ev.loc["ECB_PRESS_CONF:2022-07-21"].ts_utc == pd.Timestamp("2022-07-21 12:45", tz="UTC")
    # Winter (MEZ): 14:15 -> 13:15 UTC
    w = fc.ecb_events([date(2025, 1, 30)], cfg, FA).set_index("event_id")
    assert w.loc["ECB_RATE_DECISION:2025-01-30"].ts_utc == pd.Timestamp("2025-01-30 13:15", tz="UTC")
    assert w.loc["ECB_RATE_DECISION:2025-01-30"].impact == "high"
    assert w.loc["ECB_PRESS_CONF:2025-01-30"].impact == "medium"
    assert set(w["country"]) == {"EA"}


# ------------------------------------------------------------------ adhoc ---
def test_adhoc_csv(cfg):
    df = fc.fetch_adhoc(cfg, 2003, FA, {}).set_index("event_id")
    assert list(df.index) == ["FOMC_STATEMENT:2020-03-03", "FOMC_STATEMENT:2020-03-15"]
    assert df.loc["FOMC_STATEMENT:2020-03-03"].ts_utc == pd.Timestamp("2020-03-03 15:00", tz="UTC")  # EST
    assert df.loc["FOMC_STATEMENT:2020-03-15"].ts_utc == pd.Timestamp("2020-03-15 21:00", tz="UTC")  # EDT
    assert set(df["status"]) == {"adhoc"} and set(df["impact"]) == {"high"}
    assert set(df["source"]) == {"adhoc"}
    assert fc.fetch_adhoc(cfg, 2021, FA, {}).empty


def test_adhoc_csv_rejects_bad_rows(cfg):
    head = "date_local,time_local,tz,event_type,country,impact,note\n"
    with pytest.raises(fc.ParseError, match="impact"):
        fc.parse_adhoc_csv(head + "2020-03-03,10:00,America/New_York,X,US,huge,\n", cfg, FA)
    with pytest.raises(fc.ParseError, match="tz"):
        fc.parse_adhoc_csv(head + "2020-03-03,10:00,Europe/Berlin,X,US,high,\n", cfg, FA)
    with pytest.raises(fc.ParseError, match="Uhrzeit"):
        fc.parse_adhoc_csv(head + "2020-03-03,25:00,America/New_York,X,US,high,\n", cfg, FA)


# ------------------------------------------------------------ merge/dedupe ---
def _ev(cfg, et, d, hhmm="08:30", src="fred", fa=FA, country="US"):
    return fc.make_event(cfg, et, date.fromisoformat(d), hhmm, country, src, fa)


def test_combine_newest_source_wins(cfg):
    a = fc.to_frame([_ev(cfg, "FOMC_STATEMENT", "2020-03-15", "14:00", "fomc", FA)])
    b = fc.to_frame([{**_ev(cfg, "FOMC_STATEMENT", "2020-03-15", "17:00", "adhoc",
                            FA + pd.Timedelta(seconds=1)), "status": "adhoc"}])
    out = fc.combine_new([b, a])          # Reihenfolge egal, fetched_at entscheidet
    assert len(out) == 1 and out.iloc[0].source == "adhoc"
    assert out.iloc[0].ts_utc == pd.Timestamp("2020-03-15 21:00", tz="UTC")


def test_merge_rescheduled_cancelled_retained(cfg):
    prev = fc.to_frame([
        _ev(cfg, "CPI", "2026-01-13"), _ev(cfg, "CPI", "2026-02-11"),
        _ev(cfg, "CPI", "2026-03-11"), _ev(cfg, "CPI", "2026-04-10"),
        _ev(cfg, "ECB_RATE_DECISION", "2024-12-12", "14:15", "ecb", country="EA"),
    ])
    later = FA + pd.Timedelta(days=1)
    new = fc.to_frame([
        _ev(cfg, "CPI", "2026-01-13", fa=later),
        _ev(cfg, "CPI", "2026-02-11", "10:00", fa=later),      # Uhrzeit verschoben
        _ev(cfg, "CPI", "2026-04-10", fa=later),               # 2026-03-11 fehlt jetzt
        _ev(cfg, "CPI", "2026-05-12", fa=later),
        _ev(cfg, "ECB_RATE_DECISION", "2025-01-30", "14:15", "ecb", fa=later, country="EA"),
    ])
    out, ch = fc.merge_with_previous(prev, new, later)
    st = out.set_index("event_id")["status"]
    assert st["CPI:2026-01-13"] == "scheduled"
    assert st["CPI:2026-02-11"] == "rescheduled"
    assert st["CPI:2026-03-11"] == "cancelled"
    assert st["CPI:2026-05-12"] == "scheduled"
    # von der EZB-Seite gefallener alter Termin bleibt unveraendert erhalten
    assert st["ECB_RATE_DECISION:2024-12-12"] == "scheduled"
    assert [c["event_id"] for c in ch["rescheduled"]] == ["CPI:2026-02-11"]
    assert [c["event_id"] for c in ch["cancelled"]] == ["CPI:2026-03-11"]
    assert ch["added"] == ["CPI:2026-05-12", "ECB_RATE_DECISION:2025-01-30"]
    assert out.set_index("event_id").loc["CPI:2026-02-11"].ts_utc == pd.Timestamp("2026-02-11 15:00", tz="UTC")
    # rescheduled bleibt bei unveraenderter Zeit erhalten, cancelled wird nicht doppelt gezaehlt
    out2, ch2 = fc.merge_with_previous(out, new, later + pd.Timedelta(days=1))
    st2 = out2.set_index("event_id")["status"]
    assert st2["CPI:2026-02-11"] == "rescheduled" and st2["CPI:2026-03-11"] == "cancelled"
    assert ch2 == {"added": [], "rescheduled": [], "cancelled": []}
    assert str(out2["ts_utc"].dtype) == fc.TS_DTYPE


# ------------------------------------------------------------- validation ---
def _valid_table(cfg, years=(2024, 2025)):
    rows = []
    for y in years:
        for m in range(1, 13):
            d = date(y, m, 10)
            while d.weekday() >= 5:
                d += timedelta(days=1)
            for et in ("NFP", "CPI", "PCE"):
                rows.append(_ev(cfg, et, d.isoformat()))
    return fc.to_frame(rows)


def test_validate_ok(cfg):
    rep = fc.validate(_valid_table(cfg), cfg, 2024, date(2026, 4, 10))
    assert rep["high_per_year"] == {2024: 36, 2025: 36}


def test_validate_aborts_on_missing_high_impact(cfg):
    df = _valid_table(cfg)
    df = df[~((df["event_type"] == "CPI") & (df["ts_utc"].dt.year == 2025))]
    with pytest.raises(fc.ValidationError, match=r"\{2025: 24\}"):
        fc.validate(df.reset_index(drop=True), cfg, 2024, date(2026, 4, 10))
    # laufendes Jahr wird nicht gezaehlt, Jahre vor start_year auch nicht
    fc.validate(df.reset_index(drop=True), cfg, 2024, date(2025, 6, 1))
    fc.validate(df.reset_index(drop=True), cfg, 2026, date(2026, 4, 10))
    # cancelled zaehlt nicht
    df2 = _valid_table(cfg)
    df2.loc[df2["ts_utc"].dt.year == 2024, "status"] = "cancelled"
    with pytest.raises(fc.ValidationError, match="2024: 0"):
        fc.validate(df2, cfg, 2024, date(2026, 4, 10))


@pytest.mark.parametrize("breaker, msg", [
    (lambda d: d.assign(ts_utc=d["ts_utc"].dt.tz_localize(None)), "dtype"),
    (lambda d: d.assign(ts_utc=d["ts_utc"].where(d.index != 3)), "NaT"),
    (lambda d: d.assign(event_id=d["event_id"].where(d.index != 0, "CPI:2024-01-11")), "event_id"),
    (lambda d: pd.concat([d, d.iloc[[0]]], ignore_index=True), "doppelte"),
    (lambda d: d.assign(impact=d["impact"].where(d.index != 0, "medium")), "impact"),
    (lambda d: d.assign(status=d["status"].where(d.index != 0, "maybe")), "status"),
    (lambda d: d.assign(country=d["country"].where(d.index != 0, "DE")), "country"),
    (lambda d: d.drop(columns=["source"]), "Spalten"),
])
def test_validate_hard_errors(cfg, breaker, msg):
    with pytest.raises(fc.ValidationError, match=msg):
        fc.validate(breaker(_valid_table(cfg)), cfg, 2024, date(2026, 4, 10))


def test_validate_implausible_local_hour(cfg):
    df = fc.to_frame(_valid_table(cfg).to_dict("records") +
                     [_ev(cfg, "GDP", "2025-03-27", "03:30")])
    with pytest.raises(fc.ValidationError, match="Lokalzeit 03:30"):
        fc.validate(df, cfg, 2024, date(2026, 4, 10))


# ------------------------------------------------------------ HTTP / Mock ---
class FakeResp:
    def __init__(self, status=200, payload=None, text=None):
        self.status_code = status
        self._payload = payload
        self.text = text if text is not None else json.dumps(payload)

    def json(self):
        if self._payload is None:
            return json.loads(self.text)
        return self._payload


FRED_NAMES = {50: "Employment Situation", 10: "Consumer Price Index",
              54: "Personal Income and Outlays", 53: "Gross Domestic Product",
              46: "Producer Price Index", 9: "Advance Monthly Sales for Retail and Food Services",
              180: "Unemployment Insurance Weekly Claims Report"}


def _weekday(d):
    while d.weekday() >= 5:
        d += timedelta(days=1)
    return d


def fred_dates(rid, years=(2023, 2024, 2025, 2026)):
    if rid == 180:                         # woechentlich donnerstags
        d, out = date(years[0], 1, 5), []
        while d.year <= years[-1]:
            out.append(d)
            d += timedelta(weeks=1)
        return out
    day = {50: 3, 10: 12, 54: 27, 53: 26, 46: 14, 9: 16}[rid]
    return [_weekday(date(y, m, day)) for y in years for m in range(1, 13)]


class FakeNet:
    """Routet requests.get nach URL; Antworten im Test anpassbar."""

    def __init__(self):
        self.names = dict(FRED_NAMES)
        self.dates = {rid: fred_dates(rid) for rid in FRED_NAMES}
        self.fomc = FOMC_CURRENT
        self.ecb = ECB_HTML
        self.ff = []
        self.hist = {2020: FOMC_2020}
        self.fail: dict[str, int] = {}          # url-Teil -> HTTP-Status
        self.calls: list[tuple[str, dict]] = []

    def get(self, url, params=None, headers=None, timeout=None):
        params = dict(params or {})
        self.calls.append((url, params))
        assert headers["User-Agent"] == "marketdata-pipeline/1.0"
        for part, status in self.fail.items():
            if part in url:
                return FakeResp(status, {"error_code": status, "error_message": "kaputt"})
        if url.endswith("/fred/release"):
            rid = int(params["release_id"])
            return FakeResp(200, {"releases": [{"id": rid, "name": self.names[rid]}]})
        if url.endswith("/fred/release/dates"):
            rid = int(params["release_id"])
            assert params["include_release_dates_with_no_data"] == "true"
            assert params["realtime_start"] == "2000-01-01" and params["realtime_end"] == "9999-12-31"
            items = [{"release_id": rid, "date": d.isoformat()} for d in self.dates[rid]]
            off, lim = int(params.get("offset", 0)), int(params["limit"])
            return FakeResp(200, {"count": len(items), "offset": off, "limit": lim,
                                  "release_dates": items[off:off + lim]})
        if url.endswith("fomccalendars.htm"):
            return FakeResp(200, text=self.fomc)
        m = re.search(r"fomchistorical(\d{4})\.htm$", url)
        if m:
            y = int(m[1])
            return FakeResp(200, text=self.hist[y]) if y in self.hist else FakeResp(404, text="nf")
        if "ecb.europa.eu" in url:
            return FakeResp(200, text=self.ecb)
        if "faireconomy" in url:
            return FakeResp(200, self.ff)
        raise AssertionError(f"unerwartete URL {url}")


@pytest.fixture
def net(monkeypatch):
    n = FakeNet()
    monkeypatch.setattr(fc.requests, "get", n.get)
    monkeypatch.setenv("FRED_API_KEY", API_KEY)
    return n


def test_http_retry_backoff_and_redaction(monkeypatch, no_sleep):
    seq = [requests.ConnectionError(f"boom https://x/?api_key={API_KEY}"), FakeResp(503, {}),
           FakeResp(200, {"ok": 1})]

    def get(url, **kw):
        r = seq.pop(0)
        if isinstance(r, Exception):
            raise r
        return r
    monkeypatch.setattr(fc.requests, "get", get)
    assert fc.http_get("https://x/", backoff=1.0, pause=0.5).json() == {"ok": 1}
    assert no_sleep == [1.0, 2.0, 0.5]

    monkeypatch.setattr(fc.requests, "get",
                        lambda url, **kw: (_ for _ in ()).throw(
                            requests.ConnectionError(f"https://x/?api_key={API_KEY}")))
    with pytest.raises(fc.DownloadError) as ei:
        fc.http_get("https://x/", retries=1, secrets=[API_KEY])
    assert API_KEY not in str(ei.value) and "***" in str(ei.value)


def test_http_other_error_no_retry(monkeypatch, no_sleep):
    calls = []
    monkeypatch.setattr(fc.requests, "get",
                        lambda url, **kw: calls.append(url) or FakeResp(400, {"error_message": "Bad api_key"}))
    with pytest.raises(fc.DownloadError, match="400 \\(Bad api_key\\)"):
        fc.http_get("https://x/")
    assert len(calls) == 1


def test_fetch_fred_pagination_and_name_check(cfg, net):
    c = copy.deepcopy(cfg)
    c["fred"]["page_limit"] = 20
    ctx = {}
    df = fc.fetch_fred(c, 2024, FA, ctx)
    assert ctx["meta"]["releases"]["JOBLESS_CLAIMS"]["dates"] == len(
        [d for d in fred_dates(180) if d.year >= 2024])
    pages = [p for u, p in net.calls if u.endswith("/release/dates") and p["release_id"] == 180]
    assert len(pages) > 5 and [p["offset"] for p in pages[:3]] == [0, 20, 40]
    assert set(df["source"]) == {"fred"} and df["ts_utc"].dt.year.min() == 2024
    # Namens-Check vor Nutzung: falsche ID -> harter Abbruch, Termine nie abgefragt
    net.names[54] = "Industrial Production and Capacity Utilization"
    net.calls.clear()
    with pytest.raises(fc.ConfigError, match="release_id=54"):
        fc.fetch_fred(cfg, 2024, FA, {})
    assert not any(u.endswith("/release/dates") and p["release_id"] == 54 for u, p in net.calls)


def test_fetch_fred_requires_key(cfg, monkeypatch):
    monkeypatch.delenv("FRED_API_KEY", raising=False)
    with pytest.raises(fc.ConfigError, match="FRED_API_KEY"):
        fc.fetch_fred(cfg, 2024, FA, {})


HIST = {
    2021: ["January 26-27 Meeting", "March 16-17 Meeting SEP", "April 27-28 Meeting",
           "June 15-16 Meeting SEP", "July 27-28 Meeting", "September 21-22 Meeting SEP",
           "November 2-3 Meeting", "December 14-15 Meeting SEP"],
    2022: ["January 25-26 Meeting", "March 15-16 Meeting SEP", "May 3-4 Meeting",
           "June 14-15 Meeting SEP", "July 26-27 Meeting", "September 20-21 Meeting SEP",
           "November 1-2 Meeting", "December 13-14 Meeting SEP"],
    2023: ["January 31-February 1 Meeting", "March 21-22 Meeting SEP", "May 2-3 Meeting",
           "June 13-14 Meeting SEP", "July 25-26 Meeting", "September 19-20 Meeting SEP",
           "October 31-November 1 Meeting", "December 12-13 Meeting SEP"],
}


def test_fetch_fomc_historical_pages_and_cache(cfg, net, tmp_path):
    net.hist.update({y: _historical(y, h) for y, h in HIST.items()})
    ctx = {}
    df = fc.fetch_fomc(cfg, 2020, FA, ctx, tmp_path)
    assert ctx["meta"]["historical_years"] == [2020, 2021, 2022, 2023]
    assert ctx["meta"]["meetings_per_year"] == {2020: 7, 2021: 8, 2022: 8, 2023: 8,
                                                2024: 8, 2025: 8, 2026: 8}
    assert "FOMC_STATEMENT:2023-11-01" in set(df["event_id"])
    cached = sorted(p.name for p in (tmp_path / "raw" / "fomc").iterdir())
    assert cached == [f"fomchistorical{y}.htm" for y in range(2020, 2024)]
    # zweiter Lauf: historische Seiten aus dem Cache, nur die aktuelle Seite wird geladen
    net.calls.clear()
    df2 = fc.fetch_fomc(cfg, 2020, FA, {}, tmp_path)
    assert [u for u, _ in net.calls] == [cfg["fomc"]["current_url"]]
    pd.testing.assert_frame_equal(df, df2)


def test_fetch_fomc_missing_historical_page_is_error(cfg, net, tmp_path):
    with pytest.raises(fc.DownloadError, match="404"):
        fc.fetch_fomc(cfg, 2019, FA, {}, tmp_path)


def test_fetch_fomc_bad_historical_page_not_cached(cfg, net, tmp_path):
    net.hist[2023] = _historical(2023, HIST[2023][:5])
    with pytest.raises(fc.ParseError, match="2023: 5"):
        fc.fetch_fomc(cfg, 2023, FA, {}, tmp_path)
    assert not (tmp_path / "raw" / "fomc" / "fomchistorical2023.htm").exists()


# ------------------------------------------------------------ ganzer Lauf ---
def _files(root):
    return sorted(str(p.relative_to(root)) for p in root.rglob("*") if p.is_file())


def test_full_run_writes_events_snapshot_report(cfg, net, tmp_path):
    code, rep = fc.run(["fred", "fomc", "ecb", "adhoc"], 2024, tmp_path, now=NOW, cfg=cfg)
    assert code == 0, rep
    ev = pd.read_parquet(fc.events_path(tmp_path))
    assert list(ev.columns) == fc.COLUMNS
    assert str(ev["ts_utc"].dtype) == "datetime64[ns, UTC]"
    assert str(ev["fetched_at"].dtype) == "datetime64[ns, UTC]"
    snaps = list((tmp_path / "calendar" / "snapshots").glob("events-*.parquet"))
    assert [p.name for p in snaps] == ["events-20251104T051000Z.parquet"]
    pd.testing.assert_frame_equal(pd.read_parquet(snaps[0]), ev)
    report = json.loads((tmp_path / "calendar" / "reports" / "calendar-20251104T051000Z.json").read_text())
    assert report["exit_code"] == 0 and report["sources"]["fred"]["status"] == "ok"
    assert API_KEY not in json.dumps(report)
    assert not list(tmp_path.rglob("*.part"))
    e = ev.set_index("event_id")
    assert e.loc["NFP:2024-01-03"].ts_utc == pd.Timestamp("2024-01-03 13:30", tz="UTC")
    assert e.loc["FOMC_STATEMENT:2024-05-01"].source == "fomc"
    assert e.loc["ECB_RATE_DECISION:2025-01-30"].country == "EA"
    assert rep["validation"]["high_per_year"][2024] >= 30
    assert report["validation"]["high_per_year"]["2025"] >= 30
    assert "FOMC_STATEMENT:2020-03-15" not in e.index            # vor --start-year


def test_second_run_rescheduled_cancelled_and_new_snapshot(cfg, net, tmp_path):
    assert fc.run(["fred", "fomc", "ecb", "adhoc"], 2024, tmp_path, now=NOW, cfg=cfg)[0] == 0
    # CPI 12.05.2026 -> 13.05.2026 (neue event_id), EZB-Uhrzeit geaendert (gleiche event_id)
    net.dates[10] = [d if d != date(2026, 5, 12) else date(2026, 5, 13) for d in net.dates[10]]
    c = copy.deepcopy(cfg)
    c["ecb"]["time_rules"][-1]["decision"] = "14:00"
    later = NOW + timedelta(days=1)
    code, rep = fc.run(["fred", "fomc", "ecb", "adhoc"], 2024, tmp_path, now=later, cfg=c)
    assert code == 0, rep
    e = pd.read_parquet(fc.events_path(tmp_path)).set_index("event_id")
    assert e.loc["CPI:2026-05-12"].status == "cancelled"
    assert e.loc["CPI:2026-05-13"].status == "scheduled"
    assert e.loc["ECB_RATE_DECISION:2025-01-30"].status == "rescheduled"
    assert e.loc["ECB_RATE_DECISION:2025-01-30"].ts_utc == pd.Timestamp("2025-01-30 13:00", tz="UTC")
    assert e.loc["ECB_PRESS_CONF:2025-01-30"].status == "scheduled"
    assert len(list((tmp_path / "calendar" / "snapshots").glob("*.parquet"))) == 2
    assert {c["event_id"] for c in rep["changes"]["cancelled"]} == {"CPI:2026-05-12"}


ALL = ["fred", "fomc", "ecb", "adhoc"]


def _rows(root, source):
    df = pd.read_parquet(fc.events_path(root))
    return df[df["source"] == source].sort_values("event_id").reset_index(drop=True)


@pytest.mark.parametrize("breakage", ["http", "plausibility"])
def test_source_failure_keeps_its_events_and_updates_others(cfg, net, tmp_path, breakage):
    assert fc.run(ALL, 2024, tmp_path, now=NOW, cfg=cfg)[0] == 0
    ecb_before = _rows(tmp_path, "ecb")
    assert len(ecb_before) == 20
    if breakage == "http":
        net.fail["ecb.europa.eu"] = 503
    else:                                      # Seite umgebaut: 2025 nur noch 2 Termine erkannt
        net.ecb = ECB_HTML.replace("monetary policy meeting, Day 2", "Sitzung, Tag 2", 6)
    # gleichzeitig neuer CPI-Termin bei FRED
    net.dates[10] = net.dates[10] + [date(2027, 1, 13)]
    later = NOW + timedelta(days=1)
    code, rep = fc.run(ALL, 2024, tmp_path, now=later, cfg=cfg)
    assert code == 3
    assert rep["sources_failed"] == ["ecb"]
    assert rep["sources"]["ecb"]["status"] == "error"
    assert rep["sources"]["ecb"]["kept_from_previous"] == 20
    assert rep["written"] is not None
    # EZB-Zeilen exakt unveraendert (auch status und fetched_at), nicht cancelled
    pd.testing.assert_frame_equal(_rows(tmp_path, "ecb"), ecb_before)
    assert not any(c["source"] == "ecb" for c in rep["changes"]["cancelled"])
    # uebrige Quellen aktualisiert
    e = pd.read_parquet(fc.events_path(tmp_path)).set_index("event_id")
    assert e.loc["CPI:2027-01-13"].fetched_at == pd.Timestamp(later)
    assert e.loc["CPI:2027-01-13"].status == "scheduled"
    assert len(list((tmp_path / "calendar" / "snapshots").glob("*.parquet"))) == 2
    saved = json.loads((tmp_path / "calendar" / "reports" /
                        f"calendar-{fc.stamp(later)}.json").read_text())
    assert saved["exit_code"] == 3 and saved["sources_failed"] == ["ecb"]


def test_failed_source_then_recovers(cfg, net, tmp_path):
    assert fc.run(ALL, 2024, tmp_path, now=NOW, cfg=cfg)[0] == 0
    net.fail["ecb.europa.eu"] = 503
    assert fc.run(ALL, 2024, tmp_path, now=NOW + timedelta(days=1), cfg=cfg)[0] == 3
    del net.fail["ecb.europa.eu"]
    code, rep = fc.run(ALL, 2024, tmp_path, now=NOW + timedelta(days=2), cfg=cfg)
    assert code == 0 and rep["sources_failed"] == []
    assert rep["changes"]["cancelled"] == [] and rep["changes"]["rescheduled"] == []


def test_first_run_missing_source_writes_if_valid(cfg, net, tmp_path):
    # ohne FOMC reichen NFP+CPI+PCE (36) fuer 2024 -> wird geschrieben, Exit 3
    net.fomc = "<html><body>Seite in Wartung</body></html>"
    code, rep = fc.run(ALL, 2024, tmp_path, now=NOW, cfg=cfg)
    assert code == 3 and "ParseError" in rep["sources"]["fomc"]["error"]
    assert rep["sources"]["fomc"]["kept_from_previous"] == 0
    ev = pd.read_parquet(fc.events_path(tmp_path))
    assert "fomc" not in set(ev["source"]) and {"fred", "ecb"} <= set(ev["source"])
    assert len(list((tmp_path / "calendar" / "snapshots").glob("*.parquet"))) == 1


def test_first_run_missing_source_invalid_writes_nothing(cfg, net, tmp_path):
    # ohne FRED nur FOMC (16 High-Impact 2024) -> Validierung schlaegt fehl, Exit 2
    net.fail["stlouisfed"] = 500
    code, rep = fc.run(ALL, 2024, tmp_path, now=NOW, cfg=cfg)
    assert code == 2 and "zu wenige High-Impact" in rep["validation_error"]
    assert rep["sources_failed"] == ["fred"]
    assert not fc.events_path(tmp_path).exists()
    assert not (tmp_path / "calendar" / "snapshots").exists()


def test_fred_wrong_id_aborts_fred_only(cfg, net, tmp_path):
    assert fc.run(ALL, 2024, tmp_path, now=NOW, cfg=cfg)[0] == 0
    fred_before = _rows(tmp_path, "fred")
    net.names[10] = "Industrial Production and Capacity Utilization"
    net.calls.clear()
    code, rep = fc.run(ALL, 2024, tmp_path, now=NOW + timedelta(days=1), cfg=cfg)
    assert code == 4 and "release_id=10" in rep["sources"]["fred"]["error"]
    # harter Abbruch der Quelle: keine FRED-Termine mehr abgefragt, alter Stand bleibt
    assert not any(u.endswith("/release/dates") and p["release_id"] in (10, 54, 53, 46, 9, 180)
                   for u, p in net.calls)
    pd.testing.assert_frame_equal(_rows(tmp_path, "fred"), fred_before)


def test_fred_wrong_id_first_run_writes_nothing(cfg, net, tmp_path):
    net.names[10] = "Industrial Production and Capacity Utilization"
    code, rep = fc.run(ALL, 2024, tmp_path, now=NOW, cfg=cfg)
    assert code == 2 and rep["sources_failed"] == ["fred"]
    assert not fc.events_path(tmp_path).exists()


def test_failed_source_plus_mass_cancellation_writes_nothing(cfg, net, tmp_path):
    assert fc.run(ALL, 2024, tmp_path, now=NOW, cfg=cfg)[0] == 0
    before = fc.events_path(tmp_path).read_bytes()
    net.fail["ecb.europa.eu"] = 503
    net.dates[50] = net.dates[50][::2]
    code, rep = fc.run(ALL, 2024, tmp_path, now=NOW + timedelta(days=1), cfg=cfg)
    assert code == 2 and "gestrichen" in rep["validation_error"]
    assert fc.events_path(tmp_path).read_bytes() == before


def test_validation_abort_writes_nothing(cfg, net, tmp_path):
    # ohne FRED fehlen NFP/CPI/PCE -> 2024/2025 unter 30 High-Impact-Events
    code, rep = fc.run(["fomc", "ecb", "adhoc"], 2024, tmp_path, now=NOW, cfg=cfg)
    assert code == 2 and "zu wenige High-Impact" in rep["validation_error"]
    assert not fc.events_path(tmp_path).exists()
    assert len(list((tmp_path / "calendar" / "reports").glob("*.json"))) == 1


def test_mass_cancellation_aborts(cfg, net, tmp_path):
    assert fc.run(["fred", "fomc", "ecb", "adhoc"], 2024, tmp_path, now=NOW, cfg=cfg)[0] == 0
    before = fc.events_path(tmp_path).read_bytes()
    # FRED liefert fuer NFP ploetzlich nur noch jeden zweiten Termin
    net.dates[50] = net.dates[50][::2]
    code, rep = fc.run(["fred"], 2024, tmp_path, now=NOW + timedelta(days=1), cfg=cfg)
    assert code == 2 and "gestrichen" in rep["validation_error"]
    assert fc.events_path(tmp_path).read_bytes() == before


def test_dry_run_writes_nothing(cfg, net, tmp_path):
    code, rep = fc.run(["fred", "fomc", "ecb", "adhoc"], 2024, tmp_path, dry_run=True, now=NOW, cfg=cfg)
    assert code == 0 and rep["written"] is None
    assert _files(tmp_path) == []


def test_ff_crosscheck_report_only(cfg, net, tmp_path):
    net.ff = [
        {"title": "CPI m/m", "country": "USD", "date": "2026-05-12T08:30:00-04:00", "impact": "High"},
        {"title": "Core CPI m/m", "country": "USD", "date": "2026-05-12T08:30:00-04:00", "impact": "High"},
        {"title": "Unemployment Claims", "country": "USD", "date": "2026-05-14T09:30:00-04:00",
         "impact": "Medium"},                                     # 60 min Abweichung
        {"title": "Non-Farm Employment Change", "country": "USD",
         "date": "2026-05-08T08:30:00-04:00", "impact": "High"},  # bei uns 2026-05-04
        {"title": "German Prelim CPI m/m", "country": "EUR", "date": "2026-05-12T08:00:00-04:00"},
    ]
    code, rep = fc.run(["fred", "fomc", "ecb", "adhoc", "ff"], 2024, tmp_path, now=NOW, cfg=cfg)
    assert code == 0, rep
    ff = rep["ff"]
    assert ff["status"] == "ok" and ff["checked"] == 3 and ff["ok"] == 1
    assert [d["event_id"] for d in ff["deviations"]] == ["JOBLESS_CLAIMS:2026-05-14"]
    assert ff["deviations"][0]["diff_minutes"] == 60
    assert [m["event_id"] for m in ff["missing_in_ours"]] == ["NFP:2026-05-08"]
    e = pd.read_parquet(fc.events_path(tmp_path)).set_index("event_id")
    assert "NFP:2026-05-08" not in e.index                       # nichts uebernommen
    assert e.loc["JOBLESS_CLAIMS:2026-05-14"].ts_utc == pd.Timestamp("2026-05-14 12:30", tz="UTC")
    saved = json.loads(next((tmp_path / "calendar" / "reports").glob("*.json")).read_text())
    assert saved["ff"]["deviations"][0]["event_id"] == "JOBLESS_CLAIMS:2026-05-14"


def test_ff_failure_is_not_fatal(cfg, net, tmp_path):
    net.fail["faireconomy"] = 500
    code, rep = fc.run(["fred", "fomc", "ecb", "adhoc", "ff"], 2024, tmp_path, now=NOW, cfg=cfg)
    assert code == 0 and rep["ff"]["status"] == "error"
    assert fc.events_path(tmp_path).exists()


def test_unknown_source_rejected(cfg, tmp_path):
    with pytest.raises(fc.ConfigError):
        fc.run(["bloomberg"], 2024, tmp_path, now=NOW, cfg=cfg)


def test_default_data_root(monkeypatch, tmp_path):
    monkeypatch.delenv("MARKETDATA_DATA_ROOT", raising=False)
    assert fc.default_data_root() == fc.ROOT / "data"
    monkeypatch.setenv("MARKETDATA_DATA_ROOT", str(tmp_path))
    assert fc.default_data_root() == tmp_path
