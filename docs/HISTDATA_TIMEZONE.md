# HistData-Tickdaten: Wie die Zeitstempel auf UTC abgebildet werden

Dieser Artikel erklärt, wie die Zeitstempel der Tick-Dateien von HistData.com
(Generic ASCII, `DAT_ASCII_{SYM}_T_{YYYYMM}.csv`) nach UTC umgerechnet werden.
Er beschreibt außerdem, welche Annahmen dabei widerlegt wurden und wie die Pipeline
verhindert, dass falsch gestempelte Daten in Backtests landen. Alle Aussagen
beziehen sich auf XAUUSD und XAGUSD im Tick-Modus. Die Zahlen nennen ihre
Quelldatei im Repository (Pfade relativ zur Repository-Wurzel).

**Kurzfassung**

| Rohmonat (HistData-Datei) | Stempelungsregel | Rohzeit − UTC |
|---|---|---|
| bis einschließlich 2018-11 | `ny_local` | Ortszeit America/New_York: −5 h (EST) bzw. −4 h (EDT) |
| ab 2018-12 | `berlin_minus_6_utc_midnight` | EU-Sommerzeitstatus um 00:00 UTC des UTC-Tages, minus 6 h: −5 h im EU-Winter, −4 h im EU-Sommer |

Quelle: `config/histdata_timezone.yaml` (`schedule.tick`) und `scripts/fetch_histdata.py`
(`TZ_RULES`, `DEFAULT_TZ_SCHEDULE`).

---

## 1. Problem

HistData schreibt für jedes Format: *"TimeZone: Eastern Standard Time (EST)
time-zone WITHOUT Day Light Savings adjustments"*. Das hieße: Rohzeit = UTC − 5 h,
das ganze Jahr. Drittprojekte übernehmen diese Angabe wörtlich, zum Beispiel mit
einem festen Offset von 18 000 000 ms bzw. `-05:00` (Belege in
`docs/histdata_research.md`, Abschnitt 4).

Die Messungen an echten Dateien zeigen etwas anderes (Abschnitte 2 und 4).
Eine falsche Stunde ist kein Schönheitsfehler. Jede Strategie, die an
Sessionzeiten, Tagesschlüsse, News-Sperren oder Intraday-Fenster gebunden ist,
sieht dann um eine Stunde verschobene Kurse. Das betrifft große Teile jedes Jahres,
und ein Backtest bemerkt den Fehler nicht. Die Tagespause des Goldmarkts liegt
dann scheinbar zur falschen Uhrzeit. Ein „Breakout zur Londoner Eröffnung“
handelt eine andere Stunde. Ein Vergleich mit Broker- oder Live-Daten weicht in
der Sommerzeit systematisch ab.

Der Markt selbst bleibt New-York-basiert. Die erwarteten Marktzeiten sind die
CME-Sessionzeiten: Wochenschluss Freitag 17:00, Wochenöffnung Sonntag 18:00 und
Tagespause 17:00–18:00 America/New_York. Nur die Stempelung der Dateien
weicht ab (`docs/histdata_research.md`, Abschnitt 4, und `scripts/fetch_histdata.py`,
Docstring von `check_timezone`).

## 2. Hypothesen und wie sie widerlegt wurden

Der Weg zur endgültigen Regel lief über vier Hypothesen. Jede wurde an echten
Dateien geprüft, die der Betreiber geladen hat. Die Reihenfolge ergibt sich aus
der Commit-Historie (`git log`, Commits `ea5b3e7` bis `c4e496e`).

| # | Hypothese | Widerlegt durch | Quelle |
|---|---|---|---|
| 1 | Fest EST, Rohzeit = UTC − 5 h (`fixed_est`) | Juli 2024: alle 38 Anker bei −4 h | Commit `bf14490` |
| 2 | America/New_York mit Sommerzeit (`ny_local`) | März 2024: Rohzeit bleibt nach der US-Umstellung bis zur EU-Umstellung bei −5 h | Commit `992454e`, `docs/histdata_research.md` |
| 3 | Europe/Berlin − 6 h, Umstellung So 01:00 UTC (`berlin_minus_6`) | Oktober: Rücksprung roh **Sonntag** 19:59:59 → 19:00:00 bei offenem Markt | Commits `eb56d86`, `ea79521` |
| 4 | Europe/Berlin − 6 h, Umstellung Mo 00:00 UTC (`berlin_minus_6_utc_midnight`) | gilt (ab Rohmonat 2018-12) | `config/histdata_timezone.yaml` |

### 2.1 Fest EST → widerlegt durch die Juli-Anker

Der erste Fetcher (Commit `ea5b3e7`) rechnete wie dokumentiert mit UTC + 5 h. Er
prüfte die Zeitzone aber schon damals je Monat an den Marktlücken. Die Messung
an XAUUSD ergab (Commit `bf14490`):

- 2024-01: impliziter Offset −5 h. Das passt zu EST.
- 2024-07: **alle 38 Anker bei −4 h**. Tagespause roh 16:59:58 → 18:00:01,
  Wochenschluss Fr 16:59:58, Wochenstart So 18:00:01. Das sind genau die
  New-Yorker Sessionzeiten in Sommerzeit.

Ein fester Offset kann im Juli nicht −4 h zeigen. Damit war „EST ohne
Sommerzeit“ für die Tick-Dateien widerlegt. Der nächste Ansatz war New-York-Ortszeit.

### 2.2 America/New_York → widerlegt durch März 2024

Juli allein unterscheidet nicht zwischen „New York mit Sommerzeit“ und „irgendeiner
Zone mit Sommerzeit“. Die Messung an allen drei damals geladenen Symbolen
(XAUUSD, XAGUSD, EURUSD) zeigte (Commit `992454e`, Tabelle in
`docs/histdata_research.md`, Abschnitt 4):

| Rohmonat | Beobachtung | Folgerung |
|---|---|---|
| 2024-01 | −5 h | passt zu beiden |
| 2024-03 | ab 10.03. (US-Umstellung) bis zur EU-Umstellung am 31.03. **weiter −5 h**: Tagespause roh 15:59:59 → 17:00:00 statt 16:59 → 18:00 | nicht New York |
| 2024-07 | −4 h | passt zu beiden |
| 2024-10/11 | ab 27.10. (EU-Umstellung) schon −5 h, nicht erst ab 03.11. | nicht New York |

Der Offset folgt also dem **europäischen** Sommerzeitkalender. Das führte zur Regel
`berlin_minus_6`: Rohzeit = Europe/Berlin-Ortszeit − 6 h, also UTC−5 im EU-Winter
und UTC−4 im EU-Sommer.

### 2.3 Berlin − 6 h mit Umstellung So 01:00 UTC → widerlegt durch den Oktober-Rücksprung

Unter `berlin_minus_6` stellt die Uhr zur gleichen Zeit um wie Berlin, also am
letzten Sonntag im Oktober um 01:00 UTC. Die doppelte Rohstunde läge dann bei roh
**Samstag** 20:00–20:59, bei geschlossenem Markt. Ein Sonntagabend-Rücksprung
dürfte es nicht geben.

Der erste Survey (Commit `23cf0b1`) sah in 2021-10 und 2024-10 nur je einen
Rücksprung von 60 min und deutete ihn als „verirrte Wochenend-Ticks“. Der
erweiterte Survey 2009–2025 (Commit `eb56d86`) zeigte das Gegenteil. **Jeder
Oktober seit 2019** enthält Tausende echter Ticks in einem Rücksprung am
EU-Umstellungs-**Sonntag**: roh 19:59:59 → 19:00:00. Die Tickzahlen stehen in
Abschnitt 4.3. Der Markt ist dann offen, denn die Sonntagsöffnung ist 18:00 EDT,
also 22:00 UTC.

Unter `berlin_minus_6` wäre roh So 19:59:59 bereits Mo 00:59:59 UTC (−5 h). Der
nächste Tick roh 19:00:00 wäre Mo 00:00:00 UTC, ein Zeitsprung rückwärts in der
Dateireihenfolge. Das passt nur, wenn der Offset bis So 23:59:59 UTC noch −4 h
war und erst ab Mo 00:00:00 UTC −5 h gilt. Die Deutung als Wochenend-Ticks
(roh Sa 21:00 → 20:00) war falsch. So vermerken es `config/histdata_timezone.yaml`
und `docs/histdata_research.md` ausdrücklich.

### 2.4 Endgültige Regel

`berlin_minus_6_utc_midnight` (Commit `eb56d86`): Der Offset entspricht dem
EU-Sommerzeitstatus **um 00:00 UTC des UTC-Tages** des Ticks, minus 6 h. Der
Wechsel wirkt also erst ab Montag 00:00 UTC nach dem letzten Sonntag im März
bzw. Oktober. Ein Kontroll-Survey mit dem neuen Fahrplan endete mit Exit 0. Alle
Umstellungsnächte 2019–2025 waren für beide Metalle `ok` (Commit `ea79521`;
Kommentar zu `REAL_NIGHTS` in `tests/test_histdata_tz_v7.py`).

Für die Jahre bis 2018 gilt weiter `ny_local`. Der Survey fand 2010, 2012, 2015
und 2018 eindeutig `ny_local` (März und Okt/Nov, beide Metalle;
`config/histdata_timezone.yaml`). Warum HistData die Stempelung geändert hat, ist
unbekannt (Abschnitt 6).

## 3. Das endgültige Modell

### 3.1 Fahrplan je Rohmonat

```yaml
# config/histdata_timezone.yaml
schedule:
  tick:
    - {from: "2000-01", rule: ny_local}
    - {from: "2018-12", rule: berlin_minus_6_utc_midnight}
  m1:
    - {from: "2000-01", rule: fixed_est}     # M1 unverifiziert
```

Ein Wechsel gilt nur an **Rohmonatsgrenzen**, also an Monaten der HistData-Datei,
nicht an UTC-Monaten. Ein UTC-Monat M setzt sich aus den letzten 4 h (EU-Sommer)
bzw. 5 h (EU-Winter) des Rohmonats M−1 und dem Rohmonat M ohne diese Stunden an
seinem Ende zusammen. Er wird nur geschrieben, wenn beide Rohmonate erledigt sind
(`DATA.md`, Abschnitt „Zeitzone und Monatsgrenze“).

### 3.2 Die Regel `berlin_minus_6_utc_midnight`

Definition in `scripts/fetch_histdata.py`:

```python
TzRule("berlin_minus_6_utc_midnight", "Europe/Berlin", -6, kind="dst_daily_utc")
```

Für einen UTC-Zeitpunkt *t* gilt:

```
Rohzeit(t) − t = utcoffset(Europe/Berlin, 00:00 UTC des Kalendertags von t) − 6 h
```

Das ergibt −5 h im EU-Winter und −4 h im EU-Sommer. Die Umstellung fällt immer auf
**Montag 00:00 UTC** nach dem letzten Sonntag im März bzw. Oktober, nicht auf
Sonntag 01:00 UTC wie in Berlin. Die Regelart `dst_daily_utc` ist in der Pipeline
allgemein definiert. Eigene Regeln dieser Art lassen sich in der YAML anlegen
(`config/histdata_timezone.yaml`, Kommentar zu `rules`).

### 3.3 März: der Sprung (nicht existierende Rohstunde)

Am EU-Umstellungssonntag im März (Beispiel 2024-03-31):

| UTC | Offset | Rohzeit |
|---|---|---|
| So 23:59:59 | −5 h | So **18:59:59** |
| Mo 00:00:00 | −4 h | So **20:00:00** |

Die Rohzeit springt von 18:59:59 auf 20:00:00. **Roh Sonntag 19:00:00–19:59:59.999
existiert nicht.** Ein Tick in dieser Stunde kann nicht korrekt gestempelt sein.
`raw_to_utc_ms` wirft dann `TimezoneError`, und der Rohmonat geht in Quarantäne.
Der Markt ist in dieser Nacht offen. Die Sonntagsöffnung 18:00 EDT = 22:00 UTC
liegt bei −5 h auf roh 17:00, die Lücke liegt also mitten im Handel
(`docs/histdata_research.md`, Abschnitt 4).

### 3.4 Oktober: die Falte (doppelte Rohstunde)

Am EU-Umstellungssonntag im Oktober (Beispiel 2024-10-27):

| UTC | Offset | Rohzeit |
|---|---|---|
| So 23:00:00–23:59:59 | −4 h | So 19:00:00–19:59:59 (1. Durchgang) |
| Mo 00:00:00–00:59:59 | −5 h | So 19:00:00–19:59:59 (2. Durchgang) |

**Die Rohstunde Sonntag 19:00–19:59:59.999 kommt zweimal vor.** Aus der Rohzeit
allein lässt sich ein Tick in dieser Stunde nicht zuordnen. Die Pipeline löst die
Falte über die **Dateireihenfolge** auf, denn die Datei ist in UTC-Reihenfolge
geschrieben (`scripts/fetch_histdata.py`, `raw_to_utc_ms` und `fold_reversal`):

1. Gesucht wird der erste Index *i* mit raw[i] < raw[i−1] − 30 min, wobei raw[i]
   in der doppelten Stunde liegt. Die Schwelle steht in `REGRESSION_MAX_MS = 30 * MIN_MS`.
2. Ticks der Stunde vor *i* bekommen −4 h, Ticks ab *i* bekommen −5 h.
3. Die so aufgelöste Nacht muss in Dateireihenfolge monotone UTC-Zeiten ergeben.
   Die Toleranz beträgt 5 s (`FOLD_JITTER_MS = 5_000`).
4. Gibt es keinen solchen Rücksprung (z. B. keine Ticks auf einer Seite) oder ist
   das Ergebnis nicht monoton, folgt `TimezoneError` und Quarantäne. Es wird nie
   geraten und nie etwas verworfen.

Derselbe Detektor (`fold_reversal`) dient der Umrechnung, der Monatsprüfung
(`expected_reversals`) und dem Bericht (`transition_nights`). Es gibt also nur eine
Definition des Rücksprungs (Commit `6449982`).

### 3.5 Warum Monate ohne Umstellungsfenster unter beiden Regeln identisch sind

`ny_local` und `berlin_minus_6_utc_midnight` liefern beide −5 h im Winter und
−4 h im Sommer. Sie unterscheiden sich nur dort, wo die USA schon (bzw. noch)
Sommerzeit haben, die EU-Tagesregel aber nicht. Das sind zwei Fenster je Jahr. In
beiden gilt `ny_local` = −4 h und die Tagesregel = −5 h:

- **März:** vom US-Beginn (2. Sonntag im März, 07:00 UTC) bis Mo 00:00 UTC nach dem
  EU-Beginn.
- **Okt/Nov:** von Mo 00:00 UTC nach dem EU-Ende bis zum US-Ende (1. Sonntag im
  November, 06:00 UTC).

Außerhalb dieser Fenster ist die Abbildung Rohzeit → UTC unter beiden Regeln
dieselbe. Ein Rohmonat ohne Fensterstunden wird daher Tick für Tick gleich
umgerechnet, egal welche der beiden Regeln für ihn eingetragen ist. Nach der
Rechnung des Surveys haben in der Praxis alle Rohmonate 03, 10 und 11 Fenster,
alle anderen keine (`DATA.md`, Abschnitt „Zeitzonen-Survey“).

Fenster 2009–2025 (UTC, Beginn inklusive, Ende exklusive). Quelle:
`config/histdata_timezone.yaml`, dort mit zoneinfo auf der Viertelstunde berechnet.
Für diesen Artikel wurde die Tabelle unabhängig mit Python `zoneinfo` im
15-Minuten-Raster nachgerechnet. Das Ergebnis ist identisch, und es gibt keine
weiteren Abweichungen zwischen den Regeln:

| Jahr | März (UTC) | Okt/Nov (UTC) | Fahrplan |
|---|---|---|---|
| 2009 | 03-08 07:00 .. 03-30 00:00 | 10-26 00:00 .. 11-01 06:00 | ny_local |
| 2010 | 03-14 07:00 .. 03-29 00:00 | 11-01 00:00 .. 11-07 06:00 | ny_local |
| 2011 | 03-13 07:00 .. 03-28 00:00 | 10-31 00:00 .. 11-06 06:00 | ny_local |
| 2012 | 03-11 07:00 .. 03-26 00:00 | 10-29 00:00 .. 11-04 06:00 | ny_local |
| 2013 | 03-10 07:00 .. 04-01 00:00 | 10-28 00:00 .. 11-03 06:00 | ny_local |
| 2014 | 03-09 07:00 .. 03-31 00:00 | 10-27 00:00 .. 11-02 06:00 | ny_local |
| 2015 | 03-08 07:00 .. 03-30 00:00 | 10-26 00:00 .. 11-01 06:00 | ny_local |
| 2016 | 03-13 07:00 .. 03-28 00:00 | 10-31 00:00 .. 11-06 06:00 | ny_local |
| 2017 | 03-12 07:00 .. 03-27 00:00 | 10-30 00:00 .. 11-05 06:00 | ny_local |
| 2018 | 03-11 07:00 .. 03-26 00:00 | 10-29 00:00 .. 11-04 06:00 | ny_local (letztes Jahr) |
| 2019 | 03-10 07:00 .. 04-01 00:00 | 10-28 00:00 .. 11-03 06:00 | berlin_minus_6_utc_midnight (erstes Jahr) |
| 2020 | 03-08 07:00 .. 03-30 00:00 | 10-26 00:00 .. 11-01 06:00 | berlin_minus_6_utc_midnight |
| 2021 | 03-14 07:00 .. 03-29 00:00 | 11-01 00:00 .. 11-07 06:00 | berlin_minus_6_utc_midnight |
| 2022 | 03-13 07:00 .. 03-28 00:00 | 10-31 00:00 .. 11-06 06:00 | berlin_minus_6_utc_midnight |
| 2023 | 03-12 07:00 .. 03-27 00:00 | 10-30 00:00 .. 11-05 06:00 | berlin_minus_6_utc_midnight |
| 2024 | 03-10 07:00 .. 04-01 00:00 | 10-28 00:00 .. 11-03 06:00 | berlin_minus_6_utc_midnight |
| 2025 | 03-09 07:00 .. 03-31 00:00 | 10-27 00:00 .. 11-02 06:00 | berlin_minus_6_utc_midnight |

Ein Fenster bis „04-01 00:00 UTC“ (2013, 2019, 2024) endet in Rohzeit am 31.03.
um 20:00. Die März-Fenster liegen also ganz im Rohmonat März. Das Okt/Nov-Fenster
reicht dagegen über die Rohmonatsgrenze 10/11. Ein Regelwechsel zwischen Rohmonat
10 und 11 wäre deshalb **nicht** gleichwertig (`config/histdata_timezone.yaml`).

Zum Vergleich: `berlin_minus_6` und `berlin_minus_6_utc_midnight` unterscheiden
sich nur in den beiden EU-Umstellungsnächten, je 23 h zwischen So 01:00 UTC und
Mo 00:00 UTC, zusammen etwa 46 h je Jahr (`DATA.md`, Abschnitt „Zeitzonen-Survey“).
Für diesen Artikel ebenfalls mit zoneinfo nachgerechnet.

### 3.6 Warum die Grenze 2018-11/2018-12 gleichwertig ist

Der letzte Beleg für `ny_local` ist das Okt/Nov-Fenster 2018. Es endet am
2018-11-04 um 06:00 UTC. Der erste Beleg für die Berliner Familie ist das
März-Fenster 2019, es beginnt am 2019-03-10 um 07:00 UTC. **Dazwischen liegt kein
Fenster.** Jede Rohmonatsgrenze von 2018-12 bis 2019-03 liefert daher für jeden
Tick dieselbe UTC-Zeit. Der Fahrplan setzt 2018-12. Wann genau HistData in diesem
Zeitraum umgestellt hat, ist weder bekannt noch für die Umrechnung relevant
(`config/histdata_timezone.yaml`, Begründung im Kommentar; Commit `eb56d86`).

## 4. Belege aus dem Survey

### 4.1 Methode (`scripts/histdata_tz_survey.py`)

Der Survey prüft offline, welche Stempelungsregel die Rohdaten in welchem Monat
erklärt. Er schreibt nie in die Konfiguration, sondern schlägt nur einen Fahrplan
vor.

1. **Kandidaten:** alle Regeln aus `TZ_RULES` (`ny_local`, `berlin_minus_6`,
   `berlin_minus_6_utc_midnight`, `fixed_est`) plus eigene Regeln aus der YAML.
2. **Bewertung je Rohmonat und Regel:** `evaluate_raw_month` liest nur den
   Roh-Cache und die Randticks der Nachbarmonate als Kontext. Das Ergebnis ist
   `ok`, `failed` oder `not_checkable`, dazu die Median-Abweichung in Stunden.
   Grundlage ist dieselbe Prüfung wie im Fetcher (Abschnitt 5.1).
3. **Kontroll-Ausschluss:** Januar und Juli (sowie jeder Monat ohne Fenster) sind
   Kontrollmonate. Dort stempeln alle Sommerzeit-Regeln gleich. Fällt eine Regel
   dort **trennend** durch, zählt sie in den Umstellungsmonaten desselben Jahres
   und Metalls nicht mit. Trennend heißt: Eine andere Regel besteht denselben
   Monat, und die Median-Abweichung beträgt mindestens 0,5 h
   (`MIN_DISCRIMINATING_DEV_H = 0.5`). Beispiel: `fixed_est` im Juli bei +1,0 h
   (impliziter Offset −4 h statt −5 h). Scheitern alle Regeln (Datenfehler),
   heißt das „Kontrolle unbrauchbar“, und nichts wird ausgeschlossen
   (`year_exclusions`).
4. **Regelfamilien:** Regeln, die sich höchstens 7 × 24 h pro Jahr unterscheiden
   (`VARIANT_MAX_H = 7 * 24`, Referenzjahr 2019), bilden eine Familie. Das gilt für
   `berlin_minus_6` und `berlin_minus_6_utc_midnight`. Ein Monat, der nur die
   Familie von den anderen Regeln trennt, entscheidet für die Familie. Zwischen
   den Varianten entscheiden die Umstellungsnächte: Ein Oktober-Rücksprung an
   der Falte spricht für `utc_midnight`, Ticks in der März-Lücke sprechen dagegen.
5. **Entscheidender Rohmonat:** Alle bestehenden, nicht ausgeschlossenen Regeln
   gehören zu **einer** Familie, und alle anderen fallen durch. Über beide Metalle
   wird die Schnittmenge gebildet. Ist sie leer, ist das ein Konflikt.
6. **Wechselanalyse (genau ein Wechsel):** Die entscheidenden Rohmonate werden
   chronologisch durchlaufen. Verlangt sind **höchstens ein** Regelwechsel und
   kein Konflikt. Jeder Block braucht mindestens zwei entscheidende Monate oder
   beide Metalle im selben Monat (`MIN_BLOCK_MONTHS = 2`). Jeder Rohmonat mit
   Fensterstunden zwischen dem letzten alten und dem ersten neuen Beleg muss
   begründet abgedeckt sein. Sonst gibt es keinen Vorschlag und Exit 4.
7. **Umstellungsnächte:** Für jedes Jahr, jedes Metall und jede Umstellung (März,
   Oktober) berichtet der Survey den letzten Tick vor und den ersten nach der
   Umstellung (roh), Ticks in Lücke bzw. Falte und den Status
   (`ok | no_data | unexpected`).

Exit-Codes: 0 = Fahrplan bestätigt. 3 = unvollständig. 4 = Befund: Die
konfigurierte Regel fällt durch, es gibt mehr als einen Wechsel oder einen
Konflikt, oder der Wechsel ist nicht belegt (Docstring von
`scripts/histdata_tz_survey.py`).

### 4.2 Ergebnisse je Zeitraum

Quelle: Kommentar in `config/histdata_timezone.yaml`, `docs/histdata_research.md`
(Abschnitt 4), `DATA.md` (Abschnitt „Zeitzonen-Survey“), Commits `23cf0b1`,
`eb56d86` und `ea79521`.

| Survey | Umfang | Ergebnis |
|---|---|---|
| Erster Survey (Exit 4, 124 Requests) | 2010, 2012, 2015, 2018, 2021, 2024; Umstellungs- und Kontrollmonate | 2010, 2012, 2015, 2018: eindeutig `ny_local` (März und Okt/Nov, beide Metalle). 2021-03, 2024-03: Berliner Familie |
| Erweiterter Survey (Exit 4) | 2009–2025, Rohmonate 03/10/11 | 2019-03: `ny_local` fällt durch, es bestehen nur die Berliner Familie und `fixed_est`[^1]. Jeder Oktober seit 2019: Rücksprung roh So 19:59:59 → 19:00:00 mit echten Ticks |
| Kontroll-Survey (Exit 0) | 2009–2025, Monate 01/03/07/10/11 | Fahrplan bestätigt. Alle Umstellungsnächte 2019–2025 `ok` für XAUUSD und XAGUSD |

[^1]: Die Quellen benennen die bestehende Berliner Variante für 2019-03
unterschiedlich. `config/histdata_timezone.yaml` und `docs/histdata_research.md`
nennen `berlin_minus_6`, `DATA.md` nennt `berlin_minus_6_utc_midnight`. Der
erweiterte Survey lief vor der Einführung von `berlin_minus_6_utc_midnight`
(Commit `eb56d86`). Für das Ergebnis „Berliner Familie, nicht New York, nicht
fest EST“ spielt das keine Rolle. Die Variante entscheiden die Umstellungsnächte
(4.3).

**Kontrollmonate:** `fixed_est` besteht im Rohmonat 2019-03 ebenfalls. Im März
ist der EU-Offset vor der Umstellung −5 h, genau wie bei festem EST. Ausgeschlossen wird `fixed_est` über die Juli-Kontrollmonate
desselben Jahres: Dort liegt der implizite Offset bei −4 h, Abweichung +1,0 h,
während die Berliner Familie besteht. Das ist derselbe Befund wie in 2.1, nur je
Jahr und Metall automatisch angewandt (`year_exclusions` in
`scripts/histdata_tz_survey.py`; Beispieltext „fixed_est ausgeschlossen
(2019-07: +1.0 h)“ in `DATA.md`).

### 4.3 Umstellungsnächte 2019–2025 (XAUUSD und XAGUSD)

Quelle: `REAL_NIGHTS` in `tests/test_histdata_tz_v7.py` (`_REAL_FOLD_TICKS`,
`_REAL_SPRING_EDGES`), übernommen aus dem Bericht des Kontroll-Surveys des
Betreibers. Dazu die Commits `ea79521` und `c4e496e`. Rohzeiten sind
sekundengenau. Erwartetes Muster: März roh 18:59:59 → 20:00:00, Oktober roh
19:59:59 → 19:00:00.

**März (Sprung).** Letzter Tick vor / erster Tick nach der Lücke, roh. In allen
Nächten lagen **0 Ticks** in der nicht existierenden Stunde roh 19:00–19:59.

| Jahr | EU-Sonntag | XAUUSD | XAGUSD |
|---|---|---|---|
| 2019 | 2019-03-31 | 18:59:53 → 20:00:00 | 18:58:00 → 20:00:00 |
| 2020 | 2020-03-29 | 18:59:59 → 20:00:00 | 18:59:47 → 20:00:00 |
| 2021 | 2021-03-28 | 18:59:58 → 20:00:00 | 18:59:59 → 20:00:00 |
| 2022 | 2022-03-27 | 18:59:59 → 20:00:00 | 18:59:56 → 20:00:00 |
| 2023 | 2023-03-26 | 18:59:59 → 20:00:00 | 18:59:50 → 20:00:00 |
| 2024 | 2024-03-31 | 18:59:58 → 20:00:00 | 18:59:58 → 20:00:00 |
| 2025 | 2025-03-30 | 18:59:59 → 20:00:00 | 18:59:58 → 20:00:00 |

Der erste Tick nach der Lücke liegt in jedem Jahr genau bei roh 20:00:00, also
Mo 00:00:00 UTC. Unter `berlin_minus_6` (Umstellung So 01:00 UTC) hätte schon ab
So 01:00 UTC −4 h gegolten. Die fehlende Rohstunde läge dann bei roh Sa
20:00–20:59 im geschlossenen Markt, und an der UTC-Mitternacht gäbe es keine
Lücke.

**Oktober (Falte).** Ticks in der doppelten Rohstunde So 19:00:00–19:59:59.999,
beide Durchgänge zusammen. Der Rücksprung 19:59:59 → 19:00:00 wurde in jeder
Nacht gefunden.

| Jahr | EU-Sonntag | XAUUSD | XAGUSD |
|---|---|---|---|
| 2019 | 2019-10-27 | 5 472 | 1 886 |
| 2020 | 2020-10-25 | 8 146 | 4 030 |
| 2021 | 2021-10-31 | 9 516 | 2 310 |
| 2022 | 2022-10-30 | 7 342 | 3 554 |
| 2023 | 2023-10-29 | 8 272 | 2 798 |
| 2024 | 2024-10-27 | 14 158 | 5 320 |
| 2025 | 2025-10-26 | 28 010 | 17 032 |

Das sind Ticks eines offenen Markts am Sonntagabend und keine Streuticks. Die
Docs fassen die Spanne gerundet als „2k–28k echte Ticks je Monat“ zusammen. Die
genauen Werte reichen von 1 886 bis 28 010.

**Nicht erhoben** (in `REAL_NIGHTS` als `NICHT_ERHOBEN` markiert): die
Millisekunden der Ränder und die Aufteilung der Faltenticks auf die beiden
Durchgänge. Der Testgenerator nimmt 55 % / 45 % an (`DEFAULT_FIRST_PASS_SHARE = 0.55`).
Das ist eine **Testannahme, kein Messwert**.

## 5. Schutzmechanismen in der Pipeline

Grundsatz: Ein Monat mit zweifelhafter Zeitzone wird **nicht geschrieben**.
Lieber fehlt ein Monat, als dass ein verschobener Monat in einen Backtest gelangt.

### 5.1 Prüfung je Rohmonat mit erwartetem Offset (`check_timezone`, Prüfversion 7)

Quelle: `scripts/fetch_histdata.py` (Docstring und Konstanten), `DATA.md`.

- **Anker:** Je Marktlücke gibt es einen Schluss- und einen Öffnungsanker.
  Wochenende: Schluss Fr 17:00, Öffnung So 18:00 New York. Tagespause
  Mo–Do: 17:00–18:00 New York, nur wenn eindeutig und |Abweichung| ≤ 1 h.
  Andere Lücken ≥ 24 h gelten als Ausfälle und werden nur berichtet.
- Je Anker wird der **implizite** Offset (Rohzeit − UTC des Marktereignisses) mit
  dem **erwarteten** Offset der Stempelungsregel in genau diesem Zeitpunkt
  verglichen. Abweichung = implizit − erwartet, Sollwert 0.
- **Hart (Quarantäne, Exit 4)** nur bei Systematischem oder Bestätigtem:
  Median-Abweichung ≠ 0. Anteil der Anker mit Abweichung 0 unter 60 %
  (`TZ_MIN_SHARE = 0.6`). Mindestens zwei **unabhängige** Lücken in Folge mit
  derselben Abweichung ≠ 0 (`TZ_MAX_RUN = 2`), auch über den Monatsrand hinweg,
  mit 8 Tagen Kontext aus den Nachbar-Rohmonaten (`TZ_PAD_MS`). Ein Rücksprung
  ≥ 30 min außerhalb der erwarteten Falte. Nicht eindeutige Ortszeit.
- **Nur Warnung:** einzelne Ausreißer (verirrter Tick, Feed-Kante, Feiertag,
  Ausfall).
- **Umstellungsnächte** (`transition_nights`): Der Bericht prüft je Umstellung, ob
  das erwartete Muster vorliegt. `unexpected` ist ein Fehlergrund. Im Herbst heißt
  das: Daten auf beiden Seiten, aber kein Rücksprung. Im Frühjahr: Ticks in der
  fehlenden Stunde.
- **Altbestand:** Monatsdateien tragen die Regel und die Prüfversion im
  Parquet-Metadatum `source_timezone`. Ändert sich der Fahrplan, erkennt der
  nächste Lauf betroffene Dateien. Verschobene Dateien kommen sofort nach
  `metals/stale/` und werden aus dem Roh-Cache neu gebaut. Dateien mit
  identischer UTC-Abbildung werden nur neu gekennzeichnet.

### 5.2 Quarantäne statt Raten

- Ein Rohmonat, der die Prüfung nicht besteht, bekommt einen Quarantäne-Marker
  (JSON mit Prüfbericht). Der Lauf endet mit Exit 4, und der betroffene UTC-Monat
  wird nicht geschrieben.
- **Nicht existierende und doppelte Rohzeiten** werfen `TimezoneError` (Unterklasse
  von `FormatError`): ein Tick in der März-Lücke oder eine Oktober-Falte ohne
  erkennbaren Rücksprung. Es gibt keine Heuristik, die eine Seite wählt
  (`raw_to_utc_ms`).
- Eine pauschale Freigabe (`"accepted": true`) gilt **nie** für Rücksprünge oder
  nicht eindeutige Ortszeit. Nur Ticks im geschlossenen Wochenendmarkt lassen sich
  nach Prüfung verwerfen (`--accept-weekend-ticks`). Sie werden dann nie
  umgerechnet (`docs/histdata_research.md`, Abschnitt 4).

### 5.3 Menschliche Auflösung unauflösbarer Falten

Kann die Oktober-Falte nicht automatisch aufgelöst werden (dünne Daten, keine
Ticks auf einer Seite, Sprung < 30 min, nicht monoton), trägt der Marker
`fold_unresolved` mit Datum, Tickzahl und erster/letzter Rohzeit. Ein Mensch
entscheidet nach Prüfung:

```
python scripts/fetch_histdata.py --symbol XAGUSD --resolve-fold YYYY-MM --side pre|post|split-at RAW_TS
```

`pre` ordnet alle Ticks der Stunde dem 1. Durchgang zu (−4 h), `post` dem
2. Durchgang (−5 h). `split-at` trennt in Dateireihenfolge ab dem ersten Tick mit
der angegebenen Rohzeit. Das Kommando zeigt jeden betroffenen Tick mit seiner
UTC-Zeit an, prüft die Monotonie und setzt nur `fold_resolution` im Marker. Die
Entscheidung gilt nur für genau diese Ticks (Anzahl, erste und letzte Rohzeit).
Ändern sich die Daten, ist der Monat wieder in Quarantäne (Commit `6449982`,
`README.md`).

### 5.4 Vorläufige Quarantäne für junge Monate

Quelle: `config/histdata.yaml` (Abschnitt `provisional` und Kommentar zu
`history.end`), Commits `83ab221` und `2af5d64`.

- **Beispiel 2026-10:** Rohmonat 2026-10 (XAUUSD und XAGUSD) kam in Quarantäne.
  Die Rohstunde am 25.10. 19:00–19:59 war doppelt (XAGUSD 8 416 Ticks), aber
  **ohne Rücksprung**, und alle Beispielticks hatten `.000` ms. Vermutlich ist das
  eine vorläufige Dateiversion bei HistData. Der Anteil der `.000`-ms-Ticks steht
  als Diagnose `ms_zero_share` im Monatsreport. Er fließt nicht in Entscheidungen ein.
- Deshalb endet der Historienbereich bei **2026-09** (`history.end`).
- Ein Rohmonat **nach** `history.end`, der weniger als `window_days: 60` Tage nach
  Monatsende in Quarantäne geht, gilt als vorläufig. Nach `recheck_days: 30` Tagen
  wird die ZIP neu geladen und neu geprüft, höchstens `max_rechecks: 3`-mal. Danach
  gilt die normale Quarantäne (Exit 4). Bis dahin endet ein Lauf deswegen nicht
  mit einem Fehler (`months_provisional` im Laufreport).

### 5.5 Historienbeginn 2009-06 wegen vertauschter Bid/Ask-Spalten

Quelle: `config/histdata.yaml` (Kommentar zu `history.start`), Commit `83ab221`,
`docs/histdata_research.md` (Abschnitt 5).

- Der volle Historienlauf (185 Monate je Symbol) endete mit Exit 2. Grund waren
  allein die ersten Monate: **XAUUSD 2009-03, 2009-04, 2009-05 und XAGUSD 2009-05**
  haben in **76–99 % der Bars `ask_close < bid_close`**. Sehr wahrscheinlich sind
  die Bid/Ask-Spalten in den frühen Dateien vertauscht, im Mai 2009 teilweise.
- HistData listet XAUUSD ab 2009-03 und XAGUSD ab 2009-05. Konfiguriert ist
  `history.start` = **2009-06** für beide Metalle. Frühere Rohmonate werden nie
  geladen oder geschrieben. Spalten werden **nicht** getauscht, und es wird
  nichts geraten.
- Der erste UTC-Monat 2009-06 entsteht nur aus Rohmonaten ab 2009-06. Seine
  ersten Stunden (aus Rohmonat 2009-05) fehlen, und der Monatsreport setzt
  `partial_start: true`.

### 5.6 EURUSD nicht über HistData

EURUSD-Ticks haben keine Tagespause, und die Prüfung hat zu wenige Anker je Monat.
`--verify-month 2024-11` bestand trotz falscher Stempelung. Der Fetcher lehnt
`--symbol EURUSD` deshalb ab (`scripts/fetch_histdata.py`, Docstring;
`docs/histdata_research.md`, Abschnitt 4). Das zeigt die Grenze dieser
Prüfmethode: Sie braucht einen Markt mit klaren Sessionlücken.

## 6. Offene Punkte (ehrlich benannt)

- **M1-Dateien sind nicht verifiziert.** Für `--mode m1` gilt weiter `fixed_est`
  laut HistData-Angabe. Die Tick-Befunde legen nahe, dass auch M1 anders gestempelt
  sein könnte. Ein Drittprojekt fand je 60 exakt doppelte M1-Zeilen in jedem
  Oktober 2019–2024, also eine doppelte Stunde. Das ist nicht selbst geprüft
  (`docs/histdata_research.md`, Abschnitt 4). Prüfen mit einem Sommermonat (siehe
  Abschnitt 7). Liegt die Median-Abweichung bei +1, ist auch M1 mit Sommerzeit
  gestempelt.
- **Warum HistData die Stempelung geändert hat, ist unbekannt.** Belegt ist nur
  der Befund: `ny_local` bis 2018 und die EU-Tagesregel ab 2019. Ebenso unbekannt
  ist, warum der Wechsel an der UTC-Mitternacht liegt und nicht an der
  Berliner Umstellungszeit. Vermutungen über die Datenerfassung bei HistData
  werden hier bewusst nicht angestellt.
- **Der genaue Umstellungsmonat** zwischen 2018-11 und 2019-03 ist unbekannt. Er
  ist für die Umrechnung irrelevant (3.6).
- **Nicht erhoben:** Millisekunden der Ränder in den Umstellungsnächten und die
  Aufteilung der Oktober-Faltenticks auf die beiden Durchgänge (4.3).
- **Die Survey-Berichte selbst** (JSON/Markdown) liegen nicht im Repository. Die
  Zahlen in diesem Artikel stammen aus den Tabellen in
  `tests/test_histdata_tz_v7.py`, den Kommentaren in `config/` und den
  Commit-Nachrichten, die sie aus diesen Berichten übernommen haben.
- **Aktuelle Monate:** Vorläufige Dateiversionen (Beispiel 2026-10) können sich
  ändern. Sie werden automatisch neu geprüft (5.4).

## 7. Reproduktion

Voraussetzungen: Python mit `requirements.txt`. Die Daten liegen unter einem frei
wählbaren `<data_root>` (Umgebungsvariable `MARKETDATA_DATA_ROOT`, sonst
`<repo>/data`). HistData-Rohdaten werden nicht mit dem Repository verteilt.

```bash
export MARKETDATA_DATA_ROOT=<data_root>

# Survey 2009-2025 (lädt fehlende Rohmonate in den Cache, je Rohmonat 2 Requests,
# 5 s Pause, setzt nach Abbruch fort; die Config wird nie geschrieben)
python scripts/histdata_tz_survey.py --preset full \
    --json <data_root>/metals/reports/histdata-tz-survey-full.json \
    --markdown <data_root>/metals/reports/histdata-tz-survey-full.md

# Nur den Cache auswerten (soll mit dem ausgelieferten Fahrplan Exit 0 liefern)
python scripts/histdata_tz_survey.py --preset full --offline

# Einzelne Monate gegen den Fahrplan prüfen (schreibt keine Marker)
for m in 2024-01 2024-03 2024-07 2024-11; do
  python scripts/fetch_histdata.py --symbol XAUUSD --verify-month "$m"; echo "$m Exit $?"
done

# M1 prüfen (offen, siehe Abschnitt 6)
python scripts/fetch_histdata.py --symbol XAUUSD --mode m1 --verify-month 2024-07

# Regressionstests mit den echten Umstellungsnächten (synthetische Daten, kein Netz)
python -m pytest tests/test_histdata_tz_v7.py
```

Die Fenstertabelle aus 3.5 lässt sich ohne Daten nachrechnen:

```python
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

UTC, NY, BER = timezone.utc, ZoneInfo("America/New_York"), ZoneInfo("Europe/Berlin")
h = timedelta(hours=1)

def ny_local(t):                      # Offset Rohzeit - UTC in h
    return t.astimezone(NY).utcoffset() / h

def berlin_minus_6_utc_midnight(t):
    mid = t.replace(hour=0, minute=0, second=0, microsecond=0)
    return mid.astimezone(BER).utcoffset() / h - 6

for y in range(2009, 2026):
    t, end, start, wins = datetime(y, 1, 1, tzinfo=UTC), datetime(y + 1, 1, 1, tzinfo=UTC), None, []
    while t < end:
        differ = ny_local(t) != berlin_minus_6_utc_midnight(t)
        if differ and start is None:
            start = t
        if not differ and start is not None:
            wins.append(f"{start:%m-%d %H:%M} .. {t:%m-%d %H:%M}")
            start = None
        t += timedelta(minutes=15)
    print(y, " | ".join(wins))
```

## Quellen im Repository

| Datei | Inhalt |
|---|---|
| `config/histdata_timezone.yaml` | Regeln, Fahrplan, Belegtabelle, Fenstertabelle 2009–2025 |
| `config/histdata.yaml` | Historienbereich 2009-06..2026-09, Begründungen, vorläufige Quarantäne |
| `scripts/fetch_histdata.py` | `TzRule`, `TZ_RULES`, `raw_to_utc_ms`, `fold_reversal`, `check_timezone`, `transition_nights`, `--resolve-fold` |
| `scripts/histdata_tz_survey.py` | Survey: Kandidaten, Kontroll-Ausschluss, Familien, Wechselanalyse |
| `tests/test_histdata_tz_v7.py` | `REAL_NIGHTS`: echte Umstellungsnächte 2019–2025 |
| `docs/histdata_research.md` | Recherche, Messtabellen, offene Punkte |
| `DATA.md`, `README.md` | Betriebsdokumentation (HistData, Zeitzonen-Survey) |
| `git log` | Chronologie: `ea5b3e7` (fest EST), `bf14490` (New York), `992454e` (Berlin − 6), `23cf0b1`/`4d2b576` (v6, vorläufiger Fahrplan), `eb56d86`/`6449982` (v7, UTC-Mitternacht), `ea79521`/`c4e496e` (echte Nächte), `83ab221`/`2af5d64` (Bereich, vorläufige Quarantäne) |
