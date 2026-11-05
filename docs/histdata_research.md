# HistData.com: Download, Formate, Zeitzone - Recherche

Stand 2026-11-05. **histdata.com war aus dieser Umgebung nicht erreichbar** (Proxy 403, WebFetch: Host nicht auflösbar). Nichts wurde live gegen HistData verifiziert. Alles unten stammt aus Quellcode-Lektüre (nichts davon ausgeführt), aus Dokumenten Dritter, die echte HistData-Dateien verarbeitet haben, und aus einer Websuche. Getestet ist `scripts/fetch_histdata.py` ausschließlich mit synthetischen Daten und gemocktem HTTP.

## Quellen

| Kürzel | Quelle | Rolle |
|---|---|---|
| HD | PyPI `histdata` 1.1 (Philippe Remy), `histdata/api.py` und `METADATA` | Download-Ablauf, M1-Format, Zeitzonen-Aussage |
| HDC | PyPI `histdatacom` 2.4.0 (David Midlo), Repo `github.com/dmidlo/histdata.com-tools` | Tick-Parser, Formular-Parser, ZIP-Inhalt, Verfügbarkeit je Paar, Beobachtungen an echten Dateien |
| HDC-SPEC | `histdata.com_data_specs.md` im HDC-Repo, Commit `ce96454` (2022-03-23, "added csv data files specification reference") | Abschrift der HistData-Seite "Data Files: Detailed Specification" (`https://www.histdata.com/f-a-q/data-files-detailed-specification/`) |
| HF | PyPI `histdata-fetcher` 0.1.0 (Njenjo, 2026-08-22), `client.py`, README, Tests | Ablauf, Verhalten bei fehlenden Perioden, Beobachtungen 2026 |
| HDL | PyPI `histdata-downloader` 0.5.1 | nur Bestätigung `get.php` und Dateiname per Content-Disposition |
| AIT | GitHub `Tarek-Assaf/ai-trader`: `docs/reports/histdata_qa.md`, `docs/reports/histdata_coverage.json`, `docs/evidence/q024_histdata_pod_import.md` | QA an **echten** XAUUSD-Archiven (Tick und M1, 2009-2026) |
| WCT | GitHub `webclinic017/testbed2`, `FX_spread_stats.py` | rechnet Spread aus `DAT_ASCII_XAUUSD_T_202005.csv` als Spalte 2 minus Spalte 1 |
| WEB | Websuche: README `github.com/AwesomeTrading/FX-1-Minute-Data` (Fork von HD), Tickstory-Forum | Zeitzonen-Aussage (Drittquelle) |

## 1. Download-Ablauf

Belegt von HD, HDC, HF und AIT unabhängig voneinander (gleicher Ablauf, verschiedene Implementierungen):

1. `GET https://www.histdata.com/download-free-forex-historical-data/?/ascii/{tf}/{pair}/{YYYY}[/{M}]`
   - `tf` = `tick-data-quotes` (Generic ASCII Ticks) bzw. `1-minute-bar-quotes` (Generic ASCII M1)
   - `pair` klein (`xauusd`), Monat ohne führende Null (HD `get_referer`, HF `_period_page_url`)
2. Aus der Seite das Formular `id="file_down"` lesen: versteckte Felder `tk`, `date`, `datemonth`, `platform`, `timeframe`, `fxpair`.
   **Wichtig** (HF `_extract_form_fields`, HF-Test `conftest.py`): Die Seite hat ein zweites Formular `file_status` (Ziel `getStatus.php`) mit einem **gleichnamigen** `tk`. HD sucht nur `id="tk"`, was von der Reihenfolge im HTML abhängt. Der Fetcher sucht deshalb nur innerhalb von `file_down` (wie HDC `_HistDataDownloadFormParser` und HF).
3. `POST https://www.histdata.com/get.php` mit genau diesen Feldern, Header `Referer` = Periodenseite (HD: "Referer is the most important thing here"). Antwort: das ZIP (Dateiname per `Content-Disposition`, HDC/HDL).
   - `datemonth` = `YYYYMM` bei Monatsdateien, leer bzw. `YYYY` bei Jahresdateien (HD; AIT: "the yearly form accepts datemonth only when absent or empty").
4. Keine Daten: laut HF zeigt die Seite trotzdem ein Formular mit `tk`, der POST liefert aber einen **leeren Body (0 Byte)**. HF und AIT behandeln leere Antworten nie als Erfolg.

Granularität (HD-Assertion, HF-Docstring, AIT T-082):
- **Ticks: immer Monatsdateien**, auch für alte Jahre.
- **M1: vergangene Jahre nur als Jahres-ZIP**, laufendes Jahr monatlich.

Je Datei also **2 Requests** (Seite + POST). Ein Session-Cookie ist laut HF nicht nötig; das Token hängt an den Parametern.

Nicht verifiziert: ob HistData einen Nicht-Browser-User-Agent ablehnt (HF setzt einen Chrome-UA, AIT `"ai-trader HistData importer"` und lädt damit erfolgreich). Der Fetcher nutzt `Mozilla/5.0 (X11; Linux x86_64) marketdata-pipeline/1.0`, keine E-Mail-Adresse.

## 2. Formate - welches enthält Bid UND Ask?

Aus HDC-SPEC (Abschrift der offiziellen Spezifikation), bestätigt durch HD-README, HDC-Parser, HF-Parser, AIT:

| Format | Zeile | Inhalt |
|---|---|---|
| Generic ASCII Tick (`DAT_ASCII_{SYM}_T_{YYYYMM}.csv`) | `20120201 000003660,1.306600,1.306770,0` | `DateTime Stamp,Bid Quote,Ask Quote,Volume`, Zeitstempel `YYYYMMDD HHMMSSNNN` (NNN = Millisekunden), Trenner `,` |
| Generic ASCII M1 (`DAT_ASCII_{SYM}_M1_{YYYY[MM]}.csv`) | `20120201 000000;1.306600;1.306600;1.306560;1.306560;0` | `DateTime Stamp;Bar OPEN Bid Quote;Bar HIGH Bid Quote;Bar LOW Bid Quote;Bar CLOSE Bid Quote;Volume`, Trenner `;` |

- **Nur die Tickdateien haben Bid und Ask.** M1 ist laut Spezifikation reines Bid-OHLC.
- Für **XAUUSD** konkret belegt: WCT berechnet aus `DAT_ASCII_XAUUSD_T_202005.csv` den Spread als Spalte 2 minus Spalte 1; AIT verarbeitet `DAT_ASCII_XAUUSD_T_202608.zip` (5.213.048 Quotes) und nennt die Tickdateien "timestamp, bid, ask, volume".
- Volumen ist laut HD und HF bei FX/CFD immer 0.

**Entscheidung:** Default `--mode tick` (Ticks selbst zu 1m-Bid/Ask-OHLC verdichtet, echter `tick_count`). `--mode m1` ist ein Notbehelf: `ask = bid`, Spread 0, Parquet-Metadatum `spread_available=false`. Den Spread liefert dann das Kostenmodell des Bots.

Weitere Beobachtungen an echten Dateien (Drittquellen, nicht selbst geprüft):
- **Zeitstempel nicht streng steigend**: AIT zählt in XAUUSD 2026-08 2.707 Rücksprünge ("timestamp reversals"), Größe nicht angegeben. Der Fetcher sortiert stabil (Dateireihenfolge bei Gleichstand bleibt erhalten, wie AIT) und toleriert kleine Rücksprünge.
- **Gleiche Zeitstempel häufig**: HF misst bei EURUSD ~4 % (2026-06) bis ~50 % (2026-07/08) geteilte Zeitstempel, dort oft volle Sekunden (Millisekunden `000`). Ticks werden deshalb nie dedupliziert.
- **M1 mit widersprüchlichen Doppelminuten**: AIT findet in XAUUSD-M1 2026-08 98 Minuten mit mehreren, unterschiedlichen Zeilen und baut sie aus Ticks neu. Außerdem je 60 **exakt** doppelte Zeilen in jedem Oktober-Archiv 2019-2024. Der Fetcher verwirft im M1-Modus exakte Duplikate und fasst abweichende zusammen (open erste, high max, low min, close letzte Zeile), mit Zählern im Monatsreport. Auch deshalb ist Tick der Default.

## 3. ZIP-Inhalt

- Genau eine CSV `DAT_ASCII_{SYM}_{T|M1}_{period}.csv` plus eine gleichnamige `.txt` (Statusreport mit Lücken). Belege: HD-README ("a status report (containing some meta data such as gaps)"), HDC `histdata_ascii.py` Z. 319 ("Live HistData ASCII archives include a same-stem .txt status report"), HF ("plus one informational .txt file"), AIT (prüft erlaubte Member `{csv}` und `{csv[:-4]}.txt`).
- ZIP-Name: `HISTDATA_COM_ASCII_{SYM}_{T|M1}{period}.zip` (HDC `inventory.py`, AIT T-082). Der Cache nutzt diesen Namen.
- **Format der Status-.txt: NICHT verifiziert.** Keine der Quellen enthält eine echte Datei. Der Parser erwartet Zeilen der Form `Gap of 62s found between 20120201000106 and 20120201000208` (Rohzeit) und rechnet sie nach UTC um. Diese Form ist aus dem Gedächtnis rekonstruiert und nicht belegt. Passt keine Zeile, landen die ersten 8 Zeilen roh im Report (`head`). Die Status-Lücken sind nur Report, keine Validierung.

## 4. Zeitzone

**Aussage HistData** (HDC-SPEC, für jedes Format wörtlich): "TimeZone: Eastern Standard Time (EST) time-zone WITHOUT Day Light Savings adjustments". Gleichlautend: HD-README, HF ("Per HistData's own FAQ ... EST, UTC-5 ... WITHOUT daylight-savings adjustment, year-round"), HDC (`EST_NO_DST_OFFSET_MS = 18_000_000`, `SOURCE_UTC_OFFSET = "-05:00"`), AIT (`UTC_MINUS_FIVE`), WEB.

Daraus: `ts_utc = roh + 5 h`, ohne Sommerzeitlogik. Folge: UTC-Monat M = Rohmonat M-1 (dessen letzte 5 Stunden) + Rohmonat M (ohne dessen letzte 5 Stunden).

**Zweifel aus Daten Dritter (wichtig, nicht selbst geprüft):**
1. HDC `docs/powered-reconstruction-qualification.md`: "October 2025, contained a synchronized one-hour timestamp regression in all three source files". HDC lehnt solche Dateien ab. Code-Kommentar HDC `histdata_ascii.py` Z. 19: "Real HistData tick archives can preserve one one-hour source-order fallback around the late-October clock transition". Ende Oktober ist die **europäische** Zeitumstellung, nicht die US-amerikanische.
2. AIT: je 60 exakt doppelte M1-Zeilen in **jedem Oktober** 2019-2024, also eine Stunde doppelt.
3. AIT: XAUUSD-Tickarchiv 2026-08, umgerechnet mit +5 h, beginnt am **2026-08-02 23:00 UTC**. Also erster Tick roh Sonntag 18:00. Gold öffnet sonntags um 18:00 New-York-Ortszeit, im August (EDT) also 22:00 UTC. Bei fest UTC-5 müsste der erste Tick roh um 17:00 stehen. **Roh 18:00 im August passt eher zu Ortszeit mit Sommerzeit** (UTC-4 im Sommer) oder zu einer um eine Stunde späteren Öffnung des HistData-Feeds.

Hypothese (unbewiesen): Die Rohzeit folgt zumindest zeitweise einer Uhr mit Sommerzeit (New York oder Europa minus fester Abstand), nicht durchgehend UTC-5. Wenn das stimmt, würden Sommermonate im Code-Check **durchfallen**.

**Umsetzung: Prüfung je Rohmonat im Code** (`check_timezone`):
- Ankerpunkte aus den Daten: Wochenschluss (letzter Tick vor Lücke ≥ 24 h, erwartet Freitag 17:00 New York), Wochenstart (erster Tick danach, erwartet 18:00 Metalle / 17:00 EURUSD), bei Metallen zusätzlich Tagespause Mo-Do (Lücke 45 min bis 3 h, Beginn 17:00, Ende 18:00 New York). Erwartete Zeiten über `zoneinfo("America/New_York")`, also mit Sommerzeit.
- Je Anker der implizite Offset (Rohzeit minus UTC, auf ganze Stunden gerundet). Erwartet -5.
- Bestanden nur, wenn alle Bedingungen gelten:
  - mindestens 3 Anker
  - Median -5
  - Anteil -5 mindestens 60 %
  - weniger als 3 aufeinanderfolgende Anker mit gleichem abweichendem Offset (sonst Regimewechsel mitten im Monat)
  - kein Rücksprung ≥ 30 min in Dateireihenfolge (sonst Uhrumstellung)
- Feiertage und frühe Schlüsse sind einzelne Ausreißer und kippen das Ergebnis nicht.
- Sonst **Quarantäne** (`<zip>.quarantine` mit vollem Prüfbericht, Exit 4). Die UTC-Monate, die diesen Rohmonat brauchen, werden nicht geschrieben. Es wird nie verschoben geschrieben.
- Tests (synthetisch): fest EST besteht im Januar und Juli (XAUUSD, XAGUSD, EURUSD). New-York-Ortszeit mit DST fällt im Juli durch (Median -4). UTC-gestempelt fällt durch. Ein Wechsel mitten im Monat fällt durch, ebenso eine wiederholte Stunde. Ein Feiertag (früher Schluss) besteht.
- Grenzen: Im Winter sind New-York-Ortszeit und EST identisch und nicht unterscheidbar (korrekt). EURUSD hat nur 2 Anker pro Woche (keine Tagespause). Eine Verschiebung, die kürzer als etwa 1,5 Wochen dauert, kann dort unentdeckt bleiben. Eine Rückstellung erkennt der Rücksprung-Check trotzdem.

**Erster Schritt im Homelab:** `--verify-month` für einen Winter- und einen Sommermonat (z.B. 2024-01 und 2024-07). Der Verify-Report zeigt je Rohmonat `median_offset_h`, die Offset-Verteilung und abweichende Beispiele. Liegt der Sommer bei -4, ist die HistData-Aussage für diese Daten falsch. Dann muss entschieden werden, ob eine belegte Umrechnungsregel eingebaut wird (nicht Teil dieses Fetchers, bewusst nicht geraten).

## 5. Historienbeginn

| Symbol | Erster Monat (ASCII M1) | Beleg | Ticks |
|---|---|---|---|
| XAUUSD | 2009-03 | HDC `data/.repo` (Stand 2026-06/07), AIT T-082 ("XAUUSD beginning in March 2009", Auswahlliste), AIT Q-024 (Lake 2009-03..2026-09) | **nicht verifiziert** |
| XAGUSD | 2009-05 | HDC `data/.repo` | **nicht verifiziert** |
| EURUSD | 2000-05 | HDC `data/.repo`, HF-README (`2000-05-01`) | Hinweis: GitHub-Code nennt `DAT_ASCII_EURUSD_T_200005` (rpitchumani/histdata Tests, kaiaeberli/ml_projects), nicht verifiziert |

Der Fetcher nimmt diese M1-Startmonate als `HISTORY_START`. Monate davor gelten ohne Request als nicht verfügbar. Fehlen Tickdaten für spätere Monate, liefert HistData einen leeren Body. Der Monat wird dann nach 24 h erneut geprüft, als "keine Daten" bestätigt und im Report unter `months_unavailable` geführt (kein Exit 3).

## 6. Veröffentlichungsverzug, Rate-Limits, Nutzungsbedingungen

- Verzug: Laut HDC `data/.repo` war am 2026-07-07 der Juni 2026 verfügbar (Ende `202606`). AIT lud am 2026-10-03 den August 2026, und am 2026-10-04 zeigte die Auswahlliste den September 2026. Also wenige Tage bis wenige Wochen. **Kein genauer Wert verifiziert.** Der Fetcher behandelt einen leeren Monat bis 45 Tage nach Monatsende als `pending` (Exit 3, erneute Prüfung frühestens nach 24 h). Danach gilt er als "keine Daten", erst unbestätigt, nach Wiederholung ≥ 24 h bestätigt.
- Rate-Limits: **keine Angaben gefunden.** HF lädt mit 8 parallelen Workern, AIT empfiehlt 1 s Pause. Der Fetcher arbeitet konservativ: ein Request zur Zeit, 5 s Mindestabstand, und HTTP 429 über `scripts/ratelimit.py` (≥ 15 min Pause, persistiert, kein Versuchsverbrauch).
- Nutzungsbedingungen: **nicht gelesen** (Seite nicht erreichbar). HistData bietet die Daten kostenlos zum Download an. Weitergabe der Rohdaten vermeiden, `data/` ist git-ignored. Vor produktiver Nutzung selbst prüfen.

## 7. Offene Punkte (unverifiziert)

1. Zeitzone im Sommer (Abschnitt 4): mit `--verify-month` auf einem Sommermonat prüfen. Ergebnis entscheidet, ob HistData für Sommermonate nutzbar ist.
2. Tick-Historienbeginn je Symbol.
3. Format der Status-.txt.
4. Ob HistData bestimmte User-Agents oder Request-Raten blockt, und wie eine Fehler-, Captcha- oder WAF-Seite aussieht. Jede Nicht-ZIP-Antwort mit Inhalt ist ein Fehlschlag mit Textausschnitt im Report, nie Daten.
5. Ob gekreuzte Quotes (ask < bid) in HistData-Ticks vorkommen. `fetch_dukascopy.validate` bricht bei mehr als 0,1 % gekreuzter Bars hart ab (Exit 2).
6. Speicherbedarf: Ein Tickmonat hat Millionen Zeilen (AIT: 5,2 Mio. XAUUSD 2026-08; HF: ~1,4 Mio. EURUSD, ~9 MB ZIP). Gemessen synthetisch: 1,7 Mio. Ticks parsen in 0,6 s (pyarrow.csv). Höchstens 2 Rohmonate liegen gleichzeitig im Speicher. Grob 1-2 GB RAM einplanen.
