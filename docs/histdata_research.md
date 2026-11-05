# HistData.com: Download, Formate, Zeitzone - Recherche

Stand 2026-11-05. **histdata.com war aus dieser Umgebung nicht erreichbar** (Proxy 403, WebFetch: Host nicht auflösbar). Von hier aus wurde nichts live gegen HistData verifiziert. **Ausnahme: die Tick-Zeitzone**, gemessen vom Eigentümer im Homelab an echten XAUUSD-Ticks (Abschnitt 4). Alles andere unten stammt aus Quellcode-Lektüre (nichts davon ausgeführt), aus Dokumenten Dritter, die echte HistData-Dateien verarbeitet haben, und aus einer Websuche. Getestet ist `scripts/fetch_histdata.py` ausschließlich mit synthetischen Daten und gemocktem HTTP.

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

### Verifizierter Befund: Ticks sind New-York-Ortszeit MIT Sommerzeit

Messung des Eigentümers im Homelab an **echten** HistData-Daten (XAUUSD, `--mode tick`, `check_timezone` der Vorversion, die fest UTC-5 erwartete):

| Rohmonat | Ergebnis | Rohdaten |
|---|---|---|
| 2024-01 | impliziter Offset -5 h, ok | (Winter: EST = UTC-5) |
| 2024-07 | Quarantäne, **alle 38 Anker bei -4 h** | Tagespause 16:59:58 -> 18:00:01, Wochenschluss Fr 16:59:58, Wochenstart So 18:00:01; 0 Rücksprünge, Daten sonst sauber |

Die Rohzeiten sind im Juli genau die Sessionzeiten in New-York-Ortszeit (Schluss 17:00, Öffnung 18:00), also EDT = UTC-4. **Schluss: HistData-Tick-Zeitstempel sind America/New_York-Ortszeit mit Sommerzeit, nicht fest EST.** Die HistData-Angabe stimmt für Ticks nicht. Das passt zur Beobachtung 3 unten (AIT, August 2026: erster Tick roh Sonntag 18:00).

**M1 ist NICHT verifiziert.** Für M1 bleibt die Annahme fest UTC-5 (`Etc/GMT+5`) laut HistData-Angabe. Sie wird nicht stillschweigend umgestellt. Prüfen mit einem Sommermonat:

```
python scripts/fetch_histdata.py --symbol XAUUSD --mode m1 --verify-month 2024-07
```

Der Verify-Report zeigt je Rohmonat `raw_tz`, `median_offset_h`, `median_deviation_h`, `expected_offsets`, `deviations` und abweichende Beispiele. Liegt die Median-Abweichung bei +1 (impliziter Offset -4 h statt -5 h), sind auch die M1-Zeitstempel New-York-Ortszeit. Dann muss `RAW_TZ["m1"]` bewusst auf `America/New_York` umgestellt werden (mit Test).

### Umrechnung (`RAW_TZ`, `raw_to_utc_ms`)

- `RAW_TZ = {"tick": "America/New_York", "m1": "Etc/GMT+5"}` (zoneinfo; `Etc/GMT+5` ist fest UTC-5).
- Die Rohziffern werden als Wanduhrzeit in `RAW_TZ[mode]` gelesen und vektorisiert nach UTC umgerechnet: Je möglichem Offset der Zone (-5 h, -4 h) wird ein Kandidat gebildet. Er gilt, wenn die Zone in genau diesem UTC-Zeitpunkt diesen Offset hat (Wechseltabelle aus zoneinfo, je Jahr berechnet, also auch die US-Regel vor 2007). Exakt auf die Millisekunde.
- **Nie raten:** Ortszeiten in der Sommerzeit-Lücke (z.B. 2024-03-10 02:00:00-02:59:59.999) existieren nicht, Ortszeiten in der doppelten Stunde (z.B. 2024-11-03 01:00:00-01:59:59.999) sind mehrdeutig. Beides ergibt `TimezoneError` (Unterklasse von `FormatError`) mit den ersten Beispielen. Der Markt ist zu diesen Zeiten (Sonntag früh) geschlossen. Ein Tick dort heißt, dass etwas nicht stimmt. Der Rohmonat geht in Quarantäne (Exit 4), auch wenn sein Marker `"accepted": true` hat.
- Folge: UTC-Monat M = Rohmonat M-1 (dessen letzte 4 h in EDT bzw. 5 h in EST) + Rohmonat M (ohne diese Stunden an seinem Ende). Die Rohmonate werden aus `RAW_TZ` berechnet, nicht fest verdrahtet. US-Umstellungen liegen nie auf einem Monatsrand.

### Prüfung je Rohmonat (`check_timezone`)

- Anker: je Marktlücke ein Schluss- und ein Öffnungsanker. Wochenende (Lücke ≥ 24 h): Schluss Freitag 17:00, Öffnung Sonntag 18:00 (Metalle) bzw. 17:00 (EURUSD) New York. Bei Metallen die Tagespause Mo-Do (17:00-18:00 New York): höchstens eine je Tag (die längste), nur wenn eindeutig und höchstens 3 h vom erwarteten Beginn entfernt. Dünne Märkte wie Silber haben weitere Lücken. Erwartete Marktzeiten über `zoneinfo("America/New_York")`.
- **Randticks der Nachbar-Rohmonate:** Die letzten Ticks des gecachten Vormonats und die ersten des Folgemonats werden mitbetrachtet. Dadurch wird auch das Wochenende über den Monatsrand zur Lücke; Anker zählen nur auf der eigenen Seite. Vorher hatte EURUSD (keine Tagespause) dort gar keinen Anker (Review: EURUSD 2025-10 mit EU-Stempelung bestand mit `{0: 7, -1: 1}`).
- Je Anker der **implizite** Offset (Rohzeit minus UTC des Marktereignisses) und der **erwartete** Offset von `RAW_TZ[mode]` in genau diesem Zeitpunkt (tick: -4 h in EDT, -5 h in EST; m1: immer -5 h). Entschieden wird über die **Abweichung** = implizit - erwartet.
- Die Abweichung einer Lücke wird **aus beiden Seiten gemeinsam** bestimmt: alle ganzen Abweichungen, mit denen der Schluss nicht vor dem letzten und die Öffnung nicht nach dem ersten Tick läge (±5 min). Bei richtigen Daten jeder Dichte liegt 0 immer darin. Genau eine: beide Anker damit, auch wenn die Ticks Minuten vom Ereignis entfernt sind (dünne Daten; vorher rundete ein letzter Tick > 30 min vor 17:00 auf -1 h, und die Monatsrand-Regel ließ jeden dünnen Monat durchfallen). Mehrere: Lücke zählt nicht, außer Seiten mit Tick am Ereignis. Keine (Widerspruch): Stempelung wechselt über das Wochenende.
- **Belastbar** ist ein Anker, wenn die Abweichung seiner Lücke eindeutig ist (Wochenende), die Seiten eines Wochenendes sich widersprechen, oder beide Ticks höchstens 2 min an der vollen Stunde liegen und der Markt dort aktiv ist (Nachbartick ebenfalls ≤ 2 min). Tagespausen sind nur mit Ticks am Ereignis belastbar.
- Bestanden nur, wenn alle Bedingungen gelten:
  - mindestens 3 Anker
  - Median-Abweichung 0
  - Anteil Abweichung 0 mindestens 60 %
  - weniger als 3 aufeinanderfolgende Lücken mit gleicher Abweichung ≠ 0 (sonst Regimewechsel mitten im Monat)
  - kein Rücksprung ≥ 30 min in Dateireihenfolge (sonst Uhrumstellung)
  - kein belastbarer Anker mit genau ±1 h Abweichung in den ersten oder letzten 72 h des Rohmonats. Feiertage weichen um ≥ 2 h ab und bleiben erlaubt.
  - bei Zonen mit Sommerzeit (tick): kein belastbarer Anker mit Abweichung ≠ 0 innerhalb von 7 Tagen vor oder nach einer New-York-Umstellung (andere Sommerzeitregel, z.B. EU oder US vor 2007)
  - kein Wochenende, über das die belastbare Abweichung um genau 1 h springt
- Report-Felder: `status`, `ok`, `reason`, `raw_tz`, `anchors`, `reliable_anchors`, `offsets` (implizit), `expected_offsets`, `deviations`, `median_offset_h` (implizit, zur Anzeige), `median_deviation_h`, `share_expected`, `edge_violations`, `transition_violations`, `weekend_switches`, `deviating_examples`, `reversals`.
- Synthetisch geprüft (Tests `test_other_dst_rules_never_written_2001_2026`, `test_new_york_stamping_passes_every_month_2001_2026`, `test_thin_silver_one_tick_per_20_min`): New-York-Stempelung besteht 2001-2026 jeden Monat (XAUUSD, EURUSD). EU- und Vor-2007-US-Stempelung landen nie in einem geschriebenen UTC-Monat. Dünnes Silber (~1 Tick/20 min) besteht jeden Monat, fest-EST- und EU-gestempelt wird nie geschrieben. **Grenzen:** EURUSD (nur Wochenend-Anker) mit ~1 Tick/20 min um die Sonntagsöffnung kann eine nur eine Woche dauernde Verschiebung übersehen (echte EURUSD-Ticks sind dicht). Bei ~1 Tick/50 min fallen die meisten Monate durch (Quarantäne, also auf der sicheren Seite).
- Beginnt der gecachte Folge-Rohmonat schon mit ±1 h Abweichung, wird auch der Monat davor nicht geschrieben (Umstellung nach dessen letztem Anker). M+1 wird zuerst geholt, wenn es ohnehin gebraucht wird.
- Zu wenige Anker (z.B. kurzer erster Historienmonat): Prüfung gemeinsam mit gecachten Nachbar-Rohmonaten. Bleibt das Ergebnis unentscheidbar, folgt Quarantäne mit dem Grund "NICHT PRUEFBAR".
- Sonst **Quarantäne** (`<zip>.quarantine` mit vollem Prüfbericht, `raw_tz` und `tz_check_version`, Exit 4). Die UTC-Monate, die diesen Rohmonat brauchen, werden nicht geschrieben. Es wird nie verschoben geschrieben.
  - Freigeben nach manueller Prüfung: `"accepted": true` im Marker. Gilt nur für dieselbe Umrechnung (`raw_tz`). Ein Tick in nicht eindeutiger Ortszeit sperrt trotzdem.
  - Marker löschen: erneute Prüfung ohne Download.
  - ZIP löschen: neuer Download; dabei wird der Marker entfernt.
  - Nicht freigegebene Marker einer älteren Prüfversion (`tz_check_version` fehlt oder ist kleiner) oder anderer Zone werden entfernt (WARNING) und der Rohmonat ohne Download neu geprüft.
  - Freigaben unter einer anderen Umrechnung (Vorversion "fest UTC-5", ohne `raw_tz`) werden neu geprüft: besteht der Monat, verschwindet der Marker; sonst wird er als `accepted: false` mit Hinweis (`note`) neu geschrieben. Grund: die Freigabe galt der alten Umrechnung. Ein fest-EST-gestempelter Juli mit v1-Freigabe würde sonst 1 h zu früh geschrieben.
  - Ein unlesbarer Marker (kein gültiges JSON, z.B. Tippfehler beim Freigeben) bleibt liegen, wird als ERROR gemeldet und sperrt weiter.
- Grenzen: Im Winter sind New-York-Ortszeit und EST identisch und nicht unterscheidbar (darum sind alte Wintermonate inhaltlich richtig).

### Altbestand

Monatsdateien tragen das Parquet-Metadatum `source_timezone`, abgeleitet aus `RAW_TZ` und `TZ_CHECK_VERSION` (z.B. `America/New_York -> UTC, je Rohmonat geprueft (tz_check v3)`). Wird `RAW_TZ` geändert (z.B. für M1 nach einer Messung), passen alte Dateien automatisch nicht mehr. Zu Beginn **jedes** Laufs werden alle Monatsdateien des Symbols/Modus geprüft, nicht nur `--start`/`--end`:

- Gleiche Umrechnungszone, nur altes Metadatum (z.B. M1-Dateien der Vorversion): nur das Metadatum wird erneuert, die Datei nie verschoben, keine Requests (`relabelled_timezone`).
- Andere Umrechnung, die im Monat UTC-Zeiten verschiebt (Tick-Monate mit EDT aus der Fest-UTC-5-Version, 1 h verschoben): sofort nach `metals/stale/...` verschoben (nichts gelöscht; der Bot sieht eher eine Lücke als einen verschobenen Monat), auch außerhalb des Bereichs (`stale_retired_out_of_range`).
- Andere Umrechnung, im Monat aber gleiche UTC-Zeiten (Dezember bis Februar, nur EST): bleibt liegen, bis der Neubau sie atomar ersetzt (`stale_out_of_range`).

Monate im Bereich werden aus dem Roh-Cache neu gebaut (WARNING, `rebuilt_timezone`, Ergebnis `summary.months_rebuilt_timezone` bzw. `summary.months_stale_timezone_not_rebuilt`; der nächste Lauf versucht es erneut). Ohne Requests nur, wenn die Roh-ZIPs gecached sind; sonst wird normal geladen. `--verify-month` meldet den Zustand der Monatsdatei (`store_file`), ändert aber nichts.

### Weitere Beobachtungen Dritter (nicht selbst geprüft)

1. HDC `docs/powered-reconstruction-qualification.md`: "October 2025, contained a synchronized one-hour timestamp regression in all three source files". HDC lehnt solche Dateien ab. Code-Kommentar HDC `histdata_ascii.py` Z. 19: "Real HistData tick archives can preserve one one-hour source-order fallback around the late-October clock transition". Ende Oktober ist die **europäische** Zeitumstellung, nicht die US-amerikanische. Der Rücksprung-Check und der Monatsrand-Check fangen so etwas ab (Quarantäne).
2. AIT: je 60 exakt doppelte M1-Zeilen in **jedem Oktober** 2019-2024, also eine Stunde doppelt. Für M1 ein Hinweis auf eine Uhr mit Umstellung (unverifiziert, s. oben).
3. AIT: XAUUSD-Tickarchiv 2026-08, umgerechnet mit +5 h, beginnt am 2026-08-02 23:00 UTC, also erster Tick roh Sonntag 18:00. Gold öffnet sonntags 18:00 New York, im August 22:00 UTC. Passt zum Befund oben (New-York-Ortszeit).

Tests (synthetisch): Ticks in New-York-Ortszeit bestehen im Januar, März, Juli und November (XAUUSD, XAGUSD, EURUSD). Fest UTC-5 gestempelte Ticks fallen im Juli durch, UTC-gestempelte immer. M1 fest UTC-5 besteht im Januar und Juli, New-York-gestempeltes M1 fällt im Juli durch. Ein Wechsel mitten im Monat, eine Umstellung kurz vor Monatsende und eine wiederholte Stunde fallen durch. Ein Feiertag (früher Schluss) besteht. Ein Tick in der doppelten Stunde führt zu Quarantäne.

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

1. Zeitzone der **M1**-Dateien (Abschnitt 4): `--mode m1 --verify-month 2024-07`. Ticks sind verifiziert (New York mit Sommerzeit), M1 nicht.
2. Tick-Historienbeginn je Symbol.
3. Format der Status-.txt.
4. Ob HistData bestimmte User-Agents oder Request-Raten blockt, und wie eine Fehler-, Captcha- oder WAF-Seite aussieht. Jede Nicht-ZIP-Antwort mit Inhalt ist ein Fehlschlag mit Textausschnitt im Report, nie Daten.
5. Ob gekreuzte Quotes (ask < bid) in HistData-Ticks vorkommen. `fetch_dukascopy.validate` bricht bei mehr als 0,1 % gekreuzter Bars hart ab (Exit 2).
6. Speicherbedarf: Ein Tickmonat hat Millionen Zeilen (AIT: 5,2 Mio. XAUUSD 2026-08; HF: ~1,4 Mio. EURUSD, ~9 MB ZIP). Gemessen synthetisch: 1,7 Mio. Ticks parsen in 0,6 s (pyarrow.csv). Höchstens 2 Rohmonate liegen gleichzeitig im Speicher. Grob 1-2 GB RAM einplanen.
