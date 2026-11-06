# HistData.com: Download, Formate, Zeitzone - Recherche

Stand 2026-11-05. **histdata.com war aus dieser Umgebung nicht erreichbar** (Proxy 403, WebFetch: Host nicht auflösbar). Von hier aus wurde nichts live gegen HistData verifiziert. **Ausnahme: die Tick-Zeitzone**, gemessen vom Eigentümer im Homelab an echten Ticks (XAUUSD, XAGUSD, EURUSD; Abschnitt 4). Alles andere unten stammt aus Quellcode-Lektüre (nichts davon ausgeführt), aus Dokumenten Dritter, die echte HistData-Dateien verarbeitet haben, und aus einer Websuche. Getestet ist `scripts/fetch_histdata.py` ausschließlich mit synthetischen Daten und gemocktem HTTP.

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

### Verifizierter Befund: Ticks = Europe/Berlin minus 6 h (`berlin_minus_6`)

Zweite Messung des Eigentümers im Homelab an **echten** HistData-Ticks, **alle drei Symbole** (XAUUSD, XAGUSD, EURUSD), `--mode tick`:

| Rohmonat | Rohdaten | Folgerung |
|---|---|---|
| 2024-01 | impliziter Offset -5 h | passt (EU-Winter) |
| 2024-03 | ab 10.03. (US-Umstellung) bis Monatsende weiter -5 h: Tagespause roh 15:59:59 -> 17:00:00 statt 16:59 -> 18:00; Wechsel auf -4 h erst zur EU-Umstellung (31.03.) | nicht New York |
| 2024-07 | -4 h: Tagespause roh 16:59:58 -> 18:00:01, Wochenschluss Fr 16:59:58, Wochenstart So 18:00:01 | passt (EU-Sommer) |
| 2024-10/11 | ab 27.10. (EU-Umstellung) schon -5 h, nicht erst ab 03.11. | nicht New York |

**Schluss: Rohzeit = Europe/Berlin-Ortszeit minus 6 h**, also UTC-5 in der EU-Winterzeit und UTC-4 in der EU-Sommerzeit; Wechsel am letzten Sonntag im März bzw. Oktober um 01:00 UTC. Das widerlegt sowohl die HistData-Angabe "EST ohne Sommerzeit" als auch die erste Deutung (America/New_York, Prüfversion 2-4; die Juli-Messung allein konnte beide nicht unterscheiden). **Der Markt selbst bleibt New-York-basiert:** die erwarteten Anker sind weiter die CME/New-York-Sessionzeiten (17:00/18:00 America/New_York); nur die Stempelungsregel ist eine andere.

**M1 ist NICHT verifiziert.** Für M1 bleibt `fixed_est` (`Etc/GMT+5`, fest UTC-5) laut HistData-Angabe. Sie wird nicht stillschweigend umgestellt. Prüfen mit einem Sommermonat:

```
python scripts/fetch_histdata.py --symbol XAUUSD --mode m1 --verify-month 2024-07
```

Liegt die Median-Abweichung bei +1 (impliziter Offset -4 h statt -5 h), ist auch M1 mit Sommerzeit gestempelt; dann den m1-Fahrplan in `config/histdata_timezone.yaml` bewusst umstellen (z.B. auf `berlin_minus_6`, vorher mit dem Survey prüfen).

**EURUSD** wird nicht mehr über HistData geladen: keine Tagespause, zu wenige Anker je Monat, und `--verify-month 2024-11` bestand trotz falscher Stempelung (NY-Regel). Der Bot nutzt einen Tages-Referenzkurs (EZB/FRED). `--symbol EURUSD` wird mit diesem Hinweis abgelehnt; `fetch_dukascopy.py` ist unverändert. Jeder Lauf verschiebt ein altes HistData-EURUSD-Storeverzeichnis (Prüfversion 4) nach `metals/stale/` (WARNING, Laufreport `retired_removed_symbols`); Roh-Cache und Marker bleiben.

### Stempelungsregeln und Fahrplan (`config/histdata_timezone.yaml`)

- Regel (`TzRule`): Name, zoneinfo-Zone, ganze Stunden Verschiebung; Rohzeit = Ortszeit in `zone` + `shift_h`. Eingebaut: `berlin_minus_6` (Europe/Berlin, -6), `ny_local` (America/New_York, 0), `fixed_est` (Etc/GMT+5, 0). Weitere Regeln in der Datei (`rules`, Namen eindeutig, Zone wird geprüft).
- `schedule` je Modus: Liste `{from: "YYYY-MM", rule: name}` nach **Roh**monaten, aufsteigend; ein Wechsel gilt nur an Rohmonatsgrenzen. Default: tick `berlin_minus_6` ab 2000-01, m1 `fixed_est` ab 2000-01. Andere Datei: `$HISTDATA_TZ_CONFIG`. Unbekannte Regel/Zone, unsortiertes oder doppeltes `from`, fehlender Modus oder `schedule`, ein Fahrplan, der erst nach dem frühesten benötigten Rohmonat beginnt (Historienbeginn minus 1 Monat = 2009-02), oder eine fehlende Datei (auch die Default-Datei im Repo) → Abbruch beim Laden, nie still ein Default.
- Regel für einen Zeitraum ändern (z.B. falls ältere Jahre anders gestempelt sind): Kandidaten erst offline gegen den Roh-Cache abgleichen (`scripts/histdata_tz_survey.py`, nutzt `evaluate_raw_month`: nur Cache, kein Download, keine Marker, mit Nachbarmonaten als Kontext wie im echten Bau), dann einen Eintrag in `schedule` ergänzen. Der nächste Lauf erkennt betroffene Monatsdateien an `source_timezone` (s. Altbestand).

### Umrechnung (`raw_to_utc_ms`)

- `raw_to_utc_ms(raw_ms, rule)`: Rohziffern minus `shift_h` sind Wanduhrzeit in `rule.zone`; vektorisiert nach UTC: je möglichem Offset der Zone ein Kandidat, gültig, wenn die Zone in genau diesem UTC-Zeitpunkt diesen Offset hat (Wechseltabelle aus zoneinfo, je Jahr). Exakt auf die Millisekunde. Ein Rohmonat wird mit der Regel dieses Rohmonats umgerechnet (`raw_to_utc_ms_mode`).
- **Nie raten:** Ortszeiten in der Sommerzeit-Lücke existieren nicht, Ortszeiten in der doppelten Stunde sind mehrdeutig - für `berlin_minus_6` roh Samstag 20:00:00-20:59:59.999 in den beiden Umstellungsnächten (z.B. 2024-03-30, 2024-10-26). Beides ergibt `TimezoneError` (Unterklasse von `FormatError`) mit den ersten Beispielen. Der Markt ist dann geschlossen; ein Tick dort heißt, dass etwas nicht stimmt. Der Rohmonat geht per Default in Quarantäne (Exit 4). Liegen **alle** betroffenen Ticks im geschlossenen Wochenendmarkt (roh Samstag oder Sonntag vor 12:00), steht im Marker `"tz_error_releasable": true`; nach manueller Prüfung gibt `"accepted": true` frei: diese Ticks werden **verworfen** (nie umgerechnet), Anzahl und Rohzeiten stehen im Monatsreport (`dropped_ticks`, `dropped_raw`). Ein solcher Tick außerhalb des Wochenendes ist nie freigebbar.
- Folge: UTC-Monat M = Rohmonat M-1 (dessen letzte 4 h im EU-Sommer bzw. 5 h im EU-Winter) + Rohmonat M (ohne diese Stunden an seinem Ende). Die Rohmonate werden aus dem Fahrplan berechnet, nicht fest verdrahtet. EU-Umstellungen liegen nie auf einem Monatsrand (Sonntag 01:00 UTC am Monatsende, Rohzeit Samstag 20:00).

### Prüfung je Rohmonat (`check_timezone`, Prüfversion 5)

Grundsatz (Re-Review von v3, das echte Datenfehler als Zeitzonenfehler wertete): **hart nur Systematisches oder Bestätigtes, Einzelausreißer sind Warnungen.**

- Anker: je Marktlücke ein Schluss- und ein Öffnungsanker.
  - Wochenende: Lücke ≥ 24 h, Schluss Do/Fr (Freitag oder Feiertagsvorabend), Öffnung So/Mo; Schluss 17:00, Öffnung 18:00 New York. Andere Lücken ≥ 24 h sind Ausfälle: nur Bericht (`outages`), nie Anker.
  - Tagespause der Metalle Mo-Do (17:00-18:00 New York): nur wenn **alle** pausenartigen Lücken des Tages (45 min bis 3 h, verträglich mit ±1 h) dieselbe eindeutige Abweichung ergeben. Bei richtigen Daten passt die echte Pause immer zu 0, ein Ausfall (z.B. genau 20:00-21:00) kann sie also nicht ersetzen.
- **Kontext:** Die Ticks der ersten/letzten 8 Tage der gecachten Nachbar-Rohmonate werden mitbetrachtet. Lücken dort liefern Kontext-Einträge (für Läufe über den Monatsrand), aber keine Anker für Median und Anteil. Vorher hatte EURUSD (keine Tagespause) am Monatsrand gar keinen Anker (Review: EURUSD 2025-10 mit EU-Stempelung bestand mit `{0: 7, -1: 1}`).
- Je Anker der **implizite** Offset (Rohzeit minus UTC des Marktereignisses) und der **erwartete** Offset der Stempelungsregel in genau diesem Zeitpunkt (`berlin_minus_6`: -5 h im EU-Winter, -4 h im EU-Sommer; `fixed_est`: immer -5 h). `check_timezone(..., rule=...)` prüft gegen eine vorgegebene Kandidatenregel, sonst die Regel des Rohmonats; der Bericht nennt sie (`rule`). Entschieden wird über die **Abweichung** = implizit - erwartet.
- Die Abweichung einer Lücke wird **aus beiden Seiten gemeinsam** bestimmt: alle ganzen Abweichungen F, mit denen der Schluss nicht vor dem letzten Tick (Toleranz 15 min: verirrte Ticks, alter 17:15-Schluss) und die Öffnung nicht nach dem ersten Tick läge (Toleranz 5 min). Bei richtigen Daten jeder Dichte liegt 0 in F.
  - Genau ein Wert: **eindeutiger** Eintrag, auch wenn die Ticks Minuten vom Ereignis entfernt sind (dünne Daten). Nur diese zählen für Median und Anteil.
  - F leer (Widerspruch): je Seite ein Eintrag (Schluss = kleinste, Öffnung = größte verträgliche Abweichung).
  - F mehrdeutig (nur Wochenende): je Seite ein schwacher Eintrag, wenn der Tick höchstens 10 min an einer vollen Stunde liegt und der Markt dort aktiv ist (Nachbartick ≤ 5 min).
- **Hart** (Quarantäne):
  - Median-Abweichung der eindeutigen Anker ≠ 0, oder weniger als 60 % bei 0
  - **≥ 2 unabhängige Lücken in Folge mit derselben Abweichung ≠ 0**, ohne 0 dazwischen, mindestens eine im eigenen Monat (Stempelungswechsel, z.B. eine Woche nach EU- oder US-vor-2007-Regel); Kontext-Einträge zählen mit
  - Rücksprung ≥ 30 min in Dateireihenfolge, nicht eindeutige Ortszeit
- **Nur Warnung** (`warnings`, Monat wird geschrieben): einzelne Ausreißer (verirrter Tick nach dem Schluss oder vor der Öffnung, Feed stoppt Freitag 16:00 oder öffnet 1 h spät, Feiertag, Ausfall), widersprüchliche oder mehrdeutige Lücken. `edge_violations` und `transition_violations` sind nur noch Bericht.
- **Reihenfolge im Lauf:**
  - Ergebnisse ohne einen Nachbar-Rohmonat, der existieren sollte (abgeschlossen, in der Historie, nicht im Cache), sind **vorläufig**: weder gemerkt noch als Marker gespeichert (Grund beginnt mit "VORLAEUFIG"). Im Backfill wird Rohmonat M+1 vor dem Schreiben von M geholt, wenn er ohnehin gebraucht wird.
  - Ein einzelner abweichender Eintrag am Monatsrand ohne Kontext dahinter (`leading_suspect`/`trailing_suspect`) hält den Monat zurück, bis der Nachbar da ist.
  - Neuester Monat (M+1 noch nicht veröffentlicht): geschrieben mit Parquet-Metadatum `tz_right_padding=false`. Sobald M+1 im Cache ist (Laufbeginn oder -ende), wird aus dem Cache nachgeprüft: besteht → `true`, sonst nach `metals/stale/` und Quarantäne.
  - Reicht ein bestätigter Wechsel im Folge-Rohmonat M+1 in Monat M zurück (`switch_reaches_prev_month`), wird auch UTC-Monat M nicht geschrieben.
- Report-Felder: `status`, `ok`, `reason`, `raw_tz`, `anchors`, `entries`, `offsets` (implizit), `expected_offsets`, `deviations`, `median_offset_h` (implizit, zur Anzeige), `median_deviation_h`, `share_expected`, `switch_runs`, `warnings`, `outages`, `contradictions`, `ambiguous_gaps`, `padding`, `edge_violations`, `transition_violations`, `deviating_examples`, `reversals`.
- Synthetisch geprüft: siehe "Messungen v4" unten.
- Zu wenige Anker (z.B. kurzer erster Historienmonat): Prüfung gemeinsam mit gecachten Nachbar-Rohmonaten. Bleibt das Ergebnis unentscheidbar, folgt Quarantäne mit dem Grund "NICHT PRUEFBAR".
- Quarantäne: `<zip>.quarantine` mit vollem Prüfbericht, `raw_tz` und `tz_check_version`, Exit 4. Die UTC-Monate, die diesen Rohmonat brauchen, werden nicht geschrieben. Es wird nie verschoben geschrieben.
  - Freigeben nach manueller Prüfung: `"accepted": true` im Marker. Gilt nur für dieselbe Umrechnung (`raw_tz`). Ein Tick in nicht eindeutiger Ortszeit sperrt trotzdem.
  - Marker löschen: erneute Prüfung ohne Download.
  - ZIP löschen: neuer Download; dabei wird der Marker entfernt.
  - Nicht freigegebene Marker einer älteren Prüfversion (`tz_check_version` fehlt oder ist kleiner, auch v3) oder anderer Zone werden ohne Download neu geprüft; entfernt nur, wenn die Neuprüfung endgültig (nicht vorläufig) besteht.
  - Freigaben unter einer anderen Umrechnung (Vorversion "fest UTC-5", ohne `raw_tz`) werden neu geprüft: besteht der Monat, verschwindet der Marker; sonst wird er als `accepted: false` mit Hinweis (`note`) neu geschrieben.
  - Ein unlesbarer Marker (kein gültiges JSON, z.B. Tippfehler beim Freigeben) bleibt liegen, wird als ERROR gemeldet und sperrt weiter.
  - `--verify-month` ist nur Bericht: es schreibt und löscht keine Marker.
- Grenzen: Im Winter sind New-York-Ortszeit und EST identisch und nicht unterscheidbar (darum sind alte Wintermonate inhaltlich richtig).

### Altbestand

Monatsdateien tragen das Parquet-Metadatum `source_timezone` = Regel(n) ihrer Rohmonate mit Zone/Verschiebung und `TZ_CHECK_VERSION` (z.B. `berlin_minus_6 (Europe/Berlin -6 h) -> UTC, je Rohmonat geprueft (tz_check v5)`), dazu `tz_right_padding`. Wird der Fahrplan geändert, passen betroffene Dateien automatisch nicht mehr. Zu Beginn **jedes** Laufs werden alle Monatsdateien des Symbols/Modus geprüft, nicht nur `--start`/`--end`:

- Andere oder ältere Kennzeichnung, die im UTC-Monat **dieselben UTC-Zeiten** ergibt (ältere Prüfversion; `ny_local`-Dateien von Januar, Februar, April bis September und Dezember, wo beide Regeln übereinstimmen; M1-Dateien der Vorversionen): Rohmonate aus dem Cache unter der aktuellen Regel neu prüfen (kein Download). Besteht → nur Metadaten erneuert (`relabelled_timezone`). Fällt durch → nach `metals/stale/` und Quarantäne (`retired_after_recheck`). Roh-Cache fehlt → Datei bleibt, gemeldet als `unverified_after_version_bump`.
- `tz_right_padding=false` und Rohmonat M+1 jetzt im Cache: ebenso nachprüfen.
- Andere Umrechnung, die im Monat **UTC-Zeiten verschiebt** (`ny_local`: März, Oktober, November = die Wochen zwischen US- und EU-Umstellung; fest UTC-5: alle Monate mit EU-Sommerzeit): sofort nach `metals/stale/...` verschoben (nichts gelöscht; der Bot sieht eher eine Lücke als einen verschobenen Monat), im Bereich neu gebaut, außerhalb gemeldet (`stale_retired_out_of_range`).
- Quarantäne-Marker tragen `tz_rule`, `raw_tz` (Zone), `tz_shift_h`, `tz_check_version`. Marker von v4 (`ny_local`) werden ohne Download unter der neuen Regel neu geprüft; `"accepted": true` gilt nur für dieselbe Regel.

Monate im Bereich werden aus dem Roh-Cache neu gebaut (WARNING, `rebuilt_timezone`, Ergebnis `summary.months_rebuilt_timezone` bzw. `summary.months_stale_timezone_not_rebuilt`; der nächste Lauf versucht es erneut). Ohne Requests nur, wenn die Roh-ZIPs gecached sind; sonst wird normal geladen. `--verify-month` meldet den Zustand der Monatsdatei (`store_file`), ändert aber nichts.

### Messungen v4 (unter der damaligen Annahme America/New_York)

Die Zahlen unten stammen aus Prüfversion 4 mit `ny_local` als richtiger Stempelung und EURUSD; die Entscheidungslogik ist in v5 unverändert, nur die erwartete Regel (`berlin_minus_6`) und die Symbolliste (ohne EURUSD) sind neu. Die Regressionstests laufen mit `berlin_minus_6`: richtige Stempelung besteht 2001-2026 jeden Monat (XAUUSD, XAGUSD), US-Regel (`ny_local`) und US-Regel vor 2007 landen nie in einem geschriebenen UTC-Monat, dünnes Silber (~1 Tick/20 min) besteht jeden Monat und falsch gestempeltes (fest UTC-5, US-Regel) wird nie geschrieben, die Datenfehler des Re-Reviews (verirrte Ticks, Feed-Kanten, 17:15-Schluss, Ausfälle) bleiben Warnungen.

Synthetisch (Re-Review-Skripte `/tmp/claude-0/rereview_tz/`, Generator mit US-Feiertagskalender; eigene Durchläufe durch `run()`). Stand Prüfversion 4.

Richtige New-York-Stempelung, **falsche Quarantäne** (vorher v3 → nachher v4, je 69 Monate 2021-01..2026-09 je Symbol XAUUSD/XAGUSD/EURUSD):

| Fall | v3 | v4 |
|---|---|---|
| Feed stoppt Fr 16:00 (eine Woche) | 69/69/69 | 0 |
| Sonntag öffnet 1 h spät | 69/69/69 | 0 |
| verirrter Tick vor der Sonntagsöffnung | 69/69/69 | 0 |
| Ausfall Di 10:00 + 24,5 h | 69/69/8 | 0 |
| Ausfall + 26 h | 8/8/0 | 0 |
| Ausfall 20:00-21:01 jede Woche | 14/14/0 | 0 |
| Sonntag fehlt, Daten ab Montag | 0/0/69 | 0 |
| Einzeltests `stray` (Tick Fr 17:06/17:20, So -10/-30 min) | 24/24 | 0 |
| `close1715` (Fr-Schluss 17:15, alter CME-Plan) | 3/3 | 0 |
| `midweek` (Ausfälle ≥ 24 h) | 19/30 | 0 |
| `hourout` (60-min-Ausfall auf vollen Stunden) | 4/18 | 0 |

Vollständig 2009-01..2026-09 mit Feiertagen (`run1`, mit und ohne rechten Nachbarn): XAUUSD, XAGUSD, EURUSD je 213/213 bestanden. Dünnes Silber (~1 Tick/20 min, 2010-2025): 0 falsche Quarantänen.

Falsch gestempelte Daten **im echten Ablauf** (`run()`, Backfill bzw. Monat für Monat, Store nach jedem Schritt geprüft) - Zahl der geschriebenen UTC-Monate mit falsch umgerechneten Ticks:

| Stempelung | XAUUSD Backfill 2009-2026 | EURUSD Backfill 2016-2026 | Monat für Monat 2024-2026 (beide) |
|---|---|---|---|
| richtig (New York) | 0 (214 von 216 geschrieben, Historie ab 2009-03) | 0 (132/132) | 0 (36/36) |
| fest UTC-5 / UTC | 0 / 0 | 0 / 0 | - |
| EU-Regel / US-Regel vor 2007 | 0 / 0 | 0 / 0 | 0 / 0 |
| eine Woche +1 h mitten im Monat (jeder 3. Monat) | 0 | 0 | 0 |
| letzte Woche +1 h / -1 h über den Monatsrand | 0 / 0 | 0 / 0 | 0 / 0 |
| Re-Review `e2e_week` (EURUSD 2025-05/06) | - | UTC-Mai nicht geschrieben (v3: 7.196 verschobene Ticks geschrieben) | - |

**Bekannte Grenzen:**
- EURUSD hat nur Wochenend-Anker. Bei sehr dünnen Daten (~1 Tick/20 min) um die Sonntagsöffnung wird eine nur eine Woche dauernde Verschiebung oft nicht bestätigt (synthetisch 2010-2025: 30 Monate mit verschobenen Ticks geschrieben). Echte EURUSD-Ticks sind dicht; dort 0.
- Ab ~1 Tick/50 min fallen viele Monate als "nicht prüfbar" oder durch (Quarantäne, also auf der sicheren Seite).
- Der neueste Monat wird ohne rechten Nachbarn geschrieben (`tz_right_padding=false`). Ein Wechsel, der genau in dessen letzter Woche beginnt und dort nur einen abweichenden Eintrag hat, hält den Monat zurück (`trailing_suspect`). Ein Wechsel, der nach dem letzten Eintrag des Monats beginnt (ohne Lücke dazwischen), wird erst mit der Nachprüfung erkannt, sobald M+1 im Cache ist.
- Die Re-Review-Prüfskripte rufen `check_timezone` nur mit je 2 Randticks auf. Der echte Ablauf nutzt 8 Tage Kontext. Mit 2 Randticks melden `run3` EU/US-alt/letzte Woche bei EURUSD noch Rohmonate als "ok"; mit 8 Tagen und rechtem Nachbarn sind alle verschobenen Rohmonate durchgefallen. Im Ablauf werden sie nie geschrieben (Tabelle oben).

### Weitere Beobachtungen Dritter (nicht selbst geprüft)

1. HDC `docs/powered-reconstruction-qualification.md`: "October 2025, contained a synchronized one-hour timestamp regression in all three source files". HDC lehnt solche Dateien ab. Code-Kommentar HDC `histdata_ascii.py` Z. 19: "Real HistData tick archives can preserve one one-hour source-order fallback around the late-October clock transition". Ende Oktober ist die **europäische** Zeitumstellung, nicht die US-amerikanische. Der Rücksprung-Check und der Monatsrand-Check fangen so etwas ab (Quarantäne).
2. AIT: je 60 exakt doppelte M1-Zeilen in **jedem Oktober** 2019-2024, also eine Stunde doppelt. Für M1 ein Hinweis auf eine Uhr mit Umstellung (unverifiziert, s. oben).
3. AIT: XAUUSD-Tickarchiv 2026-08, umgerechnet mit +5 h, beginnt am 2026-08-02 23:00 UTC, also erster Tick roh Sonntag 18:00. Gold öffnet sonntags 18:00 New York, im August 22:00 UTC. Passt zum Befund oben (New-York-Ortszeit).

Tests (synthetisch, Prüfversion 5): Ticks nach `berlin_minus_6` bestehen im Januar, März, Juli, Oktober und November (XAUUSD, XAGUSD); nach US-Regel gestempelte Ticks fallen im März und Oktober durch (bestätigter Wechsel in den Lückenwochen). Fest UTC-5 gestempelte Ticks fallen im Juli durch, UTC-gestempelte immer. M1 fest UTC-5 besteht im Januar und Juli, New-York-gestempeltes M1 fällt im Juli durch. Ein Wechsel mitten im Monat, eine Umstellung kurz vor Monatsende und eine wiederholte Stunde fallen durch. Ein Feiertag (früher Schluss) besteht. Ein Tick in der doppelten Stunde führt zu Quarantäne.

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

1. Zeitzone der **M1**-Dateien (Abschnitt 4): `--mode m1 --verify-month 2024-07`. Ticks sind für 2024 verifiziert (`berlin_minus_6`), M1 nicht.
1a. Ob ältere Jahre anders gestempelt sind (vor 2024 nicht gemessen): `scripts/histdata_tz_survey.py` über mehrere Jahre laufen lassen und bei Bedarf den Fahrplan in `config/histdata_timezone.yaml` je Zeitraum ändern.
2. Tick-Historienbeginn je Symbol.
3. Format der Status-.txt.
4. Ob HistData bestimmte User-Agents oder Request-Raten blockt, und wie eine Fehler-, Captcha- oder WAF-Seite aussieht. Jede Nicht-ZIP-Antwort mit Inhalt ist ein Fehlschlag mit Textausschnitt im Report, nie Daten.
5. Ob gekreuzte Quotes (ask < bid) in HistData-Ticks vorkommen. `fetch_dukascopy.validate` bricht bei mehr als 0,1 % gekreuzter Bars hart ab (Exit 2).
6. Speicherbedarf: Ein Tickmonat hat Millionen Zeilen (AIT: 5,2 Mio. XAUUSD 2026-08; HF: ~1,4 Mio. EURUSD, ~9 MB ZIP). Gemessen synthetisch: 1,7 Mio. Ticks parsen in 0,6 s (pyarrow.csv). Höchstens 2 Rohmonate liegen gleichzeitig im Speicher. Grob 1-2 GB RAM einplanen.
