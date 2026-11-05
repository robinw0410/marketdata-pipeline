# Datenbestand

## Spot-Klines (1m)

| Symbol  | Erster Bar (UTC)  | Letzter Bar (UTC) | Bars      | Fehlend | Abdeckung  | OHLC-Fehler |
|---------|-------------------|-------------------|-----------|---------|------------|-------------|
| BTCUSDT | 2019-01-01 00:00  | 2026-09-30 23:59  | 4.071.111 | 4.089   | 99,8997 %  | 0           |
| ETHUSDT | 2019-01-01 00:00  | 2026-09-30 23:59  | 4.071.110 | 4.090   | 99,8996 %  | 0           |

Erwartet: 4.075.200 Bars je Symbol (2.830 Tage x 1440). Luecken: [GAPS.md](GAPS.md)

Stand:    Monatsarchive bis 2026-09, Oktober folgt nach Monatsende
Quelle:   data.binance.vision, monthly klines, jede Datei SHA256-verifiziert
Format:   Parquet/zstd, hive-partitioniert nach symbol/year
Zeitzone: durchgehend UTC

## Live-Collector

Start:        2026-08-20 15:23 UTC
Symbole:      BTCUSDT, ETHUSDT (1m)
Verifiziert:  2026-10-24 (checked day): BTCUSDT und ETHUSDT je 1440/1440 Bars,
              OHLCV identisch mit offiziellem Binance-Archiv

Dies ist der einzige garantiert out-of-sample Datensatz des Projekts.
Nicht loeschen, nicht ueberschreiben, nicht "aufraeumen".

## Metalle / FX (Dukascopy)

Symbole:  XAUUSD, XAGUSD, EURUSD (1m, Bid- und Ask-OHLC)
Stand:    noch nicht geladen (scripts/fetch_dukascopy.py)
Quelle:   datafeed.dukascopy.com; Default 1-Minuten-Kerzen je Tag und Seite
          (--mode candles), optional stuendliche Ticks (--mode ticks)
Format:   Parquet/zstd, hive-partitioniert nach source/symbol/year
Zeitzone: durchgehend UTC

Store:
    data/metals/source=dukascopy/symbol={SYM}/year={YYYY}/{SYM}-1m-{YYYY}-{MM}.parquet
    data/metals/reports/{SYM}-1m-{YYYY}-{MM}.json       Validierungsreport je Monat (mit "mode")
    data/metals/reports/{SYM}-dukascopy-run.json        Laufreport + Lueckenliste (letzter Lauf)
    data/metals/reports/{SYM}-verify-{YYYY-MM-DD}.json  Ergebnis --verify-day
    data/raw/dukascopy/{SYM}/{YYYY}/{MM}/{DD}/{HH}h_ticks.bi5           Roh-Cache Ticks
    data/raw/dukascopy/{SYM}/{YYYY}/{MM}/{DD}/{BID|ASK}_candles_min_1.bi5  Roh-Cache Kerzen
        (Monat hier 1-basiert; in der Dukascopy-URL ist er 0-basiert,
        Januar = 00. 404 als Marker <name>.404 gecached. Kerzendatei mit
        unklarer Feldreihenfolge: <name>.unclear, gilt NICHT als Cache.)
    Wurzel per MARKETDATA_DATA_ROOT ueberschreibbar (nur fetch_dukascopy.py).

Spalten: ts (UTC, ms, Label links [t, t+1min)), bid_open/high/low/close,
ask_open/high/low/close (float64), tick_count, bid_volume, ask_volume
(Rohwerte von Dukascopy), source = "dukascopy". Nur Minuten mit Handel,
nichts aufgefuellt. Parquet-Schema-Metadatum dukascopy_mode = candles|ticks.
- ticks:   tick_count = echte Tickzahl, Volumen = Summe der Tickvolumina.
- candles: tick_count = 1 fuer jede behaltene Minute (KEIN Tick-Zaehler,
           erfuellt nur tick_count >= 1), Volumen = Kerzenvolumen je Seite.

Formate (nicht offiziell dokumentiert):
- Ticks  {SYM}/{YYYY}/{MM0}/{DD}/{HH}h_ticks.bi5: LZMA, Records a 20 Byte
  big-endian '>3I2f' (ms seit Stundenbeginn, ask, bid, ask_volume,
  bid_volume). 0 Bytes = keine Ticks.
- Kerzen {SYM}/{YYYY}/{MM0}/{DD}/{BID|ASK}_candles_min_1.bi5: LZMA, Records
  a 24 Byte big-endian: uint32 Sekunden seit Tagesbeginn UTC, 4x uint32
  Preise, float32 volume. Preis = int / POINT_DIVISOR (wie Ticks).
  Feldreihenfolge der Preise wird JE DATEI aus den Daten bestimmt (s. u.).

Belege Kerzenformat (Quellcode gelesen, nichts ausgefuehrt, Dukascopy hier
nicht erreichbar -> nichts live verifiziert):
  npm dukascopy-node 1.46.4, dist/index.js (gleicher Code in
  dist/esm/chunk-V645LDJD.js); letzte Version mit datafeed/bi5, ab 1.49
  (2026-07) nutzt das Paket eine andere JSON-API mit AWS-WAF-Behandlung:
    Z. 16459-16468  URL_ROOT + ".../{MM}/{DD}/{SIDE}_candles_min_1.bi5"
    Z. 16348-16351  getUTCMonth() -> Monat 0-basiert
    Z. 16678-16679  Recordgroesse 24 (Kerzen) / 20 (Ticks)
    Z. 16690-16695  readInt32BE(0,4,8,12,16) + readFloatBE(20)
    Z. 16743-16745  [sec, open, close, low, high, volume], ts = sec*1000 + Tagesbeginn
    Z. 16899 / 1716 ignoreFlats (Default true): Kerzen mit volume == 0 verwerfen
    Z. 16486        laufender Tag -> Rueckfall auf Stunden-Ticks
    Z. 12935/12926/6149  decimalFactor XAUUSD 1e3, XAGUSD 1e3, EURUSD 1e5
  PyPI dukascopy-downloader 1.1.0 (neue JSON-API, nicht bi5), export.py Z. 31:
    "Dukascopy fills inactive minutes with flat candles" (Indiz fuer Fuellkerzen).
  Keine zweite unabhaengige bi5-Kerzen-Implementierung gefunden (tickterial,
  duka: nur Ticks; dukascopy-python, duka-dl: neue JSON-API).
  Abgleich: docs/dukascopy_candles_research.md (unabhaengige Recherche).

Unsicher / Absicherung:
- Feldreihenfolge: dukascopy-node sagt open, close, low, high (OCLH); eine
  StackOverflow-Quelle sagt OHLC. Der Decoder prueft je Datei, welche der
  beiden Reihenfolgen in ALLEN nicht flachen Kerzen high >= max(open, close)
  und low <= min(open, close) erfuellt. Genau eine -> verwendet (im
  Monatsreport field_order gezaehlt). Keine oder beide (z.B. close == high in
  allen aktiven Kerzen) -> Datei = Fehlschlag "Feldreihenfolge unklar"
  (Lueckenliste, Monat nicht geschrieben), es wird nicht geraten. Nur flache
  Kerzen -> Reihenfolge egal (alle Felder gleich). Zusaetzlich im Report:
  continuity = |open[t+1] - close[t]| in Punkten (Median, Anteil <= 1 Punkt).
- Zeitbasis Sekunden: hart geprueft (Offset < 86400 und Vielfaches von 60;
  Millisekunden wuerden sofort auffallen).
- Fuellkerzen: Annahme flache Kerzen mit volume 0. Verworfen wird eine
  Minute nur, wenn BEIDE Seiten volume == 0 UND flache Preise haben;
  volume 0 mit Preisbewegung bleibt (zero_volume_nonflat_minutes im Report).
  Minuten nur auf einer Seite werden verworfen und gezaehlt.
- Wochenende/Pausen: Datei fehlt (404) oder nur Fuellkerzen - beides ok.
  Tag mit Daten nur auf einer Seite: verworfen, days_one_side_only.
- Laufender Tag: Tagesdatei erst nach Tagesende; es werden nur
  abgeschlossene Monate geladen, und der Fetcher verweigert Kerzendateien
  des laufenden UTC-Tages grundsaetzlich.
- Volumen-Einheit nicht verifiziert; Werte unveraendert uebernommen.
- Preis = int / POINT_DIVISOR, XAUUSD 1000, XAGUSD 1000, EURUSD 100000.
  Abgesichert durch Plausibilitaetsbereich (XAUUSD 200..20000, XAGUSD
  3..500, EURUSD 0.5..2.0) und Jahresanker; ausserhalb harter Abbruch.
- Pruefen gegen Ticks auf dem LXC (ein Befehl, 26 Requests):
      python scripts/fetch_dukascopy.py --symbol XAUUSD --verify-day 2024-03-06
  Vergleicht je Minute bid/ask OHLC (Kerzen vs. aus Ticks aggregiert), gibt
  erkannte Feldreihenfolge, Minuten nur in Ticks/nur in Kerzen, max.
  Abweichung in Punkten, Volumenabweichung und continuity aus. Exit 0 =
  Uebereinstimmung (Toleranz --verify-tol 0.5 Punkte, --verify-max-share 0),
  4 = Abweichung, 3 = Download gescheitert.

Fehlertoleranz / Fortsetzen:
- Eine endgueltig gescheiterte Datei bricht den Lauf nicht ab: sie wird
  gesammelt, der Lauf geht weiter; am Ende --final-retries Runden (2) nach
  je --final-wait (300 s). Monatsdatei nur, wenn fuer den Monat keine
  ungeloesten Fehlschlaege bleiben (404 = keine Daten ist kein Fehlschlag).
- Laufreport immer (auch bei Luecken/SIGTERM/Validierungsfehler): Zusammen-
  fassung + gaps (Symbol, Monat, Datei, Tag/Stunde/Seite, URL, HTTP-Status,
  letzter Fehler, Versuche). Beim Start werden diese Dateien zuerst versucht.
- Rohdateien, Parquet und Reports atomar (.part + os.replace). Folgelauf
  laedt nur fehlende Dateien; Cache-Treffer sind keine Requests.
- Drosselung: --workers 1, --pause 1.0 s je Worker, --attempts 5,
  Backoff --backoff-base 2 s x 2^n mit Jitter, Deckel --max-backoff 600 s,
  Retry-After respektiert (bis Deckel); 403/429/503 -> globale Abkuehlung
  aller Worker. 403 = vermutlich WAF, wird wiederholt, im Report gezaehlt
  (http_403_waf_suspected), bei >= 50 % 403 deutlicher Hinweis (hint).
- Exit: 0 vollstaendig, 2 Validierungsfehler, 3 Luecken/unterbrochen,
  4 verify-day-Abweichung.

Validierung: hart (Abbruch, nichts geschrieben) bei Duplikaten, nicht
monotoner Zeit, inkonsistentem Bid-/Ask-OHLC, Preisen <= 0, NaN,
Plausibilitaetsbereich, Jahresanker, > 0,1 % Bars mit ask_close <
bid_close, kaputtem bi5 (Laenge, Offsets). Weich (nur Report): Spread > 20x
Monatsmedian, |Mid-Rendite 1m| > 2 %, Minuten ohne Ticks Mo-Fr gegen ein
grobes Raster, continuity. Keine harte Lueckengrenze: Metalle/FX haben
Tagespausen und Wochenenden; geplant/ungeplant entscheidet der
Handelskalender des Bots.

Lizenz: Dukascopy-Nutzungsbedingungen selbst pruefen. Daten (roh und
abgeleitet) nicht weitergeben und nicht committen.

## Wirtschaftskalender

Zweck:    News-Sperre des Gold/Silber-Bots (scripts/fetch_calendar.py)
Stand:    noch nicht geladen
Zeitzone: ts_utc UTC; Regeluhrzeiten lokal (America/New_York bzw.
          Europe/Berlin) per zoneinfo umgerechnet, nie fester Offset

Store:
    data/calendar/events.parquet                             aktueller Stand
    data/calendar/snapshots/events-YYYYMMDDTHHMMSSZ.parquet  Stand je Abruf
    data/calendar/reports/calendar-YYYYMMDDTHHMMSSZ.json     Report je Lauf
    data/raw/fomc/fomchistorical{YYYY}.htm                   Roh-Cache
    Wurzel per MARKETDATA_DATA_ROOT ueberschreibbar.

Schema (verbindlich, wird vom Bot gelesen):
    event_id    str   f"{event_type}:{Datum lokal YYYY-MM-DD}", stabil
    ts_utc      datetime64[ns, UTC]
    event_type  str   NFP, CPI, PCE, GDP, PPI, RETAIL_SALES, JOBLESS_CLAIMS,
                      FOMC_STATEMENT, FOMC_PRESS_CONF, FOMC_MINUTES,
                      ECB_RATE_DECISION, ECB_PRESS_CONF
    country     str   "US" | "EA"
    impact      str   "high" | "medium" | "low" (config/calendar.yaml)
    source      str   "fred" | "fomc" | "ecb" | "adhoc"
    status      str   "scheduled" | "rescheduled" | "cancelled" | "adhoc"
    fetched_at  datetime64[ns, UTC]  Lauf, der die Zeile zuletzt geliefert
                                     oder geaendert hat

Status-Regeln: gleiche event_id mit anderem ts_utc als im letzten Stand ->
rescheduled (bleibt es, solange ts_utc gleich bleibt). Verschwindet ein
Event einer Quelle innerhalb des neu abgerufenen Datumsbereichs (je source +
event_type), wird es cancelled, die Zeile bleibt. Eine Terminverschiebung auf
einen anderen Tag erscheint daher als cancelled (alter Tag) + neues Event
(neuer Tag). adhoc bleibt adhoc. Zeilen ausserhalb des abgerufenen Bereichs
bleiben unveraendert (die EZB-Seite zeigt nur den aktuellen Kalender; die
EZB-Historie waechst ab dem ersten Lauf).

Fehler je Quelle: Schlaegt eine Quelle fehl (Abruf/Parser/Plausibilitaet),
bleiben ihre Zeilen aus dem letzten Stand unveraendert (nicht cancelled),
die uebrigen Quellen werden aktualisiert und geschrieben; Report nennt die
Quelle (sources_failed), Exit 3 (4 bei Konfigfehler). Harte Validierung der
zusammengefuehrten Tabelle verletzt -> nichts geschrieben, Exit 2.

Regeln / Uhrzeiten:
- FRED-Releases 08:30 ET (Employment Situation=NFP, CPI, Personal Income
  and Outlays=PCE, GDP, PPI, Retail Sales, Jobless Claims)
- FOMC: Statement am letzten Sitzungstag. Uhrzeiten ET je Zeitraum
  (fomc.time_rules): bis 2012-12-31 Statement 14:15; Sitzungen MIT PK
  2011-04-27..2012-12-31 Statement 12:30, PK 14:15; ab 2013-01-01 Statement
  14:00, PK 14:30. PK ab 2019 bei jeder Sitzung, davor nur bei SEP-Sitzungen
  ab April 2011. Minutes 14:00 ET nur, wenn das Datum auf der Seite steht. unscheduled /
  notation vote / Conference Call werden uebersprungen (Report).
- EZB: Entscheidung 13:45, PK 14:30 Europe/Berlin bis 2022-07-20; ab
  2022-07-21 14:15 bzw. 14:45.
- Ad hoc (config/calendar_adhoc.csv): 2020-03-03 10:00 ET und 2020-03-15
  17:00 ET (FOMC-Notfallsitzungen), impact high.

Annahmen (ohne Netz erstellt, nicht verifiziert):
- FRED release_id 50/10/54/53/46/9/180 aus dem Gedaechtnis; abgesichert
  durch Namens-Check zur Laufzeit (falscher Name -> Abbruch, Exit 4).
- Seitenstruktur federalreserve.gov / ecb.europa.eu: Parser gegen selbst
  geschriebene Fixtures gebaut; abgesichert durch Plausibilitaet je Jahr
  (FOMC 7-10 regulaere Sitzungen, EZB 6-12 Entscheidungen).
- 08:30 ET fuer alle FRED-Releases ueber die ganze Historie; FOMC-Uhrzeiten
  vor 2013 (14:15 bzw. 12:30/14:15) aus Vorwissen.
- Nach jeder geldpolitischen EZB-Sitzung eine Pressekonferenz.
- ff-Titel (Kreuzcheck, nur Report).
