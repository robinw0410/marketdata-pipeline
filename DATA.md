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
        Januar = 00.) Nebendateien je Rohdatei (Marker-Logik s. u.):
          <name>.404 / <name>.empty  "keine Daten" (JSON: first_seen, confirmed)
          <name>.quarantine          Feldreihenfolge passt nicht (Rohinhalt)
          <name>.corrupt             beiseitegelegte kaputte Cache-Datei
    data/metals/.lock-{SYM}          Prozess-Lock je Symbol (flock)
    Wurzel per MARKETDATA_DATA_ROOT ueberschreibbar (nur fetch_dukascopy.py).
    Temporaerdateien: .{name}.{pid}.{thread}.part (Punkt vorn -> von
    pyarrow/hive ignoriert), fsync auf Datei und Verzeichnis, verwaiste
    werden beim Start aus Partition und Reports entfernt.

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
  bid_volume). 0 Bytes = keine Ticks (gecached als Marker .empty).
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
  und low <= min(open, close) erfuellt (Monatsreport field_order zaehlt):
    genau eine  -> verwendet
    beide       -> "ambiguous": geht nur, wenn in jeder aktiven Kerze
                   close == high; beide Lesarten liefern identische Werte,
                   Datei wird normal verwendet
    nur flache  -> "flat", Reihenfolge egal
    keine       -> QUARANTAENE: Datei als <name>.quarantine abgelegt (nicht
                   Cache, kein erneuter Download, kein Endlos-Retry), Monat
                   NICHT geschrieben, Laufreport "quarantine" mit Grund,
                   Exit 4 (manuelle Pruefung). Freigeben: --verify-day fuer
                   den Tag, dann <name>.quarantine loeschen -> neu laden.
  Zusaetzlich im Report: continuity = |open[t+1] - close[t]| in Punkten
  (Median, Anteil <= 1 Punkt).
- Zeitbasis Sekunden: hart geprueft (Offset < 86400 und Vielfaches von 60;
  Millisekunden wuerden sofort auffallen).
- Fuellkerzen: Annahme flache Kerzen mit volume 0. Verworfen wird eine
  Minute nur, wenn BEIDE Seiten volume == 0 UND flache Preise haben;
  volume 0 mit Preisbewegung bleibt (zero_volume_nonflat_minutes im Report).
  Minuten nur auf einer Seite werden verworfen und gezaehlt.
- "Keine Daten" (404 oder leere 200-Antwort) = Marker, nie 0-Byte-Rohdatei:
    Sa/So, 25.12., 1.1. -> sofort endgueltig (confirmed "auto")
    Mo-Fr               -> UNBESTAETIGT: Luecke (Laufreport "unconfirmed"),
                           Monat nicht geschrieben, Exit 3; endgueltig erst,
                           wenn ein Request >= 24 h spaeter wieder keine Daten
                           liefert (confirmed "repeat"); kommen dann Daten,
                           verschwindet der Marker
    Tickmodus: nur Werktags-404 braucht Bestaetigung (leere Stunden sind
    werktags normal, Tagespause). Alte 0-Byte-.404-Marker gelten als bestaetigt.
- Tag mit Daten nur auf einer Seite: ist die fehlende Seite nur "auto"
  bestaetigt (Wochenende), wird sie auf unbestaetigt gesetzt (Luecke);
  nach Wiederholungsbestaetigung wird der Tag verworfen (days_one_side_only).
- Laufender Tag: Kerzendateien erst 6 h nach Tagesende; Monate erst 6 h nach
  Monatsende. Der Fetcher verweigert fruehere Kerzendateien grundsaetzlich.
- Kaputte Cache-Datei (LZMA-Fehler, 0 Byte): als <name>.corrupt beiseite-
  gelegt, als Luecke gefuehrt und neu geladen (kein Validierungsabbruch).
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
  4 = Abweichung/Quarantaene, 3 = Download gescheitert.

Fehlertoleranz / Fortsetzen:
- Eine endgueltig gescheiterte Datei bricht den Lauf nicht ab: sie wird
  gesammelt, der Lauf geht weiter; am Ende --final-retries Runden (2) nach
  je --final-wait (300 s). Monatsdatei nur, wenn fuer den Monat keine
  ungeloesten Fehlschlaege bleiben (404 = keine Daten ist kein Fehlschlag).
- Laufreport immer (auch bei Luecken/SIGTERM/Validierungsfehler): Zusammen-
  fassung + gaps (Symbol, Monat, Datei, Tag/Stunde/Seite, URL, HTTP-Status,
  letzter Fehler, Versuche). Beim Start werden diese Dateien zuerst versucht.
- Rohdateien, Parquet und Reports atomar (versteckte Temp-Datei + fsync +
  os.replace + fsync Verzeichnis). Folgelauf laedt nur fehlende Dateien;
  Cache-Treffer sind keine Requests.
- Validierungsfehler werden je Monat gesammelt (validation_errors), die
  uebrigen Monate laufen weiter; Exit 2 erst am Ende.
- Drosselung: --workers 1, --pause 1.0 s je Worker, --attempts 5,
  requests.Session (Keep-Alive) je Worker. Jede Drossel-/Fehlerantwort
  (403/408/425/429/5xx, Timeout, Verbindungsfehler) erhoeht einen gemeinsamen
  Zaehler k (ueber Dateigrenzen hinweg) und setzt eine globale Abkuehlung
  fuer ALLE Worker: --backoff-base 2 s x 2^k mit Jitter, Deckel
  --max-backoff 600 s, Retry-After respektiert (bis Deckel); Reset erst bei
  200/404. Schutzschalter: nach --max-consecutive-failures (20) Fehlant-
  worten in Folge endet der Lauf sauber (Report, Exit 3, status
  circuit_breaker); den Neustart uebernimmt systemd. 403 = vermutlich WAF,
  im Report gezaehlt (http_403_waf_suspected), bei >= 50 % 403 Hinweis.
- Lock: je Symbol flock auf metals/.lock-{SYM}; ist er belegt (z.B.
  History-Service und Monats-Timer gleichzeitig), endet der zweite Lauf
  sofort mit Exit 3, ohne Reports zu schreiben.
- Exit-Codes (Vorrang in dieser Reihenfolge):
    2  Validierungsfehler in mindestens einem Monat (kein Neustart)
    3  Luecken: Download-Fehlschlaege, unbestaetigte "keine Daten",
       SIGTERM, Schutzschalter, Lock belegt -> spaeter erneut (Neustart ok)
    4  manuelle Pruefung noetig: Quarantaene (bzw. --verify-day-Abweichung)
       -> kein Neustart (Units behandeln 4 wie 2)
    0  vollstaendig
  3 geht BEWUSST vor 4: systemd startet bei 3 neu, bei 4 nicht; ein Exit 4
  bei offenen Luecken wuerde den Backfill anhalten. Damit Quarantaene
  trotzdem nicht untergeht: Laufreport status "<grund>+quarantine" (z.B.
  gaps+quarantine), Liste "quarantined" (Monate) oben im Laufreport und
  eine ERROR-Zeile "QUARANTAENE ..." in JEDEM Lauf, solange sie besteht.
- --verify-day --candles-only: eine Seite da, Gegenseite unbestaetigt leer
  -> Exit 3 (spaeter erneut). Ganztaegige Feiertage (25.12., 1.1.,
  Karfreitag) wie Wochenende: keine Daten = sofort bestaetigt (Exit 4
  "Handelstag waehlen"), Daten vorhanden = keine Mindest-Minutenzahl.
- 429: nach Ablauf einer Sperre laesst die gemeinsame Sperre genau EINEN
  Request durch (Probe); die anderen Worker warten, bis er ohne 429
  zurueck ist (sonst feuern alle gleichzeitig und holen die naechste
  Sperre). Ein persistierter Sperrzeitpunkt wird beim Lesen auf hoechstens
  jetzt + 24 h begrenzt.

Validierung: hart (Monat wird nicht geschrieben) bei Duplikaten, nicht
monotoner Zeit, inkonsistentem Bid-/Ask-OHLC, Preisen <= 0, NaN,
Plausibilitaetsbereich, Jahresanker, > 0,1 % Bars mit ask_close <
bid_close, kaputtem bi5 (Laenge, Offsets). Weich (nur Report): Spread > 20x
Monatsmedian, |Mid-Rendite 1m| > 2 %, Minuten ohne Ticks Mo-Fr gegen ein
grobes Raster, continuity. Keine harte Lueckengrenze: Metalle/FX haben
Tagespausen und Wochenenden; geplant/ungeplant entscheidet der
Handelskalender des Bots.

Lizenz: Dukascopy-Nutzungsbedingungen selbst pruefen. Daten (roh und
abgeleitet) nicht weitergeben und nicht committen.

## Metalle (HistData, Default-Quelle)

Symbole:  XAUUSD, XAGUSD (1m, Bid- und Ask-OHLC). EURUSD NICHT mehr ueber
          HistData (keine Tagespause, zu wenige Zeitanker, --verify-month
          2024-11 bestand faelschlich); der Bot nutzt einen Tages-
          Referenzkurs (EZB/FRED). --symbol EURUSD wird mit diesem Hinweis
          abgelehnt; fetch_dukascopy.py unveraendert. Jeder Lauf verschiebt
          ein altes HistData-EURUSD-Storeverzeichnis (Pruefversion 4) einmal
          nach metals/stale/ (WARNING, Laufreport retired_removed_symbols);
          Roh-Cache und Marker bleiben.
Stand:    noch nicht geladen (scripts/fetch_histdata.py)
Quelle:   histdata.com, Generic ASCII; Default Ticks je Monat (--mode tick),
          optional M1 nur Bid (--mode m1, EIGENE Partition). Dukascopy
          liefert im Homelab HTTP 429 je IP.
Format:   wie Dukascopy (Parquet/zstd, gleiche Spalten und dtypes), nur
          Partition source=histdata (Ticks) bzw. source=histdata_m1 (M1)
Zeitzone: UTC; Rohzeit Ticks laut Survey (vorlaeufiger Fahrplan, v6):
          America/New_York (ny_local) bis 2018-11, offen ("undecided":
          ny_local oder berlin_minus_6) 2018-12..2021-02, Europe/Berlin - 6 h
          (berlin_minus_6) ab 2021-03; M1 fest UTC-5 laut HistData (NICHT
          verifiziert). Regeln/Fahrplan: config/histdata_timezone.yaml. JE
          ROHMONAT im Code geprueft (s. u.)
Recherche, Quellen, Offenes: docs/histdata_research.md

Store (SRC = histdata im Tickmodus, histdata_m1 im M1-Modus):
    data/metals/source={SRC}/symbol={SYM}/year={YYYY}/{SYM}-1m-{YYYY}-{MM}.parquet
    data/metals/reports/{SYM}-{SRC}-1m-{YYYY}-{MM}.json   Validierungsreport je Monat
                                                          (inkl. Zeitzonen-Pruefung je Rohmonat)
    data/metals/reports/{SYM}-{SRC}-run.json              Laufreport (immer, auch bei Fehlern)
    data/metals/reports/{SYM}-{SRC}-verify-{YYYY-MM}.json Ergebnis --verify-month
    data/metals/raw/source=histdata/symbol={SYM}/mode={tick|m1}/
        HISTDATA_COM_ASCII_{SYM}_{T|M1}{YYYY[MM]}.zip      Roh-Cache (ZIP wie geladen)
        <zip>.empty                keine Daten (JSON: first_seen, last_checked,
                                   confirmed, confirmed_at, checks; "accepted": true
                                   = Luecke in der Historie manuell akzeptiert)
        <zip>[.YYYY-MM].quarantine Zeitzone/Format (JSON mit Pruefbericht,
                                   tz_rule, raw_tz, tz_shift_h, tz_check_version):
                                   loeschen = erneut pruefen (ohne Download);
                                   "accepted": true = nach manueller Pruefung frei;
                                   ZIP loeschen = neu laden, der Download entfernt
                                   den Marker und es wird neu geprueft; nicht
                                   freigegebene Marker aelterer tz_check_version
                                   oder anderer Regel (z.B. v4 ny_local): neu
                                   geprueft (ohne Download); "accepted": true gilt
                                   nur bei gleicher Regel (Zone + Verschiebung) -
                                   eine Freigabe unter anderer Regel wird neu
                                   geprueft: besteht -> Marker
                                   weg, sonst accepted false mit "note"; Tick in
                                   nicht eindeutiger Ortszeit oder Ruecksprung
                                   >= 30 min sperrt per Default; liegen ALLE
                                   betroffenen Ticks (Ruecksprung: Rohzeit in
                                   [back_to_raw, at_raw]; verbotene Stunden) im
                                   geschlossenen Wochenendmarkt (WEEKEND_CLOSED_RAW,
                                   Rohuhr: Fr ab 18:00, ganzer Sa, So vor 16:00),
                                   traegt der Marker "releasable_weekend"
                                   (reversal_ticks, impossible_hour_ticks,
                                   raw_ranges). Freigabe: fetch_histdata.py
                                   --symbol SYM --accept-weekend-ticks YYYY-MM
                                   (ROHmonat; zeigt die Ticks, kein Download; nicht
                                   freigebbar -> Exit 2) oder "accepted": true:
                                   genau diese Ticks werden VERWORFEN (nie
                                   umgerechnet), der Rest neu geprueft und bei
                                   Erfolg geschrieben; Monatsreport dropped_ticks,
                                   dropped_raw, warning. Ausserhalb des
                                   geschlossenen Wochenendmarkts nie freigebbar;
                                   unlesbarer Marker (JSON-Fehler): ERROR, bleibt,
                                   sperrt weiter
        <zip>.failed               dauerhafter Inhaltsfehler (JSON: error, count,
                                   last_checked); naechster Versuch fruehestens 24 h
        <zip>.corrupt              beiseitegelegtes kaputtes ZIP
    data/metals/state/histdata-ratelimit.json  429-Sperre (alle HistData-Prozesse)
    data/metals/.lock-histdata-{SYM}           Prozess-Lock je Symbol (unabhaengig von Dukascopy)

Spalten: wie Dukascopy (ts UTC ms, Label links; bid_/ask_open/high/low/close
float64; tick_count int64; bid_volume, ask_volume float64; source = SRC).
Parquet-Metadaten: histdata_mode = histdata_tick|histdata_m1,
spread_available = true|false, tz_right_padding = true|false, source_timezone
= Regel(n) der Rohmonate mit Zone/Verschiebung und TZ_CHECK_VERSION, z.B.
"berlin_minus_6 (Europe/Berlin -6 h) -> UTC, je Rohmonat geprueft (tz_check v6)"
(tick) bzw. "fixed_est (Etc/GMT+5 +0 h) -> ..." (m1); im undecided-Zeitraum
"ny_local (America/New_York +0 h) [undecided: ny_local|berlin_minus_6, hier
identisch] -> ...". v5 hatte alle Tick-Monate unter berlin_minus_6
geschrieben: fuer heute ny_local/undecided-Monate gilt unten "verschiebt"
(Maerz, Oktober, November -> metals/stale/, Neubau unter ny_local bzw.
rule_undecided) oder "dieselben UTC-Zeiten" (uebrige Monate -> neu
gekennzeichnet). Zu Beginn JEDES Laufs
werden alle Monatsdateien des Symbols/Modus geprueft (nicht nur
--start/--end):
  andere/aeltere Kennzeichnung, im UTC-Monat aber dieselben UTC-Zeiten
    (aeltere Pruefversion; ny_local- bzw. berlin_minus_6-Dateien von
    Jan/Feb/Apr-Sep/Dez, wo beide Regeln gleich sind; m1 der Vorversionen): Rohmonate aus dem Cache
    unter der aktuellen Regel neu pruefen (kein Download); besteht -> nur
    Metadaten erneuert (relabelled_timezone), faellt durch -> nach
    metals/stale/ + Quarantaene (retired_after_recheck), Roh-Cache fehlt ->
    Datei bleibt (unverified_after_version_bump)
  andere Umrechnung, die im Monat UTC-Zeiten verschiebt (ny_local <->
    berlin_minus_6: Maerz, Oktober, November = EU/US-Lueckenwochen; bei
    undecided: falls IRGENDEIN Kandidat abweicht; fest UTC-5: Monate mit
    EU-Sommerzeit): SOFORT nach metals/stale/... verschoben (nichts
    geloescht; der Bot sieht eher eine Luecke als einen verschobenen Monat),
    auch ausserhalb des Bereichs (stale_retired_out_of_range)
Monate im Bereich werden aus dem Roh-Cache neu gebaut (WARNING, Laufreport
rebuilt_timezone; Ergebnis summary.months_rebuilt_timezone bzw.
summary.months_stale_timezone_not_rebuilt, der naechste Lauf versucht es
erneut). Ohne Requests nur, wenn die Roh-ZIPs gecached sind; sonst wird
normal geladen. --verify-month ist nur Bericht: Zustand der Monatsdatei
(store_file) und Pruefergebnis; aendert keine Datei und schreibt/loescht
keine Quarantaene-Marker.
- tick: aus Ticks "YYYYMMDD HHMMSSmmm,bid,ask,volume" verdichtet
        (fetch_dukascopy.aggregate_1m, stabil sortiert: gleiche Zeitstempel
        behalten die Dateireihenfolge). tick_count = echte Tickzahl.
        Volumen: HistData liefert 0, bid_volume = ask_volume = 0.
- m1:   "YYYYMMDD HHMMSS;o;h;l;c;v", NUR Bid -> ask_* = bid_*, Spread 0,
        tick_count = 1. Den Spread liefert das Kostenmodell des Bots.
        Eigene Partition source=histdata_m1 (der Bot liest nur
        source=histdata/dukascopy), damit Bid-only-Monate nie unter
        Tick-Monate geraten. Eine vorhandene Monatsdatei mit anderem
        histdata_mode wird nie ueberschrieben (auch nicht mit --rebuild):
        store_mode_conflict, Exit 4. Exakt doppelte Minuten verworfen,
        abweichende Doppelminuten zusammengefasst (open erste, high max,
        low min, close letzte Zeile) und gezaehlt. Vergangene Jahre nur
        als Jahres-ZIP, laufendes Jahr monatlich.

Zeitzone und Monatsgrenze:
- Stempelungsregel (TzRule: Rohzeit = Ortszeit in zone + shift_h) je
  ROHmonat nach Fahrplan in config/histdata_timezone.yaml (andere Datei:
  $HISTDATA_TZ_CONFIG). Eingebaut: berlin_minus_6 (Europe/Berlin, -6),
  ny_local (America/New_York, 0), fixed_est (Etc/GMT+5, 0); weitere Regeln
  in der Datei (Name eindeutig, Zone geprueft). schedule je Modus:
  [{from: "YYYY-MM", rule: name}] aufsteigend, Wechsel nur an
  Rohmonatsgrenzen. Unbekannte Regel/Zone, unsortiert/doppelt, fehlender
  Modus oder schedule, Fahrplan beginnt nach dem fruehesten benoetigten
  Rohmonat (Historienbeginn minus 1 Monat = 2009-02), fehlende Datei (auch
  die Default-Datei im Repo) -> Abbruch beim Laden (nie still ein Default).
  Eintrag {from: "YYYY-MM", rule: undecided, candidates: [a, b, ...]}
  (>= 2 bekannte Regeln) = Regel noch offen (UndecidedRule).
- VORLAEUFIGER Tick-Fahrplan (v6) nach dem Survey des Owners (Homelab,
  XAUUSD/XAGUSD; Belegtabelle im Kommentar der YAML):
    {from: "2000-01", rule: ny_local}
    {from: "2018-12", rule: undecided, candidates: [ny_local, berlin_minus_6]}
    {from: "2021-03", rule: berlin_minus_6}
  m1 fixed_est ab 2000-01. Belege: 2010, 2012, 2015, 2018 (Maerz und
  Okt/Nov, beide Metalle) eindeutig ny_local; 2021-03 und 2024-03 eindeutig
  berlin_minus_6; 2021-10 und 2024-10 berlin_minus_6 bis auf EINEN 60-min-
  Ruecksprung (roh Sa 21:00 -> 20:00, verirrte Ticks im geschlossenen Markt
  der EU-Umstellungsnacht; freigebbar, s. Quarantaene). Nicht untersucht:
  2009, 2011, 2013, 2014, 2016, 2017, 2019, 2020, 2022, 2023, 2025.
- Warum der Wechsel an jeder Rohmonatsgrenze dazwischen liegen kann: beide
  Regeln liefern ausserhalb zweier Fenster je Jahr dieselbe Abbildung roh ->
  UTC (UTC-5 im Winter, UTC-4 im Sommer). Nur in den Fenstern rechnet
  ny_local UTC-4, berlin_minus_6 UTC-5: Maerz vom US-Beginn (2. So 07:00 UTC)
  bis zum EU-Beginn (letzter So 01:00 UTC), Okt/Nov vom EU-Ende (letzter So
  Okt 01:00 UTC) bis zum US-Ende (1. So Nov 06:00 UTC); mit zoneinfo
  nachgerechnet (Liste 2009-2025 in config/histdata_timezone.yaml), z.B.
  2019: 03-10 07:00 .. 03-31 01:00 und 10-27 01:00 .. 11-03 06:00 UTC;
  2020: 03-08 07:00 .. 03-29 01:00 und 10-25 01:00 .. 11-01 06:00 UTC.
- tick berlin_minus_6 = UTC-5 im EU-Winter, UTC-4 in der EU-Sommerzeit
  (Wechsel letzter So Maerz/Oktober 01:00 UTC); 2024-03 bleibt von der
  US-Umstellung (10.03.) bis Monatsende bei -5 h (Tagespause roh 15:59:59 ->
  17:00:00), 2024-10 ab 27.10. (EU) schon -5 h. ny_local = America/New_York
  (Wechsel 2. So Maerz / 1. So November). Die Marktzeiten bleiben New York
  (Anker 17:00/18:00 America/New_York). m1 fixed_est = fest UTC-5
  (HistData-Angabe, fuer M1 NICHT verifiziert; pruefen mit --mode m1
  --verify-month 2024-07).
- undecided: raw_to_utc_ms rechnet alle Kandidaten; identisch -> Ergebnis,
  sonst RuleUndecided. Ein UTC-Monat, in den ein verschieden umgerechneter
  Tick (unter irgendeinem Kandidaten) fallen kann, wird NICHT geschrieben:
  Laufreport months_rule_undecided (auch in summary) mit raw_month,
  raw_ranges, differing_ticks; Eintrag kind rule_undecided in "quarantined",
  Exit 4 (3 hat Vorrang), KEIN Marker, keine Extra-Requests, jeder Lauf
  prueft neu aus dem Cache (bis der Fahrplan entschieden ist). UTC-Monate,
  die vom Rohmonat nur identisch umgerechnete Ticks brauchen (z.B. UTC-April
  mit den letzten Stunden des Rohmaerz), werden geschrieben. Pruefung unter
  dem ersten Kandidaten; hat der Rohmonat verschieden umgerechnete Ticks,
  unter jedem Kandidaten (bestanden, wenn einer passt).
- Regel fuer einen Zeitraum aendern: Eintrag in schedule (z.B.
  {from: "2012-01", rule: berlin_minus_6} nach {from: "2000-01", rule:
  fixed_est}); vorher Kandidaten offline gegen den Roh-Cache abgleichen
  (scripts/histdata_tz_survey.py, nutzt evaluate_raw_month: nur Cache, kein
  Download, keine Marker). Der naechste Lauf erkennt betroffene
  Monatsdateien an source_timezone (s. o.).
- Umrechnung raw_to_utc_ms(raw, rule): Rohziffern - shift_h als Ortszeit der
  Regel-Zone, exakt nach UTC. Ortszeit in der Sommerzeit-Luecke oder der
  doppelten Stunde (berlin_minus_6: roh Samstag 20:00-20:59:59.999 in den
  Umstellungsnaechten) wird NIE geraten: TimezoneError -> Quarantaene (Exit
  4); der Markt ist dann geschlossen.
- UTC-Monat M = letzte 4 h (EU-Sommer) bzw. 5 h (EU-Winter/M1) des
  Rohmonats M-1 + Rohmonat M ohne diese Stunden an seinem Ende (aus dem
  Fahrplan berechnet). Geschrieben wird M nur, wenn beide Rohmonate erledigt
  sind (Daten, bestaetigt leer oder vor Historienbeginn).
- Pruefung je Rohmonat (check_timezone, Pruefversion 6): je Marktluecke ein
  Schluss- und ein Oeffnungsanker: Wochenende (>= 24 h, Schluss Do/Fr,
  Oeffnung So/Mo; Fr 17:00 -> So 18:00 New York;
  andere Luecken >= 24 h sind Ausfaelle: nur Bericht "outages"),
  Tagespause der Metalle Mo-Do (17:00-18:00 New York; nur wenn ALLE
  pausenartigen Luecken des Tages dieselbe eindeutige Abweichung <= 1 h
  ergeben - so ersetzt kein Ausfall die echte Pause). Kontext: die Ticks der
  ersten/letzten 8 Tage der gecachten Nachbar-Rohmonate (Luecken ueber den
  Monatsrand). Erwartete Marktzeiten per zoneinfo America/New_York,
  erwarteter Offset je Anker aus der Regel des Rohmonats im Ereigniszeitpunkt
  (berlin_minus_6 -5 h im EU-Winter, -4 h im EU-Sommer; fixed_est immer
  -5 h); entschieden wird ueber Abweichung =
  implizit - erwartet, je Luecke aus beiden Seiten gemeinsam (Schluss nicht
  vor dem letzten Tick, Toleranz 15 min; Oeffnung nicht nach dem ersten,
  5 min) - auch bei duennen Daten richtig.
  HART (Quarantaene) nur Systematisches oder Bestaetigtes:
    Median-Abweichung != 0 oder < 60 % der eindeutigen Anker bei 0,
    >= 2 UNABHAENGIGE Luecken in Folge mit derselben Abweichung != 0 ohne 0
      dazwischen, mindestens eine im eigenen Monat (Stempelungswechsel, z.B.
      eine Woche nach EU- oder US-vor-2007-Regel; Kontext zaehlt mit),
    Ruecksprung >= 30 min in Dateireihenfolge, nicht eindeutige Ortszeit.
  Nur WARNUNG (Monat wird geschrieben): einzelne Ausreisser - verirrter Tick
  nach Schluss/vor Oeffnung, Feed stoppt Fr 16:00 oder oeffnet 1 h spaet,
  Schluss 17:15, Feiertag, Ausfall >= 24 h unter der Woche, Ausfall genau
  60 min auf vollen Stunden -, widerspruechliche/mehrdeutige Luecken.
  Report: raw_tz, offsets, expected_offsets, deviations, median_offset_h
  (implizit), median_deviation_h, share_expected, switch_runs, warnings,
  outages, contradictions, edge_violations/transition_violations (nur
  Bericht), padding, deviating_examples, reversals, large_reversals (je
  Ruecksprung at_raw, back_to_raw, minutes, n_ticks, first_raw, last_raw,
  weekend_closed), reversal_ticks_total, releasable_weekend (nur wenn alle
  Ruecksprung-/Verbotene-Stunden-Ticks im geschlossenen Wochenendmarkt).
  Reihenfolge: Ergebnisse ohne einen Nachbar-Rohmonat, der existieren sollte
  (abgeschlossen, in der Historie, nicht im Cache), sind VORLAEUFIG: weder
  gemerkt noch als Marker gespeichert. Ein einzelner abweichender Eintrag am
  Monatsrand ohne Kontext dahinter haelt den Monat zurueck (vorlaeufige
  Quarantaene), bis der Nachbar da ist. Neuester Monat (Rohmonat M+1 noch
  nicht veroeffentlicht): geschrieben mit Parquet-Metadatum
  tz_right_padding=false, nachgeprueft, sobald M+1 im Cache ist (Laufbeginn
  und -ende): besteht -> true, sonst nach metals/stale/ + Quarantaene.
  Reicht ein bestaetigter Wechsel im Folge-Rohmonat M+1 in Monat M zurueck,
  wird auch UTC-Monat M nicht geschrieben. Zu wenige Anker (kurzer erster
  Historienmonat): Pruefung gemeinsam mit gecachten Nachbar-Rohmonaten
  (joint_with im Report); bleibt es unpruefbar -> Quarantaene "NICHT
  PRUEFBAR". Sonst QUARANTAENE (Exit 4), nichts verschoben geschrieben.
  Synthetisch geprueft: s. docs/histdata_research.md (Zahlen, Grenzen).
- Unverifiziert: Zeitzone der M1-Dateien. Drittquellen sehen je Oktober
  60 doppelte M1-Minuten (Hinweis auf eine Uhr mit Umstellung). Vor dem
  ersten M1-Volllauf: --mode m1 --verify-month 2024-07; liegt die
  Median-Abweichung bei +1, den m1-Fahrplan in config/histdata_timezone.yaml
  bewusst umstellen.

Verfuegbarkeit / "keine Daten":
- Historienbeginn (ASCII M1, Auswahlliste): XAUUSD 2009-03, XAGUSD 2009-05.
  Tick-Beginn NICHT verifiziert. Monate davor ohne Request
  als nicht verfuegbar gelistet.
- Angefragt wird ein Monat erst nach Rohmonatsende (UTC-Monatsende + 4 h
  im EU-Sommer bzw. + 5 h im EU-Winter/M1) + 6 h. Leerer Body (keine Daten):
    bis 45 Tage nach Monatsende -> pending (Exit 3), erneute Pruefung
                                   fruehestens nach 24 h, nie bestaetigt
    danach                      -> unbestaetigt (Exit 3), bestaetigt, wenn
                                   eine Pruefung >= 24 h spaeter wieder leer ist
- Bestaetigt leer VOR dem ersten Monat mit Daten (Tick-Historie beginnt
  spaeter als M1) -> months_unavailable, kein Exit 3/4.
- Bestaetigt leer INNERHALB der Historie (nach dem ersten Monat mit Daten,
  oder ueberhaupt noch nie Daten fuer das Symbol) -> verdaechtig:
  months_missing_in_history, Exit 4 (wenn sonst keine Luecken), erneute
  Pruefung alle 30 Tage bzw. sofort mit --rebuild. Nach manueller
  Pruefung "accepted": true im .empty-Marker -> unavailable, Exit 0.
- 3 leere Antworten in Folge innerhalb der Historie in EINEM Lauf ->
  Lauf beendet (status aborted, Exit 3, Hinweis Token/WAF/Server).
- Dauerhafte Inhaltsfehler (Seite ohne Formular file_down, Formular passt
  nicht, HTML/Text statt ZIP, ZIP ohne/mit falscher CSV): EIN Versuch je
  Lauf, kein Schutzschalter, Marker <zip>.failed, naechster Versuch
  fruehestens nach 24 h; spaetere Monate laufen weiter (Report
  content_errors, Exit 3). 3 Inhaltsfehler in Folge -> Lauf beendet
  (status aborted).
- Kaputtes Cache-ZIP (BadZipFile/CRC) -> <zip>.corrupt, neu geladen.
  Unlesbare CSV (Spalten, Trenner, Zeitstempel) -> Quarantaene.

Drosselung (ein Request zur Zeit, je Datei 2 Requests: Seite + POST):
- --pause 5 s Mindestabstand, --attempts 5, Abkuehlung --backoff-base 10 s
  x 2^k (Deckel --max-backoff 600 s) bei 5xx/403/408/425/Timeout/
  Verbindungsfehler/abgeschnittenem ZIP, Schutzschalter nach
  --max-consecutive-failures 10 Fehlschlaegen in Folge (Exit 3).
- HTTP 429 ueber scripts/ratelimit.py: vor jedem Request gate.wait(); bei
  429 Pause fuer alle max(Retry-After, --rate-limit-pause) (Default 900 s,
  kleiner abgelehnt), persistiert in metals/state/histdata-ratelimit.json;
  KEIN Versuch verbraucht, keine Eskalation, kein Schutzschalter. 429 auf
  den POST: nach der Pause wieder ab Seiten-GET (frisches Token). Nach
  --max-rate-limit-wait (21600 s) Lauf sauber beendet (Exit 3, Report).
- Requests: XAUUSD-Gesamthistorie im Tickmodus ca. 210 Monate x 2 = 420.

Exit-Codes (Vorrang wie Dukascopy):
    2  Validierungsfehler (fetch_dukascopy.validate) in mindestens einem Monat
    3  Luecken, Inhaltsfehler, pending, unbestaetigt, Lauf abgebrochen
       (leer/Inhaltsfehler in Folge), 429-Budget, Schutzschalter, SIGTERM,
       Lock belegt -> spaeter erneut
    4  manuelle Pruefung: Quarantaene (Zeitzone, Format, nicht pruefbar),
       bestaetigt leerer Monat innerhalb der Historie, store_mode_conflict
    0  vollstaendig
  3 geht BEWUSST vor 4 (systemd startet bei 3 neu, bei 4 nicht; sonst
  stoppt der Backfill). Sichtbar bleibt die manuelle Pruefung trotzdem:
  status "<grund>+quarantine", Liste "quarantined" (Monat, Art, Grund) oben
  im Laufreport, ERROR-Zeile "MANUELLE PRUEFUNG NOETIG ..." in jedem Lauf.

Pruefen auf dem LXC (max. 4 Requests, schreibt nur den Verify-Report):
    runuser -u mkt -- env MARKETDATA_DATA_ROOT=/opt/marketdata/data \
      /opt/marketdata/.venv/bin/python /opt/marketdata/scripts/fetch_histdata.py \
      --symbol XAUUSD --verify-month 2024-03

Lizenz: HistData-Nutzungsbedingungen selbst pruefen (hier nicht
erreichbar, nicht gelesen). Daten nicht weitergeben und nicht committen.

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

## Zeitzonen-Survey (HistData-Ticks)

Prueft, welche Stempelregel die HistData-Ticks wann hatten, und grenzt den
Wechsel ein. Kandidaten: `ny_local` (America/New_York-Ortszeit),
`berlin_minus_6` (Europe/Berlin minus 6 h), `fixed_est` und eigene Regeln aus
config/histdata_timezone.yaml. Nur Tick-Modus, XAUUSD und XAGUSD.

Stand (erster Survey, Exit 4, 124 Requests): 2010, 2012, 2015, 2018 (Maerz und
Okt/Nov, beide Metalle) eindeutig ny_local; 2021-03 und 2024-03 eindeutig
berlin_minus_6; 2021-10 und 2024-10 "keine Regel passt" nur wegen eines
60-min-Ruecksprungs in der Dateireihenfolge (verirrte Ticks im geschlossenen
Wochenendmarkt der EU-Umstellungsnacht: unter berlin_minus_6 springt die
Rohuhr Sa 21:00 -> 20:00). Der Wechsel liegt also zwischen 2018-11 und 2021-03.

Erweiterter Scan (Homelab, alle Jahre ab Historienbeginn, Pflichtmonate 2019/2020):

    runuser -u mkt -- env MARKETDATA_DATA_ROOT=/opt/marketdata/data /opt/marketdata/.venv/bin/python /opt/marketdata/scripts/histdata_tz_survey.py --preset full --periods 2019-03 2019-10 2019-11 2020-03 2020-10 2020-11 --json /opt/marketdata/data/metals/reports/histdata-tz-survey-full.json --markdown /opt/marketdata/data/metals/reports/histdata-tz-survey-full.md

Erwartete Requests: der Plan umfasst 2009-2025 x Maerz/Oktober/November x 2
Metalle = 101 Rohdateien (XAGUSD 2009-03 liegt vor Historienbeginn; die
--periods-Monate sind im Preset schon enthalten). 36 davon liegen seit dem
ersten Survey im Cache (2010/2012/2015/2018/2021/2024 x 3 Monate x 2), es
fehlen also hoechstens 65 Rohmonate = 130 Requests (je Rohmonat Seiten-GET +
POST), bei 5 s Pause mindestens ca. 11 min. Was sonst schon im Roh-Cache
liegt (z.B. vom normalen Fetcher), wird nicht geladen; die genaue Zahl steht
vor dem ersten Download auf der Konsole ("Plan: ...", "Geplant: N Rohmonate
laden = 2N Requests"). Mit dem Default --max-downloads 80 reicht ein Aufruf.
Bricht er ab (429, Sperre aktiv, Download gescheitert), endet er mit Exit 3
und nennt die noch fehlenden Rohmonate ("noch fehlend: N"); derselbe Befehl
spaeter setzt fort, der Cache bleibt.

Monate waehlen:
- ohne Optionen: Jahre 2010 2012 2015 2018 2021 2024, Umstellungsmonate
  3 4 10 11, Kontrollmonate 1 7 (alter Default).
- `--preset full`: alle Jahre vom fruehesten Historienbeginn (2009) bis 2025,
  Monate 3 10 11, keine Kontrollmonate. --years/--months/--controls
  ueberschreiben das Preset.
- `--periods YYYY-MM ...`: einzelne Rohmonate zusaetzlich; ohne --preset und
  --years nur diese.
- weitere Optionen: --symbols, --rules, --offline (nur Cache, kein Request),
  --max-downloads N (Rohmonate je Aufruf, default 80), --pause (default 5 s),
  --json PATH, --markdown PATH, -v.

Ablauf: Fehlende Rohmonate werden in den normalen Roh-Cache geladen, ueber den
Client von fetch_histdata.py mit Symbol-Lock und der gemeinsamen 429-Sperre
(data/metals/state/histdata-ratelimit.json). Danach wird jeder gecachte
Rohmonat mit jeder Regel geprueft (fetch_histdata.evaluate_raw_month mit
drop_releasable_weekend=True: nur Cache, schreibt nichts). Freigebbare
Wochenend-Ticks (alle verbotenen Ticks im geschlossenen Wochenendmarkt, z.B.
roh Sa 20:00-20:59 in der EU-Umstellungsnacht) werden dabei verworfen, nie
umgerechnet - wie im Fetcher nach einer Freigabe.

Ergebnis lesen:
- Tabelle je Symbol: je Rohmonat und Regel ok / failed / not_checkable und
  die Median-Abweichung in h, z.B. "ok +0.0 (3 Wochenend-Ticks verworfen)";
  Art T = Umstellungsmonat, K = Kontrollmonat; Spalte "konfiguriert" = Regel
  laut Fahrplan ("offen" = undecided). Darunter jeder grosse Ruecksprung in
  der Dateireihenfolge mit Rohzeit, Minuten, Tick-Zahl und ob er im
  geschlossenen Wochenendmarkt liegt.
- Entscheidend ist ein Rohmonat, in dem GENAU EINE Regel besteht und alle
  anderen durchfallen (Monate mit verworfenen Wochenend-Ticks zaehlen normal).
- Umstellungsperioden: je Jahr "Maerz" (Rohmonat 03) und "Okt/Nov" (10 + 11).
  Nur dort unterscheiden sich ny_local und berlin_minus_6: Maerz vom US-Beginn
  (2. So Maerz) bis zum EU-Beginn (letzter So Maerz), Okt/Nov vom EU-Ende
  (letzter So Oktober) bis zum US-Ende (1. So November). Urteil je Symbol =
  die eine Regel, die in den Monaten der Periode entscheidend besteht; nicht
  entscheidende oder fehlende Monate widersprechen nicht, zwei Monate mit
  verschiedenen Gewinnern sind ein KONFLIKT. Gesamturteil: wo beide Metalle
  entscheiden, muessen sie uebereinstimmen, sonst KONFLIKT. Bestaetigt =
  fuer mindestens ein Metall entscheidend und ohne Widerspruch.
- Wechselanalyse: Zahl der Wechsel und Konflikte, letzte bestaetigte Periode
  der alten Regel, erste der neuen, offene Perioden dazwischen (auch nicht
  untersuchte). Verlangt ist hoechstens EIN Wechsel und kein Konflikt; sonst
  Exit 4 mit Erklaerung und ohne Vorschlag.
- Fahrplan-Vorschlag (YAML zum Kopieren, mit Evidenztabelle Periode x Symbol
  und Begruendung als Kommentar; die Config wird nie geschrieben):

      - {from: "2000-01", rule: ny_local}       # bis zur letzten ny_local-Periode
      - {from: "<Monat danach>", rule: undecided, candidates: [ny_local, berlin_minus_6]}
      - {from: "<YYYY-03 oder YYYY-10>", rule: berlin_minus_6}   # erste berlin-Periode

  Der undecided-Eintrag kommt nur, wenn zwischen den beiden Perioden
  Umstellungsperioden offen sind (fehlend oder nicht entscheidend). Begruendung:
  Rohmonate ohne Umstellungsfenster (Dezember bis Februar, April bis
  September) sind unter beiden Regeln identisch - der Survey rechnet das
  stuendlich nach und nennt die Fenster -, daher ist jede Grenze zwischen der
  letzten ny_local- und der ersten berlin_minus_6-Periode fuer diese Monate
  gleichwertig. Ein Block (gleiche Regel) braucht mindestens 2 entscheidende
  Monate ODER beide Metalle im selben Monat, sonst "zu wenig Evidenz" und kein
  Vorschlag. Bei Exit 3 ist der Vorschlag als VORLAEUFIG markiert.
- --markdown schreibt Evidenztabelle, Wechselanalyse, Vorschlag, Begruendung,
  verworfene Wochenend-Ticks und Ruecksprunge als Markdown (fuer die Doku und
  die Kommentare in config/histdata_timezone.yaml).
- Exit 4: die konfigurierte Regel faellt in einem Monat durch, dessen
  Fahrplan-Regel nicht undecided ist (Listen "KONFIGURIERTE REGEL FAELLT
  DURCH" und, wenn entscheidend anders, "WIDERSPRUCH"), ODER mehr als ein
  Wechsel, ODER ein Konflikt. Monate unter undecided stehen als "offen" da,
  nie als Fehler. Exit 3: unvollstaendig (Download gescheitert, 429,
  --max-downloads erreicht, Symbol-Lock belegt, bei --offline nicht im
  Cache). 4 geht vor 3. Exit 0: sonst.

Fahrplan aendern: erst wenn der Scan vollstaendig ist (kein Exit 3) und
genau einen Wechsel ohne Konflikt zeigt, den Vorschlag in
config/histdata_timezone.yaml unter `schedule.tick` uebernehmen (ROH-Monate,
aufsteigend; der erste Eintrag deckt alles davor ab), Evidenztabelle und
Begruendung als Kommentar mitnehmen, `schedule.m1` unveraendert lassen.
Danach den Survey mit --offline erneut starten (soll Exit 0 liefern). Der
naechste Lauf von fetch_histdata.py erkennt betroffene Monatsdateien an
source_timezone und baut sie aus dem Roh-Cache neu.
