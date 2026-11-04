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

Symbole:  XAUUSD, XAGUSD, EURUSD (1m, Bid- und Ask-OHLC aus Ticks)
Stand:    noch nicht geladen (scripts/fetch_dukascopy.py)
Quelle:   datafeed.dukascopy.com, stuendliche .bi5-Tickdateien
Format:   Parquet/zstd, hive-partitioniert nach source/symbol/year
Zeitzone: durchgehend UTC

Store:
    data/metals/source=dukascopy/symbol={SYM}/year={YYYY}/{SYM}-1m-{YYYY}-{MM}.parquet
    data/metals/reports/{SYM}-1m-{YYYY}-{MM}.json       Validierungsreport je Monat
    data/raw/dukascopy/{SYM}/{YYYY}/{MM}/{DD}/{HH}h_ticks.bi5   Roh-Cache
        (Monat hier 1-basiert; in der Dukascopy-URL ist er 0-basiert,
        Januar = 00. 404 als Marker {HH}h_ticks.404 gecached.)
    Wurzel per MARKETDATA_DATA_ROOT ueberschreibbar (nur fetch_dukascopy.py).

Spalten: ts (UTC, ms, Label links [t, t+1min)), bid_open/high/low/close,
ask_open/high/low/close (float64), tick_count, bid_volume, ask_volume
(Summen, Rohwerte von Dukascopy), source = "dukascopy". Nur Minuten mit
Ticks, nichts aufgefuellt.

bi5-Format: LZMA, Records a 20 Byte big-endian '>3I2f' (ms seit
Stundenbeginn, ask, bid, ask_volume, bid_volume). 0 Bytes = keine Ticks.

Annahmen (zu verifizieren):
- Preis = int / POINT_DIVISOR, XAUUSD 1000, XAGUSD 1000, EURUSD 100000.
  Abgesichert durch Plausibilitaetsbereich (XAUUSD 200..20000,
  XAGUSD 3..500, EURUSD 0.5..2.0); ausserhalb bricht der Lauf hart ab.
- Volumen-Einheit nicht verifiziert; Werte werden unveraendert summiert.

Validierung: hart (Abbruch, nichts geschrieben) bei Duplikaten, nicht
monotoner Zeit, inkonsistentem Bid-/Ask-OHLC, Preisen <= 0, NaN,
Plausibilitaetsbereich, > 0,1 % Bars mit ask_close < bid_close. Weich (nur
Report): Spread > 20x Monatsmedian, |Mid-Rendite 1m| > 2 %, Minuten ohne
Ticks Mo-Fr gegen ein grobes Raster. Keine harte Lueckengrenze: Metalle/FX
haben Tagespausen und Wochenenden; geplant/ungeplant entscheidet der
Handelskalender des Bots.

Lizenz: Dukascopy-Nutzungsbedingungen selbst pruefen. Daten (roh und
abgeleitet) nicht weitergeben und nicht committen.
