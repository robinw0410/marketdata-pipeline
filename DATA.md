# Datenbestand

## Spot-Klines (1m)

| Symbol  | Von        | Bis        | Bars      | Fehlend | Abdeckung | OHLC-Fehler |
|---------|------------|------------|-----------|---------|-----------|-------------|
| BTCUSDT | 2019-01-01 | 2026-08-19 | 4.010.631 | 4.089   | 99,898 %  | 0           |
| ETHUSDT | 2019-01-01 | 2026-08-19 | download in progress | download in progress | download in progress | 0 |

Quelle:   data.binance.vision, monthly klines, jede Datei SHA256-verifiziert
Erhoben:  2026-08-20
Format:   Parquet/zstd, hive-partitioniert nach symbol/year
Zeitzone: durchgehend UTC

## Live-Collector

Start:        2026-08-20 15:23 UTC
Symbole:      BTCUSDT, ETHUSDT (1m)
Verifiziert:  2026-08-21 gegen offizielles Tagesarchiv
              1440/1440 Bars, 0 fehlend, 0 Phantom, OHLCV identisch (Toleranz 0)
              Herkunft: 1440x ws, 0x rest_backfill

Dies ist der einzige garantiert out-of-sample Datensatz des Projekts.
Nicht loeschen, nicht ueberschreiben, nicht "aufraeumen".
