# Datenluecken BTCUSDT 1m

4.089 fehlende Minuten in 7,6 Jahren (0,102 %). Alle Luecken liegen vor 2024.

## Interpretation

14 der 22 Luecken beginnen um 01:59 UTC -> planmaessige Binance-Wartung (02:00 UTC).
Die uebrigen haben krumme Startzeiten und sind ungeplante Ausfaelle unter Last:

  2020-02-19 11:35 (354 min)
  2020-03-04 09:21 (128 min)   <- COVID-Crash
  2020-12-21 14:09 (230 min)
  2021-02-11 03:40  (79 min)
  2021-04-25 04:00 (284 min)   <- Flash-Crash
  2023-03-24 12:39  (80 min)

Muster: die Boerse faellt bevorzugt bei hoher Volatilitaet aus.
Genau dann haettest du live nicht handeln koennen.

## Konsequenz fuer den Backtest

- Rolling-Fenster rechnen NIE ueber eine Session-Grenze
  (bt/data.py: add_sessions, warmup_ok)
- Positionen bestehen ueber Luecken FORT. Vorher rauszugehen waere
  Lookahead - du siehst den Ausfall nicht kommen.
- Betroffene Trades sind im Trade-Log als spans_gap markiert;
  metrics.summarize meldet gap_pnl_share.

## Vollstaendige Liste

[Ausgabe von scripts/gaps.sql, Stand 2026-08-20]
