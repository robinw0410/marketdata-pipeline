# Datenluecken Spot-Klines 1m

Stand: Monatsarchive bis 2026-09, Zeitraum 2019-01-01 00:00 .. 2026-09-30 23:59 UTC.

| Symbol  | Luecken | Fehlende Minuten | Anteil   |
|---------|---------|------------------|----------|
| BTCUSDT | 22      | 4.089            | 0,1003 % |
| ETHUSDT | 22      | 4.090            | 0,1004 % |

Seit 2023-03-24 keine Luecken mehr. Der Holdout (2024-01-01 .. 2026-08-20)
ist damit lueckenlos.

Beide Symbole haben identische Luecken. Einzige Abweichung: 2020-12-21,
1 Minute (ETHUSDT ab 14:08 statt 14:09). Das weist auf boersenseitige
Ausfaelle hin, nicht auf Download-Fehler.

## Liste

Zeiten wie in der Ausgabe von stats.py: letzter Bar VOR der Luecke und
erster Bar DANACH. Die Luecke beginnt eine Minute nach der ersten Zeit.

| #  | Letzter Bar vor  | Erster Bar nach  | BTC | ETH | Einordnung |
|----|------------------|------------------|----:|----:|------------|
|  1 | 2019-03-12 01:59 | 2019-03-12 08:00 | 360 | 360 | Wartung 02:00 UTC |
|  2 | 2019-05-15 02:59 | 2019-05-15 13:00 | 600 | 600 | UNERKLAERT (volle Stunde, aber nicht 02:00) |
|  3 | 2019-06-07 21:13 | 2019-06-07 22:15 |  61 |  61 | UNERKLAERT |
|  4 | 2019-08-15 01:59 | 2019-08-15 10:00 | 480 | 480 | Wartung 02:00 UTC |
|  5 | 2019-11-13 01:59 | 2019-11-13 04:20 | 140 | 140 | Wartung 02:00 UTC |
|  6 | 2019-11-13 05:29 | 2019-11-13 05:33 |   3 |   3 | UNERKLAERT (selber Tag wie #5) |
|  7 | 2019-11-25 01:59 | 2019-11-25 04:00 | 120 | 120 | Wartung 02:00 UTC |
|  8 | 2020-02-09 01:59 | 2020-02-09 03:00 |  60 |  60 | Wartung 02:00 UTC |
|  9 | 2020-02-19 11:35 | 2020-02-19 17:30 | 354 | 354 | ungeplanter Ausfall |
| 10 | 2020-03-04 09:21 | 2020-03-04 11:30 | 128 | 128 | ungeplanter Ausfall, Ursache unbekannt |
| 11 | 2020-04-25 01:59 | 2020-04-25 04:30 | 150 | 150 | Wartung 02:00 UTC |
| 12 | 2020-06-28 01:59 | 2020-06-28 05:30 | 210 | 210 | Wartung 02:00 UTC |
| 13 | 2020-11-30 05:59 | 2020-11-30 07:00 |  60 |  60 | UNERKLAERT (volle Stunde, aber nicht 02:00) |
| 14 | 2020-12-21 14:09 | 2020-12-21 18:00 | 230 | 231 | ungeplanter Ausfall; ETH ab 14:08 |
| 15 | 2020-12-25 01:59 | 2020-12-25 03:00 |  60 |  60 | Wartung 02:00 UTC |
| 16 | 2021-02-11 03:40 | 2021-02-11 05:00 |  79 |  79 | ungeplanter Ausfall |
| 17 | 2021-03-06 01:59 | 2021-03-06 03:30 |  90 |  90 | Wartung 02:00 UTC |
| 18 | 2021-04-20 01:59 | 2021-04-20 04:30 | 150 | 150 | Wartung 02:00 UTC |
| 19 | 2021-04-25 04:00 | 2021-04-25 08:45 | 284 | 284 | ungeplanter Ausfall, bisherige Notiz: Flash-Crash |
| 20 | 2021-08-13 01:59 | 2021-08-13 06:30 | 270 | 270 | Wartung 02:00 UTC |
| 21 | 2021-09-29 06:59 | 2021-09-29 09:00 | 120 | 120 | UNERKLAERT (volle Stunde, aber nicht 02:00) |
| 22 | 2023-03-24 12:39 | 2023-03-24 14:00 |  80 |  80 | ungeplanter Ausfall |

## Interpretation

| Einordnung          | Luecken | Minuten BTC | Minuten ETH |
|---------------------|--------:|------------:|------------:|
| Wartung 02:00 UTC   | 11      | 2.090       | 2.090       |
| ungeplanter Ausfall | 6       | 1.155       | 1.156       |
| UNERKLAERT          | 5       | 844         | 844         |
| Summe               | 22      | 4.089       | 4.090       |

- **Wartung:** 11 Luecken beginnen direkt nach dem Bar 01:59 UTC, passend zur
  planmaessigen Binance-Wartung um 02:00 UTC. Eine fruehere Fassung dieser
  Datei zaehlte 14; nur 11 passen zu 02:00.
- **Ungeplante Ausfaelle:** die 6 Luecken mit krummen Startzeiten aus der
  bisherigen Notiz; die Zeiten stimmen mit der Liste ueberein. Bisherige
  Deutung: Die Boerse faellt bevorzugt bei hoher Volatilitaet aus, also genau
  dann, wenn man live nicht haette handeln koennen.
- **UNERKLAERT:** 3 Luecken beginnen zur vollen Stunde, aber nicht um 02:00
  (#2, #13, #21), dazu 2 mit krummen Startzeiten (#3, #6). Fuer keine davon
  liegt eine Erklaerung vor.

## Konsequenz fuer den Backtest

- Rolling-Fenster rechnen NIE ueber eine Session-Grenze
  (bt/data.py: add_sessions, warmup_ok)
- Positionen bestehen ueber Luecken FORT. Vorher rauszugehen waere
  Lookahead - du siehst den Ausfall nicht kommen.
- Betroffene Trades sind im Trade-Log als spans_gap markiert;
  metrics.summarize meldet gap_pnl_share.
