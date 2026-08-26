# Splits, eingefroren am 2026-08-20

Exploration:  2019-01-01 .. 2023-12-31   hier passiert alles
Holdout:      2024-01-01 .. heute        EINMAL, am Ende, fuer EINE Strategie
Forward:      ab 2026-08-21              Collector-Daten, der eigentliche Test

Holdout am Ende der Zeitreihe, nicht in der Mitte: Maerkte sind nicht
stationaer. Interessant ist, ob eine Regel im juengsten Regime haelt.

Jede Holdout-Nutzung wird in logs/holdout_uses.jsonl mit Zaehlnummer
protokolliert. Ab der zweiten ist der Test als unabhaengiger Beleg wertlos.
