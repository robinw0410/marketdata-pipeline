# Splits, eingefroren am 2026-08-20

Exploration:  2019-01-01 .. 2023-12-31   hier passiert alles
Holdout:      2024-01-01 .. 2026-08-20   EINMAL, am Ende, fuer EINE Strategie
Forward:      ab 2026-08-21              Collector-Daten, der eigentliche Test

Alle Grenzen inklusive, UTC. Massgeblich sind EXPLORE und HOLDOUT in
scripts/bt/data.py. Holdout und Forward ueberlappen nicht.

2026-10-25: Holdout-Ende von "heute" (im Code 2026-12-31) auf 2026-08-20
festgelegt, damit der Holdout nicht in den Forward-Zeitraum hineinlaeuft.

Holdout am Ende der Zeitreihe, nicht in der Mitte: Maerkte sind nicht
stationaer. Interessant ist, ob eine Regel im juengsten Regime haelt.

Jede Holdout-Nutzung wird in logs/holdout_uses.jsonl mit Zaehlnummer
protokolliert. Ab der zweiten ist der Test als unabhaengiger Beleg wertlos.
