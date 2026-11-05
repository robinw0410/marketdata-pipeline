# Dukascopy Datafeed: 1-Minuten-Kerzen (BID/ASK) - Recherche

Stand 2026-11-05. Dukascopy war nicht erreichbar (Proxy 403): **nichts wurde live verifiziert**, alles stammt aus Quellcode-Lektüre (nichts ausgeführt) und einer Websuche.

## Quellen

| Kürzel | Paket / Version | Rolle |
|---|---|---|
| NODE-OLD | npm `dukascopy-node` 1.46.4, `dist/esm/chunk-V645LDJD.js` | nutzt `datafeed.dukascopy.com` mit Kerzen-`.bi5`; **Hauptbeleg** |
| NODE-NEW | npm `dukascopy-node` 1.51.1, `dist/esm/chunk-C4M4JTYJ.js` | nutzt nicht mehr den Datafeed, sondern `https://<sub>.dukascopy.com/v1/...` (JSON, mit WAF-Behandlung) |
| DD | PyPI `dukascopy-downloader` 1.1.0 | ebenfalls neue JSON-API (`jetta.dukascopy.com`), kein bi5 |
| DP | PyPI `dukascopy-python` 5.0.0 | v2 = neue JSON-API (`/v1/candles/...`) |
| TT, DUKA | PyPI `tickterial` 1.1.2, `duka` 0.2.0 | nur Ticks (`{HH}h_ticks.bi5`), keine Kerzen-Dateien |
| SO | Websuche (StackOverflow-Antwort, per Suchtreffer zitiert) | Gegenstimme zur Feldreihenfolge, siehe Unsicherheiten |

Wichtig: Eine **zweite unabhängige Implementierung des Kerzen-bi5-Formats** habe ich auf npm/PyPI nicht gefunden. Alle Kerzen-Pakete außer NODE-OLD sind auf die neue JSON-API umgestiegen. Die Aussagen zum bi5-Kerzenformat stützen sich also auf NODE-OLD (viele Jahre produktiv genutzt) plus eine Websuche-Quelle (SO). Der Umstieg von NODE-NEW/DD/DP auf die JSON-API deutet darauf hin, dass der alte Datafeed für Skripte unzuverlässig oder per WAF geschützt sein könnte. Das ist eine Vermutung.

## 1. URL-Muster (NODE-OLD, `getUrl`, ca. Zeile 16446)

```
URL_ROOT = https://datafeed.dukascopy.com/datafeed
Jahr  : {ROOT}/{INSTR}/{YYYY}/{SIDE}_candles_day_1.bi5
Monat : {ROOT}/{INSTR}/{YYYY}/{MM}/{SIDE}_candles_hour_1.bi5
Tag   : {ROOT}/{INSTR}/{YYYY}/{MM}/{DD}/{SIDE}_candles_min_1.bi5
Stunde: {ROOT}/{INSTR}/{YYYY}/{MM}/{DD}/{HH}h_ticks.bi5
```
`INSTR` = `instrument.toUpperCase()` (z. B. `XAUUSD`), `SIDE` = `BID` oder `ASK` (`priceType.toUpperCase()`).
Datumsteile kommen aus `getUTCFullYear/getUTCMonth/getUTCDate/getUTCHours`, zweistellig gepolstert.

- **Monat ist 0-basiert**: `pad(getUTCMonth())` ohne +1. Januar = `00`, Dezember = `11`. Tickterial bestätigt das für Ticks (`'month': timestamp.month - 1`).
- Tag und Stunde sind 0-gepolstert (Tag 1-basiert, Stunde 0-23).
- Gegenprobe: In NODE-NEW (neue API) ist der Monat **1-basiert** (`zeroBasedMonth + 1`) und das Muster ein anderes (`/v1/candles/minute/{CODE}/{SIDE}/{Y}/{M}/{D}`, `CODE` z. B. `XAU-USD`). Beide Welten nicht vermischen.
- Beispiel: XAUUSD, BID, 2024-03-05 -> `.../datafeed/XAUUSD/2024/02/05/BID_candles_min_1.bi5`.
- Die SO-Quelle nennt `.../datafeed/EURUSD/2022/11/06/BID_candles_min_1.bi5` (Monat 11 = Dezember, 0-basiert: konsistent).

## 2. Binärformat (NODE-OLD, `decompress`/`unpackBuffer`/`normalise`)

- Kompression: LZMA (`lzma-purejs-requirejs.decompressFile`; Python: `lzma.LZMADecompressor(FORMAT_AUTO)`, wie bei Ticks in TT).
- Record: **24 Byte** für Kerzen (Ticks: 20 Byte), **Big-Endian**.
- Struktur Kerze: `>5i f` also `readInt32BE` an Offset 0, 4, 8, 12, 16 und `readFloatBE` an Offset 20.
- Feldreihenfolge laut NODE-OLD (`const [sec, open, close, low, high, volume] = values`):

| Offset | Typ | Feld |
|---|---|---|
| 0 | int32 BE | Sekunden seit Beginn der Datei-Periode (Tag/Monat/Jahr, UTC) |
| 4 | int32 BE | open |
| 8 | int32 BE | **close** |
| 12 | int32 BE | **low** |
| 16 | int32 BE | **high** |
| 20 | float32 BE | volume |

- Zeitbasis: `ts_ms = sec*1000 + startTs`, `startTs` = Beginn der Periode aus der URL (Tag: 00:00 UTC; Monat: 1. des Monats; Jahr: 1. Jan.). Minutenkerze: sec = 0, 60, 120, ...
- Preisdivisor: `price = int / decimalFactor` aus `instrumentMetaData[instrument].decimalFactor`, **identisch für Ticks und Kerzen**. Tabelle in NODE-OLD:
  - EURUSD `1e5`, USDJPY `1e3`, **XAUUSD `1e3`**, **XAGUSD `1e3`**. (Insgesamt 224 Instrumente mit `1e5`; weitere haben `1e3`, `1e2` etc.)
  - NODE-NEW kennt kein `decimalFactor` mehr (JSON hat `multiplier`, z. B. `1e-5`).
  - Die SO-Quelle erwähnt widersprüchlich 1000 vs. 100000 je nach Instrument: also Instrument-spezifisch, nicht global.
- Volumen: float32; Einheit laut Paket "millions" = Rohwert (`formatVolume` gibt bei `millions` den Wert unverändert zurück; `thousands` multipliziert mit 1e3, sonst 1e6). Bei Ticks ist das Volumen die gleiche Einheit (float32, "Mio. Basiswährung"); bei Kerzen vermutlich Summe der Tickvolumina. **Nicht verifiziert.**
- Tick-Referenz (NODE-OLD): `[ms, ask, bid, askVol, bidVol]` (3 int32 + 2 float32). Merke: **ask vor bid** bei Ticks; Kerzen haben getrennte BID-/ASK-Dateien.

## 3. Füllkerzen

- NODE-OLD: `ignoreFlats` Default **true**. Bei m1 wird gefiltert `data.filter(item => item[5] !== 0)` (nur `volume == 0` entfernt, nicht Preisgleichheit). Beim Aggregieren zu höheren Zeitrahmen ebenfalls `data[5] !== 0`.
- Die Existenz des Filters und der Kommentar in DD ("Dukascopy fills inactive minutes with flat candles", Option `--drop-flat`: "drop zero-volume candles") sprechen dafür, dass die Dateien **Füllkerzen mit volume 0** enthalten. Das gilt für die neue API (DD); für die alte bi5-Datei ist es indirekt (Filter in NODE-OLD), nicht direkt belegt.
- Empfehlung: Kerzen mit `volume == 0` als "kein Handel" markieren/verwerfen; nicht als echte Preise behandeln. Wochenende kann entweder fehlen (leere Datei / 404 -> NODE-OLD gibt bei Status != 200 einen leeren Buffer zurück) oder aus Füllkerzen bestehen: offen.

## 4. Laufender Tag

- Der Datafeed liefert nur abgeschlossene Perioden. NODE-OLD (`getConstructor`): Ist das letzte Element der aktuelle Zeitraum (`isCurrentRange`), wird **auf die nächstkleinere Einheit** zurückgefallen (Jahr -> Monat -> Tag -> Stunde-Ticks). Für m1 heißt das: heutiger Tag = 24 `{HH}h_ticks.bi5`, daraus werden Minutenkerzen selbst aggregiert (`getMinuteOHLCfromTicks`).
- `getDateLimit` begrenzt das Ende auf "jetzt" (m1: auf Stundenbeginn, d. h. die laufende Stunde bleibt aus).
- Ob die Minutendatei des heutigen Tages tatsächlich nicht existiert (404/leer), ist **nicht verifiziert**; die Pakete gehen davon aus. Für die neue API belegt DD: "A chunk is served only after its period has fully ended (otherwise 400 'From time is too late')".

## 5. BID vs. ASK, Spread

- Zwei getrennte Dateien (`BID_candles_...`, `ASK_candles_...`), identisches Format. Pakete laden pro Lauf nur eine Seite (`priceType`).
- Spread aus Kerzen: Schlusskurs ask_close - bid_close (gleicher Zeitstempel) ist rekonstruierbar, aber nur eine **Momentaufnahme am Minutenende** (Close = letzter Tick der Minute). Mittelwert/Max-Spread der Minute ist daraus nicht ableitbar (Ticks nötig). In Minuten ohne Ticks sind beide Seiten flach (Füllkerzen), der Spread ist dann der letzte bekannte, nicht aktuell.
- Hinweis: BID-/ASK-Kerzen werden getrennt aus den jeweiligen Seiten des Tickstroms gebildet. Der Zeitpunkt der letzten Tick-Aktualisierung kann je Seite verschieden sein.

## 6. Rate-Limits / Retries (Defaults)

- NODE-OLD: `batchSize 10`, `pauseBetweenBatchesMs 1000`, `retryCount 0`, `pauseBetweenRetriesMs 500`, `retryOnEmpty false`, `failAfterRetryCount true`. Nicht-200 wird **still als leerer Buffer** behandelt (kein Fehler!), es sei denn `retryCount > 0`. Fehlende Daten sind also nicht von Fehlern unterscheidbar.
- NODE-NEW: `requestIntervalMs 0`, `rateLimitRetryCount 3`, max. Wartezeit 5 min, respektiert `Retry-After`, bricht bei 429/403 (WAF) mit Fehler ab; Hinweis auf "bulk exports" über eine offizielle Wiki-Seite.
- DD: Token-Bucket `rate_limit 8 req/s`, halbiert bei 429/403 (min. 0,5), `max_retries 8`, Retries bei 403/408/425/429/5xx, Backoff max. 120 s.
- DP (`create_session`): `retries 5`, `backoff_factor 0.3`, Statusliste 429/500/502/503/504, `Retry-After` respektiert.
- TT: Timeout 13 s, `allow_redirects=False`. DUKA: Retry mit `sleep(0.5*i)`.
- Empfehlung: <= 2-5 Requests parallel, Pause >= 0,5-1 s zwischen Batches, Retries bei 429/5xx mit exponentiellem Backoff, 404/leer separat protokollieren.

## 7. Spezifikation (kompakt)

| Punkt | Wert | Sicherheit |
|---|---|---|
| Tagesdatei Minuten | `/datafeed/{SYM}/{YYYY}/{MM0}/{DD}/{BID\|ASK}_candles_min_1.bi5` | hoch (NODE-OLD, SO) |
| Monat 0-basiert | ja (Jan = 00) | hoch |
| Stundenkerzen | `/datafeed/{SYM}/{YYYY}/{MM0}/{BID\|ASK}_candles_hour_1.bi5` (pro Monat) | mittel (nur NODE-OLD) |
| Tageskerzen | `/datafeed/{SYM}/{YYYY}/{BID\|ASK}_candles_day_1.bi5` (pro Jahr) | mittel |
| Kompression | LZMA (.lzma/alone-Header) | hoch |
| Record | 24 B, Big-Endian: `>iiiiif` | hoch |
| Feldfolge | sec, open, **close, low, high**, volume (NODE-OLD) | **mittel, Widerspruch mit SO (O,H,L,C)** |
| Zeit | Sekunden seit Dateiperiodenbeginn UTC | hoch |
| Divisor | Instrument-spezifisch, wie Ticks; XAUUSD 1e3, XAGUSD 1e3, EURUSD 1e5 | mittel |
| Volumen | float32, Einheit wie Ticks ("millions"-Rohwert) | niedrig |
| Füllkerzen | wahrscheinlich ja, volume 0; Pakete filtern `volume==0` | mittel |
| Aktueller Tag | nicht verfügbar, Fallback auf Stunden-Ticks | mittel |

## Unsicherheiten

1. **Feldreihenfolge**: NODE-OLD sagt `open, close, low, high`; die SO-Antwort nennt `O,H,L,C`. Unabhängig von der Quelle durch Plausibilitätsprüfung klären (siehe unten): in einer gültigen Kerze gilt `high >= max(open, close)` und `low <= min(open, close)`; dadurch ist die Reihenfolge eindeutig bestimmbar.
2. Keine zweite unabhängige bi5-Kerzen-Implementierung gefunden; NODE-OLD ist faktisch die einzige Quelle.
3. Ob der Datafeed weiterhin ohne WAF/403 für Skripte erreichbar ist (alle neuen Pakete nutzen eine andere API).
4. Volumeneinheit der Kerzen; Divisor-Tabelle für weitere Instrumente.
5. Verhalten an Wochenenden und der Tagespause (Datei fehlt vs. Füllkerzen); Verfügbarkeit des laufenden Tages.
6. Zeitzonen-/DST-Grenzen: NODE-OLD rechnet strikt UTC; die Dateiperiode beginnt 00:00 UTC.

## Prüfbefehl-Vorschlag für den Betreiber

Einen abgeschlossenen Handelstag (z. B. Mittwoch 2024-03-06, Monat 0-basiert = `02`) laden und Kerzen gegen aus Ticks gebildete Kerzen vergleichen:

```bash
D=XAUUSD; B=https://datafeed.dukascopy.com/datafeed/$D/2024/02/06
mkdir -p /tmp/duka && cd /tmp/duka
curl -sS -A "Mozilla/5.0" -o bid_min.bi5 $B/BID_candles_min_1.bi5
curl -sS -A "Mozilla/5.0" -o ask_min.bi5 $B/ASK_candles_min_1.bi5
for h in $(seq -w 0 23); do curl -sS -A "Mozilla/5.0" -o t$h.bi5 $B/${h}h_ticks.bi5; sleep 1; done
python3 - <<'PY'
import lzma, struct, glob
def dec(p):
    b = open(p,'rb').read()
    return lzma.LZMADecompressor(lzma.FORMAT_AUTO).decompress(b) if b else b''
for name in ('bid_min','ask_min'):
    d = dec(name+'.bi5'); n = len(d)//24
    print(name, 'bytes', len(d), 'records', n, 'rest', len(d)%24)
    rows = [struct.unpack('>5if', d[i*24:i*24+24]) for i in range(n)]
    for r in rows[:5] + rows[-3:]: print(r)
    # Feldreihenfolge pruefen: welche Spalte ist stets das Maximum / Minimum?
    cols = list(zip(*rows))
    mx = [sum(1 for r in rows if r[k]==max(r[1:5])) for k in range(1,5)]
    mn = [sum(1 for r in rows if r[k]==min(r[1:5])) for k in range(1,5)]
    print('Anteil Spalte==Max (idx1..4):', mx, ' ==Min:', mn, ' von', n)
    print('volume==0:', sum(1 for r in rows if r[5]==0), 'sec-Bereich', min(cols[0]), max(cols[0]))
PY
```
Auswertung: Die Spalte, die fast immer das Maximum ist, ist `high` (NODE-OLD: Index 4; SO: Index 2). Rohwerte durch 1000 (XAUUSD) teilen und mit einem Chart vergleichen (Gold 2024-03 ca. 2150-2180). Danach Tick-Dateien (`>3i2f`: ms, ask, bid, askVol, bidVol) zu Minuten-OHLC aggregieren und open/high/low/close sowie Spread-Close (ask_min.close - bid_min.close) vergleichen. Zusätzlich HTTP-Status und Antwortlänge für Wochenende (z. B. 2024-03-09) und den heutigen Tag prüfen.
