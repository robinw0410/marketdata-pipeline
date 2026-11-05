"""
Gemeinsame Behandlung von HTTP 429 (Rate-Limit pro Client-IP) fuer alle
Download-Skripte (fetch_dukascopy.py, fetch_histdata.py).

Regeln:
- 429 ist KEIN Fehlversuch: kein Verbrauch eines Versuchs, keine Eskalation
  des Backoffs, kein Hochzaehlen des Schutzschalters (Throttle). Ein 429
  heisst nur "jetzt nicht" - wer weiter zaehlt, verbrennt nach wenigen
  Dateien alle Versuche und meldet falsche Luecken.
- Stattdessen sofort eine lange Pause fuer ALLE Worker und Prozesse:
  max(Retry-After, min_pause), min_pause default 15 min.
- Retry-After wird respektiert (Sekunden oder HTTP-Datum), auch wenn er
  laenger als min_pause ist.
- Die Sperre wird persistiert (state_path, flock): ein Neustart durch
  systemd oder ein paralleler Prozess (anderes Symbol, Cross-Check) fragt
  nicht vor Ablauf erneut an.
- Gesamtbudget je Lauf (max_total_wait, default 6 h): danach endet der Lauf
  sauber mit Luecken (Exit 3), systemd setzt spaeter fort. Ohne Budget
  koennte ein dauerhaft gesperrter Client den Lauf ewig blockieren.
"""
from __future__ import annotations

import email.utils
import fcntl
import json
import logging
import os
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger(__name__)

MIN_PAUSE_S = 900.0             # >= 15 min nach jedem 429
MAX_TOTAL_WAIT_S = 6 * 3600.0   # Budget je Lauf, danach Exit 3
CHUNK_S = 30.0                  # Schlafscheibe (Stop-Signal, fremde Sperren)


class RateLimitExhausted(Exception):
    """Wartebudget fuer 429-Sperren aufgebraucht -> Lauf mit Luecken beenden."""

    def __init__(self, msg: str, remaining_s: float):
        super().__init__(msg)
        self.remaining_s = remaining_s


def parse_retry_after(value, now: datetime | None = None) -> float | None:
    """Retry-After in Sekunden (Zahl oder HTTP-Datum); None bei fehlend/ungueltig."""
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    try:
        v = float(s)
        return v if v >= 0 else None
    except ValueError:
        pass
    try:
        dt = email.utils.parsedate_to_datetime(s)
    except (TypeError, ValueError, IndexError):
        return None
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    now = now or datetime.now(timezone.utc)
    return max(0.0, (dt - now).total_seconds())


class RateLimitGate:
    """
    Sperre nach 429, geteilt von allen Workern eines Prozesses und - ueber
    `state_path` - von allen Prozessen auf dem Host.

    wait()  vor JEDEM Request aufrufen; True = weiter, False = Stop-Signal.
            Wirft RateLimitExhausted, wenn das Budget nicht reicht.
    hit()   bei 429 aufrufen; setzt die Sperre, gibt die Pause zurueck.
    """

    def __init__(self, state_path: str | Path | None = None, *, min_pause: float = MIN_PAUSE_S,
                 max_total_wait: float = MAX_TOTAL_WAIT_S, wall=time.time, sleep=None,
                 stop: threading.Event | None = None, chunk: float = CHUNK_S):
        self.state_path = Path(state_path) if state_path else None
        self.min_pause = float(min_pause)
        self.max_total_wait = float(max_total_wait)
        self.wall, self._sleep, self.stop, self.chunk = wall, sleep, stop, float(chunk)
        self.lock = threading.Lock()
        self.blocked_until = float("-inf")
        self.hits = 0
        self.waited = 0.0
        self.last_retry_after: float | None = None
        self._load()

    # ------------------------------------------------------------ state ---
    @contextmanager
    def _flock(self):
        if self.state_path is None:
            yield None
            return
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(str(self.state_path) + ".lock", os.O_RDWR | os.O_CREAT, 0o644)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            yield fd
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    def _read_state(self) -> float:
        if self.state_path is None or not self.state_path.exists():
            return float("-inf")
        try:
            return float(json.loads(self.state_path.read_text())["blocked_until"])
        except (OSError, ValueError, KeyError, TypeError):
            return float("-inf")          # kaputter Zustand blockiert nicht ewig

    def _load(self) -> None:
        with self._flock():
            self.blocked_until = max(self.blocked_until, self._read_state())

    def _persist(self, retry_after: float | None, url: str) -> None:
        if self.state_path is None:
            return
        with self._flock():
            until = max(self.blocked_until, self._read_state())
            self.blocked_until = until
            tmp = self.state_path.with_name(f".{self.state_path.name}.{os.getpid()}.part")
            tmp.write_text(json.dumps({
                "blocked_until": until,
                "blocked_until_utc": datetime.fromtimestamp(until, timezone.utc).isoformat(),
                "last_hit_utc": datetime.fromtimestamp(self.wall(), timezone.utc).isoformat(),
                "retry_after_s": retry_after, "url": url, "pid": os.getpid()}, indent=1))
            os.replace(tmp, self.state_path)

    # -------------------------------------------------------------- api ---
    def remaining(self) -> float:
        with self.lock:
            return max(0.0, self.blocked_until - self.wall())

    def hit(self, retry_after: float | None = None, url: str = "") -> float:
        pause = max(self.min_pause, float(retry_after or 0.0))
        with self.lock:
            until = self.wall() + pause
            if until > self.blocked_until:
                self.blocked_until = until
            self.hits += 1
            self.last_retry_after = retry_after
            self._persist(retry_after, url)
        log.warning("HTTP 429 (Rate-Limit pro IP): Pause %.0f min fuer alle Requests%s (%s)",
                    pause / 60, f", Retry-After {retry_after:.0f}s" if retry_after else "", url)
        return pause

    def _sleep_chunk(self, s: float) -> bool:
        """Schlaeft s Sekunden; False wenn das Stop-Signal kam."""
        if self._sleep is not None:
            self._sleep(s)
            return not (self.stop is not None and self.stop.is_set())
        if self.stop is not None:
            return not self.stop.wait(s)
        time.sleep(s)
        return True

    def wait(self) -> bool:
        while True:
            if self.stop is not None and self.stop.is_set():
                return False
            with self.lock:
                if self.state_path is not None:      # Sperre eines anderen Prozesses?
                    self.blocked_until = max(self.blocked_until, self._read_state())
                rem = self.blocked_until - self.wall()
            if rem <= 0:
                return True
            if self.waited + rem > self.max_total_wait + 1e-9:
                raise RateLimitExhausted(
                    f"429-Sperre noch {rem / 60:.0f} min, Wartebudget {self.max_total_wait / 3600:.1f} h "
                    f"(bereits {self.waited / 60:.0f} min gewartet) reicht nicht", rem)
            step = min(rem, self.chunk)
            self.waited += step
            if not self._sleep_chunk(step):
                return False

    def summary(self) -> dict:
        return {"hits_429": self.hits, "waited_s": round(self.waited, 1),
                "blocked_until_utc": (datetime.fromtimestamp(self.blocked_until, timezone.utc).isoformat()
                                      if self.blocked_until > 0 else None),
                "min_pause_s": self.min_pause, "max_total_wait_s": self.max_total_wait}
