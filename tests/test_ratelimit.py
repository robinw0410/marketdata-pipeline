import threading
from datetime import datetime, timezone

import pytest

import ratelimit as rl


class Clock:
    def __init__(self, t=1_700_000_000.0):
        self.t = t
        self.slept = []

    def __call__(self):
        return self.t

    def sleep(self, s):
        self.slept.append(s)
        self.t += s


def test_parse_retry_after_seconds_and_date():
    assert rl.parse_retry_after("120") == 120.0
    assert rl.parse_retry_after(None) is None
    assert rl.parse_retry_after("quatsch") is None
    assert rl.parse_retry_after("-5") is None
    now = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    assert rl.parse_retry_after("Thu, 01 Jan 2026 12:30:00 GMT", now) == pytest.approx(1800.0)
    assert rl.parse_retry_after("Thu, 01 Jan 2026 11:00:00 GMT", now) == 0.0


def test_hit_sets_at_least_min_pause_and_respects_longer_retry_after():
    c = Clock()
    g = rl.RateLimitGate(min_pause=900, wall=c, sleep=c.sleep)
    assert g.hit(None) == 900
    assert g.remaining() == pytest.approx(900)
    assert g.hit(30) == 900                    # kurzer Retry-After -> trotzdem 15 min
    assert g.hit(3600) == 3600                 # langer Retry-After wird respektiert
    assert g.remaining() == pytest.approx(3600)
    assert g.wait() is True
    assert sum(c.slept) == pytest.approx(3600)
    assert max(c.slept) <= rl.CHUNK_S
    assert g.hits == 3


def test_wait_without_block_does_not_sleep():
    c = Clock()
    g = rl.RateLimitGate(wall=c, sleep=c.sleep)
    assert g.wait() is True and c.slept == []


def test_budget_exhausted_raises():
    c = Clock()
    g = rl.RateLimitGate(min_pause=900, max_total_wait=1000, wall=c, sleep=c.sleep)
    g.hit()
    assert g.wait() is True                    # 900 s passen ins Budget
    g.hit()
    with pytest.raises(rl.RateLimitExhausted) as e:
        g.wait()
    assert e.value.remaining_s == pytest.approx(900)


def test_block_persists_across_processes(tmp_path):
    c = Clock()
    p = tmp_path / "state" / "dukascopy-ratelimit.json"
    g1 = rl.RateLimitGate(p, min_pause=900, wall=c, sleep=c.sleep)
    g1.hit(1200, url="u")
    g2 = rl.RateLimitGate(p, wall=c, sleep=c.sleep)     # "Neustart"
    assert g2.remaining() == pytest.approx(1200)
    # Sperre, die ein anderer Prozess WAEHREND des Wartens setzt, wird gesehen
    g3 = rl.RateLimitGate(p, min_pause=900, wall=c, sleep=c.sleep)
    c.t += 1300
    assert g3.remaining() == 0
    g1.hit(None)
    assert g3.wait() is True
    assert c.t >= g1.blocked_until


def test_corrupt_state_does_not_block(tmp_path):
    p = tmp_path / "s.json"
    p.write_text("{kaputt")
    c = Clock()
    g = rl.RateLimitGate(p, wall=c, sleep=c.sleep)
    assert g.remaining() == 0 and g.wait() is True


def test_stop_signal_interrupts_wait():
    c = Clock()
    stop = threading.Event()

    def sl(s):
        c.sleep(s)
        stop.set()

    g = rl.RateLimitGate(min_pause=900, wall=c, sleep=sl, stop=stop)
    g.hit()
    assert g.wait() is False
    assert sum(c.slept) <= rl.CHUNK_S


def test_nonfinite_retry_after_never_blocks_forever(tmp_path):
    assert rl.parse_retry_after("inf") is None and rl.parse_retry_after("nan") is None
    c = Clock()
    p = tmp_path / "s.json"
    g = rl.RateLimitGate(p, min_pause=900, wall=c, sleep=c.sleep)
    assert g.hit(float("inf")) == 900                       # wie fehlend
    assert g.hit(1e12) == rl.MAX_RETRY_AFTER_S              # Muell-Header gedeckelt
    assert g.summary()["blocked_until_utc"] is not None
    p.write_text('{"blocked_until": Infinity}')             # kaputter Zustand
    assert rl.RateLimitGate(p, wall=c, sleep=c.sleep).remaining() == 0


def test_budget_counts_wall_time_not_sum_over_workers():
    """4 gleichzeitig wartende Worker verbrauchen das Budget nicht 4x so schnell."""
    import time
    scale = 30_000.0                          # 900 s Sperre = 30 ms echte Zeit
    t0 = time.monotonic()
    g = rl.RateLimitGate(min_pause=900, max_total_wait=1200, chunk=30,
                         wall=lambda: (time.monotonic() - t0) * scale,
                         sleep=lambda s: time.sleep(s / scale))
    g.hit()
    barrier = threading.Barrier(4)
    results = []

    def waiter():
        barrier.wait()
        try:
            results.append(g.wait())
        except rl.RateLimitExhausted as e:
            results.append(e)

    ts = [threading.Thread(target=waiter) for _ in range(4)]
    for t in ts:
        t.start()
    for t in ts:
        t.join(10)
    # vorher: 4 x ~900 s = 3600 s > 1200 s Budget -> RateLimitExhausted
    assert results == [True] * 4
    assert g.waited <= 900 + 30 + 1e-6


def test_exhausted_is_sticky():
    c = Clock()
    g = rl.RateLimitGate(min_pause=900, max_total_wait=0, wall=c, sleep=c.sleep)
    g.hit()
    with pytest.raises(rl.RateLimitExhausted):
        g.wait()
    c.t += 10_000                             # Sperre laengst abgelaufen
    with pytest.raises(rl.RateLimitExhausted):
        g.wait()                              # trotzdem: keine weiteren Requests im Lauf
    assert g.summary()["exhausted"]
