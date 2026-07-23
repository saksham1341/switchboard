import asyncio

from switchboard.scheduler import Scheduler


async def _wait(pred, timeout=3.0):
    async def loop():
        while not pred():
            await asyncio.sleep(0.005)
    await asyncio.wait_for(loop(), timeout)


async def test_timer_does_not_fire_before_start():
    s = Scheduler()
    calls = []
    s.for_owner("a").every(0.01, lambda: _noop(calls), first_after=0.0)
    await asyncio.sleep(0.1)
    assert calls == []


async def _noop(calls):
    calls.append(1)


async def test_timer_fires_after_start_and_repeats():
    s = Scheduler()
    calls = []
    s.for_owner("a").every(0.02, lambda: _noop(calls), first_after=0.0)
    s.start("a")
    try:
        await _wait(lambda: len(calls) >= 3)
    finally:
        await s.stop("a")


async def test_timer_stops_after_stop():
    s = Scheduler()
    calls = []
    s.for_owner("a").every(0.01, lambda: _noop(calls), first_after=0.0)
    s.start("a")
    await _wait(lambda: len(calls) >= 1)
    await s.stop("a")
    seen = len(calls)
    await asyncio.sleep(0.1)
    assert len(calls) == seen          # no further ticks


async def test_first_after_defaults_to_the_interval():
    s = Scheduler()
    calls = []
    s.for_owner("a").every(1.0, lambda: _noop(calls))     # no first_after
    s.start("a")
    try:
        await asyncio.sleep(0.15)
        assert calls == []             # nothing fires at t=0
    finally:
        await s.stop("a")


async def test_raising_callback_does_not_kill_the_loop():
    s = Scheduler()
    calls = []

    async def boom():
        calls.append(1)
        raise RuntimeError("nope")

    s.for_owner("a").every(0.02, boom, first_after=0.0)
    s.start("a")
    try:
        await _wait(lambda: len(calls) >= 3)
    finally:
        await s.stop("a")


async def test_slow_callback_does_not_overlap_itself():
    s = Scheduler()
    concurrent, peak = [], []

    async def slow():
        concurrent.append(1)
        peak.append(len(concurrent))
        await asyncio.sleep(0.05)
        concurrent.pop()

    s.for_owner("a").every(0.01, slow, first_after=0.0)
    s.start("a")
    try:
        await _wait(lambda: len(peak) >= 3)
        assert max(peak) == 1
    finally:
        await s.stop("a")


async def test_declaring_while_running_launches_immediately():
    s = Scheduler()
    calls = []
    s.start("a")                        # started with nothing declared
    try:
        s.for_owner("a").every(0.02, lambda: _noop(calls), first_after=0.0)
        await _wait(lambda: len(calls) >= 2)
    finally:
        await s.stop("a")


async def test_owners_are_independent():
    s = Scheduler()
    a_calls, b_calls = [], []
    s.for_owner("a").every(0.02, lambda: _noop(a_calls), first_after=0.0)
    s.for_owner("b").every(0.02, lambda: _noop(b_calls), first_after=0.0)
    s.start("a")
    s.start("b")
    await _wait(lambda: a_calls and b_calls)
    await s.stop("a")
    seen_a = len(a_calls)
    await _wait(lambda: len(b_calls) > seen_a + 1)
    assert len(a_calls) == seen_a       # a stayed stopped while b kept ticking
    await s.stop("b")


async def test_start_is_idempotent():
    s = Scheduler()
    calls = []
    s.for_owner("a").every(0.02, lambda: _noop(calls), first_after=0.0)
    s.start("a")
    s.start("a")                      # must not launch a second loop
    try:
        await _wait(lambda: len(calls) >= 4)
        # One loop at 0.02s cannot have produced more than ~2x the elapsed ticks;
        # two loops would roughly double the count. Assert the task count directly.
        assert len(s._tasks["a"]) == 1
    finally:
        await s.stop("a")


async def test_stop_clears_declarations_so_a_restart_does_not_double_up():
    s = Scheduler()
    calls = []
    own = s.for_owner("a")
    own.every(0.02, lambda: _noop(calls), first_after=0.0)
    s.start("a")
    await _wait(lambda: calls)
    await s.stop("a")

    # A role re-declares in bind() on the next startup.
    own = s.for_owner("a")
    own.every(0.02, lambda: _noop(calls), first_after=0.0)
    s.start("a")
    try:
        assert len(s._tasks["a"]) == 1        # not 2
    finally:
        await s.stop("a")


async def test_stopped_owner_with_no_redeclaration_launches_nothing():
    s = Scheduler()
    calls = []
    s.for_owner("a").every(0.01, lambda: _noop(calls), first_after=0.0)
    s.start("a")
    await _wait(lambda: calls)
    await s.stop("a")
    seen = len(calls)
    s.start("a")                      # nothing declared since the stop
    try:
        await asyncio.sleep(0.1)
        assert len(calls) == seen
    finally:
        await s.stop("a")
