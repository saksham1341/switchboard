from switchboard.sensors.clock import ClockSensor


class _Ctx:
    def __init__(self):
        self.emitted = []
        self.scheduled = []
        self.schedule = self
        self.store = None
    def every(self, seconds, fn, **kw):
        self.scheduled.append((seconds, fn, kw))
    async def emit(self, name, payload, text=None):
        self.emitted.append((name, payload)); return len(self.emitted)


def test_declares_its_timer_at_bind():
    s = ClockSensor(interval=30.0)
    ctx = _Ctx(); s.bind(ctx)
    assert ctx.scheduled and ctx.scheduled[0][0] == 30.0


async def test_first_tick_has_no_delta():
    """There is no previous tick. Reporting 0.0 would be a number a consumer
    could act on; null says 'unknown' honestly."""
    s = ClockSensor(); ctx = _Ctx(); s.bind(ctx)
    await s.tick()
    name, payload = ctx.emitted[0]
    assert name == "clock.tick"
    assert payload["delta"] is None
    assert payload["seq"] == 1
    assert isinstance(payload["at"], float)


async def test_delta_is_measured_not_assumed():
    """A blocked loop or slow consumer makes the real gap larger than the
    configured interval. A consumer reasoning about elapsed time must see
    what actually happened."""
    s = ClockSensor(interval=60.0); ctx = _Ctx(); s.bind(ctx)
    times = iter([1000.0, 1000.5, 1090.0])
    s._now = lambda: next(times)
    await s.tick(); await s.tick(); await s.tick()
    deltas = [p["delta"] for _, p in ctx.emitted]
    assert deltas[0] is None
    assert deltas[1] == 0.5          # not 60.0
    assert deltas[2] == 89.5


async def test_seq_is_monotonic_so_a_missed_tick_is_visible():
    s = ClockSensor(); ctx = _Ctx(); s.bind(ctx)
    for _ in range(3):
        await s.tick()
    assert [p["seq"] for _, p in ctx.emitted] == [1, 2, 3]


async def test_a_failed_emit_does_not_break_the_sequence():
    """The scheduler survives a raising callback (test_scheduler pins that), so
    the next tick must still be coherent rather than repeating a seq."""
    s = ClockSensor(); ctx = _Ctx(); s.bind(ctx)
    async def boom(name, payload, text=None): raise RuntimeError("nope")
    ctx.emit = boom
    try:
        await s.tick()
    except RuntimeError:
        pass
    ctx.emit = _Ctx.emit.__get__(ctx)
    await s.tick()
    assert ctx.emitted[0][1]["seq"] == 2
