import asyncio
from switchboard.bus import Bus
from switchboard.message import OBS_LOG, CMD_LOG


async def _wait(pred, timeout=8.0):
    async def loop():
        while not pred():
            await asyncio.sleep(0.01)
    await asyncio.wait_for(loop(), timeout)


class _Decider:
    name = "trigger"
    def bind(self, ctx): self.ctx = ctx
    def subscribes(self, obs): return obs.name == "thing.happened"
    async def decide(self, obs, ctx):
        await ctx.command("do.it", {"echo": obs.payload["v"]})


class _Actuator:
    name = "do.it"
    def __init__(self): self.acted = []; self.closed = False
    def bind(self, ctx): self.ctx = ctx
    async def act(self, cmd, ctx):
        self.acted.append(cmd.args["echo"])
        await ctx.result("ok", {"handled": cmd.args["echo"]})
    async def close(self): self.closed = True


class _Tap:
    name = "spy"
    logs = (OBS_LOG, CMD_LOG)
    def __init__(self): self.seen = []
    def bind(self, ctx): self.ctx = ctx
    async def observe(self, log, view): self.seen.append((log, view.name))


class _Sensor:
    name = "probe"
    def __init__(self): self.bound = None; self.started = False; self.stopped = False
    def bind(self, ctx): self.bound = ctx
    async def start(self): self.started = True
    async def stop(self): self.stopped = True


async def test_full_spine_obs_to_cmd_to_result(tmp_path):
    act, tap = _Actuator(), _Tap()
    bus = Bus(str(tmp_path / "mm.db"), wait_ms=50, reaper_interval=3600.0)
    bus.add_decider(_Decider())
    bus.add_actuator(act)
    bus.add_tap(tap)
    await bus.start()
    try:
        await bus.emit_observation("thing.happened", {"v": 42})
        await _wait(lambda: act.acted == [42])
        # result observation flowed back onto the obs log and the tap saw the whole spine
        await _wait(lambda: ("obs", "do.it.ok") in tap.seen)
        names = {(log, n) for (log, n) in tap.seen}
        assert ("obs", "thing.happened") in names
        assert ("cmd", "do.it") in names
        assert ("obs", "do.it.ok") in names
    finally:
        await bus.stop()


async def test_actuator_only_consumes_its_command_name(tmp_path):
    act = _Actuator()  # name "do.it"
    bus = Bus(str(tmp_path / "mm.db"), wait_ms=50, reaper_interval=3600.0)
    bus.add_actuator(act)
    await bus.start()
    try:
        await bus.emit_command("something.else", {"x": 1}, observation_id=1)
        await asyncio.sleep(0.3)
        assert act.acted == []            # not its command → skipped, no effect
    finally:
        await bus.stop()


async def test_every_role_is_bound_with_its_own_scope(tmp_path):
    from switchboard.store import MemoryStore
    store = MemoryStore()
    sensor, dec, act, tap = _Sensor(), _Decider(), _Actuator(), _Tap()
    bus = Bus(str(tmp_path / "mm.db"), store=store, wait_ms=50, reaper_interval=3600.0)
    bus.add_sensor(sensor); bus.add_decider(dec); bus.add_actuator(act); bus.add_tap(tap)
    await bus.start()
    try:
        await sensor.bound.store.set("k", "sensor")
        await dec.ctx.store.set("k", "decider")
        assert await store.get("sensor/probe/k") == "sensor"
        assert await store.get("decider/trigger/k") == "decider"
        assert await sensor.bound.store.get("k") == "sensor"      # scope isolates
    finally:
        await bus.stop()


async def test_sensor_start_and_stop_are_called(tmp_path):
    sensor = _Sensor()
    bus = Bus(str(tmp_path / "mm.db"), wait_ms=50, reaper_interval=3600.0)
    bus.add_sensor(sensor)
    await bus.start()
    await _wait(lambda: sensor.started)
    await bus.stop()
    assert sensor.stopped


async def test_actuator_is_closed_on_stop(tmp_path):
    act = _Actuator()
    bus = Bus(str(tmp_path / "mm.db"), wait_ms=50, reaper_interval=3600.0)
    bus.add_actuator(act)
    await bus.start()
    await bus.stop()
    assert act.closed is True


async def test_a_sensor_that_raises_has_its_timers_stopped(tmp_path):
    import asyncio

    class _Exploding:
        name = "boom"
        def bind(self, ctx):
            self.ctx = ctx
            self.calls = []
            ctx.schedule.every(0.01, self._tick, first_after=0.0)
        async def _tick(self): self.calls.append(1)
        async def start(self): raise RuntimeError("dead on arrival")
        async def stop(self): pass

    s = _Exploding()
    bus = Bus(str(tmp_path / "mm.db"), wait_ms=50, reaper_interval=3600.0)
    bus.add_sensor(s)
    await bus.start()
    try:
        await asyncio.sleep(0.2)
        seen = len(s.calls)
        assert seen >= 1, "timer never ticked, so its stopping proves nothing"
        await asyncio.sleep(0.2)
        assert len(s.calls) == seen        # timers stopped with the sensor
    finally:
        await bus.stop()


async def test_a_sensor_that_returns_cleanly_keeps_its_timers(tmp_path):
    class _Quiet:
        name = "quiet"
        def bind(self, ctx):
            self.ctx = ctx
            self.calls = []
            ctx.schedule.every(0.02, self._tick, first_after=0.0)
        async def _tick(self): self.calls.append(1)
        async def start(self): return      # no loop to supervise, not "finished"
        async def stop(self): pass

    s = _Quiet()
    bus = Bus(str(tmp_path / "mm.db"), wait_ms=50, reaper_interval=3600.0)
    bus.add_sensor(s)
    await bus.start()
    try:
        await _wait(lambda: len(s.calls) >= 3)
    finally:
        await bus.stop()


async def test_routes_are_registered_before_the_server_starts(tmp_path):
    calls = []

    class _RecordingHttp:
        def __init__(self): self.app = None
        def route(self, path, handler, *, methods=("GET",), owner="?"):
            calls.append(("route", path))
        async def start(self): calls.append(("start", None))
        async def stop(self): calls.append(("stop", None))

    class _Routed:
        name = "routed"
        def bind(self, ctx):
            ctx.http.route("/x", None, methods=["POST"], owner=self.name)
        async def start(self): return
        async def stop(self): pass

    bus = Bus(str(tmp_path / "mm.db"), http=_RecordingHttp(),
              wait_ms=50, reaper_interval=3600.0)
    bus.add_sensor(_Routed())
    await bus.start()
    try:
        assert calls.index(("route", "/x")) < calls.index(("start", None))
    finally:
        await bus.stop()
