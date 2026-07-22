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
    def subscribes(self, obs): return obs.name == "thing.happened"
    async def decide(self, obs, ctx):
        await ctx.command("do.it", {"echo": obs.payload["v"]})


class _Actuator:
    name = "do.it"
    def __init__(self): self.acted = []
    def context(self): return None
    async def act(self, cmd, ctx):
        self.acted.append(cmd.args["echo"])
        await ctx.result("ok", {"handled": cmd.args["echo"]})


class _Tap:
    name = "spy"
    logs = (OBS_LOG, CMD_LOG)
    def __init__(self): self.seen = []
    async def observe(self, log, view):
        self.seen.append((log, view.name))


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
