import io, json
from switchboard.taps.logger import LoggerTap
from switchboard.message import Observation, Command, TapCtx
from switchboard.store import MemoryStore


async def test_logs_observation_and_command_lines():
    buf = io.StringIO()
    tap = LoggerTap(stream=buf)
    tap.bind(TapCtx(store=MemoryStore()))
    assert tap.name == "logger" and tap.logs == ("obs", "cmd")
    await tap.observe("obs", Observation(id=5, name="github.home.pr.opened", payload={"n": 1}))
    await tap.observe("cmd", Command(id=3, name="discord.post", args={"c": "x"}, observation_id=5))
    lines = [json.loads(l) for l in buf.getvalue().splitlines()]
    assert lines[0] == {"log": "obs", "id": 5, "name": "github.home.pr.opened", "payload": {"n": 1}}
    assert lines[1]["log"] == "cmd" and lines[1]["name"] == "discord.post" and lines[1]["observation_id"] == 5


async def test_tap_ctx_store_is_scoped_and_usable():
    tap = LoggerTap(stream=io.StringIO())
    tap.bind(TapCtx(store=MemoryStore()))
    await tap.ctx.store.set("last-seen:github.home", "42")
    assert await tap.ctx.store.get("last-seen:github.home") == "42"


async def test_logger_includes_text_when_present():
    import io, json
    from switchboard.taps.logger import LoggerTap
    from switchboard.message import Observation

    class M:
        id = 1
    m = M(); m.payload = {"a": 1}; m.metadata = {"name": "x", "text": "PRETTY"}
    buf = io.StringIO()
    tap = LoggerTap(stream=buf); tap.bind(object())
    await tap.observe("obs", Observation.from_message(m))
    assert json.loads(buf.getvalue())["text"] == "PRETTY"


async def test_logger_omits_text_when_absent():
    import io, json
    from switchboard.taps.logger import LoggerTap
    from switchboard.message import Observation

    class M:
        id = 1
    m = M(); m.payload = {"a": 1}; m.metadata = {"name": "x"}
    buf = io.StringIO()
    tap = LoggerTap(stream=buf); tap.bind(object())
    await tap.observe("obs", Observation.from_message(m))
    assert "text" not in json.loads(buf.getvalue())
