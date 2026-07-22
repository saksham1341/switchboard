import io, json
from switchboard.taps.logger import LoggerTap
from switchboard.message import Observation, Command


async def test_logs_observation_and_command_lines():
    buf = io.StringIO()
    tap = LoggerTap(stream=buf)
    assert tap.name == "logger" and tap.logs == ("obs", "cmd")
    await tap.observe("obs", Observation(id=5, name="github.home.pr.opened", payload={"n": 1}))
    await tap.observe("cmd", Command(id=3, name="discord.post", args={"c": "x"}, observation_id=5))
    lines = [json.loads(l) for l in buf.getvalue().splitlines()]
    assert lines[0] == {"log": "obs", "id": 5, "name": "github.home.pr.opened", "payload": {"n": 1}}
    assert lines[1]["log"] == "cmd" and lines[1]["name"] == "discord.post" and lines[1]["observation_id"] == 5
