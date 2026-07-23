import inspect, discord
from starlette.testclient import TestClient
from switchboard.sensors.discord import DiscordSensor, CommandSpec, Option


def _sensor():
    return DiscordSensor("bot", commands=[
        CommandSpec("ping", "Ping"),
        CommandSpec("echo", "Echo", options=(Option("message", "text", type=str, required=True),)),
    ], guild_id="456")


def test_sensor_registers_typed_commands_without_network():
    s = _sensor()
    assert s.name == "discord"
    names = {c.name for c in s._tree.get_commands()}
    assert {"ping", "echo"} <= names
    echo = next(c for c in s._tree.get_commands() if c.name == "echo")
    params = {p.name: p for p in echo.parameters}
    assert params["message"].type is discord.AppCommandOptionType.string
    assert inspect.iscoroutinefunction(s.start) and inspect.iscoroutinefunction(s.stop)


class _FakeUser:
    id = 123
    def __str__(self): return "alice#0001"


class _FakeInteraction:
    token = "int-tok"
    channel_id = 7
    guild_id = 9
    user = _FakeUser()


def test_command_observation_shaping():
    from switchboard.sensors.discord import _command_observation
    name, payload = _command_observation("echo", _FakeInteraction(), {"message": "hi"})
    assert name == "discord.command.echo"
    assert payload == {
        "interaction_token": "int-tok", "channel_id": "7", "guild_id": "9",
        "user_id": "123", "user_name": "alice#0001", "options": {"message": "hi"},
    }


def test_bind_stores_ctx_and_declares_no_routes():
    from switchboard.http import HttpServer
    from switchboard.store import MemoryStore
    from switchboard.message import SensorCtx
    from switchboard.scheduler import Scheduler

    async def emit(name, payload): return 1

    http = HttpServer(serve=False)
    s = _sensor()
    s.bind(SensorCtx(emit=emit, http=http, store=MemoryStore(),
                     schedule=Scheduler().for_owner("discord")))
    assert TestClient(http.app).post("/webhook/discord").status_code == 404
