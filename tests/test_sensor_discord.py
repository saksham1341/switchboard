import inspect, discord
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
