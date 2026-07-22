import inspect
import discord
from switchboard.ingress.discord import DiscordIngress, Command, Option


def _ingress():
    return DiscordIngress(
        "bot-tok",
        commands=[
            Command("ping", "Ping Switchboard"),
            Command("echo", "Echo a message back",
                    options=(Option("message", "Text to echo back", type=str, required=True),
                             Option("times", "How many times", type=int, required=False))),
        ],
        guild_id="456",
    )


def test_ingress_registers_configured_commands_without_network():
    ing = _ingress()
    assert ing.name == "discord"
    registered = {c.name for c in ing._tree.get_commands()}
    assert {"ping", "status"} & registered == {"ping"}   # sanity: ping present
    assert {"ping", "echo"} <= registered
    assert inspect.iscoroutinefunction(ing.start)
    assert inspect.iscoroutinefunction(ing.stop)


def test_ping_declares_no_options():
    ing = _ingress()
    ping = next(c for c in ing._tree.get_commands() if c.name == "ping")
    assert list(ping.parameters) == []


def test_echo_declares_typed_options_with_descriptions_and_requiredness():
    ing = _ingress()
    echo = next(c for c in ing._tree.get_commands() if c.name == "echo")
    params = {p.name: p for p in echo.parameters}
    assert set(params) == {"message", "times"}
    assert params["message"].type is discord.AppCommandOptionType.string
    assert params["message"].required is True
    assert params["message"].description == "Text to echo back"
    assert params["times"].type is discord.AppCommandOptionType.integer
    assert params["times"].required is False
