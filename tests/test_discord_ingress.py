import inspect
import discord
import pytest
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


async def test_sync_commands_retries_after_a_failed_sync():
    # A first sync that fails (e.g. the app not yet authorized in the guild ->
    # 403) must NOT latch `_synced`; the next reconnect's on_ready should retry.
    ing = DiscordIngress("bot-tok", commands=[Command("ping", "Ping")], guild_id="456")
    calls = {"sync": 0}

    def fake_copy(*, guild):
        pass

    async def fake_sync_fail(*, guild):
        calls["sync"] += 1
        raise RuntimeError("403 Missing Access")

    async def fake_sync_ok(*, guild):
        calls["sync"] += 1

    ing._tree.copy_global_to = fake_copy

    ing._tree.sync = fake_sync_fail
    with pytest.raises(RuntimeError):
        await ing._sync_commands()
    assert ing._synced is False                  # not latched off after failure

    ing._tree.sync = fake_sync_ok
    await ing._sync_commands()
    assert ing._synced is True                   # retry succeeds

    await ing._sync_commands()                   # already synced -> no-op
    assert calls["sync"] == 2                    # fail + success only; not called again
