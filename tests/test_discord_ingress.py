import inspect
from switchboard.ingress.discord import DiscordIngress


def test_ingress_registers_configured_commands_without_network():
    ing = DiscordIngress(
        "bot-tok",
        commands=[("ping", "Ping Switchboard"), ("status", "Show status")],
        guild_id="456",
    )
    assert ing.name == "discord"
    registered = {c.name for c in ing._tree.get_commands()}
    assert {"ping", "status"} <= registered
    assert inspect.iscoroutinefunction(ing.start)
    assert inspect.iscoroutinefunction(ing.stop)
