from switchboard.app import build
from switchboard.sensors.github import GitHubSensor
from switchboard.sensors.discord import DiscordSensor


def _base(tmp_path):
    return {"mamamia_db_path": str(tmp_path / "mm.db"),
            "switchboard_db_path": str(tmp_path / "sb.db"),
            "github_secret": "s"}


def test_build_github_only(tmp_path):
    bus, sensors = build(_base(tmp_path))
    assert any(isinstance(s, GitHubSensor) for s in sensors)
    assert not any(isinstance(s, DiscordSensor) for s in sensors)
    names = {a.name for a in bus._actuators}
    assert "discord.post" not in names and "discord.reply" not in names
    assert any(t.name == "logger" for t in bus._taps)


def test_build_wires_discord_and_relay(tmp_path):
    cfg = _base(tmp_path) | {"discord_bot_token": "bot", "discord_application_id": "app",
                             "discord_github_notify_channel_id": "chan-9"}
    bus, sensors = build(cfg)
    assert any(isinstance(s, DiscordSensor) for s in sensors)
    assert {"discord.post", "discord.reply"} <= {a.name for a in bus._actuators}
    assert {"ping", "echo", "github-notify"} <= {d.name for d in bus._deciders}


def test_max_log_messages_reaches_the_bus(tmp_path):
    # SB_MAX_LOG_MESSAGES is passed through compose; it must not be inert.
    bus, _ = build(_base(tmp_path) | {"max_log_messages": 250})
    assert bus._max_log_messages == 250
    assert build(_base(tmp_path))[0]._max_log_messages == 10_000   # default


def test_relay_decider_absent_without_notify_channel(tmp_path):
    cfg = _base(tmp_path) | {"discord_bot_token": "bot", "discord_application_id": "app"}
    bus, _ = build(cfg)
    dnames = {d.name for d in bus._deciders}
    assert "ping" in dnames and "github-notify" not in dnames
    assert "discord.reply" in {a.name for a in bus._actuators}
    assert "discord.post" not in {a.name for a in bus._actuators}


def test_build_fails_fast_without_application_id(tmp_path):
    import pytest
    cfg = _base(tmp_path) | {"discord_bot_token": "bot"}
    with pytest.raises(ValueError, match="discord_application_id is required"):
        build(cfg)


def test_discord_commands_shape():
    from switchboard.app import DISCORD_COMMANDS
    from switchboard.sensors.discord import CommandSpec
    names = {c.name for c in DISCORD_COMMANDS}
    assert {"ping", "echo"} <= names
    echo = next(c for c in DISCORD_COMMANDS if c.name == "echo")
    assert isinstance(echo, CommandSpec)
    assert [o.name for o in echo.options] == ["message"]
    assert echo.options[0].type is str and echo.options[0].required is True


def test_build_gives_the_bus_a_real_http_server_and_store(tmp_path):
    from switchboard.http import HttpServer
    from switchboard.store import SqliteStore
    bus, sensors = build({
        "mamamia_db_path": str(tmp_path / "mm.db"),
        "switchboard_db_path": str(tmp_path / "sb.db"),
        "github_secret": "s",
        "port": 8099,
    })
    assert isinstance(bus._http, HttpServer)
    assert isinstance(bus._store, SqliteStore)


def test_github_sensor_no_longer_takes_port_or_seen_db(tmp_path):
    import inspect
    from switchboard.sensors.github import GitHubSensor
    params = inspect.signature(GitHubSensor.__init__).parameters
    assert "port" not in params and "seen_db" not in params and "host" not in params


async def test_maintenance_timer_is_registered_and_started(tmp_path):
    from switchboard.bus import Bus
    from tests.test_bus import _wait
    calls = []
    bus = Bus(str(tmp_path / "mm.db"), wait_ms=50, reaper_interval=3600.0)
    bus.schedule_maintenance("store", 0.02, lambda: calls.append(1))
    await bus.start()
    try:
        await _wait(lambda: len(calls) >= 2)
    finally:
        await bus.stop()


def test_kv_actuator_is_always_wired(tmp_path):
    """Memory needs no key and no external call, so it ships with the platform
    rather than waiting for the decider that will use it."""
    bus, _ = build({
        "mamamia_db_path": str(tmp_path / "mm.db"),
        "switchboard_db_path": str(tmp_path / "sb.db"),
        "github_secret": "s", "port": 8131,
    })
    assert "kv" in bus.topology()["actuators"]


def test_wiring_discord_needs_no_flag_to_listen_for_messages(tmp_path):
    # Message listening used to be opt-in while the path was unproven. It is
    # now unconditional, so the only question the config answers is whether
    # Discord is wired at all.
    from switchboard.app import build
    bus, _ = build({
        "mamamia_db_path": str(tmp_path / "mm.db"),
        "switchboard_db_path": str(tmp_path / "sb.db"),
        "github_secret": "s", "port": 8131,
        "discord_bot_token": "t", "discord_application_id": "1",
    })
    sensor = next(s for s in bus._sensors if s.name == "discord")
    assert sensor._client.intents.message_content
    assert hasattr(sensor._client, "on_message")


def test_discord_history_actuator_is_wired_with_discord(tmp_path):
    from switchboard.app import build
    bus, _ = build({
        "mamamia_db_path": str(tmp_path / "mm.db"),
        "switchboard_db_path": str(tmp_path / "sb.db"),
        "github_secret": "s", "port": 8133,
        "discord_bot_token": "t", "discord_application_id": "1",
    })
    assert "discord.history" in {a.name for a in bus._actuators}


def test_no_discord_means_no_history_actuator(tmp_path):
    from switchboard.app import build
    bus, _ = build({
        "mamamia_db_path": str(tmp_path / "mm.db"),
        "switchboard_db_path": str(tmp_path / "sb.db"),
        "github_secret": "s", "port": 8134,
    })
    assert "discord.history" not in {a.name for a in bus._actuators}


def test_llm_actuator_is_not_wired(tmp_path):
    """Registering it would put an API key and real spend in the deployment for
    a feature nothing drives yet."""
    bus, _ = build({
        "mamamia_db_path": str(tmp_path / "mm.db"),
        "switchboard_db_path": str(tmp_path / "sb.db"),
        "github_secret": "s", "port": 8132,
    })
    assert "llm" not in bus.topology()["actuators"]


def test_no_anthropic_key_means_no_agent_and_no_llm(tmp_path):
    from switchboard.app import build
    bus, _ = build({
        "mamamia_db_path": str(tmp_path / "mm.db"),
        "switchboard_db_path": str(tmp_path / "sb.db"),
        "github_secret": "s", "port": 8141,
        "discord_bot_token": "t", "discord_application_id": "1",
    })
    assert "llm" not in {a.name for a in bus._actuators}
    assert "agent" not in {d.name for d in bus._deciders}


def test_the_agent_is_wired_when_the_key_and_discord_are_present(tmp_path):
    from switchboard.app import build
    bus, _ = build({
        "mamamia_db_path": str(tmp_path / "mm.db"),
        "switchboard_db_path": str(tmp_path / "sb.db"),
        "github_secret": "s", "port": 8142,
        "discord_bot_token": "t", "discord_application_id": "1",
        "anthropic_api_key": "sk-test",
    })
    assert "llm" in {a.name for a in bus._actuators}
    agent = next(d for d in bus._deciders if d.name == "agent")
    names = {t["name"] for t in agent._tools}
    assert names == {"discord.post", "discord.history"}


def test_the_agent_needs_discord_not_just_a_key(tmp_path):
    # Wiring the agent without its tools would give it a tool list naming
    # actuators nobody bound - spec 7.5's trusted-binding assumption broken
    # by our own wiring.
    from switchboard.app import build
    bus, _ = build({
        "mamamia_db_path": str(tmp_path / "mm.db"),
        "switchboard_db_path": str(tmp_path / "sb.db"),
        "github_secret": "s", "port": 8143,
        "anthropic_api_key": "sk-test",
    })
    assert "agent" not in {d.name for d in bus._deciders}


def test_discord_post_is_registered_once_even_with_notify_and_agent_both_set(tmp_path):
    # The trap: discord.post is wanted by both the github-notify branch and
    # the agent branch. Registering it twice would create two consumer groups
    # for one command name, and every discord.post command would be handled -
    # and posted - twice.
    from switchboard.app import build
    bus, _ = build({
        "mamamia_db_path": str(tmp_path / "mm.db"),
        "switchboard_db_path": str(tmp_path / "sb.db"),
        "github_secret": "s", "port": 8144,
        "discord_bot_token": "t", "discord_application_id": "1",
        "discord_github_notify_channel_id": "chan-9",
        "anthropic_api_key": "sk-test",
    })
    names = [a.name for a in bus._actuators]
    assert names.count("discord.post") == 1
    assert names.count("discord.history") == 1
    assert names.count("discord.reply") == 1


def test_agent_topology_lists_the_agent_decider(tmp_path):
    # The dashboard draws the real graph from topology(), so the agent must
    # be visible there once wired.
    from switchboard.app import build
    bus, _ = build({
        "mamamia_db_path": str(tmp_path / "mm.db"),
        "switchboard_db_path": str(tmp_path / "sb.db"),
        "github_secret": "s", "port": 8145,
        "discord_bot_token": "t", "discord_application_id": "1",
        "anthropic_api_key": "sk-test",
    })
    assert "agent" in bus.topology()["deciders"]
