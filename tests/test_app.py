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
