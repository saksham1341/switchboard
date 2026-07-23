from switchboard.app import build
from switchboard.sensors.github import GitHubSensor
from switchboard.sensors.discord import DiscordSensor


def _base(tmp_path):
    return {"mamamia_db_path": str(tmp_path / "mm.db"), "github_secret": "s"}


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
