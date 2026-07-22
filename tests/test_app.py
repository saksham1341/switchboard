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
                             "discord_notify_channel_id": "chan-9"}
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
