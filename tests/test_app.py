from switchboard.app import build, DISCORD_COMMANDS
from switchboard.ingress.github import GitHubIngress
from switchboard.ingress.discord import DiscordIngress, Command, Option


def _base(tmp_path):
    return {
        "mamamia_db_path": str(tmp_path / "e.db"),
        "switchboard_db_path": str(tmp_path / "s.db"),
        "github_secret": "s3cret",
        "max_log_messages": 10_000,
    }


def test_build_github_only(tmp_path):
    broker, ingresses = build(_base(tmp_path))
    assert "logger" in broker._egresses
    kinds = {type(i) for i in ingresses}
    assert GitHubIngress in kinds
    assert DiscordIngress not in kinds
    assert "discord" not in broker._egresses


def test_build_wires_discord_when_configured(tmp_path):
    cfg = _base(tmp_path) | {
        "discord_bot_token": "bot-tok",
        "discord_application_id": "app-123",
        "discord_guild_id": "456",
    }
    broker, ingresses = build(cfg)
    assert "discord" in broker._egresses
    assert any(isinstance(i, DiscordIngress) for i in ingresses)
    assert any(isinstance(i, GitHubIngress) for i in ingresses)


def test_configured_commands_include_parameterized_echo():
    names = {c.name for c in DISCORD_COMMANDS}
    assert {"ping", "echo"} <= names
    echo = next(c for c in DISCORD_COMMANDS if c.name == "echo")
    assert isinstance(echo, Command)
    assert [o.name for o in echo.options] == ["message"]
    assert echo.options[0].type is str and echo.options[0].required is True


def test_build_fails_fast_when_token_set_without_application_id(tmp_path):
    # token present but application_id omitted -> clear ValueError at build time,
    # not a cryptic KeyError and not a silent /webhooks/None/... at send time.
    import pytest

    cfg = _base(tmp_path) | {"discord_bot_token": "bot-tok"}
    with pytest.raises(ValueError, match="discord_application_id is required"):
        build(cfg)


async def test_teardown_is_best_effort():
    from switchboard.app import _teardown

    stopped = []

    class _Ing:
        def __init__(self, name, boom=False):
            self.name = name
            self._boom = boom
        async def stop(self):
            stopped.append(self.name)
            if self._boom:
                raise RuntimeError("stop failed")

    class _Broker:
        def __init__(self):
            self.stopped = False
        async def stop(self):
            self.stopped = True

    b = _Broker()
    await _teardown([_Ing("a", boom=True), _Ing("b")], b)
    assert stopped == ["a", "b"]      # a raising did not skip b
    assert b.stopped is True          # broker still stopped


def test_relay_handler_present_when_notify_channel_set(tmp_path):
    cfg = _base(tmp_path) | {
        "discord_bot_token": "bot-tok",
        "discord_application_id": "app-123",
        "discord_notify_channel_id": "chan-9",
    }
    broker, _ = build(cfg)
    discord = broker._egresses["discord"]
    assert "notify-github" in [h.name for h in discord.handlers]


def test_relay_handler_absent_without_notify_channel(tmp_path):
    cfg = _base(tmp_path) | {
        "discord_bot_token": "bot-tok",
        "discord_application_id": "app-123",
    }
    broker, _ = build(cfg)
    discord = broker._egresses["discord"]
    assert "notify-github" not in [h.name for h in discord.handlers]
