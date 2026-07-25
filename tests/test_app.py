import pytest

from switchboard.app import build
from switchboard.sensors.github import GitHubSensor
from switchboard.sensors.discord import DiscordSensor


def _base(tmp_path):
    return {"mamamia_db_path": str(tmp_path / "mm.db"),
            "switchboard_db_path": str(tmp_path / "sb.db"),
            "github_secret": "s"}


def _cfg(tmp_path, port, **kw):
    base = {"mamamia_db_path": str(tmp_path / "mm.db"),
            "switchboard_db_path": str(tmp_path / "sb.db"),
            "github_secret": "s", "port": port,
            "discord_bot_token": "t", "discord_application_id": "1"}
    base.update(kw)
    return base


def test_build_github_only(tmp_path):
    bus, sensors = build(_base(tmp_path))
    assert any(isinstance(s, GitHubSensor) for s in sensors)
    assert not any(isinstance(s, DiscordSensor) for s in sensors)
    names = {a.name for a in bus._actuators}
    assert "discord.post" not in names and "discord.reply_to_command" not in names
    assert any(t.name == "logger" for t in bus._taps)


def test_build_wires_discord_and_relay(tmp_path):
    cfg = _base(tmp_path) | {"discord_bot_token": "bot", "discord_application_id": "app",
                             "discord_github_notify_channel_id": "chan-9"}
    bus, sensors = build(cfg)
    assert any(isinstance(s, DiscordSensor) for s in sensors)
    assert {"discord.post", "discord.reply_to_command"} <= {a.name for a in bus._actuators}
    assert {"ping", "echo", "github-notify"} <= {d.name for d in bus._deciders}


def test_log_max_messages_reaches_the_bus(tmp_path):
    # SB_LOG_MAX_MESSAGES is passed through compose; it must not be inert.
    bus, _ = build(_base(tmp_path) | {"log_max_messages": 250})
    assert bus._log_max_messages == 250
    assert build(_base(tmp_path))[0]._log_max_messages == 100_000   # default


def test_env_names_reach_the_bus(tmp_path, monkeypatch):
    """Every knob is env-configurable and lands where its name says."""
    from switchboard.app import build
    cfg = {"mamamia_db_path": str(tmp_path / "mm.db"),
           "switchboard_db_path": str(tmp_path / "sb.db"),
           "github_secret": "s", "port": 8161,
           "message_max_retries": 3, "handler_timeout_s": 7.0,
           "retry_backoff_max_s": 11.0, "retry_after_max_s": 13.0,
           "log_max_messages": 100_000}
    bus, _ = build(cfg)
    assert bus._message_max_retries == 3
    assert bus._handler_timeout_s == 7.0
    assert bus._retry_backoff_max_s == 11.0
    assert bus._retry_after_max_s == 13.0
    assert bus._log_max_messages == 100_000


def test_log_max_messages_defaults_to_100k(tmp_path):
    """clock.tick emits 1440/day; at the old 10k default ticks would fill the
    log in about a week and evict real history."""
    from switchboard.app import build
    bus, _ = build({"mamamia_db_path": str(tmp_path / "mm.db"),
                    "switchboard_db_path": str(tmp_path / "sb.db"),
                    "github_secret": "s", "port": 8162})
    assert bus._log_max_messages == 100_000


def test_relay_decider_absent_without_notify_channel(tmp_path):
    cfg = _base(tmp_path) | {"discord_bot_token": "bot", "discord_application_id": "app"}
    bus, _ = build(cfg)
    dnames = {d.name for d in bus._deciders}
    assert "ping" in dnames and "github-notify" not in dnames
    assert "discord.reply_to_command" in {a.name for a in bus._actuators}
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
    bus = Bus(str(tmp_path / "mm.db"), consumer_wait_ms=50, lease_reaper_interval_s=3600.0)
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


def test_the_clock_sensor_is_always_wired(tmp_path):
    """It has no dependencies and nothing to configure — like the kv actuator,
    it ships with the platform and sits idle until something subscribes."""
    from switchboard.app import build
    bus, _ = build({"mamamia_db_path": str(tmp_path / "mm.db"),
                    "switchboard_db_path": str(tmp_path / "sb.db"),
                    "github_secret": "s", "port": 8163})
    assert "clock" in bus.topology()["sensors"]


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
        "llm_backend": "anthropic", "llm_api_key": "sk-test",
        "llm_model": "claude-sonnet-5",
    })
    assert "llm" in {a.name for a in bus._actuators}
    agent = next(d for d in bus._deciders if d.name == "agent")
    names = {t["name"] for t in agent._tools}
    assert names == {"discord.post", "discord.history", "discord.react"}


def test_the_agent_needs_discord_not_just_a_key(tmp_path):
    # Wiring the agent without its tools would give it a tool list naming
    # actuators nobody bound - spec 7.5's trusted-binding assumption broken
    # by our own wiring.
    from switchboard.app import build
    bus, _ = build({
        "mamamia_db_path": str(tmp_path / "mm.db"),
        "switchboard_db_path": str(tmp_path / "sb.db"),
        "github_secret": "s", "port": 8143,
        "llm_backend": "anthropic", "llm_api_key": "sk-test",
        "llm_model": "claude-sonnet-5",
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
        "llm_backend": "anthropic", "llm_api_key": "sk-test",
        "llm_model": "claude-sonnet-5",
    })
    names = [a.name for a in bus._actuators]
    assert names.count("discord.post") == 1
    assert names.count("discord.history") == 1
    assert names.count("discord.reply_to_command") == 1


def test_agent_topology_lists_the_agent_decider(tmp_path):
    # The dashboard draws the real graph from topology(), so the agent must
    # be visible there once wired.
    from switchboard.app import build
    bus, _ = build({
        "mamamia_db_path": str(tmp_path / "mm.db"),
        "switchboard_db_path": str(tmp_path / "sb.db"),
        "github_secret": "s", "port": 8145,
        "discord_bot_token": "t", "discord_application_id": "1",
        "llm_backend": "anthropic", "llm_api_key": "sk-test",
        "llm_model": "claude-sonnet-5",
    })
    assert "agent" in bus.topology()["deciders"]


def test_openai_backend_is_selected_and_carries_its_base_url(tmp_path):
    from switchboard.app import build
    from switchboard.actuators.llm.backends.openai import OpenAiBackend
    bus, _ = build(_cfg(tmp_path, 8151, llm_backend="openai", llm_api_key="k",
                        llm_base_url="https://api.groq.com/openai/v1",
                        llm_model="llama-3.3-70b-versatile"))
    llm = next(a for a in bus._actuators if a.name == "llm")
    assert isinstance(llm._backend, OpenAiBackend)


def test_anthropic_backend_is_selected(tmp_path):
    from switchboard.app import build
    from switchboard.actuators.llm.backends.anthropic import AnthropicBackend
    bus, _ = build(_cfg(tmp_path, 8152, llm_backend="anthropic", llm_api_key="k",
                        llm_model="claude-sonnet-5"))
    llm = next(a for a in bus._actuators if a.name == "llm")
    assert isinstance(llm._backend, AnthropicBackend)


def test_the_agent_gets_the_configured_model(tmp_path):
    from switchboard.app import build
    bus, _ = build(_cfg(tmp_path, 8153, llm_backend="openai", llm_api_key="k",
                        llm_base_url="http://x", llm_model="some-model"))
    agent = next(d for d in bus._deciders if d.name == "agent")
    assert agent._model == "some-model"


def test_no_llm_key_means_no_agent_and_no_llm(tmp_path):
    from switchboard.app import build
    bus, _ = build(_cfg(tmp_path, 8154))
    assert "llm" not in {a.name for a in bus._actuators}
    assert "agent" not in {d.name for d in bus._deciders}


def test_an_unknown_backend_fails_fast(tmp_path):
    # A typo must not silently produce a Switchboard with no agent - that is
    # the failure mode with no error to observe.
    from switchboard.app import build
    with pytest.raises(ValueError):
        build(_cfg(tmp_path, 8155, llm_backend="gpt5", llm_api_key="k",
                   llm_model="m"))


def test_a_missing_model_fails_fast(tmp_path):
    from switchboard.app import build
    with pytest.raises(ValueError):
        build(_cfg(tmp_path, 8156, llm_backend="openai", llm_api_key="k",
                   llm_base_url="http://x"))


def test_the_watchdog_threshold_is_derived_from_the_bus_not_hardcoded(tmp_path):
    """A literal would silently go stale: the handler timeout is multiplied by
    (retries + 1), so changing it moves the legitimate-work window by minutes.
    Derived, the watchdog follows."""
    from switchboard.app import build
    cfg = {"mamamia_db_path": str(tmp_path / "mm.db"),
           "switchboard_db_path": str(tmp_path / "sb.db"),
           "github_secret": "s", "port": 8164,
           "discord_bot_token": "t", "discord_application_id": "1",
           "llm_backend": "openai", "llm_api_key": "k",
           "llm_base_url": "http://x", "llm_model": "m",
           "handler_timeout_s": 100.0}
    bus, _ = build(cfg)
    agent = next(d for d in bus._deciders if d.name == "agent")
    assert agent._stuck_after > bus.worst_case_retry_seconds


def test_raising_the_handler_timeout_moves_the_watchdog(tmp_path):
    from switchboard.app import build
    def mk(port, t):
        bus, _ = build({"mamamia_db_path": str(tmp_path / f"mm{port}.db"),
                        "switchboard_db_path": str(tmp_path / f"sb{port}.db"),
                        "github_secret": "s", "port": port,
                        "discord_bot_token": "t", "discord_application_id": "1",
                        "llm_backend": "openai", "llm_api_key": "k",
                        "llm_base_url": "http://x", "llm_model": "m",
                        "handler_timeout_s": t})
        return next(d for d in bus._deciders if d.name == "agent")._stuck_after
    assert mk(8165, 100.0) > mk(8166, 30.0)


def _agent_with(tmp_path, port, **extra):
    from switchboard.app import build
    bus, _ = build({"mamamia_db_path": str(tmp_path / f"mm{port}.db"),
                    "switchboard_db_path": str(tmp_path / f"sb{port}.db"),
                    "github_secret": "s", "port": port,
                    "discord_bot_token": "t", "discord_application_id": "1",
                    "llm_backend": "openai", "llm_api_key": "k",
                    "llm_base_url": "http://x", "llm_model": "m", **extra})
    return bus, next(d for d in bus._deciders if d.name == "agent")


def test_the_stuck_margin_is_a_multiplier_of_the_derived_window(tmp_path):
    bus, agent = _agent_with(tmp_path, 8167, stuck_margin=2.0)
    assert agent._stuck_after == bus.worst_case_retry_seconds * 2.0


def test_a_stuck_margin_below_one_is_clamped(tmp_path):
    """The whole point of deriving the threshold is that it sits PAST the
    retry window. A margin under 1.0 would put it inside, freeing sessions
    whose commands are still legitimately retrying -- so it cannot be
    expressible, however the env is set."""
    bus, agent = _agent_with(tmp_path, 8168, stuck_margin=0.1)
    assert agent._stuck_after >= bus.worst_case_retry_seconds
