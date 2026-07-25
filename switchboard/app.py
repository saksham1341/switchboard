import asyncio
import os

from switchboard.bus import Bus
from switchboard.http import HttpServer
from switchboard.store import SqliteStore
from switchboard.sensors.github import GitHubSensor
from switchboard.sensors.deadletter import DeadLetterSensor
from switchboard.sensors.clock import ClockSensor
from switchboard.sensors.discord import DiscordSensor, CommandSpec, Option
from switchboard.deciders.github_notify import GitHubNotifyDecider
from switchboard.deciders.discord_cmds import PingDecider, EchoDecider
from switchboard.deciders.agent import AgentDecider
from switchboard.actuators.discord import (
    DiscordPost, DiscordReplyToCommand, DiscordHistory, DiscordReact)
from switchboard.actuators.kv import KvActuator
from switchboard.actuators.llm import LlmActuator
from switchboard.actuators.llm.backends.anthropic import AnthropicBackend
from switchboard.actuators.llm.backends.openai import OpenAiBackend
from switchboard.taps.logger import LoggerTap
from switchboard.dashboard import Dashboard, DashboardTap

DISCORD_COMMANDS = [
    CommandSpec("ping", "Ping Switchboard"),
    CommandSpec("echo", "Echo a message back",
                options=(Option("message", "Text to echo back", type=str, required=True),)),
    CommandSpec("reset", "Clear this channel's conversation"),
]


def _llm_backend(config):
    """Construct the configured provider backend, or None if no key is set.

    Fails loudly on a bad name or a missing model rather than returning None:
    a typo that silently produced a Switchboard with no agent would be the
    failure mode with no error to observe (spec §7.5).
    """
    key = config.get("llm_api_key")
    if not key:
        return None
    name = (config.get("llm_backend") or "anthropic").lower()
    model = config.get("llm_model")
    if not model:
        raise ValueError("llm_model is required when llm_api_key is set")
    if name == "anthropic":
        return AnthropicBackend(key)
    if name == "openai":
        base = config.get("llm_base_url")
        if not base:
            raise ValueError("llm_base_url is required for the openai backend")
        return OpenAiBackend(key, base_url=base)
    raise ValueError(f"unknown llm_backend: {name!r}")


def build(config: dict):
    http = HttpServer(host=config.get("host", "0.0.0.0"),
                      port=int(config.get("port", 8080)))
    store = SqliteStore(config["switchboard_db_path"])
    bus = Bus(config["mamamia_db_path"], store=store, http=http,
              message_max_retries=int(config.get("message_max_retries", 10)),
              handler_timeout_s=float(config.get("handler_timeout_s", 100.0)),
              retry_backoff_max_s=float(config.get("retry_backoff_max_s", 300.0)),
              retry_after_max_s=float(config.get("retry_after_max_s", 120.0)),
              consumer_wait_ms=int(config.get("consumer_wait_ms", 30_000)),
              lease_reaper_interval_s=float(config.get("lease_reaper_interval_s", 60.0)),
              dedup_ttl_s=float(config.get("dedup_ttl_s", 3600.0)),
              log_max_messages=int(config.get("log_max_messages", 100_000)),
              log_max_dead=int(config.get("log_max_dead", 500)))
    bus.add_tap(LoggerTap())
    # Memory, always available. No key, no cost, no external call — it is local
    # storage over the store the Bus already holds, and it sits idle until
    # something emits kv commands. Wired now so the backend is proven live
    # before the decider that depends on it arrives. (The llm actuator stays
    # unwired: registering it would put an API key and real spend in the
    # deployment for a feature nothing drives yet.)
    bus.add_actuator(KvActuator())

    # Validated unconditionally, before the Discord branch below even runs:
    # an unknown llm_backend or a missing llm_model must raise here regardless
    # of whether Discord is configured. Otherwise a GitHub-only deployment
    # with a typo'd backend name would build fine and simply have no agent -
    # the one failure mode with no error to observe.
    backend = _llm_backend(config)

    sensors = [GitHubSensor(secret=config["github_secret"]),
               DeadLetterSensor(config["mamamia_db_path"]),
               ClockSensor(interval=config.get("clock_tick_s", 60.0))]
    for s in sensors:
        bus.add_sensor(s)

    if config.get("discord_bot_token"):
        token = config["discord_bot_token"]
        app_id = config.get("discord_application_id")
        if not app_id:
            raise ValueError("discord_application_id is required when discord_bot_token is set")
        discord_sensor = DiscordSensor(token, commands=DISCORD_COMMANDS,
                                       guild_id=config.get("discord_guild_id"))
        bus.add_sensor(discord_sensor); sensors.append(discord_sensor)
        bus.add_decider(PingDecider()); bus.add_decider(EchoDecider())
        bus.add_actuator(DiscordReplyToCommand(token, app_id))
        # Registered whenever Discord is wired: it reads
        # over REST and needs no intent, and Phase 4 hands its tool_spec to the
        # agent. Idle until something emits the command.
        history = DiscordHistory(token, app_id)
        bus.add_actuator(history)
        react = DiscordReact(token, app_id)
        bus.add_actuator(react)

        # DiscordPost is wanted by two independent branches below - the
        # github-notify relay and the agent's tool list. Construct and register
        # it at most once, whichever branch needs it first: the command name
        # "discord.post" must map to exactly one actuator, because the Bus
        # gives each *registration* its own consumer group filtered on that
        # name (see Bus._act). Adding it twice would create two consumer
        # groups for one command name, and every discord.post command would
        # be handled - and posted - twice.
        post = None

        def _discord_post():
            nonlocal post
            if post is None:
                post = DiscordPost(token, app_id)
                bus.add_actuator(post)
            return post

        if config.get("discord_github_notify_channel_id"):
            bus.add_decider(GitHubNotifyDecider(
                channel_id=config["discord_github_notify_channel_id"]))
            _discord_post()

        # The agent is wired only with BOTH a key and Discord: its tool list is
        # a promise that every named tool has a bound actuator (spec 7.5), and
        # the wiring is what keeps that promise. A key alone would hand it tools
        # that reach nothing - a command nobody consumes never fails, it simply
        # hangs, which is the one failure mode with no error to observe.
        if backend is not None:
            agent_post = _discord_post()
            bus.add_actuator(LlmActuator(backend))
            bus.add_decider(AgentDecider(
                model=config["llm_model"],
                # The watchdog's threshold is derived from the Bus's own
                # worst-case retry window, never a literal: that window is
                # the longest a message can honestly stay in flight, and a
                # decider guessing its own number is exactly the failure the
                # derivation exists to prevent. SB_STUCK_MARGIN scales that
                # window, it does not replace it: a duration knob would make
                # "fire before the retries are done" expressible, and a
                # watchdog that frees a session whose command is still in
                # flight delivers the result to a session that moved on.
                # Clamped at 1.0 so even a hostile value keeps the invariant.
                stuck_after=(bus.worst_case_retry_seconds
                             * max(1.0, float(config.get("stuck_margin", 1.2)))),
                session_ttl_s=config.get("session_ttl_s", 14400.0),
                tools=[agent_post.tool_spec | {"name": agent_post.name},
                       history.tool_spec | {"name": history.name},
                       react.tool_spec | {"name": react.name}]))

    # Expired keys are already invisible to reads, so this is only about
    # reclaiming disk — and only some backends need it. Sqlite and memory stores
    # sweep; a Redis-backed store expires natively and exposes no purge at all.
    # purge is deliberately not part of the KeyStore contract, so ask rather than
    # assume.
    purge = getattr(store, "purge", None)
    if purge is not None:
        bus.schedule_maintenance("store", 3600.0, purge)

    # No token, no dashboard: fail closed rather than serving an unauthenticated
    # ingest endpoint. There is deliberately no default token.
    if config.get("dashboard_token"):
        dash = Dashboard(topology=bus.topology(), token=config["dashboard_token"],
                         db_path=config["mamamia_db_path"])
        http.route("/", dash.page, owner="dashboard")
        http.route("/dashboard/stream", dash.stream, owner="dashboard")
        http.route("/dashboard/ingest", dash.ingest, methods=["POST"], owner="dashboard")
        bus.add_tap(DashboardTap(url=config["dashboard_ingest_url"],
                                 token=config["dashboard_token"]))
        # The only polling in the design: a dead command emits no result
        # observation, so absence is all the stream can show.
        bus.schedule_maintenance("dashboard-dead", 5.0, dash.refresh_dead)

    return bus, sensors


async def run() -> None:
    data_dir = os.environ.get("SB_DATA_DIR", "/data")
    config = {
        "mamamia_db_path": os.path.join(data_dir, "events.db"),
        "switchboard_db_path": os.path.join(data_dir, "switchboard.db"),
        "github_secret": os.environ["GITHUB_WEBHOOK_SECRET"],
        "port": int(os.environ.get("SB_PORT", "8080")),
        "message_max_retries": int(os.environ.get("SB_MESSAGE_MAX_RETRIES", "10")),
        "handler_timeout_s": float(os.environ.get("SB_HANDLER_TIMEOUT_S", "100")),
        "retry_backoff_max_s": float(os.environ.get("SB_RETRY_BACKOFF_MAX_S", "300")),
        "retry_after_max_s": float(os.environ.get("SB_RETRY_AFTER_MAX_S", "120")),
        "consumer_wait_ms": int(os.environ.get("SB_CONSUMER_WAIT_MS", "30000")),
        "lease_reaper_interval_s": float(os.environ.get("SB_LEASE_REAPER_INTERVAL_S", "60")),
        "dedup_ttl_s": float(os.environ.get("SB_DEDUP_TTL_S", "3600")),
        "log_max_messages": int(os.environ.get("SB_LOG_MAX_MESSAGES", "100000")),
        "log_max_dead": int(os.environ.get("SB_LOG_MAX_DEAD", "500")),
        "clock_tick_s": float(os.environ.get("SB_CLOCK_TICK_S", "60")),
        "session_ttl_s": float(os.environ.get("SB_SESSION_TTL_S", "14400")),
        # A MULTIPLIER, not a duration -- see the clamp in build().
        "stuck_margin": float(os.environ.get("SB_STUCK_MARGIN", "1.2")),
        "discord_bot_token": os.environ.get("DISCORD_BOT_TOKEN"),
        "discord_application_id": os.environ.get("DISCORD_APPLICATION_ID"),
        "discord_guild_id": os.environ.get("DISCORD_GUILD_ID"),
        "discord_github_notify_channel_id": os.environ.get("DISCORD_GITHUB_NOTIFY_CHANNEL_ID"),
        "llm_backend": os.environ.get("SB_LLM_BACKEND", "anthropic"),
        "llm_api_key": os.environ.get("SB_LLM_API_KEY"),
        "llm_base_url": os.environ.get("SB_LLM_BASE_URL"),
        "llm_model": os.environ.get("SB_LLM_MODEL"),
        "dashboard_token": os.environ.get("SB_DASHBOARD_TOKEN"),
        "dashboard_ingest_url": os.environ.get(
            "SB_DASHBOARD_INGEST_URL",
            f"http://127.0.0.1:{os.environ.get('SB_PORT', '8080')}/dashboard/ingest"),
    }
    bus, _ = build(config)
    await bus.start()
    try:
        while True:
            await asyncio.sleep(3600)
    finally:
        await bus.stop()


if __name__ == "__main__":
    asyncio.run(run())
