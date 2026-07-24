import asyncio
import os

from switchboard.bus import Bus
from switchboard.http import HttpServer
from switchboard.store import SqliteStore
from switchboard.sensors.github import GitHubSensor
from switchboard.sensors.deadletter import DeadLetterSensor
from switchboard.sensors.discord import DiscordSensor, CommandSpec, Option
from switchboard.deciders.github_notify import GitHubNotifyDecider
from switchboard.deciders.discord_cmds import PingDecider, EchoDecider
from switchboard.deciders.agent import AgentDecider
from switchboard.actuators.discord import DiscordPost, DiscordReply, DiscordHistory
from switchboard.actuators.kv import KvActuator
from switchboard.actuators.llm import LlmActuator
from switchboard.taps.logger import LoggerTap
from switchboard.dashboard import Dashboard, DashboardTap

DISCORD_COMMANDS = [
    CommandSpec("ping", "Ping Switchboard"),
    CommandSpec("echo", "Echo a message back",
                options=(Option("message", "Text to echo back", type=str, required=True),)),
]


def build(config: dict):
    http = HttpServer(host=config.get("host", "0.0.0.0"),
                      port=int(config.get("port", 8080)))
    store = SqliteStore(config["switchboard_db_path"])
    bus = Bus(config["mamamia_db_path"], store=store, http=http,
              max_log_messages=int(config.get("max_log_messages", 10_000)))
    bus.add_tap(LoggerTap())
    # Memory, always available. No key, no cost, no external call — it is local
    # storage over the store the Bus already holds, and it sits idle until
    # something emits kv commands. Wired now so the backend is proven live
    # before the decider that depends on it arrives. (The llm actuator stays
    # unwired: registering it would put an API key and real spend in the
    # deployment for a feature nothing drives yet.)
    bus.add_actuator(KvActuator())

    sensors = [GitHubSensor(secret=config["github_secret"]),
               DeadLetterSensor(config["mamamia_db_path"])]
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
        bus.add_actuator(DiscordReply(token, app_id))
        # Registered whenever Discord is wired: it reads
        # over REST and needs no intent, and Phase 4 hands its tool_spec to the
        # agent. Idle until something emits the command.
        history = DiscordHistory(token, app_id)
        bus.add_actuator(history)

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
        if config.get("anthropic_api_key"):
            agent_post = _discord_post()
            bus.add_actuator(LlmActuator(config["anthropic_api_key"]))
            bus.add_decider(AgentDecider(tools=[
                agent_post.tool_spec | {"name": agent_post.name},
                history.tool_spec | {"name": history.name},
            ]))

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
        "max_log_messages": int(os.environ.get("SB_MAX_LOG_MESSAGES", "10000")),
        "discord_bot_token": os.environ.get("DISCORD_BOT_TOKEN"),
        "discord_application_id": os.environ.get("DISCORD_APPLICATION_ID"),
        "discord_guild_id": os.environ.get("DISCORD_GUILD_ID"),
        "discord_github_notify_channel_id": os.environ.get("DISCORD_GITHUB_NOTIFY_CHANNEL_ID"),
        "anthropic_api_key": os.environ.get("ANTHROPIC_API_KEY"),
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
