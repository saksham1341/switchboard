import asyncio
import os

from switchboard.bus import Bus
from switchboard.http import HttpServer
from switchboard.store import SqliteStore
from switchboard.sensors.github import GitHubSensor
from switchboard.sensors.discord import DiscordSensor, CommandSpec, Option
from switchboard.deciders.github_notify import GitHubNotifyDecider
from switchboard.deciders.discord_cmds import PingDecider, EchoDecider
from switchboard.actuators.discord import DiscordPost, DiscordReply
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

    sensors = [GitHubSensor(secret=config["github_secret"])]
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
        if config.get("discord_github_notify_channel_id"):
            bus.add_decider(GitHubNotifyDecider(
                channel_id=config["discord_github_notify_channel_id"]))
            bus.add_actuator(DiscordPost(token, app_id))

    # Expired keys are already invisible to reads; this is only about disk.
    bus.schedule_maintenance("store", 3600.0, store.purge)

    # No token, no dashboard: fail closed rather than serving an unauthenticated
    # ingest endpoint. There is deliberately no default token.
    if config.get("dashboard_token"):
        # The dashboard gets its own HttpServer on its own port. The main server
        # is what the public tunnel reaches; this one is published loopback-only,
        # so the page and stream are reachable via an SSH tunnel and never from
        # the internet, while the webhook and /health stay public.
        dash_http = HttpServer(host=config.get("host", "0.0.0.0"),
                               port=int(config.get("dashboard_port", 8090)))
        dash = Dashboard(topology=bus.topology(), token=config["dashboard_token"],
                         db_path=config["mamamia_db_path"])
        dash_http.route("/", dash.page, owner="dashboard")
        dash_http.route("/dashboard/stream", dash.stream, owner="dashboard")
        dash_http.route("/dashboard/ingest", dash.ingest, methods=["POST"], owner="dashboard")
        bus.add_server(dash_http)
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
        "dashboard_token": os.environ.get("SB_DASHBOARD_TOKEN"),
        "dashboard_port": int(os.environ.get("SB_DASHBOARD_PORT", "8090")),
        "dashboard_ingest_url": os.environ.get(
            "SB_DASHBOARD_INGEST_URL",
            f"http://127.0.0.1:{os.environ.get('SB_DASHBOARD_PORT', '8090')}/dashboard/ingest"),
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
