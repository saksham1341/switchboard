import asyncio
import os

from switchboard.broker import Broker
from switchboard.egress import LoggerEgress
from switchboard.egress.discord import DiscordEgress
from switchboard.ingress.github import GitHubIngress
from switchboard.ingress.discord import DiscordIngress, Command, Option

DISCORD_COMMANDS = [
    Command("ping", "Ping Switchboard"),
    Command("echo", "Echo a message back",
            options=(Option("message", "Text to echo back", type=str, required=True),)),
]


def build(config: dict) -> tuple[Broker, list]:
    broker = Broker(
        mamamia_db_path=config["mamamia_db_path"],
        switchboard_db_path=config["switchboard_db_path"],
        max_log_messages=config.get("max_log_messages", 10_000),
    )
    broker.attach(LoggerEgress())  # truly log-all

    ingresses: list = [
        GitHubIngress(
            secret=config["github_secret"],
            host=config.get("host", "0.0.0.0"),
            port=int(config.get("port", 8080)),
        )
    ]

    if config.get("discord_bot_token"):
        broker.attach(DiscordEgress(
            config["discord_bot_token"], config["discord_application_id"],
        ))
        ingresses.append(DiscordIngress(
            config["discord_bot_token"],
            commands=DISCORD_COMMANDS, guild_id=config.get("discord_guild_id"),
        ))

    return broker, ingresses


async def run() -> None:
    data_dir = os.environ.get("SB_DATA_DIR", "/data")
    config = {
        "mamamia_db_path": os.path.join(data_dir, "events.db"),
        "switchboard_db_path": os.path.join(data_dir, "switchboard.db"),
        "github_secret": os.environ["GITHUB_WEBHOOK_SECRET"],
        "max_log_messages": int(os.environ.get("SB_MAX_LOG_MESSAGES", "10000")),
        "port": int(os.environ.get("SB_PORT", "8080")),
        "discord_bot_token": os.environ.get("DISCORD_BOT_TOKEN"),
        "discord_application_id": os.environ.get("DISCORD_APPLICATION_ID"),
        "discord_guild_id": os.environ.get("DISCORD_GUILD_ID"),
    }
    broker, ingresses = build(config)
    await broker.start()
    try:
        # each ingress owns its transport and serves until cancelled
        await asyncio.gather(*(ing.start(broker.publish) for ing in ingresses))
    finally:
        for ing in ingresses:
            await ing.stop()
        await broker.stop()


if __name__ == "__main__":
    asyncio.run(run())
