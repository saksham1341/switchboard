import asyncio
import os

from switchboard.broker import Broker
from switchboard.egress import LoggerEgress
from switchboard.ingress.github import GitHubIngress


def build(config: dict) -> tuple[Broker, GitHubIngress]:
    broker = Broker(
        mamamia_db_path=config["mamamia_db_path"],
        switchboard_db_path=config["switchboard_db_path"],
        max_log_messages=config.get("max_log_messages", 10_000),
    )
    broker.attach(LoggerEgress(filter=lambda e: e.source == "github"))
    ingress = GitHubIngress(
        secret=config["github_secret"],
        host=config.get("host", "0.0.0.0"),
        port=int(config.get("port", 8080)),
    )
    return broker, ingress


async def run() -> None:
    data_dir = os.environ.get("SB_DATA_DIR", "/data")
    config = {
        "mamamia_db_path": os.path.join(data_dir, "events.db"),
        "switchboard_db_path": os.path.join(data_dir, "switchboard.db"),
        "github_secret": os.environ["GITHUB_WEBHOOK_SECRET"],
        "max_log_messages": int(os.environ.get("SB_MAX_LOG_MESSAGES", "10000")),
        "port": int(os.environ.get("SB_PORT", "8080")),
    }
    broker, ingress = build(config)
    await broker.start()
    try:
        await ingress.start(broker.publish)   # serves until cancelled
    finally:
        await broker.stop()


if __name__ == "__main__":
    asyncio.run(run())
