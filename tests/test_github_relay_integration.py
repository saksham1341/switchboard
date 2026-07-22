import asyncio
import json
import httpx
from switchboard.broker import Broker
from switchboard.egress.discord import DiscordEgress
from switchboard.event import EventInput


async def _wait_for(predicate, timeout=8.0):
    async def loop():
        while not predicate():
            await asyncio.sleep(0.01)
    await asyncio.wait_for(loop(), timeout)


async def test_github_pr_event_reaches_channel_with_embed_and_buttons(tmp_path):
    seen = {}

    def handler(request):
        seen["url"] = str(request.url)
        seen["auth"] = request.headers.get("authorization")
        seen["body"] = json.loads(request.content)
        return httpx.Response(200, json={})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    b = Broker(
        mamamia_db_path=str(tmp_path / "events.db"),
        switchboard_db_path=str(tmp_path / "sb.db"),
        wait_ms=50, reaper_interval=3600.0,
    )
    b.attach(DiscordEgress("bot-tok", "app-123", notify_channel_id="chan-9", client=client))
    await b.start()
    try:
        await b.publish(EventInput(
            kind="github.home.pr.opened", source="github",
            payload={"repository": {"full_name": "yp/home", "html_url": "https://github.com/yp/home"},
                     "sender": {"login": "alice"},
                     "pull_request": {"number": 7, "title": "Add retry backoff",
                                      "html_url": "https://github.com/yp/home/pull/7"}},
            dedupe_key="delivery-1",
            meta={"delivery": "delivery-1", "depth": "0"},
        ))
        await _wait_for(lambda: "url" in seen)
        assert seen["url"] == "https://discord.com/api/v10/channels/chan-9/messages"
        assert seen["auth"] == "Bot bot-tok"
        assert seen["body"]["embeds"][0]["title"] == "🔀 PR #7 opened"
        buttons = seen["body"]["components"][0]["components"]
        assert [(x["label"], x["url"]) for x in buttons] == [
            ("View PR", "https://github.com/yp/home/pull/7"),
            ("View diff", "https://github.com/yp/home/pull/7/files"),
        ]
    finally:
        await b.stop()
        await client.aclose()
