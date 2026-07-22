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


def _broker(tmp_path):
    return Broker(
        mamamia_db_path=str(tmp_path / "events.db"),
        switchboard_db_path=str(tmp_path / "sb.db"),
        wait_ms=50, reaper_interval=3600.0,
    )


async def test_ping_command_reaches_followup_end_to_end(tmp_path):
    seen = {}

    def handler(request):
        seen["url"] = str(request.url)
        seen["body"] = json.loads(request.content)
        return httpx.Response(200, json={})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    b = _broker(tmp_path)
    b.attach(DiscordEgress("bot-tok", "app-123", client=client))
    await b.start()
    try:
        await b.publish(EventInput(
            kind="discord.9.command.ping", source="discord",
            payload={"command": "ping", "options": {}, "user": {"id": "1", "name": "u"},
                     "channel_id": "7", "guild_id": "9"},
            dedupe_key="interaction-1",
            meta={"interaction_token": "tok-1", "channel_id": "7"},
        ))
        await _wait_for(lambda: "url" in seen)
        assert seen["url"] == "https://discord.com/api/v10/webhooks/app-123/tok-1"
        assert seen["body"] == {"content": "pong (via the durable path)"}
    finally:
        await b.stop()
        await client.aclose()


async def test_echo_option_round_trips_into_the_followup(tmp_path):
    seen = {}

    def handler(request):
        seen["url"] = str(request.url)
        seen["body"] = json.loads(request.content)
        return httpx.Response(200, json={})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    b = _broker(tmp_path)
    b.attach(DiscordEgress("bot-tok", "app-123", client=client))
    await b.start()
    try:
        await b.publish(EventInput(
            kind="discord.9.command.echo", source="discord",
            payload={"command": "echo", "options": {"message": "round-trip!"},
                     "user": {"id": "1", "name": "u"}, "channel_id": "7", "guild_id": "9"},
            dedupe_key="interaction-2",
            meta={"interaction_token": "tok-2", "channel_id": "7"},
        ))
        await _wait_for(lambda: "url" in seen)
        assert seen["url"] == "https://discord.com/api/v10/webhooks/app-123/tok-2"
        assert seen["body"] == {"content": "round-trip!"}   # the declared option, echoed
    finally:
        await b.stop()
        await client.aclose()
