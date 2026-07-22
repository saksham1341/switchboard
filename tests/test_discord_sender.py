import json
import httpx
import pytest
from switchboard.egress.discord import DiscordSender, DISCORD_API


def _client(handler):
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


async def test_reply_posts_interaction_followup_without_auth():
    seen = {}

    def handler(request):
        seen["method"] = request.method
        seen["url"] = str(request.url)
        seen["auth"] = request.headers.get("authorization")
        seen["body"] = json.loads(request.content)
        return httpx.Response(200, json={})

    s = DiscordSender("bot-tok", "app-123", client=_client(handler))
    await s.reply("int-tok", "pong")
    await s.close()

    assert seen["method"] == "POST"
    assert seen["url"] == f"{DISCORD_API}/webhooks/app-123/int-tok"
    assert seen["auth"] is None                       # followups carry no bot auth
    assert seen["body"] == {"content": "pong"}


async def test_send_posts_channel_message_with_bot_auth():
    seen = {}

    def handler(request):
        seen["url"] = str(request.url)
        seen["auth"] = request.headers.get("authorization")
        seen["body"] = json.loads(request.content)
        return httpx.Response(200, json={})

    s = DiscordSender("bot-tok", "app-123", client=_client(handler))
    await s.send("chan-9", "hello channel")
    await s.close()

    assert seen["url"] == f"{DISCORD_API}/channels/chan-9/messages"
    assert seen["auth"] == "Bot bot-tok"
    assert seen["body"] == {"content": "hello channel"}


async def test_reply_raises_on_error_status():
    def handler(request):
        return httpx.Response(404, json={"message": "Unknown Webhook"})

    s = DiscordSender("bot-tok", "app-123", client=_client(handler))
    with pytest.raises(httpx.HTTPStatusError):
        await s.reply("expired-tok", "too late")
    await s.close()
