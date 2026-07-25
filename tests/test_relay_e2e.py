import asyncio, json, httpx
from switchboard.bus import Bus
from switchboard.deciders.github_notify import GitHubNotifyDecider
from switchboard.deciders.discord_cmds import PingDecider
from switchboard.actuators.discord import DiscordPost, DiscordReplyToCommand


async def _wait(pred, timeout=8.0):
    async def loop():
        while not pred(): await asyncio.sleep(0.01)
    await asyncio.wait_for(loop(), timeout)


async def test_github_observation_reaches_channel(tmp_path):
    seen = {}
    def h(req):
        seen["url"] = str(req.url); seen["body"] = json.loads(req.content)
        return httpx.Response(200, json={"id": "m1"})
    client = httpx.AsyncClient(transport=httpx.MockTransport(h))
    bus = Bus(str(tmp_path / "mm.db"), consumer_wait_ms=50, lease_reaper_interval_s=3600.0)
    bus.add_decider(GitHubNotifyDecider(channel_id="chan-9"))
    bus.add_actuator(DiscordPost("bot", "app", client=client))
    await bus.start()
    try:
        await bus.emit_observation("github.home.pr.opened",
            {"repository": {"full_name": "yp/home", "html_url": "https://github.com/yp/home"},
             "sender": {"login": "alice"},
             "pull_request": {"number": 7, "title": "Add retry", "html_url": "https://github.com/yp/home/pull/7"}})
        await _wait(lambda: "url" in seen)
        assert seen["url"] == "https://discord.com/api/v10/channels/chan-9/messages"
        assert seen["body"]["embeds"][0]["title"] == "🔀 PR #7 opened"
        assert [b["label"] for b in seen["body"]["components"][0]["components"]] == ["View PR", "View diff"]
    finally:
        await bus.stop(); await client.aclose()


async def test_ping_observation_reaches_followup(tmp_path):
    seen = {}
    def h(req):
        seen["url"] = str(req.url); seen["body"] = json.loads(req.content)
        return httpx.Response(200, json={})
    client = httpx.AsyncClient(transport=httpx.MockTransport(h))
    bus = Bus(str(tmp_path / "mm.db"), consumer_wait_ms=50, lease_reaper_interval_s=3600.0)
    bus.add_decider(PingDecider())
    bus.add_actuator(DiscordReplyToCommand("bot", "app", client=client))
    await bus.start()
    try:
        await bus.emit_observation("discord.command.ping", {"interaction_token": "tok-1"})
        await _wait(lambda: "url" in seen)
        assert seen["url"] == "https://discord.com/api/v10/webhooks/app/tok-1"
        assert seen["body"] == {"content": "pong (via the durable path)"}
    finally:
        await bus.stop(); await client.aclose()
