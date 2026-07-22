import asyncio, json, httpx
from switchboard.actuators.discord import DiscordPost, DiscordReply, DISCORD_API
from switchboard.message import Command, ActCtx


def _client(handler):
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


def _cmd(name, args):
    class M:  # minimal message
        id = 1
        payload = args
        metadata = {"name": name, "observation_id": 7}
    return Command.from_message(M())


async def _recorder(results):
    async def emit_result(name, payload, cmd_id):   # matches ActCtx._emit_result (awaited)
        results.append((name, payload, cmd_id)); return 0
    return emit_result


async def test_discord_post_sends_embed_and_reports_result():
    seen, results = {}, []
    def h(req):
        seen["url"] = str(req.url); seen["body"] = json.loads(req.content)
        seen["auth"] = req.headers.get("authorization")
        return httpx.Response(200, json={"id": "m-1"})
    a = DiscordPost("bot", "app", channel_id="chan-9", client=_client(h))
    ctx = ActCtx(cmd=_cmd("discord.post", {"channel_id": "chan-9", "embed": {"title": "hi"}}),
                 context=a.context(), _emit_result=await _recorder(results))
    await a.act(ctx.cmd, ctx)
    assert seen["url"] == f"{DISCORD_API}/channels/chan-9/messages"
    assert seen["body"]["embeds"] == [{"title": "hi"}]
    assert seen["auth"] == "Bot bot"
    assert results and results[0][0] == "discord.post.ok"


async def test_discord_reply_uses_followup_and_reports_result():
    seen, results = {}, []
    def h(req):
        seen["url"] = str(req.url); seen["body"] = json.loads(req.content)
        seen["auth"] = req.headers.get("authorization")
        return httpx.Response(200, json={})
    a = DiscordReply("bot", "app", client=_client(h))
    ctx = ActCtx(cmd=_cmd("discord.reply", {"interaction_token": "tok", "content": "pong"}),
                 context=a.context(), _emit_result=await _recorder(results))
    await a.act(ctx.cmd, ctx)
    assert seen["url"] == f"{DISCORD_API}/webhooks/app/tok"
    assert seen["body"] == {"content": "pong"}
    assert seen["auth"] is None
    assert results and results[0][0] == "discord.reply.ok"
