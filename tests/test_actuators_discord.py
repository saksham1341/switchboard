import asyncio, json, httpx
from switchboard.actuators.discord import DiscordPost, DiscordReply, DISCORD_API
from switchboard.message import Command, ActCtx, ActuatorCtx
from switchboard.store import MemoryStore


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


def _bind(a):
    a.bind(ActuatorCtx(store=MemoryStore()))
    return a


async def test_discord_post_sends_embed_and_reports_result():
    seen, results = {}, []
    def h(req):
        seen["url"] = str(req.url); seen["body"] = json.loads(req.content)
        seen["auth"] = req.headers.get("authorization")
        return httpx.Response(200, json={"id": "m-1"})
    a = _bind(DiscordPost("bot", "app", channel_id="chan-9", client=_client(h)))
    ctx = ActCtx(cmd=_cmd("discord.post", {"channel_id": "chan-9", "embed": {"title": "hi"}}),
                 _emit_result=await _recorder(results))
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
    a = _bind(DiscordReply("bot", "app", client=_client(h)))
    ctx = ActCtx(cmd=_cmd("discord.reply", {"interaction_token": "tok", "content": "pong"}),
                 _emit_result=await _recorder(results))
    await a.act(ctx.cmd, ctx)
    assert seen["url"] == f"{DISCORD_API}/webhooks/app/tok"
    assert seen["body"] == {"content": "pong"}
    assert seen["auth"] is None
    assert results and results[0][0] == "discord.reply.ok"


async def test_actuator_ctx_store_is_available_after_bind():
    a = _bind(DiscordReply("bot", "app", client=_client(lambda r: httpx.Response(200, json={}))))
    await a.ctx.store.set("idem:cmd-1", "sent")
    assert await a.ctx.store.get("idem:cmd-1") == "sent"


async def test_discord_post_sends_plain_content():
    """The agent speaks in plain text; the actuator only ever forwarded embeds."""
    seen = {}
    def h(req):
        seen["body"] = json.loads(req.content)
        return httpx.Response(200, json={"id": "m-7"})
    a = _bind(DiscordPost("bot", "app", channel_id="chan-9", client=_client(h)))
    results = []
    ctx = ActCtx(cmd=_cmd("discord.post", {"content": "hello there"}),
                 _emit_result=await _recorder(results))
    await a.act(ctx.cmd, ctx)
    assert seen["body"]["content"] == "hello there"


async def test_discord_post_returns_the_message_id():
    """The decider needs the id to reference the message later."""
    def h(req):
        return httpx.Response(200, json={"id": "m-7"})
    a = _bind(DiscordPost("bot", "app", channel_id="chan-9", client=_client(h)))
    results = []
    ctx = ActCtx(cmd=_cmd("discord.post", {"content": "hi"}),
                 _emit_result=await _recorder(results))
    await a.act(ctx.cmd, ctx)
    assert results[0][1] == {"channel_id": "chan-9", "message_id": "m-7"}


async def test_explicit_channel_id_wins_over_the_default():
    seen = {}
    def h(req):
        seen["url"] = str(req.url)
        return httpx.Response(200, json={"id": "m-1"})
    a = _bind(DiscordPost("bot", "app", channel_id="chan-9", client=_client(h)))
    results = []
    ctx = ActCtx(cmd=_cmd("discord.post", {"content": "x", "channel_id": "other-1"}),
                 _emit_result=await _recorder(results))
    await a.act(ctx.cmd, ctx)
    assert seen["url"].endswith("/channels/other-1/messages")


async def test_tool_spec_exposes_content_and_destination():
    spec = DiscordPost.tool_spec
    assert spec is not None
    assert set(spec["input_schema"]["properties"]) == {"content", "channel_id"}
    assert spec["input_schema"]["required"] == ["content"]
