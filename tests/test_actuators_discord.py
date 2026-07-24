import asyncio, json, httpx
import pytest
from switchboard.actuators.discord import DiscordPost, DiscordReplyToCommand, DISCORD_API
from switchboard.actuators.discord import DiscordHistory, HISTORY_DEFAULT, HISTORY_MAX
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


async def test_reply_to_command_uses_interaction_followup():
    seen, results = {}, []
    def h(req):
        seen["url"] = str(req.url); seen["body"] = json.loads(req.content)
        seen["auth"] = req.headers.get("authorization")
        return httpx.Response(200, json={})
    a = _bind(DiscordReplyToCommand("bot", "app", client=_client(h)))
    ctx = ActCtx(cmd=_cmd("discord.reply_to_command", {"interaction_token": "tok", "content": "pong"}),
                 _emit_result=await _recorder(results))
    await a.act(ctx.cmd, ctx)
    assert seen["url"] == f"{DISCORD_API}/webhooks/app/tok"
    assert seen["body"] == {"content": "pong"}
    assert seen["auth"] is None
    assert results and results[0][0] == "discord.reply_to_command.ok"


async def test_actuator_ctx_store_is_available_after_bind():
    a = _bind(DiscordReplyToCommand("bot", "app", client=_client(lambda r: httpx.Response(200, json={}))))
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
    assert results[0][1] == {"delivered_by": "you", "channel_id": "chan-9", "message_id": "m-7"}


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
    assert set(spec["input_schema"]["properties"]) == {
        "content", "channel_id", "reply_to_message_id"}
    # channel_id is required for the agent: it is constructed with no default
    # channel, so an omitted channel_id would post to /channels/None/messages.
    assert spec["input_schema"]["required"] == ["content", "channel_id"]


async def test_non_object_json_body_still_reports_ok():
    """Valid JSON that isn't an object must not raise: a post we could not read
    the id from is still a successful post."""
    def h(req):
        return httpx.Response(200, json=["unexpected"])
    a = _bind(DiscordPost("bot", "app", channel_id="chan-9", client=_client(h)))
    results = []
    ctx = ActCtx(cmd=_cmd("discord.post", {"content": "hi"}),
                 _emit_result=await _recorder(results))
    await a.act(ctx.cmd, ctx)
    assert results[0][0] == "discord.post.ok"
    assert results[0][1] == {"delivered_by": "you", "channel_id": "chan-9", "message_id": None}


async def test_non_json_body_still_reports_ok():
    def h(req):
        return httpx.Response(200, text="not json at all")
    a = _bind(DiscordPost("bot", "app", channel_id="chan-9", client=_client(h)))
    results = []
    ctx = ActCtx(cmd=_cmd("discord.post", {"content": "hi"}),
                 _emit_result=await _recorder(results))
    await a.act(ctx.cmd, ctx)
    assert results[0][0] == "discord.post.ok"
    assert results[0][1]["message_id"] is None


def _hcmd(args):
    class M:
        id = 1
        payload = args
        metadata = {"name": "discord.history", "observation_id": 7}
    return Command.from_message(M())


async def _run_history(act, args):
    results = []
    async def emit_result(name, payload, cmd_id):
        results.append((name, payload)); return 0
    cmd = _hcmd(args)
    await act.act(cmd, ActCtx(cmd=cmd, _emit_result=emit_result))
    return results[0]


def _history_actuator(handler):
    """Bind a DiscordHistory over a mock transport. `handler(request)` returns
    the httpx.Response the Discord API would have."""
    transport = httpx.MockTransport(handler)
    a = DiscordHistory("bot-token", "app-id",
                       client=httpx.AsyncClient(transport=transport))
    a.bind(object())
    return a


def _discord_message(mid, name, content, bot=False):
    return {"id": mid, "author": {"username": name, "bot": bot}, "content": content}


async def test_history_returns_oldest_first():
    # Discord returns newest-first; the agent reads a conversation forwards.
    def handler(request):
        return httpx.Response(200, json=[_discord_message("3", "carol", "third"),
                                         _discord_message("2", "bob", "second"),
                                         _discord_message("1", "alice", "first")])
    name, payload = await _run_history(_history_actuator(handler), {"channel_id": "222"})
    assert name == "discord.history.ok"
    assert [m["content"] for m in payload["messages"]] == ["first", "second", "third"]
    assert payload["count"] == 3
    assert payload["channel_id"] == "222"


async def test_history_defaults_to_fifty_and_passes_before_through():
    seen = {}
    def handler(request):
        seen.update(dict(request.url.params))
        return httpx.Response(200, json=[])
    await _run_history(_history_actuator(handler),
                       {"channel_id": "222", "before": "999"})
    assert seen == {"limit": str(HISTORY_DEFAULT), "before": "999"}


async def test_history_omits_before_when_not_given():
    seen = {}
    def handler(request):
        seen.update(dict(request.url.params))
        return httpx.Response(200, json=[])
    await _run_history(_history_actuator(handler), {"channel_id": "222"})
    assert "before" not in seen


@pytest.mark.parametrize("asked,sent", [(500, HISTORY_MAX), (0, 1), (-5, 1), (10, 10)])
async def test_history_clamps_the_limit(asked, sent):
    seen = {}
    def handler(request):
        seen.update(dict(request.url.params))
        return httpx.Response(200, json=[])
    await _run_history(_history_actuator(handler),
                       {"channel_id": "222", "limit": asked})
    assert seen["limit"] == str(sent)


async def test_history_without_a_channel_id_is_a_reported_error():
    def handler(request):
        raise AssertionError("must not call Discord without a channel_id")
    name, payload = await _run_history(_history_actuator(handler), {})
    assert name == "discord.history.error"
    assert "channel_id" in payload["message"]


async def test_history_rejects_a_non_integer_limit_without_calling_discord():
    def handler(request):
        raise AssertionError("must not call Discord with a bad limit")
    name, payload = await _run_history(_history_actuator(handler),
                                       {"channel_id": "222", "limit": "fifty"})
    assert name == "discord.history.error"
    assert "limit" in payload["message"]


async def test_history_reports_a_permission_failure_rather_than_raising():
    # 403 is permanent: retrying cannot fix it and the agent should be told.
    def handler(request):
        return httpx.Response(403, json={"message": "Missing Access", "code": 50001})
    name, payload = await _run_history(_history_actuator(handler), {"channel_id": "222"})
    assert name == "discord.history.error"
    assert "Missing Access" in payload["message"]


async def test_history_raises_on_a_server_error_so_the_bus_retries():
    def handler(request):
        return httpx.Response(500, text="upstream boom")
    with pytest.raises(httpx.HTTPStatusError):
        await _run_history(_history_actuator(handler), {"channel_id": "222"})


async def test_history_survives_a_body_that_is_not_a_list():
    def handler(request):
        return httpx.Response(200, json={"unexpected": "shape"})
    name, payload = await _run_history(_history_actuator(handler), {"channel_id": "222"})
    assert name == "discord.history.error"


async def test_history_error_body_that_is_not_an_object_still_reports():
    # The Phase 2 defect class: a non-object body must not raise AttributeError.
    def handler(request):
        return httpx.Response(403, json=["nope"])
    name, payload = await _run_history(_history_actuator(handler), {"channel_id": "222"})
    assert name == "discord.history.error"
    assert isinstance(payload["message"], str)


async def test_history_skips_entries_that_are_not_objects():
    def handler(request):
        return httpx.Response(200, json=["junk", _discord_message("1", "alice", "hi")])
    name, payload = await _run_history(_history_actuator(handler), {"channel_id": "222"})
    assert name == "discord.history.ok"
    assert payload["count"] == 1


def test_history_declares_a_tool_spec_with_before_and_limit():
    props = DiscordHistory.tool_spec["input_schema"]["properties"]
    assert set(props) >= {"channel_id", "limit", "before"}
    assert DiscordHistory.tool_spec["input_schema"]["required"] == ["channel_id"]
    assert DiscordHistory.name == "discord.history"


async def test_history_stringifies_an_integer_message_id():
    # msgpack round-trips large ints lossily; ids must always come back as str.
    def handler(request):
        return httpx.Response(200, json=[{"id": 123456789012345678,
                                          "author": {"username": "alice", "bot": False},
                                          "content": "hi"}])
    name, payload = await _run_history(_history_actuator(handler), {"channel_id": "222"})
    assert name == "discord.history.ok"
    assert payload["messages"][0]["id"] == "123456789012345678"
    assert isinstance(payload["messages"][0]["id"], str)


async def test_history_rejects_a_non_string_before_without_calling_discord():
    def handler(request):
        raise AssertionError("must not call Discord with a bad before")
    name, payload = await _run_history(_history_actuator(handler),
                                       {"channel_id": "222", "before": 999})
    assert name == "discord.history.error"
    assert "before" in payload["message"]


async def test_history_rejects_a_bool_limit_without_calling_discord():
    # bool is an int subclass; an unguarded clamp would silently turn True into 1.
    def handler(request):
        raise AssertionError("must not call Discord with a bool limit")
    name, payload = await _run_history(_history_actuator(handler),
                                       {"channel_id": "222", "limit": True})
    assert name == "discord.history.error"
    assert "limit" in payload["message"]


async def test_discord_post_reply_reference_when_asked():
    seen = {}
    def h(req):
        seen["body"] = json.loads(req.content)
        return httpx.Response(200, json={"id": "m-9"})
    a = _bind(DiscordPost("bot", "app", channel_id="chan-9", client=_client(h)))
    results = []
    ctx = ActCtx(cmd=_cmd("discord.post",
                          {"content": "answering you", "reply_to_message_id": "src-42"}),
                 _emit_result=await _recorder(results))
    await a.act(ctx.cmd, ctx)
    assert seen["body"]["message_reference"] == {
        "message_id": "src-42", "fail_if_not_exists": False}
    assert results[0][0] == "discord.post.ok"


async def test_discord_post_has_no_reference_when_not_replying():
    seen = {}
    def h(req):
        seen["body"] = json.loads(req.content)
        return httpx.Response(200, json={"id": "m-1"})
    a = _bind(DiscordPost("bot", "app", channel_id="chan-9", client=_client(h)))
    ctx = ActCtx(cmd=_cmd("discord.post", {"content": "hi"}),
                 _emit_result=await _recorder([]))
    await a.act(ctx.cmd, ctx)
    assert "message_reference" not in seen["body"]


def test_reply_to_is_in_the_tool_spec():
    props = DiscordPost.tool_spec["input_schema"]["properties"]
    assert "reply_to_message_id" in props


# --- discord.react -----------------------------------------------------------

from switchboard.actuators.discord import DiscordReact


def _react_actuator(handler):
    a = DiscordReact("bot", "app", client=_client(handler))
    a.bind(ActuatorCtx(store=MemoryStore()))
    return a


async def _run_react(act, args):
    results = []
    async def emit(name, payload, cid):
        results.append((name, payload)); return 0
    cmd = _cmd("discord.react", args)
    await act.act(cmd, ActCtx(cmd=cmd, _emit_result=emit))
    return results[0]


async def test_react_puts_the_reaction_and_reports_ok():
    seen = {}
    def h(req):
        seen["method"] = req.method
        seen["url"] = str(req.url)
        seen["auth"] = req.headers.get("authorization")
        return httpx.Response(204)          # Discord's success is 204 No Content
    name, payload = await _run_react(
        _react_actuator(h),
        {"channel_id": "222", "message_id": "42", "emoji": "\U0001f44d"})
    assert seen["method"] == "PUT"
    assert seen["auth"] == "Bot bot"
    # emoji URL-encoded into the path, /@me at the end
    assert "/channels/222/messages/42/reactions/" in seen["url"]
    assert seen["url"].endswith("/@me")
    assert name == "discord.react.ok"
    assert payload == {"reacted_by": "you", "channel_id": "222", "message_id": "42", "emoji": "\U0001f44d"}


async def test_react_missing_field_is_a_reported_error_without_calling_discord():
    def h(req):
        raise AssertionError("must not call Discord without all fields")
    for args in ({"message_id": "42", "emoji": "x"},
                 {"channel_id": "222", "emoji": "x"},
                 {"channel_id": "222", "message_id": "42"}):
        name, payload = await _run_react(_react_actuator(h), args)
        assert name == "discord.react.error"


async def test_react_403_is_reported_not_raised():
    # Missing the Add Reactions permission is permanent; the agent should learn.
    def h(req):
        return httpx.Response(403, json={"message": "Missing Permissions", "code": 50013})
    name, payload = await _run_react(
        _react_actuator(h), {"channel_id": "222", "message_id": "42", "emoji": "x"})
    assert name == "discord.react.error"
    assert "Missing Permissions" in payload["message"]


async def test_react_5xx_raises_so_the_bus_retries():
    def h(req):
        return httpx.Response(502, text="bad gateway")
    with pytest.raises(httpx.HTTPStatusError):
        await _run_react(_react_actuator(h),
                         {"channel_id": "222", "message_id": "42", "emoji": "x"})


def test_react_tool_spec_requires_all_three_fields():
    req = DiscordReact.tool_spec["input_schema"]["required"]
    assert set(req) == {"channel_id", "message_id", "emoji"}
    assert DiscordReact.name == "discord.react"
