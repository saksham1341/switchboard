"""The whole loop through a real Bus: a mention becomes a reply command.

Only the two world-facing edges are faked (the Anthropic HTTP call and the
Discord HTTP call). Everything between them is the real substrate.
"""
import asyncio
import json

import pytest

from switchboard.bus import Bus
from switchboard.deciders.agent import AgentDecider
from switchboard.store import MemoryStore

TOOL = {"name": "discord.post", "description": "post a message",
        "input_schema": {"type": "object",
                         "properties": {"content": {"type": "string"}},
                         "required": ["content"]}}


class _FakeLlm:
    """Returns a tool_use on the first call, then end_turn."""
    name = "llm"
    tool_spec = None

    def __init__(self):
        self.calls = []

    def bind(self, ctx):
        self.ctx = ctx

    async def act(self, cmd, ctx):
        self.calls.append(cmd.args)
        if len(self.calls) == 1:
            return await ctx.result("ok", {
                "stop_reason": "tool_use",
                "content": [{"type": "tool_use", "id": "toolu_A",
                             "name": "discord.post",
                             "input": {"content": "hello there"}}],
                "usage": {"input_tokens": 1, "output_tokens": 1}})
        await ctx.result("ok", {"stop_reason": "end_turn",
                                "content": [{"type": "text", "text": "done"}],
                                "usage": {}})


class _FakePost:
    name = "discord.post"
    tool_spec = TOOL

    def __init__(self):
        self.posted = []

    def bind(self, ctx):
        self.ctx = ctx

    async def act(self, cmd, ctx):
        self.posted.append(cmd.args)
        await ctx.result("ok", {"message_id": "999"})


async def test_a_mention_produces_a_reply_through_the_real_bus(tmp_path):
    llm, post = _FakeLlm(), _FakePost()
    bus = Bus(str(tmp_path / "mm.db"), store=MemoryStore(),
              consumer_wait_ms=50, lease_reaper_interval_s=3600.0)
    bus.add_decider(AgentDecider(tools=[TOOL], model="test-model", stuck_after=1800.0))
    bus.add_actuator(llm)
    bus.add_actuator(post)
    await bus.start()
    try:
        await bus.emit_observation("discord.message", {
            "message_id": "1", "channel_id": "222", "thread_id": "222",
            "user_id": "123", "user_name": "alice#0001",
            "content": "hey @switchboard say hello",
            "mentions": ["555"], "mentions_bot": True,
            "thread": {"is_thread": True, "message_count": 1}},
            emitted_by="sensor/discord")

        for _ in range(100):                       # let the loop turn
            if post.posted:
                break
            await asyncio.sleep(0.05)

        assert post.posted, "the agent never reached discord.post"
        assert post.posted[0]["content"] == "hello there"
        # Second llm call carries the tool_result turn back to the model.
        for _ in range(100):
            if len(llm.calls) >= 2:
                break
            await asyncio.sleep(0.05)
        assert len(llm.calls) >= 2
        last = llm.calls[-1]["messages"][-1]
        assert last["role"] == "user"
        assert last["content"][0]["tool_use_id"] == "toolu_A"
    finally:
        await bus.stop()


async def test_a_producers_rendered_text_reaches_the_model(tmp_path):
    llm, post = _FakeLlm(), _FakePost()
    bus = Bus(str(tmp_path / "mm.db"), store=MemoryStore(),
              consumer_wait_ms=50, lease_reaper_interval_s=3600.0)
    bus.add_decider(AgentDecider(tools=[TOOL], model="test-model", stuck_after=1800.0))
    bus.add_actuator(llm)
    bus.add_actuator(post)
    await bus.start()
    try:
        await bus.emit_observation(
            "discord.message",
            {"message_id": "1", "channel_id": "222", "thread_id": "222",
             "user_id": "669", "user_name": "alice", "content": "hi",
             "mentions_bot": True,
             "thread": {"is_thread": True, "message_count": 1}},
            emitted_by="sensor/discord",
            text="[discord.message] channel_id=222 user_id=669\n"
                 "<untrusted>\nhi\n</untrusted>")

        for _ in range(100):
            if llm.calls:
                break
            await asyncio.sleep(0.05)

        assert llm.calls, "the agent never called the model"
        sent = json.dumps(llm.calls[0]["messages"])
        assert "<untrusted>" in sent          # the producer's rendering, verbatim
        assert '"channel_id": "222"' not in sent   # not the raw payload JSON
    finally:
        await bus.stop()
