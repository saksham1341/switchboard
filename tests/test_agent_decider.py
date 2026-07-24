import json

import pytest

from switchboard.deciders.agent import AgentDecider
from switchboard.deciders.agent.session import Sessions
from switchboard.message import DecideCtx, DeciderCtx, Observation
from switchboard.store import MemoryStore

TOOL = {"name": "discord.post", "description": "post",
        "input_schema": {"type": "object", "properties": {}}}


def _obs(name, payload, *, oid=100, command_id=None):
    class M:
        id = oid
        metadata = {"name": name, "command_id": command_id}
    m = M()
    m.payload = payload
    return Observation.from_message(m)


def _agent(**kw):
    a = AgentDecider(tools=[TOOL], **kw)
    a.bind(DeciderCtx(store=MemoryStore()))
    return a


_next_command_id = [500]    # module-level: a real Bus hands out globally unique,
                            # monotonic command ids across the process's whole
                            # lifetime, never just within one decide() call. A
                            # counter reset per _Recorder would let two separate
                            # _deliver() calls mint the same numeric id, which is
                            # a scenario the real system cannot produce.


class _Recorder:
    """Captures the commands a decide() call emits."""
    def __init__(self, obs):
        self.emitted = []
        self.ctx = DecideCtx(obs=obs, _emit_command=self._emit)

    async def _emit(self, name, args, observation_id):
        _next_command_id[0] += 1
        cid = _next_command_id[0]
        self.emitted.append((name, args, cid))
        return cid


def _message(content="hello", *, mentions_bot=True, mid="1", thread="222",
             channel="111"):
    # channel and thread are deliberately distinct ids: a test that hardcodes
    # both to the same value can pass even when the routing key comes from
    # the wrong field, since the key would compute identically either way.
    return {"message_id": mid, "channel_id": channel, "thread_id": thread,
            "parent_id": None, "guild_id": "9", "user_id": "123",
            "user_name": "alice#0001", "content": content,
            "mentions": ["555"] if mentions_bot else [],
            "mentions_bot": mentions_bot,
            "thread": {"is_thread": bool(thread), "message_count": 3}}


async def _deliver(agent, obs):
    rec = _Recorder(obs)
    await agent.decide(obs, rec.ctx)
    return rec


class _KeyRecorder:
    """Wraps a KeyStore and records every key written, in order. Used to pin
    the relative order of two writes without caring about their values."""

    def __init__(self, store):
        self._store = store
        self.writes: list[str] = []

    async def get(self, key):
        return await self._store.get(key)

    async def set(self, key, value, *, ttl=None):
        self.writes.append(key)
        await self._store.set(key, value, ttl=ttl)

    async def delete(self, key):
        await self._store.delete(key)

    async def keys(self, prefix=""):
        return await self._store.keys(prefix)


# --- subscribes: coarse, sync, cannot touch the store ------------------------

def test_subscribes_to_discord_messages():
    assert _agent().subscribes(_obs("discord.message", {}))


def test_subscribes_to_anything_carrying_a_command_id():
    assert _agent().subscribes(_obs("llm.ok", {}, command_id=501))


def test_ignores_an_unrelated_observation():
    assert not _agent().subscribes(_obs("github.pr.opened", {}))


def test_subscribes_to_deadletter():
    assert _agent().subscribes(_obs("switchboard.deadletter", {"message_id": 501}))


# --- on_message --------------------------------------------------------------

async def test_a_mention_mints_a_session_and_emits_llm():
    a = _agent()
    rec = await _deliver(a, _obs("discord.message", _message()))
    assert [name for name, _, _ in rec.emitted] == ["llm"]
    assert await a._sessions.load(100) is not None


async def test_a_non_mention_with_no_session_is_ignored_entirely():
    a = _agent()
    rec = await _deliver(a, _obs("discord.message", _message(mentions_bot=False)))
    assert rec.emitted == []
    assert await a._sessions.load(100) is None


async def test_a_non_mention_in_a_live_thread_is_buffered_without_advancing():
    a = _agent()
    await _deliver(a, _obs("discord.message", _message()))          # mint, busy
    rec = await _deliver(a, _obs("discord.message",
                                 _message(content="context", mentions_bot=False),
                                 oid=101))
    assert rec.emitted == []                                        # no second llm
    s = await a._sessions.load(100)
    assert len(s["buffer"]) == 1 and s["buffer"][0]["is_mention"] is False


async def test_a_mention_while_busy_is_buffered_not_advanced():
    a = _agent()
    await _deliver(a, _obs("discord.message", _message()))
    rec = await _deliver(a, _obs("discord.message", _message(mid="2"), oid=101))
    assert rec.emitted == []
    s = await a._sessions.load(100)
    assert s["buffer"][0]["is_mention"] is True


async def test_the_session_is_busy_after_advancing():
    a = _agent()
    await _deliver(a, _obs("discord.message", _message()))
    assert (await a._sessions.load(100))["state"] == "busy"


async def test_advance_flushes_the_whole_buffer_into_one_user_turn():
    a = _agent()
    await _deliver(a, _obs("discord.message", _message(content="first")))
    s = await a._sessions.load(100)
    assert len(s["messages"]) == 1
    assert s["messages"][0]["role"] == "user"
    assert s["buffer"] == []


async def test_the_llm_command_carries_system_messages_and_tools():
    a = _agent()
    rec = await _deliver(a, _obs("discord.message", _message()))
    _, args, _ = rec.emitted[0]
    assert args["messages"] and isinstance(args["system"], str)
    assert TOOL in args["tools"]


async def test_a_pending_entry_is_recorded_for_the_llm_command():
    a = _agent()
    rec = await _deliver(a, _obs("discord.message", _message()))
    _, _, cid = rec.emitted[0]
    assert await a._sessions.take_pending(cid) == {"kind": "llm", "sid": 100}


async def test_a_threaded_message_routes_on_the_thread_id():
    a = _agent()
    await _deliver(a, _obs("discord.message", _message()))          # thread="222", channel="111"
    assert await a._sessions.route("discord", "222") == 100
    assert await a._sessions.route("discord", "111") is None        # not keyed on channel_id


async def test_a_plain_channel_message_routes_on_the_channel_id():
    a = _agent()
    await _deliver(a, _obs("discord.message", _message(thread=None)))
    assert await a._sessions.route("discord", "111") == 100
    assert await a._sessions.route("discord", "222") is None        # not keyed on thread_id


async def test_a_result_for_an_unknown_command_is_ignored():
    a = _agent()
    rec = await _deliver(a, _obs("llm.ok", {"content": []}, command_id=999))
    assert rec.emitted == []


# --- write ordering: session before pending (FIX 1) --------------------------

async def test_advance_saves_the_session_before_recording_the_pending_entry():
    """Pins the write order inside _advance. The store has no transactions, so
    a crash between the two writes is always possible; saving the session
    first means the worst case is a recoverable stuck-busy session, never a
    pending entry pointing at a stale, un-flushed one. A future refactor that
    reverts the order should fail this test, not surface as transcript
    corruption in production."""
    a = _agent()
    recorder = _KeyRecorder(a.ctx.store)
    a._sessions = Sessions(recorder)
    await _deliver(a, _obs("discord.message", _message()))

    session_writes = [i for i, k in enumerate(recorder.writes) if k.startswith("session:")]
    pending_writes = [i for i, k in enumerate(recorder.writes) if k.startswith("pending:")]
    assert session_writes and pending_writes
    assert max(session_writes) < min(pending_writes)


# --- idempotent buffering under at-least-once redelivery (FIX 2) -------------

async def test_redelivering_the_same_message_while_busy_buffers_it_once():
    """decide() can be re-entered with the same observation after bus.py's
    _consume redelivers a message whose handler previously raised. Buffering
    the same message_id twice must not duplicate it into one turn."""
    a = _agent()
    await _deliver(a, _obs("discord.message", _message()))          # mint, busy
    dup = _obs("discord.message",
               _message(content="context", mentions_bot=False, mid="2"), oid=101)
    await _deliver(a, dup)
    await _deliver(a, dup)                                          # redelivery

    s = await a._sessions.load(100)
    assert len(s["buffer"]) == 1
    assert s["buffer"][0]["message_id"] == "2"


# --- on_response, gather, finish (Task 4) -------------------------------------

def _llm_ok(blocks, oid=200, command_id=501):
    return _obs("llm.ok", {"stop_reason": "tool_use", "content": blocks,
                           "usage": {"input_tokens": 10, "output_tokens": 5}},
                oid=oid, command_id=command_id)


def _use(tid, name="discord.post", args=None):
    return {"type": "tool_use", "id": tid, "name": name, "input": args or {}}


async def _mint(a):
    """Mint a session and return the llm command id it emitted."""
    rec = await _deliver(a, _obs("discord.message", _message()))
    return rec.emitted[0][2]


async def test_a_tool_use_becomes_a_command():
    a = _agent()
    cid = await _mint(a)
    rec = await _deliver(a, _llm_ok([_use("toolu_A")], command_id=cid))
    assert [n for n, _, _ in rec.emitted] == ["discord.post"]


async def test_the_tool_command_carries_the_models_input_verbatim():
    a = _agent()
    cid = await _mint(a)
    rec = await _deliver(a, _llm_ok(
        [_use("toolu_A", args={"content": "hi", "channel_id": "222"})], command_id=cid))
    assert rec.emitted[0][1] == {"content": "hi", "channel_id": "222"}


async def test_a_response_with_no_tool_use_finishes_the_session():
    a = _agent()
    cid = await _mint(a)
    rec = await _deliver(a, _obs("llm.ok",
                                 {"stop_reason": "end_turn",
                                  "content": [{"type": "text", "text": "hello!"}]},
                                 oid=200, command_id=cid))
    assert rec.emitted == []                       # text-blind: nothing delivered
    assert (await a._sessions.load(100))["state"] == "idle"


async def test_the_assistant_turn_is_appended_before_the_tool_commands():
    a = _agent()
    cid = await _mint(a)
    await _deliver(a, _llm_ok([_use("toolu_A")], command_id=cid))
    s = await a._sessions.load(100)
    assert s["messages"][-1]["role"] == "assistant"


async def test_a_single_tool_result_closes_the_gather_and_advances():
    a = _agent()
    cid = await _mint(a)
    rec1 = await _deliver(a, _llm_ok([_use("toolu_A")], command_id=cid))
    tool_cid = rec1.emitted[0][2]
    rec2 = await _deliver(a, _obs("discord.post.ok", {"message_id": "9"},
                                  oid=300, command_id=tool_cid))
    assert [n for n, _, _ in rec2.emitted] == ["llm"]        # next turn fired
    s = await a._sessions.load(100)
    assert s["gather"] is None
    assert s["messages"][-1]["role"] == "user"               # the tool_result turn


async def test_two_tool_uses_wait_for_both_results():
    a = _agent()
    cid = await _mint(a)
    rec1 = await _deliver(a, _llm_ok([_use("toolu_A"), _use("toolu_B")], command_id=cid))
    a_cid, b_cid = rec1.emitted[0][2], rec1.emitted[1][2]
    rec2 = await _deliver(a, _obs("discord.post.ok", {}, oid=300, command_id=a_cid))
    assert rec2.emitted == []                                # still waiting on B
    rec3 = await _deliver(a, _obs("discord.post.ok", {}, oid=301, command_id=b_cid))
    assert [n for n, _, _ in rec3.emitted] == ["llm"]


async def test_tool_results_are_assembled_in_the_models_original_order():
    a = _agent()
    cid = await _mint(a)
    rec1 = await _deliver(a, _llm_ok([_use("toolu_A"), _use("toolu_B")], command_id=cid))
    a_cid, b_cid = rec1.emitted[0][2], rec1.emitted[1][2]
    await _deliver(a, _obs("discord.post.ok", {}, oid=300, command_id=b_cid))   # B first
    await _deliver(a, _obs("discord.post.ok", {}, oid=301, command_id=a_cid))
    s = await a._sessions.load(100)
    results = s["messages"][-1]["content"]
    assert [r["tool_use_id"] for r in results] == ["toolu_A", "toolu_B"]


async def test_a_tool_error_becomes_an_is_error_tool_result():
    a = _agent()
    cid = await _mint(a)
    rec1 = await _deliver(a, _llm_ok([_use("toolu_A")], command_id=cid))
    tool_cid = rec1.emitted[0][2]
    await _deliver(a, _obs("discord.post.error", {"message": "nope"},
                           oid=300, command_id=tool_cid))
    s = await a._sessions.load(100)
    block = s["messages"][-1]["content"][0]
    assert block["is_error"] is True and "nope" in block["content"]


async def test_a_dead_lettered_command_becomes_an_is_error_tool_result():
    # A dead command emits no result observation. The deadletter sensor is the
    # only signal, and it deliberately carries no command_id (a sensor cannot
    # forge a result), so correlation comes from the payload.
    a = _agent()
    cid = await _mint(a)
    rec1 = await _deliver(a, _llm_ok([_use("toolu_A")], command_id=cid))
    tool_cid = rec1.emitted[0][2]
    rec2 = await _deliver(a, _obs("switchboard.deadletter",
                                  {"message_id": tool_cid, "log": "cmd"}, oid=300))
    assert [n for n, _, _ in rec2.emitted] == ["llm"]
    s = await a._sessions.load(100)
    assert s["messages"][-1]["content"][0]["is_error"] is True


async def test_a_mention_that_landed_while_busy_advances_at_finish():
    a = _agent()
    cid = await _mint(a)
    await _deliver(a, _obs("discord.message", _message(mid="2"), oid=101))  # buffered
    rec = await _deliver(a, _obs("llm.ok",
                                 {"stop_reason": "end_turn", "content": []},
                                 oid=200, command_id=cid))
    assert [n for n, _, _ in rec.emitted] == ["llm"]         # drained the buffer


async def test_non_mention_context_alone_does_not_advance_at_finish():
    a = _agent()
    cid = await _mint(a)
    await _deliver(a, _obs("discord.message",
                           _message(mentions_bot=False), oid=101))
    rec = await _deliver(a, _obs("llm.ok",
                                 {"stop_reason": "end_turn", "content": []},
                                 oid=200, command_id=cid))
    assert rec.emitted == []
    s = await a._sessions.load(100)
    assert s["state"] == "idle" and len(s["buffer"]) == 1    # kept as context


async def test_an_llm_error_finishes_rather_than_looping():
    a = _agent()
    cid = await _mint(a)
    rec = await _deliver(a, _obs("llm.error", {"message": "overloaded"},
                                 oid=200, command_id=cid))
    assert rec.emitted == []
    assert (await a._sessions.load(100))["state"] == "idle"


async def test_a_redelivered_llm_result_is_a_no_op():
    a = _agent()
    cid = await _mint(a)
    await _deliver(a, _llm_ok([_use("toolu_A")], command_id=cid))
    rec = await _deliver(a, _llm_ok([_use("toolu_A")], command_id=cid))
    assert rec.emitted == []             # take_pending already consumed it
