import pytest

from switchboard.actuators.kv import KvActuator
from switchboard.message import ActCtx, ActuatorCtx, Command
from switchboard.store import MemoryStore, ScopedStore


def _cmd(args):
    class M:
        id = 1
        payload = args
        metadata = {"name": "kv", "observation_id": 7}
    return Command.from_message(M())


async def _run(act, args):
    """Drive one command through the actuator, returning the result it emitted."""
    results = []
    async def emit_result(name, payload, cmd_id):
        results.append((name, payload)); return 0
    cmd = _cmd(args)
    await act.act(cmd, ActCtx(cmd=cmd, _emit_result=emit_result))
    return results[0]


def _bound(store=None):
    a = KvActuator()
    a.bind(ActuatorCtx(store=store or MemoryStore()))
    return a


async def test_set_then_get_round_trips():
    a = _bound()
    assert await _run(a, {"op": "set", "key": "draft", "value": "hello"}) == ("kv.ok", {})
    assert await _run(a, {"op": "get", "key": "draft"}) == ("kv.ok", {"value": "hello"})


async def test_get_missing_key_returns_null_value():
    a = _bound()
    assert await _run(a, {"op": "get", "key": "nope"}) == ("kv.ok", {"value": None})


async def test_delete_removes():
    a = _bound()
    await _run(a, {"op": "set", "key": "k", "value": "v"})
    await _run(a, {"op": "delete", "key": "k"})
    assert await _run(a, {"op": "get", "key": "k"}) == ("kv.ok", {"value": None})


async def test_set_honours_ttl():
    clock = type("C", (), {"t": 1000.0, "__call__": lambda self: self.t})()
    a = _bound(MemoryStore(time_fn=clock))
    await _run(a, {"op": "set", "key": "k", "value": "v", "ttl": 60.0})
    clock.t += 61.0
    assert await _run(a, {"op": "get", "key": "k"}) == ("kv.ok", {"value": None})


async def test_unknown_op_is_an_error_result_not_a_raise():
    """A bad op is a failure the actuator understands, so it reports rather than
    raising — raising would burn the whole retry cycle first."""
    a = _bound()
    name, payload = await _run(a, {"op": "obliterate", "key": "k"})
    assert name == "kv.error"
    assert "obliterate" in payload["message"]


async def test_non_string_value_is_an_error_result():
    a = _bound()
    name, payload = await _run(a, {"op": "set", "key": "k", "value": 5})
    assert name == "kv.error"


async def test_kv_is_one_actuator_with_one_scope():
    """Two actuators (kv.get / kv.set) would get two scopes and never see each
    other's writes. One actuator, one scope, dispatched on op."""
    assert KvActuator.name == "kv"


async def test_kv_declares_no_tool_spec():
    """The agent reaches kv only through decider-injected memory tools, never
    directly — so it must not advertise itself as a tool."""
    assert getattr(KvActuator, "tool_spec", None) is None


async def test_list_returns_keys_under_a_prefix():
    a = _bound()
    await _run(a, {"op": "set", "key": "note:a", "value": "1"})
    await _run(a, {"op": "set", "key": "note:b", "value": "2"})
    await _run(a, {"op": "set", "key": "other", "value": "3"})
    name, payload = await _run(a, {"op": "list", "prefix": "note:"})
    assert name == "kv.ok"
    assert sorted(payload["keys"]) == ["note:a", "note:b"]
    assert payload["truncated"] is False


async def test_list_is_capped_and_reports_truncation():
    """An agent listing a large memory would blow its own context and the token
    bill. The cap is the actuator's policy, not the store's."""
    from switchboard.actuators.kv import LIST_MAX
    a = _bound()
    for i in range(LIST_MAX + 10):
        await _run(a, {"op": "set", "key": f"k{i:04d}", "value": "v"})
    name, payload = await _run(a, {"op": "list", "prefix": "k"})
    assert len(payload["keys"]) == LIST_MAX
    assert payload["truncated"] is True


async def test_writes_land_in_the_actuators_own_scope():
    store = MemoryStore()
    a = KvActuator()
    a.bind(ActuatorCtx(store=ScopedStore(store, "actuator/kv/")))
    await _run(a, {"op": "set", "key": "draft", "value": "hello"})
    assert await store.get("actuator/kv/draft") == "hello"
