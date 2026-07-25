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


# --- delete_prefix: the drain a decider emits when a session ends -------------
#
# Sharp enough that every guard below is load-bearing: an empty prefix would
# take the whole store with it, and the store is a primitive that will do
# exactly what it is told. The policy therefore lives here, in the actuator,
# not at the call site — the same argument that put LIST_MAX here.

async def _seed(a, *keys):
    for k in keys:
        await _run(a, {"op": "set", "key": k, "value": "v"})


async def test_delete_prefix_removes_every_key_under_it():
    a = _bound()
    await _seed(a, "session:7:draft", "session:7:plan")
    name, payload = await _run(a, {"op": "delete_prefix", "prefix": "session:7:"})
    assert name == "kv.ok"
    assert payload["removed"] == 2
    assert payload["truncated"] is False
    assert await _run(a, {"op": "list", "prefix": "session:7:"}) == (
        "kv.ok", {"keys": [], "truncated": False})


async def test_delete_prefix_leaves_everything_outside_it_alone():
    a = _bound()
    await _seed(a, "session:7:draft", "session:70:draft", "global:prefs")
    await _run(a, {"op": "delete_prefix", "prefix": "session:7:"})
    _, payload = await _run(a, {"op": "list", "prefix": ""})
    assert sorted(payload["keys"]) == ["global:prefs", "session:70:draft"]


async def test_delete_prefix_with_no_prefix_is_rejected_and_deletes_nothing():
    """The one that matters: a missing prefix must not be read as "" and take
    the entire store with it."""
    a = _bound()
    await _seed(a, "global:prefs", "session:7:draft")
    name, payload = await _run(a, {"op": "delete_prefix"})
    assert name == "kv.error"
    _, listed = await _run(a, {"op": "list", "prefix": ""})
    assert sorted(listed["keys"]) == ["global:prefs", "session:7:draft"]


async def test_delete_prefix_with_an_empty_prefix_is_rejected_and_deletes_nothing():
    a = _bound()
    await _seed(a, "global:prefs", "session:7:draft")
    name, _ = await _run(a, {"op": "delete_prefix", "prefix": ""})
    assert name == "kv.error"
    _, listed = await _run(a, {"op": "list", "prefix": ""})
    assert len(listed["keys"]) == 2


async def test_delete_prefix_with_a_whitespace_prefix_is_rejected_and_deletes_nothing():
    a = _bound()
    await _seed(a, "global:prefs", "session:7:draft")
    for blank in ["   ", "\t", "\n"]:
        name, _ = await _run(a, {"op": "delete_prefix", "prefix": blank})
        assert name == "kv.error"
    _, listed = await _run(a, {"op": "list", "prefix": ""})
    assert len(listed["keys"]) == 2


async def test_delete_prefix_with_a_non_string_prefix_is_rejected_and_deletes_nothing():
    a = _bound()
    await _seed(a, "global:prefs", "session:7:draft")
    for hostile in [None, 0, [], {}, True]:
        name, _ = await _run(a, {"op": "delete_prefix", "prefix": hostile})
        assert name == "kv.error"
    _, listed = await _run(a, {"op": "list", "prefix": ""})
    assert len(listed["keys"]) == 2


async def test_delete_prefix_is_capped_and_reports_truncation():
    """Bounded work per command, the same policy shape as LIST_MAX. The
    remainder is reported rather than silently dropped."""
    from switchboard.actuators.kv import DELETE_PREFIX_MAX
    a = _bound()
    await _seed(a, *[f"session:7:k{i:05d}" for i in range(DELETE_PREFIX_MAX + 5)])
    _, payload = await _run(a, {"op": "delete_prefix", "prefix": "session:7:"})
    assert payload["removed"] == DELETE_PREFIX_MAX
    assert payload["truncated"] is True
    _, listed = await _run(a, {"op": "list", "prefix": "session:7:"})
    assert len(listed["keys"]) == 5


async def test_delete_prefix_on_an_already_drained_prefix_is_a_quiet_no_op():
    """At-least-once: the same drain command can be delivered twice. The second
    must be an ordinary ok, not an error."""
    a = _bound()
    await _seed(a, "session:7:draft")
    await _run(a, {"op": "delete_prefix", "prefix": "session:7:"})
    assert await _run(a, {"op": "delete_prefix", "prefix": "session:7:"}) == (
        "kv.ok", {"removed": 0, "truncated": False})


def test_delete_prefix_is_not_in_the_model_facing_op_list():
    """OPS is what the two memory tools advertise to the model. A drain is the
    decider's to emit when it ends a session, never the model's to call: an
    op that empties a namespace has no business in a hallucinatable enum."""
    from switchboard.actuators.kv import OPS
    assert "delete_prefix" not in OPS
