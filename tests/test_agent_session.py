from switchboard.deciders.agent.session import Sessions
from switchboard.store import MemoryStore


def _sessions():
    return Sessions(MemoryStore())


async def test_new_session_has_the_documented_shape():
    s = await _sessions().new(sid=100, source="discord", channel_id="222",
                              thread_id="222", anchor="1234567890")
    # `last_seen` is stamped at mint, so it is checked by type and then removed
    # rather than pinned to a value. It is what an IDLE session's expiry is
    # measured against -- `busy_since` is None whenever the session is idle, so
    # it could never serve.
    assert isinstance(s.pop("last_seen"), float)
    assert s == {"sid": 100, "source": "discord", "channel_id": "222",
                 "thread_id": "222", "anchor": "1234567890",
                 "state": "idle", "turn": 0, "busy_since": None,
                 "messages": [], "buffer": [], "gather": None}


async def test_new_session_is_persisted_and_round_trips():
    sess = _sessions()
    await sess.new(sid=100, source="discord", channel_id="222",
                   thread_id="222", anchor="1")
    assert (await sess.load(100))["sid"] == 100


async def test_load_of_an_unknown_session_is_none():
    assert await _sessions().load(999) is None


async def test_save_round_trips_nested_structure():
    sess = _sessions()
    s = await sess.new(sid=1, source="discord", channel_id="c",
                       thread_id=None, anchor="a")
    s["messages"] = [{"role": "user", "content": [{"type": "text", "text": "hi"}]}]
    s["gather"] = {"order": ["toolu_A"], "remaining": 1, "results": {}}
    await sess.save(s)
    back = await sess.load(1)
    assert back["messages"][0]["content"][0]["text"] == "hi"
    assert back["gather"]["order"] == ["toolu_A"]


async def test_route_round_trips_as_an_int():
    sess = _sessions()
    await sess.set_route("discord", "222", 100)
    assert await sess.route("discord", "222") == 100


async def test_route_is_none_when_unknown():
    assert await _sessions().route("discord", "nope") is None


async def test_routes_are_namespaced_by_source():
    # A Discord id and some future source's id must never collide.
    sess = _sessions()
    await sess.set_route("discord", "222", 1)
    await sess.set_route("slack", "222", 2)
    assert await sess.route("discord", "222") == 1
    assert await sess.route("slack", "222") == 2


async def test_pending_is_read_and_deleted_in_one_go():
    sess = _sessions()
    await sess.put_pending(7, {"kind": "llm", "sid": 100})
    assert await sess.take_pending(7) == {"kind": "llm", "sid": 100}
    # The second take must be None: a redelivered result must not be
    # processed twice, and take_pending is the guard that makes it so.
    assert await sess.take_pending(7) is None


async def test_take_pending_of_an_unknown_command_is_none():
    assert await _sessions().take_pending(999) is None


async def test_clear_pending_drops_only_the_given_sessions_entries():
    sess = _sessions()
    await sess.put_pending(1, {"kind": "llm", "sid": 100})
    await sess.put_pending(2, {"kind": "tool", "sid": 100, "tool_use_id": "A"})
    await sess.put_pending(3, {"kind": "tool", "sid": 200, "tool_use_id": "B"})
    assert await sess.clear_pending(100) == 2
    assert await sess.take_pending(1) is None
    assert await sess.take_pending(2) is None
    assert await sess.take_pending(3) is not None


async def test_clear_pending_on_a_session_with_none_is_harmless():
    sess = _sessions()
    await sess.put_pending(1, {"kind": "llm", "sid": 100})
    assert await sess.clear_pending(999) == 0
    assert await sess.take_pending(1) is not None


async def test_clear_pending_survives_a_corrupt_entry():
    """It scans every pending key, including ones it did not write. A single
    unparseable value must not take the watchdog's repair path down with it."""
    store = _TtlStore()
    sess = Sessions(store)
    await store.set("pending:1", "{not json")
    await sess.put_pending(2, {"kind": "llm", "sid": 100})
    assert await sess.clear_pending(100) == 1
    assert await store.get("pending:1") == "{not json"


class _TtlStore:
    """Records the ttl every key was last written with."""
    def __init__(self):
        self._v = {}
        self.ttls = {}
    async def get(self, key): return self._v.get(key)
    async def set(self, key, value, *, ttl=None):
        self._v[key] = value; self.ttls[key] = ttl
    async def delete(self, key): self._v.pop(key, None); self.ttls.pop(key, None)
    async def keys(self, prefix=""):
        return [k for k in self._v if k.startswith(prefix)]


async def test_save_refreshes_the_ttl_on_both_the_session_and_its_route():
    """The route is written once at mint but the session every turn. Without
    refreshing both, an ACTIVE conversation loses its route after one TTL and
    the next message cannot find the session."""
    store = _TtlStore()
    sess = Sessions(store, ttl=3600.0)
    s = await sess.new(sid=100, source="discord", channel_id="222",
                       thread_id="222", anchor="1")
    await sess.set_route("discord", "222", 100)
    store.ttls.clear()
    await sess.save(s)
    assert store.ttls["session:100"] == 3600.0
    assert store.ttls["thread:discord:222"] == 3600.0


async def test_a_pending_entry_carries_the_session_ttl():
    """Sessions expire; a pending entry whose result never arrives must not
    outlive the session it points at. Without a TTL it sits in sqlite forever —
    harmless (load() returns None) but permanent, and it accumulates."""
    store = _TtlStore()
    sess = Sessions(store, ttl=3600.0)
    await sess.put_pending(7, {"kind": "llm", "sid": 100})
    assert store.ttls["pending:7"] == 3600.0


async def test_a_pending_entry_stays_immortal_when_sessions_do():
    store = _TtlStore()
    await Sessions(store).put_pending(7, {"kind": "llm", "sid": 100})
    assert store.ttls["pending:7"] is None


async def test_delete_removes_both_keys():
    store = _TtlStore()
    sess = Sessions(store, ttl=3600.0)
    s = await sess.new(sid=100, source="discord", channel_id="222",
                       thread_id="222", anchor="1")
    await sess.set_route("discord", "222", 100)
    await sess.delete(s)
    assert await sess.load(100) is None
    assert await sess.route("discord", "222") is None
