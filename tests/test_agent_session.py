from switchboard.deciders.agent.session import Sessions
from switchboard.store import MemoryStore


def _sessions():
    return Sessions(MemoryStore())


async def test_new_session_has_the_documented_shape():
    s = await _sessions().new(sid=100, source="discord", channel_id="222",
                              thread_id="222", anchor="1234567890")
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
