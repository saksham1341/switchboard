import sqlite3

from mamamia.core.models import MessageState

from switchboard.sensors.deadletter import DeadLetterSensor
from switchboard.message import SensorCtx
from switchboard.store import MemoryStore
from switchboard.http import HttpServer
from switchboard.scheduler import Scheduler


def _db(tmp_path, rows):
    """A stand-in mamamia db with just the two tables the sweep reads."""
    import msgpack
    p = str(tmp_path / "mm.db")
    c = sqlite3.connect(p)
    c.execute("CREATE TABLE message_state (log_id TEXT, group_id TEXT, "
              "message_id INTEGER, state INTEGER)")
    c.execute("CREATE TABLE messages (log_id TEXT, id INTEGER, metadata BLOB)")
    for log, group, mid, name, state in rows:
        c.execute("INSERT INTO message_state VALUES (?,?,?,?)", (log, group, mid, state))
        c.execute("INSERT INTO messages VALUES (?,?,?)",
                  (log, mid, msgpack.packb({"name": name})))
    c.commit(); c.close()
    return p


def _bound(db_path, store=None):
    emitted = []
    async def emit(name, payload):
        emitted.append((name, payload)); return len(emitted)
    s = DeadLetterSensor(db_path)
    s.bind(SensorCtx(emit=emit, http=HttpServer(serve=False),
                     store=store or MemoryStore(),
                     schedule=Scheduler().for_owner("deadletter")))
    return s, emitted


async def test_first_sweep_baselines_without_emitting(tmp_path):
    """A fresh store must not replay history as if it just happened."""
    db = _db(tmp_path, [("cmd", "actuator/web_search", 42, "web_search",
                         MessageState.DEAD.value)])
    s, emitted = _bound(db)
    await s.sweep()
    assert emitted == []                       # baselined, not announced


async def test_new_dead_row_is_announced(tmp_path):
    db = _db(tmp_path, [])
    store = MemoryStore()
    s, emitted = _bound(db, store)
    await s.sweep()                            # baseline on an empty table
    c = sqlite3.connect(db)
    import msgpack
    c.execute("INSERT INTO message_state VALUES (?,?,?,?)",
              ("cmd", "actuator/web_search", 42, MessageState.DEAD.value))
    c.execute("INSERT INTO messages VALUES (?,?,?)",
              ("cmd", 42, msgpack.packb({"name": "web_search"})))
    c.commit(); c.close()
    await s.sweep()
    assert len(emitted) == 1
    name, payload = emitted[0]
    assert name == "switchboard.deadletter"
    assert payload["log"] == "cmd"
    assert payload["group"] == "actuator/web_search"
    assert payload["message_id"] == 42
    assert payload["name"] == "web_search"


async def test_a_row_is_announced_only_once(tmp_path):
    db = _db(tmp_path, [])
    store = MemoryStore()
    s, emitted = _bound(db, store)
    await s.sweep()
    import msgpack
    c = sqlite3.connect(db)
    c.execute("INSERT INTO message_state VALUES (?,?,?,?)",
              ("obs", "decider/notify", 7, MessageState.DEAD.value))
    c.execute("INSERT INTO messages VALUES (?,?,?)",
              ("obs", 7, msgpack.packb({"name": "github.pr.opened"})))
    c.commit(); c.close()
    await s.sweep()
    await s.sweep()                            # second pass must be silent
    assert len(emitted) == 1


async def test_observation_deaths_are_announced_too(tmp_path):
    """Deciders and taps dying is a health fact, same as a command dying."""
    db = _db(tmp_path, [])
    s, emitted = _bound(db)
    await s.sweep()
    import msgpack
    c = sqlite3.connect(db)
    c.execute("INSERT INTO message_state VALUES (?,?,?,?)",
              ("obs", "decider/notify", 7, MessageState.DEAD.value))
    c.execute("INSERT INTO messages VALUES (?,?,?)",
              ("obs", 7, msgpack.packb({"name": "github.pr.opened"})))
    c.commit(); c.close()
    await s.sweep()
    assert emitted[0][1]["log"] == "obs"


async def test_cascade_guard(tmp_path):
    """A consumer dying on switchboard.deadletter must not announce forever."""
    db = _db(tmp_path, [])
    s, emitted = _bound(db)
    await s.sweep()
    import msgpack
    c = sqlite3.connect(db)
    c.execute("INSERT INTO message_state VALUES (?,?,?,?)",
              ("obs", "decider/x", 9, MessageState.DEAD.value))
    c.execute("INSERT INTO messages VALUES (?,?,?)",
              ("obs", 9, msgpack.packb({"name": "switchboard.deadletter"})))
    c.commit(); c.close()
    await s.sweep()
    assert emitted == []                       # never announce our own kind
