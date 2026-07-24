# Agent Phase 1 — Framework Prerequisites Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the two substrate guarantees the agentic decider depends on — at-least-once dedup at the consume layer, and dead-letter visibility so a command that dies always becomes an observation.

**Architecture:** Task 1 lives in `switchboard/bus.py`, inside `_consume` (the single loop every consumer — decider, actuator, tap — runs through), adding a `(group_id, msg.id)` processed-guard backed by the Bus's own `KeyStore`. Task 2 is **not** Bus code: mamamia can mark a message DEAD without the Bus seeing it, so dead-letter announcement is a scheduled **sensor** that reads the DEAD table — the table is the source of truth, the core stays untouched, and the behaviour is opt-in.

**Tech Stack:** Python 3.12, asyncio, mamamia (`Outcome`), the existing `KeyStore`. pytest with `asyncio_mode = "auto"` (bare `async def test_*`, no decorator). Run tests with `./scripts/dev.sh test`.

**Spec:** `docs/superpowers/specs/2026-07-23-agentic-decider-design.md` §10 (framework prerequisites), §11 (at-least-once principle).

## Global Constraints

- Dedup is keyed on `(group_id, msg.id)`, stored in the Bus's own `self._store` (unscoped) under the key `f"_processed:{group_id}:{msg.id}"`, value `"1"`, with a TTL (default `3600.0`s).
- A message is marked processed **only on the SUCCESS path** — after `handle` returns, before `settle(SUCCESS)`. Never mark on RETRY, DEAD, or the not-kept path.
- There is **no `<cmd>.failed` observation.** `SensorCtx.emit` takes no `command_id` — a sensor cannot forge a result observation, and should not be able to. One signal only: `switchboard.deadletter`, payload `{log, group, message_id, name, reason}`. Consumers correlate from the payload.
- `switchboard.deadletter` is emitted for **every** dead-letter, both logs — a decider or tap dying is a health fact exactly as a command dying is.
- The sensor never announces a dead row whose own message name is `switchboard.deadletter` (cascade guard), and its first sweep baselines existing rows without emitting.
- `_consume`'s failure branches are **unchanged**: `PermanentError → DEAD`, otherwise `RETRY` with backoff. No Bus-owned retry cap. mamamia may dead-letter however it likes; the sweep observes the result.
- The dead-letter CLI (`switchboard/cli.py`, reads `message_state` DEAD rows) is untouched and keeps working.
- The `switchboard.dashboard` and every existing role are unchanged. This is additive platform work.

## File Structure

- **Modify:** `switchboard/bus.py` — the `_consume` loop, plus two helpers (`_already_processed`/`_mark_processed`) and one `__init__` field (`_processed_ttl`). Task 1 only.
- **Create:** `tests/test_bus_framework.py` — a fake-registry harness that drives `_consume` deterministically, plus the dedup tests.
- **Create:** `switchboard/sensors/deadletter.py` — the scheduled sweep. Reads mamamia's DEAD table read-only, same precedent as `switchboard/cli.py` and `switchboard/dashboard/stats.py`.
- **Create:** `tests/test_sensor_deadletter.py`.
- **Modify:** `switchboard/app.py` — register the sensor.

The two tasks are independent: Task 1 touches only the Bus, Task 2 only adds a sensor. Either could ship alone.

---

### Task 1: Consume-layer dedup

**Files:**
- Modify: `switchboard/bus.py` — `__init__` (add `_processed_ttl`), add `_already_processed`/`_mark_processed`, guard in `_consume`.
- Test: `tests/test_bus_framework.py` (create).

**Interfaces:**
- Consumes: `self._store` (a `KeyStore` with `async get(key)`/`async set(key, value, *, ttl=None)`), `Outcome` from mamamia, `Command.from_message` and `OBS_LOG`/`CMD_LOG` from `switchboard.message`.
- Produces: `Bus._already_processed(group_id, mid) -> bool`, `Bus._mark_processed(group_id, mid) -> None`, and a `_consume` that skips a `(group_id, msg.id)` it has already handled. Consumed by Task 2 (same `_consume` block).

- [ ] **Step 1: Write the failing test (shared harness + dedup)**

Create `tests/test_bus_framework.py`:

```python
from switchboard.bus import Bus
from switchboard.message import OBS_LOG, CMD_LOG, Command, Observation
from switchboard.errors import PermanentError
from mamamia.core.models import Outcome


class _Msg:
    """Minimal stand-in for a mamamia message."""
    def __init__(self, id, name):
        self.id = id
        self.payload = {}
        self.metadata = {"name": name}


class _SS:
    def __init__(self, n): self._n = n
    async def get_retry_count(self, log, group, mid): return self._n


class _Orch:
    def __init__(self, retry_count=0):
        self.settled = []
        self.state_store = _SS(retry_count)
    async def settle(self, log, group, mid, inst, *, outcome, retry_after=0.0):
        self.settled.append((mid, outcome))


class _Reg:
    """Hands _consume a fixed list of messages, then stops the loop."""
    def __init__(self, msgs, bus, orch):
        self._msgs = list(msgs); self._bus = bus; self._orch = orch
    def get_orchestrator(self, log): return self._orch
    async def acquire_blocking(self, log, group, inst, *, duration, wait_ms):
        if self._msgs:
            return self._msgs.pop(0)
        self._bus._running = False
        return None


def _drive(msgs, orch, *, retry_count=0, max_retries=10):
    """Build a Bus wired to a fake registry, ready for _consume."""
    bus = Bus(":memory:", max_retries=max_retries)
    bus._registry = _Reg(msgs, bus, orch)
    bus._running = True
    return bus


async def test_consume_handles_a_message_once():
    orch = _Orch()
    m = _Msg(5, "boom")
    bus = _drive([m, m], orch)          # SAME message delivered twice
    calls = []
    async def handle(v): calls.append(v.id)
    await bus._consume(CMD_LOG, "actuator/boom", Command.from_message, lambda v: True, handle)
    assert calls == [5]                 # handled once despite two deliveries
    assert orch.settled == [(5, Outcome.SUCCESS), (5, Outcome.SUCCESS)]  # both settled


async def test_dedup_is_per_group():
    orch = _Orch()
    m = _Msg(5, "boom")
    bus = _drive([m], orch)
    await bus._mark_processed("actuator/a", 5)
    assert await bus._already_processed("actuator/a", 5) is True
    assert await bus._already_processed("actuator/b", 5) is False   # different group, not seen


async def test_not_kept_message_is_not_marked():
    orch = _Orch()
    m = _Msg(5, "boom")
    bus = _drive([m], orch)
    await bus._consume(CMD_LOG, "actuator/boom", Command.from_message, lambda v: False, lambda v: None)
    assert await bus._already_processed("actuator/boom", 5) is False  # skipped, never handled
    assert orch.settled == [(5, Outcome.SUCCESS)]
```

- [ ] **Step 2: Run to verify failure**

Run: `./scripts/dev.sh test tests/test_bus_framework.py -q`
Expected: FAIL — `AttributeError: 'Bus' object has no attribute '_mark_processed'`, and `test_consume_handles_a_message_once` fails because handle runs twice (`calls == [5, 5]`).

- [ ] **Step 3: Add the `_processed_ttl` field**

In `switchboard/bus.py`, `Bus.__init__`, find the signature line ending `max_dead=500):` and add a parameter:

```python
    def __init__(self, mamamia_db_path, *, store=None, http=None,
                 default_timeout_s=30.0, wait_ms=30_000, reaper_interval=60.0,
                 max_retries=10, max_log_messages=10_000, max_dead=500,
                 processed_ttl=3600.0):
```

Near the other `self._` assignments in `__init__` (e.g. after `self._max_retries = max_retries`), add:

```python
        self._processed_ttl = processed_ttl
```

- [ ] **Step 4: Add the dedup helpers**

In `switchboard/bus.py`, add these two methods to the `Bus` class (place them just above `_consume`):

```python
    # at-least-once dedup: the same (group, msg.id) delivered twice runs once.
    # Uses the Bus's own store, unscoped, keyed to keep it off role scopes.
    async def _already_processed(self, group_id, mid) -> bool:
        return await self._store.get(f"_processed:{group_id}:{mid}") is not None

    async def _mark_processed(self, group_id, mid) -> None:
        await self._store.set(f"_processed:{group_id}:{mid}", "1", ttl=self._processed_ttl)
```

- [ ] **Step 5: Guard `_consume`**

In `_consume`, replace the block from `if msg is None:` through the success `settle`:

```python
                if msg is None:
                    continue
                if await self._already_processed(group_id, msg.id):
                    await settle(msg.id, Outcome.SUCCESS)   # redelivery of handled work
                    continue
                view = decode(msg)
                if not keep(view):
                    await settle(msg.id, Outcome.SUCCESS)
                    continue
                try:
                    async with asyncio.timeout(timeout_s):
                        await handle(view)
                    await self._mark_processed(group_id, msg.id)   # mark BEFORE settle
                    await settle(msg.id, Outcome.SUCCESS)
                except asyncio.CancelledError:
                    raise
                except PermanentError:
                    await settle(msg.id, Outcome.DEAD)
                except Exception:
                    attempts = await orch.state_store.get_retry_count(log, group_id, msg.id)
                    await settle(msg.id, Outcome.RETRY, retry_after=backoff(attempts))
```

(The `PermanentError`/`Exception` branches are untouched here — Task 2 changes them.)

- [ ] **Step 6: Run to verify pass**

Run: `./scripts/dev.sh test tests/test_bus_framework.py -q`
Expected: PASS, 3 tests.

- [ ] **Step 7: Run the full suite (no regressions)**

Run: `./scripts/dev.sh test`
Expected: PASS — existing count + 3. The dedup adds a store write per handled message; harmless to every existing consumer.

- [ ] **Step 8: Commit**

```bash
git add switchboard/bus.py tests/test_bus_framework.py
git commit -m "feat(bus): at-least-once dedup at the consume layer"
```

---

### Task 2: DeadLetterSensor

**Files:**
- Create: `switchboard/sensors/deadletter.py`
- Test: `tests/test_sensor_deadletter.py`
- Modify: `switchboard/app.py` — register the sensor in `build()`

**Interfaces:**
- Consumes: `SensorCtx` (`emit(name, payload)`, `store`, `schedule.every(seconds, fn)`) from `switchboard.message`; `MessageState` from `mamamia.core.models`.
- Produces: `DeadLetterSensor(db_path, *, interval=10.0)` with `name = "deadletter"`, emitting `switchboard.deadletter` observations with payload `{log, group, message_id, name, reason}`. Consumed by Phase 4 (the agent decider resolves a gather slot by looking up `pending:<payload["message_id"]>`) and eventually by the dashboard.

**Why a sensor and not Bus code:** mamamia can mark a message DEAD without the Bus ever seeing it — its own retry cap, the reaper, lease-expiry churn. Inline emission from `_consume` could only ever cover the cases the Bus itself decides, so it cannot make "every dead-letter is announced" an invariant. Reading the DEAD table makes the table the single source of truth, and a sensor is exactly the right shape for it: woken by a clock, it brings a fact into the log that was not already in it. It also keeps the core untouched and is opt-in — don't register it and the behaviour is simply absent.

**Note on naming:** there is deliberately no `<cmd>.failed` observation. `SensorCtx.emit` takes no `command_id` — a sensor cannot forge a result observation, and should not be able to. One signal, `switchboard.deadletter`, carries everything; consumers correlate from its payload.

- [ ] **Step 1: Write the failing test**

Create `tests/test_sensor_deadletter.py`:

```python
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
```

- [ ] **Step 2: Run to verify failure**

Run: `./scripts/dev.sh test tests/test_sensor_deadletter.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'switchboard.sensors.deadletter'`

- [ ] **Step 3: Write the sensor**

Create `switchboard/sensors/deadletter.py`:

```python
"""Announce dead-lettered messages as observations.

mamamia can mark a message DEAD without the Bus seeing it — its own retry cap,
the reaper, lease-expiry churn — so inline emission from the consume loop can
never be complete. Reading the DEAD table makes the table the single source of
truth, and this is exactly a sensor's job: bring a fact into the log that was
not already in it.
"""
import logging
import sqlite3

from mamamia.core.models import MessageState

logger = logging.getLogger(__name__)

DEADLETTER = "switchboard.deadletter"


def _decode(blob):
    import msgpack
    return msgpack.unpackb(blob, raw=False)


class DeadLetterSensor:
    name = "deadletter"

    def __init__(self, db_path: str, *, interval: float = 10.0):
        self._db = db_path
        self._interval = interval
        self.ctx = None

    def bind(self, ctx) -> None:
        self.ctx = ctx
        ctx.schedule.every(self._interval, self.sweep, first_after=0.0)

    async def start(self) -> None:
        return                       # timer-driven: no loop to supervise

    async def stop(self) -> None:
        return

    async def sweep(self) -> None:
        try:
            rows = self._dead_rows()
        except Exception as exc:
            logger.debug("dead-letter sweep skipped: %s", exc)
            return

        # First run establishes a baseline: existing DEAD rows are recorded as
        # seen but not announced, so a fresh store never replays history.
        baselined = await self.ctx.store.get("baselined") is not None

        for log, group, mid, name in rows:
            key = f"seen:{log}:{group}:{mid}"
            if await self.ctx.store.get(key) is not None:
                continue
            await self.ctx.store.set(key, "1")
            if not baselined:
                continue
            # Never announce our own kind: a consumer dying on a deadletter
            # observation would otherwise announce that, forever.
            if name == DEADLETTER:
                continue
            await self.ctx.emit(DEADLETTER, {
                "log": log, "group": group, "message_id": mid,
                "name": name, "reason": "dead-lettered",
            })

        if not baselined:
            await self.ctx.store.set("baselined", "1")

    def _dead_rows(self):
        # uri=True + mode=ro so a bug here can never write to the relay's db.
        conn = sqlite3.connect(f"file:{self._db}?mode=ro", uri=True, timeout=2.0)
        try:
            dead = conn.execute(
                "SELECT log_id, group_id, message_id FROM message_state WHERE state = ?",
                (MessageState.DEAD.value,),
            ).fetchall()
            out = []
            for log, group, mid in dead:
                row = conn.execute(
                    "SELECT metadata FROM messages WHERE log_id = ? AND id = ?",
                    (log, mid),
                ).fetchone()
                md = _decode(row[0]) if row and row[0] else {}
                out.append((log, group, mid, md.get("name", "")))
            return out
        finally:
            conn.close()
```

- [ ] **Step 4: Run to verify pass**

Run: `./scripts/dev.sh test tests/test_sensor_deadletter.py -q`
Expected: PASS, 5 tests.

- [ ] **Step 5: Register it in the app**

In `switchboard/app.py`, add the import beside the other sensors:

```python
from switchboard.sensors.deadletter import DeadLetterSensor
```

and in `build()`, extend the sensor list so it reads:

```python
    sensors = [GitHubSensor(secret=config["github_secret"]),
               DeadLetterSensor(config["mamamia_db_path"])]
```

- [ ] **Step 6: Run the full suite**

Run: `./scripts/dev.sh test`
Expected: PASS — existing count + 5. `tests/test_app.py` still passes; the extra sensor only adds a timer.

- [ ] **Step 7: Commit**

```bash
git add switchboard/sensors/deadletter.py tests/test_sensor_deadletter.py switchboard/app.py
git commit -m "feat(sensors): announce dead-letters as switchboard.deadletter observations"
```

---

## Self-Review

**Spec coverage (§10):**
- §10.1 `_consume` `(group, msg.id)` dedup, marked after handle / before settle, in the Bus's own store, distinct from role scopes → Task 1. ✓
- §10.2 dead-letter visibility — reworked to a sensor emitting `switchboard.deadletter` for every DEAD row on both logs, with cascade guard and first-run baseline → Task 2. ✓
- The "not exactly-once, crash window remains" property is inherent (marking before settle) and needs no code — documented in the spec, not enforced here. ✓

**Placeholder scan:** No TBD/TODO; every code step shows complete code; every test has real assertions. ✓

**Type consistency:** `_already_processed`/`_mark_processed` signatures match their `_consume` call sites. `DeadLetterSensor` implements the `Sensor` protocol (`name`, `bind`, `async start`, `async stop`) and uses only `ctx.emit`/`ctx.store`/`ctx.schedule` — no `command_id`, which `SensorCtx.emit` does not accept. ✓

**Risk removed:** the earlier draft made the retry cap Bus-owned so every DEAD transition would be Bus-visible. That rested on an unverified assumption about whether `get_retry_count` is pre- or post-increment, and still missed the reaper and lease-expiry churn. Reading the DEAD table removes both the assumption and the incompleteness — `_consume`'s failure branches are now untouched.

**One thing for the reviewer:** `tests/test_sensor_deadletter.py` builds a stand-in sqlite db with the two columns the sweep reads (`message_state`, `messages`). If mamamia's real schema differs, these tests pass while the sweep fails in production — the reviewer should confirm the column names against the live `data/events.db` (or against `switchboard/cli.py:list_dead_letters`, which queries the same tables).

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-07-23-agent-phase1-framework.md`. Two execution options:

**1. Subagent-Driven (recommended)** — a fresh subagent per task, spec+quality review between tasks, fast iteration.

**2. Inline Execution** — execute the two tasks in this session with checkpoints.

Which approach?
