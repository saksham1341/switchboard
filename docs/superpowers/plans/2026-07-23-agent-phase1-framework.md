# Agent Phase 1 — Framework Prerequisites Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the two substrate guarantees the agentic decider depends on — at-least-once dedup at the consume layer, and a dead-letter result observation so a failed command always produces a terminal result.

**Architecture:** Both changes live in `switchboard/bus.py`, inside `_consume` (the single loop every consumer — decider, actuator, tap — runs through). Task 1 adds a `(group_id, msg.id)` processed-guard backed by the Bus's own `KeyStore`. Task 2 routes every command dead-letter through one helper that emits a `<cmd>.failed` observation.

**Tech Stack:** Python 3.12, asyncio, mamamia (`Outcome`), the existing `KeyStore`. pytest with `asyncio_mode = "auto"` (bare `async def test_*`, no decorator). Run tests with `./scripts/dev.sh test`.

**Spec:** `docs/superpowers/specs/2026-07-23-agentic-decider-design.md` §10 (framework prerequisites), §11 (at-least-once principle).

## Global Constraints

- Dedup is keyed on `(group_id, msg.id)`, stored in the Bus's own `self._store` (unscoped) under the key `f"_processed:{group_id}:{msg.id}"`, value `"1"`, with a TTL (default `3600.0`s).
- A message is marked processed **only on the SUCCESS path** — after `handle` returns, before `settle(SUCCESS)`. Never mark on RETRY, DEAD, or the not-kept path.
- The `.failed` observation is emitted **only for `CMD_LOG` deaths.** `OBS_LOG` deaths settle DEAD with no synthetic observation, exactly as today.
- The `.failed` observation: name `f"{command_name}.failed"` (command_name from `msg.metadata["name"]`), `command_id = msg.id`, payload `{"reason": <str>, "attempts": <int>}`, `emitted_by = group_id`.
- Both dead-letter paths — `PermanentError` (immediate) and retries exhausted — route through one `_dead_letter` helper.
- Existing DEAD-after-max-retries behaviour and the dead-letter CLI (`switchboard/cli.py`, reads `message_state` DEAD rows) must keep working. The retry cap becomes Bus-owned (`attempts >= self._max_retries → DEAD`) so every DEAD transition is Bus-visible.
- The `switchboard.dashboard` and every existing role are unchanged. This is additive platform work.

## File Structure

- **Modify:** `switchboard/bus.py` — the `_consume` loop, plus two small helpers (`_already_processed`/`_mark_processed`, `_dead_letter`) and one `__init__` field (`_processed_ttl`).
- **Create:** `tests/test_bus_framework.py` — a shared fake-registry harness that drives `_consume` deterministically, plus the dedup and `.failed` tests.

`_consume` currently ends each success with `await settle(msg.id, Outcome.SUCCESS)` and handles failure with `except PermanentError: settle DEAD` / `except Exception: settle RETRY`. Both tasks edit this block; Task 2 builds on the block Task 1 leaves.

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

### Task 2: Dead-letter result observation

**Files:**
- Modify: `switchboard/bus.py` — add `_dead_letter`, route both DEAD paths through it, make the retry cap Bus-owned.
- Test: `tests/test_bus_framework.py` (extend).

**Interfaces:**
- Consumes: the `_consume` block from Task 1, `self.emit_observation(name, payload, command_id=, emitted_by=)`, `self._max_retries`, `Outcome`, `CMD_LOG`.
- Produces: `Bus._dead_letter(log, group_id, msg, settle, reason, attempts) -> None`, and a `_consume` that emits `f"{cmd_name}.failed"` (command_id set) for every command that dead-letters. Consumed by Phase 4 (the agent decider correlates results by command_id, `.failed` included).

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_bus_framework.py`:

```python
def _capture_emits(bus):
    """Replace emit_observation with a recorder; return the record list."""
    emitted = []
    async def cap(name, payload, command_id=None, emitted_by=None):
        emitted.append((name, command_id, emitted_by, payload)); return 1
    bus.emit_observation = cap
    return emitted


async def test_permanent_error_emits_failed_for_a_command():
    orch = _Orch()
    m = _Msg(7, "discord.post")
    bus = _drive([m], orch)
    emitted = _capture_emits(bus)
    async def handle(v): raise PermanentError()
    await bus._consume(CMD_LOG, "actuator/discord.post", Command.from_message, lambda v: True, handle)
    assert orch.settled == [(7, Outcome.DEAD)]
    assert emitted[0][:3] == ("discord.post.failed", 7, "actuator/discord.post")
    assert emitted[0][3]["reason"] == "permanent"


async def test_retry_exhaustion_emits_failed():
    orch = _Orch(retry_count=3)                 # already at the cap
    m = _Msg(9, "web_search")
    bus = _drive([m], orch, max_retries=3)
    emitted = _capture_emits(bus)
    async def handle(v): raise RuntimeError("boom")
    await bus._consume(CMD_LOG, "actuator/web_search", Command.from_message, lambda v: True, handle)
    assert orch.settled == [(9, Outcome.DEAD)]  # not RETRY — cap reached
    assert emitted[0][:2] == ("web_search.failed", 9)
    assert emitted[0][3]["attempts"] == 3


async def test_retry_below_cap_does_not_dead_letter():
    orch = _Orch(retry_count=0)
    m = _Msg(11, "web_search")
    bus = _drive([m], orch, max_retries=3)
    emitted = _capture_emits(bus)
    async def handle(v): raise RuntimeError("boom")
    await bus._consume(CMD_LOG, "actuator/web_search", Command.from_message, lambda v: True, handle)
    assert orch.settled[0][1] == Outcome.RETRY   # retried, not dead
    assert emitted == []                          # no .failed yet


async def test_observation_death_emits_no_failed():
    orch = _Orch()
    m = _Msg(3, "thing.happened")
    bus = _drive([m], orch)
    emitted = _capture_emits(bus)
    async def handle(v): raise PermanentError()
    await bus._consume(OBS_LOG, "decider/x", Observation.from_message, lambda v: True, handle)
    assert orch.settled == [(3, Outcome.DEAD)]
    assert emitted == []                          # obs deaths get no synthetic obs
```

- [ ] **Step 2: Run to verify failure**

Run: `./scripts/dev.sh test tests/test_bus_framework.py -q`
Expected: FAIL — `test_permanent_error_emits_failed_for_a_command` and `test_retry_exhaustion_emits_failed` fail (no `.failed` emitted); `test_retry_exhaustion_emits_failed` also fails because the current code settles RETRY, not DEAD, at the cap.

- [ ] **Step 3: Add the `_dead_letter` helper**

In `switchboard/bus.py`, add to the `Bus` class (next to the dedup helpers from Task 1):

```python
    async def _dead_letter(self, log, group_id, msg, settle, reason, attempts) -> None:
        """Settle a message DEAD and, for a command, emit a terminal result so a
        consumer waiting on that command_id is never left hanging."""
        await settle(msg.id, Outcome.DEAD)
        if log == CMD_LOG:
            name = (msg.metadata or {}).get("name", "")
            await self.emit_observation(
                f"{name}.failed", {"reason": reason, "attempts": attempts},
                command_id=msg.id, emitted_by=group_id)
```

- [ ] **Step 4: Route both DEAD paths through it**

In `_consume`, replace the `except PermanentError:` and `except Exception:` branches (from Task 1's block) with:

```python
                except PermanentError:
                    await self._dead_letter(log, group_id, msg, settle, "permanent", 0)
                except Exception:
                    attempts = await orch.state_store.get_retry_count(log, group_id, msg.id)
                    if attempts >= self._max_retries:
                        await self._dead_letter(log, group_id, msg, settle,
                                                "retries exhausted", attempts)
                    else:
                        await settle(msg.id, Outcome.RETRY, retry_after=backoff(attempts))
```

- [ ] **Step 5: Run to verify pass**

Run: `./scripts/dev.sh test tests/test_bus_framework.py -q`
Expected: PASS, 7 tests total.

- [ ] **Step 6: Run the full suite (no regressions)**

Run: `./scripts/dev.sh test`
Expected: PASS. In particular `tests/test_cli.py` still lists dead letters — the `_Boom` decider there raises `PermanentError` on the **obs** log, which still settles DEAD and (correctly) emits no `.failed`. The dead-letter `message_state` rows the CLI reads are unchanged.

- [ ] **Step 7: Commit**

```bash
git add switchboard/bus.py tests/test_bus_framework.py
git commit -m "feat(bus): emit <cmd>.failed on command dead-letter"
```

---

## Self-Review

**Spec coverage (§10):**
- §10.1 `_consume` `(group, msg.id)` dedup, marked after handle / before settle, in the Bus's own store, distinct from role scopes → Task 1. ✓
- §10.2 `.failed` on dead-letter, `command_id` set, CMD_LOG only, both DEAD paths, additive → Task 2. ✓
- The "not exactly-once, crash window remains" property is inherent (marking before settle) and needs no code — it is documented in the spec, not enforced here. ✓

**Placeholder scan:** No TBD/TODO; every code step shows complete code; every test has real assertions. ✓

**Type consistency:** `_already_processed`/`_mark_processed`/`_dead_letter` signatures match between their definitions and their `_consume` call sites; `emit_observation(name, payload, command_id=, emitted_by=)` matches the existing signature in `bus.py:98`. ✓

**One risk flagged for review, not a gap:** Task 2 makes the retry cap Bus-owned (`attempts >= self._max_retries → DEAD`) instead of relying on mamamia's internal cap. The full-suite run in Task 2 Step 6 is the check that this preserves existing dead-letter behaviour; the reviewer should confirm no test depended on mamamia performing the DEAD transition itself.

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-07-23-agent-phase1-framework.md`. Two execution options:

**1. Subagent-Driven (recommended)** — a fresh subagent per task, spec+quality review between tasks, fast iteration.

**2. Inline Execution** — execute the two tasks in this session with checkpoints.

Which approach?
