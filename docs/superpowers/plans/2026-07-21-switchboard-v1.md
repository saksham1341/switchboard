# Switchboard v1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the Switchboard v1 vertical slice — a GitHub webhook is verified, turned into an event, durably logged via mamamia, leased, dispatched to a logger egress, and retried/dead-lettered on failure — on a Raspberry Pi.

**Architecture:** Switchboard is the application layer over mamamia `v0.2.0`, used in-process as a library. A `Broker` facade wraps mamamia's `LogRegistry` (durable SQLite log, leasing, retry, retention, long-polling). Each handler is a mamamia consumer group with its own asyncio task. Switchboard owns the event envelope, per-handler dispatch, the retry *schedule*, and deduplication (in its own separate SQLite database). Ingress/egress adapters do translation only.

**Tech Stack:** Python 3.12, asyncio, mamamia `v0.2.0` (in-process library), stdlib `sqlite3` + `hmac`, `httpx` (later, for Discord), `starlette` + `uvicorn` (webhook endpoint), `pytest` + `pytest-asyncio`.

## Global Constraints

- **Python 3.12**, asyncio throughout; every interface boundary is a coroutine.
- **mamamia pinned to `v0.2.0`** — consume it as a library; never fork or depend on an unreleased branch. Dev installs it editable from the sibling checkout (`../mamamia`).
- **No aiosqlite.** mamamia uses stdlib `sqlite3`; Switchboard opens its *own* stdlib `sqlite3` connection for its dedup db. The two databases are never shared.
- **Adapters do translation only** — no scheduling, no retry, no queueing inside an adapter.
- **One event log**, `log_id = "events"`. A handler's `group_id` is `"<egress>/<handler>"`.
- **Deduplication is ordered, not transactional:** check `seen` → append to mamamia → record `seen` → `notify`. A crash yields a harmless duplicate, never loss.
- **msgpack-serializable payloads only** (mamamia round-trips message payloads through msgpack on SQLite): string dict keys, lists not tuples, 64-bit ints.
- TDD: every behavior gets a failing test first. Commit after each green task.
- Test faking is at the HTTP boundary — no live GitHub/Discord calls.

**mamamia API reference (verified against v0.2.0):**

```python
from mamamia.server.db import connect                      # async connect(path) -> sqlite3.Connection
from mamamia.server.storage.sqlite import SQLiteStorage    # SQLiteStorage(conn)
from mamamia.server.state.sqlite import SQLiteStateStore    # SQLiteStateStore(conn)
from mamamia.server.lease.sqlite import SQLiteLeaseManager  # SQLiteLeaseManager(conn)
from mamamia.server.transaction import SQLiteTransaction    # SQLiteTransaction(conn)
from mamamia.server.registry import LogRegistry
from mamamia.core.models import Outcome, Message            # Outcome.SUCCESS/RETRY/DEAD; Message.id/payload/metadata

reg = LogRegistry(storage=SQLiteStorage(conn), state=SQLiteStateStore(conn),
                  lease=SQLiteLeaseManager(conn), transaction=SQLiteTransaction(conn),
                  max_log_messages=10_000, max_dead=500)
reg.start_reaper(interval=60.0)                             # background reaper task
storage = reg.get_storage()                                # await storage.append(log_id, payload_dict) -> int (msg id)
orch = reg.get_orchestrator("events")                       # orch.storage / orch.state_store / orch.lease_manager
msg = await reg.acquire_blocking("events", group_id, client_id, duration=lease_s, wait_ms=30_000)  # Optional[Message]
reg.notify("events")                                        # wake blocked consumers after a publish
await orch.settle("events", group_id, msg.id, client_id, outcome=Outcome.SUCCESS)
await orch.settle("events", group_id, msg.id, client_id, outcome=Outcome.RETRY, retry_after=delay)
await orch.settle("events", group_id, msg.id, client_id, outcome=Outcome.DEAD)
attempts = await orch.state_store.get_retry_count("events", group_id, msg.id)   # int, pre-settle count
```

---

## File Structure

```
switchboard/
├── pyproject.toml                     # package + deps + pytest config
├── switchboard/
│   ├── __init__.py
│   ├── event.py                       # ulid(), Event, EventInput, PublishResult, now_iso()
│   ├── errors.py                      # PermanentError, ChainTooDeep
│   ├── backoff.py                     # exponential-with-jitter schedule
│   ├── dedup.py                       # SeenStore (own sqlite3 db)
│   ├── egress.py                      # Handler/Egress protocols, Ctx, LoggerEgress
│   ├── broker.py                      # Broker facade over mamamia LogRegistry
│   ├── ingress/
│   │   ├── __init__.py
│   │   └── github.py                  # verify_signature(), map_event(), GitHubIngress
│   ├── cli.py                         # `switchboard dead-letters`
│   └── app.py                         # wiring entrypoint
├── tests/
│   ├── conftest.py                    # broker fixture, tmp dirs
│   ├── test_event.py
│   ├── test_backoff.py
│   ├── test_dedup.py
│   ├── test_egress.py
│   ├── test_broker.py                 # publish, dispatch, retry, dead-letter, isolation, durability
│   ├── test_github_map.py             # kind mapping + HMAC
│   ├── test_github_endpoint.py        # HTTP endpoint
│   ├── test_cli.py
│   └── fixtures/github/               # recorded webhook payloads
├── Dockerfile
├── docker-compose.yml
├── .env.example
└── README.md
```

---

## Task 1: Project scaffold and mamamia dependency

**Files:**
- Create: `pyproject.toml`, `switchboard/__init__.py`, `tests/__init__.py`, `tests/conftest.py`
- Test: `tests/test_smoke.py`

**Interfaces:**
- Produces: an installable `switchboard` package with mamamia importable; `pytest` runs.

- [ ] **Step 1: Write `pyproject.toml`**

```toml
[project]
name = "switchboard"
version = "0.1.0"
description = "Event-driven relay engine over mamamia."
requires-python = ">=3.12"
dependencies = [
    "mamamia>=0.2.0,<0.3",
    "starlette",
    "uvicorn",
    "httpx",
]

[project.optional-dependencies]
dev = ["pytest>=8.0", "pytest-asyncio>=0.24"]

[project.scripts]
switchboard = "switchboard.cli:main"

[tool.pytest.ini_options]
asyncio_mode = "auto"

[build-system]
requires = ["setuptools>=68"]
build-backend = "setuptools.build_meta"
```

- [ ] **Step 2: Create package files**

```bash
mkdir -p switchboard/ingress tests/fixtures/github
touch switchboard/__init__.py switchboard/ingress/__init__.py tests/__init__.py
```

- [ ] **Step 3: Write the smoke test**

`tests/test_smoke.py`:
```python
def test_mamamia_importable():
    import mamamia.server.registry  # noqa: F401
    from mamamia.core.models import Outcome
    assert {o.value for o in Outcome} == {"success", "retry", "dead"}


def test_switchboard_importable():
    import switchboard  # noqa: F401
```

- [ ] **Step 4: Create the venv and install (mamamia editable, switchboard editable)**

Run:
```bash
python3.12 -m venv venv
. venv/bin/activate
pip install -e ../mamamia
pip install -e ".[dev]"
```
Expected: both install without error.

- [ ] **Step 5: Run the smoke test**

Run: `. venv/bin/activate && python -m pytest tests/test_smoke.py -v`
Expected: 2 passed.

- [ ] **Step 6: Add `.gitignore` and commit**

`.gitignore` (append if present):
```
venv/
__pycache__/
*.egg-info/
*.db
*.db-wal
*.db-shm
.env
```

```bash
git add pyproject.toml .gitignore switchboard/ tests/
git commit -m "scaffold: switchboard package, mamamia v0.2.0 dependency, pytest"
```

---

## Task 2: Core types — Event, identity, errors

**Files:**
- Create: `switchboard/event.py`, `switchboard/errors.py`
- Test: `tests/test_event.py`

**Interfaces:**
- Produces:
  - `ulid() -> str` — 26-char Crockford base32, time-sortable.
  - `now_iso() -> str` — current UTC ISO-8601.
  - `@dataclass(frozen=True) Event(id, kind, source, at, payload, dedupe_key=None, meta={})`
  - `@dataclass EventInput(kind, source, payload, at=None, dedupe_key=None, meta={})`
  - `@dataclass PublishResult(status: Literal["accepted","duplicate"], event_id: str)`
  - `PermanentError(Exception)`, `ChainTooDeep(Exception)`

- [ ] **Step 1: Write the failing test**

`tests/test_event.py`:
```python
import time
from switchboard.event import ulid, now_iso, Event, EventInput, PublishResult


def test_ulid_is_26_char_crockford():
    u = ulid()
    assert len(u) == 26
    assert all(c in "0123456789ABCDEFGHJKMNPQRSTVWXYZ" for c in u)


def test_ulid_is_time_sortable():
    a = ulid()
    time.sleep(0.002)
    b = ulid()
    assert b > a


def test_ulid_unique():
    assert len({ulid() for _ in range(1000)}) == 1000


def test_now_iso_roundtrips():
    from datetime import datetime
    datetime.fromisoformat(now_iso())  # must not raise


def test_event_is_frozen():
    e = Event(id="x", kind="github.home.pr.opened", source="github",
              at=now_iso(), payload={"n": 1})
    import dataclasses
    try:
        e.kind = "y"
        assert False, "Event must be frozen"
    except dataclasses.FrozenInstanceError:
        pass


def test_event_input_defaults():
    ei = EventInput(kind="k", source="s", payload={})
    assert ei.at is None and ei.dedupe_key is None and ei.meta == {}


def test_publish_result():
    r = PublishResult(status="accepted", event_id="abc")
    assert r.status == "accepted" and r.event_id == "abc"
```

- [ ] **Step 2: Run to verify it fails**

Run: `python -m pytest tests/test_event.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'switchboard.event'`.

- [ ] **Step 3: Write `switchboard/errors.py`**

```python
class PermanentError(Exception):
    """Raised by a handler for a failure that cannot succeed on retry.
    The consumer loop maps it to Outcome.DEAD."""


class ChainTooDeep(Exception):
    """A published event exceeded MAX_CHAIN_DEPTH — a runaway pipeline."""
```

- [ ] **Step 4: Write `switchboard/event.py`**

```python
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Literal

_CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"


def _encode(value: int, length: int) -> str:
    chars = []
    for _ in range(length):
        chars.append(_CROCKFORD[value & 0x1F])
        value >>= 5
    return "".join(reversed(chars))


def ulid() -> str:
    """26-char Crockford base32 ULID: 48-bit ms timestamp + 80-bit randomness.
    Lexicographically sortable by time; not strictly monotonic within a
    millisecond, which is fine — mamamia's integer message id is the ordering
    of record."""
    ms = int(time.time() * 1000) & ((1 << 48) - 1)
    rand = int.from_bytes(os.urandom(10), "big")  # 80 bits
    return _encode(ms, 10) + _encode(rand, 16)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class Event:
    id: str
    kind: str
    source: str
    at: str
    payload: dict
    dedupe_key: str | None = None
    meta: dict[str, str] = field(default_factory=dict)


@dataclass
class EventInput:
    kind: str
    source: str
    payload: dict
    at: str | None = None
    dedupe_key: str | None = None
    meta: dict[str, str] = field(default_factory=dict)


@dataclass
class PublishResult:
    status: Literal["accepted", "duplicate"]
    event_id: str
```

- [ ] **Step 5: Run to verify it passes**

Run: `python -m pytest tests/test_event.py -v`
Expected: all passed.

- [ ] **Step 6: Commit**

```bash
git add switchboard/event.py switchboard/errors.py tests/test_event.py
git commit -m "feat: Event envelope, ULID identity, error types"
```

---

## Task 3: Backoff schedule

**Files:**
- Create: `switchboard/backoff.py`
- Test: `tests/test_backoff.py`

**Interfaces:**
- Produces: `backoff(attempts: int, *, base=1.0, cap=300.0) -> float` — exponential with equal jitter, monotone ceiling, capped.

- [ ] **Step 1: Write the failing test**

`tests/test_backoff.py`:
```python
from switchboard.backoff import backoff


def test_backoff_grows_and_caps():
    # Sample many draws; the *ceiling* per attempt is base*2**attempts, capped.
    for attempts, ceil in [(0, 1.0), (1, 2.0), (2, 4.0), (3, 8.0)]:
        draws = [backoff(attempts) for _ in range(200)]
        assert min(draws) >= ceil / 2          # equal jitter: at least half the ceiling
        assert max(draws) <= ceil + 1e-9        # never above the ceiling


def test_backoff_capped_at_5_minutes():
    draws = [backoff(20) for _ in range(200)]   # 2**20 s uncapped
    assert max(draws) <= 300.0
    assert min(draws) >= 150.0                   # half of the 300s cap


def test_backoff_nonnegative():
    assert backoff(0) >= 0
```

- [ ] **Step 2: Run to verify it fails**

Run: `python -m pytest tests/test_backoff.py -v`
Expected: FAIL, module not found.

- [ ] **Step 3: Write `switchboard/backoff.py`**

```python
import random


def backoff(attempts: int, *, base: float = 1.0, cap: float = 300.0) -> float:
    """Exponential backoff with equal jitter. The ceiling for a given attempt is
    min(cap, base * 2**attempts); the actual delay is a uniform draw in
    [ceiling/2, ceiling]. Jitter spreads retries so a burst of failures does not
    resynchronize into a thundering herd."""
    ceiling = min(cap, base * (2 ** attempts))
    return ceiling / 2 + random.random() * (ceiling / 2)
```

- [ ] **Step 4: Run to verify it passes**

Run: `python -m pytest tests/test_backoff.py -v`
Expected: all passed.

- [ ] **Step 5: Commit**

```bash
git add switchboard/backoff.py tests/test_backoff.py
git commit -m "feat: exponential-with-jitter backoff schedule"
```

---

## Task 4: Deduplication store (Switchboard's own database)

**Files:**
- Create: `switchboard/dedup.py`
- Test: `tests/test_dedup.py`

**Interfaces:**
- Produces: `class SeenStore` with:
  - `SeenStore(db_path: str)` — opens its own WAL sqlite3 connection, migrates the `seen` table.
  - `get(key: str) -> str | None` — the event id previously produced for `key`, or None.
  - `record(key: str, event_id: str) -> None` — idempotent insert (`INSERT OR IGNORE`).
  - `prune(keep_last: int) -> int` — keep the most recent `keep_last` rows by rowid; return deleted count.
  - `close() -> None`.

- [ ] **Step 1: Write the failing test**

`tests/test_dedup.py`:
```python
from switchboard.dedup import SeenStore


def test_unseen_key_returns_none(tmp_path):
    s = SeenStore(str(tmp_path / "sb.db"))
    assert s.get("delivery-1") is None
    s.close()


def test_record_then_get(tmp_path):
    s = SeenStore(str(tmp_path / "sb.db"))
    s.record("delivery-1", "EVT1")
    assert s.get("delivery-1") == "EVT1"
    s.close()


def test_record_is_idempotent_first_writer_wins(tmp_path):
    s = SeenStore(str(tmp_path / "sb.db"))
    s.record("d", "EVT1")
    s.record("d", "EVT2")           # ignored
    assert s.get("d") == "EVT1"
    s.close()


def test_survives_reopen(tmp_path):
    p = str(tmp_path / "sb.db")
    s = SeenStore(p); s.record("d", "EVT1"); s.close()
    s2 = SeenStore(p)
    assert s2.get("d") == "EVT1"
    s2.close()


def test_prune_keeps_most_recent(tmp_path):
    s = SeenStore(str(tmp_path / "sb.db"))
    for i in range(10):
        s.record(f"d{i}", f"E{i}")
    deleted = s.prune(keep_last=3)
    assert deleted == 7
    assert s.get("d0") is None and s.get("d9") == "E9"
    s.close()
```

- [ ] **Step 2: Run to verify it fails**

Run: `python -m pytest tests/test_dedup.py -v`
Expected: FAIL, module not found.

- [ ] **Step 3: Write `switchboard/dedup.py`**

```python
import sqlite3

_SCHEMA_VERSION = 1


class SeenStore:
    """Switchboard's own durable dedup table, in a database mamamia never sees.
    Maps a provider idempotency key (e.g. GitHub's X-GitHub-Delivery) to the
    event id it produced. Synchronous sqlite3, driven on the event-loop thread
    like mamamia's own backends; operations are microseconds."""

    def __init__(self, db_path: str):
        self._conn = sqlite3.connect(db_path, isolation_level=None, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")  # a lost tail = a possible dup, never loss
        self._migrate()

    def _migrate(self) -> None:
        version = self._conn.execute("PRAGMA user_version").fetchone()[0]
        if version < 1:
            self._conn.execute(
                "CREATE TABLE IF NOT EXISTS seen ("
                "  key      TEXT PRIMARY KEY,"
                "  event_id TEXT NOT NULL"
                ")"
            )
            self._conn.execute(f"PRAGMA user_version={_SCHEMA_VERSION}")

    def get(self, key: str) -> str | None:
        row = self._conn.execute(
            "SELECT event_id FROM seen WHERE key = ?", (key,)
        ).fetchone()
        return row[0] if row else None

    def record(self, key: str, event_id: str) -> None:
        self._conn.execute(
            "INSERT OR IGNORE INTO seen (key, event_id) VALUES (?, ?)",
            (key, event_id),
        )

    def prune(self, keep_last: int) -> int:
        cur = self._conn.execute(
            "DELETE FROM seen WHERE rowid NOT IN ("
            "  SELECT rowid FROM seen ORDER BY rowid DESC LIMIT ?"
            ")",
            (keep_last,),
        )
        return cur.rowcount

    def close(self) -> None:
        self._conn.close()
```

- [ ] **Step 4: Run to verify it passes**

Run: `python -m pytest tests/test_dedup.py -v`
Expected: all passed.

- [ ] **Step 5: Commit**

```bash
git add switchboard/dedup.py tests/test_dedup.py
git commit -m "feat: SeenStore dedup in Switchboard's own sqlite db"
```

---

## Task 5: Egress protocols and LoggerEgress

**Files:**
- Create: `switchboard/egress.py`
- Test: `tests/test_egress.py`

**Interfaces:**
- Produces:
  - `Filter = Callable[[Event], bool]`
  - `@dataclass Handler(name, filter, handle, timeout_s=None, lease_s=None)` where `handle` is `Callable[[Event, Ctx], Awaitable[None]]`.
  - `class Egress(Protocol)`: `name: str`, `filter: Filter | None`, `handlers: list[Handler]`, `def context(self) -> Any`.
  - `@dataclass Ctx(publish, egress)` — capabilities handed to a handler.
  - `class LoggerEgress` — one handler `log_all`, `filter=lambda e: e.source=="github"`, writing structured JSON to a provided stream (default stdout).

- [ ] **Step 1: Write the failing test**

`tests/test_egress.py`:
```python
import io
import json
import asyncio
from switchboard.event import Event, now_iso
from switchboard.egress import LoggerEgress, Ctx


def _event(**kw):
    base = dict(id="E1", kind="github.home.pr.opened", source="github",
                at=now_iso(), payload={"n": 1})
    base.update(kw)
    return Event(**base)


def test_logger_egress_shape():
    eg = LoggerEgress()
    assert eg.name == "logger"
    assert len(eg.handlers) == 1
    h = eg.handlers[0]
    assert h.filter(_event()) is True
    assert h.filter(_event(source="discord")) is False


def test_logger_handler_writes_json():
    buf = io.StringIO()
    eg = LoggerEgress(stream=buf)
    h = eg.handlers[0]
    ctx = Ctx(publish=None, egress=eg.context())
    asyncio.run(h.handle(_event(), ctx))
    line = json.loads(buf.getvalue())
    assert line["event_id"] == "E1"
    assert line["kind"] == "github.home.pr.opened"
```

- [ ] **Step 2: Run to verify it fails**

Run: `python -m pytest tests/test_egress.py -v`
Expected: FAIL, module not found.

- [ ] **Step 3: Write `switchboard/egress.py`**

```python
import json
import sys
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Protocol, runtime_checkable

from switchboard.event import Event

Filter = Callable[[Event], bool]
Handle = Callable[[Event, "Ctx"], Awaitable[None]]


@dataclass
class Handler:
    name: str
    filter: Filter
    handle: Handle
    timeout_s: float | None = None
    lease_s: float | None = None


@runtime_checkable
class Egress(Protocol):
    name: str
    filter: Filter | None
    handlers: list[Handler]

    def context(self) -> Any: ...


@dataclass
class Ctx:
    publish: Any     # Publish callable from the broker (None where unused)
    egress: Any      # from egress.context()


class LoggerEgress:
    """Structured-JSON debug tap. No external dependency; exercises every
    durability/retry property before Discord becomes a translation problem."""

    name = "logger"

    def __init__(self, filter: Filter | None = None, stream=None):
        self.filter = filter or (lambda e: e.source == "github")
        self._stream = stream or sys.stdout
        self.handlers = [Handler(name="log-all", filter=lambda e: True, handle=self._log)]

    def context(self) -> Any:
        return None

    async def _log(self, event: Event, ctx: Ctx) -> None:
        self._stream.write(json.dumps({
            "event_id": event.id,
            "kind": event.kind,
            "source": event.source,
            "at": event.at,
            "payload": event.payload,
        }) + "\n")
        self._stream.flush()
```

- [ ] **Step 4: Run to verify it passes**

Run: `python -m pytest tests/test_egress.py -v`
Expected: all passed.

- [ ] **Step 5: Commit**

```bash
git add switchboard/egress.py tests/test_egress.py
git commit -m "feat: Egress/Handler protocols and LoggerEgress"
```

---

## Task 6: Broker — construction, lifecycle, publish

**Files:**
- Create: `switchboard/broker.py`
- Test: `tests/test_broker.py`, `tests/conftest.py`

**Interfaces:**
- Consumes: mamamia `LogRegistry`/`connect`/`SQLite*`/`SQLiteTransaction`/`Outcome`; `SeenStore`; `Event`/`EventInput`/`PublishResult`/`ulid`/`now_iso`; `ChainTooDeep`.
- Produces: `class Broker` with (this task) `__init__`, `attach`, `on`, `async start`, `async stop`, `async publish`. The consumer loop is Task 7.
  - `Broker(mamamia_db_path, switchboard_db_path, *, max_log_messages=10_000, max_dead=500, default_timeout_s=30.0, wait_ms=30_000, max_chain_depth=16, reaper_interval=60.0)`
  - `attach(egress: Egress) -> None`
  - `on(hook: Literal["success","failed","dead"], fn: Callable[[Event, str], None]) -> None`
  - `async start() -> None`
  - `async stop() -> None`
  - `async publish(ev: EventInput) -> PublishResult`

- [ ] **Step 1: Write `tests/conftest.py`**

```python
import pytest
from switchboard.broker import Broker


@pytest.fixture
async def broker(tmp_path):
    b = Broker(
        mamamia_db_path=str(tmp_path / "events.db"),
        switchboard_db_path=str(tmp_path / "sb.db"),
        max_log_messages=10_000,
        wait_ms=50,               # short waits so tests are fast
        reaper_interval=3600.0,   # keep the reaper out of tests
    )
    await b.start()
    yield b
    await b.stop()
```

- [ ] **Step 2: Write the failing test (publish + dedup)**

`tests/test_broker.py`:
```python
import pytest
from switchboard.event import EventInput
from switchboard.errors import ChainTooDeep


async def test_publish_accepts_and_assigns_id(broker):
    r = await broker.publish(EventInput(kind="github.home.pr.opened", source="github",
                                        payload={"n": 1}))
    assert r.status == "accepted"
    assert len(r.event_id) == 26


async def test_publish_dedupes_on_key(broker):
    ev = EventInput(kind="k", source="github", payload={}, dedupe_key="delivery-1")
    first = await broker.publish(ev)
    second = await broker.publish(EventInput(kind="k", source="github", payload={},
                                             dedupe_key="delivery-1"))
    assert first.status == "accepted"
    assert second.status == "duplicate"
    assert second.event_id == first.event_id


async def test_publish_without_key_never_dedupes(broker):
    a = await broker.publish(EventInput(kind="k", source="github", payload={}))
    b = await broker.publish(EventInput(kind="k", source="github", payload={}))
    assert a.status == "accepted" and b.status == "accepted"
    assert a.event_id != b.event_id


async def test_publish_rejects_over_max_depth(broker):
    ev = EventInput(kind="k", source="github", payload={}, meta={"depth": "17"})
    with pytest.raises(ChainTooDeep):
        await broker.publish(ev)
```

- [ ] **Step 3: Run to verify it fails**

Run: `python -m pytest tests/test_broker.py -v`
Expected: FAIL (Broker has no `publish` / import error).

- [ ] **Step 4: Write `switchboard/broker.py` (this task's methods only)**

```python
import asyncio
import uuid
from dataclasses import asdict
from typing import Callable, Literal

from mamamia.core.models import Outcome
from mamamia.server.db import connect
from mamamia.server.storage.sqlite import SQLiteStorage
from mamamia.server.state.sqlite import SQLiteStateStore
from mamamia.server.lease.sqlite import SQLiteLeaseManager
from mamamia.server.transaction import SQLiteTransaction
from mamamia.server.registry import LogRegistry

from switchboard.backoff import backoff
from switchboard.dedup import SeenStore
from switchboard.egress import Ctx, Egress
from switchboard.errors import ChainTooDeep, PermanentError
from switchboard.event import Event, EventInput, PublishResult, now_iso, ulid

LOG_ID = "events"


class Broker:
    def __init__(
        self,
        mamamia_db_path: str,
        switchboard_db_path: str,
        *,
        max_log_messages: int = 10_000,
        max_dead: int = 500,
        default_timeout_s: float = 30.0,
        wait_ms: int = 30_000,
        max_chain_depth: int = 16,
        reaper_interval: float = 60.0,
    ):
        self._mamamia_db_path = mamamia_db_path
        self._switchboard_db_path = switchboard_db_path
        self._max_log_messages = max_log_messages
        self._max_dead = max_dead
        self._default_timeout_s = default_timeout_s
        self._wait_ms = wait_ms
        self._max_chain_depth = max_chain_depth
        self._reaper_interval = reaper_interval

        self._instance_id = f"sb-{uuid.uuid4().hex}"
        self._egresses: dict[str, Egress] = {}
        self._hooks: dict[str, list[Callable[[Event, str], None]]] = {
            "success": [], "failed": [], "dead": []
        }
        self._registry: LogRegistry | None = None
        self._seen: SeenStore | None = None
        self._conn = None
        self._tasks: list[asyncio.Task] = []
        self._running = False

    def attach(self, egress: Egress) -> None:
        self._egresses[egress.name] = egress  # idempotent by name

    def on(self, hook: Literal["success", "failed", "dead"],
           fn: Callable[[Event, str], None]) -> None:
        self._hooks[hook].append(fn)

    def _fire(self, hook: str, event: Event, group_id: str) -> None:
        for fn in self._hooks[hook]:
            try:
                fn(event, group_id)
            except Exception:
                pass  # observability must never break dispatch

    async def start(self) -> None:
        self._conn = await connect(self._mamamia_db_path)
        self._registry = LogRegistry(
            storage=SQLiteStorage(self._conn),
            state=SQLiteStateStore(self._conn),
            lease=SQLiteLeaseManager(self._conn),
            transaction=SQLiteTransaction(self._conn),
            max_log_messages=self._max_log_messages,
            max_dead=self._max_dead,
        )
        self._seen = SeenStore(self._switchboard_db_path)
        self._running = True
        self._registry.start_reaper(interval=self._reaper_interval)
        for egress in self._egresses.values():
            for handler in egress.handlers:
                self._tasks.append(asyncio.create_task(self._consume(egress, handler)))

    async def stop(self) -> None:
        self._running = False
        for t in self._tasks:
            t.cancel()
        for t in self._tasks:
            try:
                await t
            except asyncio.CancelledError:
                pass
        self._tasks.clear()
        if self._seen is not None:
            self._seen.close()
        if self._conn is not None:
            self._conn.close()

    async def publish(self, ev: EventInput) -> PublishResult:
        depth = int(ev.meta.get("depth", "0"))
        if depth > self._max_chain_depth:
            raise ChainTooDeep(f"publish depth {depth} exceeds {self._max_chain_depth}")

        if ev.dedupe_key is not None:
            existing = self._seen.get(ev.dedupe_key)
            if existing is not None:
                return PublishResult(status="duplicate", event_id=existing)

        event = Event(
            id=ulid(), kind=ev.kind, source=ev.source,
            at=ev.at or now_iso(), payload=ev.payload,
            dedupe_key=ev.dedupe_key, meta=dict(ev.meta),
        )
        # Ordered for crash-safety: append first (durable), then record seen.
        await self._registry.get_storage().append(LOG_ID, asdict(event))
        if ev.dedupe_key is not None:
            self._seen.record(ev.dedupe_key, event.id)
        self._registry.notify(LOG_ID)
        return PublishResult(status="accepted", event_id=event.id)

    async def _consume(self, egress: Egress, handler) -> None:
        raise NotImplementedError  # Task 7
```

- [ ] **Step 5: Run to verify it passes**

Run: `python -m pytest tests/test_broker.py -v`
Expected: the four Task-6 tests pass. (No consumers run yet — publish does not depend on `_consume`.)

- [ ] **Step 6: Commit**

```bash
git add switchboard/broker.py tests/test_broker.py tests/conftest.py
git commit -m "feat: Broker construction, lifecycle, ordered dedup+publish"
```

---

## Task 7: Broker — the dispatcher (consumer loop)

**Files:**
- Modify: `switchboard/broker.py` (replace `_consume`)
- Test: `tests/test_broker.py` (add dispatch/retry/dead-letter/isolation/durability tests)

**Interfaces:**
- Consumes: everything from Task 6, plus `Handler`, `Ctx`, `Outcome`, `backoff`, `PermanentError`, mamamia `acquire_blocking`/`settle`/`get_retry_count`.
- Produces: a running per-handler consumer that filters, dispatches under a timeout, and settles `SUCCESS`/`DEAD`/`RETRY(retry_after)`.

- [ ] **Step 1: Write failing tests (dispatch, retry, dead-letter, isolation, durability)**

Add to `tests/test_broker.py`:
```python
import asyncio
from switchboard.egress import Handler, Ctx
from switchboard.errors import PermanentError


class RecordingEgress:
    """A fake egress whose single handler records events and can be scripted to
    fail. `fail_times` transient failures, then success."""
    def __init__(self, name="rec", fail_times=0, permanent=False, hang=False):
        self.name = name
        self.filter = None
        self.seen = []
        self._fail_times = fail_times
        self._permanent = permanent
        self._hang = hang
        self.handlers = [Handler(name="h", filter=lambda e: True, handle=self._handle,
                                 timeout_s=0.2, lease_s=0.4)]

    def context(self):
        return None

    async def _handle(self, event, ctx):
        if self._hang:
            await asyncio.sleep(10)
        if self._permanent:
            raise PermanentError("nope")
        if len(self.seen) < self._fail_times:
            self.seen.append(("fail", event.id))
            raise RuntimeError("transient")
        self.seen.append(("ok", event.id))


async def _wait_for(predicate, timeout=5.0):
    async def loop():
        while not predicate():
            await asyncio.sleep(0.01)
    await asyncio.wait_for(loop(), timeout)
```

> **Note for the implementer:** `conftest`'s `broker` fixture already calls
> `start()`, but an egress must be attached *before* `start()`. So the dispatch
> tests build their own broker via a `make_broker` factory. Add it to
> `conftest.py`:

```python
# tests/conftest.py  (add)
import pytest
from switchboard.broker import Broker

@pytest.fixture
def make_broker(tmp_path):
    created = []
    def _make(**kw):
        b = Broker(
            mamamia_db_path=str(tmp_path / "events.db"),
            switchboard_db_path=str(tmp_path / "sb.db"),
            wait_ms=50, reaper_interval=3600.0, **kw,
        )
        created.append(b)
        return b
    yield _make
```

The dispatch tests (using `make_broker`):
```python
async def test_dispatch_delivers_once(make_broker):
    b = make_broker()
    eg = RecordingEgress()
    b.attach(eg)
    await b.start()
    try:
        r = await b.publish(EventInput(kind="k", source="github", payload={"n": 1}))
        await _wait_for(lambda: len(eg.seen) >= 1)
        assert eg.seen == [("ok", r.event_id)]
        await asyncio.sleep(0.2)                      # no redelivery
        assert eg.seen == [("ok", r.event_id)]
    finally:
        await b.stop()


async def test_transient_failure_retries_then_succeeds(make_broker):
    b = make_broker()
    eg = RecordingEgress(fail_times=2)
    b.attach(eg)
    await b.start()
    try:
        await b.publish(EventInput(kind="k", source="github", payload={}))
        await _wait_for(lambda: ("ok" in [s[0] for s in eg.seen]), timeout=15)
        outcomes = [s[0] for s in eg.seen]
        assert outcomes == ["fail", "fail", "ok"]
    finally:
        await b.stop()


async def test_permanent_error_dead_letters_without_retry(make_broker):
    b = make_broker()
    eg = RecordingEgress(permanent=True)
    dead = []
    b.attach(eg)
    b.on("dead", lambda e, g: dead.append(e.id))
    await b.start()
    try:
        await b.publish(EventInput(kind="k", source="github", payload={}))
        await _wait_for(lambda: len(dead) >= 1)
        await asyncio.sleep(0.3)
        assert len(dead) == 1                          # dead once, never retried
    finally:
        await b.stop()


async def test_hung_handler_times_out_and_isolates(make_broker):
    b = make_broker()
    slow = RecordingEgress(name="slow", hang=True)
    fast = RecordingEgress(name="fast")
    b.attach(slow)
    b.attach(fast)
    await b.start()
    try:
        await b.publish(EventInput(kind="k", source="github", payload={}))
        # fast handler delivers despite slow one being blocked/timing out
        await _wait_for(lambda: len(fast.seen) >= 1, timeout=5)
        assert fast.seen[0][0] == "ok"
    finally:
        await b.stop()


async def test_durability_redelivers_after_crash_midflight(tmp_path):
    # True crash recovery: a handler takes the message (lease written, durable)
    # and is cancelled MID-DISPATCH without settling. On restart, the held lease
    # expires and mamamia redelivers to the same consumer group.
    from switchboard.broker import Broker
    from switchboard.egress import Handler
    paths = dict(mamamia_db_path=str(tmp_path / "e.db"),
                 switchboard_db_path=str(tmp_path / "s.db"))

    started = asyncio.Event()

    async def hang(event, ctx):
        started.set()
        await asyncio.sleep(30)          # never settles; cancelled by stop()

    class Hanger:
        name = "grp"; filter = None
        def context(self): return None
        handlers = [Handler(name="h", filter=lambda e: True, handle=hang,
                            timeout_s=60, lease_s=0.5)]

    b1 = Broker(wait_ms=50, reaper_interval=3600.0, **paths)
    b1.attach(Hanger()); await b1.start()
    await b1.publish(EventInput(kind="k", source="github", payload={}))
    await asyncio.wait_for(started.wait(), timeout=5)   # in-flight, lease held
    await b1.stop()                                     # crash: cancel mid-dispatch

    got = []

    async def recover(event, ctx):
        got.append(event.id)

    class Recover:
        name = "grp"; filter = None       # SAME group_id 'grp/h'
        def context(self): return None
        handlers = [Handler(name="h", filter=lambda e: True, handle=recover,
                            timeout_s=1.0, lease_s=1.0)]

    b2 = Broker(wait_ms=50, reaper_interval=3600.0, **paths)
    b2.attach(Recover()); await b2.start()
    try:
        await _wait_for(lambda: len(got) >= 1, timeout=10)   # lease (0.5s) lapses → redeliver
        assert len(got) == 1
    finally:
        await b2.stop()
```

> **Implementer note:** the group_id is `"<egress>/<handler>"`. The recovery
> egress reuses `name="grp"` and handler `name="h"` so its group_id matches the
> crashed run's — that is what makes the same message redeliver to it. Keep both
> names identical. The hang handler's `timeout_s` is large and `lease_s` small on
> purpose: the crash is the cancellation, and the short lease is what makes the
> orphaned message reacquirable quickly after restart.

- [ ] **Step 2: Run to verify they fail**

Run: `python -m pytest tests/test_broker.py -v`
Expected: dispatch tests FAIL with `NotImplementedError` from `_consume`.

- [ ] **Step 3: Replace `_consume` in `switchboard/broker.py`**

```python
    async def _consume(self, egress: Egress, handler) -> None:
        group_id = f"{egress.name}/{handler.name}"
        orch = self._registry.get_orchestrator(LOG_ID)
        ctx = Ctx(publish=self.publish, egress=egress.context())
        timeout_s = handler.timeout_s or self._default_timeout_s
        lease_s = handler.lease_s or timeout_s * 2

        def passes(event: Event) -> bool:
            if egress.filter is not None and not egress.filter(event):
                return False
            return handler.filter(event)

        while self._running:
            try:
                msg = await self._registry.acquire_blocking(
                    LOG_ID, group_id, self._instance_id,
                    duration=lease_s, wait_ms=self._wait_ms,
                )
            except asyncio.CancelledError:
                raise
            if msg is None:
                continue

            event = Event(**msg.payload)
            if not passes(event):
                await orch.settle(LOG_ID, group_id, msg.id, self._instance_id,
                                  outcome=Outcome.SUCCESS)
                continue

            try:
                async with asyncio.timeout(timeout_s):
                    await handler.handle(event, ctx)
                await orch.settle(LOG_ID, group_id, msg.id, self._instance_id,
                                  outcome=Outcome.SUCCESS)
                self._fire("success", event, group_id)
            except asyncio.CancelledError:
                raise
            except PermanentError:
                await orch.settle(LOG_ID, group_id, msg.id, self._instance_id,
                                  outcome=Outcome.DEAD)
                self._fire("dead", event, group_id)
            except Exception:
                attempts = await orch.state_store.get_retry_count(
                    LOG_ID, group_id, msg.id)
                await orch.settle(LOG_ID, group_id, msg.id, self._instance_id,
                                  outcome=Outcome.RETRY, retry_after=backoff(attempts))
                self._fire("failed", event, group_id)
```

> **Note:** `asyncio.timeout` raises `TimeoutError`, a subclass of `Exception`,
> so a hung handler falls into the retry branch — exactly the desired behavior.
> `asyncio.CancelledError` is a `BaseException` (not caught by `except Exception`)
> and is re-raised so `stop()` can cancel the loop cleanly.

- [ ] **Step 4: Run to verify they pass**

Run: `python -m pytest tests/test_broker.py -v`
Expected: all broker tests pass. (The retry test may take a few seconds due to backoff.)

- [ ] **Step 5: Run the whole suite**

Run: `python -m pytest -q`
Expected: all passed.

- [ ] **Step 6: Commit**

```bash
git add switchboard/broker.py tests/test_broker.py tests/conftest.py
git commit -m "feat: per-handler dispatcher — filter, timeout, settle, retry, dead-letter, durability"
```

---

## Task 8: GitHub ingress — signature verification and event mapping

**Files:**
- Create: `switchboard/ingress/github.py` (the pure functions only)
- Test: `tests/test_github_map.py`, `tests/fixtures/github/*.json`

**Interfaces:**
- Produces:
  - `verify_signature(secret: str, body: bytes, header: str | None) -> bool` — HMAC-SHA256, constant-time.
  - `map_event(gh_event: str, payload: dict) -> EventInput | None` — maps a GitHub webhook to an `EventInput`, or `None` for an event we ignore. Sets `dedupe_key=None` here; the endpoint (Task 9) fills it from `X-GitHub-Delivery`.

- [ ] **Step 1: Add fixtures**

Create minimal recorded payloads (trimmed to fields used):

`tests/fixtures/github/pull_request.opened.json`:
```json
{"action": "opened", "repository": {"name": "home"}, "number": 7,
 "pull_request": {"title": "Add thing", "html_url": "https://x/pr/7", "merged": false}}
```

`tests/fixtures/github/pull_request.closed_merged.json`:
```json
{"action": "closed", "repository": {"name": "home"}, "number": 7,
 "pull_request": {"title": "Add thing", "html_url": "https://x/pr/7", "merged": true}}
```

`tests/fixtures/github/check_run.failed.json`:
```json
{"action": "completed", "repository": {"name": "home"},
 "check_run": {"name": "ci", "conclusion": "failure", "html_url": "https://x/run/1"}}
```

`tests/fixtures/github/check_run.success.json`:
```json
{"action": "completed", "repository": {"name": "home"},
 "check_run": {"name": "ci", "conclusion": "success", "html_url": "https://x/run/1"}}
```

- [ ] **Step 2: Write the failing test**

`tests/test_github_map.py`:
```python
import hmac
import hashlib
import json
from pathlib import Path
from switchboard.ingress.github import verify_signature, map_event

FIX = Path(__file__).parent / "fixtures" / "github"


def _load(name):
    return json.loads((FIX / name).read_text())


def test_verify_signature_ok():
    secret, body = "s3cret", b'{"a":1}'
    sig = "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    assert verify_signature(secret, body, sig) is True


def test_verify_signature_rejects_tampered():
    secret, body = "s3cret", b'{"a":1}'
    sig = "sha256=" + hmac.new(secret.encode(), b'{"a":2}', hashlib.sha256).hexdigest()
    assert verify_signature(secret, body, sig) is False


def test_verify_signature_rejects_missing_header():
    assert verify_signature("s", b"x", None) is False


def test_map_pr_opened():
    ei = map_event("pull_request", _load("pull_request.opened.json"))
    assert ei.kind == "github.home.pr.opened"
    assert ei.source == "github"
    assert ei.payload["number"] == 7


def test_map_pr_merged():
    ei = map_event("pull_request", _load("pull_request.closed_merged.json"))
    assert ei.kind == "github.home.pr.merged"


def test_map_check_run_failed():
    ei = map_event("check_run", _load("check_run.failed.json"))
    assert ei.kind == "github.home.check_run.failed"


def test_map_check_run_success_is_ignored():
    assert map_event("check_run", _load("check_run.success.json")) is None


def test_map_unknown_event_is_ignored():
    assert map_event("star", {"repository": {"name": "home"}}) is None
```

- [ ] **Step 3: Run to verify it fails**

Run: `python -m pytest tests/test_github_map.py -v`
Expected: FAIL, module not found.

- [ ] **Step 4: Write `switchboard/ingress/github.py` (pure functions)**

```python
import hashlib
import hmac

from switchboard.event import EventInput


def verify_signature(secret: str, body: bytes, header: str | None) -> bool:
    """Constant-time HMAC-SHA256 check against the X-Hub-Signature-256 header
    (format 'sha256=<hex>')."""
    if not header or not header.startswith("sha256="):
        return False
    expected = "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, header)


def map_event(gh_event: str, payload: dict) -> EventInput | None:
    """Translate a GitHub webhook (event type + payload) into an EventInput, or
    None for events we deliberately ignore. `dedupe_key` is filled by the caller
    from X-GitHub-Delivery."""
    repo = payload.get("repository", {}).get("name", "unknown")

    if gh_event == "pull_request":
        action = payload.get("action")
        if action == "closed" and payload.get("pull_request", {}).get("merged"):
            action = "merged"
        if action in {"opened", "closed", "merged"}:
            return _event(f"github.{repo}.pr.{action}", payload)
        if action == "review_requested":
            return _event(f"github.{repo}.review.requested", payload)
        return None

    if gh_event == "pull_request_review" and payload.get("action") == "submitted":
        state = payload.get("review", {}).get("state", "commented")
        return _event(f"github.{repo}.review.{state}", payload)

    if gh_event == "issues" and payload.get("action") in {"opened", "closed"}:
        return _event(f"github.{repo}.issue.{payload['action']}", payload)

    if gh_event == "check_run" and payload.get("action") == "completed":
        if payload.get("check_run", {}).get("conclusion") == "failure":
            return _event(f"github.{repo}.check_run.failed", payload)
        return None

    return None


def _event(kind: str, payload: dict) -> EventInput:
    return EventInput(kind=kind, source="github", payload=payload)
```

- [ ] **Step 5: Run to verify it passes**

Run: `python -m pytest tests/test_github_map.py -v`
Expected: all passed.

- [ ] **Step 6: Commit**

```bash
git add switchboard/ingress/github.py tests/test_github_map.py tests/fixtures/github/
git commit -m "feat: GitHub HMAC verification and webhook->event mapping"
```

---

## Task 9: GitHub ingress — HTTP endpoint

**Files:**
- Modify: `switchboard/ingress/github.py` (add `GitHubIngress`)
- Test: `tests/test_github_endpoint.py`

**Interfaces:**
- Consumes: `verify_signature`, `map_event`, the broker's `publish` callable (`Callable[[EventInput], Awaitable[PublishResult]]`).
- Produces: `class GitHubIngress`:
  - `GitHubIngress(secret: str, *, host="0.0.0.0", port=8080)`
  - `app` — a Starlette app with `POST /webhook` and `GET /health` (usable directly by `httpx`/TestClient without binding a port).
  - `async start(self, publish) -> None` / `async stop(self) -> None` — bind uvicorn (used by the real app; tests drive `app` directly).

- [ ] **Step 1: Write the failing test**

`tests/test_github_endpoint.py`:
```python
import hmac, hashlib, json
from pathlib import Path
import pytest
from starlette.testclient import TestClient
from switchboard.ingress.github import GitHubIngress
from switchboard.event import PublishResult

FIX = Path(__file__).parent / "fixtures" / "github"
SECRET = "s3cret"


def _sign(body: bytes) -> str:
    return "sha256=" + hmac.new(SECRET.encode(), body, hashlib.sha256).hexdigest()


class Spy:
    def __init__(self): self.calls = []
    async def publish(self, ei):
        self.calls.append(ei)
        return PublishResult(status="accepted", event_id="E1")


@pytest.fixture
def client_and_spy():
    spy = Spy()
    ingress = GitHubIngress(secret=SECRET)
    ingress.bind(spy.publish)          # inject publish without starting uvicorn
    return TestClient(ingress.app), spy


def test_health(client_and_spy):
    client, _ = client_and_spy
    assert client.get("/health").status_code == 200


def test_valid_pr_opened_publishes(client_and_spy):
    client, spy = client_and_spy
    body = (FIX / "pull_request.opened.json").read_bytes()
    r = client.post("/webhook", content=body, headers={
        "X-Hub-Signature-256": _sign(body),
        "X-GitHub-Event": "pull_request",
        "X-GitHub-Delivery": "d-123",
    })
    assert r.status_code == 200
    assert len(spy.calls) == 1
    assert spy.calls[0].kind == "github.home.pr.opened"
    assert spy.calls[0].dedupe_key == "d-123"


def test_bad_signature_401_no_publish(client_and_spy):
    client, spy = client_and_spy
    body = (FIX / "pull_request.opened.json").read_bytes()
    r = client.post("/webhook", content=body, headers={
        "X-Hub-Signature-256": "sha256=deadbeef",
        "X-GitHub-Event": "pull_request",
        "X-GitHub-Delivery": "d-1",
    })
    assert r.status_code == 401
    assert spy.calls == []


def test_ignored_event_200_no_publish(client_and_spy):
    client, spy = client_and_spy
    body = (FIX / "check_run.success.json").read_bytes()
    r = client.post("/webhook", content=body, headers={
        "X-Hub-Signature-256": _sign(body),
        "X-GitHub-Event": "check_run",
        "X-GitHub-Delivery": "d-2",
    })
    assert r.status_code == 200
    assert spy.calls == []


def test_malformed_json_400(client_and_spy):
    client, spy = client_and_spy
    body = b"{not json"
    r = client.post("/webhook", content=body, headers={
        "X-Hub-Signature-256": _sign(body),
        "X-GitHub-Event": "pull_request",
        "X-GitHub-Delivery": "d-3",
    })
    assert r.status_code == 400
    assert spy.calls == []
```

- [ ] **Step 2: Run to verify it fails**

Run: `python -m pytest tests/test_github_endpoint.py -v`
Expected: FAIL (`GitHubIngress` has no `bind`/`app`).

- [ ] **Step 3: Add `GitHubIngress` to `switchboard/ingress/github.py`**

Append:
```python
import json as _json

from starlette.applications import Starlette
from starlette.responses import JSONResponse, PlainTextResponse
from starlette.routing import Route


class GitHubIngress:
    name = "github"

    def __init__(self, secret: str, *, host: str = "0.0.0.0", port: int = 8080):
        self._secret = secret
        self._host = host
        self._port = port
        self._publish = None
        self._server = None
        self.app = Starlette(routes=[
            Route("/webhook", self._webhook, methods=["POST"]),
            Route("/health", lambda request: PlainTextResponse("ok"), methods=["GET"]),
        ])

    def bind(self, publish) -> None:
        """Inject the broker's publish callable. Called by start(); exposed so
        tests can drive `app` without binding a port."""
        self._publish = publish

    async def _webhook(self, request):
        body = await request.body()
        sig = request.headers.get("X-Hub-Signature-256")
        if not verify_signature(self._secret, body, sig):
            return JSONResponse({"error": "invalid signature"}, status_code=401)
        try:
            payload = _json.loads(body)
        except ValueError:
            return JSONResponse({"error": "malformed json"}, status_code=400)

        gh_event = request.headers.get("X-GitHub-Event", "")
        ei = map_event(gh_event, payload)
        if ei is None:
            return JSONResponse({"status": "ignored"}, status_code=200)

        ei.dedupe_key = request.headers.get("X-GitHub-Delivery")
        ei.meta = {"delivery": ei.dedupe_key or "", "depth": "0"}
        result = await self._publish(ei)
        return JSONResponse({"status": result.status, "event_id": result.event_id},
                            status_code=200)

    async def start(self, publish) -> None:
        import uvicorn
        self.bind(publish)
        config = uvicorn.Config(self.app, host=self._host, port=self._port, log_level="info")
        self._server = uvicorn.Server(config)
        await self._server.serve()

    async def stop(self) -> None:
        if self._server is not None:
            self._server.should_exit = True
```

- [ ] **Step 4: Run to verify it passes**

Run: `python -m pytest tests/test_github_endpoint.py -v`
Expected: all passed.

- [ ] **Step 5: Run the whole suite**

Run: `python -m pytest -q`
Expected: all passed.

- [ ] **Step 6: Commit**

```bash
git add switchboard/ingress/github.py tests/test_github_endpoint.py
git commit -m "feat: GitHub webhook HTTP endpoint (Starlette) with /health"
```

---

## Task 10: CLI — dead-letter inspection

**Files:**
- Create: `switchboard/cli.py`
- Test: `tests/test_cli.py`

**Interfaces:**
- Consumes: mamamia `connect`, `SQLiteStateStore`, `MessageState`; the event log.
- Produces:
  - `async def list_dead_letters(mamamia_db_path: str) -> list[dict]` — the retained `DEAD` `(group_id, message_id)` rows joined to the stored event (kind, id).
  - `def main(argv=None) -> int` — argparse; subcommand `dead-letters --db <path>` prints JSON lines.

- [ ] **Step 1: Write the failing test**

`tests/test_cli.py`:
```python
import asyncio
from switchboard.cli import list_dead_letters
from switchboard.broker import Broker
from switchboard.event import EventInput
from switchboard.egress import Handler
from switchboard.errors import PermanentError


class Killer:
    name = "k"; filter = None
    def context(self): return None
    async def _h(self, event, ctx): raise PermanentError("x")
    @property
    def handlers(self):
        return [Handler(name="h", filter=lambda e: True, handle=self._h,
                        timeout_s=0.2, lease_s=0.3)]


async def test_dead_letters_lists_dead(tmp_path):
    mm = str(tmp_path / "e.db")
    b = Broker(mamamia_db_path=mm, switchboard_db_path=str(tmp_path / "s.db"),
               wait_ms=50, reaper_interval=3600.0)
    b.attach(Killer()); await b.start()
    dead = []
    b.on("dead", lambda e, g: dead.append(e.id))
    try:
        await b.publish(EventInput(kind="github.home.pr.opened", source="github",
                                   payload={"n": 1}))
        for _ in range(500):
            if dead: break
            await asyncio.sleep(0.01)
        assert dead, "handler never dead-lettered"
    finally:
        await b.stop()

    rows = await list_dead_letters(mm)
    assert any(r["group_id"] == "k/h" and r["kind"] == "github.home.pr.opened"
               for r in rows)
```

- [ ] **Step 2: Run to verify it fails**

Run: `python -m pytest tests/test_cli.py -v`
Expected: FAIL, module not found.

- [ ] **Step 3: Write `switchboard/cli.py`**

```python
import argparse
import asyncio
import json
import sys

from mamamia.core.models import MessageState
from mamamia.server.db import connect

LOG_ID = "events"


async def list_dead_letters(mamamia_db_path: str) -> list[dict]:
    """Return retained DEAD deliveries joined to their stored event. Reads the
    mamamia database directly (read-only) — mamamia has no query API for this,
    and the schema is stable within a pinned version."""
    conn = await connect(mamamia_db_path)
    try:
        dead = conn.execute(
            "SELECT group_id, message_id FROM message_state WHERE state = ? "
            "ORDER BY message_id DESC",
            (MessageState.DEAD.value,),
        ).fetchall()
        rows = []
        for group_id, message_id in dead:
            payload = conn.execute(
                "SELECT payload FROM messages WHERE log_id = ? AND id = ?",
                (LOG_ID, message_id),
            ).fetchone()
            event = _decode(payload[0]) if payload else {}
            rows.append({
                "group_id": group_id,
                "message_id": message_id,
                "event_id": event.get("id"),
                "kind": event.get("kind"),
            })
        return rows
    finally:
        conn.close()


def _decode(blob):
    import msgpack
    return msgpack.unpackb(blob, raw=False)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="switchboard")
    sub = parser.add_subparsers(dest="cmd", required=True)
    dl = sub.add_parser("dead-letters", help="list retained dead-lettered events")
    dl.add_argument("--db", required=True, help="path to mamamia's events db")
    args = parser.parse_args(argv)

    if args.cmd == "dead-letters":
        rows = asyncio.run(list_dead_letters(args.db))
        for r in rows:
            sys.stdout.write(json.dumps(r) + "\n")
        return 0
    return 1
```

- [ ] **Step 4: Run to verify it passes**

Run: `python -m pytest tests/test_cli.py -v`
Expected: passed.

- [ ] **Step 5: Commit**

```bash
git add switchboard/cli.py tests/test_cli.py
git commit -m "feat: `switchboard dead-letters` CLI"
```

---

## Task 11: Application wiring entrypoint

**Files:**
- Create: `switchboard/app.py`
- Test: `tests/test_app.py`

**Interfaces:**
- Consumes: `Broker`, `LoggerEgress`, `GitHubIngress`; env vars.
- Produces:
  - `def build(config: dict) -> tuple[Broker, GitHubIngress]` — wires a broker + logger egress + github ingress from a config dict (no I/O).
  - `async def run() -> None` — reads env, builds, starts, serves the ingress. Entrypoint for the container.

- [ ] **Step 1: Write the failing test**

`tests/test_app.py`:
```python
from switchboard.app import build


def test_build_wires_logger_and_github(tmp_path):
    broker, ingress = build({
        "mamamia_db_path": str(tmp_path / "e.db"),
        "switchboard_db_path": str(tmp_path / "s.db"),
        "github_secret": "s3cret",
        "max_log_messages": 10_000,
    })
    assert "logger" in broker._egresses
    assert ingress.name == "github"
    assert ingress._secret == "s3cret"
```

- [ ] **Step 2: Run to verify it fails**

Run: `python -m pytest tests/test_app.py -v`
Expected: FAIL, module not found.

- [ ] **Step 3: Write `switchboard/app.py`**

```python
import asyncio
import os

from switchboard.broker import Broker
from switchboard.egress import LoggerEgress
from switchboard.ingress.github import GitHubIngress


def build(config: dict) -> tuple[Broker, GitHubIngress]:
    broker = Broker(
        mamamia_db_path=config["mamamia_db_path"],
        switchboard_db_path=config["switchboard_db_path"],
        max_log_messages=config.get("max_log_messages", 10_000),
    )
    broker.attach(LoggerEgress(filter=lambda e: e.source == "github"))
    ingress = GitHubIngress(
        secret=config["github_secret"],
        host=config.get("host", "0.0.0.0"),
        port=int(config.get("port", 8080)),
    )
    return broker, ingress


async def run() -> None:
    data_dir = os.environ.get("SB_DATA_DIR", "/data")
    config = {
        "mamamia_db_path": os.path.join(data_dir, "events.db"),
        "switchboard_db_path": os.path.join(data_dir, "switchboard.db"),
        "github_secret": os.environ["GITHUB_WEBHOOK_SECRET"],
        "max_log_messages": int(os.environ.get("SB_MAX_LOG_MESSAGES", "10000")),
        "port": int(os.environ.get("SB_PORT", "8080")),
    }
    broker, ingress = build(config)
    await broker.start()
    try:
        await ingress.start(broker.publish)   # serves until cancelled
    finally:
        await broker.stop()


if __name__ == "__main__":
    asyncio.run(run())
```

- [ ] **Step 4: Run to verify it passes**

Run: `python -m pytest tests/test_app.py -v`
Expected: passed.

- [ ] **Step 5: Run the whole suite**

Run: `python -m pytest -q`
Expected: all passed.

- [ ] **Step 6: Commit**

```bash
git add switchboard/app.py tests/test_app.py
git commit -m "feat: application wiring entrypoint (broker + logger + github)"
```

---

## Task 12: Deployment — Docker, Compose, env, README

**Files:**
- Create: `Dockerfile`, `docker-compose.yml`, `.env.example`, `README.md`

**Interfaces:**
- Produces: a `linux/arm64`-buildable image that runs `python -m switchboard.app`, a compose file mounting a data volume and reading `.env`, and docs.

> **mamamia in the image:** both repos are private and co-developed. For v1 the
> image installs mamamia from the pinned tag. Two supported options — pick per
> your CI's auth:
> - vendor a built wheel: `pip install ./vendor/mamamia-0.2.0-py3-none-any.whl`, or
> - install from git: `pip install "mamamia @ git+https://<token>@github.com/yellowpages-ink/mamamia@v0.2.0"`.
>
> The Dockerfile below uses the vendored-wheel path (no build-time secrets).
> Build the wheel once with `python -m build` in the mamamia checkout and copy it
> into `vendor/`.

- [ ] **Step 1: Write `Dockerfile`**

```dockerfile
FROM python:3.12-slim

WORKDIR /app

# mamamia (pinned v0.2.0) as a vendored wheel — see README.
COPY vendor/ /app/vendor/
COPY pyproject.toml /app/
COPY switchboard/ /app/switchboard/

RUN pip install --no-cache-dir /app/vendor/mamamia-0.2.0-py3-none-any.whl \
    && pip install --no-cache-dir .

ENV SB_DATA_DIR=/data
VOLUME ["/data"]
EXPOSE 8080

CMD ["python", "-m", "switchboard.app"]
```

- [ ] **Step 2: Write `docker-compose.yml`**

```yaml
services:
  switchboard:
    build: .
    restart: unless-stopped
    env_file: .env
    ports:
      - "8080:8080"
    volumes:
      - ./data:/data

  # Cloudflare Tunnel — outbound-only ingress for the GitHub webhook.
  tunnel:
    image: cloudflare/cloudflared:latest
    restart: unless-stopped
    command: tunnel run
    env_file: .env        # TUNNEL_TOKEN=...
```

- [ ] **Step 3: Write `.env.example`**

```bash
# GitHub webhook HMAC secret (Settings → Webhooks → Secret)
GITHUB_WEBHOOK_SECRET=replace-me

# Cloudflare Tunnel token (Zero Trust → Access → Tunnels)
TUNNEL_TOKEN=replace-me

# Optional
SB_MAX_LOG_MESSAGES=10000
SB_PORT=8080
```

- [ ] **Step 4: Write `README.md`**

```markdown
# Switchboard

Event-driven relay engine over [mamamia](https://github.com/yellowpages-ink/mamamia).
v1 relays GitHub repository activity into a structured log (Discord egress is v1.1).

## Design
See [docs/superpowers/specs/2026-07-21-switchboard-design.md](docs/superpowers/specs/2026-07-21-switchboard-design.md).

## Develop
```bash
python3.12 -m venv venv && . venv/bin/activate
pip install -e ../mamamia          # co-developed sibling checkout
pip install -e ".[dev]"
python -m pytest -q
```

## Run (Docker, on the Pi)
1. Build the mamamia wheel and vendor it:
   ```bash
   (cd ../mamamia && python -m build)
   mkdir -p vendor && cp ../mamamia/dist/mamamia-0.2.0-py3-none-any.whl vendor/
   ```
2. `cp .env.example .env` and fill in `GITHUB_WEBHOOK_SECRET` and `TUNNEL_TOKEN`
   (`chmod 600 .env`).
3. `docker compose up -d --build`
4. Point the GitHub webhook at the tunnel hostname's `/webhook`, content-type
   `application/json`, with the same secret.

## Inspect dead letters
```bash
python -m switchboard.cli dead-letters --db ./data/events.db
```
```

- [ ] **Step 5: Add `vendor/` to `.gitignore` (wheels are build artifacts)**

Append to `.gitignore`:
```
vendor/
data/
```

- [ ] **Step 6: Commit**

```bash
git add Dockerfile docker-compose.yml .env.example README.md .gitignore
git commit -m "chore: Docker image, Compose with Cloudflare Tunnel, env, README"
```

---

## Final verification

- [ ] **Run the full suite one last time**

Run: `python -m pytest -q`
Expected: all passed.

- [ ] **Sanity-check the vertical slice end to end** (optional manual)

Run a throwaway script: build the broker + logger egress in-process, `publish` a
fake GitHub-shaped event, and confirm a JSON line appears on stdout and the
message reaches `PROCESSED` (no redelivery). This exercises webhook-map → publish
→ log → lease → dispatch → logger with no network.

---

## Notes for the executor

- **Ordering matters in `publish`** — append before recording `seen`. Do not
  "optimize" it into record-first; that reintroduces the loss window the design
  rejects.
- **Group ids are `"<egress>/<handler>"`** and are how a redelivery finds the
  same consumer across a restart — keep egress/handler names stable.
- **`asyncio.timeout` → retry** (TimeoutError is an `Exception`); **`PermanentError`
  → dead**; **`CancelledError` propagates** to stop the loop. Keep the `except`
  order exactly as written (Cancelled, Permanent, then Exception).
- The retry test genuinely waits on backoff (base 1s); keep its timeout generous.
- mamamia's `synchronous=FULL` on the Pi means each settle fsyncs — fine at this
  volume (validated ~293 durable ops/s), but do not add per-message extra writes.
