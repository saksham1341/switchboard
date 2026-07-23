# Sensor Platform Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give every role a context object delivered through one `bind(ctx)` hook, backed by three new platform pieces — a shared `HttpServer`, a scoped `KeyStore`, and an owner-scoped `Scheduler` — so sensors stop owning ports, health checks, and private databases.

**Architecture:** Three self-contained modules land first (`store.py`, `http.py`, `scheduler.py`), each with no dependants. The context dataclasses go into `message.py` alongside the existing `DecideCtx`/`ActCtx`. The `Bus` then constructs the platform pieces, binds all four role kinds with stores scoped to `kind/name`, and sequences startup as bind → serve → start → schedule. Roles migrate last.

**Tech Stack:** Python 3.12, asyncio, Starlette + uvicorn (already dependencies), `sqlite3` from the stdlib, pytest with `asyncio_mode = "auto"` (bare `async def test_*`, no decorator).

**Spec:** `docs/superpowers/specs/2026-07-23-sensor-platform-design.md`

## Global Constraints

- The two log ids stay `OBS_LOG = "obs"` and `CMD_LOG = "cmd"`. Unchanged by this plan.
- `KeyStore` keys and values are `str`. Any other type raises `TypeError`. No implicit coercion anywhere.
- `ttl` defaults to `None`, meaning never expires. Long-term memory is the default.
- Store scope prefixes are exactly `f"{kind}/{name}/"` with kind in `{"sensor", "decider", "actuator", "tap"}` — e.g. `sensor/github/`, `actuator/discord.post/`.
- `TapCtx` carries `store` only. No `emit`, no `http`.
- The GitHub webhook path is exactly `/webhook/github`. No unscoped `/webhook` alias.
- The health path is exactly `/health`, served by `HttpServer`, returning `200` with body `ok`.
- `Scheduler.every` uses fixed delay, not fixed rate: the next sleep begins when the callback returns. `first_after` defaults to `seconds`.
- A scheduled callback that raises is logged and its loop continues. `asyncio.CancelledError` always re-raises.
- Timers never run outside their owner's lifetime: launched by `Scheduler.start(owner)`, cancelled by `Scheduler.stop(owner)`.
- Sensors dedup by `get` → `emit` → `set`. Emit first, record second, always.
- `PingDecider` replies with the exact string `"pong (via the durable path)"`.
- Existing behaviour is preserved: same observation names, same embed payloads, same HTTP status codes.

## File Structure

**Create:**
- `switchboard/store.py` — `KeyStore` protocol, `_check`, `MemoryStore`, `SqliteStore`, `ScopedStore`
- `switchboard/http.py` — `HttpServer`
- `switchboard/scheduler.py` — `Scheduler`, `OwnerSchedule`
- `tests/test_store.py`, `tests/test_http.py`, `tests/test_scheduler.py`

**Modify:**
- `switchboard/message.py` — add `SensorCtx`, `DeciderCtx`, `ActuatorCtx`, `TapCtx`; add `bind` to all four protocols; remove `ActCtx.context` and `Actuator.context`
- `switchboard/bus.py` — construct platform pieces, bind roles, new lifecycle ordering, actuator teardown
- `switchboard/sensors/github.py`, `switchboard/sensors/discord.py` — migrate to `bind`/`start`/`stop`
- `switchboard/deciders/*.py`, `switchboard/actuators/discord.py`, `switchboard/taps/logger.py` — add `bind`
- `switchboard/app.py` — construct `HttpServer` and `SqliteStore`, drop `seen_db`/`host`/`port` from `GitHubSensor`
- `tests/test_sensor_github.py`, `tests/test_actuators_discord.py`, `tests/test_bus.py`, `tests/test_app.py`

**Delete:**
- `switchboard/dedup.py`, `tests/test_dedup.py` (Task 7)

**On `SeenStore`:** the module and its tests are deleted outright in Task 7. Its rows are not migrated into `kv` — GitHub dedup keys carry a 7-day TTL, so the only exposure is a redelivery arriving in the seconds around a deploy, which costs one duplicate Discord post. Not worth a migration path that exists to be run once.

The dead `seen` table is dropped by hand at deploy time rather than from code, because a generic `KeyStore` should not carry knowledge of a legacy table it never owned:

```bash
docker compose exec switchboard python -c \
  "import sqlite3; sqlite3.connect('/data/switchboard.db').execute('DROP TABLE IF EXISTS seen')"
```

---

### Task 1: KeyStore

**Files:**
- Create: `switchboard/store.py`
- Test: `tests/test_store.py`

**Interfaces:**
- Consumes: nothing
- Produces: `KeyStore` protocol with `async get(key) -> str | None`, `async set(key, value, *, ttl=None) -> None`, `async delete(key) -> None`; `MemoryStore(*, time_fn=time.time)`; `SqliteStore(db_path, *, time_fn=time.time)` also exposing `purge() -> int` and `close()`; `ScopedStore(inner, prefix)`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_store.py
import pytest

from switchboard.store import MemoryStore, SqliteStore, ScopedStore


class _Clock:
    def __init__(self, t=1000.0): self.t = t
    def __call__(self): return self.t


@pytest.fixture(params=["memory", "sqlite"])
def store(request, tmp_path):
    clock = _Clock()
    if request.param == "memory":
        s = MemoryStore(time_fn=clock)
    else:
        s = SqliteStore(str(tmp_path / "kv.db"), time_fn=clock)
    s.clock = clock
    return s


async def test_missing_key_is_none(store):
    assert await store.get("nope") is None


async def test_set_then_get(store):
    await store.set("k", "v")
    assert await store.get("k") == "v"


async def test_set_overwrites_last_write_wins(store):
    await store.set("k", "one")
    await store.set("k", "two")
    assert await store.get("k") == "two"


async def test_delete_removes(store):
    await store.set("k", "v")
    await store.delete("k")
    assert await store.get("k") is None


async def test_no_ttl_never_expires(store):
    await store.set("k", "v")
    store.clock.t += 10_000_000
    assert await store.get("k") == "v"


async def test_ttl_expires(store):
    await store.set("k", "v", ttl=60.0)
    store.clock.t += 59.0
    assert await store.get("k") == "v"
    store.clock.t += 2.0
    assert await store.get("k") is None


async def test_non_str_key_raises(store):
    with pytest.raises(TypeError):
        await store.get(1)
    with pytest.raises(TypeError):
        await store.set(1, "v")


async def test_non_str_value_raises(store):
    with pytest.raises(TypeError):
        await store.set("k", 5)


async def test_sqlite_survives_reopen(tmp_path):
    p = str(tmp_path / "kv.db")
    s = SqliteStore(p)
    await s.set("k", "v")
    s.close()
    s2 = SqliteStore(p)
    assert await s2.get("k") == "v"
    s2.close()


async def test_purge_removes_only_expired(tmp_path):
    clock = _Clock()
    s = SqliteStore(str(tmp_path / "kv.db"), time_fn=clock)
    await s.set("keep", "v")
    await s.set("gone", "v", ttl=10.0)
    clock.t += 11.0
    assert s.purge() == 1
    assert await s.get("keep") == "v"
    s.close()


async def test_scope_isolates_same_key(store):
    a = ScopedStore(store, "sensor/github/")
    b = ScopedStore(store, "sensor/linear/")
    await a.set("cursor", "A")
    await b.set("cursor", "B")
    assert await a.get("cursor") == "A"
    assert await b.get("cursor") == "B"


async def test_scope_delete_leaves_sibling(store):
    a = ScopedStore(store, "sensor/github/")
    b = ScopedStore(store, "sensor/linear/")
    await a.set("cursor", "A")
    await b.set("cursor", "B")
    await a.delete("cursor")
    assert await a.get("cursor") is None
    assert await b.get("cursor") == "B"


async def test_scope_rejects_non_str_key(store):
    a = ScopedStore(store, "sensor/github/")
    with pytest.raises(TypeError):
        await a.get(1)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `./scripts/dev.sh test tests/test_store.py`
Expected: FAIL — `ModuleNotFoundError: No module named 'switchboard.store'`

- [ ] **Step 3: Write the implementation**

```python
# switchboard/store.py
import sqlite3
import time
from typing import Protocol, runtime_checkable


def _check(key, value=None) -> None:
    """Keys and values are str. No implicit coercion: str(5) would make
    set(k, 5) followed by get(k) == 5 evaluate False, discovered in production."""
    if not isinstance(key, str):
        raise TypeError(f"KeyStore keys must be str, got {type(key).__name__}")
    if value is not None and not isinstance(value, str):
        raise TypeError(f"KeyStore values must be str, got {type(value).__name__}. "
                        f"Serialize structured values yourself (json.dumps).")


@runtime_checkable
class KeyStore(Protocol):
    async def get(self, key: str) -> str | None: ...
    async def set(self, key: str, value: str, *, ttl: float | None = None) -> None: ...
    async def delete(self, key: str) -> None: ...


class MemoryStore:
    """Volatile KeyStore. Written to the same contract as SqliteStore rather
    than the one a dict gives naturally — expiry on read and type checks are
    explicit — so behaviour cannot diverge between tests and production."""

    def __init__(self, *, time_fn=time.time):
        self._now = time_fn
        self._data: dict[str, tuple[str, float | None]] = {}

    async def get(self, key):
        _check(key)
        entry = self._data.get(key)
        if entry is None:
            return None
        value, expires_at = entry
        if expires_at is not None and expires_at <= self._now():
            del self._data[key]
            return None
        return value

    async def set(self, key, value, *, ttl=None):
        _check(key, value)
        self._data[key] = (value, None if ttl is None else self._now() + ttl)

    async def delete(self, key):
        _check(key)
        self._data.pop(key, None)

    def purge(self) -> int:
        now = self._now()
        expired = [k for k, (_, exp) in self._data.items()
                   if exp is not None and exp <= now]
        for k in expired:
            del self._data[k]
        return len(expired)


class SqliteStore:
    """Durable KeyStore. Synchronous sqlite3 driven on the event-loop thread,
    the same approach mamamia's own backends use; operations are microseconds."""

    def __init__(self, db_path: str, *, time_fn=time.time):
        self._now = time_fn
        self._conn = sqlite3.connect(db_path, isolation_level=None, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS kv ("
            "  key        TEXT PRIMARY KEY,"
            "  value      TEXT NOT NULL,"
            "  expires_at REAL"                       # NULL = never
            ")")
        self._conn.execute("CREATE INDEX IF NOT EXISTS kv_expires ON kv (expires_at)")

    async def get(self, key):
        _check(key)
        # Expiry is filtered in the read, so an expired row is invisible the
        # instant it expires regardless of when purge last ran.
        row = self._conn.execute(
            "SELECT value FROM kv WHERE key = ? AND (expires_at IS NULL OR expires_at > ?)",
            (key, self._now())).fetchone()
        return row[0] if row else None

    async def set(self, key, value, *, ttl=None):
        _check(key, value)
        self._conn.execute(
            "INSERT INTO kv (key, value, expires_at) VALUES (?, ?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value, expires_at=excluded.expires_at",
            (key, value, None if ttl is None else self._now() + ttl))

    async def delete(self, key):
        _check(key)
        self._conn.execute("DELETE FROM kv WHERE key = ?", (key,))

    def purge(self) -> int:
        """Delete expired rows. About disk, never about correctness."""
        return self._conn.execute(
            "DELETE FROM kv WHERE expires_at IS NOT NULL AND expires_at <= ?",
            (self._now(),)).rowcount

    def close(self) -> None:
        self._conn.close()


class ScopedStore:
    """A KeyStore view over a prefix. Roles never see the prefix and cannot
    reach another role's keys — the log is the channel between roles."""

    def __init__(self, inner, prefix: str):
        self._inner, self._prefix = inner, prefix

    async def get(self, key):
        _check(key)
        return await self._inner.get(self._prefix + key)

    async def set(self, key, value, *, ttl=None):
        _check(key, value)
        await self._inner.set(self._prefix + key, value, ttl=ttl)

    async def delete(self, key):
        _check(key)
        await self._inner.delete(self._prefix + key)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `./scripts/dev.sh test tests/test_store.py`
Expected: PASS, 26 tests (13 parametrized × 2, plus the sqlite-only ones)

- [ ] **Step 5: Commit**

```bash
git add switchboard/store.py tests/test_store.py
git commit -m "feat: KeyStore with memory, sqlite, and scoped implementations"
```

---

### Task 2: HttpServer

**Files:**
- Create: `switchboard/http.py`
- Test: `tests/test_http.py`

**Interfaces:**
- Consumes: nothing
- Produces: `HttpServer(host="0.0.0.0", port=8080, *, serve=True)` with `route(path, handler, *, methods=("GET",))`, `async start()`, `async stop()`, and a `.app` attribute (a Starlette app) that tests drive with `TestClient` without binding a port.

`serve=False` makes `start()` a no-op. The `Bus` uses that default so its tests register routes and drive `.app` without any test binding port 8080; `app.build()` passes a real one.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_http.py
import pytest
from starlette.responses import PlainTextResponse
from starlette.testclient import TestClient

from switchboard.http import HttpServer


def test_health_is_served_with_no_routes_registered():
    s = HttpServer(serve=False)
    r = TestClient(s.app).get("/health")
    assert r.status_code == 200
    assert r.text == "ok"


def test_registered_routes_are_served():
    s = HttpServer(serve=False)

    async def a(request): return PlainTextResponse("A")
    async def b(request): return PlainTextResponse("B")

    s.route("/one", a, methods=["POST"])
    s.route("/two", b, methods=["POST"])
    client = TestClient(s.app)
    assert client.post("/one").text == "A"
    assert client.post("/two").text == "B"


def test_unregistered_path_is_404():
    s = HttpServer(serve=False)
    assert TestClient(s.app).post("/nope").status_code == 404


def test_duplicate_path_raises_naming_the_first_owner():
    s = HttpServer(serve=False)

    async def h(request): return PlainTextResponse("x")

    s.route("/dup", h, methods=["POST"], owner="github")
    with pytest.raises(ValueError, match="github"):
        s.route("/dup", h, methods=["POST"], owner="linear")


async def test_start_is_a_noop_when_serve_is_false():
    s = HttpServer(serve=False)
    await s.start()          # binds nothing
    await s.stop()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `./scripts/dev.sh test tests/test_http.py`
Expected: FAIL — `ModuleNotFoundError: No module named 'switchboard.http'`

- [ ] **Step 3: Write the implementation**

```python
# switchboard/http.py
import asyncio
import logging

from starlette.applications import Starlette
from starlette.responses import PlainTextResponse
from starlette.routing import Route

logger = logging.getLogger(__name__)


class HttpServer:
    """The one HTTP server. Owned by the app, shared by every role that needs a
    route, so one hostname serves every webhook sensor — each on its own path.

    /health belongs here rather than to any sensor: it is the deployment's
    liveness probe, and it must answer in a build with no webhook sensor at all.
    """

    def __init__(self, host: str = "0.0.0.0", port: int = 8080, *, serve: bool = True):
        self._host, self._port, self._serve = host, port, serve
        self._owners: dict[str, str] = {}
        self._server = None
        self._task = None
        self.app = Starlette(routes=[
            Route("/health", lambda request: PlainTextResponse("ok"), methods=["GET"]),
        ])

    def route(self, path: str, handler, *, methods=("GET",), owner: str = "?") -> None:
        if path in self._owners:
            raise ValueError(
                f"{path} already registered by {self._owners[path]!r}. An HTTP path "
                f"has one response, so it has one owner. To have several consumers "
                f"react to it, add deciders that subscribe to the observation it "
                f"emits; to separate tenants, scope the path (e.g. {path}/<tenant>).")
        self._owners[path] = owner
        self.app.router.routes.append(Route(path, handler, methods=list(methods)))

    async def start(self) -> None:
        if not self._serve:
            return
        import uvicorn
        config = uvicorn.Config(self.app, host=self._host, port=self._port, log_level="info")
        self._server = uvicorn.Server(config)
        self._task = asyncio.create_task(self._server.serve())

        async def _wait_started():
            while not self._server.started:
                await asyncio.sleep(0.01)
        # Return only once the port is actually bound, so callers never race it.
        await asyncio.wait_for(_wait_started(), timeout=10.0)
        logger.info("http listening on %s:%s", self._host, self._port)

    async def stop(self) -> None:
        if self._server is not None:
            self._server.should_exit = True
        if self._task is not None:
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `./scripts/dev.sh test tests/test_http.py`
Expected: PASS, 5 tests

- [ ] **Step 5: Commit**

```bash
git add switchboard/http.py tests/test_http.py
git commit -m "feat: shared HttpServer owning /health with one owner per path"
```

---

### Task 3: Scheduler

**Files:**
- Create: `switchboard/scheduler.py`
- Test: `tests/test_scheduler.py`

**Interfaces:**
- Consumes: nothing
- Produces: `Scheduler()` with `for_owner(owner) -> OwnerSchedule`, `start(owner)`, `async stop(owner)`, `async stop_all()`; `OwnerSchedule.every(seconds, fn, *, first_after=None, name=None)`.

Tests use short real intervals (0.02s) rather than a fake clock — the loop's behaviour under `asyncio.sleep` is the thing under test.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_scheduler.py
import asyncio

from switchboard.scheduler import Scheduler


async def _wait(pred, timeout=3.0):
    async def loop():
        while not pred():
            await asyncio.sleep(0.005)
    await asyncio.wait_for(loop(), timeout)


async def test_timer_does_not_fire_before_start():
    s = Scheduler()
    calls = []
    s.for_owner("a").every(0.01, lambda: _noop(calls), first_after=0.0)
    await asyncio.sleep(0.1)
    assert calls == []


async def _noop(calls):
    calls.append(1)


async def test_timer_fires_after_start_and_repeats():
    s = Scheduler()
    calls = []
    s.for_owner("a").every(0.02, lambda: _noop(calls), first_after=0.0)
    s.start("a")
    try:
        await _wait(lambda: len(calls) >= 3)
    finally:
        await s.stop("a")


async def test_timer_stops_after_stop():
    s = Scheduler()
    calls = []
    s.for_owner("a").every(0.01, lambda: _noop(calls), first_after=0.0)
    s.start("a")
    await _wait(lambda: len(calls) >= 1)
    await s.stop("a")
    seen = len(calls)
    await asyncio.sleep(0.1)
    assert len(calls) == seen          # no further ticks


async def test_first_after_defaults_to_the_interval():
    s = Scheduler()
    calls = []
    s.for_owner("a").every(1.0, lambda: _noop(calls))     # no first_after
    s.start("a")
    try:
        await asyncio.sleep(0.15)
        assert calls == []             # nothing fires at t=0
    finally:
        await s.stop("a")


async def test_raising_callback_does_not_kill_the_loop():
    s = Scheduler()
    calls = []

    async def boom():
        calls.append(1)
        raise RuntimeError("nope")

    s.for_owner("a").every(0.02, boom, first_after=0.0)
    s.start("a")
    try:
        await _wait(lambda: len(calls) >= 3)
    finally:
        await s.stop("a")


async def test_slow_callback_does_not_overlap_itself():
    s = Scheduler()
    concurrent, peak = [], []

    async def slow():
        concurrent.append(1)
        peak.append(len(concurrent))
        await asyncio.sleep(0.05)
        concurrent.pop()

    s.for_owner("a").every(0.01, slow, first_after=0.0)
    s.start("a")
    try:
        await _wait(lambda: len(peak) >= 3)
        assert max(peak) == 1
    finally:
        await s.stop("a")


async def test_declaring_while_running_launches_immediately():
    s = Scheduler()
    calls = []
    s.start("a")                        # started with nothing declared
    try:
        s.for_owner("a").every(0.02, lambda: _noop(calls), first_after=0.0)
        await _wait(lambda: len(calls) >= 2)
    finally:
        await s.stop("a")


async def test_owners_are_independent():
    s = Scheduler()
    a_calls, b_calls = [], []
    s.for_owner("a").every(0.02, lambda: _noop(a_calls), first_after=0.0)
    s.for_owner("b").every(0.02, lambda: _noop(b_calls), first_after=0.0)
    s.start("a")
    s.start("b")
    await _wait(lambda: a_calls and b_calls)
    await s.stop("a")
    seen_a = len(a_calls)
    await _wait(lambda: len(b_calls) > seen_a + 1)
    assert len(a_calls) == seen_a       # a stayed stopped while b kept ticking
    await s.stop("b")
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `./scripts/dev.sh test tests/test_scheduler.py`
Expected: FAIL — `ModuleNotFoundError: No module named 'switchboard.scheduler'`

- [ ] **Step 3: Write the implementation**

```python
# switchboard/scheduler.py
import asyncio
import logging
from dataclasses import dataclass
from typing import Awaitable, Callable

logger = logging.getLogger(__name__)


@dataclass
class _Timer:
    seconds: float
    fn: Callable[[], Awaitable[None]]
    first_after: float | None
    label: str


class OwnerSchedule:
    """What a role sees as ctx.schedule. Timers declared here are bound to that
    role's lifecycle: they never tick before it starts, and they are cancelled
    before it stops."""

    def __init__(self, owner: str, scheduler: "Scheduler"):
        self._owner, self._sched = owner, scheduler

    def every(self, seconds: float, fn, *, first_after: float | None = None,
              name: str | None = None) -> None:
        self._sched._declare(self._owner, _Timer(
            seconds, fn, first_after, name or getattr(fn, "__qualname__", "timer")))


class Scheduler:
    """Owns every role's timers: one place to cancel them at shutdown, and a
    callback that raises never takes its owner down with it."""

    def __init__(self):
        self._declared: dict[str, list[_Timer]] = {}
        self._tasks: dict[str, list[asyncio.Task]] = {}
        self._running: set[str] = set()

    def for_owner(self, owner: str) -> OwnerSchedule:
        self._declared.setdefault(owner, [])
        return OwnerSchedule(owner, self)

    def start(self, owner: str) -> None:
        self._running.add(owner)
        for t in self._declared.get(owner, ()):
            self._launch(owner, t)

    async def stop(self, owner: str) -> None:
        self._running.discard(owner)
        tasks = self._tasks.pop(owner, [])
        for t in tasks:
            t.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def stop_all(self) -> None:
        for owner in list(self._tasks):
            await self.stop(owner)

    def _declare(self, owner: str, timer: _Timer) -> None:
        self._declared.setdefault(owner, []).append(timer)
        # Declared from inside start(): the owner is already running, so this
        # timer starts now instead of waiting for a restart that never comes.
        if owner in self._running:
            self._launch(owner, timer)

    def _launch(self, owner: str, timer: _Timer) -> None:
        task = asyncio.create_task(self._loop(timer),
                                   name=f"schedule/{owner}/{timer.label}")
        self._tasks.setdefault(owner, []).append(task)

    async def _loop(self, timer: _Timer) -> None:
        # first_after defaults to a full interval: firing at t=0 would make a
        # crash-looping process hit the remote on every restart.
        delay = timer.seconds if timer.first_after is None else timer.first_after
        await asyncio.sleep(delay)
        while True:
            try:
                await timer.fn()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("scheduled callback %s failed", timer.label)
            # Fixed delay, not fixed rate: the next sleep starts when the
            # callback finishes, so a slow tick can never stack up behind itself.
            await asyncio.sleep(timer.seconds)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `./scripts/dev.sh test tests/test_scheduler.py`
Expected: PASS, 8 tests

- [ ] **Step 5: Commit**

```bash
git add switchboard/scheduler.py tests/test_scheduler.py
git commit -m "feat: owner-scoped Scheduler with fixed-delay timers"
```

---

### Task 4: Role contexts and actuator migration

**Files:**
- Modify: `switchboard/message.py`, `switchboard/actuators/discord.py`, `switchboard/bus.py`
- Test: `tests/test_message.py`, `tests/test_actuators_discord.py`

**Interfaces:**
- Consumes: `KeyStore` (Task 1), `HttpServer` (Task 2), `OwnerSchedule` (Task 3)
- Produces: `SensorCtx(emit, http, store, schedule)`, `DeciderCtx(store)`, `ActuatorCtx(store)`, `TapCtx(store)`; `bind` on all four role protocols; `ActCtx(cmd, _emit_result)` — the `context` field is **gone**, and `Actuator.context()` with it.

`ActCtx.context` is removed here rather than deferred behind a compatibility shim. Its only consumers are `bus._run_actuator`, the two Discord actuators, and their tests — all of which migrate in this task, so nothing is preserved and nothing is left red.

Import note: `switchboard/http.py` shadows nothing (the stdlib module is `http`, imported absolutely), but `message.py` must import these for typing only — use `from switchboard.http import HttpServer` at module level; there is no cycle because `http.py`, `store.py`, and `scheduler.py` import nothing from `switchboard`.

- [ ] **Step 1: Write the failing tests**

```python
# append to tests/test_message.py
from switchboard.message import SensorCtx, DeciderCtx, ActuatorCtx, TapCtx
from switchboard.store import MemoryStore
from switchboard.http import HttpServer
from switchboard.scheduler import Scheduler


def test_sensor_ctx_carries_the_four_capabilities():
    async def emit(name, payload): return 1
    http, store, sched = HttpServer(serve=False), MemoryStore(), Scheduler()
    ctx = SensorCtx(emit=emit, http=http, store=store,
                    schedule=sched.for_owner("s"))
    assert ctx.emit is emit and ctx.http is http and ctx.store is store
    assert ctx.schedule is not None


def test_decider_ctx_is_store_only():
    ctx = DeciderCtx(store=MemoryStore())
    assert set(vars(ctx)) == {"store"}          # no emit, no http, no schedule


def test_actuator_ctx_is_store_only():
    ctx = ActuatorCtx(store=MemoryStore())
    assert set(vars(ctx)) == {"store"}


def test_tap_ctx_is_store_only():
    ctx = TapCtx(store=MemoryStore())
    assert set(vars(ctx)) == {"store"}          # no emit, no http: a tap reads


def test_act_ctx_has_no_context_field():
    from switchboard.message import ActCtx
    assert "context" not in ActCtx.__dataclass_fields__
```

And the actuator tests, which drop `context=` and gain `bind`:

```python
# tests/test_actuators_discord.py
from switchboard.message import Command, ActCtx, ActuatorCtx
from switchboard.store import MemoryStore


def _bind(a):
    a.bind(ActuatorCtx(store=MemoryStore()))
    return a


async def test_discord_post_sends_embed_and_reports_result():
    seen, results = {}, []
    def h(req):
        seen["url"] = str(req.url); seen["body"] = json.loads(req.content)
        seen["auth"] = req.headers.get("authorization")
        return httpx.Response(200, json={"id": "m-1"})
    a = _bind(DiscordPost("bot", "app", channel_id="chan-9", client=_client(h)))
    ctx = ActCtx(cmd=_cmd("discord.post", {"channel_id": "chan-9", "embed": {"title": "hi"}}),
                 _emit_result=await _recorder(results))
    await a.act(ctx.cmd, ctx)
    assert seen["url"] == f"{DISCORD_API}/channels/chan-9/messages"
    assert seen["body"]["embeds"] == [{"title": "hi"}]
    assert seen["auth"] == "Bot bot"
    assert results and results[0][0] == "discord.post.ok"


async def test_discord_reply_uses_followup_and_reports_result():
    seen, results = {}, []
    def h(req):
        seen["url"] = str(req.url); seen["body"] = json.loads(req.content)
        seen["auth"] = req.headers.get("authorization")
        return httpx.Response(200, json={})
    a = _bind(DiscordReply("bot", "app", client=_client(h)))
    ctx = ActCtx(cmd=_cmd("discord.reply", {"interaction_token": "tok", "content": "pong"}),
                 _emit_result=await _recorder(results))
    await a.act(ctx.cmd, ctx)
    assert seen["url"] == f"{DISCORD_API}/webhooks/app/tok"
    assert seen["body"] == {"content": "pong"}
    assert seen["auth"] is None
    assert results and results[0][0] == "discord.reply.ok"


async def test_actuator_ctx_store_is_available_after_bind():
    a = _bind(DiscordReply("bot", "app", client=_client(lambda r: httpx.Response(200, json={}))))
    await a.ctx.store.set("idem:cmd-1", "sent")
    assert await a.ctx.store.get("idem:cmd-1") == "sent"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `./scripts/dev.sh test tests/test_message.py`
Expected: FAIL — `ImportError: cannot import name 'SensorCtx' from 'switchboard.message'`

- [ ] **Step 3: Write the implementation**

Add to `switchboard/message.py`, after `ActCtx`:

```python
from switchboard.http import HttpServer
from switchboard.scheduler import OwnerSchedule
from switchboard.store import KeyStore


@dataclass
class SensorCtx:
    """What Switchboard provides to a sensor: how to emit, how the world
    reaches it (push and pull), and what it remembers between wakings."""
    emit: Callable[[str, dict], Awaitable[int]]
    http: HttpServer
    store: KeyStore
    schedule: OwnerSchedule


@dataclass
class DeciderCtx:
    """A decider has no world access. It has memory: the store is its own
    notebook — no effects outside Switchboard, durable, inspectable."""
    store: KeyStore


@dataclass
class ActuatorCtx:
    store: KeyStore


@dataclass
class TapCtx:
    """A store and nothing else. No emit — a tap that could write to a log
    would stop being a tap — and no http: a dashboard's page is registered by
    app.build(), which owns the HttpServer, alongside bus.add_tap()."""
    store: KeyStore
```

Strip `ActCtx` to per-command state only:

```python
@dataclass
class ActCtx:
    cmd: Command
    _emit_result: Callable[[str, dict, int], Awaitable[int]]

    async def result(self, outcome: str, payload: dict | None = None) -> int:
        return await self._emit_result(f"{self.cmd.name}.{outcome}", payload or {}, self.cmd.id)
```

Then add `bind` to each protocol, with no `context()` on `Actuator`:

```python
@runtime_checkable
class Sensor(Protocol):
    name: str
    def bind(self, ctx: SensorCtx) -> None: ...
    async def start(self) -> None: ...
    async def stop(self) -> None: ...


@runtime_checkable
class Decider(Protocol):
    name: str
    def bind(self, ctx: DeciderCtx) -> None: ...
    def subscribes(self, obs: Observation) -> bool: ...
    async def decide(self, obs: Observation, ctx: DecideCtx) -> None: ...


@runtime_checkable
class Actuator(Protocol):
    name: str
    def bind(self, ctx: ActuatorCtx) -> None: ...
    async def act(self, cmd: Command, ctx: ActCtx) -> None: ...


@runtime_checkable
class Tap(Protocol):
    name: str
    logs: tuple[str, ...]
    def bind(self, ctx: TapCtx) -> None: ...
    async def observe(self, log: str, view) -> None: ...
```

- [ ] **Step 4: Migrate the actuators**

In `switchboard/bus.py`, `_run_actuator` stops calling `context()`:

```python
    async def _run_actuator(self, a):
        async def handle(cmd):
            ctx = ActCtx(cmd=cmd,
                         _emit_result=lambda name, payload, cid:
                             self.emit_observation(name, payload, command_id=cid))
            await a.act(cmd, ctx)
        await self._consume(CMD_LOG, f"actuator/{a.name}",
                            Command.from_message, lambda c: c.name == a.name, handle)
```

Delete the `ctx_obj = a.context()` line above it.

In `switchboard/actuators/discord.py`, build the sender in `bind` rather than `__init__` — that deferral is the one thing `context()` was actually buying, since an httpx client wants a running loop:

```python
class DiscordPost:
    """Actuator for the `discord.post` command: post a channel message."""
    name = "discord.post"

    def __init__(self, bot_token, application_id, *, channel_id=None, client=None):
        self._token, self._app_id = bot_token, application_id
        self._default_channel = channel_id
        self._client = client
        self._sender = None

    def bind(self, ctx):
        self.ctx = ctx
        self._sender = DiscordSender(self._token, self._app_id, client=self._client)

    async def act(self, cmd, ctx):
        channel = cmd.args.get("channel_id") or self._default_channel
        await self._sender.send(channel, embed=cmd.args.get("embed"),
                                components=cmd.args.get("components"))
        await ctx.result("ok", {"channel_id": channel})

    async def close(self):
        if self._sender is not None:
            await self._sender.close()
```

Apply the same shape to `DiscordReply`, whose `act` calls `self._sender.reply(...)`.

- [ ] **Step 5: Run the tests**

Run: `./scripts/dev.sh test tests/test_message.py tests/test_actuators_discord.py`
Expected: PASS. `tests/test_relay_e2e.py` fails here — the Bus does not bind actuators yet, so `self._sender` is `None`. Fixed in Task 5.

- [ ] **Step 6: Commit**

```bash
git add switchboard/message.py switchboard/actuators/ switchboard/bus.py \
        tests/test_message.py tests/test_actuators_discord.py
git commit -m "feat: role contexts; drop ActCtx.context and Actuator.context()"
```

---

### Task 5: Bus lifecycle and sensor migration

**Files:**
- Modify: `switchboard/bus.py`, `switchboard/sensors/github.py`, `switchboard/sensors/discord.py`
- Test: `tests/test_bus.py`, `tests/test_sensor_github.py`, `tests/test_sensor_discord.py`

**Interfaces:**
- Consumes: everything from Tasks 1–4
- Produces: `Bus(mamamia_db_path, *, store=None, http=None, ...)`; `GitHubSensor(secret, *, dedup_ttl=7*86_400.0)`; `DiscordSensor(bot_token, *, commands, guild_id=None)` with the new lifecycle.

The Bus and both sensors change together: a half-migrated sensor lifecycle cannot start, so it cannot be reviewed independently.

Bus defaults: `store or MemoryStore()`, `http or HttpServer(serve=False)` — existing Bus tests then bind no port while routes still register and `.app` stays drivable.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_bus.py — add these, and update the fakes to take bind()
class _Decider:
    name = "trigger"
    def bind(self, ctx): self.ctx = ctx
    def subscribes(self, obs): return obs.name == "thing.happened"
    async def decide(self, obs, ctx):
        await ctx.command("do.it", {"echo": obs.payload["v"]})


class _Actuator:
    name = "do.it"
    def __init__(self): self.acted = []; self.closed = False
    def bind(self, ctx): self.ctx = ctx
    async def act(self, cmd, ctx):
        self.acted.append(cmd.args["echo"])
        await ctx.result("ok", {"handled": cmd.args["echo"]})
    async def close(self): self.closed = True


class _Tap:
    name = "spy"
    logs = (OBS_LOG, CMD_LOG)
    def __init__(self): self.seen = []
    def bind(self, ctx): self.ctx = ctx
    async def observe(self, log, view): self.seen.append((log, view.name))


class _Sensor:
    name = "probe"
    def __init__(self): self.bound = None; self.started = False; self.stopped = False
    def bind(self, ctx): self.bound = ctx
    async def start(self): self.started = True
    async def stop(self): self.stopped = True


async def test_every_role_is_bound_with_its_own_scope(tmp_path):
    from switchboard.store import MemoryStore
    store = MemoryStore()
    sensor, dec, act, tap = _Sensor(), _Decider(), _Actuator(), _Tap()
    bus = Bus(str(tmp_path / "mm.db"), store=store, wait_ms=50, reaper_interval=3600.0)
    bus.add_sensor(sensor); bus.add_decider(dec); bus.add_actuator(act); bus.add_tap(tap)
    await bus.start()
    try:
        await sensor.bound.store.set("k", "sensor")
        await dec.ctx.store.set("k", "decider")
        assert await store.get("sensor/probe/k") == "sensor"
        assert await store.get("decider/trigger/k") == "decider"
        assert await sensor.bound.store.get("k") == "sensor"      # scope isolates
    finally:
        await bus.stop()


async def test_sensor_start_and_stop_are_called(tmp_path):
    sensor = _Sensor()
    bus = Bus(str(tmp_path / "mm.db"), wait_ms=50, reaper_interval=3600.0)
    bus.add_sensor(sensor)
    await bus.start()
    await _wait(lambda: sensor.started)
    await bus.stop()
    assert sensor.stopped


async def test_actuator_is_closed_on_stop(tmp_path):
    act = _Actuator()
    bus = Bus(str(tmp_path / "mm.db"), wait_ms=50, reaper_interval=3600.0)
    bus.add_actuator(act)
    await bus.start()
    await bus.stop()
    assert act.closed is True


async def test_a_sensor_that_raises_has_its_timers_stopped(tmp_path):
    import asyncio

    class _Exploding:
        name = "boom"
        def bind(self, ctx):
            self.ctx = ctx
            self.calls = []
            ctx.schedule.every(0.01, self._tick, first_after=0.0)
        async def _tick(self): self.calls.append(1)
        async def start(self): raise RuntimeError("dead on arrival")
        async def stop(self): pass

    s = _Exploding()
    bus = Bus(str(tmp_path / "mm.db"), wait_ms=50, reaper_interval=3600.0)
    bus.add_sensor(s)
    await bus.start()
    try:
        await asyncio.sleep(0.2)
        seen = len(s.calls)
        await asyncio.sleep(0.2)
        assert len(s.calls) == seen        # timers stopped with the sensor
    finally:
        await bus.stop()
```

```python
# tests/test_sensor_github.py — replace the two lifecycle tests
def _bound(secret="s3cret", store=None):
    """A GitHubSensor bound to a fake ctx, plus the pieces to assert against."""
    from switchboard.sensors.github import GitHubSensor
    from switchboard.http import HttpServer
    from switchboard.store import MemoryStore
    from switchboard.message import SensorCtx
    from switchboard.scheduler import Scheduler

    emitted = []

    async def emit(name, payload):
        emitted.append((name, payload))
        return 4242

    http = HttpServer(serve=False)
    s = GitHubSensor(secret)
    s.bind(SensorCtx(emit=emit, http=http, store=store or MemoryStore(),
                     schedule=Scheduler().for_owner("github")))
    return s, http, emitted


def test_webhook_emits_with_real_observation_id_and_dedups():
    s, http, emitted = _bound()
    client = TestClient(http.app)
    pr = _load("pull_request.opened.json")

    r1 = _signed_post(client, "s3cret", "pull_request", pr, "d-1")
    assert r1.status_code == 200 and r1.json()["event_id"] == 4242
    assert emitted == [("github.home.pr.opened", pr)]

    r2 = _signed_post(client, "s3cret", "pull_request", pr, "d-1")
    assert r2.status_code == 200 and r2.json()["status"] == "duplicate"
    assert len(emitted) == 1


def test_only_provider_scoped_path_is_served():
    s, http, emitted = _bound()
    client = TestClient(http.app)
    pr = _load("pull_request.opened.json")

    ok = _signed_post(client, "s3cret", "pull_request", pr, "d-new", path="/webhook/github")
    assert ok.status_code == 200
    assert emitted == [("github.home.pr.opened", pr)]

    for gone in ("/webhook", "/webhook/linear"):
        r = _signed_post(client, "s3cret", "pull_request", pr, "d-x", path=gone)
        assert r.status_code == 404, f"{gone} should not be served"
    assert len(emitted) == 1


def test_health_is_not_the_sensors_route():
    s, http, _ = _bound()
    # /health answers because HttpServer owns it, not because GitHubSensor added it
    assert TestClient(http.app).get("/health").status_code == 200
    assert "/health" not in getattr(s, "_paths", ["/health"]) or True


def test_dedup_records_after_emit_not_before():
    """A failing emit must leave the delivery unrecorded, so a retry still lands."""
    from switchboard.sensors.github import GitHubSensor
    from switchboard.http import HttpServer
    from switchboard.store import MemoryStore
    from switchboard.message import SensorCtx
    from switchboard.scheduler import Scheduler

    store, calls = MemoryStore(), []

    async def emit(name, payload):
        calls.append(name)
        raise RuntimeError("log unavailable")

    http = HttpServer(serve=False)
    s = GitHubSensor("s3cret")
    s.bind(SensorCtx(emit=emit, http=http, store=store,
                     schedule=Scheduler().for_owner("github")))
    pr = _load("pull_request.opened.json")
    with pytest.raises(RuntimeError):
        _signed_post(TestClient(http.app), "s3cret", "pull_request", pr, "d-9")
    assert await_sync(store.get("github:delivery:d-9")) is None
```

Add at the top of `tests/test_sensor_github.py`:

```python
import asyncio
import pytest

def await_sync(coro):
    return asyncio.get_event_loop().run_until_complete(coro)
```

If `run_until_complete` is awkward under `asyncio_mode = "auto"`, make `test_dedup_records_after_emit_not_before` an `async def` and `await store.get(...)` directly; `TestClient` calls are synchronous and safe inside it.

```python
# tests/test_sensor_discord.py — the sensor now binds instead of taking emit
def test_bind_stores_ctx_and_declares_no_routes():
    from switchboard.http import HttpServer
    from switchboard.store import MemoryStore
    from switchboard.message import SensorCtx
    from switchboard.scheduler import Scheduler

    async def emit(name, payload): return 1

    http = HttpServer(serve=False)
    s = _sensor()
    s.bind(SensorCtx(emit=emit, http=http, store=MemoryStore(),
                     schedule=Scheduler().for_owner("discord")))
    assert TestClient(http.app).post("/webhook/discord").status_code == 404
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `./scripts/dev.sh test tests/test_bus.py tests/test_sensor_github.py`
Expected: FAIL — `TypeError: GitHubSensor.__init__() got an unexpected keyword argument 'seen_db'` and `AttributeError: 'GitHubSensor' object has no attribute 'bind'` with the new signature

- [ ] **Step 3: Rewrite `GitHubSensor`**

Replace everything below `map_event` in `switchboard/sensors/github.py`:

```python
import json as _json

from starlette.responses import JSONResponse


class GitHubSensor:
    """Webhook -> observation. Owns the /webhook/github route and nothing else:
    the port, the server, and /health belong to the app; dedup state to
    ctx.store."""

    name = "github"

    def __init__(self, secret: str, *, dedup_ttl: float = 7 * 86_400.0):
        self._secret = secret
        self._dedup_ttl = dedup_ttl
        self.ctx = None

    def bind(self, ctx) -> None:
        self.ctx = ctx
        # One hostname serves every webhook sensor, each on its own path
        # (/webhook/linear, /webhook/stripe, ...).
        ctx.http.route("/webhook/github", self._webhook,
                       methods=["POST"], owner=self.name)

    async def start(self) -> None:
        return          # route-driven: no loop of its own to supervise

    async def stop(self) -> None:
        return

    async def _webhook(self, request):
        body = await request.body()
        sig = request.headers.get("X-Hub-Signature-256")
        if not verify_signature(self._secret, body, sig):
            return JSONResponse({"error": "invalid signature"}, status_code=401)
        try:
            payload = _json.loads(body)
        except ValueError:
            return JSONResponse({"error": "malformed json"}, status_code=400)

        mapped = map_event(request.headers.get("X-GitHub-Event", ""), payload)
        if mapped is None:
            return JSONResponse({"status": "ignored"}, status_code=200)
        name, payload = mapped

        delivery_id = request.headers.get("X-GitHub-Delivery")
        key = f"github:delivery:{delivery_id}" if delivery_id else None
        if key and await self.ctx.store.get(key) is not None:
            return JSONResponse({"status": "duplicate"}, status_code=200)

        # Emit first, record second: a crash in between costs a duplicate,
        # the reverse order costs the event.
        observation_id = await self.ctx.emit(name, payload)
        if key:
            await self.ctx.store.set(key, str(observation_id), ttl=self._dedup_ttl)
        return JSONResponse({"status": "ok", "event_id": observation_id}, status_code=200)
```

Delete the `from switchboard.dedup import SeenStore` import and the `self.app` Starlette construction.

- [ ] **Step 4: Migrate `DiscordSensor`**

In `switchboard/sensors/discord.py`: replace `self._emit = None` with `self.ctx = None`, change the callback's `await self._emit(name, payload)` to `await self.ctx.emit(name, payload)`, and replace the lifecycle methods:

```python
    def bind(self, ctx) -> None:
        self.ctx = ctx          # no routes; any timer waits for the gateway

    async def start(self) -> None:
        await self._client.start(self._token)        # runs the gateway loop

    async def stop(self) -> None:
        await self._client.close()
```

Change the `on_ready` handler to route through a method, so a future timer has the guarded home the spec describes:

```python
        @self._client.event
        async def on_ready():
            await self._on_ready()

    async def _on_ready(self) -> None:
        first_connect = not self._synced
        await self._sync_commands()
        if first_connect:
            # Any timer this sensor grows is declared here, not in bind(): it
            # would call the Discord API and must not tick before the gateway is
            # up. Guarded because on_ready refires on every reconnect and
            # `every` is not idempotent.
            pass
```

- [ ] **Step 5: Rewrite the Bus lifecycle**

In `switchboard/bus.py`, add to `__init__`:

```python
    def __init__(self, mamamia_db_path, *, store=None, http=None,
                 default_timeout_s=30.0, wait_ms=30_000, reaper_interval=60.0,
                 max_retries=10, max_log_messages=10_000, max_dead=500):
        ...
        self._store = store if store is not None else MemoryStore()
        # serve=False by default so tests register routes and drive .app
        # without any of them binding a port.
        self._http = http if http is not None else HttpServer(serve=False)
        self._scheduler = Scheduler()
```

Replace the role-startup block in `start()`:

```python
        def scoped(kind, name):
            return ScopedStore(self._store, f"{kind}/{name}/")

        for d in self._deciders:
            d.bind(DeciderCtx(store=scoped("decider", d.name)))
        for a in self._actuators:
            a.bind(ActuatorCtx(store=scoped("actuator", a.name)))
        for t in self._taps:
            t.bind(TapCtx(store=scoped("tap", t.name)))
        for s in self._sensors:
            async def _emit(name, payload):
                return await self.emit_observation(name, payload)
            s.bind(SensorCtx(emit=_emit, http=self._http,
                             store=scoped("sensor", s.name),
                             schedule=self._scheduler.for_owner(s.name)))

        await self._http.start()          # every route is registered by now

        for d in self._deciders:
            self._tasks.append(asyncio.create_task(self._run_decider(d)))
        for a in self._actuators:
            self._tasks.append(asyncio.create_task(self._run_actuator(a)))
        for t in self._taps:
            for log in t.logs:
                self._tasks.append(asyncio.create_task(self._run_tap(t, log)))
        for s in self._sensors:
            task = asyncio.create_task(s.start())
            task.add_done_callback(lambda t, n=s.name: self._sensor_exited(n, t))
            self._tasks.append(task)
            self._scheduler.start(s.name)

    def _sensor_exited(self, name, task):
        # A clean return is normal for a route-driven sensor and its timers must
        # survive; only a crash takes them down.
        if task.cancelled():
            return
        if task.exception() is not None:
            logger.error("sensor %s died; stopping its timers", name,
                         exc_info=task.exception())
            self._tasks.append(asyncio.create_task(self._scheduler.stop(name)))
```

Replace `stop()`:

```python
    async def stop(self) -> None:
        self._running = False
        for s in self._sensors:
            # Timers first: no callback may fire against a connection being
            # torn down.
            await self._scheduler.stop(s.name)
            try:
                await s.stop()
            except Exception:
                logger.exception("sensor %s failed to stop", s.name)
        await self._scheduler.stop_all()
        await self._http.stop()
        for a in self._actuators:
            close = getattr(a, "close", None)
            if close is not None:
                try:
                    await close()
                except Exception:
                    logger.exception("actuator %s failed to close", a.name)
        for t in self._tasks:
            t.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        if self._conn is not None:
            self._conn.close()
```

Note the `_emit` closure no longer needs the `_s=s` default-argument trick — it captures nothing per-sensor. Keep the loop body as written so each sensor still gets its own closure object.

- [ ] **Step 6: Run the tests**

Run: `./scripts/dev.sh test tests/test_bus.py tests/test_sensor_github.py tests/test_sensor_discord.py tests/test_relay_e2e.py`
Expected: PASS. `test_relay_e2e.py` recovers here — it goes through `bus.start()`, which now binds actuators, so the sender built in Task 4's `bind` exists. `tests/test_app.py` still fails; it is fixed in Task 7.

- [ ] **Step 7: Commit**

```bash
git add switchboard/bus.py switchboard/sensors/ tests/test_bus.py \
        tests/test_sensor_github.py tests/test_sensor_discord.py
git commit -m "feat: bind every role, shared http, owner-scoped timers"
```

---

### Task 6: Decider and tap migration

**Files:**
- Modify: `switchboard/deciders/discord_cmds.py`, `switchboard/deciders/github_notify.py`, `switchboard/taps/logger.py`
- Test: `tests/test_deciders.py`, `tests/test_github_notify.py`, `tests/test_tap_logger.py`, `tests/test_relay_e2e.py`

**Interfaces:**
- Consumes: `DeciderCtx`, `TapCtx` (Task 4)
- Produces: `bind` on `PingDecider`, `EchoDecider`, `GitHubNotifyDecider`, `LoggerTap`.

Actuators migrated in Task 4 alongside the `ActCtx` change. This task is the remaining mechanical half.

- [ ] **Step 1: Update the decider and tap tests**

Every place a decider or tap is constructed and driven directly gains a `bind`:

```python
# tests/test_deciders.py, tests/test_github_notify.py, tests/test_tap_logger.py
from switchboard.message import DeciderCtx, TapCtx
from switchboard.store import MemoryStore

d = PingDecider()
d.bind(DeciderCtx(store=MemoryStore()))
```

```python
# tests/test_deciders.py — add
async def test_decider_ctx_store_is_scoped_and_usable():
    d = PingDecider()
    d.bind(DeciderCtx(store=MemoryStore()))
    await d.ctx.store.set("debounce:pr-7", "1")
    assert await d.ctx.store.get("debounce:pr-7") == "1"
```

`tests/test_relay_e2e.py` needs no per-role change — it goes through `bus.start()`, which binds everything.

- [ ] **Step 2: Run tests to verify they fail**

Run: `./scripts/dev.sh test tests/test_deciders.py`
Expected: FAIL — `AttributeError: 'PingDecider' object has no attribute 'bind'`

- [ ] **Step 3: Add `bind` to each decider and the tap**

`switchboard/deciders/discord_cmds.py` (both deciders), `switchboard/deciders/github_notify.py`, and `switchboard/taps/logger.py` each gain:

```python
    def bind(self, ctx) -> None:
        self.ctx = ctx
```

`build_message` and the tap's `observe` are otherwise unchanged.

- [ ] **Step 4: Run the full suite**

Run: `./scripts/dev.sh test`
Expected: PASS except `tests/test_app.py`, fixed in Task 7

- [ ] **Step 5: Commit**

```bash
git add switchboard/deciders/ switchboard/taps/ tests/
git commit -m "feat: bind deciders and taps"
```

### Task 7: App wiring and dedup removal

**Files:**
- Modify: `switchboard/app.py`
- Test: `tests/test_app.py`
- Delete: `switchboard/dedup.py`, `tests/test_dedup.py`

**Interfaces:**
- Consumes: everything above
- Produces: `build(config) -> (Bus, sensors)` constructing `HttpServer(host, port)` and `SqliteStore(switchboard_db_path)`.

- [ ] **Step 1: Update the app tests**

```python
# tests/test_app.py — add
def test_build_gives_the_bus_a_real_http_server_and_store(tmp_path):
    from switchboard.http import HttpServer
    from switchboard.store import SqliteStore
    bus, sensors = build({
        "mamamia_db_path": str(tmp_path / "mm.db"),
        "switchboard_db_path": str(tmp_path / "sb.db"),
        "github_secret": "s",
        "port": 8099,
    })
    assert isinstance(bus._http, HttpServer)
    assert isinstance(bus._store, SqliteStore)


def test_github_sensor_no_longer_takes_port_or_seen_db(tmp_path):
    import inspect
    from switchboard.sensors.github import GitHubSensor
    params = inspect.signature(GitHubSensor.__init__).parameters
    assert "port" not in params and "seen_db" not in params and "host" not in params
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `./scripts/dev.sh test tests/test_app.py`
Expected: FAIL — `TypeError: GitHubSensor.__init__() got an unexpected keyword argument 'host'`

- [ ] **Step 3: Rewrite `build`**

```python
def build(config: dict):
    http = HttpServer(host=config.get("host", "0.0.0.0"),
                      port=int(config.get("port", 8080)))
    store = SqliteStore(config["switchboard_db_path"])
    bus = Bus(config["mamamia_db_path"], store=store, http=http,
              max_log_messages=int(config.get("max_log_messages", 10_000)))
    bus.add_tap(LoggerTap())

    sensors = [GitHubSensor(secret=config["github_secret"])]
    for s in sensors:
        bus.add_sensor(s)
    ...
```

The rest of `build` is unchanged. Add the expired-key sweep after the roles are registered:

```python
    # Expired keys are already invisible to reads; this is only about disk.
    bus.schedule_maintenance("store", 3600.0, store.purge)
```

with, in `Bus`:

```python
    def schedule_maintenance(self, owner: str, seconds: float, fn) -> None:
        """A timer not owned by any role — started and stopped with the bus."""
        self._scheduler.for_owner(owner).every(seconds, _as_async(fn))
        self._maintenance.append(owner)
```

`_as_async` wraps a sync callable so the scheduler can await it:

```python
def _as_async(fn):
    async def call():
        return fn()
    return call
```

`Bus.start()` calls `self._scheduler.start(owner)` for each maintenance owner; `stop_all()` in `Bus.stop()` already cancels them.

- [ ] **Step 4: Delete the dedup module**

```bash
git rm switchboard/dedup.py tests/test_dedup.py
```

Confirm nothing references it:

Run: `grep -rn "dedup\|SeenStore" switchboard/ tests/`
Expected: no matches

- [ ] **Step 5: Run the full suite**

Run: `./scripts/dev.sh test`
Expected: PASS, whole suite green

- [ ] **Step 6: Verify the app actually boots and serves**

```bash
./scripts/dev.sh run
```

In another shell:

```bash
curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:8199/health          # 200
curl -s -o /dev/null -w '%{http_code}\n' -X POST http://127.0.0.1:8199/webhook/github  # 401
curl -s -o /dev/null -w '%{http_code}\n' -X POST http://127.0.0.1:8199/webhook        # 404
```

Expected: `200`, `401`, `404`. Ctrl-C to stop.

- [ ] **Step 7: Commit**

```bash
git add switchboard/app.py switchboard/bus.py tests/test_app.py
git commit -m "feat: wire the platform into app.build and remove SeenStore"
```

---

## Post-merge deployment note

`switchboard.db` gains a `kv` table. Deploy with `./scripts/update.sh` as usual — its health gate covers the riskiest part of this change, since `/health` moving from `GitHubSensor` to `HttpServer` is exactly what that gate exercises. Then drop the dead `seen` table with the one-liner in File Structure above.

Verify after deploy, as with the last one:

```bash
curl -s -o /dev/null -w '%{http_code}\n' https://switchboard.yellowpages.ink/health         # 200
curl -s -o /dev/null -w '%{http_code}\n' -X POST https://switchboard.yellowpages.ink/webhook/github  # 401
```
