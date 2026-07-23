# Sensor Platform — Design

**Goal:** Give sensors a platform to stand on. Today each sensor privately owns whatever infrastructure it needs — `GitHubSensor` binds its own port, serves `/health`, and keeps its own SQLite dedup table — so a second webhook sensor cannot exist and a polling sensor has nowhere to put a timer. Switchboard should hand a sensor the four capabilities it can't reasonably own itself, through one context object.

**Status:** Approved design (co-designed in conversation). Additive plus one refactor of `GitHubSensor`; no change to the four-role model, the two logs, or the message schema.

---

## The observation that started it

> "What DiscordSensor does is outbound, what GitHubSensor does is inbound. Switchboard needs to provide a couple of things from its side to the sensors."

Deciders and actuators already receive a context — `DecideCtx` gives `command()`, `ActCtx` gives `result()` and the actuator's world handle. Sensors receive a bare `emit` callable and are left to improvise everything else. That asymmetry is the bug.

A sensor is the only role that touches the outside world *inbound*. Everything it needs beyond emitting is about how it gets woken up and what it remembers between wakings:

| capability | what it answers |
|---|---|
| `emit` | how do I put an observation on the log |
| `http` | how does a remote platform push to me |
| `schedule` | how do I wake myself up when nothing pushes |
| `store` | what do I remember between wakings and restarts |

Those four are `SensorCtx`.

---

## Role contexts

Every role has the same two-tier need: things it receives once at wiring, and things it receives per invocation. Today each role solves that differently — sensors take a bare `emit`, actuators expose a `context()` factory whose result the Bus stashes and re-delivers inside every `ActCtx`, and deciders have nothing. Three mechanisms for one concept.

One mechanism instead. **Lifetime dependencies arrive through `bind(ctx)`; per-invocation state stays a call argument.**

```python
@dataclass
class SensorCtx:
    emit: Callable[[str, dict], Awaitable[int]]   # -> observation id
    http: HttpServer                              # .route(path, handler, methods=)
    store: KeyStore                               # .get / .set / .delete
    schedule: OwnerSchedule                       # .every(seconds, fn)

@dataclass
class DeciderCtx:
    store: KeyStore

@dataclass
class ActuatorCtx:
    store: KeyStore

@dataclass
class TapCtx:
    store: KeyStore
```

```python
class Sensor(Protocol):
    name: str
    def bind(self, ctx: SensorCtx) -> None: ...   # sync: declare routes and timers
    async def start(self) -> None: ...            # long-running loop, or return
    async def stop(self) -> None: ...

class Decider(Protocol):
    name: str
    def bind(self, ctx: DeciderCtx) -> None: ...
    def subscribes(self, obs: Observation) -> bool: ...
    async def decide(self, obs: Observation, ctx: DecideCtx) -> None: ...

class Actuator(Protocol):
    name: str
    def bind(self, ctx: ActuatorCtx) -> None: ...  # replaces context()
    async def act(self, cmd: Command, ctx: ActCtx) -> None: ...

class Tap(Protocol):
    name: str
    logs: tuple[str, ...]
    def bind(self, ctx: TapCtx) -> None: ...
    async def observe(self, log: str, view) -> None: ...
```

All four roles bind. `LoggerTap` needs nothing from its ctx today, but a tap reads a log, so the projection rule applies to it as cleanly as to a decider — a metrics tap counting observations by name is exactly a read model. Leaving one role out is how the three-mechanism mess this section removes came about in the first place.

`TapCtx` is a store and nothing else. No `emit`, because a tap that could write to a log would stop being a tap — and no `http` either, though a live dashboard makes that tempting.

A dashboard is a tap *plus* a served page, and the two halves do not have to be the same object's responsibility. `app.build()` already holds the `HttpServer`, so it wires both — `http.route("/", dash.page)` and `bus.add_tap(dash.tap)` — with the dashboard object holding whatever projection it keeps. Nothing in the platform changes. Putting `http` on `TapCtx` instead would have required a rule that a tap's routes stay read-only, and a capability that needs a rule to stop it breaking the role's invariant is in the wrong place. The same goes for outbound: a tap that wants to push somewhere holds its own client, exactly as an actuator does.

Each role stashes its ctx the way any Python object holds a dependency:

```python
def bind(self, ctx) -> None:
    self.ctx = ctx
    ...declare routes and timers, build clients...
```

The alternative — the Bus injecting `role.ctx` so `bind()` takes no argument and only declares — saves one line per role but hides the contract. Roles are structural `Protocol`s, so nothing would catch a role reading `self.ctx` before the Bus set it. Not worth trading a visible parameter for a shorter method.

### `Actuator.context()` is removed

The factory existed to defer client construction until an event loop was running — an aiohttp session wants one. That reason was never written down, and it survives here: `bind` runs inside `Bus.start()`, with the loop up. So the actuator simply holds its client.

```python
class DiscordPost:
    name = "discord.post"

    def __init__(self, token, application_id):
        self._token, self._app_id = token, application_id

    def bind(self, ctx):
        self.ctx = ctx
        self._sender = DiscordSender(self._token, self._app_id)
```

`ActCtx` correspondingly loses its `context: Any` field and carries only per-command state (`cmd`, `result()`).

Moving client construction into `bind` also forces a teardown that is missing today: `DiscordSender` opens an `httpx.AsyncClient` that nothing ever closes, so every `Bus.start()`/`stop()` cycle leaks one. `Bus.stop()` calls `close()` on any actuator that defines it, alongside the existing sensor teardown.

### Deciders get memory, not world access

Giving a decider a store looks like it weakens the role's defining constraint. It doesn't, because the constraint was never purity — nothing today stops a decider keeping `self._seen = set()`. Refusing it a store buys no determinism; it only makes decider state invisible, non-durable, and unreadable by any tap or CLI.

So the constraint is restated rather than relaxed:

> A decider has **no world access**. It has memory. The store is its own notebook: no effects outside Switchboard, durable, inspectable. Determinism means "no side effects outside Switchboard", not "pure function of the observation".

That is what makes the real cases expressible — debounce, rate limiting, and the PR→Discord-message-id mapping an agent-decider needs in order to reply in a thread.

The decider ctx is deliberately *only* a store. No `emit`, no `http`, no `schedule`: nothing that could reach the world, and nothing that could produce a message outside the one path `DecideCtx.command()` already provides.

### Why the lifecycle splits

`ctx.http.route(...)` must run before the shared server binds its port, and `ctx.schedule.every(...)` needs a running event loop. If both lived in `start()` — which for `DiscordSensor` blocks forever on the gateway — ordering against the shared server would be a race resolvable only by sleeps.

Splitting gives a deterministic sequence with no timing assumptions:

```python
# Bus.start()
def scoped(kind, name):
    return ScopedStore(self._store, f"{kind}/{name}/")

for d in self._deciders:
    d.bind(DeciderCtx(store=scoped("decider", d.name)))
for a in self._actuators:
    a.bind(ActuatorCtx(store=scoped("actuator", a.name)))
for t in self._taps:
    t.bind(TapCtx(store=scoped("tap", t.name)))
for s in self._sensors:
    s.bind(SensorCtx(emit=..., http=self._http,
                     store=scoped("sensor", s.name),
                     schedule=self._scheduler.for_owner(s.name)))
await self._http.start()                    # every route is registered by now
for s in self._sensors:
    task = asyncio.create_task(s.start())
    task.add_done_callback(lambda t, n=s.name: self._sensor_exited(n, t))
    self._tasks.append(task)
    self._scheduler.start(s.name)

# Bus.stop()
for s in self._sensors:
    await self._scheduler.stop(s.name)      # timers first: no callback may fire
    try:                                    # against a connection being torn down
        await s.stop()
    except Exception:
        logger.exception("sensor %s failed to stop", s.name)
```

`bind` is not new machinery. `GitHubSensor` already has a `bind(emit)` method, added so tests could drive its app without binding a port — this formalizes a shape the code found on its own.

A `start()` that returns immediately means "no loop of my own to supervise", not "finished". Route-driven and timer-driven sensors both return; their work happens on the server's and the scheduler's tasks.

---

## HttpServer

One Starlette app and one uvicorn server, owned by the app, shared by every webhook sensor.

```python
class HttpServer:
    def __init__(self, host="0.0.0.0", port=8080): ...
    def route(self, path, handler, *, methods=("GET",)) -> None: ...
    async def start(self) -> None: ...
    async def stop(self) -> None: ...
```

It serves `/health` itself. That endpoint is not GitHub's — it is the deployment's liveness probe, gating `scripts/update.sh`, and it must answer in a build configured with no webhook sensor at all.

### One owner per request

Ownership is keyed on `(method, path)`, not on path alone — a request is identified by both, and that is the granularity at which exactly one response exists. `GET /x` and `POST /x` never contend, so they may have different owners.

This also matches the router underneath: Starlette already dispatches on method, and providers exist that use one URL for two verbs — Meta webhooks answer a `GET` verification challenge at the same URL that receives event `POST`s. Path-only exclusivity would have stopped even a single sensor from registering those as two handlers.

```python
raise ValueError(
    f"{method} {path} already registered by {self._owners[(method, path)]!r}. "
    f"One request has one response, so it has one owner. To have several "
    f"consumers react to it, add deciders that subscribe to the observation "
    f"it emits; to separate tenants, scope the path (e.g. {path}/<tenant>)."
)
```

The underlying need — several interested parties, one inbound URL — is real, and it is already served one layer down. Fan-out is the log's job: one sensor emits, N deciders subscribe, and each settles, retries, and dead-letters independently.

Sharing an HTTP path would instead couple those consumers through a single status code. If handler A returns 200 and handler B raises, the provider sees one response; a 500 makes GitHub redeliver, and **A processes the delivery twice**. Two sensors' failure domains fused through a status code, to buy something the obs log already gives for free.

Relaxing this later is backwards-compatible, so choosing exclusivity now costs nothing.

---

## KeyStore

A string-to-string key-value store with expiry. Replaces `SeenStore`, which was a dedup table with a dedup-shaped API (`record`, `prune(keep_last)`) usable only by the sensor that owned it.

```python
class KeyStore(Protocol):
    async def get(self, key: str) -> str | None: ...
    async def set(self, key: str, value: str, *, ttl: float | None = None) -> None: ...
    async def delete(self, key: str) -> None: ...
```

**Async, though `MemoryStore` is a dict.** The only decision here that is expensive to reverse. Converting a sync interface to async later touches every call site in every sensor; paying the `await` now keeps a networked backend possible without a migration.

**`str` → `str`, enforced, no coercion.**

```python
def _check_key(key):
    if not isinstance(key, str):
        raise TypeError(f"KeyStore keys must be str, got {type(key).__name__}")


def _check_value(value):
    if not isinstance(value, str):
        raise TypeError(f"KeyStore values must be str, got {type(value).__name__}. "
                        f"Serialize structured values yourself (json.dumps).")
```

Two functions rather than one with an optional `value` parameter. A single `_check(key, value=None)` would have to treat `None` as "no value supplied", which silently exempts `set(k, None)` from validation and stores a `None` that reads back indistinguishable from an unset key.

Implicit `str()` would make `set(k, 5)` followed by `get(k) == 5` evaluate `False`, discovered in production. Callers that want structure call `json.dumps` — visible, and their choice of format.

### Two memories, one API

`ttl` is optional and defaults to `None`, meaning never expires. Long-term memory is the default; short-term is opt-in.

```python
await store.set("cursor", ts)                 # remembered until deleted
await store.set(key, oid, ttl=7 * 86_400)     # forgets itself
```

Caveat for the docstring: long-term keys grow without bound, and some are per-entity. A sweep cursor is one key forever; a PR→message-id mapping is one key per PR, accumulating for the life of the deployment. Give per-entity mappings a TTL unless they genuinely must be permanent.

### Every store is scoped, and there is no global store

Each role gets a `ScopedStore` — a view over the shared backend, prefixed with the role's kind and name:

```
sensor/github/delivery:abc123
decider/github_notify/thread:pr-7
actuator/discord.post/idem:cmd-4412
```

```python
class ScopedStore:
    """A KeyStore view over a prefix. Roles never see the prefix and cannot
    reach another role's keys — the log is the channel between roles."""

    def __init__(self, inner: KeyStore, prefix: str):
        self._inner, self._prefix = inner, prefix

    async def get(self, key):
        _check_key(key)
        return await self._inner.get(self._prefix + key)
    ...
```

Collision safety is the obvious benefit — `cursor` is the natural key name and every polling sensor wants it — but the reason is architectural. **Roles communicate through logs. A store is private memory.** A shared keyspace would be an out-of-band channel between roles that bypasses the durable log: invisible to taps, unordered, unreplayable, impossible to dead-letter. Two roles coordinating through a shared key would be doing precisely what the two-log design exists to prevent.

The cross-role case people reach for does not need it. An actuator posts to Discord, receives a message id, and emits a result observation carrying it; a decider that wants to remember "PR #7 → message 123" reads that observation and writes its own key. The log already moved the data between roles.

Scoping by `kind/name` rather than by class also gives two properties for free: state survives restarts because names are stable (they are already the consumer-group ids), and two instances of the same class must already have distinct names, so two GitHub orgs get separate keyspaces without special handling.

### What belongs in a store

Scoping is per handler, not per role kind — two deciders cannot share either. The rule that makes that principled rather than merely tidy:

> **A store holds only what its owner can derive from what it has seen.**

Sensors project the world into the log; deciders and actuators project the log into memory. Each store is a private read model over the stream its owner consumes. Two consumers of the same stream keeping their own projections is the normal shape, not duplication to be optimised away — the obs log is broadcast, so every decider that needs a fact sees the observation carrying it and builds its own copy.

The smell test when two handlers appear to want the same state: **either they are one handler with two branches, or the state belongs to a third role that owns the resource.** Both beat a shared key.

Worked through the cases that look like exceptions:

| looks like it wants sharing | where it actually belongs |
|---|---|
| Two deciders needing the same PR→message-id map | Each projects it from the same result observation. A handful of duplicated keys. |
| A global rate limit on Discord posts | The `discord.post` actuator — one owner of the channel, one counter. Not spread across the deciders feeding it. |
| The repos we care about, the channel id | Config, not state. Injected at construction. |

One consequence to know: a decider consumes `obs` only, so it cannot react to another decider's *command*. Influence between deciders flows A → command → actuator → result observation → B, which means it only flows through things that actually happened, never through intentions. A decision that never becomes an action is invisible to everyone, by design.

### Two implementations, one test suite

`MemoryStore` is written to the *stricter* contract, not the one a dict gives naturally: it implements expiry-on-read and the same type checks explicitly. Otherwise tests pass in memory and behavior diverges in production, which is worse than shipping only one implementation. One parametrized suite runs against both.

`SqliteStore` keeps the durability `SeenStore` already had — dedup state on the Docker volume, surviving restarts — and reuses its sync-sqlite-on-the-loop-thread approach:

```sql
CREATE TABLE kv (
  key        TEXT PRIMARY KEY,
  value      TEXT NOT NULL,
  expires_at REAL              -- NULL = never
);
CREATE INDEX kv_expires ON kv (expires_at);
```

Reads filter on expiry (`WHERE key = ? AND (expires_at IS NULL OR expires_at > ?)`), so an expired row is invisible the instant it expires regardless of when a sweep last ran. A `purge()` method deletes expired rows and is about disk, never correctness. The Bus schedules it by reusing the `Scheduler` with a non-sensor owner:

```python
self._scheduler.for_owner("store").every(3600, self._store.purge)
self._scheduler.start("store")
```

### Semantics chosen, and the boundary accepted

`set` is last-write-wins. There is no locking, no compare-and-set, no `add()`.

Sensors dedup by `get` → `emit` → `set` — **emit first, record second**. A crash between the two costs a duplicate; the reverse order costs the event permanently, which is the exact bug `GitHubSensor` shipped once and had to be fixed for.

That sequence has a theoretical race: `await emit` is a suspension point, so two concurrent writers on the same key could both pass `get` before either `set`. It does not occur for any current sensor — GitHub retries a delivery sequentially after a timeout, never concurrently, and a sweep cursor has exactly one writer because fixed-delay scheduling means a sweep never overlaps itself. Recorded here as a known boundary rather than left to be rediscovered.

If a sensor ever does have two concurrent writers on one key, the fix is a `var()` context manager — a per-key async mutex yielding a handle that writes back on exit — which is purely additive and breaks no existing `get`/`set` caller. Deliberately not built now.

Locks are process-local in any case. `SqliteStore` makes the *record* durable across restarts, but nothing coordinates across processes; consistent with mamamia being single-node by design.

---

## Scheduler

A sensor with no inbound push needs a clock. Polling platforms are the driver; reconcile sweeps are the near-term use even for platforms that do push, since webhooks are at-least-once *delivery* but not guaranteed delivery — a delivery lost while the process is restarting is gone, and only a sweep recovers it.

### The tick is stimulus, not a message

An early alternative was to implement scheduling on mamamia's `available_after`: append a delayed observation and let a decider consume the tick. That breaks the role model. The tick observation is empty, and the actual work — fetch, diff, decide what is new — lands on a decider, which by construction has no world access. It would emit a command, an actuator would fetch, a result observation would come back, and another decider would need to diff it against state deciders do not hold. Four hops and two roles doing work they are defined not to do, to accomplish what a polling sensor does in one function.

Nobody thinks "an HTTP request arrived" should be an observation; the *mapped event* is the observation and the request is how the sensor woke up. A timer is the same thing. So `schedule` is exactly symmetric with `http`:

| capability | edge | who wakes the sensor |
|---|---|---|
| `http` | inbound push | the remote platform |
| `schedule` | inbound pull | the clock |

Both deliver stimulus. Neither produces a message. The sensor wakes, fetches, diffs against `ctx.store`, and emits observations indistinguishable from the ones its webhook path emits.

### Durability is a cursor, not a durable timer

A missed tick is not a lost message: the sensor sweeps *since the cursor in `ctx.store`*, so a tick missed while the process was down means the next sweep covers a wider window. The cursor is required regardless — a tick that is delivered and then crashes loses its work anyway — so a durable timer would add a second mechanism that solves nothing the cursor does not already solve.

### Timers live and die with their sensor

Declared at `bind`, launched when the sensor starts, cancelled when it stops. A timer must never fire before its sensor is running or after it has stopped.

```python
class Scheduler:
    def for_owner(self, owner: str) -> OwnerSchedule: ...
    def start(self, owner: str) -> None: ...          # launch declared timers
    async def stop(self, owner: str) -> None: ...     # cancel and await them

class OwnerSchedule:
    def every(self, seconds, fn, *, first_after=None, name=None) -> None: ...
```

`_declare` launches a timer immediately when its owner is already running, which gives two meaningful declaration points:

- **In `bind()`** — "tick from the moment I am started." Right for a sensor ready as soon as it exists.
- **In `start()` or a readiness callback** — "tick from the moment I say I am ready." Right for a sensor that needs a live connection first. `DiscordSensor` declares in `on_ready`, not in `bind`, because its timers call the Discord API.

The loop:

```python
async def _loop(self, seconds, first_delay, fn, label):
    await asyncio.sleep(first_delay)
    while True:
        try:
            await fn()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("scheduled callback %s failed", label)
        # Fixed delay, not fixed rate: the next sleep starts when the callback
        # finishes, so a slow tick can never stack up behind itself.
        await asyncio.sleep(seconds)
```

Four decisions:

- **Fixed delay, not fixed rate.** No overlap ever, so callbacks need no reentrancy guard and a cursor has exactly one writer.
- **`first_after` defaults to `seconds`.** Nothing fires at t=0. Immediate-on-boot means a crash-looping container sweeps the remote API on every restart; delaying costs nothing because the first sweep reads from the cursor and covers whatever window elapsed. `first_after=0` opts in explicitly.
- **A raising callback logs and the loop survives.** Same contract as `Bus._consume`.
- **No backoff on consecutive failures.** The interval *is* the rate limit. A knob nobody needs yet.

### One timer belongs to the bus, not to a role

`SqliteStore.purge` needs a driver and no role owns it, so the `Bus` declares it against a non-role owner:

```python
    def schedule_maintenance(self, owner: str, seconds: float, fn) -> None:
        """A timer owned by the bus rather than by any role. Started with the
        bus, cancelled by stop_all()."""
        self._scheduler.for_owner(owner).every(seconds, _as_async(fn), name=owner)
        self._maintenance.append(owner)
```

`_as_async` wraps the sync `purge` because the scheduler awaits its callbacks. Owners are just strings, so this needs no new mechanism — which is the point: a scheduler keyed on owner names rather than on role objects absorbs the case for free.

### A crashed sensor stops ticking

"Only while started" has a third case beyond start and stop: a sensor whose `start()` raised. Its timers would otherwise keep sweeping against a dead connection forever.

```python
def _sensor_exited(self, name, task):
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        logger.error("sensor %s died; stopping its timers", name, exc_info=exc)
        asyncio.create_task(self._scheduler.stop(name))
```

Only on exception. A clean return is normal for a route-driven sensor, and those timers must survive.

---

## Migration

### GitHubSensor

Loses `host`, `port`, and `seen_db` — none of which were ever GitHub's business — and loses `/health`.

```python
class GitHubSensor:
    name = "github"

    def __init__(self, secret: str, *, dedup_ttl: float = 7 * 86_400.0):
        self._secret = secret
        self._dedup_ttl = dedup_ttl
        self._ctx = None

    def bind(self, ctx) -> None:
        self._ctx = ctx
        ctx.http.route("/webhook/github", self._webhook, methods=["POST"])

    async def start(self) -> None:
        return                       # route-driven: no loop to supervise

    async def stop(self) -> None:
        return

    async def _webhook(self, request):
        body = await request.body()
        if not verify_signature(self._secret, body, request.headers.get("X-Hub-Signature-256")):
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
        if key and await self._ctx.store.get(key) is not None:
            return JSONResponse({"status": "duplicate"}, status_code=200)

        # Emit first, record second: a crash in between costs a duplicate,
        # the reverse order costs the event.
        observation_id = await self._ctx.emit(name, payload)
        if key:
            await self._ctx.store.set(key, str(observation_id), ttl=self._dedup_ttl)
        return JSONResponse({"status": "ok", "event_id": observation_id}, status_code=200)
```

`map_event` and `verify_signature` are unchanged.

### DiscordSensor

Keeps its blocking gateway loop. `bind` stores the ctx and declares nothing; the emit call inside the command callback becomes `self._ctx.emit(...)`.

```python
    def bind(self, ctx) -> None:
        self._ctx = ctx              # no routes; timers wait for the gateway

    async def start(self) -> None:
        await self._client.start(self._token)

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

`DiscordSensor` declares no timers today, so the `if first_connect` block stays empty in this change. It is specified now so that whoever adds the first timer does not stack one per reconnect — a flapping gateway would otherwise accumulate a copy per connect.

### Composite sensors

The point of the platform is a sensor that uses several capabilities at once — real-time webhook plus a reconcile sweep, both funnelling through one emit path so an event arriving twice emits once:

```python
class LinearSensor:
    name = "linear"

    def bind(self, ctx):
        self._ctx = ctx
        ctx.http.route("/webhook/linear", self._webhook, methods=["POST"])
        ctx.schedule.every(self._sweep_interval, self._sweep)

    async def _emit_issue(self, issue) -> int | None:
        # Keyed on (id, updatedAt), not on a webhook delivery id: the sweep has
        # no delivery id, and a shared key is what makes the two paths dedup
        # against each other rather than each against itself.
        key = f"linear:seen:{issue['id']}:{issue['updatedAt']}"
        if await self._ctx.store.get(key) is not None:
            return None
        observation_id = await self._ctx.emit(f"linear.issue.{issue['action']}", issue)
        await self._ctx.store.set(key, str(observation_id), ttl=self._dedup_ttl)
        return observation_id

    async def _sweep(self):
        cursor = await self._ctx.store.get("linear:cursor")
        if cursor is None:
            # First run, or a store that lost it. Start from now — defaulting to
            # the epoch would replay all history into the log.
            await self._ctx.store.set("linear:cursor", utcnow_iso())
            return
        newest = cursor
        for issue in await self._api.issues_updated_since(cursor):
            await self._emit_issue(issue)
            newest = max(newest, issue["updatedAt"])
        # Advance after the batch. A crash mid-sweep re-reads the window on the
        # next tick, and dedup makes the replay a no-op.
        if newest != cursor:
            await self._ctx.store.set("linear:cursor", newest)
```

Not built in this change — included as the worked example the design is answerable to.

---

## Configuration

No new environment variables. `SB_PORT` and `SB_DATA_DIR` now reach `HttpServer` and `SqliteStore` through `app.build()` instead of through `GitHubSensor`'s constructor. `switchboard.db` keeps its path and gains the `kv` table alongside the existing `seen` table, which is dropped once nothing reads it.

---

## Testing

- `HttpServer` — routes registered by several fake sensors are all served; duplicate path raises at bind; `/health` answers with no sensors registered at all.
- `KeyStore` — one parametrized suite over `MemoryStore` and `SqliteStore`: get/set/delete, overwrite, `ttl=None` never expires, TTL expiry via an injected clock, non-`str` key and value both raise, `purge` removes only expired rows. `SqliteStore` additionally: values survive reopening the database.
- `ScopedStore` — two scopes writing the same key do not see each other; the prefix never leaks back through `get`; a scoped `delete` leaves the other scope intact.
- Role binding — `bind` is called on every sensor, decider, actuator, and tap before any message is consumed; each receives a store scoped to its own `kind/name`.
- `Scheduler` — a timer does not fire before `start(owner)`; fires after; stops firing after `stop(owner)`; a raising callback does not kill the loop; a slow callback does not overlap itself; `every` called while running launches immediately; a sensor whose `start()` raises has its timers stopped.
- `GitHubSensor` — existing tests port to the new lifecycle: construct, `bind` with a fake ctx, drive `ctx.http`'s app with `TestClient`. Signature verification, event mapping, dedup, and the emit-then-record ordering assertions all carry over unchanged in intent.
- `Bus` — bind happens before the server starts; timers stop before `stop()` is awaited.

---

## Deliberately not in scope

| left out | why |
|---|---|
| `var()` / per-key locks / `add()` | No sensor has two concurrent writers on one key. Purely additive later. |
| Durable timers on `available_after` | The cursor already handles missed ticks; a durable timer would be a second mechanism solving the same problem. |
| Backoff on repeated timer failures | The interval is the rate limit. |
| Cron scheduling | Cron's step values (`*/7`) are wall-clock matching, not intervals — `*/7` fires at :00 :07 … :56 with a 4-minute gap at the hour, and only divisors of 60 behave interval-like. It also has no sub-minute resolution. So it does not subsume `every()`, and `every()` does not subsume it: cron alone expresses "3am daily". Nothing wants a wall-clock job yet, and adopting it now would import a parser dependency, a DST policy for the hour that repeats and the hour that does not exist, and a skip-if-running policy that fixed-delay currently makes unnecessary. Add later as a second verb, `at("0 9 * * 1-5", fn)`, decided against a real job. |
| Non-exclusive HTTP routes | Decider fan-out already serves the use case without coupling failure domains. |
| Cross-process coordination | mamamia is single-node by design. |
| A global / unscoped store | Shared memory between roles is an out-of-band channel that bypasses the log. The data a role needs from another role arrives as an observation; each role projects its own copy. Additive later if a genuine case appears. |
| `LinearSensor` itself | The example the design is answerable to, not a deliverable here. |
| `LoggerTap` payload redaction | Real outstanding issue — full payloads including `interaction_token` reach persistent logs — but orthogonal to this change. |
| A live dashboard | A product built on this platform, not part of it: a projection over both logs plus a streaming transport and a UI. It needs no platform change — `app.build()` owns the `HttpServer` and can register the page alongside `bus.add_tap(...)`. Gets its own spec. Note that `max_log_messages` bounds the visible history to a rolling window. |
