# Switchboard — Design

**Date:** 2026-07-19
**Status:** Approved for planning
**Owner:** yellowpages.ink

## Summary

**Switchboard turns things that happen in one system into actions in another.**

It is a general-purpose, event-driven relay engine. Ingress adapters publish
events; egress handlers consume and act on them. All domain knowledge lives in adapters;
durability, leasing, retry, and dead-lettering are delegated to
[mamamia](https://github.com/saksham1341/mamamia), an existing append-only log with
consumer groups and JIT leasing.

The first application is GitHub repository activity relayed into Discord. The engine itself
knows nothing about GitHub or Discord.

The name is literal: a switchboard patches incoming lines to outgoing lines. Any ingress,
any egress, topology as configuration.

## Goals

- Relay GitHub repo activity into Discord, without losing events that have already been
  received — including across process crashes and restarts.
- Keep the core generic so new providers are new adapters, not core changes.
- Run on a Raspberry Pi today, move to a hosted environment later with no code change.
- Reuse mamamia rather than reimplementing a queue. Switchboard contributes the persistent
  backends mamamia's roadmap already calls for.
- Async-first throughout. Every interface boundary is a coroutine.

## Non-goals (v1)

- No writes back to GitHub. Read-only relay.
- No Discord slash commands. One-way feed only.
- No multi-channel routing. One channel.
- No plugin loader or config-driven adapter discovery. Adapters are wired explicitly in code.
- No mamamia TCP server. The `Orchestrator` is used in-process as a library; the
  client/server and binary protocol layers are unused.
- **No backfill of events missed while offline.** If the process is down when a webhook
  fires, GitHub's delivery fails and that event is lost. Accepted: at current volume the
  cost of a missed notification is near zero, and the Pi is a temporary home. See
  *Future work* for the recovery path if that changes.

## Why mamamia

The delivery semantics Switchboard needs — durable log, per-consumer progress, exclusive
claim while processing, retry with a dead-letter ceiling — are exactly what mamamia
implements. Three of its decisions are better than what an independent design reached:

- **Leases carry an owner and an expiry** (`core/models.py`). Crash recovery is automatic:
  an expired lease is simply reacquirable. No boot-time reset hook, and no assumption that
  only one process is running.
- **Lazy reap** (`server/orchestrator.py`): a message in `IN_PROGRESS` with no live lease is
  reset to `PENDING` on read. Correctness lives on the read path, so the background
  `reap_expired()` is housekeeping rather than a correctness dependency.
- **Ownership check on settle** (`server/orchestrator.py`): a worker whose lease expired
  cannot settle a message another worker now holds. Without this, a handler that times out
  and *then* completes would corrupt the state of the retry already in flight.

The one thing mamamia lacks is persistence — its storage, state, and lease backends are all
in-memory. Its README lists a SQLite/WAL backend as future work. Switchboard's core
deliverable is exactly that, which makes this reuse rather than adoption of a dependency
that fits by accident.

## Division of concerns

mamamia is a message delivery system. Switchboard is an application built on it. The two
are developed together and may share an owner, but the boundary is enforced by a test
applied to every proposed upstream change:

> **Would a message delivery system need this regardless of yellowpages?**

If yes, it belongs in mamamia and must be designed generically — no GitHub, no Discord, no
Switchboard vocabulary in its interfaces. If no, Switchboard owns it, however convenient it
would be to push down.

The rule matters more than repository ownership. Shared ownership makes the boundary easier
to erode, not harder, because every Switchboard need starts to look like a mamamia feature.

Both repositories live in the `yellowpages-ink` organization and are private for now, to be
sanitized and opened if and when that becomes useful. Privacy removes the external forcing function that would otherwise keep mamamia
honest, so the test above has to be applied deliberately at review time rather than assumed.

Applying the test to the known gaps:

| Concern | Owner | Reasoning |
|---|---|---|
| Retry *mechanism* (when may this be redelivered) | mamamia | Every queue has visibility timeouts; already on its roadmap |
| Retry *policy* (exponential, jitter, caps) | Switchboard | Policy is application judgement, not delivery machinery |
| Lease ownership correctness | mamamia | A lease bug is a delivery-system bug |
| Log retention / pruning | mamamia | Every log system has retention |
| Persistent backends | mamamia | Its own roadmap item; generic to any deployment |
| Ingress deduplication | Switchboard | The key, window, and "same event" definition are all application policy |
| Filtering, routing, adapters | Switchboard | Meaningless to a delivery system |

The retry split is the clearest illustration: mamamia gains a `retry_after` parameter and
never learns what exponential backoff is, while Switchboard computes the delay and owns the
schedule.

## Upstream work in mamamia

Four changes are prerequisites for v1. Each is independently justifiable as a delivery
system feature and is upstreamed, not carried as a fork.

They are preceded by an audit phase, because we are about to build on internals that have
not been reviewed with persistence or a second consumer in mind.

**0. Audit sweep — correctness and optimization.** A pass over mamamia before any feature
work, on the grounds that extending unreviewed internals compounds whatever is already
there. Candidate areas, from a first read and to be verified rather than assumed:

- **Lock asymmetry in `InMemoryLeaseManager`.** `acquire`, `release`, and `get_lease*` take a
  per-`(log, group)` lock, but `reap_expired` takes only the global lock. A reap can
  therefore delete a lease concurrently with an acquire holding a different lock. This is the
  most likely real bug in the file, and it matters more once a persistent backend makes
  concurrent access routine.
- **Unbounded lock dictionary.** `_get_lock` inserts into `self._locks` per `(log, group)`
  and never removes, so the dict grows for the lifetime of the process.
- **Double locking on every call.** Every lease operation acquires the global lock solely to
  look up a per-key lock, serializing all groups through one mutex and negating the
  per-group granularity.
- **N+1 reads in `_slide_offset`.** It calls `get_message_state` one message at a time in a
  loop, while `get_message_states` already exists for batch reads. Free with in-memory
  dicts; a per-row round trip once the backend is SQLite.
- **Rescan cost in `acquire_next`.** Each call re-scans from the group's base offset in
  batches of 20 until it finds an eligible message. A group sitting behind a long run of
  ineligible messages re-walks them on every poll.
- **Lazy reap ignores retry count.** Resetting `IN_PROGRESS` → `PENDING` on a missing lease
  does not increment attempts, so a handler that repeatedly crashes mid-processing retries
  forever without approaching the dead-letter ceiling.
- **Test coverage.** `tests/` currently holds an integration simulation. The behaviours we
  are about to depend on — lease exclusivity, expiry, settle ownership — need direct unit
  tests before backends multiply.

Findings are fixed upstream and released before the feature work below begins. Anything the
sweep finds that is *not* a general delivery-system concern is recorded and left alone.

**1. Deferred redelivery (`retry_after`).** mamamia has no notion of *when* a failed message
becomes eligible again — `acquire_next` returns anything in `PENDING` or `FAILED` without a
live lease, so a failing message is instantly reacquirable and would exhaust its retry
ceiling in milliseconds. This is listed as "Retry Backoff" in mamamia's own future work.

`IStateStore` gains an `available_at` per `(log_id, group_id, message_id)`; `acquire_next`
skips messages whose `available_at` is in the future. The client supplies the delay, mirroring
the existing `duration` parameter on `acquire_next`. mamamia enforces *when*; it never decides
*how long*.

**2. Explicit terminal outcomes.** A consumer that knows a message is permanently
unprocessable — malformed payload, unroutable target, rejected by a business rule — should
not spend the whole retry ceiling proving it. This is standard in delivery systems
(RabbitMQ `basic.reject` with `requeue=false`, Celery's `Reject`, Kafka dead-letter topics).

`settle` takes an explicit outcome rather than a boolean plus modifiers:

```python
await orchestrator.settle(..., outcome='success')
await orchestrator.settle(..., outcome='retry', retry_after=30.0)   # deferred
await orchestrator.settle(..., outcome='retry', retry_after=0)      # immediate
await orchestrator.settle(..., outcome='dead')                      # kill, ignores retry count
```

There are three real outcomes, so the parameter names three. The rejected alternative was
overloading `retry_after`, with `None` meaning kill — that makes `settle(success=False)`,
the most natural call anyone writes, silently dead-letter instead of retry. The destructive
path should never be the default path.

`retry_after` is meaningful only for `outcome='retry'`, so no value of it can destroy a
message. `outcome='dead'` bypasses the retry count entirely.

Switchboard's counterpart: handlers raise `PermanentError` for failures that cannot succeed
on retry, which the consumer loop maps to `outcome='dead'`. mamamia supplies the mechanism;
Switchboard decides what counts as permanent.

This also removes head-of-line blocking on a poison message: while backing off, the message
is ineligible, so `acquire_next` moves past it to newer events instead of re-picking it every
cycle.

**3. Strict settle.** `settle` currently permits settlement when `get_lease` returns `None`,
intending to allow "lease expired but nobody else took it." It cannot distinguish that from
"someone else took it and already finished," so a slow worker can overwrite the final state
of a redelivery that already completed. Settle should require a live lease owned by the
caller and reject otherwise. At-least-once already tolerates the resulting redelivery, so
strictness costs nothing.

**4. Persistent backends.** SQLite/WAL implementations of `IMessageStorage`, `IStateStore`,
and `ILeaseManager`, with retention limits, tested against the same conformance suite as the
in-memory backends.

**Not upstreamed:** deduplication. See *Deduplication* below.

**Sequencing:** audit sweep, then the four changes, then a release. Switchboard builds
against a pinned tag and never depends on an unreleased branch.

### Already sufficient

`acquire_next(..., duration=...)` is already a client-supplied lease duration, so
per-handler `lease_s` needs no upstream change.

### Known mamamia limitation, not worked around

`Orchestrator._slide_lock` is an `asyncio.Lock`, so offset sliding is safe only within one
process. Multi-instance deployment is blocked by this regardless of backend, and is tracked
on mamamia's roadmap ("Shared Backends... multi-worker deployments"). Switchboard runs as a
single process and does not attempt to work around it.

## Architecture

```
GitHub ──webhook──▶ GitHubIngress
                          │ publish()
                          ▼
                   ┌──────────────────────────────────┐
                   │  mamamia Orchestrator            │
                   │    IMessageStorage  ─┐           │
                   │    IStateStore       ├─ SQLite   │
                   │    ILeaseManager    ─┘   (WAL)   │
                   └──────────────────────────────────┘
                          │ acquire_next(group_id=handler)
            ┌─────────────┼─────────────┐
            ▼             ▼             ▼
       handler task  handler task  handler task
       (log-all)     (pr-to-eng)   (ci-to-alerts)
            │             │             │
            └─────────────┴─────────────┘
                          ▼
                   LoggerEgress / DiscordEgress
```

### Layer boundaries

**mamamia owns:** the append-only log, per-group offsets and message state, leases, retry
counts, dead-lettering.

**Switchboard core owns:** the SQLite backends, the event envelope, adapter lifecycle, the
per-handler consumer loops, and dispatch timeouts.

**Adapters own** translation only: bytes → `Event` on ingress, `Event` → side effect on
egress. An adapter doing its own scheduling or retry is in the wrong layer. This is the
single most important rule in the codebase — an adapter that reimplements retry does so
subtly wrong, and that bug class is never fully cleared.

## Data model

### Event — an immutable fact

An event is the `payload` of a mamamia message. It carries no targets, no status, and no
reply address; those are delivery concerns.

```python
@dataclass(frozen=True)
class Event:
    id: str                      # ULID, assigned by core
    kind: str                    # 'github.home.pr.opened'
    source: str                  # 'github'
    at: str                      # ISO 8601, when it occurred
    payload: dict                # adapter-defined, opaque to core
    dedupe_key: str | None = None
    meta: dict[str, str] = field(default_factory=dict)
```

`meta` carries transport-level provenance that is not part of the fact itself — delivery id,
signature algorithm, receipt timestamp. Adapters write it; handlers may read it; the core
only persists it.

### Delivery state — owned by mamamia

Switchboard has **no deliveries table.** Per-handler delivery state is mamamia's
per-`(log_id, group_id, message_id)` state, plus its lease and retry count.

The mapping is exact, and was arrived at independently on both sides:

| Switchboard concept | mamamia concept |
|---|---|
| event log | `log_id` (one log, `'events'`) |
| handler | `group_id` (consumer group) |
| delivery | `(group_id, message_id)` state + lease + retry count |
| in flight | `IN_PROGRESS` + live lease |
| delivered | `PROCESSED` |
| retryable failure | `FAILED`, retry count incremented |
| dead-lettered | `DEAD` (retry ceiling reached) |

A handler is precisely a consumer group over the event log. Because every group
independently consumes every message, fan-out is inherent — there is nothing to compute or
persist at publish time.

### Storage backends

Switchboard implements all three mamamia interfaces against one SQLite database in
`journal_mode=WAL`:

- `SQLiteMessageStorage` — append-only `messages` table, monotonic index per log.
- `SQLiteStateStore` — `(log_id, group_id, message_id) → state`, plus base offsets and
  retry counts.
- `SQLiteLeaseManager` — `(log_id, group_id, message_id) → owner_id, expiry`, with
  acquisition as a single conditional `INSERT ... ON CONFLICT` so a lease cannot be granted
  twice.

SQLite is chosen over a hand-rolled log because fsync durability, torn writes, and crash
recovery are already solved there — and the Pi has no UPS, so power loss mid-write is a
realistic failure mode. Single file; migration to another host is a file copy.

Lease acquisition must be atomic in a single statement, not read-then-write. This is the one
place where a race would silently double-deliver.

### Retention

Retention is a property of the backends and therefore lives in mamamia, configured by
Switchboard. Unbounded growth on an SD card is not acceptable, so both are bounded:

- **State rows** are pruned once terminal. `PROCESSED` rows are deleted behind the group's
  base offset. `DEAD` rows are retained (capped at the most recent 500) — pruning them would
  destroy the only record of what failed, which is the point of dead-lettering.
- **Messages** are a bounded log: oldest rows deleted beyond a configured cap (default
  10,000), never below the lowest base offset across groups.

### Deduplication

Owned by Switchboard, not mamamia. The dedup key, the window, and what counts as "the same
event" are application policy — mamamia's log stays a pure append-only log with no opinion
about whether two appends mean the same thing.

Switchboard keeps a `seen` table in its own database mapping the provider's idempotency key
(GitHub's `X-GitHub-Delivery` UUID) to the event id it produced, and checks it before
appending. Entries are pruned on the same schedule as the log.

Consequence: dedup is only as durable as that table. Once an entry ages out, a redelivery of
that webhook is processed as new. At current volume the window is months, so it is accepted.

### Schema migrations

`PRAGMA user_version` plus an ordered list of migration callables applied in a transaction on
boot. That is what `user_version` exists for; roughly thirty lines, no dependency. Alembic is
viable now that the stack is Python, but it targets SQLAlchemy and we are using raw
`aiosqlite` — worth revisiting only if the schema starts moving often.

## Core interfaces

### Broker

A thin facade over mamamia's `Orchestrator`. Its job is to own the envelope, assign
identity, and run consumer loops — not to reimplement queueing.

```python
class Broker(Protocol):
    async def publish(self, event: EventInput) -> PublishResult:
        """Append an event to the log.

        Returns once the message is durably committed — NOT once handlers have run.
        This is what lets any ingress acknowledge its transport quickly.
        """

    def attach(self, egress: Egress) -> None:
        """Register an egress and its handlers. Each handler becomes a consumer group.
        Idempotent by egress name."""

    async def start(self) -> None:
        """Run migrations, then start one consumer task per handler."""

    async def stop(self) -> None:
        """Stop consumer tasks, release held leases, close the database."""

    def on(self, hook: Literal['success', 'failed', 'dead'],
           fn: Callable[[Event, str], None]) -> None:
        """Process-local observability. Not persisted, not replayed."""
```

```python
@dataclass
class EventInput:
    kind: str
    source: str
    payload: dict
    at: str | None = None            # defaults to receipt time
    dedupe_key: str | None = None
    meta: dict[str, str] = field(default_factory=dict)

@dataclass
class PublishResult:
    status: Literal['accepted', 'duplicate']
    event_id: str
```

`EventInput` has no `id` — the core assigns a ULID. Adapters supply facts; the core owns
identity.

`PublishResult` no longer reports a fan-out count. With consumer groups, no delivery rows
exist at publish time; every group implicitly owes every message.

There is no `publish_and_wait`. It was designed to support commands, and commands are out of
scope; when they arrive, the natural form is a correlation id plus a reply handler rather
than a blocking publish, since the awaiting caller does not survive a restart anyway.

### Ingress

```python
class Ingress(Protocol):
    name: str
    async def start(self, publish: Publish) -> None: ...
    async def stop(self) -> None: ...

Publish = Callable[[EventInput], Awaitable[PublishResult]]
```

An ingress owns its transport entirely — the core runs no HTTP server. `GitHubIngress`
starts a listener, verifies the HMAC, calls `publish`, and responds `200` as soon as
`publish` returns. Because `publish` only awaits the log append, that response stays fast
regardless of how slow any egress is, which keeps GitHub's 10-second webhook timeout
irrelevant.

The ingress receives only the `publish` callable, not the broker — it cannot attach
egresses or inspect delivery state.

### Egress and handlers

```python
Filter = Callable[[Event], bool]

class Handler(Protocol):
    name: str                        # 'pr-to-eng' → group_id 'discord/pr-to-eng'
    filter: Filter
    timeout_s: float | None          # per-dispatch cap; defaults to broker's 30s
    lease_s: float | None            # lease duration; defaults to timeout_s * 2
    async def handle(self, event: Event, ctx: Any) -> None: ...

class Egress(Protocol):
    name: str                        # 'discord'
    filter: Filter | None            # coarse gate
    handlers: list[Handler]
    def context(self) -> Any:        # utilities handed to its handlers
        ...
```

The egress owns the connection, auth, and rate limiter once. Attaching five handlers does
not create five rate limiters competing for the same API quota.

Handler context has two halves. The broker supplies capabilities every handler has; the
egress supplies its own, typed to itself. A Discord handler therefore gets Discord
capabilities without holding its own credentials, and gets `publish` without the Discord
egress knowing what publishing is.

```python
@dataclass
class Ctx(Generic[E]):
    publish: Publish     # from the broker — same signature an ingress gets
    egress: E            # from egress.context()

class DiscordCtx(Protocol):
    async def send(self, channel: str, msg: DiscordMessage) -> MessageRef: ...
    async def edit(self, ref: MessageRef, msg: DiscordMessage) -> None: ...
```

A handler then reads `ctx.publish(...)` and `ctx.egress.send(...)`.

### Handlers that publish

`ctx.publish` is deliberately the same callable an ingress receives. A handler is therefore
also an event source, which makes pipelines fall out of the existing machinery rather than
needing a workflow engine:

```
github.home.pr.merged → handler → deploy.requested → handler → deploy.completed → notify
```

Each stage is an independent consumer group with its own offset, retry budget, and
dead-letter queue. A stage that fails retries without re-running the stages before it.

**Duplicates are possible and accepted.** A handler that publishes and then crashes before
settling will be redelivered and will publish again. This is the at-least-once contract
applied one level up, and it is why every published event should carry a `dedupe_key`
derived from its cause rather than from wall-clock time — `f"{event.id}:{handler.name}"` is
usually right, since it is stable across redeliveries of the same input.

The stronger guarantee — appending a handler's output and settling its input in one
transaction — is a transactional outbox, and would require a new primitive in mamamia
(`settle` and `append` committing together). Deliberately deferred: at-least-once with
dedupe keys is sufficient until real pipelines exist and show that duplicates actually hurt.

**Cycle safety.** A handler whose published event matches its own filter loops forever, and
the durable log means it loops forever *across restarts*. Two guards, both cheap:

- Every published event carries `meta["caused_by"]` (the id of the event being handled) and
  `meta["depth"]` (the cause's depth plus one). Origin events from an ingress have depth 0.
- The broker rejects a publish whose depth exceeds `MAX_CHAIN_DEPTH` (default 16), raising
  rather than silently dropping, so a runaway pipeline fails loudly at its source.

`caused_by` also makes a chain reconstructable from the log after the fact — given any
event, its full causal ancestry is a walk back through `meta`.

### Filtering

Filters are predicates, not patterns. They subsume any subject-wildcard scheme while also
being able to read `payload`, which a subject matcher fundamentally cannot:

```python
filter = lambda e: e.source == 'github' and 'urgent' in e.payload.get('labels', [])
```

The effective filter for a handler is `egress.filter(e) and handler.filter(e)`.

Filters are evaluated **in the handler's own consumer loop**, at acquire time. An event that
fails the filter is settled immediately as success and the loop moves on.

This is a change from an earlier draft, which required filters to be registered upward to
the broker so it could write delivery rows before dispatching. That requirement was an
artifact of precomputing deliveries; mamamia creates per-group state lazily, defaulting any
unseen `(group, message)` to `PENDING`, so there is nothing to precompute and no window in
which a crash could lose track of who owed what.

Filters must be pure and cheap — they run once per handler per event. I/O in a filter is a
layering violation; that work belongs in `handle`.

Cost: a handler still transitions state for every event it filters out. At current volume
this is a few rows per day and is not worth optimising away.

### Kind naming

Hierarchical, dot-separated, `source.entity.action`:

```
github.home.pr.opened
github.home.check_run.failed
```

A naming convention only — used for logs, grouping, and metrics. There is no subject
matcher and no wildcard syntax.

## The dispatcher

One asyncio task per handler. Each task is an independent mamamia consumer:

```python
async def consume(handler, ctx, orchestrator):
    group_id = f"{egress.name}/{handler.name}"
    while running:
        msg = await orchestrator.acquire_next(
            log_id="events", group_id=group_id,
            client_id=instance_id, duration=handler.lease_s,
        )
        if msg is None:
            await asyncio.sleep(1.0)
            continue

        event = Event(**msg.payload)
        if not passes_filter(event):
            await orchestrator.settle(..., success=True)
            continue

        try:
            async with asyncio.timeout(handler.timeout_s):
                await handler.handle(event, ctx)
            await orchestrator.settle(..., outcome='success')
        except PermanentError:
            await orchestrator.settle(..., outcome='dead')
        except Exception:
            attempts = await state.get_retry_count(...)
            await orchestrator.settle(
                ..., outcome='retry', retry_after=backoff(attempts),
            )
```

**A slow handler blocks only itself.** Each handler is its own task and its own consumer
group with its own offset, so one blocked handler cannot delay any other. This is the
property the design exists to provide, and it comes free from consumer groups.

**Leases replace an in-flight set.** `acquire_next` atomically marks the message
`IN_PROGRESS` and takes a lease, so no other task or process can pick it up. Nothing needs
tracking in memory.

**Crash recovery is automatic.** Leases expire. A message left `IN_PROGRESS` by a crash is
reacquired once its lease lapses — no boot-time reset, no stale sweeper, and no
single-process assumption.

**Per-dispatch timeout**, default 30s, overridable per handler: a webhook POST and a slow
API call have genuinely different expectations. Exceeding it settles the delivery as failed
for normal retry.

**Lease duration defaults to `2 × timeout_s`** so a lease cannot lapse while its handler is
still legitimately working — otherwise another task could acquire the same message
concurrently. The ownership check in `settle` is the backstop when that assumption fails.

**Polling interval is 1s** when the log is drained. Dispatch latency is therefore up to one
second, deliberately: it is imperceptible on a notification and keeps a wake-signal
mechanism out of the loop.

## Delivery semantics

- **At-least-once.** A crash between handler success and `settle` replays that message once
  the lease expires. For notifications a duplicate is noise, which is the right trade
  against loss.
- **Deduplication** on `dedupe_key` — see *Deduplication* below. Rejected duplicates are
  logged, not errored.
- **Retry** is mamamia's `FAILED` state with an incrementing count. Switchboard computes the
  delay — exponential with jitter, 1s, 2s, 4s … capped at 5 minutes — and passes it as
  `retry_after` on settle. mamamia enforces the deferral; the schedule is Switchboard's.
- **Dead-letter** after 10 attempts: state `DEAD`, retained for inspection, never retried
  automatically.
- **Permanent failure short-circuits retries.** A handler raising `PermanentError` — a
  malformed payload, an unroutable target, a 4xx that will never become a 2xx — settles as
  `dead` immediately rather than retrying nine more times to reach the same conclusion.
  Distinguishing permanent from transient is the handler's judgement; only it has the
  context to know.
- **Isolation.** A handler's failure affects only its own consumer group.
- **No ordering guarantee.** A failed message retried after backoff will land after messages
  that succeeded immediately. Strict ordering is incompatible with per-message retry, and
  the alternative — head-of-line blocking — would stall a channel for the full retry budget
  on one poison event. Each notification carries its own timestamp and is independently
  meaningful.

## v1 scope

**Ingress:** `GitHubIngress` — an HTTP endpoint, HMAC-SHA256 verification via stdlib `hmac`
with `compare_digest`, mapping webhook payloads to events.

| GitHub event | Kind |
|---|---|
| `pull_request` (opened/closed/merged) | `github.<repo>.pr.<action>` |
| `pull_request_review` (submitted) | `github.<repo>.review.<state>` |
| `pull_request` (review_requested) | `github.<repo>.review.requested` |
| `issues` (opened/closed) | `github.<repo>.issue.<action>` |
| `check_run` (completed, conclusion=failure) | `github.<repo>.check_run.failed` |

CI successes are never relayed. Failures only.

**Core:** SQLite/WAL implementations of `IMessageStorage`, `IStateStore`, and
`ILeaseManager`; migrations; the broker facade; per-handler consumer tasks.

**Egress:** `LoggerEgress` — one handler, filter `e.source == 'github'`, writing structured
JSON to stdout.

Discord is deliberately not the first egress. A logger closes the vertical slice — webhook →
HMAC → event → log → lease → dispatch → output — with no external dependency, no message
formatting decisions, and no rate limits. Every durability and retry property can be
exercised against it, so `DiscordEgress` later becomes a pure translation problem against a
system already proven to work.

`LoggerEgress` also stays useful permanently: attached with `filter=lambda e: True`, it is
the debug tap.

**Not in v1:** Discord egress, commands, Discord ingress, GitHub egress, multi-channel
routing, identity mapping.

### v1.1 — Discord egress

`DiscordEgress` posting to a single channel via a channel webhook URL, using `httpx`. No bot
application, no gateway connection, no interactions endpoint. Message format is an open
question to be settled against real captured events, not designed up front.

## Stack and deployment

- **Runtime:** Python 3.12, asyncio throughout.
- **Dependencies:** `mamamia` (in-process library), `aiosqlite`, `httpx`, and
  `starlette`/`uvicorn` for the webhook endpoint. Pydantic comes in via mamamia. HMAC
  verification needs no dependency.
- **Packaging:** Docker image built for `linux/arm64`. Docker is what delivers portability —
  the identical image runs on the Pi today and on a VPS later with no code change.
- **Ingress reachability:** Cloudflare Tunnel. The Pi is behind residential NAT with no
  public address. The tunnel is outbound-only, so no ports are forwarded and no dynamic DNS
  is needed, and it gives a stable HTTPS hostname for the GitHub webhook. Moving off the Pi
  changes only where the tunnel daemon runs.
- **Persistence:** SQLite file on a mounted volume, so container replacement does not lose
  the log.
- **Secrets:** `GITHUB_WEBHOOK_SECRET` and later `DISCORD_WEBHOOK_URL`, supplied as
  environment variables from a `.env` file on the Pi that is `chmod 600` and never
  committed. Docker Compose reads it via `env_file`.

## Configuration

Adapters and handlers are wired explicitly in Python, not discovered from config — a
deliberate rejection of a plugin system. With two adapters, explicit wiring is clearer,
type-checked, and greppable.

```python
broker = Broker(db_path="/data/switchboard.db")
broker.attach(LoggerEgress(
    filter=lambda e: e.source == "github",
    handlers=[log_all],
))
await broker.start()
await GitHubIngress(secret=env.GITHUB_WEBHOOK_SECRET).start(broker.publish)
```

## Error handling

| Failure | Behavior |
|---|---|
| Invalid HMAC signature | 401, no event appended, logged as a security event |
| Malformed payload | 400, logged; nothing appended |
| Unknown webhook event type | 200 no-op — GitHub must not see failures for events we ignore |
| Egress 5xx / network error | Settled failed, retried with backoff |
| Egress 429 (v1.1, Discord) | Respect `Retry-After`; egress-level rate limiter throttles its handlers |
| Handler raises | Settled failed; other handlers unaffected |
| Handler hangs | Timeout (per-handler, default 30s), settled failed; other handlers unaffected |
| Crash mid-dispatch | Lease expires; message reacquired and redispatched |
| Lease expired before settle | `settle` rejects on ownership mismatch; the new owner's result stands |
| SQLite locked | Retry with backoff; WAL mode makes this rare for a single writer |

## Observability

- Structured JSON logs to stdout; Docker owns rotation.
- A `/health` endpoint on the ingress server, for the tunnel and for a Pi-side watchdog.
- Dead letters are inspected over SQLite directly. v1 ships a `switchboard dead-letters`
  CLI subcommand listing the retained `DEAD` rows — "SSH in and write SQL" is not an
  inspection story, and 500 retained rows are worthless without a way to read them.

## Testing

- **Unit:** filter evaluation, backoff schedule, dedup, kind mapping from webhook payloads.
- **Backend conformance:** the SQLite backends are tested against the same suite as
  mamamia's in-memory backends, since they must be behaviorally interchangeable.
- **Integration:** recorded real GitHub webhook payloads → broker → fake egress, asserting
  correct kinds and terminal states.
- **Durability:** publish, kill the process mid-dispatch, restart, assert the message is
  redelivered exactly once against a fake egress that records calls.
- **Lease exclusivity:** two consumers in the same group must never hold the same message.
- **Slow handler isolation:** with one handler blocked, other handlers must keep delivering.
- **Hung handler:** a handler that never resolves times out and is settled failed.
- **Ownership:** a settle from an expired-lease owner must be rejected.

Discord and GitHub are faked at the HTTP boundary. No live API calls in tests.

## Future work

- **Multiplexed / higher-throughput backends** in mamamia (Redis, Postgres) if event volume
  ever justifies it. No Switchboard change — that is the point of the backend interfaces.
- **Backfill after downtime:** GitHub exposes `GET /repos/{o}/{r}/hooks/{id}/deliveries`
  (last 30 days) and a redeliver endpoint, so missed events are recoverable via the API even
  though GitHub does not retry automatically. A boot-time reconcile would list deliveries
  since the newest stored event and replay the unseen ones.
- **Commands:** Discord → GitHub actions. Requires exactly-once rather than at-least-once —
  a duplicate notification is noise, a duplicate merge is damage — plus a correlation id to
  route the result back to its originating channel.
- **Discord ingress / GitHub egress:** each provider becomes a connector implementing both
  halves. Additive; no change to the event type.
- **Multi-process or distributed:** leases already carry owner and expiry, so the *locking*
  is multi-process-safe. Offset sliding is not — `Orchestrator._slide_lock` is an in-process
  `asyncio.Lock`. This is a mamamia roadmap item, not a Switchboard one, and until it lands
  Switchboard runs as a single process.
- **Multi-channel routing:** additional handlers with narrower filters. No core change.

## Risks

| Risk | Mitigation |
|---|---|
| mamamia is pre-1.0 and may change under us | Pin the version; build only against released tags, never an unreleased branch |
| Shared ownership erodes the mamamia/Switchboard boundary | Every upstream change must pass the *Division of concerns* test, applied at review time |
| v1 now spans two repos, so mamamia work gates Switchboard work | Upstream changes are small and independently valuable; land and release them before Switchboard starts |
| SQLite backends must match in-memory semantics exactly | Shared conformance suite is a v1 deliverable, not follow-up |
| Lease duration mistuned → concurrent delivery of one message | Default `2 × timeout`; ownership check on settle as backstop |
| Pi power loss corrupting the log | SQLite WAL mode; durability test in the suite |
| Residential network unreliability | Retry with backoff; the log survives disconnection |
| Pi has no RTC; clock is wrong until NTP syncs | Lease expiry uses monotonic time where possible; wall clock only for display |
| GitHub does not retry failed webhook deliveries | Ack fast — verify and append, then return 200 before any dispatch work |
