# Switchboard — Design

**Date:** 2026-07-21 (revises the 2026-07-19 design)
**Status:** Approved for planning — reconciled with mamamia `v0.2.0`
**Owner:** yellowpages.ink

> **What changed in this revision.** The original design listed four mamamia
> changes plus an audit as prerequisites, and had Switchboard *implement* the
> SQLite backends and upstream them. That work is **done and released as mamamia
> `v0.2.0`** — deferred redelivery (`retry_after`), explicit `Outcome`
> (`success`/`retry`/`dead`), strict settle, and persistent SQLite/WAL backends
> tested by a shared conformance suite, plus concurrency and durability
> hardening. So Switchboard no longer builds storage: **v1 is the application
> layer on top of mamamia `v0.2.0`, consumed as a pinned library.** Everything
> below reflects that.

## Summary

**Switchboard turns things that happen in one system into actions in another.**

It is a general-purpose, event-driven relay engine. Ingress adapters publish
events; egress handlers consume and act on them. All domain knowledge lives in
adapters; durability, leasing, retry, and dead-lettering are delegated to
[mamamia](https://github.com/yellowpages-ink/mamamia), an append-only log with
consumer groups and JIT leasing, used **in-process as a library**.

The first application is GitHub repository activity relayed into Discord. The
engine itself knows nothing about GitHub or Discord.

The name is literal: a switchboard patches incoming lines to outgoing lines. Any
ingress, any egress, topology as configuration.

## Goals

- Relay GitHub repo activity into Discord, without losing events that have
  already been received — including across process crashes and restarts.
- Keep the core generic so new providers are new adapters, not core changes.
- Run on a Raspberry Pi today, move to a hosted environment later with no code
  change.
- Reuse mamamia rather than reimplementing a queue.
- Async-first throughout. Every interface boundary is a coroutine.

## Non-goals (v1)

- No writes back to GitHub. Read-only relay.
- No Discord slash commands. One-way feed only.
- No multi-channel routing. One channel.
- No plugin loader or config-driven adapter discovery. Adapters are wired
  explicitly in code.
- No mamamia TCP server. The `Orchestrator` (via `LogRegistry`) is used
  in-process; mamamia's client/server and binary protocol are unused.
- **No backfill of events missed while offline.** If the process is down when a
  webhook fires, GitHub's delivery fails and that event is lost. Accepted: at
  current volume the cost of a missed notification is near zero, and the Pi is a
  temporary home. See *Future work* for the recovery path if that changes.

## What Switchboard builds on: mamamia v0.2.0

mamamia provides exactly the delivery semantics Switchboard needs — a durable
log, per-consumer progress, an exclusive claim while processing, retry with a
dead-letter ceiling — and as of `v0.2.0` provides them durably. The pieces
Switchboard consumes:

- **Persistent SQLite/WAL backends** — `SQLiteStorage`, `SQLiteStateStore`,
  `SQLiteLeaseManager` over one connection (`synchronous=FULL`, crash-durable),
  behaviorally interchangeable with the in-memory backends via a shared
  conformance suite. Switchboard does **not** implement these.
- **`Orchestrator`** — `acquire_next(log_id, group_id, client_id, duration)` /
  `settle(log_id, group_id, message_id, client_id, outcome, retry_after)` /
  `extend(...)`, with atomic lease-first acquisition, lazy reap that increments
  the retry count on abandonment, and a **strict settle** that requires a live
  lease owned by the caller.
- **`Outcome`** enum — `Outcome.SUCCESS`, `Outcome.RETRY` (honours
  `retry_after`), `Outcome.DEAD` (skips the retry ceiling). This is the real
  API; the original spec's string outcomes were the enum's *values*.
- **`LogRegistry`** — bundles an `Orchestrator` per log, **opt-in retention +
  a background reaper**, and **long-polling** (`acquire_blocking`) with
  **publish wake-ups** (`notify`). Switchboard's `Broker` is a thin facade over
  it rather than over the raw `Orchestrator`.
- **Per-operation transactions** — `SQLiteTransaction` groups each `Orchestrator`
  op's writes into one fsync and makes it atomic. This is internal to mamamia's
  log; Switchboard's dedup lives in a separate database and is made crash-safe by
  *ordering*, not a shared transaction (see *Deduplication*).

**mamamia now uses the standard-library `sqlite3` driver** (synchronous, on the
event-loop thread) — not aiosqlite. Switchboard therefore needs no async SQLite
dependency; it shares mamamia's connection.

Versioning: Switchboard pins mamamia `v0.2.0` (a released tag). For
co-development it may install mamamia editable from a local checkout, but CI and
the Docker image build against the pinned tag, never an unreleased branch.

### Division of concerns (still enforced)

mamamia is a message delivery system; Switchboard is an application on it. The
boundary is enforced by one test applied to every proposed mamamia change:

> **Would a message delivery system need this regardless of yellowpages?**

If yes, it belongs in mamamia and must be designed generically. If no,
Switchboard owns it. The v0.2.0 upstreaming already passed this test — retry
*mechanism*, lease correctness, retention, and persistent backends are generic;
retry *policy*, dedup, filtering, routing, and adapters are Switchboard's.

Both repos live in `yellowpages-ink` and are private for now. Privacy removes
the external forcing function that keeps the boundary honest, so the test is
applied deliberately at review time.

### Known mamamia limitation, not worked around

`Orchestrator._slide_lock` is an in-process `asyncio.Lock`, so offset sliding is
safe only within one process; the SQLite connection is single-writer and
loop-confined. Multi-instance deployment is blocked by this regardless, and is
tracked on mamamia's roadmap. **Switchboard runs as a single process** and does
not attempt to work around it.

## Architecture

```
GitHub ──webhook──▶ GitHubIngress
                          │ publish()
                          ▼
                 ┌────────────────────────────────────────┐
                 │  Broker  (facade over mamamia)          │
                 │                                         │
                 │   mamamia LogRegistry ──────▶ mamamia   │
                 │     Orchestrator("events")     db (WAL) │
                 │     SQLiteStorage/State/Lease           │
                 │        (embedded now, remote later)     │
                 │                                         │
                 │   seen dedup table ─────▶ switchboard   │
                 │     (own sqlite3 conn)      db (WAL)     │
                 └────────────────────────────────────────┘
                          │ acquire_blocking(group_id=handler)
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

**mamamia owns:** the append-only log, per-group offsets and message state,
leases, retry counts, dead-lettering, the SQLite backends, retention, and the
reaper.

**Switchboard core owns:** the event envelope and identity, adapter lifecycle,
the per-handler consumer loops, dispatch timeouts, retry *policy* (the backoff
schedule), and deduplication.

**Adapters own** translation only: bytes → `Event` on ingress, `Event` → side
effect on egress. An adapter doing its own scheduling or retry is in the wrong
layer — the single most important rule in the codebase.

## Data model

### Event — an immutable fact

An event is the `payload` of a mamamia message. It carries no targets, no
status, no reply address; those are delivery concerns.

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

The `Event` is stored as a mamamia message `payload` (a plain dict — mamamia
round-trips it through msgpack on the SQLite backend, so payloads must be
msgpack-serializable: string dict keys, list not tuple, 64-bit ints). `meta`
carries transport-level provenance (delivery id, signature alg, receipt time)
that is not part of the fact; adapters write it, handlers may read it, the core
only persists it.

### Delivery state — owned by mamamia

Switchboard has **no deliveries table.** Per-handler delivery state is mamamia's
per-`(log_id, group_id, message_id)` state, plus its lease and retry count.

| Switchboard concept | mamamia concept |
|---|---|
| event log | `log_id` (one log, `'events'`) |
| handler | `group_id` (consumer group, `'<egress>/<handler>'`) |
| delivery | `(group_id, message_id)` state + lease + retry count |
| in flight | `IN_PROGRESS` + live lease |
| delivered | `PROCESSED` |
| retryable failure | `FAILED`, retry count incremented |
| dead-lettered | `DEAD` (retry ceiling reached, or `Outcome.DEAD`) |

A handler is precisely a consumer group over the event log. Every group
independently consumes every message, so fan-out is inherent — nothing to
compute or persist at publish time.

### Storage — two databases, isolated by owner

mamamia and Switchboard keep **separate** databases; Switchboard never reaches
into mamamia's schema or shares its connection. This is deliberate: mamamia is a
*service* — embedded today, but a candidate to become a remote (TCP) server
later — and a remote mamamia has no connection to share. Designing to a shared
connection now would bake in a coupling that a remote deployment breaks.
Isolation also forces dedup to work *without* cross-system atomicity, which is
the honest constraint either way (see *Deduplication*).

- **mamamia's log.** In embedded mode Switchboard opens it via
  `mamamia.server.db.connect(mamamia_db_path)`, builds the `SQLite*` backends and
  a `SQLiteTransaction`, and hands them to a `LogRegistry`. mamamia owns this
  schema and its migrations, and its `synchronous=FULL` covers the Pi's lack of a
  UPS. In a future remote mode this is replaced by a mamamia client and the file
  lives on the server — the `Broker` talks to mamamia through the same narrow set
  of calls (publish/append, acquire, settle, retry-count, notify) either way.
- **Switchboard's own db.** A separate SQLite file (WAL) that Switchboard opens
  with its own `sqlite3` connection, holding the `seen` dedup table and any
  future Switchboard-owned state. mamamia never sees it.

Both are files on the mounted volume; each migrates as a copy. Embedded, that is
two small files; once mamamia is remote, Switchboard's own state is a single tiny
file and it is otherwise stateless.

### Retention — configured, owned by mamamia

Retention is a mamamia feature (`LogRegistry(max_log_messages=..., max_dead=...)`
plus `start_reaper(interval)`), configured by Switchboard. An SD card cannot grow
unbounded, so:

- **Messages**: bounded log, oldest rows pruned beyond a configured cap (default
  10,000), never below the lowest base offset across groups.
- **State rows**: `PROCESSED` rows pruned behind the group's base offset; `DEAD`
  rows retained (capped, default 500) — pruning them would destroy the record of
  what failed.

The reaper runs on an interval. Note (from mamamia's operational characteristics):
a very large prune runs synchronously on the loop thread, so keep the cap modest
and the interval unhurried; at Pi volumes this is a handful of rows.

### Deduplication — owned by Switchboard, in its own db

The dedup key, window, and definition of "same event" are application policy;
mamamia's log stays a pure append-only log. Switchboard keeps a `seen` table in
**its own database** mapping the provider's idempotency key (GitHub's
`X-GitHub-Delivery` UUID) to the event id it produced.

Because the log and the `seen` table are separate databases (and mamamia may be
remote), **dedup cannot share a transaction with the append.** `publish` is
therefore *ordered* so the only failure mode is a harmless duplicate, never loss:

1. Check `seen[delivery_id]`; if present, return `duplicate` — nothing appended.
2. **Append to mamamia first** (durable), obtaining the event id.
3. Record `seen[delivery_id] = event_id` in Switchboard's db.
4. Call `registry.notify("events")` so a long-polling consumer wakes.

A crash between (2) and (3) leaves an appended-but-unrecorded event: a later
redelivery re-appends it, producing a duplicate notification — noise, never loss.
The opposite order (record-then-append) would risk *dropping* an event on a crash,
which is why append comes first. True-concurrent identical deliveries (both
passing step 1) are not part of GitHub's delivery model — redeliveries are manual
or retried and temporally separated — so the check-then-act window is accepted
rather than closed with a cross-db lock.

Entries are pruned on the log's retention schedule; once one ages out, a
redelivery of that webhook is processed as new. At current volume the window is
months.

### Schema migrations

Each database owns its own schema. mamamia manages its tables via
`PRAGMA user_version` (embedded, or on its own server when remote). Switchboard's
only table is `seen`, applied as a single idempotent `CREATE TABLE IF NOT EXISTS`
tracked by `PRAGMA user_version` on the Switchboard connection at boot. No
migration framework for one table.

## Core interfaces

### Broker

A thin facade over mamamia's `LogRegistry`. It owns the envelope, assigns
identity, runs consumer loops, and configures retention — it does not reimplement
queueing.

```python
class Broker(Protocol):
    async def publish(self, event: EventInput) -> PublishResult:
        """Dedupe (own db), then append to mamamia's log, then record the
        dedup key — ordered so a crash yields a harmless duplicate, never loss.
        Returns once the message is durably committed — NOT once handlers have
        run — so any ingress can acknowledge its transport immediately."""

    def attach(self, egress: Egress) -> None:
        """Register an egress and its handlers. Each handler becomes a consumer
        group. Idempotent by egress name."""

    async def start(self) -> None:
        """Open both databases (mamamia's log via connect+migrations, and
        Switchboard's own db + seen table), build the LogRegistry, start the
        reaper, and start one consumer task per handler."""

    async def stop(self) -> None:
        """Stop consumer tasks, let held leases lapse, close the database."""

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

`EventInput` has no `id` — the core assigns a ULID. There is no `publish_and_wait`
(commands are out of scope; the natural form when they arrive is a correlation id
plus a reply handler, since the awaiting caller does not survive a restart).

### Ingress

```python
class Ingress(Protocol):
    name: str
    async def start(self, publish: Publish) -> None: ...
    async def stop(self) -> None: ...

Publish = Callable[[EventInput], Awaitable[PublishResult]]
```

An ingress owns its transport entirely — the core runs no HTTP server.
`GitHubIngress` starts a listener, verifies the HMAC, calls `publish`, and
responds `200` as soon as `publish` returns. Because `publish` only awaits the
(durable) append, that response stays fast regardless of egress speed, keeping
GitHub's 10-second webhook timeout irrelevant. The ingress receives only the
`publish` callable, not the broker.

### Egress and handlers

```python
Filter = Callable[[Event], bool]

class Handler(Protocol):
    name: str                        # 'pr-to-eng' → group 'discord/pr-to-eng'
    filter: Filter
    timeout_s: float | None          # per-dispatch cap; defaults to broker's 30s
    lease_s: float | None            # lease duration; defaults to timeout_s * 2
    async def handle(self, event: Event, ctx: Any) -> None: ...

class Egress(Protocol):
    name: str                        # 'discord'
    filter: Filter | None            # coarse gate
    handlers: list[Handler]
    def context(self) -> Any: ...     # utilities handed to its handlers
```

The egress owns the connection, auth, and rate limiter once — attaching five
handlers does not create five rate limiters competing for one API quota.

Handler context has two halves — capabilities every handler gets from the broker,
and the egress's own typed capabilities:

```python
@dataclass
class Ctx(Generic[E]):
    publish: Publish     # from the broker — same signature an ingress gets
    egress: E            # from egress.context()
```

A handler reads `ctx.publish(...)` and `ctx.egress.send(...)`.

### Handlers that publish → pipelines

`ctx.publish` is deliberately the same callable an ingress receives, so a handler
is also an event source and pipelines fall out of the existing machinery:

```
github.home.pr.merged → handler → deploy.requested → handler → deploy.completed
```

Each stage is an independent consumer group with its own offset, retry budget,
and dead-letter queue; a failed stage retries without re-running earlier ones.

**Duplicates are possible and accepted** — a handler that publishes then crashes
before settling replays and publishes again (at-least-once, one level up). Every
published event should carry a `dedupe_key` derived from its cause, not
wall-clock — `f"{event.id}:{handler.name}"` is usually right. (The stronger
guarantee, appending output and settling input in one transaction, is a
transactional outbox needing a new mamamia primitive — `settle`+`append`
together — deliberately deferred.)

**Cycle safety.** Every published event carries `meta["caused_by"]` (the handled
event's id) and `meta["depth"]` (cause depth + 1; ingress origins are depth 0).
The broker rejects a publish whose depth exceeds `MAX_CHAIN_DEPTH` (default 16),
raising loudly. `caused_by` also makes a chain's causal ancestry reconstructable
from the log.

### Filtering

Filters are predicates, not patterns — they can read `payload`, which a subject
matcher cannot:

```python
filter = lambda e: e.source == 'github' and 'urgent' in e.payload.get('labels', [])
```

The effective filter is `egress.filter(e) and handler.filter(e)`. Filters run
**in the handler's own consumer loop at acquire time**; an event that fails is
settled immediately as `Outcome.SUCCESS` and the loop moves on. mamamia creates
per-group state lazily (any unseen `(group, message)` defaults to `PENDING`), so
there is nothing to precompute and no crash window. Filters must be pure and
cheap; I/O in a filter is a layering violation.

### Kind naming

Hierarchical, dot-separated, `source.entity.action` (e.g. `github.home.pr.opened`).
A convention only — no subject matcher, no wildcards.

## The dispatcher

One asyncio task per handler, each an independent mamamia consumer. It uses
mamamia's **long-polling** `acquire_blocking` with publish wake-ups rather than a
fixed sleep-poll, so idle handlers wake within the log's notify path instead of
on a 1s timer:

```python
async def consume(handler, ctx, registry):
    group_id = f"{egress.name}/{handler.name}"
    while running:
        msg = await registry.acquire_blocking(
            log_id="events", group_id=group_id,
            client_id=instance_id, duration=handler.lease_s,
            wait_ms=poll_wait_ms,        # bounded wait (ms); wakes early on publish
        )
        if msg is None:
            continue

        event = Event(**msg.payload)
        if not (egress_ok(event) and handler.filter(event)):
            await orch.settle("events", group_id, msg.id, instance_id,
                              outcome=Outcome.SUCCESS)
            continue

        try:
            async with asyncio.timeout(handler.timeout_s):
                await handler.handle(event, ctx)
            await orch.settle("events", group_id, msg.id, instance_id,
                              outcome=Outcome.SUCCESS)
        except PermanentError:
            await orch.settle("events", group_id, msg.id, instance_id,
                              outcome=Outcome.DEAD)
        except Exception:
            attempts = await state.get_retry_count("events", group_id, msg.id)
            await orch.settle("events", group_id, msg.id, instance_id,
                              outcome=Outcome.RETRY, retry_after=backoff(attempts))
```

> **Why `acquire_blocking` (not a fixed poll).** The original spec used a
> deliberate 1s sleep-poll. mamamia `v0.2.0` ships `acquire_blocking` + `notify`,
> so sub-second dispatch is free: `publish` calls `registry.notify("events")`
> after a successful append, and each consumer long-polls with a bounded
> `wait_ms` (e.g. `30_000`) as the fallback heartbeat for the time-driven cases a
> publish can't signal — a lease expiring, a `retry_after` elapsing.

Properties, all inherited from mamamia:

- **A slow handler blocks only itself** — its own task, its own group/offset.
- **Leases replace an in-flight set** — `acquire_next` atomically marks
  `IN_PROGRESS` and takes a lease; nothing tracked in memory.
- **Crash recovery is automatic** — a lease left by a crash lapses and the
  message is reacquired; no boot-time reset.
- **Per-dispatch timeout** (default 30s, per-handler overridable) settles a hung
  handler as retry.
- **Lease duration defaults to `2 × timeout_s`** so a lease cannot lapse while a
  handler is still legitimately working; the settle ownership check is the
  backstop.

## Delivery semantics

- **At-least-once.** A crash between handler success and `settle` replays the
  message once the lease expires. For notifications a duplicate is noise, the
  right trade against loss.
- **Deduplication** on `dedupe_key`; rejected duplicates are logged, not errored.
- **Retry** is mamamia's `FAILED` with an incrementing count. **Switchboard owns
  the schedule** — exponential with jitter, 1s, 2s, 4s … capped at 5 minutes —
  passed as `retry_after`. mamamia enforces the deferral.
- **Dead-letter** after 10 attempts (`max_retries`): state `DEAD`, retained,
  never retried automatically.
- **Permanent failure short-circuits retries** — `PermanentError` →
  `Outcome.DEAD` immediately.
- **Isolation** — a handler's failure affects only its own group.
- **No ordering guarantee** — a retried message lands after ones that succeeded
  immediately; strict ordering is incompatible with per-message retry.

## v1 scope

**Ingress:** `GitHubIngress` — an HTTP endpoint (Starlette/uvicorn),
HMAC-SHA256 verification via stdlib `hmac.compare_digest`, mapping webhook
payloads to events:

| GitHub event | Kind |
|---|---|
| `pull_request` (opened/closed/merged) | `github.<repo>.pr.<action>` |
| `pull_request_review` (submitted) | `github.<repo>.review.<state>` |
| `pull_request` (review_requested) | `github.<repo>.review.requested` |
| `issues` (opened/closed) | `github.<repo>.issue.<action>` |
| `check_run` (completed, conclusion=failure) | `github.<repo>.check_run.failed` |

CI successes are never relayed; failures only.

**Core:** the `Broker` facade over `LogRegistry`; the `Event` envelope + ULID;
the `seen` table + ordered dedup-then-publish; per-handler consumer tasks; the backoff
schedule; retention configuration. **No storage implementation** — that is
mamamia's.

**Egress:** `LoggerEgress` — one handler, filter `e.source == 'github'`, writing
structured JSON to stdout. Discord is deliberately not first: a logger closes the
vertical slice (webhook → HMAC → event → log → lease → dispatch → output) with no
external dependency, so every durability/retry property is exercisable before
`DiscordEgress` becomes a pure translation problem. `LoggerEgress` also stays
useful as the debug tap (`filter=lambda e: True`).

**Not in v1:** Discord egress, commands, Discord ingress, GitHub egress,
multi-channel routing, identity mapping.

### v1.1 — Discord egress

`DiscordEgress` posting to a single channel via a channel webhook URL, using
`httpx`. No bot, no gateway, no interactions endpoint. Message format is settled
against real captured events, not designed up front.

## Stack and deployment

> **Validated on the target hardware.** mamamia `v0.2.0` benchmarked on the
> Raspberry Pi sustains ~293 durable (fsynced) consume cycles/s and ~4.5k
> appends/s, with p99 dispatch under 5 ms. The relayed workload is dozens of
> webhooks per *day*, so there is roughly five orders of magnitude of headroom;
> throughput is a non-concern. The one Pi-specific cost is the SD card's
> random-write penalty on the durable consume path, which is why retention caps
> stay modest (above) rather than because volume demands it.

- **Runtime:** Python 3.12, asyncio throughout.
- **Dependencies:** `mamamia` (pinned `v0.2.0`, in-process; pulls `pydantic` +
  `msgpack`), `httpx`, `starlette`/`uvicorn` for the webhook endpoint. HMAC needs
  no dependency. **No aiosqlite** — mamamia uses stdlib `sqlite3`, and Switchboard
  opens its own stdlib `sqlite3` connection for its dedup db.
- **Packaging:** Docker image for `linux/arm64`; the identical image runs on the
  Pi today and a VPS later.
- **Ingress reachability:** Cloudflare Tunnel — outbound-only, no port forwarding,
  stable HTTPS hostname for the GitHub webhook. Moving off the Pi changes only
  where the tunnel daemon runs.
- **Persistence:** both SQLite files (mamamia's log and Switchboard's own db) on
  a mounted volume, so container replacement does not lose the log or the dedup
  window.
- **Secrets:** `GITHUB_WEBHOOK_SECRET` (and later `DISCORD_WEBHOOK_URL`) as env
  vars from a `chmod 600`, never-committed `.env`, read via Compose `env_file`.

## Configuration

Adapters and handlers are wired explicitly in Python — a deliberate rejection of
a plugin system. With two adapters, explicit wiring is clearer, type-checked, and
greppable.

```python
broker = Broker(
    mamamia_db_path="/data/events.db",         # mamamia's durable log
    switchboard_db_path="/data/switchboard.db",  # dedup + Switchboard's own state
    max_log_messages=10_000,
)
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
| Duplicate delivery (seen key) | 200; `PublishResult(status='duplicate')`, logged, nothing appended |
| Egress 5xx / network error | Settled retry with backoff |
| Egress 429 (v1.1, Discord) | Respect `Retry-After`; egress-level rate limiter throttles its handlers |
| Handler raises | Settled retry; other handlers unaffected |
| Handler raises `PermanentError` | Settled dead immediately |
| Handler hangs | Per-handler timeout (default 30s), settled retry |
| Crash mid-dispatch | Lease expires; message reacquired and redispatched |
| Lease expired before settle | `settle` rejects on ownership mismatch; the new owner's result stands |
| SQLite locked | Rare for a single loop-confined writer; `busy_timeout` covers transient contention |

## Observability

- Structured JSON logs to stdout; Docker owns rotation.
- A `/health` endpoint on the ingress server, for the tunnel and a Pi-side
  watchdog.
- Dead letters: v1 ships a `switchboard dead-letters` CLI subcommand listing the
  retained `DEAD` rows — 500 retained rows are worthless without a way to read
  them, and "SSH in and write SQL" is not an inspection story.

## Testing

- **Unit:** filter evaluation, backoff schedule, dedup, kind mapping from webhook
  payloads, cycle-depth guard.
- **Integration:** recorded real GitHub webhook payloads → broker → fake egress,
  asserting correct kinds and terminal states.
- **Durability:** publish, kill the process mid-dispatch, restart, assert the
  message is redelivered exactly once against a fake egress that records calls.
- **Lease exclusivity / slow-handler isolation / hung-handler / ownership:**
  behaviours Switchboard depends on. These are covered by mamamia's own suite;
  Switchboard adds thin integration checks that its wiring preserves them.

Discord and GitHub are faked at the HTTP boundary — no live API calls in tests.
mamamia's SQLite backends are already conformance-tested upstream, so Switchboard
does not re-test storage semantics.

## Future work

- **Higher-throughput mamamia backends** (Redis, Postgres) if volume ever
  justifies it — no Switchboard change; that is the point of the interfaces.
- **Backfill after downtime:** GitHub's `GET .../hooks/{id}/deliveries` (last 30
  days) + redeliver endpoint make missed events recoverable; a boot-time
  reconcile would replay deliveries since the newest stored event.
- **Commands:** Discord → GitHub actions — needs exactly-once (a duplicate merge
  is damage) plus a correlation id, i.e. mamamia's deferred transactional-outbox
  primitive.
- **Discord ingress / GitHub egress:** each provider becomes a connector
  implementing both halves. Additive.
- **Multi-process / distributed:** blocked on mamamia's `_slide_lock` roadmap
  item, not a Switchboard concern until it lands.
- **Multi-channel routing:** additional handlers with narrower filters. No core
  change.

## Risks

| Risk | Mitigation |
|---|---|
| mamamia is pre-1.0 and may change under us | Pin `v0.2.0`; build only against released tags |
| Shared ownership erodes the mamamia/Switchboard boundary | Every upstream change must pass the *Division of concerns* test at review |
| SQLite ops block the loop (mamamia is synchronous) | Keep retention caps modest and the reaper interval unhurried; Pi volumes are tiny |
| Lease duration mistuned → concurrent delivery | Default `2 × timeout`; settle ownership check as backstop |
| Pi power loss corrupting the log | mamamia WAL + `synchronous=FULL`; durability test in the suite |
| Residential network unreliability | Retry with backoff; the log survives disconnection |
| Pi has no RTC; clock wrong until NTP syncs | Lease expiry tolerant of redelivery; wall clock only for display |
| GitHub does not retry failed webhook deliveries | Ack fast — verify and append, then return 200 before any dispatch work |
