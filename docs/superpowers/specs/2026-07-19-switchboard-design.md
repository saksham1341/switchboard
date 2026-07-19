# Switchboard — Design

**Date:** 2026-07-19
**Status:** Approved for planning
**Owner:** yellowpages.ink

## Summary

Switchboard is a general-purpose, event-driven relay engine. Ingress adapters publish
events onto a broker; egress adapters subscribe and act on them. The core is domain-agnostic
— it owns durability, filtering, retry, and delivery state. All domain knowledge lives in
adapters.

The first application is GitHub repository activity relayed into Discord. The engine itself
knows nothing about GitHub or Discord.

The name is literal: a switchboard patches incoming lines to outgoing lines. Any ingress,
any egress, topology as configuration.

## Goals

- Relay GitHub repo activity into Discord, without losing events that have already been
  received — including across process crashes and restarts.
- Keep the core generic so new providers are new adapters, not core changes.
- Run on a Raspberry Pi today, move to a hosted environment later with no code change.
- Stay small. The abstraction is cheap; the implementation must stay embarrassingly small.
- Async-first throughout. Every interface boundary returns a promise; no interface in this
  design returns a bare value.

## Non-goals (v1)

- No writes back to GitHub. Read-only relay.
- No Discord slash commands. One-way feed only.
- No multi-channel routing. One channel.
- No external broker (Redis/NATS). In-memory implementation behind an interface.
- No plugin loader, adapter registry, or config-driven adapter discovery. Adapters are
  wired explicitly in code.
- **No backfill of events missed while offline.** If the process is down when a webhook
  fires, GitHub's delivery fails and that event is lost. This is accepted: at current
  volume the cost of a missed notification is near zero, and the Pi is a temporary home.
  See *Future work* for the recovery path if this stops being acceptable.

## Architecture

```
GitHub ──webhook──▶ GitHubIngress ──publish──▶ ┌─────────────┐
                                               │   Broker    │
                                    ┌──────────┤  (in-mem)   │
                                    │          └──────┬──────┘
                              [ SQLite WAL ]          │ match filters
                              events (append)         │ write deliveries
                              deliveries (status)     ▼
                                               DiscordEgress
                                                 ├─ handler: pr-to-eng
                                                 └─ handler: ci-to-alerts
                                                          │
                                                          ▼
                                                      Discord
```

### Layer boundaries

**Core owns** (never duplicated in adapters): the WAL, deduplication, filter matching,
retry and backoff, delivery state, dead-lettering, dispatch ordering.

**Adapters own** translation only: bytes → `Event` on ingress, `Event` → side effect on
egress. An adapter performing its own scheduling or retry is in the wrong layer.

This boundary is the single most important rule in the codebase. Every adapter that
reimplements retry does so subtly wrong, and that bug class is never fully cleared.

## Data model

### Event — an immutable fact

```ts
type Event = {
  id: string          // ULID, assigned by core
  kind: string        // 'github.home.pr.opened'
  source: string      // 'github'
  at: string          // ISO 8601, when it occurred
  dedupeKey?: string  // provider-supplied idempotency key
  payload: unknown    // adapter-defined, opaque to core
  meta: Record<string, string>
}
```

An event carries no targets, no status, no reply address. Those are delivery concerns.
"Was it delivered?" is not a property of the fact that a PR opened.

### Delivery — a unit of work owed to one handler

```ts
type Delivery = {
  id: string
  eventId: string
  handlerId: string    // 'discord/pr-to-eng'
  status: 'pending' | 'sent' | 'failed' | 'dead'
  attempts: number
  lastError?: string
  nextRetryAt?: string
  replyTo?: string     // reserved for commands; unused in v1
}
```

Deliveries are distinct from events because their lifecycles differ:

| | `events` | `deliveries` |
|---|---|---|
| Mutability | append-only, never updated | status mutates |
| Cardinality | one per received webhook | one per matching handler |
| Meaning | a fact that happened | work owed to one handler |

Delivery identity is `(event, handler)` — not `(event, connector)`. If `#eng` succeeds
and `#alerts` fails, only `#alerts` retries. A single status field on the event cannot
represent two outcomes and would force either a duplicate post or a false failure.

Replay on boot is then `SELECT * FROM deliveries WHERE status = 'pending'`.

### Storage

SQLite with `PRAGMA journal_mode=WAL`. Chosen over a hand-rolled append-only log because
fsync durability, torn writes, replay, and compaction are already solved there — and the
Pi has no UPS, so power loss during a write is a realistic failure mode, not a hypothetical.

Single file. Migration to another host is a file copy.

### Retention

Unbounded growth on an SD card is not acceptable, so both tables are bounded:

- **Deliveries** are pruned once terminal. `sent` rows are deleted immediately.
  `dead` rows are retained (capped at the most recent 500) — pruning them on arrival would
  destroy the only record of what failed, which is the entire point of dead-lettering.
- **Events** are a bounded log: the oldest rows are deleted beyond a cap
  (`WAL_MAX_EVENTS`, default 10,000). An event is never pruned while it still has
  non-terminal deliveries.

Consequence: `dedupeKey` uniqueness is only as durable as the event log. Once an event ages
out, a redelivery of that same webhook would be processed as new. At current volume this
window is months, so it is accepted rather than tracked separately.

### Schema migrations

`PRAGMA user_version` plus an ordered array of migration functions, applied in a transaction
on boot. This is what `user_version` exists for; it is roughly thirty lines and adds no
dependency.

Alembic is SQLAlchemy-specific and Python-only, so it is not an option on this stack. The
Node equivalents (Drizzle Kit, Umzug, Kysely's migrator) are all reasonable, but each brings
a dependency and a CLI step to earn its keep — worth revisiting if the schema starts moving
often, or if we adopt a query builder for other reasons.

## Kinds and filtering

### Kind naming

Hierarchical, dot-separated, `source.entity.action`:

```
github.home.pr.opened
github.home.check_run.failed
```

This is a **naming convention only** — used for logs, grouping, and metrics. There is no
subject matcher, no trie, and no wildcard syntax. Filtering is done by predicates (below).

### Filtering is predicates, not patterns

An egress declares a coarse gate; its handlers refine it. Both are plain functions.

```ts
type Filter = (e: Event) => boolean

interface Egress<Ctx> {
  name: string
  filter?: Filter           // coarse gate, e.g. e => e.source === 'github'
  handlers: Handler<Ctx>[]
  context(): Ctx            // utilities handed to handlers
}

interface Handler<Ctx> {
  name: string
  filter: Filter            // e => e.kind.includes('.pr.')
  handle(e: Event, ctx: Ctx): Promise<void>
}
```

Predicates were chosen over pattern matching because they strictly subsume it — any subject
wildcard is expressible as a string predicate — while also being able to read `payload`,
which a subject matcher fundamentally cannot:

```ts
filter: e => e.source === 'github' && (e.payload as PrPayload).labels.includes('urgent')
```

A pattern language would be a less capable mechanism that we would then need to extend.

### Filters are evaluated by the broker, not privately by the egress

`broker.attach(egress)` registers each handler's effective filter — `egress.filter(e) &&
handler.filter(e)` — with the broker.

This is load-bearing: if the egress filtered internally, the broker could not know the
target set at publish time, so it could not write delivery rows before dispatching, and a
crash mid-fan-out would leave no record of which handlers had already run. The logical
model is still "the egress fans out to handlers whose filters pass" — the broker simply
owns the evaluation so it can be durable about the result.

Filters do not need to be serializable. They are evaluated in-process at publish time, and
what is persisted is the resulting set of handler IDs — plain strings.

Publish becomes:

```
publish(e) → evaluate filters → write N pending Delivery rows
           → dispatch each → update status
```

Filters must be pure and cheap: they run once per handler per event, on the publish path.
Side effects or I/O in a filter is a layering violation — that work belongs in `handle`.

### Handler context

Handlers receive typed utilities from their egress, so a Discord handler gets Discord
capabilities without importing transport concerns or holding its own credentials:

```ts
type DiscordCtx = {
  send(channel: string, msg: DiscordMessage): Promise<MessageRef>
  edit(ref: MessageRef, msg: DiscordMessage): Promise<void>
  thread(ref: MessageRef, name: string): Promise<ThreadRef>
}
```

The egress owns the connection, auth, and rate limiter once. Attaching five handlers does
not create five rate limiters competing against the same API quota.

## Core interfaces

### Broker

```ts
interface Broker {
  /**
   * Durably record an event and the deliveries it fans out to.
   *
   * Resolves once the event row and all delivery rows are committed to the WAL —
   * NOT once handlers have run. Dispatch proceeds in the background.
   * This guarantee is what lets any ingress acknowledge its transport quickly.
   */
  publish(input: EventInput): Promise<PublishResult>

  /**
   * As `publish`, but additionally waits for every delivery's FIRST dispatch
   * attempt to settle, and reports per-handler outcomes.
   *
   * Waits for the first attempt only — never for terminal state. Awaiting the full
   * retry budget could block for the better part of ten minutes.
   *
   * Times out after `opts.timeoutMs` (default 30s). A timeout affects only the
   * caller's view: the deliveries remain durable and continue retrying in the
   * background.
   */
  publishAndWait(input: EventInput, opts?: { timeoutMs?: number }): Promise<SettledResult>

  /** Register an egress and index its handlers' effective filters. Idempotent by name. */
  attach<Ctx>(egress: Egress<Ctx>): void

  /** Start dispatch and retry loops, replaying pending deliveries from the WAL. */
  start(): Promise<void>

  /** Stop accepting work, drain in-flight dispatches, close the WAL. */
  stop(): Promise<void>

  /** Process-local observability. Not persisted, not replayed. */
  on(hook: 'sent' | 'failed' | 'dead', fn: (d: Delivery, e: Event) => void): void
}

type EventInput = {
  kind: string
  source: string
  payload: unknown
  at?: string                        // defaults to receipt time
  dedupeKey?: string
  meta?: Record<string, string>
}

type PublishResult =
  | { status: 'accepted'; eventId: string; deliveries: number }
  | { status: 'duplicate'; eventId: string }   // dedupeKey already seen

type SettledResult = PublishResult & {
  outcomes: Array<{
    handlerId: string
    result: 'sent' | 'failed' | 'timeout'
    error?: string
  }>
}
```

Two methods rather than one method with a `wait` flag: the return types genuinely differ,
and a boolean that changes a return shape needs overloads to type honestly. Separate names
also make every blocking call site greppable.

Both are `async`. The distinction is what they await, not whether they block a thread —
the entire system is async-first, and no interface in this document returns a bare value.

Waiters are held in memory, keyed by event id. A crash discards the waiter but not the
work: deliveries are already durable and resume on boot. Nothing awaiting a `publishAndWait`
survives a restart, which is correct — the caller didn't either.

`id` is absent from `EventInput` — the core assigns a ULID. Adapters supply facts; the
core owns identity.

`PublishResult` reports the fan-out count so an ingress can log "accepted, 2 deliveries"
without querying the WAL.

### Ingress

```ts
interface Ingress {
  name: string
  start(publish: Publish): Promise<void>
  stop(): Promise<void>
}

type Publish = (input: EventInput) => Promise<PublishResult>
```

An ingress owns its own transport entirely — the core runs no HTTP server. `GitHubIngress`
starts an HTTP listener, verifies the HMAC, calls `publish`, and responds `200` as soon as
`publish` resolves. Because `publish` does not await dispatch, that response is fast
regardless of how slow Discord is, which keeps GitHub's 10-second webhook timeout
irrelevant to us.

The ingress is handed only the `publish` function, not the broker — it has no way to
attach egresses, inspect deliveries, or otherwise reach across the seam.

## Typed event kinds

The core treats `payload` as `unknown`. Adapters need real types, so a registry at the
edges provides inference without leaking domain types into the core:

The registry keys on the *invariant* part of the topic — the variable token (repo) is not
part of the key, because a wildcard string is not a matchable TypeScript key and inference
would silently never fire:

```ts
interface EventMap {
  'github.pr.opened': { repo: string; number: number; title: string; url: string; author: string }
  'github.check_run.failed': { repo: string; name: string; url: string; sha: string }
}

// Adapter publishes against the invariant key; core composes the concrete topic
// by interpolating the entity token: 'github.pr.opened' + repo 'home'
//   → 'github.home.pr.opened'
publish<K extends keyof EventMap>(kind: K, payload: EventMap[K]): void
```

One file. Core stays generic; adapters get full type checking.

## The dispatcher

One loop owns all outbound work. There is no separate "deliver now" path and "retry later"
path — first attempts and retries are the same operation on the same rows, which removes an
entire class of divergence between them.

`publish` writes rows and returns. It never dispatches inline.

```
loop:
  due = SELECT * FROM deliveries
        WHERE status = 'pending'
           OR (status = 'failed' AND nextRetryAt <= now)
        ORDER BY nextRetryAt
        LIMIT :batch
  dispatch each (up to maxConcurrent), update status
  sleep until woken, or 1s, whichever comes first
```

**Wake-on-publish.** Polling alone would add up to a full second of latency to every
notification. `publish` signals the loop after committing, so the common case dispatches
immediately and the 1s poll degrades to a safety net for due retries and anything a missed
signal dropped.

**In-flight guard.** A dispatch in progress still has `status='pending'` in the WAL, so the
next tick would pick it up again. The loop keeps an in-memory set of in-flight delivery ids
and skips them. Deliberately in-memory rather than a `dispatching` status: on crash the rows
stay `pending` and replay on boot, which is exactly the at-least-once behavior we want. A
persisted `dispatching` state would need crash-recovery logic to un-stick it.

**Concurrency cap.** `maxConcurrent` (default 5) bounds simultaneous dispatches so a burst
of due deliveries cannot stampede a downstream API.

## Delivery semantics

- **At-least-once.** A crash between "delivered" and "marked sent" replays that delivery.
  For notifications a duplicate is noise, which is the correct trade against loss.
- **Deduplication** on `dedupeKey` — GitHub's `X-GitHub-Delivery` UUID. Rejected duplicates
  are logged, not errored.
- **Retry** with exponential backoff and jitter: 1s, 2s, 4s … capped at 5 minutes.
- **Dead-letter** after 10 attempts. Status `dead`, retained for inspection, never retried
  automatically. Prevents a poison event from retrying forever.
- **Isolation.** Each dispatch is independently caught. One throwing handler marks only its
  own delivery row and never blocks siblings.
- **No ordering guarantee.** Deliveries dispatch concurrently and independently. Strict
  per-handler ordering is incompatible with per-delivery backoff — a failed `pr.opened`
  retrying in 30s while `pr.merged` succeeds immediately reorders them regardless. The
  alternative, head-of-line blocking, would stall a channel for the full retry budget on
  one poison event. Out-of-order notifications are the cheaper failure; each message
  carries its own timestamp and is independently meaningful.

## v1 scope

**Ingress:** `GitHubIngress` — HTTP endpoint, HMAC-SHA256 signature verification via
Octokit, maps webhook payloads to events.

Subscribed GitHub webhook events:

| GitHub event | Topic |
|---|---|
| `pull_request` (opened/closed/merged) | `github.<repo>.pr.<action>` |
| `pull_request_review` (submitted) | `github.<repo>.review.<state>` |
| `pull_request` (review_requested) | `github.<repo>.review.requested` |
| `issues` (opened/closed) | `github.<repo>.issue.<action>` |
| `check_run` (completed, conclusion=failure) | `github.<repo>.check_run.failed` |

CI successes are never relayed. Failures only.

**Broker:** `InMemoryBroker` — filter registry, SQLite-WAL persistence, retry loop,
replay on boot.

**Egress:** `LoggerEgress` — one handler, filter `e => e.source === 'github'`, writing each
event to stdout as structured JSON.

Discord is deliberately *not* the first egress. A logger closes the vertical slice —
webhook → HMAC → event → WAL → delivery → dispatch → output — with no external
dependency, no message formatting decisions, and no rate limits. Every durability and
retry property can be exercised against it. `DiscordEgress` then becomes a pure
translation problem against a system already proven to work, and message design is
deferred to the point where we can see real events flowing.

`LoggerEgress` also stays useful permanently: attached alongside Discord with filter
`() => true`, it is the debug tap.

**Not in v1:** Discord egress, commands, Discord ingress, GitHub egress, multi-channel
routing, slash commands, identity mapping.

### v1.1 — Discord egress

`DiscordEgress` posting to a single channel via a channel webhook URL. No bot application,
no gateway connection, no interactions endpoint. Message format is an open question to be
settled against real captured events, not designed up front.

## Deployment

- **Runtime:** Node 22 + TypeScript, shipped as a Docker image built for `linux/arm64`.
  Docker is what delivers portability — the identical image runs on the Pi today and on a
  VPS later with no code change.
- **Ingress reachability:** Cloudflare Tunnel. The Pi is behind residential NAT and has no
  public address. The tunnel is outbound-only, so no ports are forwarded and no dynamic DNS
  is needed, and it provides a stable HTTPS hostname for the GitHub webhook. Moving off the
  Pi changes only where the tunnel daemon runs.
- **Persistence:** SQLite file on a mounted volume, so container replacement does not lose
  the WAL.
- **Secrets:** environment variables — `GITHUB_WEBHOOK_SECRET`, `DISCORD_WEBHOOK_URL`.
  Never committed.

## Configuration

Adapters and handlers are wired explicitly in TypeScript, not discovered from config. This
is a deliberate rejection of a plugin system: with two adapters, explicit wiring is clearer,
type-checked, and greppable.

```ts
const broker = new InMemoryBroker({ wal: './switchboard.db' })
broker.attach(new DiscordEgress({
  webhookUrl: env.DISCORD_WEBHOOK_URL,
  filter: e => e.source === 'github',
  handlers: [prToEng],
}))
await new GitHubIngress({ secret: env.GITHUB_WEBHOOK_SECRET }).start(broker.publish)
```

## Error handling

| Failure | Behavior |
|---|---|
| Invalid HMAC signature | 401, event never created, logged as a security event |
| Malformed payload | 400, logged; no delivery rows created |
| Unknown webhook event type | 200 with no-op — GitHub must not see failures for events we ignore |
| Egress 5xx / network error | Delivery marked `failed`, retried with backoff |
| Egress 429 (v1.1, Discord) | Respect `Retry-After`; egress-level rate limiter throttles all its handlers |
| Handler throws | That delivery row marked `failed`; siblings unaffected |
| 10 failed attempts | Delivery marked `dead`, retained for inspection |
| Process crash | Pending deliveries replayed from WAL on boot |

## Testing

- **Unit:** filter evaluation (egress∧handler conjunction, a failing egress gate short-
  circuiting its handlers), backoff schedule, dedup.
- **Integration:** recorded real GitHub webhook payloads → broker → fake egress, asserting
  correct topics and delivery rows.
- **Durability:** publish, kill the process mid-dispatch, restart, assert pending deliveries
  replay exactly once against a fake egress that records calls.
- **Contract:** a shared adapter test suite every future adapter must pass — the mechanism
  that keeps adapters from drifting into core responsibilities.

Discord and GitHub are faked at the HTTP boundary. No live API calls in tests.

## Future work

Deliberately designed for, not built:

- **Backfill after downtime:** GitHub exposes `GET /repos/{o}/{r}/hooks/{id}/deliveries`
  (last 30 days) and a redeliver-attempt endpoint, so missed events are recoverable via the
  API even though GitHub does not retry automatically. A boot-time reconcile would list
  deliveries since `max(events.at)` and replay the unseen ones. Deliberately not built —
  noted so the option stays open without redesign.
- **Commands** (`intent: 'command'`): Discord → GitHub actions. Requires exactly-once
  semantics rather than at-least-once — a duplicate notification is noise, a duplicate merge
  is damage. `Delivery.replyTo` already reserves the correlation slot.
- **Discord ingress / GitHub egress:** each provider becomes a connector implementing both
  halves. Additive; no refactor of the event type.
- **NATS or Redis broker:** swap `InMemoryBroker` behind the existing interface when
  multi-process fan-out is genuinely needed. Since filters are predicates rather than
  subjects, a remote broker would subscribe coarsely (by `source`, using `kind` as the
  subject) and keep predicate refinement in-process. Nothing about predicate filtering
  blocks that migration.
- **Multi-channel routing:** additional handlers with narrower filters. No core change.

## Risks

| Risk | Mitigation |
|---|---|
| Building a worse NATS feature-by-feature | Keep implementation minimal; swap out rather than grow |
| Pi power loss corrupting the log | SQLite WAL mode; durability test in the suite |
| Residential network unreliability | Retry with backoff; WAL survives disconnection |
| GitHub does not auto-retry failed webhook deliveries | Receiver must ack fast (verify + write, then return 200) so events are captured before any downstream work |
