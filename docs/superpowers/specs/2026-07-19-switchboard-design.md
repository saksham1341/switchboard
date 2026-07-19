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

- Relay GitHub repo activity into Discord without losing events across restarts or outages.
- Keep the core generic so new providers are new adapters, not core changes.
- Run on a Raspberry Pi today, move to a hosted environment later with no code change.
- Stay small. The abstraction is cheap; the implementation must stay embarrassingly small.

## Non-goals (v1)

- No writes back to GitHub. Read-only relay.
- No Discord slash commands. One-way feed only.
- No multi-channel routing. One channel.
- No external broker (Redis/NATS). In-memory implementation behind an interface.
- No plugin loader, adapter registry, or config-driven adapter discovery. Adapters are
  wired explicitly in code.

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
- **Ordering.** Parallel across handlers, sequential within a handler. Preserves per-channel
  order (`pr.opened` before `pr.merged` in `#eng`) without one slow handler stalling the bus.

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

**Egress:** `DiscordEgress` — one handler, filter `e => e.source === 'github'`, posting to a single channel
via a Discord channel webhook URL. No bot application, no gateway connection, no
interactions endpoint.

**Not in v1:** commands, Discord ingress, GitHub egress, multi-channel routing, slash
commands, identity mapping.

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
| Discord 5xx / network error | Delivery marked `failed`, retried with backoff |
| Discord 429 | Respect `Retry-After`; egress-level rate limiter throttles all handlers |
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
