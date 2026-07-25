# Agent Phase 5 — Recovery, Lifecycle, Memory (Design)

**Status:** Approved design, co-designed in conversation. Not yet implemented.

**Relationship to the SSOT:** `2026-07-23-agentic-decider-design.md` remains the single source of truth for the agent. Much of Phase 5 is *executing* designs already settled there — §6.4 (expiry), §7.3 (memory tools). This document covers only what is **new or changed**, and where it changes the SSOT it says so; the SSOT is updated at the end of the phase, not duplicated here.

---

## 1. Scope — features and recovery, no ceilings

Phase 4 shipped watched-only because it has no automated spend backstop. Phase 5 does **not** change that, deliberately.

**In:**

| | why |
|---|---|
| `clock` sensor + `clock.tick` | the substrate has no clock; a decider must not own one |
| stuck-session watchdog | recovery, not a limit — the only net for §7.5 |
| session TTL + `/reset` | correctness fix (§6.4), and the only thing that shrinks a transcript |
| `scratchpad` / `memory` tools | the feature |
| retry-config naming + `worst_case_retry_seconds` | makes the watchdog threshold derivable instead of guessed |
| dashboard drops its dead-letter poll | makes §10.2's "one poller, many subscribers" true |

**Out, as a category:** `MAX_SPEND`, the global cost ledger, and the transcript cap. All three are *limits*, and the decision is to build capability first and add ceilings when there is evidence about where they belong. `MAX_TURNS` was already removed in Phase 4 for a related reason: it bounded the wrong thing.

**The consequence, stated plainly:** after Phase 5 there is still no automated spend backstop, so the "runs watched, never unattended" constraint from §9 survives this phase unchanged. Phase 5 makes the agent *recover* and *remember*; it does not make it safe to leave alone.

---

## 2. The `clock` sensor

The substrate has no notion of time passing. Anything periodic today either polls inside a sensor (`sensor/deadletter`) or rides `bus.schedule_maintenance` (store purge, dashboard dead-letter poll).

**New sensor `clock`**, emitting `clock.tick` on a `ctx.schedule` interval (`SB_CLOCK_TICK_S`, default 60s):

```python
{"at": 1784930178.42,   # epoch seconds when the tick fired
 "delta": 60.03,        # measured seconds since the previous tick; null on the first
 "seq": 41}             # monotonic counter since sensor start
```

Three deliberate choices:

- **`delta` is measured, not the configured interval.** A blocked event loop or a slow consumer makes the real gap larger. A consumer reasoning "have N seconds passed" must use `delta`; assuming the schedule held is how a watchdog silently under-counts.
- **First tick carries `delta: null`,** not `0.0`. There is no previous tick, and a zero is a number a consumer could act on.
- **Float seconds, not milliseconds.** Everything time-shaped in Switchboard is already float seconds — `Scheduler.every`, `store.set(ttl=)`, `backoff()`, `asyncio.timeout`. Milliseconds here would mean converting at every comparison to buy precision a 60-second tick cannot use.

The sensor knows nothing about sessions, agents, or what anyone does with a tick. It is a clock.

---

## 3. The watchdog — why it lives in the decider, reached by an observation

### 3.1 The ownership problem

Three placements were considered and two are wrong for structural reasons, not taste.

**A watchdog *sensor* cannot work.** Scoped stores mean `sensor/watchdog/` physically cannot read `decider/agent/session:*`. A sensor cannot know a session is stuck, so it cannot emit "session expired" — it does not have the information.

**A decider must not own a clock.** A decider is a pure function of observations. Give it a timer and it can act with no observation, which makes it a spontaneous source of events — sensor-nature in the wrong role.

**`bus.schedule_maintenance` looks right and is not.** Its two existing uses are infrastructure housekeeping that share no state with a consumer: `store.purge` touches sqlite internals, `dash.refresh_dead` touches dashboard-local state. A watchdog sweep touches **the very records `decide()` mutates**, and it would run *outside* the consume loop. The decider is a single serial consumer group, and §5.3 names that as the concurrency control: *"the substrate's settle discipline is the concurrency control."* A maintenance timer bypasses it — sweep awaits a read, an in-flight `decide()` awaits a write, they interleave, one clobbers the other. Lost update.

### 3.2 The resolution

**The clock is external; the knowledge and the response are the decider's.**

`AgentDecider.subscribes` gains `clock.tick`, and `decide()` routes it to a sweep. The sweep therefore arrives **as an observation through the same consumer group**, serially, under the same settle discipline as every other handler. The race cannot exist by construction.

The session record gains `busy_since: float | null`, set in `_advance`, cleared in `_finish`.

A session busy longer than the threshold is set `idle` **silently** and logged at warning. No reaction, no message. This preserves a property that holds today and is worth keeping deliberately: *everything a user sees came from the model.* The decider has never spoken, and making it a speaker is a line to cross on purpose later, not as a side effect of adding recovery.

### 3.3 The threshold must be derived, not chosen

A watchdog that fires while a message is legitimately retrying **kills live work**. The threshold must therefore sit above the longest a message can honestly stay in flight, and that number is larger and less obvious than it looks:

| path | worst case to DEAD |
|---|---|
| jittered backoff — 13.5 min delay + 11 × 30s handler time | **19.0 min** |
| explicit `retry_after` — 10 × 120s + handler time | **25.5 min** |

Two things people get wrong here, both observed while designing this:

- **The backoff is jittered, not deterministic.** `backoff()` draws uniformly from `[ceiling/2, ceiling]`, so the delay total ranges 6.8–13.5 min. Sampling it once and treating the result as the value is a mistake.
- **Handler time is a term.** `_consume` caps each attempt at `SB_HANDLER_TIMEOUT_S`, and there are `max_retries + 1` of them — 5.5 minutes of pure execution before any waiting.

So the Bus exposes the calculation:

```python
@property
def worst_case_retry_seconds(self) -> float:
    """Longest a message can legitimately stay in flight before DEAD. The two
    retry paths are the jittered backoff ceiling and an explicit retry_after;
    a message takes the worse of them, plus the handler's own time on every
    attempt. Anything watching for a stuck consumer must sit above this or it
    will kill live work."""
    jittered = sum(min(self._retry_backoff_max_s, BACKOFF_BASE * 2 ** i)
                   for i in range(self._message_max_retries))
    explicit = self._message_max_retries * self._retry_after_max_s
    return max(jittered, explicit) + (self._message_max_retries + 1) * self._handler_timeout_s
```

`app.py` wires `AgentDecider(stuck_after=bus.worst_case_retry_seconds * 1.2)` — ~1800s with today's defaults.

**A registry was considered and rejected.** The idea was that components register their backoff policies and something reports the maximum. It cannot be complete — nothing forces registration — so it would report "the max someone remembered to tell me" while *looking* authoritative. False confidence in a safety number is worse than an honest constant. It is also unnecessary: `_consume` is the only retry site in the system, so the set of retry policies is closed at two.

**`stuck_after` is deliberately not an env var.** Exposing it would let a deployment set it below the retry window and turn the watchdog into a killer of live work — the precise failure the derivation prevents. To widen the window, raise the retry knobs and it follows.

---

## 4. Retry configuration — one misplacement, and honest names

### 4.1 `retry_after`'s cap belongs to the Bus

Today the llm backend caps `retry_after` at 120s before raising `RetryableError`. That is the wrong module: the cap is a **Bus policy** — *how long am I willing to defer a message on a handler's say-so* — not a provider detail. The actuator's job is to report what the provider said; the Bus decides whether to honour it.

Moving it makes two things true. `worst_case_retry_seconds` becomes self-contained (no cross-module constant), and the contract becomes honest: **`RetryableError.retry_after` is a request the Bus may clamp**, which it always was. The llm backend gets simpler — parse the header, report it, no policy.

### 4.2 Naming and env

Every knob moves to env, and every name is rewritten to state its job. Convention: `SB_<SUBSYSTEM>_<THING>_<UNIT>`, **unit always in the name** — the current `wait_ms` sitting among six float-seconds values is exactly the confusion this prevents.

| now | env | what it does |
|---|---|---|
| `max_retries` | `SB_MESSAGE_MAX_RETRIES` | redeliveries before mamamia marks a message DEAD |
| `default_timeout_s` | `SB_HANDLER_TIMEOUT_S` | per-attempt cap on `handle()`; the lease is derived at 2× |
| `backoff(cap=)` | `SB_RETRY_BACKOFF_MAX_S` | ceiling on one *computed* backoff delay |
| `_RETRY_CAP` | `SB_RETRY_AFTER_MAX_S` | ceiling on a delay a *handler asks for* |
| `wait_ms` | `SB_CONSUMER_WAIT_MS` | long-poll park before the acquire loop spins |
| `reaper_interval` | `SB_LEASE_REAPER_INTERVAL_S` | how often expired leases are reclaimed |
| `processed_ttl` | `SB_DEDUP_TTL_S` | lifetime of the at-least-once dedup key |
| `max_log_messages` | `SB_LOG_MAX_MESSAGES` | per-log trim limit |
| `max_dead` | `SB_LOG_MAX_DEAD` | DEAD-row trim limit |
| — | `SB_SESSION_TTL_S` | 14400 (4h), sliding |
| — | `SB_CLOCK_TICK_S` | 60 |

`default_timeout_s` → `SB_HANDLER_TIMEOUT_S` is the largest win: "default timeout" for *what* was unanswerable without reading `_consume`.

**Known live consequence, unchanged by this phase:** `SB_HANDLER_TIMEOUT_S` (30s) fires before the llm backend's own `TIMEOUT = 120.0`, so an LLM call slower than 30s is cancelled by the consume loop, not by httpx. The 120 is effectively dead. Renaming makes this visible; deciding what it *should* be is out of scope here.

**`pydantic-settings` is deliberately deferred.** Config roughly doubles in this phase, which is the moment the ad-hoc `os.environ.get` pattern starts to hurt — but adopting it now would inflate Phase 5 with a refactor of every existing variable, orthogonal to the agent work. Deferring is safe **because the names are the durable part**: adopting pydantic later is mechanical if the keys are right, and churning bad names twice is the cost of getting this wrong. The cost of waiting: no validation, so a valid-but-nonsense value like `SB_SESSION_TTL_S=0` (expire instantly) passes silently. Typos crash at `int()`, which is acceptable.

---

## 5. Session lifecycle — TTL and `/reset`

The design is settled in **§6.4** and is not restated here. Phase 5 builds it as written: one record, one **sliding** TTL refreshed on every write (each turn rewrites the record anyway, so `ttl=` is free), with tracking and conversation expiring together — a route map outliving its conversation is what makes the agent answer a thread it was never invited to.

**`SB_SESSION_TTL_S` = 4 hours.** Survives a meal or a meeting; does not span a working day. This value carries more weight than it would have: with the transcript cap out of scope, **TTL is the only automatic bound on a conversation** — idle expiry is the sole mechanism that ever shrinks a transcript.

**`/reset`** is the same delete on a manual trigger: a `CommandSpec` in the Discord sensor produces `discord.command.reset`, which `AgentDecider` subscribes to and handles by deleting `session:<sid>` and `thread:discord:<key>` for the originating channel. It must be the AgentDecider — session state lives in `decider/agent/` and scoped stores make it unreachable to anything else. Acknowledged via `discord.reply_to_command`.

`/reset` is more valuable than it was before the no-limits decision: with no cap, a conversation shrinks only by going idle or by being reset, so it is the escape hatch when a long thread turns confused or expensive.

---

## 6. Memory — `scratchpad` and `memory`

The design is settled in **§7.3**. Two tools the agent sees, injected by the decider, never raw `kv`; the decider rewrites the key and emits a plain `kv` command; the prefix is applied by the decider rather than the model, so a session physically cannot name a key reaching another session's scratchpad.

Phase 5 decisions on top of that:

- **Global, not per-user.** `memory` writes `global:*`, shared across every session and every user. Simplest thing that works. Per-user memory, if wanted, arrives later as an *additional* surface alongside `scratchpad` and `memory` — the same decider-side prefix mechanism, not a redesign.
- **All four `kv` ops exposed** (`get`/`set`/`delete`/`list`). `list` on a global namespace returns everything the bot has ever been told, which is both large and a route for one session to inherit another's junk — `LIST_MAX = 200` already caps the result, and the simplest version ships first.
- **Recall is on demand only.** No preloading, no injected index. The model calls `memory` when it decides to.
- **Scratchpad TTL** — the decider injects `ttl=SB_SESSION_TTL_S` on `scratchpad` commands, so it dies with its session. Long-term memory has no TTL.

**On-demand recall is a falsifiable bet, and worth naming as one.** The risk is that a tool the model must *remember to check* is a tool it mostly will not use — llama-3.3 already under-calls `discord.history` even with a thread hint pushing it. The logs will answer this: if `memory` is never called unprompted, the fix is a hybrid — preload a small index of *keys* so the model can see memory exists and fetch what is relevant. That is additive, not a redesign, and the same shape as the §6.5 thread hint. Ship on-demand, read the logs, decide with evidence.

### 6.1 Persistent injection — recorded, not solved

Every hole in Phase 4 is per-turn: a bad post, a bad read, gone. **Poisoned memory is permanent and global.** A user — or text the agent fetched via `discord.history` and believed — can write `global:*`, and every future session for every user reads it.

This is accepted for the same reason as holes 4 and 4b: the bot lives in one private guild with trusted members, and the risk is real but not *present*. It is recorded as a hole with a trigger, not as a blocker. **Trigger:** before the bot joins a guild containing anyone outside the trust boundary, or before it processes input from a public source. The mitigation shape is already known — per-user namespacing contains the blast radius to whoever planted it, using the same prefix mechanism.

### 6.2 Future direction — replace the actuator, not the protocol

A richer memory (hierarchical, graph-shaped) is anticipated as a follow-up. The decision that keeps it cheap belongs here, before anyone reaches for the wrong seam:

**Put it in a new actuator, not in the `KeyStore` protocol.** `KeyStore` is shared by every role — `sensor/github/`, `decider/agent/`, `tap/logger/` all hold scoped views. Widening it with hierarchical or graph operations changes the contract under all of them, which is a large and unnecessary blast radius. Memory can become a richer *actuator* while `KeyStore` stays a flat `str → str`, and then the only things that move are the decider's rewriting and the actuator itself.

The agent-facing names protect the rest: the model only ever sees `scratchpad` and `memory`. Swap what is underneath and the tool names, the system prompt, and every existing transcript stay valid.

---

## 7. Dashboard drops its dead-letter poll

§10.2 claims the `sensor/deadletter` "lets the dashboard drop its own 5s `message_state` poll — one poller, many subscribers." That never happened. Both run today: the sensor sweeps the DEAD table on a schedule, and the dashboard polls the same table independently through `schedule_maintenance("dashboard-dead", 5.0, dash.refresh_dead)`.

Phase 5 makes the claim true. The dashboard becomes a consumer of `switchboard.deadletter` like the agent, and the maintenance timer is deleted. This also removes the only `schedule_maintenance` use that touches domain state, leaving it to the infrastructure housekeeping it was built for.

---

## 8. Not in scope

| deferred | why |
|---|---|
| `MAX_SPEND`, global cost ledger | limits; build capability first, add ceilings with evidence |
| transcript cap | limit; TTL is the bound for now |
| `pydantic-settings` | orthogonal refactor; names are the durable part and land in this phase |
| per-user memory | additive later, same prefix mechanism |
| hierarchical/graph memory | new actuator, after the agent decider is complete (§6.2) |
| mamamia message timestamps | a real gap (the dashboard's backfill reports `seen_at: None`), but a different repo and the watchdog does not need it |
| the decider speaking | the watchdog is silent; making the decider a speaker is a deliberate decision, not a side effect |
| holes 1/7, 4/4b, 6 | recorded in the SSOT with triggers; unchanged by this phase |
