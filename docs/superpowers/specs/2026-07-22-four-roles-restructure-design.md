# Four-Role Restructure — Design

**Goal:** Move Switchboard's internals to a **sense → decide → act** model over two durable logs, preserving all current behavior. Reveal the *decider* as a first-class role (today it's hidden inside egress handlers), so a future agent has an obvious, non-invasive home — as a decider — without ever putting non-determinism in the core.

**Status:** Approved design (co-designed in conversation). Restructure, not a feature add. Behavior-preserving.

---

## The model

```
world ─▶ SENSOR ─▶ [ obs log ] ─▶ DECIDER ─▶ [ cmd log ] ─▶ ACTUATOR ─▶ world
                        ▲                                        │
                        └──────── result observation ◀───────────┘  (always, on success)
   TAP reads any log, effects nothing (trace/observability).
```

Four roles, each a discipline over mamamia consumer-groups/producers — **no new plumbing, mamamia unchanged**:

| role | reads | writes / effects |
|---|---|---|
| **Sensor** | (the outside world) | observations → obs log *(the only world-inbound code; may self-guard duplicates)* |
| **Decider** | obs log | 0+ commands → cmd log *(never touches the world; deterministic today, swappable for an agent later)* |
| **Actuator** | cmd log | a world effect **+ a result observation → obs log** *(unhandled error → mamamia retry/dead-letter)* |
| **Tap** | any log | nothing (records the trace) |

Names chosen to encode the control-loop shape: Sensor senses, Actuator acts, Decider decides, Tap observes. (`Sensor`/`Actuator` replace the old `Ingress`/`Egress`, which named direction rather than function.)

### Why the decider is separate

Today an egress handler both *decides* ("`pr.opened` → this embed") and *acts* (sends it) — decision and execution fused. Splitting them:
- makes the decision layer swappable — a deterministic rule **or**, later, an agent — with the same contract (observation in, command out);
- keeps sensors/actuators dumb and deterministic (pure provider translation);
- concentrates all judgment (authority, "should we", formatting) in one named place.

The agent, when it comes, is simply a **non-deterministic decider** — and because deciders live entirely in the internal event space (never see a provider), it can even run as an **external process** that reads observations and writes commands. The determinism boundary is exactly the decider's edges.

---

## Two logs, not a type field

mamamia is already multi-log (`Message.log_id`, per-log orchestrators). Switchboard currently under-uses it with a single hardcoded `LOG_ID = "events"`. We use **two logs** instead:

- **`"obs"`** — observations (from sensors, and result-observations from actuators)
- **`"cmd"`** — commands (from deciders)

The log a message lives in *is* its type — so there is **no `type` field**; mamamia's native `log_id` is the discriminator. Deciders/taps consume `obs`; actuators consume `cmd`; sensors/actuators produce to `obs`; deciders produce to `cmd`.

Trade-off (accepted): two logs = two per-log id spaces, so there is no single global total-order across both streams — only per-stream order plus causal links. That is fine: the system reasons in *causal* order (via the references below), and we already dropped wall-clock timestamps.

---

## Message schema

A Switchboard event **is** a mamamia message. mamamia already gives identity, ordering, and the delivery machine; we add only a thin header.

```
# obs log
metadata = { "name": "github.acme.pr.opened", "command_id": <int>?,
             "emitted_by": "sensor/github" }        # command_id present ⇒ result-observation
payload  = { …content… }

# cmd log
metadata = { "name": "discord.post", "observation_id": <int>,
             "emitted_by": "decider/github_notify" }
payload  = { …args… }
```

- **`name`** — the dispatch key. For an observation, its class; for a command, the actuator that executes it. (Concrete commands: a command names its one executor. No semantic fan-out.)
- **Directional back-reference** — a command always carries **`observation_id`** (the observation that triggered its decider); an observation *optionally* carries **`command_id`** (present ⇒ it is the result of that command). The field name says which log it points into.
- **`emitted_by`** — `"<kind>/<name>"` of the role that produced the message. **Stamped by the Bus, never passed by a role.** The Bus builds each role's emit callable, so it already knows who is calling; a role therefore cannot claim to be another role, and cannot forget to identify itself.
- **Identity + ordering** = mamamia `msg.id` (int, per log). **Stream** = mamamia `log_id`. Nothing else.

### Why `emitted_by` exists

Attribution was already recoverable for two roles and impossible for the third. An **actuator** is exact — `Actuator.name` *is* the command name it consumes, and a result observation points back through `command_id`. A **sensor** was inferable only by convention, from the provider prefix in an observation name. A **decider** was unrecoverable: nothing recorded it, and `subscribes()` is an arbitrary predicate that cannot be introspected.

So this closes one real hole and upgrades one convention to a guarantee. It is not attribution invented from nothing.

Result-observation of a command `C` = an observation with `command_id = C.msg.id`, `name = "<C.name>.<outcome>"` (e.g. `discord.post.ok`), payload carrying the effect handle (e.g. `{message_id}`).

**Gone as primitives** (vs. today): self-minted ULID `id` (use `msg.id`), `dedupe_key`/dedup, `depth`/chain-cap, `at`/timestamp, the free-form `meta` blob, `source`/`kind` (folded into `name`).

---

## What is deliberately NOT here (deferred, accepted)

- **Deduplication** — no longer a Switchboard primitive. At-least-once means a duplicate observation flows through and may cause a duplicate effect. The GitHub `X-GitHub-Delivery` check **survives as a guard inside the GitHub sensor** (its own `SeenStore`), not in the broker. `broker.publish`-equivalent becomes a plain append.
- **Depth / loop cap** — none. A decider reacting to a result-observation can cascade, unbounded. (No baseline loop: `obs → cmd → result-obs → [no decider subscribes] → stop`.)
- **Command idempotency keys** — none (follows from dropping dedup).
- **The agent** — out of scope. This restructure only *makes room* for it (the decider slot).
- **Semantic commands / fan-out, per-lineage budgets, approval gates, global timeline** — all later.

These are conscious "optimize/guard when it bites" calls, recorded so they are not mistaken for oversights.

---

## Role contracts (interfaces)

```python
# a Sensor produces observations from the world
class Sensor(Protocol):
    name: str
    async def start(self, emit: EmitObservation) -> None: ...   # emit(name, payload) -> obs id
    async def stop(self) -> None: ...

# a Decider turns observations into commands
class Decider(Protocol):
    name: str
    def subscribes(self, obs: Observation) -> bool: ...          # cheap predicate over obs metadata/name
    async def decide(self, obs: Observation, ctx: DecideCtx) -> None: ...   # emits via ctx.command(name, args)

# an Actuator executes one command name and reports a result
class Actuator(Protocol):
    name: str            # == the command name it consumes, e.g. "discord.post"
    def context(self) -> Any: ...                               # e.g. a DiscordSender
    async def act(self, cmd: Command, ctx: ActCtx) -> None: ... # effect + ctx.result(outcome, payload)

# a Tap observes without effect
class Tap(Protocol):
    name: str
    def subscribes(self, msg) -> bool: ...                      # over obs and/or cmd
    async def observe(self, msg) -> None: ...
```

- `Observation` / `Command` are thin read views over a mamamia `Message`: `.id`, `.name`, `.payload`, and `.command_id` / `.observation_id`.
- `DecideCtx.command(name, args)` appends to cmd log with `observation_id = obs.id` set automatically.
- `ActCtx.result(outcome, payload)` appends to obs log with `command_id = cmd.id`, `name = f"{cmd.name}.{outcome}"` — the actuator calls it on success (always). On failure it either handles internally or lets the exception propagate to mamamia (retry/dead-letter).
- A decider is not given world access; an actuator is not given `command()`/publish beyond `result`. These invariants are what keep the roles clean and the decider swappable.

Broker consumer-group ids: `decider/<name>` and `tap/<name>` on the obs log; `actuator/<name>` on the cmd log. Each is an independent mamamia consumer group (leased, retried, dead-lettered) exactly as today.

---

## Behavior preservation — current → new

Every current behavior maps onto the four roles with identical observable output.

**1. GitHub → #releases relay** (`notify-github` today)
- **Sensor** `github` (was `GitHubIngress`): webhook → obs `github.<repo>.pr.opened` etc. Keeps its `SeenStore` (delivery dedup) internally. `check_run.succeeded` mapping preserved.
- **Decider** `github-notify`: subscribes `github.*` → `build_message(...)` → command `discord.post{channel, embed, components}`. (`build_message` — the *decision about what to say* — moves here, unchanged.)
- **Actuator** `discord.post` (part of the Discord connector): `DiscordSender.send(channel, embed=, components=)` → result obs `discord.post.ok{message_id}`.

**2. `/ping`**
- **Sensor** `discord` (was `DiscordIngress` gateway bot): slash command → obs `discord.command.ping` with payload `{interaction_token, channel_id, options, user…}`.
- **Decider** `ping`: subscribes `discord.command.ping` → command `discord.reply{interaction_token, content:"pong (via the durable path)"}`.
- **Actuator** `discord.reply`: `DiscordSender.reply(interaction_token, content)` → result obs `discord.reply.ok`.

**3. `/echo`**
- Same sensor. **Decider** `echo`: obs `discord.command.echo` → command `discord.reply{interaction_token, content: options.message}`. Same `discord.reply` actuator.

**4. Log-all**
- **Tap** `logger` (was `LoggerEgress`): subscribes to everything on both logs → the same structured-JSON line, now covering `obs → cmd → result` (a fuller trace than before).

The Discord slash-command **registration** (the discord.py gateway bot, typed-option `Command`/`Option` specs, guild sync) is unchanged — it lives in the `discord` sensor.

---

## Broker changes (summary)

- Two orchestrators: `get_orchestrator("obs")`, `get_orchestrator("cmd")`.
- `emit_observation(name, payload, command_id=None) -> int` (append to obs log, return id); `emit_command(name, args, observation_id) -> int` (append to cmd log).
- Registration: `add_sensor`, `add_decider`, `add_actuator`, `add_tap` (replacing `attach`).
- Consumer loops: deciders + subscribing taps on obs log; actuators (+ any cmd-taps) on cmd log. Reuse the existing acquire→handle→settle(SUCCESS/RETRY/DEAD) machinery verbatim — only the payload decode (`Observation`/`Command` from `msg`) and the per-role callback differ.
- **Remove:** `SeenStore` from the broker, the `dedupe_key` path, `max_chain_depth`/`ChainTooDeep`. The success/failed/dead hooks stay (observability).
- `app.build()/run()` rewire the same connectors into the four roles; env config unchanged (`GITHUB_WEBHOOK_SECRET`, `DISCORD_*`, `DISCORD_NOTIFY_CHANNEL_ID`).

---

## Testing

- **Behavior invariants stay green** (the point of the restructure): end-to-end tests that a GitHub PR observation produces the Discord channel POST with the right embed+buttons, and that a `/ping`/`/echo` observation produces the right interaction followup — now traced through obs→cmd→result, faked at the httpx boundary.
- **Per-role unit tests:** sensor emits the right observation (github mapping incl. `check_run.succeeded`; discord command → observation with reply address); decider maps observation→command (`github-notify` builds the embed command; `ping`/`echo` build reply commands); actuator executes a command + emits the result observation; tap records both streams.
- **Broker unit tests:** two-log emit/consume; `observation_id`/`command_id` wired automatically; no dedup, no depth cap (drop `test_dedup.py`, the chain-depth test).
- No live Discord/GitHub; `httpx.MockTransport` at the edge.

---

## Non-goals restated

Behavior-preserving restructure only. No agent, no dedup/idempotency, no depth cap, no semantic commands, no new connectors, no new events. The single deliverable is: **the same Switchboard, re-expressed as Sensor · Decider · Actuator · Tap over two logs**, with the decider now a first-class, agent-ready seam.
