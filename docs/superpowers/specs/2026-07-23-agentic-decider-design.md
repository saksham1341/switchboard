# Agentic Decider — Design (SSOT)

**Status:** Approved design, co-designed in conversation. **Not yet implemented.** Built later, one layer at a time, so each lands clean.

**What this is:** the single source of truth for how an LLM agent runs *inside* Switchboard — as a role in the four-role substrate, not as a sandboxed process bolted on. Read it top to bottom once for the mental model; after that, jump to the section you need.

---

## 1. The one idea

**The agent's mind is externalized into the substrate.** Every LLM call, every tool call, every result is a real message in the two durable logs. You can watch the agent think on the dashboard, audit it, replay it, shadow it.

This follows from a single reframe:

> **The agent is not a decider that thinks. It is a decider that routes thinking to an actuator, and routes the results back.**

- The **LLM call is an actuator** (`llm`), not code inside the decider.
- The agent's **tools are actuators** — its tool calls become commands; results come back as observations.
- The decider is a **pure, text-blind router**: given an observation and session state, it deterministically emits the next command. It does no I/O and never reads model prose — it looks only at `tool_use` blocks.

Because the non-determinism (the model) lives behind an actuator, its output *arrives as an observation the decider reacts to deterministically*. That is what preserves every substrate property: the agent decider is a decider like any other, replayable and shadowable, with no world access. A sandboxed Claude Code is a black box calling tools in a hidden loop; this is a control loop expressed in two logs.

---

## 2. Role mapping

| four-role primitive | in the agent |
|---|---|
| **Sensor** | `discord.message` — a Discord channel/thread message becomes an observation |
| **Decider** | `AgentDecider` — the router. The *only* agent-specific component |
| **Actuator** | `llm` (the model), `kv` (memory), `web_search`, the reply actuator, … |
| **Tap** | unchanged — the dashboard/logger observe the whole episode |

Everything except `AgentDecider` is a dumb, agent-unaware `command → effect + result` box. The "agent-ness" lives entirely in the decider.

---

## 3. The three stores

Three roles, three scoped `KeyStore`s, and — per the substrate rule — **they never touch; they coordinate only through the log.**

```
┌────────────────────────────┐   ┌────────────────────────────┐   ┌────────────────────────────┐
│      decider/agent          │   │       actuator/llm          │   │       actuator/kv           │
│  orchestration state        │   │  billing safety             │   │  the model's memory         │
│                             │   │                             │   │                             │
│  thread:<tid> → sid         │   │  cost ledger (tokens, $)    │   │  session:<sid>:*  scratchpad │
│  session:<sid>:messages     │   │  [done:<cmd_id> deferred]   │   │  global:*         long-term  │
│  session:<sid>:turn         │   │                             │   │                             │
│  session:<sid>:spent        │   │                             │   │                             │
│  session:<sid>:state        │   │                             │   │                             │
│  session:<sid>:buffer       │   │                             │   │                             │
│  pending:<cmd_id> → {…}      │   │                             │   │                             │
│  turn:<key>:gather          │   │                             │   │                             │
└────────────────────────────┘   └────────────────────────────┘   └────────────────────────────┘
```

- **`decider/agent/`** — the router's bookkeeping. The LLM can neither see nor address any of this.
- **`actuator/llm/`** — the cost ledger (where a *global* spend cap lives). (`done:<cmd_id>` idempotency is designed but deferred — see §11 and §12.)
- **`actuator/kv/`** — the model's memory, a flat keyspace. The memory-model split (per-session vs global) is entirely a decider-side key-prefix decision, because the `kv` actuator only ever sees flat keys.

The agent therefore has a clean **three-tier memory**:

| tier | where | lifetime | model access |
|---|---|---|---|
| conversation | `decider/agent/` `session:<sid>:messages` | the session (TTL) | *is* its context; can't address as memory |
| scratchpad | `actuator/kv/` `session:<sid>:*` | dies with session (TTL) | `scratchpad` tool |
| long-term | `actuator/kv/` `global:*` | permanent | `memory` tool |

---

## 4. Message vocabulary

Everything the agent does is one of these messages. `emitted_by` is stamped by the Bus; causal links (`observation_id`, `command_id`) are the metadata already in the substrate.

**Observations (obs log):**

| name | from | carries |
|---|---|---|
| `discord.message` | sensor/discord | `{thread_id, channel_id, user, content, mentions}` |
| `llm.ok` | actuator/llm | `{stop_reason, content:[blocks], usage}` (has `command_id`) |
| `<tool>.ok` | tool actuator | tool result payload (has `command_id`) |
| `<tool>.error` | tool actuator | `{message}` — a *handled* failure (has `command_id`) |
| `<cmd>.failed` | **the Bus** | dead-letter backstop (has `command_id`) — see §10 |

**Commands (cmd log):**

| name | to | carries |
|---|---|---|
| `llm` | actuator/llm | `{system, messages, tools, model, max_tokens}` |
| `kv` | actuator/kv | `{op, key, value?, ttl?}` |
| `web_search` | actuator/web_search | `{query}` |
| `discord.reply` | reply actuator | `{content, <destination injected by decider>}` |

---

## 5. The turn loop

The decider is a single consumer group over the obs log, so it processes observations **strictly serially** — this is what makes the whole design lock-free.

### 5.1 One correlation mechanism for everything

The `llm` response comes back as `llm.ok`, which is a *result observation* correlated by `command_id` — **exactly like any tool result.** So the decider special-cases nothing. Every command it emits gets one entry:

```
pending:<command_id> → { kind: "llm" | "tool", sid, tool_use_id?, turn_key? }
```

`subscribes()` is coarse (sync, can't hit the store): `name == "discord.message" or command_id is not None`. `decide()` loads `pending:<command_id>`; not found → not ours, ignore. The `kind` tag is the only discriminator between "the model spoke" and "a tool finished."

### 5.2 The dispatcher and handlers

```
decide(obs):
    if obs.name == "discord.message":  on_message(obs)          ; return
    p = pending.pop(obs.command_id)                              # None → not ours, ignore
    (kind=="llm")  → on_response(p.sid, obs)
    (kind=="tool") → on_gather(p, obs)

advance(sid):                          # the SOLE way a session takes a turn
    if turn >= MAX_TURNS or spent > MAX_SPEND:  halt(sid); return   # §9
    turn++
    flush buffer → one combined user turn appended to messages
    tools = configured_tool_specs + [scratchpad, memory]
    emit `llm` {system, messages, tools, …}; pending{kind:llm}; state=busy

on_response(sid, obs):                 # the model spoke
    append assistant msg; spent += cost(obs.usage)
    tool_uses = [tool_use blocks]
    if no tool_uses:  finish(sid); return          # end_turn → text-blind, deliver nothing
    open gather turn_key = {remaining: len, order: [ids]}
    for each block:
        known tool  → emit command; pending{kind:tool, sid, block.id, turn_key}
        unknown     → gather.result[block.id] = error("no such tool")   # §8

on_gather(p, obs):                     # a tool finished
    if p.tool_use_id in gather.results: return       # (redelivery handled by §11, belt anyway)
    gather.results[p.tool_use_id] = outcome(obs)     # ok / error / failed
    gather.remaining--
    if gather.remaining == 0:
        append user(tool_result blocks in gather.order) to messages
        advance(sid)                    # next turn

finish(sid):
    if buffer_has_mention(sid):  advance(sid)         # a mention landed while busy
    else:                        state = idle          # keep non-mention context in buffer
```

The shape that makes it all hold: **the agent is a flat event handler, not a call stack.** Nothing "awaits a tool." The decider emits and returns; the result arrives later as a fresh observation and re-enters `decide()`.

### 5.3 The gather

One `llm.ok` can carry several `tool_use` blocks (parallel calls), and Anthropic requires the next message to contain a `tool_result` for **all** of them, together. So fan-out opens a bucket keyed by turn; each result decrements `remaining`; at zero, the tool_results assemble (in original order) and the next `llm` command fires. **No locks** — the serial consumer means two results never process simultaneously. The substrate's settle discipline *is* the concurrency control.

---

## 6. Session lifecycle

### 6.1 Identity — free from the substrate

`session_id = the id of the discord.message observation that started it.` mamamia hands every observation a unique monotonic id, so the first message *is* the session's name — no minting mechanism. The thread mapping is **learned from the reply's result**: the reply actuator creates/uses a thread and returns its id in `discord.reply.ok`; the decider records `thread:<thread_id> → sid`. From then on every message in that thread routes to the session.

### 6.2 Engagement — mention is the only advance trigger

Every session has a **buffer** of pending input. Messages the session should hear land there; the agent only *takes a turn* on a mention.

```
on_message(obs):
    is_mention = bot in obs.mentions
    sid = thread_map[thread]
    if sid is None:
        if not is_mention: return                # no session + no mention → ignore entirely
        sid = mint(obs.id); create session
    buffer_append(sid, obs.content, is_mention)
    if is_mention and state == idle:  advance(sid)
    # else buffered — non-mention (context), or mention-while-busy (finish drains it)
```

One rule — *hold input; advance only on a mention* — gives three behaviors:

- **non-mention in a live thread** → buffered as context, agent stays silent
- **mention while idle** → flush + advance
- **mention while busy** → buffered, drained at `finish`

And it fixes consecutive-user-turns for free: `advance` **flushes the whole buffer into one combined user turn**, so ten silent messages + a mention become one user message carrying all eleven. The agent heard the whole thread and answers it as a unit.

### 6.3 State — two states

`idle ↔ busy`. "Waiting on llm" vs "waiting on tools" is not a lifecycle distinction — the `pending`/gather bookkeeping already knows which. `state` is explicit, not derived, so nothing scans.

### 6.4 Expiry

- **Idle sessions TTL out.** Each turn already re-writes `session:<sid>:messages`; add `ttl=IDLE_TTL` there and on the thread map. No activity → the session evaporates; the next message mints fresh.
- **Scratchpad** — the decider injects `ttl=IDLE_TTL` on `scratchpad` kv commands. **Long-term memory** — no ttl.
- **`/reset`** — deletes the thread map; next message starts clean.
- **Stuck-busy watchdog** (v1) — a `ctx.schedule` sweep halts any session busy > N minutes. The safety net for a result that never arrives (§12, hole 3).

---

## 7. The actuator contract

### 7.1 `tool_spec` — opt-in to being a tool

```python
class Actuator(Protocol):
    name: str
    tool_spec: ToolSpec | None = None          # None ⇒ not agent-callable
    def bind(self, ctx): ...
    async def act(self, cmd, ctx): ...

# ToolSpec = {"description": str, "input_schema": <JSON schema>}
```

Tool name **==** actuator name **==** command name — identity mapping, no registry. **Declaring a `tool_spec` is the opt-in**: an actuator with one is a tool the agent may call; without one, unreachable. So `discord.post` (spam a channel) can stay tool-less while `web_search` and the reply actuator opt in. "All available commands" means "all commands that opted in."

### 7.2 Result → tool_result

The decider correlates by `command_id` (not name), so all three outcomes flow through one code path:

| actuator emits | decider produces |
|---|---|
| `result("ok", payload)` | `tool_result{content: json(payload), is_error: false}` |
| `result("error", {message})` | `tool_result{content: message, is_error: true}` |
| Bus `<cmd>.failed` (backstop) | `tool_result{content: "tool died", is_error: true}` |

Convention: **the ok payload *is* the tool content** (json-serialized).

### 7.3 The two decider-private actuators (no `tool_spec`)

**`llm` — a generic executor, not agent-specific.** The decider emits it directly (that's `advance`); the agent never "calls the LLM as a tool." System prompt, model, tools, max_tokens all travel in the command args because the decider owns them. *Any* decider could emit `llm` commands. Cost accounting is the decider reading `usage` off each `llm.ok`.

**`kv` — reached only through decider-injected virtual tools.** The agent sees `scratchpad` and `memory`, never raw `kv`. When it calls one, the decider **rewrites the key and emits a plain `kv` command**:

```
scratchpad {op:set, key:"draft"}  →  kv {op:set, key:"session:<sid>:draft", ttl:IDLE_TTL}
memory     {op:get, key:"prefs"}   →  kv {op:get, key:"global:prefs"}
```

The prefix is a **security boundary, not just wiring**: the decider applies it, not the model, so session A physically cannot name a key that reaches session B's scratchpad. If the model did the namespacing, a prompt-injected agent could cross sessions. This is why the memory tools must be decider-injected rather than actuator-derived — session identity is inherently decider knowledge.

### 7.4 Reply destinations are decider-injected

The `discord.reply` tool takes only `{content}`. It does **not** choose where the message goes — the decider injects the destination (the session's thread) from session state, same pattern as the memory prefix. The agent physically cannot post outside its own conversation. (Impl note: the agent's reply is a channel/thread post, Bot-auth — distinct from the interaction-followup path the current `discord.reply` uses for slash commands.)

### 7.5 The tool list is the security boundary

`app.build()` hands the `AgentDecider` a **curated** list at construction — not "every actuator with a tool_spec, auto-discovered":

```python
AgentDecider(tools=[web_search.tool_spec, reply.tool_spec, …], system=…)
```

The agent's reachable surface = exactly the tools passed + the two memory tools it injects. **What the agent can touch is a config decision, not an emergent one.**

---

## 8. Hallucinated tools

Models sometimes call a tool that wasn't offered. The decider checks each `tool_use` against **the tool set it was handed** (it knows that list; it never inspects cmd-log consumers):

- **known** → emit command, add to gather
- **unknown** → produce an `is_error` tool_result immediately, counted as already-resolved (no command emitted)

Anthropic still requires a result for *every* tool_use id, so the instant error keeps the turn alive. Without it, a hallucination deadlocks the gather.

---

## 9. Safety

Every turn passes through `advance`, so it is the single chokepoint for loop safety:

- **`MAX_TURNS`** — hard stop on the loop. The important one.
- **`MAX_SPEND` per session** — the decider accumulates `usage` cost from each `llm.ok` into `session:<sid>:spent`. Suspenders.
- **Global cost ledger** in `actuator/llm/` — a hard daily ceiling the `llm` actuator refuses past. Belt.
- On breach, `halt(sid)` delivers a plain "I stopped because X" and idles — it does **not** emit another `llm`.
- **Isolation** — memory keys and reply destinations are decider-injected per session; the agent cannot reach another session's memory or post outside its thread.
- **Watchdog** — halts stuck-busy sessions (§6.4).

Because there is exactly one function the loop advances through, there is exactly one gate; nothing can leak past it.

---

## 10. Framework prerequisites

Two additive changes to the core, which the agent depends on. **These are platform work, specced/built before the agent, not part of it.**

### 10.1 `_consume` dedup — one clean place for at-least-once

Every consumer (decider, actuator, tap) runs through `Bus._consume`. Add a guard keyed by `(group_id, msg.id)`:

```
msg = acquire(...)
if already_processed(group_id, msg.id):  settle SUCCESS; continue   # redelivery → skip
handle(view)
mark_processed(group_id, msg.id)          # after handle, before settle
settle SUCCESS
```

This **subsumes every seam** — each "at-least-once" concern was really "the same message redelivered to the same group": the start-message (`agent`, obs.id), a tool result (`agent`, result.id), an `llm` command (`llm_actuator`, cmd.id) all collapse to `(group, msg.id)`. So handlers are written as if delivery were exactly-once.

It is **not** true exactly-once, and no clean place can make it so. Marking after `handle`/before `settle` collapses the *common* case (slow settle, reconnect, lease timeout) to exactly-once. What remains is one window: crash *between* `handle` finishing and `mark` landing → redelivery re-runs the handler. That is genuine at-least-once, unavoidable without transactionally coupling side effects to the offset commit (mamamia doesn't offer it). Marking *before* `handle` would trade double-execution for lost-execution — worse.

Storage: a small per-group processed-set in the Bus's own SQLite, pruned with the logs (an id older than log retention can't be redelivered). **Not** in role KeyStores — it is Bus machinery.

**Distinct from provider dedup:** the GitHub sensor's `github:delivery:<id>` guards against *GitHub* redelivering a webhook, upstream of the log entirely. Different layer, stays, untouched by this.

### 10.2 `.failed` on dead-letter — close the result loop

Invariant: **every command terminates in exactly one result observation carrying its `command_id`.**

- success → actuator emits `<cmd>.ok`
- known failure → actuator emits `<cmd>.error` (fast, rich, the LLM can react)
- crash / unhandled / retry-exhausted → the Bus, on settling a cmd `DEAD`, emits a synthetic `<cmd>.failed` obs with the command_id

Without the backstop, a permanently-failing tool produces no result and the gather hangs forever. ~5 lines in the actuator-consume path, purely additive — existing deciders don't subscribe to `.failed`; no actuator consumes obs, so no loop. A generally useful property beyond the agent: commands that die announce it.

---

## 11. The at-least-once principle

The obs log is at-least-once, so **every handler must be safe to run twice on the same observation.** With §10.1 in place, that safety is provided generically at `_consume` — handlers are written straight, and semantic idempotency (`done:<command_id>`) is added back *only* where a crash-window double is expensive (see §12, hole 1). **Deferred for v1** by decision — we solve it at the framework level first, and add semantic guards later, one layer at a time.

---

## 12. Known holes — consciously accepted for v1

| # | hole | consequence | when to close |
|---|---|---|---|
| 1 | **crash-window double** | crash between `on_response` finishing and `_consume` marking → redelivered `llm.ok` re-emits the tool command. A second `web_search` (wasted) or a second `discord.reply` (**double post**). | `done:<command_id>` on non-idempotent actuators. **Reply first** — it's user-visible. |
| 2 | **unbounded conversation** | `session:messages` grows every turn and rides in each `llm` payload — token cost + cmd-log size climb with length | truncation / summarization pass |
| 3 | **misconfig ≠ dead-letter** | a configured tool with no actuator → command sits unconsumed forever (never retried → never `.failed`); only the watchdog catches it | trusted config; watchdog is the net |
| 4 | **tools re-sent every turn** | minor payload bloat | llm actuator holds defs; decider sends names |

None are architectural. Each is "add a guard later."

---

## 13. Component inventory

| piece | kind | status |
|---|---|---|
| `_consume` `(group, msg.id)` dedup | framework | **prereq** |
| Bus `.failed` on dead-letter | framework | **prereq** |
| `Actuator.tool_spec` | contract | new |
| `llm` actuator (generic executor) | actuator | new |
| `kv` actuator (+ decider virtual memory tools) | actuator | new |
| `web_search` actuator | actuator | new, trivial |
| reply-to-thread actuator/mode | actuator | new |
| `discord.message` sensor (message-content intent) | sensor | new |
| `AgentDecider` — dispatch, buffer, gather, advance, finish, caps, watchdog, memory-prefixing, hallucination check | decider | **the meat** |

---

## 14. Worked trace — "@switchboard search the latest mamamia release and summarize"

The whole episode as it appears in the logs. This is the fastest way to understand the system.

```
OBS#100 discord.message {mentions:[bot], content:"…summarize"}
  on_message: mint sid=100, buffer+=msg(mention), idle → advance
  advance: turn 0→1, flush→1 user turn, emit CMD#1 llm{sys,msgs,tools}
           pending:1={llm,100}, state=busy

CMD#1 → llm actuator → Anthropic → tool_use(web_search, tu_A)
OBS#101 llm.ok{content:[tu_A], usage}  (command_id=1)
  on_response(100): append assistant, spent+=$, fan out
    tu_A ∈ tools → emit CMD#2 web_search; pending:2={tool,100,tu_A,turn:1}
    gather turn:1 = {remaining:1, order:[tu_A]}

CMD#2 → web_search → OBS#102 web_search.ok{results} (command_id=2)
  on_gather: results[tu_A]=ok, remaining→0
    append user(tool_result tu_A), advance → turn→2, emit CMD#3 llm

CMD#3 → llm → tool_use(discord.reply, tu_B, content:"0.2.0: …")
OBS#103 llm.ok
  on_response: fan out tu_B; inject destination from session → emit CMD#4 discord.reply
CMD#4 → reply actuator posts, returns {message_id, thread_id:T}
OBS#104 discord.reply.ok
  on_gather: record thread_map[T]=100, advance → CMD#5 llm

CMD#5 → llm → end_turn, no tools
OBS#105 llm.ok
  on_response: no tool_use → finish; buffer has no mention → state=idle
```

Episode = OBS 100–105, CMD 1–5. The entire reasoning is in the logs — replayable, and it lights up the patch-panel dashboard as it runs (`llm`, `web_search`, `discord.reply` all patching through). That is the property a sandboxed agent cannot give.

---

## 15. Edge behavior

| edge | behavior |
|---|---|
| **parallel tools** | fan out N commands, gather remaining=N, results any order, serial consumer → no race, fire at 0 |
| **mention while busy** | buffered with mention flag; `finish` sees it → advance, not idle |
| **non-mention context** | buffered, no advance; flushed into the next combined user turn |
| **tool error** | actuator `result("error")` → `<tool>.error` → gather resolves is_error → the LLM sees it and adapts |
| **tool crash** | dead-letter → Bus `<tool>.failed` → gather resolves is_error; never hangs |
| **hallucinated tool** | not in configured set → instant is_error result, no command; never hangs |
| **cap breach** | `advance` gate → `halt` (reply "step limit"), idle, no llm |
| **memory isolation** | `scratchpad` → `session:<sid>:*`; another session can't address it |
| **stuck busy** | watchdog halts sessions busy > N minutes |

---

## 16. Why this shape

- **The decider is deterministic** → the agent is replayable (stub `llm` with recorded responses) and shadowable (a new prompt against live traffic, emitting to a shadow log a tap diffs). No other agent architecture gives you this, because none separate the routing (deterministic) from the thinking (behind an actuator).
- **Actuators are agent-unaware** → the same `web_search` serves the agent, a plain decider, or a future one, with no coupling. The tool set *is* the actuator set.
- **Everything is in the log** → audit, replay, live visualization, and cost accounting all fall out of the substrate rather than being bolted on.
- **No core contamination** → the whole agent is additive: a decider, three actuators, one sensor, one protocol field (`tool_spec`), and two framework prereqs that stand on their own merits. The four-role split was built for exactly this, and this is the proof.
