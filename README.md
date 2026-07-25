# Switchboard

Event-driven agent host over [mamamia](https://github.com/yellowpages-ink/mamamia).
Relays GitHub repository activity into Discord, serves Discord slash commands,
and — when an LLM key is present — runs a conversational agent that reaches the
world only through actuators.

Internals are a **sense → decide → act** pipeline over two durable logs
(`obs`, `cmd`): **Sensor** turns the outside world into observations, **Decider**
turns observations into commands, **Actuator** executes commands against the
world and reports a result observation, **Tap** reads a log and records.

A Decider has **no world access** — it cannot make an HTTP call, read a clock, or
touch another component's store. Everything it wants done, it asks for by
emitting a command. That constraint is what makes the whole thing replayable and
testable, and it is why the agent is a flat event handler rather than a call
stack: nothing awaits, and every result re-enters the decider as a new
observation.

## Components

| role | what runs |
|---|---|
| Sensors | `github` (webhooks), `discord` (gateway + slash commands), `clock` (`clock.tick`), `deadletter` |
| Deciders | `agent` (the conversational agent), `github-notify`, `ping`, `echo` |
| Actuators | `llm`, `kv`, `discord.post`, `discord.history`, `discord.react`, `discord.reply_to_command` |
| Taps | `logger`, `dashboard` |

## Design

Start with the four-roles restructure, then the agent:

- [four-roles restructure](docs/superpowers/specs/2026-07-22-four-roles-restructure-design.md) — the sensor/decider/actuator/tap split
- [agentic decider](docs/superpowers/specs/2026-07-23-agentic-decider-design.md) — the agent, its turn loop, memory, and security holes
- [original design](docs/superpowers/specs/2026-07-21-switchboard-design.md) — the bus itself

Other specs live alongside them in `docs/superpowers/specs/`.

## Scripts

| script | when |
|---|---|
| `./scripts/dev.sh [run\|test\|shell]` | local development |
| `./scripts/deploy.sh` | first deploy on a host (idempotent) |
| `./scripts/update.sh` | redeploy an existing host |
| `./scripts/vendor-mamamia.sh` | rebuild the pinned mamamia wheel into `vendor/` |

## Develop
```bash
./scripts/dev.sh test        # venv + deps if needed, then pytest
./scripts/dev.sh run         # app on 127.0.0.1:8199, data in .devdata/
```
It creates the venv, installs mamamia editable from `../mamamia` (falling back to
the vendored wheel), and always overrides `SB_PORT`/`SB_DATA_DIR` so a dev run
can't collide with a deployment.

`dev.sh run` reads `.env` if present, so a dev run with a real
`DISCORD_BOT_TOKEN` opens a **second live bot session** — stop other instances
first.

## Run (Docker, on a host)
1. Vendor the pinned mamamia wheel — it is not on any index and `vendor/` is
   gitignored, so no `git pull` ever refreshes it:
   ```bash
   ./scripts/vendor-mamamia.sh          # MAMAMIA_DIR=../mamamia by default
   ```
   No mamamia checkout on the host? Build it elsewhere and `scp` the wheel into
   `vendor/`.
2. `cp .env.example .env`, fill it in, `chmod 600 .env`.
   `GITHUB_WEBHOOK_SECRET` is required; Discord, the agent, and the dashboard
   are each optional — see below.
3. `./scripts/deploy.sh` — checks prerequisites, validates `.env`, builds,
   starts, and waits for `/health` before reporting success.
4. Point the GitHub webhook at your hostname's `/webhook/github`, content-type
   `application/json`, same secret, events `pull_request`,
   `pull_request_review`, `issues`, `check_run`.

Afterwards, `./scripts/update.sh` pulls, re-checks the wheel, rebuilds, restarts
and re-verifies health.

### Ingress

This stack does **not** ship a tunnel. It publishes the app on **loopback only**
(`127.0.0.1:$SB_HOST_PORT`, default `8080` — never the LAN) and stops there;
getting traffic to the host is the host's job.

Point whatever you already run — cloudflared, nginx, caddy — at:

```
http://localhost:8080
```

GitHub's webhook then targets that public hostname's `/webhook/github`. Nothing in this
repo needs tunnel credentials.

## Discord (optional)
Leave `DISCORD_BOT_TOKEN` empty to run GitHub-only — no bot, no slash commands,
nothing relayed into Discord. When it is set, `DISCORD_APPLICATION_ID` is
required (the app fails fast at startup without it).

| var | effect |
|---|---|
| `DISCORD_BOT_TOKEN` | wires the gateway bot, the slash commands, and the reply actuator |
| `DISCORD_APPLICATION_ID` | required alongside the token; used for interaction followups |
| `DISCORD_GUILD_ID` | registers slash commands instantly for that guild; unset syncs globally (~1h) |
| `DISCORD_GITHUB_NOTIFY_CHANNEL_ID` | channel the `github-notify` decider posts GitHub events into; unset = no relay |

Slash commands: `/ping`, `/echo`, and `/reset` (clears this channel's agent
conversation). Invite the bot with the `bot` + `applications.commands` scopes.
Channel posts need **Send Messages** in the target channel; interaction replies
do not.

## Agent (optional)

Wired only when **both** an LLM key and Discord are configured. A key alone
would hand the agent tools that reach nothing — and a command no actuator
consumes never fails, it simply hangs, which is the one failure mode with no
error to observe.

| var | effect |
|---|---|
| `SB_LLM_BACKEND` | `anthropic` (native) or `openai` (any OpenAI-compatible endpoint) |
| `SB_LLM_API_KEY` | unset = no agent |
| `SB_LLM_BASE_URL` | required for the `openai` backend; serves Groq, Gemini, Cerebras, OpenRouter, Ollama |
| `SB_LLM_MODEL` | required, sent on every request, never defaulted by a backend |
| `SB_SESSION_TTL_S` | idle life of a conversation (sliding). Raised automatically if set below the watchdog's window |
| `SB_STUCK_MARGIN` | multiplier on the derived stuck-session threshold, never a duration; clamped at 1.0 |

Mention the bot to start a conversation; it is tracked per channel. Its tools
are `discord.post`, `discord.history` and `discord.react`, plus two the decider
injects and rewrites into `kv` commands:

- **`scratchpad`** — working notes for one conversation, drained when that
  conversation ends
- **`memory`** — durable and **global**, shared across every conversation, kept
  until explicitly deleted

The key prefixes on both are applied by the decider, never by the model, so one
conversation cannot name a key belonging to another. Note that `memory` being
global is deliberate and has a security consequence — untrusted input in one
conversation can write something another conversation later reads. See §12 of
the agent spec before pointing this at a guild containing anyone outside your
trust boundary.

## Dashboard (optional)

`SB_DASHBOARD_TOKEN` enables it; without a token there is no dashboard at all
(it fails closed rather than serving an unauthenticated ingest endpoint, and
there is deliberately no default token). The projection is **structure only** —
names, ids, causal links — and never message payloads.

## Tuning

Defaults are sane; these exist so a deployment can move them without a code
change.

| var | default | what it bounds |
|---|---|---|
| `SB_MESSAGE_MAX_RETRIES` | 10 | attempts before a message is dead-lettered |
| `SB_HANDLER_TIMEOUT_S` | 100 | one handler run; the backstop above a backend's own timeout |
| `SB_RETRY_BACKOFF_MAX_S` | 300 | ceiling on one computed backoff delay |
| `SB_RETRY_AFTER_MAX_S` | 120 | ceiling on a delay a handler asks for (`retry-after`) |
| `SB_CONSUMER_WAIT_MS` | 30000 | long-poll park before the acquire loop spins |
| `SB_LEASE_REAPER_INTERVAL_S` | 60 | how often expired leases are reclaimed |
| `SB_DEDUP_TTL_S` | 3600 | lifetime of the at-least-once dedup key |
| `SB_LOG_MAX_MESSAGES` | 100000 | per-log trim limit |
| `SB_LOG_MAX_DEAD` | 500 | DEAD-row trim limit, and the dashboard's dead-letter cap |
| `SB_CLOCK_TICK_S` | 60 | `clock.tick` interval, which drives the session watchdog and expiry |

## Inspect dead letters
```bash
python -m switchboard.cli dead-letters --db ./data/events.db
```
