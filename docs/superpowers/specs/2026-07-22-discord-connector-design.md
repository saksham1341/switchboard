# Discord Connector — Design

**Date:** 2026-07-22
**Status:** Approved for planning
**Owner:** yellowpages.ink
**Extends:** the Switchboard v1 design (2026-07-21). mamamia is unchanged.

## Summary

A **Discord connector** for Switchboard — one component that provides both an
ingress (receive **slash commands** from Discord) and an egress (send messages
**to** Discord). It is the first non-GitHub provider and the first ingress whose
transport is a long-lived connection rather than an HTTP endpoint.

The connector is deliberately thin, like every Switchboard adapter: the ingress
translates a Discord interaction into an `Event` and `publish`es it; a
**downstream Switchboard handler** does the actual work and replies via the
egress. All durability, leasing, retry, and dead-lettering stay in mamamia.

## Goals

- Receive Discord **slash commands** and turn each into a durable event, so the
  processing is leased, retried, and dead-lettered like any other event.
- Send to Discord two ways: an **interaction followup** (reply to the command)
  and a **channel message** (for work that outlives the interaction window, and
  for notifications like GitHub→Discord).
- Keep the adapter a pure translator — no scheduling, retry, or queueing inside
  it. Use `discord.py` for transport + parsing only.
- Prove the whole path end-to-end with one demo command (`/ping`).

## Non-goals (this build)

- **No message-event ingress.** Relaying every channel message is a firehose of
  low-value events; slash commands are intentional and bounded. (`on_message`
  ingest is a possible later addition.)
- **No real command catalogue.** This build ships the machinery plus a `/ping`
  demo. Real commands (e.g. Discord→GitHub actions) are follow-ons — each is a
  new command registration plus a downstream handler.
- **No GitHub→Discord notification handler yet.** It becomes a trivial handler
  on top of the egress (model B) once the egress exists; deferred to keep this
  build focused on the connector.
- **No exactly-once command *processing*.** Delivery is at-least-once as
  everywhere; a command that mutates external state must be idempotent or accept
  a rare duplicate. True exactly-once (transactional outbox) remains mamamia
  future work.
- **No buttons/menus/other interaction types.** Slash commands only.

## Why a bot for ingress, HTTP for egress

Discord — unlike GitHub — does not send outbound webhooks for activity. To
*receive* anything (including slash-command interactions) a bot holds a
persistent **Gateway** WebSocket. So the ingress is a long-lived outbound client,
not an HTTP endpoint. A pleasant consequence: the bot dials *out*, so **the
Discord ingress needs no public endpoint or tunnel** — only GitHub does.

Crucially, **you never *send* over the gateway.** Every message to Discord —
command replies and channel posts — is a plain HTTP call. So the egress is a thin
`httpx` sender, decoupled from the live gateway session. This matters for
durability: an interaction-followup reply works from a stored token even across a
bot reconnect or a Switchboard restart, within Discord's window.

`discord.py` is chosen over a hand-rolled gateway client: it handles
IDENTIFY/heartbeat/resume/intents and the slash-command framework (registration,
option parsing), which is exactly the transport-and-parsing work we do not want
to own. It is wrapped so it stays *inside* the adapter — it never becomes the
application's framework.

## Architecture

```
Discord ──gateway (WS, receive)──▶ DiscordIngress (discord.py bot)
                                        │ interaction: defer(), then publish()
                                        ▼
                                 Broker.publish → mamamia log ("events")
                                        │ acquire_blocking(group=handler)
                                        ▼
                                 downstream Switchboard handler (does the work)
                                        │ ctx.egress.reply(token)  [A, ≤15 min]
                                        │ ctx.egress.send(channel) [B, anytime]
                                        ▼
                        DiscordEgress (httpx) ──HTTP (send)──▶ Discord
```

**One `DiscordConnector` owns the credentials** (bot token, application id) and
exposes both halves. The halves share *config*, not a live socket: the ingress
receives over the WebSocket; the egress sends over HTTP.

### Layer boundaries

- **`discord.py`** owns transport + parsing (gateway, heartbeat, resume, command
  registration, option parsing). Nothing more.
- **The connector** translates: interaction → `Event` on ingress; `Event` +
  handler intent → an HTTP send on egress.
- **Durability, leasing, retry, dead-lettering** stay in mamamia. A command
  handler that reimplements retry is in the wrong layer.

## Ingress — slash commands

A `discord.py` bot (`commands.Bot` / `app_commands`) connects to the gateway and
registers a configured list of slash commands (name + description; simple string
options allowed). Registration is synced to Discord on start — **per-guild**
(`DISCORD_GUILD_ID`) for instant dev iteration, global for prod (global sync
propagates over ~1 h).

Each command handler is thin and identical in shape:

```python
async def _on_command(interaction):
    await interaction.response.defer()          # ack within Discord's 3s window
    await publish(EventInput(
        kind=f"discord.{interaction.guild_id}.command.{name}",
        source="discord",
        payload={
            "command": name,
            "options": {o.name: o.value for o in interaction.data.get("options", [])},
            "user": {"id": interaction.user.id, "name": str(interaction.user)},
            "channel_id": interaction.channel_id,
            "guild_id": interaction.guild_id,
        },
        dedupe_key=str(interaction.id),
        meta={
            "application_id": str(interaction.application_id),
            "interaction_token": interaction.token,   # reply address, 15-min TTL
            "channel_id": str(interaction.channel_id),
        },
    ))
    # return — the real work is a downstream handler
```

It fits the `Ingress` contract: `start(publish)` builds the bot, registers the
commands, and `await bot.start(token)` (serves until stopped, like
`GitHubIngress.start` serves uvicorn); `stop()` → `await bot.close()`.

**Deferring, not replying, in the handler.** `defer()` acks Discord within 3 s
and buys the *downstream* handler up to ~15 min to send the real followup. The
ingress never produces the result — it only records the request.

## Deduplication

The dedupe key is `interaction.id` — Discord's unique id per interaction. This
draws exactly the right line, the same as GitHub's `X-GitHub-Delivery`:

- The user invoking `/ping` **twice** produces **two distinct interactions** →
  two ids → **two events**. Legitimate repeat intent is never suppressed; the
  ingress does not care that the text is identical.
- The **same** interaction **redelivered** (a gateway resume replaying
  `INTERACTION_CREATE`, or Switchboard's own at-least-once) → same id →
  **deduped** by the existing `SeenStore` ordered-publish path.

So dedup suppresses only true transport duplicates, never distinct invocations.

## Egress — two send paths, pure `httpx`

`DiscordEgress.context()` hands each handler a sender exposing both models. Which
one a handler uses is *its* policy (a processing decision), not the connector's:

- **`reply(interaction_token, content, **opts)` — model A (interaction followup).**
  `POST https://discord.com/api/v10/webhooks/{application_id}/{interaction_token}`.
  Needs only the token (no bot auth). Valid **15 minutes** from the interaction.
  The proper slash-command reply. If a command is so slow it dead-letters or the
  window lapses, the followup fails and is logged — the handler may fall back to
  `send`.
- **`send(channel_id, content, **opts)` — model B (channel message).**
  `POST https://discord.com/api/v10/channels/{channel_id}/messages` with the
  bot's `Authorization: Bot <token>` header. Persistent, no window. Used for work
  that outlives the interaction, and for notifications (GitHub→Discord later).

Both take/return small dataclasses (`content`, optional embeds) and are plain
`httpx` calls — no gateway session required. `application_id` + `interaction_token`
come from the event's `meta`; `channel_id` from `meta`/`payload`; the bot token
from the connector's config.

A handler's typical pattern (policy example, not machinery):

```python
async def handle(event, ctx):
    result = do_work(event.payload)                  # may be fast or slow
    m = event.meta
    if within_reply_window(event):                   # ≤15 min since interaction
        await ctx.egress.reply(m["interaction_token"], result)     # A
    else:
        await ctx.egress.send(m["channel_id"], result)             # B
```

`within_reply_window` is derived from the event's ingest time; a handler may also
just try `reply` and fall back to `send` on a window-expired error.

## Data model

An ingested command is an ordinary Switchboard `Event`:

- `kind`: `discord.<guild_id>.command.<name>`
- `source`: `"discord"`
- `payload`: `command`, `options`, `user`, `channel_id`, `guild_id`
- `dedupe_key`: `str(interaction.id)`
- `meta`: `application_id`, `interaction_token`, `channel_id` — the reply address

No new mamamia state; it is a normal message on the `"events"` log, consumed by a
handler group like any other.

**Interaction token in `meta`.** The token grants reply ability for 15 min and is
stored in the durable log. It is short-lived and scoped to replying to that one
interaction; acceptable, and noted as a mild sensitivity (the log is local and
already holds payloads).

## Configuration and secrets

- `DISCORD_BOT_TOKEN` — the bot's token (gateway auth + channel-send REST auth).
- `DISCORD_APPLICATION_ID` — for command registration and followups.
- `DISCORD_GUILD_ID` — dev-only, for instant per-guild command sync.
- **No privileged intents** — slash-command interactions arrive without the
  message-content intent, so none is requested. (Message ingest, if ever added,
  would need it.)

Supplied as env vars from the `.env` file, like the GitHub secret.

## App wiring

`app.py` currently starts the broker and serves one ingress. With two ingresses
(GitHub's uvicorn server *and* the Discord bot), both are long-running
`start(publish)` coroutines, so `run()` starts them **concurrently**
(`asyncio.gather`/tasks) and stops both plus the broker on teardown. The Discord
ingress is wired only when its env vars are present, so a GitHub-only deployment
is unchanged.

## Vertical slice for this build

- The `DiscordConnector` — `DiscordIngress` (bot + `/ping` registered) and
  `DiscordEgress` (both send paths).
- A demo command **`/ping`** and a tiny downstream handler (`discord/ping`) that
  replies **"pong (via the durable path)"** through **model A**, proving
  interaction → defer → publish → durable log → lease → handler → interaction
  followup, end to end.
- App wiring to run the Discord bot alongside GitHub.

Live verification is manual with a real bot token (as the GitHub webhook was
tested live) — unit tests fake Discord at the HTTP boundary.

## Testing

- **Unit:** the interaction→`EventInput` mapping (pure, from a captured
  interaction shape); both egress senders (`reply`, `send`) against a faked
  `httpx` transport, asserting the right URL/headers/body; the `/ping` handler
  logic.
- **Integration:** the demo command through a real `Broker` + fake egress,
  asserting the event is published, processed, and the reply send is invoked with
  the right token — no live Discord.
- **Manual/live:** a real bot in a test guild running `/ping`.
- No live Discord API calls in automated tests.

## Boundary — division of concerns

Applying Switchboard's standing test — *"would a delivery system need this
regardless of yellowpages?"*:

| Concern | Owner |
|---|---|
| Gateway/heartbeat/resume, command registration & parsing | `discord.py` (transport) |
| interaction → Event; Event → HTTP send | Switchboard (the connector) |
| Which reply model to use, command idempotency | the downstream handler (policy) |
| Durable log, leasing, retry, dead-lettering | mamamia |

## Future work

- **Real commands** (Discord→GitHub actions), each a registration + a handler;
  when they mutate state, pair with idempotency and eventually mamamia's
  transactional-outbox primitive for exactly-once.
- **GitHub→Discord notifications** — a handler filtering GitHub events and
  calling `ctx.egress.send(channel_id, ...)` (model B). Trivial once the egress
  exists; the original product goal.
- **Message-event ingress** (`on_message`) if a use case appears — needs the
  privileged message-content intent.
- **Richer messages** — embeds, edits, components — the sender can grow to these.

## Risks

| Risk | Mitigation |
|---|---|
| `discord.py` becomes the framework, eroding the adapter model | Wrap it inside `DiscordIngress`; handlers stay thin (defer → publish); egress is separate `httpx` |
| 15-min interaction window lapses on slow/dead-lettered commands | Provide model B (channel send) as the fallback; log a lapsed reply |
| Interaction token stored in the log | Short-lived, single-interaction scope; the log is local and already holds payloads |
| At-least-once redelivery re-runs a state-mutating command | Command handlers must be idempotent; documented; exactly-once is future work |
| Global command sync propagation delay confuses testing | Dev uses instant per-guild sync (`DISCORD_GUILD_ID`) |
| Two ingresses complicate lifecycle | `run()` supervises both via tasks; Discord wired only when its env vars are set |
