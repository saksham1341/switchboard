# GitHub → Discord Relay — Design

**Goal:** Post GitHub activity (PRs, reviews, issues, CI results) into a Discord channel as rich embeds — the first cross-provider relay in Switchboard: GitHub ingress → durable log → Discord egress → channel.

**Status:** Approved design. Next: implementation plan.

---

## The architectural decision: an egress is a *sink*, not a *route*

Switchboard is an M-sources × N-sinks relay, so there are up to M×N routes (github→discord, linear→discord, sentry→slack, …). We had two ways to place this relay:

- **Route-oriented** — one egress per input→output pair (`GitHubNotifyEgress`). Up to M×N egresses, each near-identical wiring differing only in `(filter, formatter)` — which is exactly what a `Handler` already carries. Every "→discord" relay re-instantiates its own `DiscordSender`, so shared rate-limiting/credentials/sender-upgrades fragment across N copies.
- **Sink-oriented (CHOSEN)** — one egress per *output surface* (`DiscordEgress` = "our Discord bot as an output"). N egresses (a handful of real sinks), each hosting up-to-M handlers. Adding a source is **adding a handler**, not an egress.

**Decision: sink-oriented.** The mental model:

> An **egress is one output binding** — one sink instance: a bot + its connection + credentials, exposed via `context()`. **Handlers are the routes** that write to it, each owning its input `filter` and its formatting. Consumer groups stay namespaced under the sink (`discord/…`).

Rationale: the set of sinks is small and stable; the set of input×purpose routes is large and growing. Put the **unit of extension (Handler)** on the volatile axis and the **unit of resource (Egress/`context`)** on the stable axis. One `DiscordSender` is also the single place to add per-bot rate-limiting later (Discord rate-limits per bot). This mirrors how bots/notifiers are modelled in practice (one bot identity, many behaviors).

`DiscordEgress` therefore hosts every handler that writes to Discord regardless of input source: `ping`, `echo` (Discord-sourced interactions) **and** `notify-github` (GitHub-sourced relay).

**Caveat considered, doesn't flip the decision:** if a sink ever needs multiple bots/tokens (per-tenant), that becomes one egress *per bot instance* — still output-oriented, just parameterized. Per-route policy isolation (a noisy CI relay vs. interaction replies) is already handled: handlers are independent consumer groups with their own `timeout_s`/`lease_s`.

---

## Scope

**Relayed events** (all GitHub kinds the ingress produces, plus one new one):

| Category | Event kinds |
|---|---|
| Pull requests | `pr.opened`, `pr.closed`, `pr.merged`, `review.requested` |
| Reviews | `review.approved`, `review.changes_requested`, `review.commented` |
| Issues | `issue.opened`, `issue.closed` |
| CI | `check_run.failed`, **`check_run.succeeded` (new — ingress extension)** |

**Routing:** a single configured channel (`DISCORD_NOTIFY_CHANNEL_ID`). No per-repo/per-category routing in v1.

**Format:** rich Discord embeds, color-coded per event type.

---

## Components

### 1. Ingress extension — emit CI success

`switchboard/ingress/github.py`, `map_event`: today the `check_run.completed` branch emits `check_run.failed` only for `conclusion == "failure"` and drops everything else. Add a `conclusion == "success"` → `github.{repo}.check_run.succeeded` mapping. All other conclusions (neutral, cancelled, skipped, timed_out, …) stay dropped.

### 2. Sender embed support

`switchboard/egress/discord.py`, `DiscordSender.send` gains an embed path (backward-compatible — existing plain-text `send(channel_id, text)` still works; `send` has no production callers today):

```python
async def send(self, channel_id, content=None, *, embed=None) -> httpx.Response:
    payload = {}
    if content is not None:
        payload["content"] = content
    if embed is not None:
        payload["embeds"] = [embed]
    resp = await self._client.post(
        f"{DISCORD_API}/channels/{channel_id}/messages",
        headers={"Authorization": f"Bot {self._bot_token}"},
        json=payload,
    )
    resp.raise_for_status()
    return resp
```

### 3. Embed formatter (pure)

A new module `switchboard/egress/github_notify.py` exposes a pure function:

```python
def build_embed(kind: str, payload: dict) -> dict | None
```

- Returns a Discord embed dict, or `None` for a kind it doesn't recognize (the handler treats `None` as "acknowledge, don't post").
- Reads GitHub payload fields defensively (`.get` chains) so a shape surprise degrades to `None` rather than raising.

**Embed shape:**

```python
{
  "title": "<emoji> <action>[ #<number>][: <name>]",   # e.g. "🔀 PR #42 opened", "🔴 CI failed: build"
  "url":   "<html_url of the PR / issue / review / check_run>",
  "description": "**<PR or issue title>**",             # omitted where not applicable (e.g. CI)
  "color": <int>,                                        # per table below
  "author": {"name": "<repo full_name> · <actor login>"}
}
```

**Per-kind mapping** (emoji, color, and which payload objects supply title/number/url):

| kind | emoji | color (hex) | title / number / url from | description |
|---|---|---|---|---|
| `pr.opened` | 🔀 | `0x3B82F6` blue | `pull_request` (number, html_url) | PR title |
| `pr.merged` | 🟣 | `0x8B5CF6` purple | `pull_request` | PR title |
| `pr.closed` | 🚫 | `0x6B7280` grey | `pull_request` | PR title |
| `review.requested` | 👀 | `0xEAB308` yellow | `pull_request` | PR title |
| `review.approved` | ✅ | `0x22C55E` green | `review.html_url` / `pull_request.number` | PR title |
| `review.changes_requested` | 🔴 | `0xEF4444` red | `review.html_url` / `pull_request.number` | PR title |
| `review.commented` | 💬 | `0x6B7280` grey | `review.html_url` / `pull_request.number` | PR title |
| `issue.opened` | 📝 | `0x22C55E` green | `issue` (number, html_url) | issue title |
| `issue.closed` | 📕 | `0x6B7280` grey | `issue` | issue title |
| `check_run.failed` | 🔴 | `0xEF4444` red | `check_run` (name, html_url) | check name + branch |
| `check_run.succeeded` | ✅ | `0x22C55E` green | `check_run` | check name + branch |

`author.name` is `f"{repository.full_name} · {sender.login}"` (actor = the webhook `sender`).

### 4. `DiscordEgress` becomes the Discord sink

`switchboard/egress/discord.py`, `DiscordEgress`:

- **Constructor** gains `notify_channel_id: str | None = None`:
  `DiscordEgress(bot_token, application_id, *, notify_channel_id=None, client=None)`.
- **Coarse `filter` relaxes from `e.source == "discord"` to `None`** (no coarse gate; all selection moves to handler filters — the broker skips the coarse check when `filter is None`).
- **Existing handler filters become source-aware** so nothing leaks now that the coarse gate is gone:
  - `ping`: `lambda e: e.source == "discord" and e.payload.get("command") == "ping"`
  - `echo`: `lambda e: e.source == "discord" and e.payload.get("command") == "echo"`
- **New `notify-github` handler**, registered **only when `notify_channel_id` is set**:
  - filter: `lambda e: e.source == "github"`
  - handle `_notify`:
    ```python
    async def _notify(self, event, ctx) -> None:
        embed = build_embed(event.kind, event.payload)
        if embed is None:
            return                       # unrecognized kind: ack without posting
        await ctx.egress.send(self._notify_channel_id, embed=embed)
    ```
- `context()` still returns the one `DiscordSender` — the single Discord output resource shared by all three handlers.

### 5. App wiring

`switchboard/app.py`:

- Read `DISCORD_NOTIFY_CHANNEL_ID` from env into config.
- Pass `notify_channel_id=config.get("discord_notify_channel_id")` when constructing `DiscordEgress` (still only when `discord_bot_token` is set; `application_id` still required then).
- No new egress and no new ingress wiring — the relay rides the existing `DiscordEgress` attach and the existing `GitHubIngress`.

---

## Data flow

```
GitHub webhook ──▶ GitHubIngress.map_event ──▶ EventInput(source="github", kind="github.<repo>.<...>")
     │  (X-GitHub-Delivery dedup at ingest via SeenStore)
     ▼
   Broker.publish ──▶ mamamia durable log ──▶ lease (consumer group "discord/notify-github")
     ▼
DiscordEgress._notify ──▶ build_embed(kind, payload) ──▶ DiscordSender.send(channel, embed=…)
     ▼
   POST /channels/{id}/messages  {"embeds":[…]}   (Authorization: Bot <token>)
```

The existing `ping`/`echo` handlers are unaffected — they consume `source=="discord"` interaction events via their own consumer groups on the same egress.

---

## Error handling & operational notes

- **The bot needs the "Send Messages" permission in the target channel.** Unlike interaction *followups* (which need no auth/permissions), channel posts require it. Our earlier live-test invite used `permissions=0`, so testing this relay requires re-inviting with Send Messages, or granting it on the channel.
- **Send failure → durable retry.** A non-2xx from Discord raises via `raise_for_status`; the handler propagates it, mamamia retries with backoff, and dead-letters after the ceiling. No special handling in the relay.
- **At-least-once → possible duplicate post.** A redelivery after a partial success could post an embed twice. Within spec (no exactly-once). True webhook redeliveries are already collapsed by the `X-GitHub-Delivery` dedup at ingest, so this is only the rare in-broker-retry case.
- **The relay never re-publishes**, so pipeline depth / loop-prevention is not a concern here.

---

## Testing

- **`build_embed` (unit, table-driven):** one case per kind asserting emoji/title/number, `url`, `color`, and `author.name`; plus an unrecognized-kind case → `None`, and a malformed-payload case → `None` (no raise).
- **`DiscordSender.send` embed path (unit):** `httpx.MockTransport` asserts `POST /channels/{id}/messages`, `Authorization: Bot …`, and body `{"embeds":[embed]}`; and that plain-text `send(channel, "x")` still sends `{"content":"x"}`.
- **Ingress (unit):** `check_run.completed` with `conclusion=="success"` → `github.{repo}.check_run.succeeded`; `"failure"` → `.failed` (unchanged); other conclusions → `None`.
- **`DiscordEgress` source-aware filters (unit):** a `source=="github"` event triggers `notify-github` but not `ping`/`echo`; a `source=="discord"` ping triggers `ping` only; `notify-github` is absent when `notify_channel_id` is unset.
- **Integration:** publish a `github.*` event through a real `Broker` + `DiscordEgress(notify_channel_id=…)` wired to a `MockTransport`; assert the channel POST fired with the expected embed. No live Discord.
- **App wiring:** relay handler present iff `discord_notify_channel_id` configured.

---

## Non-goals (deferred)

- **Per-repo / per-category routing.** Single channel for v1; the routing table is an easy follow-on (map kind/repo → channel).
- **Default-branch-only CI filter.** v1 relays `check_run.succeeded`/`.failed` for all branches. If the feed gets noisy, gating successes to the default branch (via `check_run.check_suite.head_branch`) is a small, config-driven addition.
- **Per-bot rate limiting.** The single `DiscordSender` is the place to add it when volume warrants; not built now.
- **Exactly-once delivery.** Out of scope by design (at-least-once).
- **Threads / message updates** (e.g. editing the same message as a PR progresses). v1 posts independent messages.
