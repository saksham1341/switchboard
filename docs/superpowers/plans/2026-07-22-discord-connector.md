# Discord Connector Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a Discord connector to Switchboard — a `discord.py` bot ingress that turns slash commands into durable events, and an `httpx` egress with two send paths (interaction followup + channel message) — proven end-to-end with a `/ping` demo.

**Architecture:** One connector, two halves sharing config not a socket. The ingress receives over the Gateway (WebSocket) and, per interaction, `defer()`s then `publish`es a thin command event carrying the reply address in `meta`; a downstream Switchboard handler does the work and replies via the egress. The egress is pure HTTP (`httpx`): `reply()` (interaction followup, ≤15 min, token-only) and `send()` (channel message, bot-auth, anytime). `discord.py` is transport + parsing only; durability/retry/dead-letter stay in mamamia.

**Tech Stack:** Python 3.12, asyncio, `discord.py>=2.3` (gateway + slash-command framework), `httpx` (already a dep, for Discord REST), existing Switchboard v1 (`Broker`, `Egress`/`Handler`/`Ctx`, `Ingress`, `EventInput`), mamamia `v0.2.0`.

## Global Constraints

- **Python 3.12**, asyncio throughout; every interface boundary is a coroutine.
- **Symmetric package layout:** provider adapters live at `ingress/<provider>.py` and `egress/<provider>.py`. This plan converts the flat `egress.py` into an `egress/` package first.
- **`discord.py` is transport + parsing ONLY.** No scheduling, retry, or queueing in the adapter. Command handlers are thin: `defer()` → `publish()` → return. Real work is a downstream Switchboard handler.
- **The egress is pure HTTP (`httpx`)** — no gateway session required to send. Two paths: interaction followup (`POST /webhooks/{application_id}/{interaction_token}`, no auth, 15-min window) and channel message (`POST /channels/{channel_id}/messages`, header `Authorization: Bot <token>`).
- **Discord API base:** `https://discord.com/api/v10`.
- **Dedupe key is `str(interaction.id)`** — distinct invocations are distinct events; the same interaction redelivered is deduped by the existing `SeenStore`.
- **Command event shape:** `kind = f"discord.{guild_id}.command.{command}"`, `source = "discord"`, `payload = {command, options, user:{id,name}, channel_id, guild_id}`, `dedupe_key = str(interaction_id)`, `meta = {interaction_token, channel_id}` (all strings). The bot's application id is deployment config (held by the egress sender), NOT per-event — it is not in `meta`.
- **No privileged intents** — construct the client with `discord.Intents.none()`; interactions arrive regardless.
- **msgpack-serializable payloads/meta** (strings, lists, string-keyed dicts) — mamamia round-trips them; stringify all Discord snowflake ids.
- **Secrets from env:** `DISCORD_BOT_TOKEN`, `DISCORD_APPLICATION_ID`, `DISCORD_GUILD_ID` (dev). Discord is wired only when `DISCORD_BOT_TOKEN` is set; GitHub-only deployments are unchanged.
- TDD; commit after each green task; no live Discord calls in automated tests (fake at the `httpx` boundary with `httpx.MockTransport`).

**Existing Switchboard API this builds on (verified on the branch):**
```python
from switchboard.event import EventInput, Event          # dataclasses; Event(**msg.payload) round-trips
from switchboard.egress import Handler, Egress, Ctx, LoggerEgress   # Handler(name, filter, handle, timeout_s=None, lease_s=None)
from switchboard.broker import Broker                     # attach(egress), on(hook,fn), publish(ev), start(), stop()
# Egress protocol: attrs name:str, filter:Filter|None, handlers:list[Handler]; method context()->Any
# Ingress protocol: attr name:str; async start(publish), async stop()
# A handler runs as consumer group f"{egress.name}/{handler.name}"; it receives (event, ctx) where ctx.egress = egress.context()
```

---

## File Structure

```
switchboard/
├── egress/                    # was egress.py — converted to a package (Task 1)
│   ├── __init__.py            # Filter, Handler, Egress, Ctx protocols + LoggerEgress re-export
│   ├── logger.py              # LoggerEgress (moved)
│   └── discord.py             # DiscordSender + DiscordEgress + /ping handler (Tasks 2, 4)
├── ingress/
│   ├── github.py              # (unchanged)
│   └── discord.py             # build_command_event() + DiscordIngress (Tasks 3, 5)
└── app.py                     # MODIFY: wire the connector; run ingresses concurrently (Task 7)
tests/
├── test_discord_sender.py     # DiscordSender HTTP paths
├── test_discord_events.py     # build_command_event mapping
├── test_discord_egress.py     # DiscordEgress shape + /ping handler
├── test_discord_ingress.py    # DiscordIngress construction + command registration (no network)
├── test_discord_integration.py# Broker + DiscordEgress: publish a command event -> handler -> followup
└── test_app.py                # MODIFY: build() wires discord when env present
pyproject.toml                 # MODIFY: add discord.py dependency (Task 5)
```

---

## Task 1: Restructure `egress.py` into an `egress/` package

Pure refactor so provider egresses can live at `egress/<provider>.py`, symmetric with `ingress/<provider>.py`. Import-transparent: `from switchboard.egress import Handler, Egress, Ctx, LoggerEgress` keeps working.

**Files:**
- Create: `switchboard/egress/__init__.py`, `switchboard/egress/logger.py`
- Delete: `switchboard/egress.py`
- Test: existing suite must stay green (no test changes)

**Interfaces:**
- Produces (unchanged names, new locations): `Filter`, `Handler`, `Egress`, `Ctx` in `switchboard.egress`; `LoggerEgress` re-exported from `switchboard.egress`.

- [ ] **Step 1: Create `switchboard/egress/__init__.py`**

```python
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Protocol, runtime_checkable

from switchboard.event import Event

Filter = Callable[[Event], bool]
Handle = Callable[[Event, "Ctx"], Awaitable[None]]


@dataclass
class Handler:
    name: str
    filter: Filter
    handle: Handle
    timeout_s: float | None = None
    lease_s: float | None = None


@runtime_checkable
class Egress(Protocol):
    name: str
    filter: Filter | None
    handlers: list[Handler]

    def context(self) -> Any: ...


@dataclass
class Ctx:
    publish: Any
    egress: Any


# Re-exported so `from switchboard.egress import LoggerEgress` still works. MUST
# come after the protocols above — logger.py imports them from this package
# while it is being initialized.
from switchboard.egress.logger import LoggerEgress  # noqa: E402,F401
```

- [ ] **Step 2: Create `switchboard/egress/logger.py`**

```python
import json
import sys

from switchboard.event import Event
from switchboard.egress import Ctx, Filter, Handler


class LoggerEgress:
    """Structured-JSON debug tap — truly log-all (accepts every event, any source)."""

    name = "logger"

    def __init__(self, filter: Filter | None = None, stream=None):
        self.filter = filter or (lambda e: True)
        self._stream = stream or sys.stdout
        self.handlers = [Handler(name="log-all", filter=lambda e: True, handle=self._log)]

    def context(self):
        return None

    async def _log(self, event: Event, ctx: Ctx) -> None:
        self._stream.write(json.dumps({
            "event_id": event.id,
            "kind": event.kind,
            "source": event.source,
            "at": event.at,
            "payload": event.payload,
        }) + "\n")
        self._stream.flush()
```

- [ ] **Step 3: Delete the old flat module**

Run: `git rm switchboard/egress.py`

- [ ] **Step 4: Run the full suite (transparency check)**

Run: `. venv/bin/activate && python -m pytest -q`
Expected: all pass unchanged (imports resolve via the package; `test_egress.py`, `broker.py`, `app.py` are untouched).

- [ ] **Step 5: Commit**

```bash
git add switchboard/egress/ && git rm switchboard/egress.py
git commit -m "refactor(egress): egress.py -> egress/ package (symmetric with ingress/)"
```

---

## Task 2: DiscordSender — the two HTTP send paths

**Files:**
- Create: `switchboard/egress/discord.py` (only `DiscordSender` in this task)
- Test: `tests/test_discord_sender.py`

**Interfaces:**
- Produces:
  - module constant `DISCORD_API = "https://discord.com/api/v10"`
  - `class DiscordSender(bot_token: str, application_id: str, *, client: httpx.AsyncClient | None = None)`
  - `async reply(self, interaction_token: str, content: str) -> httpx.Response` — interaction followup, no bot auth
  - `async send(self, channel_id: str, content: str) -> httpx.Response` — channel message, `Authorization: Bot <token>`
  - `async close(self) -> None`

- [ ] **Step 1: Write the failing test**

`tests/test_discord_sender.py`:
```python
import json
import httpx
import pytest
from switchboard.egress.discord import DiscordSender, DISCORD_API


def _client(handler):
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


async def test_reply_posts_interaction_followup_without_auth():
    seen = {}

    def handler(request):
        seen["method"] = request.method
        seen["url"] = str(request.url)
        seen["auth"] = request.headers.get("authorization")
        seen["body"] = json.loads(request.content)
        return httpx.Response(200, json={})

    s = DiscordSender("bot-tok", "app-123", client=_client(handler))
    await s.reply("int-tok", "pong")
    await s.close()

    assert seen["method"] == "POST"
    assert seen["url"] == f"{DISCORD_API}/webhooks/app-123/int-tok"
    assert seen["auth"] is None                       # followups carry no bot auth
    assert seen["body"] == {"content": "pong"}


async def test_send_posts_channel_message_with_bot_auth():
    seen = {}

    def handler(request):
        seen["url"] = str(request.url)
        seen["auth"] = request.headers.get("authorization")
        seen["body"] = json.loads(request.content)
        return httpx.Response(200, json={})

    s = DiscordSender("bot-tok", "app-123", client=_client(handler))
    await s.send("chan-9", "hello channel")
    await s.close()

    assert seen["url"] == f"{DISCORD_API}/channels/chan-9/messages"
    assert seen["auth"] == "Bot bot-tok"
    assert seen["body"] == {"content": "hello channel"}


async def test_reply_raises_on_error_status():
    def handler(request):
        return httpx.Response(404, json={"message": "Unknown Webhook"})

    s = DiscordSender("bot-tok", "app-123", client=_client(handler))
    with pytest.raises(httpx.HTTPStatusError):
        await s.reply("expired-tok", "too late")
    await s.close()
```

- [ ] **Step 2: Run to verify it fails**

Run: `python -m pytest tests/test_discord_sender.py -v`
Expected: FAIL, `ModuleNotFoundError: No module named 'switchboard.egress.discord'`.

- [ ] **Step 3: Write `switchboard/egress/discord.py`** (sender only)

```python
import httpx

DISCORD_API = "https://discord.com/api/v10"


class DiscordSender:
    """The Discord egress's two send paths, both plain HTTP (no gateway):

    - reply(): an interaction *followup* — POST to the interaction webhook, which
      needs only the application id + interaction token (no bot auth) and is valid
      for 15 minutes. This is how a slash command's result reaches the user.
    - send(): a channel message via the bot REST API (Bot-token auth), with no
      time window — for work that outlives the interaction, and for notifications.
    """

    def __init__(self, bot_token: str, application_id: str, *,
                 client: httpx.AsyncClient | None = None):
        self._bot_token = bot_token
        self._application_id = str(application_id)
        self._client = client or httpx.AsyncClient(timeout=10.0)

    async def reply(self, interaction_token: str, content: str) -> httpx.Response:
        resp = await self._client.post(
            f"{DISCORD_API}/webhooks/{self._application_id}/{interaction_token}",
            json={"content": content},
        )
        resp.raise_for_status()
        return resp

    async def send(self, channel_id: str, content: str) -> httpx.Response:
        resp = await self._client.post(
            f"{DISCORD_API}/channels/{channel_id}/messages",
            headers={"Authorization": f"Bot {self._bot_token}"},
            json={"content": content},
        )
        resp.raise_for_status()
        return resp

    async def close(self) -> None:
        await self._client.aclose()
```

- [ ] **Step 4: Run to verify it passes**

Run: `python -m pytest tests/test_discord_sender.py -v`
Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
git add switchboard/egress/discord.py tests/test_discord_sender.py
git commit -m "feat(discord): DiscordSender — interaction-followup and channel-message HTTP paths"
```

---

## Task 3: Command event mapping

**Files:**
- Create: `switchboard/ingress/discord.py` (only `build_command_event` in this task)
- Test: `tests/test_discord_events.py`

**Interfaces:**
- Produces: `build_command_event(*, command, interaction_id, token, channel_id, guild_id, user_id, user_name, options) -> EventInput` — pure, all primitives.

- [ ] **Step 1: Write the failing test**

`tests/test_discord_events.py`:
```python
from switchboard.ingress.discord import build_command_event


def test_build_command_event_shape():
    ei = build_command_event(
        command="ping",
        interaction_id=42,
        token="int-tok",
        channel_id=7,
        guild_id=9,
        user_id=1,
        user_name="alice#0001",
        options={"target": "prod"},
    )
    assert ei.kind == "discord.9.command.ping"
    assert ei.source == "discord"
    assert ei.dedupe_key == "42"
    assert ei.payload == {
        "command": "ping",
        "options": {"target": "prod"},
        "user": {"id": "1", "name": "alice#0001"},
        "channel_id": "7",
        "guild_id": "9",
    }
    assert ei.meta == {
        "interaction_token": "int-tok",
        "channel_id": "7",
    }


def test_build_command_event_stringifies_ids_for_msgpack():
    ei = build_command_event(
        command="deploy", interaction_id=1, token="t",
        channel_id=3, guild_id=4, user_id=5, user_name="u", options={},
    )
    assert all(isinstance(v, str) for v in ei.meta.values())
    assert isinstance(ei.payload["user"]["id"], str)
```

- [ ] **Step 2: Run to verify it fails**

Run: `python -m pytest tests/test_discord_events.py -v`
Expected: FAIL, module not found.

- [ ] **Step 3: Write `switchboard/ingress/discord.py`** (mapping only)

```python
from switchboard.event import EventInput


def build_command_event(*, command, interaction_id, token,
                        channel_id, guild_id, user_id, user_name, options) -> EventInput:
    """Translate a Discord slash-command interaction into a Switchboard event.

    Ids are stringified because mamamia round-trips payloads through msgpack and
    Discord ids are 64-bit snowflakes. `meta` carries the reply address
    (interaction token + channel) so a downstream handler can reply via the
    egress, even after a restart, within Discord's 15-min window. The bot's
    application id is deployment config on the egress sender, not per-event.
    """
    return EventInput(
        kind=f"discord.{guild_id}.command.{command}",
        source="discord",
        payload={
            "command": command,
            "options": options,
            "user": {"id": str(user_id), "name": user_name},
            "channel_id": str(channel_id),
            "guild_id": str(guild_id),
        },
        dedupe_key=str(interaction_id),
        meta={
            "interaction_token": token,
            "channel_id": str(channel_id),
        },
    )
```

- [ ] **Step 4: Run to verify it passes**

Run: `python -m pytest tests/test_discord_events.py -v`
Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add switchboard/ingress/discord.py tests/test_discord_events.py
git commit -m "feat(discord): pure interaction->EventInput command mapping"
```

---

## Task 4: DiscordEgress + the /ping handler

**Files:**
- Modify: `switchboard/egress/discord.py` (add `DiscordEgress` + `/ping`)
- Test: `tests/test_discord_egress.py`

**Interfaces:**
- Consumes: `DiscordSender` (Task 2, same module); `Handler` from `switchboard.egress`.
- Produces: `class DiscordEgress(bot_token, application_id, *, client=None)` — `name = "discord"`, `filter = lambda e: e.source == "discord"`, `handlers = [Handler("ping", ...)]`; `context() -> DiscordSender`; `async close()`. The `/ping` handler replies `"pong (via the durable path)"` via `ctx.egress.reply(...)` (model A).

- [ ] **Step 1: Write the failing test**

`tests/test_discord_egress.py`:
```python
import asyncio
from switchboard.egress.discord import DiscordEgress
from switchboard.egress import Ctx
from switchboard.event import Event, now_iso


def _cmd_event(command="ping", token="int-tok"):
    return Event(
        id="E1", kind=f"discord.9.command.{command}", source="discord", at=now_iso(),
        payload={"command": command, "options": {}, "user": {"id": "1", "name": "u"},
                 "channel_id": "7", "guild_id": "9"},
        meta={"interaction_token": token, "channel_id": "7"},
    )


class _RecordingSender:
    def __init__(self):
        self.replies = []
        self.sends = []
    async def reply(self, token, content):
        self.replies.append((token, content))
    async def send(self, channel_id, content):
        self.sends.append((channel_id, content))


def test_discord_egress_shape():
    eg = DiscordEgress("bot", "app")
    assert eg.name == "discord"
    assert eg.filter(_cmd_event()) is True
    other = _cmd_event(); object.__setattr__(other, "source", "github")
    assert eg.filter(other) is False                  # non-discord gated out
    assert "ping" in [h.name for h in eg.handlers]


def test_ping_handler_filters_to_ping_only():
    eg = DiscordEgress("bot", "app")
    ping = next(h for h in eg.handlers if h.name == "ping")
    assert ping.filter(_cmd_event(command="ping")) is True
    assert ping.filter(_cmd_event(command="deploy")) is False


def test_ping_handler_replies_via_followup():
    eg = DiscordEgress("bot", "app")
    ping = next(h for h in eg.handlers if h.name == "ping")
    sender = _RecordingSender()
    ctx = Ctx(publish=None, egress=sender)
    asyncio.run(ping.handle(_cmd_event(token="tok-9"), ctx))
    assert sender.replies == [("tok-9", "pong (via the durable path)")]
    assert sender.sends == []
```

- [ ] **Step 2: Run to verify it fails**

Run: `python -m pytest tests/test_discord_egress.py -v`
Expected: FAIL, `ImportError: cannot import name 'DiscordEgress'`.

- [ ] **Step 3: Append `DiscordEgress` to `switchboard/egress/discord.py`**

Add below `DiscordSender` (keep the sender + `DISCORD_API` intact; add the import):
```python
from switchboard.egress import Handler


class DiscordEgress:
    """Egress half of the Discord connector. Its `context()` hands handlers a
    DiscordSender (the two HTTP send paths); this egress also hosts the /ping
    demo handler. Real command handlers are added the same way as scope grows."""

    name = "discord"

    def __init__(self, bot_token: str, application_id: str, *,
                 client: httpx.AsyncClient | None = None):
        self._sender = DiscordSender(bot_token, application_id, client=client)
        self.filter = lambda e: e.source == "discord"      # coarse gate
        self.handlers = [
            Handler(
                name="ping",
                filter=lambda e: e.payload.get("command") == "ping",
                handle=self._ping,
            ),
        ]

    def context(self) -> DiscordSender:
        return self._sender

    async def _ping(self, event, ctx) -> None:
        # model A: reply to the interaction via its stored token
        await ctx.egress.reply(event.meta["interaction_token"], "pong (via the durable path)")

    async def close(self) -> None:
        await self._sender.close()
```

- [ ] **Step 4: Run to verify it passes**

Run: `python -m pytest tests/test_discord_egress.py -v`
Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
git add switchboard/egress/discord.py tests/test_discord_egress.py
git commit -m "feat(discord): DiscordEgress + /ping handler (replies via interaction followup)"
```

---

## Task 5: DiscordIngress — the gateway bot

**Files:**
- Modify: `switchboard/ingress/discord.py` (add `DiscordIngress`)
- Modify: `pyproject.toml` (add `discord.py`)
- Test: `tests/test_discord_ingress.py`

**Interfaces:**
- Consumes: `build_command_event` (Task 3, same module); `discord.py`.
- Produces: `class DiscordIngress(bot_token, *, commands: list[tuple[str, str]], guild_id: str | None = None)` — `name = "discord"`; `async start(publish)`, `async stop()`; commands registered in the tree at construction. (No `application_id` — discord.py derives the app identity from the bot token.)

- [ ] **Step 1: Add `discord.py` to `pyproject.toml`**

In `[project]` `dependencies`, add `"discord.py>=2.3"`. Then:
Run: `. venv/bin/activate && pip install -e ".[dev]"`
Expected: `discord.py` installs.

- [ ] **Step 2: Write the failing test**

`tests/test_discord_ingress.py`:
```python
import inspect
from switchboard.ingress.discord import DiscordIngress


def test_ingress_registers_configured_commands_without_network():
    ing = DiscordIngress(
        "bot-tok",
        commands=[("ping", "Ping Switchboard"), ("status", "Show status")],
        guild_id="456",
    )
    assert ing.name == "discord"
    registered = {c.name for c in ing._tree.get_commands()}
    assert {"ping", "status"} <= registered
    assert inspect.iscoroutinefunction(ing.start)
    assert inspect.iscoroutinefunction(ing.stop)
```

- [ ] **Step 3: Run to verify it fails**

Run: `python -m pytest tests/test_discord_ingress.py -v`
Expected: FAIL, `ImportError: cannot import name 'DiscordIngress'`.

- [ ] **Step 4: Add `DiscordIngress` to `switchboard/ingress/discord.py`**

Append (keep `build_command_event` above):
```python
import discord
from discord import app_commands


class DiscordIngress:
    """Ingress half of the Discord connector: a discord.py bot on the Gateway.
    Registers the configured slash commands; each interaction is deferred (acked
    within Discord's 3s window) and published as a thin command event. The real
    work is a downstream Switchboard handler. discord.py is transport + parsing
    only — no application logic lives here.
    """

    name = "discord"

    def __init__(self, bot_token: str, *,
                 commands: list[tuple[str, str]], guild_id: str | None = None):
        self._token = bot_token
        self._guild_id = guild_id
        self._publish = None
        self._synced = False

        self._client = discord.Client(intents=discord.Intents.none())
        self._tree = app_commands.CommandTree(self._client)
        for name, description in commands:
            self._tree.add_command(self._make_command(name, description))

        @self._client.event
        async def on_ready():
            if self._synced:                              # on_ready can refire on reconnect
                return
            self._synced = True
            if self._guild_id:
                guild = discord.Object(id=int(self._guild_id))
                self._tree.copy_global_to(guild=guild)    # instant per-guild in dev
                await self._tree.sync(guild=guild)
            else:
                await self._tree.sync()                    # global (~1h propagation)

    def _make_command(self, name: str, description: str) -> app_commands.Command:
        # The callback MUST take only `interaction` — discord.py inspects the
        # signature and treats any further parameter as a user-facing slash
        # option (requiring a type annotation). `name` is captured from this
        # method's scope, which is a fresh binding per command, so no closure
        # late-binding bug and no need for a default-arg trick.
        async def callback(interaction: discord.Interaction):
            await interaction.response.defer()             # ack within 3s
            options = {}
            for opt in (interaction.data or {}).get("options", []):
                options[opt.get("name")] = opt.get("value")
            await self._publish(build_command_event(
                command=name,
                interaction_id=interaction.id,
                token=interaction.token,
                channel_id=interaction.channel_id,
                guild_id=interaction.guild_id,
                user_id=interaction.user.id,
                user_name=str(interaction.user),
                options=options,
            ))
            # return — no work here; a downstream handler processes and replies

        return app_commands.Command(name=name, description=description, callback=callback)

    async def start(self, publish) -> None:
        self._publish = publish
        await self._client.start(self._token)             # runs the gateway loop

    async def stop(self) -> None:
        await self._client.close()
```

- [ ] **Step 5: Run to verify it passes**

Run: `python -m pytest tests/test_discord_ingress.py -v`
Expected: 1 passed.

- [ ] **Step 6: Full suite**

Run: `python -m pytest -q`
Expected: all pass.

- [ ] **Step 7: Commit**

```bash
git add switchboard/ingress/discord.py tests/test_discord_ingress.py pyproject.toml
git commit -m "feat(discord): DiscordIngress gateway bot — slash commands defer+publish"
```

---

## Task 6: End-to-end integration (publish → handler → followup)

**Files:**
- Test: `tests/test_discord_integration.py`

**Interfaces:**
- Consumes: `Broker`, `DiscordEgress`, `EventInput`.

- [ ] **Step 1: Write the test**

`tests/test_discord_integration.py`:
```python
import asyncio
import json
import httpx
from switchboard.broker import Broker
from switchboard.egress.discord import DiscordEgress
from switchboard.event import EventInput


async def _wait_for(predicate, timeout=5.0):
    async def loop():
        while not predicate():
            await asyncio.sleep(0.01)
    await asyncio.wait_for(loop(), timeout)


async def test_ping_command_reaches_followup_end_to_end(tmp_path):
    seen = {}

    def handler(request):
        seen["url"] = str(request.url)
        seen["body"] = json.loads(request.content)
        return httpx.Response(200, json={})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    b = Broker(
        mamamia_db_path=str(tmp_path / "events.db"),
        switchboard_db_path=str(tmp_path / "sb.db"),
        wait_ms=50, reaper_interval=3600.0,
    )
    eg = DiscordEgress("bot-tok", "app-123", client=client)
    b.attach(eg)
    await b.start()
    try:
        await b.publish(EventInput(
            kind="discord.9.command.ping", source="discord",
            payload={"command": "ping", "options": {}, "user": {"id": "1", "name": "u"},
                     "channel_id": "7", "guild_id": "9"},
            dedupe_key="interaction-1",
            meta={"interaction_token": "tok-1", "channel_id": "7"},
        ))
        await _wait_for(lambda: "url" in seen, timeout=8)
        assert seen["url"] == "https://discord.com/api/v10/webhooks/app-123/tok-1"
        assert seen["body"] == {"content": "pong (via the durable path)"}
    finally:
        await b.stop()
        await client.aclose()
```

- [ ] **Step 2: Run to verify it passes**

Run: `python -m pytest tests/test_discord_integration.py -v`
Expected: 1 passed. (Proves publish → durable log → lease → /ping handler → interaction-followup HTTP, no live gateway.)

- [ ] **Step 3: Full suite**

Run: `python -m pytest -q`
Expected: all pass.

- [ ] **Step 4: Commit**

```bash
git add tests/test_discord_integration.py
git commit -m "test(discord): /ping command end-to-end through the durable path to a followup"
```

---

## Task 7: App wiring — run GitHub and Discord together

**Files:**
- Modify: `switchboard/app.py`
- Modify: `tests/test_app.py`

**Interfaces:**
- Produces: `build(config) -> tuple[Broker, list]` (broker + a list of ingresses); `run()` starts the broker, runs all ingresses concurrently, and stops all + the broker on teardown. Discord is wired only when `config["discord_bot_token"]` is set.

- [ ] **Step 1: Write the failing test**

Replace `tests/test_app.py` with:
```python
from switchboard.app import build
from switchboard.ingress.github import GitHubIngress
from switchboard.ingress.discord import DiscordIngress


def _base(tmp_path):
    return {
        "mamamia_db_path": str(tmp_path / "e.db"),
        "switchboard_db_path": str(tmp_path / "s.db"),
        "github_secret": "s3cret",
        "max_log_messages": 10_000,
    }


def test_build_github_only(tmp_path):
    broker, ingresses = build(_base(tmp_path))
    assert "logger" in broker._egresses
    kinds = {type(i) for i in ingresses}
    assert GitHubIngress in kinds
    assert DiscordIngress not in kinds
    assert "discord" not in broker._egresses


def test_build_wires_discord_when_configured(tmp_path):
    cfg = _base(tmp_path) | {
        "discord_bot_token": "bot-tok",
        "discord_application_id": "app-123",
        "discord_guild_id": "456",
    }
    broker, ingresses = build(cfg)
    assert "discord" in broker._egresses
    assert any(isinstance(i, DiscordIngress) for i in ingresses)
    assert any(isinstance(i, GitHubIngress) for i in ingresses)
```

- [ ] **Step 2: Run to verify it fails**

Run: `python -m pytest tests/test_app.py -v`
Expected: FAIL (`build` returns a single ingress / no discord wiring).

- [ ] **Step 3: Rewrite `switchboard/app.py`**

```python
import asyncio
import os

from switchboard.broker import Broker
from switchboard.egress import LoggerEgress
from switchboard.egress.discord import DiscordEgress
from switchboard.ingress.github import GitHubIngress
from switchboard.ingress.discord import DiscordIngress

DISCORD_COMMANDS = [("ping", "Ping Switchboard")]


def build(config: dict) -> tuple[Broker, list]:
    broker = Broker(
        mamamia_db_path=config["mamamia_db_path"],
        switchboard_db_path=config["switchboard_db_path"],
        max_log_messages=config.get("max_log_messages", 10_000),
    )
    broker.attach(LoggerEgress())  # truly log-all

    ingresses: list = [
        GitHubIngress(
            secret=config["github_secret"],
            host=config.get("host", "0.0.0.0"),
            port=int(config.get("port", 8080)),
        )
    ]

    if config.get("discord_bot_token"):
        broker.attach(DiscordEgress(
            config["discord_bot_token"], config["discord_application_id"],
        ))
        ingresses.append(DiscordIngress(
            config["discord_bot_token"],
            commands=DISCORD_COMMANDS, guild_id=config.get("discord_guild_id"),
        ))

    return broker, ingresses


async def run() -> None:
    data_dir = os.environ.get("SB_DATA_DIR", "/data")
    config = {
        "mamamia_db_path": os.path.join(data_dir, "events.db"),
        "switchboard_db_path": os.path.join(data_dir, "switchboard.db"),
        "github_secret": os.environ["GITHUB_WEBHOOK_SECRET"],
        "max_log_messages": int(os.environ.get("SB_MAX_LOG_MESSAGES", "10000")),
        "port": int(os.environ.get("SB_PORT", "8080")),
        "discord_bot_token": os.environ.get("DISCORD_BOT_TOKEN"),
        "discord_application_id": os.environ.get("DISCORD_APPLICATION_ID"),
        "discord_guild_id": os.environ.get("DISCORD_GUILD_ID"),
    }
    broker, ingresses = build(config)
    await broker.start()
    try:
        # each ingress owns its transport and serves until cancelled
        await asyncio.gather(*(ing.start(broker.publish) for ing in ingresses))
    finally:
        for ing in ingresses:
            await ing.stop()
        await broker.stop()


if __name__ == "__main__":
    asyncio.run(run())
```

- [ ] **Step 4: Run to verify it passes**

Run: `python -m pytest tests/test_app.py -v`
Expected: 2 passed.

- [ ] **Step 5: Full suite**

Run: `python -m pytest -q`
Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add switchboard/app.py tests/test_app.py
git commit -m "feat(app): wire the Discord connector; run GitHub + Discord ingresses concurrently"
```

---

## Final verification

- [ ] **Full suite**

Run: `. venv/bin/activate && python -m pytest -q`
Expected: all pass.

- [ ] **Manual live check (optional, needs a real bot)** — create a Discord application + bot, invite it to a test guild with the `applications.commands` scope, set `DISCORD_BOT_TOKEN`/`DISCORD_APPLICATION_ID`/`DISCORD_GUILD_ID`, run `python -m switchboard.app`, and run `/ping` in the guild — expect "pong (via the durable path)" plus a `discord.<guild>.command.ping` line from the LoggerEgress and a `processed` row in the log.

---

## Notes for the executor

- **Thin handlers.** The discord.py command callback must only `defer()` → `publish()` → return. No work inline — that would bypass mamamia's durability. The `/ping` *result* comes from the downstream `discord/ping` handler.
- **Stringify all Discord ids** before they enter an event (snowflakes are 64-bit; payloads round-trip through msgpack). `build_command_event` does this — don't bypass it.
- **The egress is HTTP, not the gateway.** `DiscordEgress`/`DiscordSender` never touch the bot/client; sends are `httpx`. Followups need no auth (token only); channel sends need `Authorization: Bot <token>`.
- **`Intents.none()`** — interactions arrive without privileged intents; don't request any.
- **Command sync:** per-guild (`guild_id`) is instant for dev; global sync propagates over ~1h. Tests never sync (no network).
- **Dedupe is `interaction.id`** — distinct invocations are distinct events by design; only true redeliveries dedupe.
- **Task 1 is a pure refactor** — the suite must stay green with zero test changes; if an import breaks, the `egress/__init__.py` re-export ordering is wrong (protocols first, `LoggerEgress` re-export last).
```
