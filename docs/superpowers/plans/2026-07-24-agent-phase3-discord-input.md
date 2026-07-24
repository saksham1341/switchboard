# Agent Phase 3 — Discord Input Layer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give the agent its input channel — a `discord.message` observation for every human message in a watched guild, carrying the thread hint that tells the agent when unseen context exists, plus a `discord.history` actuator it can call as a tool to fetch that context.

**Architecture:** Both halves extend the existing Discord connector rather than adding a second one. `DiscordSensor` already owns the gateway client, so the message listener goes there behind an opt-in `messages=` flag (it needs the *privileged* message-content intent, which must stay off for deployments that do not want it). `DiscordHistory` is a new actuator alongside `DiscordPost`/`DiscordReply`, sharing `DiscordSender` — it reads over REST, not the gateway, so it needs no intent at all.

**Tech Stack:** Python 3.11+, discord.py (gateway), httpx (REST), pytest with `asyncio_mode = "auto"`.

## Global Constraints

- Spec is `docs/superpowers/specs/2026-07-23-agentic-decider-design.md`. §6.5 governs this phase.
- **No decider work in this phase.** Phase 3 produces observations and a callable tool; `AgentDecider` is Phase 4. Nothing here may reference sessions, buffers, or turns.
- Snowflake ids are **stringified** in every payload — msgpack round-trips ints lossily at Discord's magnitudes. Existing `_command_observation` already does this; match it.
- Actuators report understood failures via `ctx.result("error", {"message": ...})` and **never raise** for them. Raising is reserved for failures worth a retry cycle. Precedent: `switchboard/actuators/kv.py`.
- Never trust the shape of a parsed JSON body. Guard with `isinstance(body, dict)` / `isinstance(body, list)` before `.get()` or iteration — a non-object body must not turn a *reported* failure into a *raised* `AttributeError`. This exact defect was found twice in Phase 2.
- The default fetch size is **50**; Discord's hard ceiling is **100**. Clamp, do not trust the caller.
- `tool_spec` is the opt-in to being agent-callable (§7.1). `discord.history` declares one. Do not add one to anything else.
- Run the suite with `pytest -q` from the repo root. It must stay green.

---

## File Structure

| file | responsibility |
|---|---|
| `switchboard/sensors/discord.py` (modify) | add `_message_observation` + `on_message` listener + `messages=` intent opt-in |
| `switchboard/actuators/discord.py` (modify) | add `DiscordSender.fetch_messages` + `DiscordHistory` actuator |
| `switchboard/app.py` (modify) | wire both behind config |
| `tests/test_sensor_discord.py` (modify) | observation shaping, filtering, intent selection |
| `tests/test_actuators_discord.py` (modify) | history fetch, clamping, ordering, error reporting |
| `tests/test_app.py` (modify) | wiring assertions |

---

### Task 1: `discord.message` sensor

**Files:**
- Modify: `switchboard/sensors/discord.py`
- Modify: `switchboard/app.py`
- Test: `tests/test_sensor_discord.py`

**Interfaces:**
- Consumes: `SensorCtx.emit(name, payload)` — already bound in `bind()`.
- Produces: observation `discord.message` with the payload shape below. Phase 4's `AgentDecider` reads `mentions_bot`, `thread_id`, `channel_id`, `message_id`, `content`, `user_name`, and `thread.message_count`.

Payload contract (every value a `str`, `bool`, `int`, `None`, or list thereof):

```python
{
    "message_id": "1234567890",
    "channel_id": "222",        # where the message IS — the thread id when in a thread
    "parent_id": "111",         # the thread's parent channel, else None
    "thread_id": "222",         # same as channel_id when in a thread, else None
    "guild_id": "9",
    "user_id": "123",
    "user_name": "alice#0001",
    "content": "hey @switchboard thoughts?",
    "mentions": ["555"],        # every mentioned user id
    "mentions_bot": True,       # whether OUR id is among them
    "thread": {"is_thread": True, "message_count": 23},
}
```

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_sensor_discord.py`:

```python
class _FakeAuthor:
    def __init__(self, uid=123, bot=False):
        self.id, self.bot = uid, bot
    def __str__(self): return "alice#0001"


class _FakeThread:
    def __init__(self, cid=222, parent=111, count=23):
        self.id, self.parent_id, self.message_count = cid, parent, count


class _FakeChannel:
    def __init__(self, cid=222):
        self.id = cid


class _FakeGuild:
    id = 9


class _FakeMessage:
    def __init__(self, channel=None, mentions=(), author=None, content="hi", mid=1234567890):
        self.id = mid
        self.channel = channel if channel is not None else _FakeChannel()
        self.guild = _FakeGuild()
        self.author = author or _FakeAuthor()
        self.content = content
        self.mentions = list(mentions)


def test_message_observation_in_thread_carries_the_hint():
    from switchboard.sensors.discord import _message_observation
    bot = _FakeAuthor(uid=555, bot=True)
    name, payload = _message_observation(
        _FakeMessage(channel=_FakeThread(), mentions=[bot],
                     content="hey <@555> thoughts?"), bot_id=555)
    assert name == "discord.message"
    assert payload == {
        "message_id": "1234567890", "channel_id": "222", "parent_id": "111",
        "thread_id": "222", "guild_id": "9", "user_id": "123",
        "user_name": "alice#0001", "content": "hey <@555> thoughts?",
        "mentions": ["555"], "mentions_bot": True,
        "thread": {"is_thread": True, "message_count": 23},
    }


def test_message_observation_in_plain_channel_has_no_thread():
    from switchboard.sensors.discord import _message_observation
    _, payload = _message_observation(_FakeMessage(), bot_id=555)
    assert payload["thread_id"] is None
    assert payload["parent_id"] is None
    assert payload["thread"] == {"is_thread": False, "message_count": None}
    assert payload["mentions"] == [] and payload["mentions_bot"] is False


def test_message_observation_tolerates_a_thread_without_a_count():
    # Discord omits message_count on threads created before it was tracked.
    from switchboard.sensors.discord import _message_observation
    _, payload = _message_observation(
        _FakeMessage(channel=_FakeThread(count=None)), bot_id=555)
    assert payload["thread"] == {"is_thread": True, "message_count": None}


def test_message_observation_outside_a_guild_has_no_guild_id():
    from switchboard.sensors.discord import _message_observation
    msg = _FakeMessage()
    msg.guild = None
    _, payload = _message_observation(msg, bot_id=555)
    assert payload["guild_id"] is None


async def test_on_message_emits_for_a_human():
    emitted = []
    s = _sensor(messages=True)
    s.ctx = type("C", (), {"emit": staticmethod(
        lambda n, p: emitted.append((n, p)) or _done())})()
    await s._on_message(_FakeMessage(), bot_id=555)
    assert emitted and emitted[0][0] == "discord.message"


async def test_on_message_ignores_bots_including_itself():
    emitted = []
    s = _sensor(messages=True)
    s.ctx = type("C", (), {"emit": staticmethod(
        lambda n, p: emitted.append((n, p)) or _done())})()
    await s._on_message(_FakeMessage(author=_FakeAuthor(uid=555, bot=True)), bot_id=555)
    await s._on_message(_FakeMessage(author=_FakeAuthor(uid=777, bot=True)), bot_id=555)
    assert emitted == []


def test_messages_off_keeps_intents_none_and_registers_no_listener():
    s = _sensor()
    assert s._client.intents.value == discord.Intents.none().value
    assert not s.messages


def test_messages_on_requests_exactly_the_needed_intents():
    s = _sensor(messages=True)
    i = s._client.intents
    assert i.guilds and i.guild_messages and i.message_content
    assert not i.members and not i.presences
    assert s.messages
```

Also add this helper near the top of the file (the fake `emit` must be awaitable):

```python
async def _done():
    return 1
```

and widen the existing `_sensor()` helper:

```python
def _sensor(**kw):
    return DiscordSensor("bot", commands=[
        CommandSpec("ping", "Ping"),
        CommandSpec("echo", "Echo", options=(Option("message", "text", type=str, required=True),)),
    ], guild_id="456", **kw)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pytest tests/test_sensor_discord.py -q`
Expected: FAIL — `ImportError: cannot import name '_message_observation'`, and `TypeError: __init__() got an unexpected keyword argument 'messages'`.

- [ ] **Step 3: Implement the shaping function**

Add to `switchboard/sensors/discord.py`, below `_command_observation`:

```python
def _message_observation(message, bot_id: int) -> tuple[str, dict]:
    """Shape a gateway message into a `discord.message` observation.

    `channel_id` is always where the message *is* — the thread id when it is in
    a thread — so replying to it lands in place without the reader knowing which
    it got. The `thread` block is the hint from spec §6.5: a mid-thread mention
    gives the agent a conversation starting at the mention, and it cannot ask
    for context it does not know exists. `message_count` tells it there is more
    above; `discord.history` is how it reads it.
    """
    channel = message.channel
    is_thread = isinstance(channel, discord.Thread)
    mention_ids = [str(u.id) for u in message.mentions]
    return ("discord.message", {
        "message_id": str(message.id),
        "channel_id": str(channel.id),
        "parent_id": str(channel.parent_id) if is_thread else None,
        "thread_id": str(channel.id) if is_thread else None,
        "guild_id": str(message.guild.id) if message.guild else None,
        "user_id": str(message.author.id),
        "user_name": str(message.author),
        "content": message.content,
        "mentions": mention_ids,
        "mentions_bot": str(bot_id) in mention_ids,
        "thread": {"is_thread": is_thread,
                   "message_count": getattr(channel, "message_count", None) if is_thread else None},
    })
```

The test fakes must satisfy `isinstance(channel, discord.Thread)`, so make `_FakeThread` subclass it without running discord.py's `__init__`:

```python
class _FakeThread(discord.Thread):
    def __init__(self, cid=222, parent=111, count=23):
        self.id, self.parent_id, self.message_count = cid, parent, count
```

Subclassing without calling `discord.Thread.__init__` is deliberate — the real
one demands a connection state and a full payload. discord.py uses `__slots__`,
so if that assignment raises `AttributeError`, add `__slots__ = ()` to the fake
or set the attributes via `object.__setattr__`. Do **not** switch the production
check away from `isinstance(channel, discord.Thread)` to work around a test
fake; it is the correct discriminator and duck-typing on `parent_id` would
misclassify future channel types.

- [ ] **Step 4: Implement the intent opt-in and the listener**

In `switchboard/sensors/discord.py`, replace the `__init__` signature and intent construction:

```python
    def __init__(self, bot_token: str, *,
                 commands: list[CommandSpec], guild_id: str | None = None,
                 messages: bool = False):
        self._token = bot_token
        self._guild_id = guild_id
        self.messages = messages
        self.ctx = None
        self._synced = False

        # message_content is a *privileged* intent: the gateway refuses the
        # connection outright unless it is also enabled in the Developer Portal.
        # So it is opt-in — a deployment that only wants slash commands keeps
        # Intents.none() and needs no portal change.
        if messages:
            intents = discord.Intents.none()
            intents.guilds = True
            intents.guild_messages = True
            intents.message_content = True
        else:
            intents = discord.Intents.none()

        self._client = discord.Client(intents=intents)
        self._tree = app_commands.CommandTree(self._client)
        for spec in commands:
            self._tree.add_command(self._make_command(spec))

        @self._client.event
        async def on_ready():
            await self._on_ready()

        if messages:
            @self._client.event
            async def on_message(message):
                await self._on_message(message, bot_id=self._client.user.id)
```

And add the handler as a method:

```python
    async def _on_message(self, message, *, bot_id: int) -> None:
        # Every bot is ignored, ourselves included. Ignoring only ourselves would
        # let two Switchboard-shaped bots talk each other into an endless loop,
        # and the failure would be a live spend, not a test failure.
        if message.author.bot:
            return
        name, payload = _message_observation(message, bot_id)
        await self.ctx.emit(name, payload)
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `pytest tests/test_sensor_discord.py -q`
Expected: PASS, all tests in the file.

- [ ] **Step 6: Wire it in `app.py`**

In `switchboard/app.py`, add to the `config` dict inside `run()`:

```python
        "discord_messages": os.environ.get("DISCORD_MESSAGES", "").lower() in ("1", "true", "yes"),
```

and pass it through in `build()`:

```python
        discord_sensor = DiscordSensor(token, commands=DISCORD_COMMANDS,
                                       guild_id=config.get("discord_guild_id"),
                                       messages=bool(config.get("discord_messages")))
```

Add to `docker-compose.yml`, under the Discord block:

```yaml
      # Gateway message listening. Requires the privileged MESSAGE CONTENT
      # intent to be enabled in the Discord Developer Portal first — the
      # gateway refuses the connection otherwise. Unset = slash commands only.
      DISCORD_MESSAGES: ${DISCORD_MESSAGES:-}
```

- [ ] **Step 7: Add the wiring test**

In `tests/test_app.py`:

```python
def test_discord_messages_is_off_unless_configured(tmp_path):
    from switchboard.app import build
    bus, _ = build({
        "mamamia_db_path": str(tmp_path / "mm.db"),
        "switchboard_db_path": str(tmp_path / "sb.db"),
        "github_secret": "s", "port": 8131,
        "discord_bot_token": "t", "discord_application_id": "1",
    })
    sensor = next(s for s in bus._sensors if s.name == "discord")
    assert sensor.messages is False


def test_discord_messages_opt_in_is_honoured(tmp_path):
    from switchboard.app import build
    bus, _ = build({
        "mamamia_db_path": str(tmp_path / "mm.db"),
        "switchboard_db_path": str(tmp_path / "sb.db"),
        "github_secret": "s", "port": 8132,
        "discord_bot_token": "t", "discord_application_id": "1",
        "discord_messages": True,
    })
    sensor = next(s for s in bus._sensors if s.name == "discord")
    assert sensor.messages is True
```

- [ ] **Step 8: Run the full suite**

Run: `pytest -q`
Expected: PASS, no regressions.

- [ ] **Step 9: Commit**

```bash
git add switchboard/sensors/discord.py switchboard/app.py docker-compose.yml \
        tests/test_sensor_discord.py tests/test_app.py
git commit -m "feat: discord.message sensor with thread hint (opt-in message intent)"
```

---

### Task 2: `discord.history` actuator

**Files:**
- Modify: `switchboard/actuators/discord.py`
- Modify: `switchboard/app.py`
- Test: `tests/test_actuators_discord.py`

**Interfaces:**
- Consumes: `DiscordSender` (existing, in the same file) and `ActCtx.result(name, payload)`.
- Produces: actuator `discord.history` with a `tool_spec`. Phase 4 passes that `tool_spec` into `AgentDecider(tools=[...])`.

Result payloads:

```python
("discord.history.ok",    {"messages": [{"id","user","content","bot"}...],
                           "count": int, "channel_id": str})
("discord.history.error", {"message": "..."})
```

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_actuators_discord.py`:

```python
import httpx
import pytest

from switchboard.actuators.discord import DiscordHistory, HISTORY_DEFAULT, HISTORY_MAX
from switchboard.message import ActCtx, Command


def _hcmd(args):
    class M:
        id = 1
        payload = args
        metadata = {"name": "discord.history", "observation_id": 7}
    return Command.from_message(M())


async def _run_history(act, args):
    results = []
    async def emit_result(name, payload, cmd_id):
        results.append((name, payload)); return 0
    cmd = _hcmd(args)
    await act.act(cmd, ActCtx(cmd=cmd, _emit_result=emit_result))
    return results[0]


def _history_actuator(handler):
    """Bind a DiscordHistory over a mock transport. `handler(request)` returns
    the httpx.Response the Discord API would have."""
    transport = httpx.MockTransport(handler)
    a = DiscordHistory("bot-token", "app-id",
                       client=httpx.AsyncClient(transport=transport))
    a.bind(object())
    return a


def _discord_message(mid, name, content, bot=False):
    return {"id": mid, "author": {"username": name, "bot": bot}, "content": content}


async def test_history_returns_oldest_first():
    # Discord returns newest-first; the agent reads a conversation forwards.
    def handler(request):
        return httpx.Response(200, json=[_discord_message("3", "carol", "third"),
                                         _discord_message("2", "bob", "second"),
                                         _discord_message("1", "alice", "first")])
    name, payload = await _run_history(_history_actuator(handler), {"channel_id": "222"})
    assert name == "discord.history.ok"
    assert [m["content"] for m in payload["messages"]] == ["first", "second", "third"]
    assert payload["count"] == 3
    assert payload["channel_id"] == "222"


async def test_history_defaults_to_fifty_and_passes_before_through():
    seen = {}
    def handler(request):
        seen.update(dict(request.url.params))
        return httpx.Response(200, json=[])
    await _run_history(_history_actuator(handler),
                       {"channel_id": "222", "before": "999"})
    assert seen == {"limit": str(HISTORY_DEFAULT), "before": "999"}


async def test_history_omits_before_when_not_given():
    seen = {}
    def handler(request):
        seen.update(dict(request.url.params))
        return httpx.Response(200, json=[])
    await _run_history(_history_actuator(handler), {"channel_id": "222"})
    assert "before" not in seen


@pytest.mark.parametrize("asked,sent", [(500, HISTORY_MAX), (0, 1), (-5, 1), (10, 10)])
async def test_history_clamps_the_limit(asked, sent):
    seen = {}
    def handler(request):
        seen.update(dict(request.url.params))
        return httpx.Response(200, json=[])
    await _run_history(_history_actuator(handler),
                       {"channel_id": "222", "limit": asked})
    assert seen["limit"] == str(sent)


async def test_history_without_a_channel_id_is_a_reported_error():
    def handler(request):
        raise AssertionError("must not call Discord without a channel_id")
    name, payload = await _run_history(_history_actuator(handler), {})
    assert name == "discord.history.error"
    assert "channel_id" in payload["message"]


async def test_history_rejects_a_non_integer_limit_without_calling_discord():
    def handler(request):
        raise AssertionError("must not call Discord with a bad limit")
    name, payload = await _run_history(_history_actuator(handler),
                                       {"channel_id": "222", "limit": "fifty"})
    assert name == "discord.history.error"
    assert "limit" in payload["message"]


async def test_history_reports_a_permission_failure_rather_than_raising():
    # 403 is permanent: retrying cannot fix it and the agent should be told.
    def handler(request):
        return httpx.Response(403, json={"message": "Missing Access", "code": 50001})
    name, payload = await _run_history(_history_actuator(handler), {"channel_id": "222"})
    assert name == "discord.history.error"
    assert "Missing Access" in payload["message"]


async def test_history_raises_on_a_server_error_so_the_bus_retries():
    def handler(request):
        return httpx.Response(500, text="upstream boom")
    with pytest.raises(Exception):
        await _run_history(_history_actuator(handler), {"channel_id": "222"})


async def test_history_survives_a_body_that_is_not_a_list():
    def handler(request):
        return httpx.Response(200, json={"unexpected": "shape"})
    name, payload = await _run_history(_history_actuator(handler), {"channel_id": "222"})
    assert name == "discord.history.error"


async def test_history_error_body_that_is_not_an_object_still_reports():
    # The Phase 2 defect class: a non-object body must not raise AttributeError.
    def handler(request):
        return httpx.Response(403, json=["nope"])
    name, payload = await _run_history(_history_actuator(handler), {"channel_id": "222"})
    assert name == "discord.history.error"
    assert isinstance(payload["message"], str)


async def test_history_skips_entries_that_are_not_objects():
    def handler(request):
        return httpx.Response(200, json=["junk", _discord_message("1", "alice", "hi")])
    name, payload = await _run_history(_history_actuator(handler), {"channel_id": "222"})
    assert name == "discord.history.ok"
    assert payload["count"] == 1


def test_history_declares_a_tool_spec_with_before_and_limit():
    props = DiscordHistory.tool_spec["input_schema"]["properties"]
    assert set(props) >= {"channel_id", "limit", "before"}
    assert DiscordHistory.tool_spec["input_schema"]["required"] == ["channel_id"]
    assert DiscordHistory.name == "discord.history"
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pytest tests/test_actuators_discord.py -q`
Expected: FAIL — `ImportError: cannot import name 'DiscordHistory'`.

- [ ] **Step 3: Add the fetch path to `DiscordSender`**

In `switchboard/actuators/discord.py`, add a method to `DiscordSender` (after `send`):

```python
    async def fetch_messages(self, channel_id: str, *, limit: int,
                             before: str | None = None) -> httpx.Response:
        """Read a channel's or thread's recent messages. Returns the response
        unraised — the caller decides which statuses are worth a retry, because
        a 403 is a fact to report to the agent while a 502 is worth retrying.

        No gateway intent is involved: message_content gates gateway *events*,
        not the REST API, which needs only Read Message History on the channel.
        """
        params: dict = {"limit": limit}
        if before is not None:
            params["before"] = before
        return await self._client.get(
            f"{DISCORD_API}/channels/{channel_id}/messages",
            headers={"Authorization": f"Bot {self._bot_token}"},
            params=params,
        )
```

- [ ] **Step 4: Implement the actuator**

Append to `switchboard/actuators/discord.py`:

```python
HISTORY_DEFAULT = 50           # enough to recover a mid-thread mention's context
HISTORY_MAX = 100              # Discord's hard ceiling for this endpoint


def _error_message(resp) -> str:
    """Discord's message for a failed call, defensively. Never assume the body
    parsed into an object: a non-dict body must not turn a reported failure
    into a raised AttributeError."""
    try:
        body = resp.json()
    except Exception:
        body = None
    if isinstance(body, dict):
        message = body.get("message")
        if isinstance(message, str) and message:
            return message
    return f"discord returned {resp.status_code}"


class DiscordHistory:
    """Actuator for `discord.history`: read prior messages from a channel or thread.

    This is the answer to spec §6.5 — a mention arriving twenty messages into a
    thread gives the agent a conversation that starts at the mention. Rather than
    hydrating every new session (paying on every `@switchboard what's 2+2`), the
    fetch is a tool the agent calls when the thread hint tells it context exists.

    `before` lets it exclude messages already in its conversation. Nothing
    enforces that — the decider does not rewrite the agent's arguments — the
    schema only makes a clean fetch expressible.
    """

    name = "discord.history"
    tool_spec = {
        "description": (
            "Read earlier messages from a Discord channel or thread, oldest "
            "first. Use this when you are mentioned partway into a thread and "
            "the request refers to something you cannot see."),
        "input_schema": {
            "type": "object",
            "properties": {
                "channel_id": {"type": "string",
                               "description": "The channel or thread to read."},
                "limit": {"type": "integer",
                          "description": f"How many messages, 1-{HISTORY_MAX}. "
                                         f"Defaults to {HISTORY_DEFAULT}."},
                "before": {"type": "string",
                           "description": "Only messages older than this message "
                                          "id. Use it to skip messages you have "
                                          "already seen."},
            },
            "required": ["channel_id"],
        },
    }

    def __init__(self, bot_token, application_id, *, client=None):
        self._token, self._app_id = bot_token, application_id
        self._client = client
        self._sender = None

    def bind(self, ctx):
        self.ctx = ctx
        self._sender = DiscordSender(self._token, self._app_id, client=self._client)

    async def act(self, cmd, ctx):
        args = cmd.args or {}
        channel_id = args.get("channel_id")
        if not isinstance(channel_id, str) or not channel_id:
            return await ctx.result("error", {"message": "channel_id is required"})

        limit = args.get("limit", HISTORY_DEFAULT)
        # bool is an int subclass, and `True` as a limit is a caller bug, not a 1.
        if isinstance(limit, bool) or not isinstance(limit, int):
            return await ctx.result("error", {"message": "limit must be an integer"})
        limit = max(1, min(HISTORY_MAX, limit))

        before = args.get("before")
        if before is not None and not isinstance(before, str):
            return await ctx.result("error", {"message": "before must be a message id string"})

        resp = await self._sender.fetch_messages(channel_id, limit=limit, before=before)
        if resp.status_code >= 500:
            # Transient: raise so the bus retries with backoff rather than
            # telling the agent something permanent went wrong.
            resp.raise_for_status()
        if resp.status_code >= 400:
            return await ctx.result("error", {"message": _error_message(resp)})

        try:
            body = resp.json()
        except Exception:
            body = None
        if not isinstance(body, list):
            return await ctx.result("error", {"message": "discord returned an unreadable body"})

        messages = []
        for entry in body:
            if not isinstance(entry, dict):
                continue
            author = entry.get("author")
            author = author if isinstance(author, dict) else {}
            messages.append({
                "id": str(entry.get("id")),
                "user": author.get("username"),
                "bot": bool(author.get("bot")),
                "content": entry.get("content"),
            })
        messages.reverse()          # Discord returns newest-first; read it forwards

        await ctx.result("ok", {"channel_id": channel_id,
                                "messages": messages,
                                "count": len(messages)})

    async def close(self):
        if self._sender is not None:
            await self._sender.close()
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `pytest tests/test_actuators_discord.py -q`
Expected: PASS, all tests in the file.

- [ ] **Step 6: Verify the defensive guard is real, not decorative**

Temporarily change `_error_message` so `body.get("message")` runs without the
`isinstance(body, dict)` check, then run:

Run: `pytest tests/test_actuators_discord.py::test_history_error_body_that_is_not_an_object_still_reports -q`
Expected: FAIL with `AttributeError: 'list' object has no attribute 'get'`.

Restore the guard and re-run:

Run: `pytest tests/test_actuators_discord.py -q`
Expected: PASS.

- [ ] **Step 7: Wire it in `app.py`**

In `switchboard/app.py`, change the import:

```python
from switchboard.actuators.discord import DiscordPost, DiscordReply, DiscordHistory
```

and register it inside the `if config.get("discord_bot_token"):` block, next to `DiscordReply`:

```python
        bus.add_actuator(DiscordReply(token, app_id))
        # Registered whenever Discord is wired, not gated on messages=: it reads
        # over REST and needs no intent, and Phase 4 hands its tool_spec to the
        # agent. Idle until something emits the command.
        bus.add_actuator(DiscordHistory(token, app_id))
```

- [ ] **Step 8: Add the wiring test**

In `tests/test_app.py`:

```python
def test_discord_history_actuator_is_wired_with_discord(tmp_path):
    from switchboard.app import build
    bus, _ = build({
        "mamamia_db_path": str(tmp_path / "mm.db"),
        "switchboard_db_path": str(tmp_path / "sb.db"),
        "github_secret": "s", "port": 8133,
        "discord_bot_token": "t", "discord_application_id": "1",
    })
    assert "discord.history" in {a.name for a in bus._actuators}


def test_no_discord_means_no_history_actuator(tmp_path):
    from switchboard.app import build
    bus, _ = build({
        "mamamia_db_path": str(tmp_path / "mm.db"),
        "switchboard_db_path": str(tmp_path / "sb.db"),
        "github_secret": "s", "port": 8134,
    })
    assert "discord.history" not in {a.name for a in bus._actuators}
```

- [ ] **Step 9: Run the full suite**

Run: `pytest -q`
Expected: PASS, no regressions.

- [ ] **Step 10: Commit**

```bash
git add switchboard/actuators/discord.py switchboard/app.py \
        tests/test_actuators_discord.py tests/test_app.py
git commit -m "feat: discord.history actuator as an agent tool"
```

---

## Not in scope

| deferred | why |
|---|---|
| `AgentDecider` reading these observations | Phase 4. Phase 3 deliberately produces observations nothing consumes yet — same shape as `kv` shipping before its decider. |
| Message-count cap on conversations | Spec §12 hole 2 — a post-production fix once real thread shapes show what the limit should be. |
| Channel-name masking for `channel_id` | Spec §7.4 / §12 hole 4, with a recorded trigger. |
| Thread *creation* by the agent | Phase 4+; `discord.post` already reaches an existing thread by id. |
| DM support | Guild-only for v1. `message.guild is None` is shaped correctly but nothing routes it. |
| Attachments, embeds, reactions in history | Text is what the agent reads. Additive later. |

## Operator note

Before the first live test with `DISCORD_MESSAGES=1`, the **MESSAGE CONTENT** privileged intent must be enabled in the Discord Developer Portal (Applications → Bot → Privileged Gateway Intents). Without it the gateway rejects the connection at login with `PrivilegedIntentsRequired` — the sensor will not start at all, and the failure is a hard crash rather than a silent degradation.
