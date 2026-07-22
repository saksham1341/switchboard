# Four-Role Restructure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Re-express Switchboard's internals as **Sensor → Decider → Actuator → Tap** over two mamamia logs (`obs`, `cmd`), preserving all current behavior (GitHub→#releases relay, `/ping`, `/echo`, log-all).

**Architecture:** A new core (`message.py` + `bus.py`) and four role packages (`sensors/`, `deciders/`, `actuators/`, `taps/`) are built **alongside** the current code, each independently unit-tested and green. A final **cutover** wires the new stack into `app.py` and deletes the old core (`broker.py`, `event.py`, `ingress/`, `egress/`, `dedup.py`, `errors.py`). mamamia is unchanged.

**Tech Stack:** Python 3.12, asyncio, mamamia `v0.2.0` (two logs via `log_id`; `append(log, payload, metadata) -> int`), `httpx` (Discord REST, faked with `MockTransport` in tests), `discord.py` (gateway sensor).

## Global Constraints

- **Roles & names:** `Sensor` (world→observation), `Decider` (observation→command, no world access), `Actuator` (command→world effect **+ result observation**), `Tap` (reads a log, effects nothing). These replace Ingress/Egress/handlers.
- **Two logs:** `OBS_LOG = "obs"`, `CMD_LOG = "cmd"`. The log a message is in *is* its type — **no `type` field**.
- **Message schema (mamamia-native):** identity + ordering = `msg.id` (int, per log). Header lives in mamamia `metadata`; content in `payload`:
  - observation: `metadata = {"name": <class>, "command_id": <int>?}` (`command_id` present ⇒ result-observation), `payload = {...}`
  - command: `metadata = {"name": <actuator>, "observation_id": <int>}`, `payload = {...args}`
  - A command always carries `observation_id` (its trigger). A result-observation of command `C` has `command_id = C.id` and `name = f"{C.name}.{outcome}"`.
- **No dedup primitive:** the bus never dedups. The GitHub delivery-id guard lives **inside the GitHub sensor** (its own `SeenStore`).
- **No depth cap:** no `ChainTooDeep`, no chain-depth guard.
- **Behavior preserved:** identical observable output for the relay, `/ping`, `/echo`, log-all.
- **Coexistence:** new files do not import old ones (temporary small duplication of `DiscordSender` etc. is fine); the old stack is deleted only in the final cutover task. TDD; commit per task; no live Discord/GitHub calls in tests.

**mamamia API this builds on (verified):**
```python
storage = registry.get_storage()
msg_id = await storage.append(log_id, payload, metadata={...})   # returns int id, per-log counter
msg = await registry.acquire_blocking(log_id, group_id, instance_id, duration=lease_s, wait_ms=wait_ms)  # msg.id, msg.payload, msg.metadata
registry.notify(log_id)
orch = registry.get_orchestrator(log_id); orch.max_retries = N
await orch.settle(log_id, group_id, msg.id, instance_id, outcome=Outcome.SUCCESS|RETRY|DEAD, retry_after=...)
# Outcome, MessageState from mamamia.core.models; connect/SQLiteStorage/... as in current broker.py
```

---

## File Structure

```
switchboard/
├── message.py            # NEW (Task 1): Observation/Command views, role Protocols, DecideCtx/ActCtx, OBS_LOG/CMD_LOG
├── bus.py                # NEW (Task 2): Bus — two-log core (emit_*, add_*, consume loops)
├── backoff.py            # (reused unchanged by bus.py)
├── sensors/
│   ├── __init__.py       # (Task 4/5)
│   ├── discord.py        # NEW (Task 4): DiscordSensor (+ Command/Option specs) — slash command → observation
│   └── github.py         # NEW (Task 5): GitHubSensor (+ SeenStore inside) — webhook → observation
├── deciders/
│   ├── __init__.py       # (Task 6)
│   ├── github_notify.py  # NEW (Task 6): build_message + GitHubNotifyDecider (github.* → discord.post)
│   └── discord_cmds.py   # NEW (Task 6): PingDecider, EchoDecider (discord.command.* → discord.reply)
├── actuators/
│   ├── __init__.py       # (Task 3)
│   └── discord.py        # NEW (Task 3): DiscordSender + DiscordPost + DiscordReply actuators
├── taps/
│   ├── __init__.py       # (Task 7)
│   └── logger.py         # NEW (Task 7): LoggerTap
└── app.py                # REWRITE (Task 8): wire the four-role stack
# deleted in Task 9: broker.py, event.py, ingress/, egress/, dedup.py, errors.py
tests/
├── test_message.py               # Task 1
├── test_bus.py                   # Task 2 (fake roles, full spine)
├── test_actuators_discord.py     # Task 3
├── test_sensor_discord.py        # Task 4
├── test_sensor_github.py         # Task 5
├── test_deciders.py              # Task 6
├── test_tap_logger.py            # Task 7
├── test_relay_e2e.py             # Task 8 (rewrite of the two integration tests)
├── test_app.py                   # Task 8 (rewrite)
└── (removed in Task 9: test_broker, test_dedup, test_event, test_egress, test_discord_events,
     test_discord_egress, test_discord_ingress, test_discord_sender, test_discord_integration,
     test_github_map, test_github_endpoint, test_github_relay_integration)
```

---

## Task 1: `message.py` — event views, role protocols, ctx

**Files:**
- Create: `switchboard/message.py`
- Test: `tests/test_message.py`

**Interfaces (produces):** `OBS_LOG`, `CMD_LOG`; `Observation`, `Command` (frozen dataclasses with `.from_message(msg)`); `DecideCtx` (`await ctx.command(name, args) -> int`); `ActCtx` (`.context`, `await ctx.result(outcome, payload=None) -> int`); Protocols `Sensor`, `Decider`, `Actuator`, `Tap`.

- [ ] **Step 1: Write the failing test** — `tests/test_message.py`:
```python
from switchboard.message import Observation, Command, OBS_LOG, CMD_LOG


class _Msg:
    def __init__(self, id, payload, metadata):
        self.id, self.payload, self.metadata = id, payload, metadata


def test_logs_are_named():
    assert OBS_LOG == "obs" and CMD_LOG == "cmd"


def test_observation_from_message():
    o = Observation.from_message(_Msg(5, {"a": 1}, {"name": "github.home.pr.opened"}))
    assert (o.id, o.name, o.payload, o.command_id) == (5, "github.home.pr.opened", {"a": 1}, None)


def test_result_observation_carries_command_id():
    o = Observation.from_message(_Msg(9, {"message_id": "m1"}, {"name": "discord.post.ok", "command_id": 3}))
    assert o.command_id == 3 and o.name == "discord.post.ok"


def test_command_from_message():
    c = Command.from_message(_Msg(3, {"channel": "c"}, {"name": "discord.post", "observation_id": 5}))
    assert (c.id, c.name, c.args, c.observation_id) == (3, "discord.post", {"channel": "c"}, 5)
```

- [ ] **Step 2: Run to verify it fails** — `. venv/bin/activate && python -m pytest tests/test_message.py -v` → module missing.

- [ ] **Step 3: Write `switchboard/message.py`:**
```python
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Protocol, runtime_checkable

OBS_LOG = "obs"
CMD_LOG = "cmd"


@dataclass(frozen=True)
class Observation:
    id: int
    name: str
    payload: dict
    command_id: int | None = None      # present ⇒ this is a result observation

    @classmethod
    def from_message(cls, msg) -> "Observation":
        md = msg.metadata or {}
        return cls(id=msg.id, name=md.get("name", ""),
                   payload=msg.payload or {}, command_id=md.get("command_id"))


@dataclass(frozen=True)
class Command:
    id: int
    name: str
    args: dict
    observation_id: int | None = None  # the observation that triggered this command

    @classmethod
    def from_message(cls, msg) -> "Command":
        md = msg.metadata or {}
        return cls(id=msg.id, name=md.get("name", ""),
                   args=msg.payload or {}, observation_id=md.get("observation_id"))


@dataclass
class DecideCtx:
    obs: Observation
    _emit_command: Callable[[str, dict, int], Awaitable[int]]

    async def command(self, name: str, args: dict) -> int:
        return await self._emit_command(name, args, self.obs.id)


@dataclass
class ActCtx:
    cmd: Command
    context: Any
    _emit_result: Callable[[str, dict, int], Awaitable[int]]

    async def result(self, outcome: str, payload: dict | None = None) -> int:
        return await self._emit_result(f"{self.cmd.name}.{outcome}", payload or {}, self.cmd.id)


@runtime_checkable
class Sensor(Protocol):
    name: str
    async def start(self, emit: Callable[[str, dict], Awaitable[int]]) -> None: ...
    async def stop(self) -> None: ...


@runtime_checkable
class Decider(Protocol):
    name: str
    def subscribes(self, obs: Observation) -> bool: ...
    async def decide(self, obs: Observation, ctx: DecideCtx) -> None: ...


@runtime_checkable
class Actuator(Protocol):
    name: str                          # == the command name it executes
    def context(self) -> Any: ...
    async def act(self, cmd: Command, ctx: ActCtx) -> None: ...


@runtime_checkable
class Tap(Protocol):
    name: str
    logs: tuple[str, ...]              # which logs it reads, e.g. ("obs", "cmd")
    async def observe(self, log: str, view) -> None: ...
```

- [ ] **Step 4: Run to verify it passes** — 4 passed.
- [ ] **Step 5: Commit** — `git add switchboard/message.py tests/test_message.py && git commit -m "feat(core): message views + four-role protocols"`

---

## Task 2: `bus.py` — the two-log core

**Files:**
- Create: `switchboard/bus.py`
- Test: `tests/test_bus.py`

**Interfaces (produces):** `class Bus(mamamia_db_path, *, default_timeout_s=30.0, wait_ms=30_000, reaper_interval=60.0, max_retries=10)` with `add_sensor/add_decider/add_actuator/add_tap`, `async emit_observation(name, payload, command_id=None) -> int`, `async emit_command(name, args, observation_id) -> int`, `async start()`, `async stop()`.

**Consumes:** `switchboard.message` (views, protocols, OBS_LOG, CMD_LOG); `switchboard.backoff.backoff`; mamamia (as in current `broker.py`).

- [ ] **Step 1: Write the failing test** — `tests/test_bus.py`:
```python
import asyncio
from switchboard.bus import Bus
from switchboard.message import OBS_LOG, CMD_LOG


async def _wait(pred, timeout=8.0):
    async def loop():
        while not pred():
            await asyncio.sleep(0.01)
    await asyncio.wait_for(loop(), timeout)


class _Decider:
    name = "trigger"
    def subscribes(self, obs): return obs.name == "thing.happened"
    async def decide(self, obs, ctx):
        await ctx.command("do.it", {"echo": obs.payload["v"]})


class _Actuator:
    name = "do.it"
    def __init__(self): self.acted = []
    def context(self): return None
    async def act(self, cmd, ctx):
        self.acted.append(cmd.args["echo"])
        await ctx.result("ok", {"handled": cmd.args["echo"]})


class _Tap:
    name = "spy"
    logs = (OBS_LOG, CMD_LOG)
    def __init__(self): self.seen = []
    async def observe(self, log, view):
        self.seen.append((log, view.name))


async def test_full_spine_obs_to_cmd_to_result(tmp_path):
    act, tap = _Actuator(), _Tap()
    bus = Bus(str(tmp_path / "mm.db"), wait_ms=50, reaper_interval=3600.0)
    bus.add_decider(_Decider())
    bus.add_actuator(act)
    bus.add_tap(tap)
    await bus.start()
    try:
        await bus.emit_observation("thing.happened", {"v": 42})
        await _wait(lambda: act.acted == [42])
        # result observation flowed back onto the obs log and the tap saw the whole spine
        await _wait(lambda: ("obs", "do.it.ok") in tap.seen)
        names = {(log, n) for (log, n) in tap.seen}
        assert ("obs", "thing.happened") in names
        assert ("cmd", "do.it") in names
        assert ("obs", "do.it.ok") in names
    finally:
        await bus.stop()


async def test_actuator_only_consumes_its_command_name(tmp_path):
    act = _Actuator()  # name "do.it"
    bus = Bus(str(tmp_path / "mm.db"), wait_ms=50, reaper_interval=3600.0)
    bus.add_actuator(act)
    await bus.start()
    try:
        await bus.emit_command("something.else", {"x": 1}, observation_id=1)
        await asyncio.sleep(0.3)
        assert act.acted == []            # not its command → skipped, no effect
    finally:
        await bus.stop()
```

- [ ] **Step 2: Run to verify it fails** — module missing.

- [ ] **Step 3: Write `switchboard/bus.py`** (mirrors the current broker's mamamia setup + consume loop, adapted to two logs and the four roles; dedup and depth-cap removed):
```python
import asyncio
import logging
import uuid

from mamamia.core.models import MessageState, Outcome
from mamamia.server.db import connect
from mamamia.server.storage.sqlite import SQLiteStorage
from mamamia.server.state.sqlite import SQLiteStateStore
from mamamia.server.lease.sqlite import SQLiteLeaseManager
from mamamia.server.transaction import SQLiteTransaction
from mamamia.server.registry import LogRegistry

from switchboard.backoff import backoff
from switchboard.message import (
    OBS_LOG, CMD_LOG, Observation, Command, DecideCtx, ActCtx,
)

logger = logging.getLogger(__name__)


class Bus:
    def __init__(self, mamamia_db_path, *, default_timeout_s=30.0, wait_ms=30_000,
                 reaper_interval=60.0, max_retries=10, max_log_messages=10_000, max_dead=500):
        self._db = mamamia_db_path
        self._default_timeout_s = default_timeout_s
        self._wait_ms = wait_ms
        self._reaper_interval = reaper_interval
        self._max_retries = max_retries
        self._max_log_messages = max_log_messages
        self._max_dead = max_dead

        self._instance = f"sb-{uuid.uuid4().hex}"
        self._sensors, self._deciders, self._actuators, self._taps = [], [], [], []
        self._registry = None
        self._conn = None
        self._tasks: list[asyncio.Task] = []
        self._running = False

    # registration
    def add_sensor(self, s): self._sensors.append(s)
    def add_decider(self, d): self._deciders.append(d)
    def add_actuator(self, a): self._actuators.append(a)
    def add_tap(self, t): self._taps.append(t)

    # emit
    async def _append(self, log, name, payload, *, command_id=None, observation_id=None) -> int:
        md = {"name": name}
        if command_id is not None:
            md["command_id"] = command_id
        if observation_id is not None:
            md["observation_id"] = observation_id
        mid = await self._registry.get_storage().append(log, payload, metadata=md)
        self._registry.notify(log)
        return mid

    async def emit_observation(self, name, payload, command_id=None) -> int:
        return await self._append(OBS_LOG, name, payload, command_id=command_id)

    async def emit_command(self, name, args, observation_id) -> int:
        return await self._append(CMD_LOG, name, args, observation_id=observation_id)

    async def start(self) -> None:
        self._conn = await connect(self._db)
        self._registry = LogRegistry(
            storage=SQLiteStorage(self._conn), state=SQLiteStateStore(self._conn),
            lease=SQLiteLeaseManager(self._conn), transaction=SQLiteTransaction(self._conn),
            max_log_messages=self._max_log_messages, max_dead=self._max_dead,
        )
        for log in (OBS_LOG, CMD_LOG):
            self._registry.get_orchestrator(log).max_retries = self._max_retries
        self._running = True
        self._registry.start_reaper(interval=self._reaper_interval)

        for d in self._deciders:
            self._tasks.append(asyncio.create_task(self._run_decider(d)))
        for a in self._actuators:
            self._tasks.append(asyncio.create_task(self._run_actuator(a)))
        for t in self._taps:
            for log in t.logs:
                self._tasks.append(asyncio.create_task(self._run_tap(t, log)))
        for s in self._sensors:
            async def _emit(name, payload, _s=s):
                return await self.emit_observation(name, payload)
            self._tasks.append(asyncio.create_task(s.start(_emit)))

    async def stop(self) -> None:
        self._running = False
        for s in self._sensors:
            try:
                await s.stop()
            except Exception:
                pass
        for t in self._tasks:
            t.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        if self._conn is not None:
            self._conn.close()

    # generic consume loop shared by all consuming roles
    async def _consume(self, log, group_id, decode, keep, handle):
        orch = self._registry.get_orchestrator(log)
        timeout_s, lease_s = self._default_timeout_s, self._default_timeout_s * 2

        async def settle(mid, outcome, retry_after=0.0):
            try:
                await orch.settle(log, group_id, mid, self._instance, outcome=outcome, retry_after=retry_after)
            except PermissionError:
                logger.warning("settle skipped for %s msg %s: lease lost", group_id, mid)

        while self._running:
            try:
                msg = await self._registry.acquire_blocking(
                    log, group_id, self._instance, duration=lease_s, wait_ms=self._wait_ms)
                if msg is None:
                    continue
                view = decode(msg)
                if not keep(view):
                    await settle(msg.id, Outcome.SUCCESS)
                    continue
                try:
                    async with asyncio.timeout(timeout_s):
                        await handle(view)
                    await settle(msg.id, Outcome.SUCCESS)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    attempts = await orch.state_store.get_retry_count(log, group_id, msg.id)
                    await settle(msg.id, Outcome.RETRY, retry_after=backoff(attempts))
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("consume loop error in %s", group_id)
                await asyncio.sleep(0.1)

    async def _run_decider(self, d):
        async def handle(obs):
            ctx = DecideCtx(obs=obs, _emit_command=self.emit_command)
            await d.decide(obs, ctx)
        await self._consume(OBS_LOG, f"decider/{d.name}",
                            Observation.from_message, d.subscribes, handle)

    async def _run_actuator(self, a):
        ctx_obj = a.context()
        async def handle(cmd):
            ctx = ActCtx(cmd=cmd, context=ctx_obj,
                         _emit_result=lambda name, payload, cid: self.emit_observation(name, payload, command_id=cid))
            await a.act(cmd, ctx)
        await self._consume(CMD_LOG, f"actuator/{a.name}",
                            Command.from_message, lambda c: c.name == a.name, handle)

    async def _run_tap(self, t, log):
        decode = Observation.from_message if log == OBS_LOG else Command.from_message
        await self._consume(log, f"tap/{t.name}/{log}", decode,
                            lambda v: True, lambda v: t.observe(log, v))
```

- [ ] **Step 4: Run to verify it passes** — 2 passed.
- [ ] **Step 5: Commit** — `git add switchboard/bus.py tests/test_bus.py && git commit -m "feat(core): Bus — two-log sense/decide/act spine"`

---

## Task 3: `actuators/discord.py` — sender + post/reply actuators

**Files:**
- Create: `switchboard/actuators/__init__.py` (empty), `switchboard/actuators/discord.py`
- Test: `tests/test_actuators_discord.py`

**Interfaces (produces):** `DISCORD_API`; `DiscordSender(bot_token, application_id, *, client=None)` with `reply(interaction_token, content)`, `send(channel_id, content=None, *, embed=None, components=None)`, `close()` — **copied verbatim from the current `switchboard/egress/discord.py` `DiscordSender`** (do not modify it). `DiscordPost(bot_token, application_id, *, channel_id, client=None)` — `name="discord.post"`, `context()->DiscordSender`, `act` sends `args["embed"]`/`args["components"]` to `args["channel_id"]`, then `ctx.result("ok", {"message_id": None})`. `DiscordReply(bot_token, application_id, *, client=None)` — `name="discord.reply"`, `act` calls `sender.reply(args["interaction_token"], args["content"])`, then `ctx.result("ok")`.

- [ ] **Step 1: Write the failing test** — `tests/test_actuators_discord.py`:
```python
import asyncio, json, httpx
from switchboard.actuators.discord import DiscordPost, DiscordReply, DISCORD_API
from switchboard.message import Command, ActCtx


def _client(handler):
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


def _cmd(name, args):
    class M:  # minimal message
        id = 1
        payload = args
        metadata = {"name": name, "observation_id": 7}
    return Command.from_message(M())


async def _recorder(results):
    async def emit_result(name, payload, cmd_id):   # matches ActCtx._emit_result (awaited)
        results.append((name, payload, cmd_id)); return 0
    return emit_result


async def test_discord_post_sends_embed_and_reports_result():
    seen, results = {}, []
    def h(req):
        seen["url"] = str(req.url); seen["body"] = json.loads(req.content)
        return httpx.Response(200, json={"id": "m-1"})
    a = DiscordPost("bot", "app", channel_id="chan-9", client=_client(h))
    ctx = ActCtx(cmd=_cmd("discord.post", {"channel_id": "chan-9", "embed": {"title": "hi"}}),
                 context=a.context(), _emit_result=await _recorder(results))
    await a.act(ctx.cmd, ctx)
    assert seen["url"] == f"{DISCORD_API}/channels/chan-9/messages"
    assert seen["body"]["embeds"] == [{"title": "hi"}]
    assert results and results[0][0] == "discord.post.ok"


async def test_discord_reply_uses_followup_and_reports_result():
    seen, results = {}, []
    def h(req):
        seen["url"] = str(req.url); seen["body"] = json.loads(req.content)
        return httpx.Response(200, json={})
    a = DiscordReply("bot", "app", client=_client(h))
    ctx = ActCtx(cmd=_cmd("discord.reply", {"interaction_token": "tok", "content": "pong"}),
                 context=a.context(), _emit_result=await _recorder(results))
    await a.act(ctx.cmd, ctx)
    assert seen["url"] == f"{DISCORD_API}/webhooks/app/tok"
    assert seen["body"] == {"content": "pong"}
    assert results and results[0][0] == "discord.reply.ok"
```

- [ ] **Step 2: Run to verify it fails** — module missing.

- [ ] **Step 3: Write `switchboard/actuators/discord.py`** — first copy the `DiscordSender` class and `DISCORD_API` **verbatim** from `switchboard/egress/discord.py` (the current version with the embed/components `send`), then add:
```python
class DiscordPost:
    """Actuator for the `discord.post` command: post a channel message."""
    name = "discord.post"

    def __init__(self, bot_token, application_id, *, channel_id=None, client=None):
        self._sender = DiscordSender(bot_token, application_id, client=client)
        self._default_channel = channel_id

    def context(self):
        return self._sender

    async def act(self, cmd, ctx):
        channel = cmd.args.get("channel_id") or self._default_channel
        await ctx.context.send(channel, embed=cmd.args.get("embed"),
                               components=cmd.args.get("components"))
        await ctx.result("ok", {"channel_id": channel})

    async def close(self):
        await self._sender.close()


class DiscordReply:
    """Actuator for the `discord.reply` command: interaction followup (model A)."""
    name = "discord.reply"

    def __init__(self, bot_token, application_id, *, client=None):
        self._sender = DiscordSender(bot_token, application_id, client=client)

    def context(self):
        return self._sender

    async def act(self, cmd, ctx):
        await ctx.context.reply(cmd.args["interaction_token"], cmd.args["content"])
        await ctx.result("ok")

    async def close(self):
        await self._sender.close()
```

- [ ] **Step 4: Run to verify it passes** — 2 passed.
- [ ] **Step 5: Commit** — `git add switchboard/actuators/ tests/test_actuators_discord.py && git commit -m "feat(actuators): discord.post + discord.reply (+ DiscordSender)"`

---

## Task 4: `sensors/discord.py` — DiscordSensor (gateway → observation)

**Files:**
- Create: `switchboard/sensors/__init__.py` (empty), `switchboard/sensors/discord.py`
- Test: `tests/test_sensor_discord.py`

**Interfaces (produces):** `Option`, `Command as CommandSpec` (the typed-option specs — **move the `Option`/`Command` dataclasses verbatim** from `switchboard/ingress/discord.py`, but rename the spec dataclass to `CommandSpec` to avoid clashing with `message.Command`); `DiscordSensor(bot_token, *, commands: list[CommandSpec], guild_id=None)` — `name="discord"`, `async start(emit)`, `async stop()`. On each slash interaction it `defer()`s and calls `await emit(f"discord.command.{name}", {"interaction_token", "channel_id", "guild_id", "user_id", "user_name", "options"})`.

**Move instructions:** copy the whole of `switchboard/ingress/discord.py` into `sensors/discord.py`, then change exactly:
- rename the `class DiscordIngress` → `class DiscordSensor`; rename the spec dataclass `Command` → `CommandSpec` (update its uses in `_make_command`/`__init__`).
- `__init__` param `commands: list[CommandSpec]`.
- `start(self, publish)` → `start(self, emit)`: store `self._emit = emit` then `await self._client.start(self._token)`.
- delete `build_command_event`; the callback now emits directly:
```python
        async def callback(interaction: discord.Interaction, **kwargs):
            await interaction.response.defer()
            await self._emit(f"discord.command.{spec.name}", {
                "interaction_token": interaction.token,
                "channel_id": str(interaction.channel_id),
                "guild_id": str(interaction.guild_id),
                "user_id": str(interaction.user.id),
                "user_name": str(interaction.user),
                "options": dict(kwargs),
            })
```
- keep everything else (client with `Intents.none()`, `CommandTree`, `_make_command` dynamic-signature builder, `on_ready`/`_sync_commands` with the retry fix) identical.

- [ ] **Step 1: Write the failing test** — `tests/test_sensor_discord.py`:
```python
import inspect, discord
from switchboard.sensors.discord import DiscordSensor, CommandSpec, Option


def _sensor():
    return DiscordSensor("bot", commands=[
        CommandSpec("ping", "Ping"),
        CommandSpec("echo", "Echo", options=(Option("message", "text", type=str, required=True),)),
    ], guild_id="456")


def test_sensor_registers_typed_commands_without_network():
    s = _sensor()
    assert s.name == "discord"
    names = {c.name for c in s._tree.get_commands()}
    assert {"ping", "echo"} <= names
    echo = next(c for c in s._tree.get_commands() if c.name == "echo")
    params = {p.name: p for p in echo.parameters}
    assert params["message"].type is discord.AppCommandOptionType.string
    assert inspect.iscoroutinefunction(s.start) and inspect.iscoroutinefunction(s.stop)
```

- [ ] **Step 2: Run to verify it fails** — module missing.
- [ ] **Step 3: Create `switchboard/sensors/discord.py`** per the move instructions above. (`pip`/`discord.py` already installed.)
- [ ] **Step 4: Run to verify it passes** — 1 passed.
- [ ] **Step 5: Full suite** — `python -m pytest -q` still green (new file additive).
- [ ] **Step 6: Commit** — `git add switchboard/sensors/ tests/test_sensor_discord.py && git commit -m "feat(sensors): DiscordSensor — slash command -> observation"`

---

## Task 5: `sensors/github.py` — GitHubSensor (webhook → observation, dedup inside)

**Files:**
- Create: `switchboard/sensors/github.py`
- Test: `tests/test_sensor_github.py`

**Interfaces (produces):** `verify_signature(secret, body, header)` and `map_event(gh_event, payload) -> tuple[str, dict] | None` (returns `(name, payload)` — **move `map_event` from `ingress/github.py`, changing only the return**: instead of `EventInput(kind=..., source="github", payload=payload)` return `(kind, payload)`; keep every mapping incl. `check_run.succeeded`). `GitHubSensor(secret, *, host="0.0.0.0", port=8080, seen_db=":memory:")` — `name="github"`, holds a `SeenStore` internally keyed by `X-GitHub-Delivery` (copy the current `switchboard/dedup.py` `SeenStore` into this module or import it; the guard is now the sensor's, not the bus's). `async start(emit)` serves the webhook (uvicorn, as today); on a valid, non-duplicate delivery it `await emit(name, payload)`. `async stop()`.

**Move instructions:** copy `ingress/github.py` into `sensors/github.py`; change `map_event` to return `(kind, payload)` (or `None`); rename `GitHubIngress` → `GitHubSensor`; in `_webhook`, after signature/JSON checks and `map_event`, do the delivery dedup with the sensor's own `SeenStore` (skip if seen, else record + `await self._emit(name, payload)`); `start(self, publish)` → `start(self, emit)` storing `self._emit`. Bring the `SeenStore` implementation along (copy from `switchboard/dedup.py`).

- [ ] **Step 1: Write the failing test** — `tests/test_sensor_github.py`:
```python
import json
from pathlib import Path
from switchboard.sensors.github import map_event, verify_signature

FIX = Path(__file__).parent / "fixtures" / "github"
def _load(n): return json.loads((FIX / n).read_text())


def test_map_pr_opened_returns_name_and_payload():
    got = map_event("pull_request", _load("pull_request.opened.json"))
    assert got is not None
    name, payload = got
    assert name == "github.home.pr.opened"
    assert payload["number"] == 7


def test_map_check_run_success():
    name, _ = map_event("check_run", _load("check_run.success.json"))
    assert name == "github.home.check_run.succeeded"


def test_map_unknown_ignored():
    assert map_event("star", {"repository": {"name": "home"}}) is None


def test_verify_signature_roundtrip():
    import hmac, hashlib
    body = b'{"a":1}'
    sig = "sha256=" + hmac.new(b"s", body, hashlib.sha256).hexdigest()
    assert verify_signature("s", body, sig) is True
    assert verify_signature("s", body, None) is False
```

- [ ] **Step 2: Run to verify it fails** — module missing.
- [ ] **Step 3: Create `switchboard/sensors/github.py`** per the move instructions.
- [ ] **Step 4: Run to verify it passes** — 4 passed.
- [ ] **Step 5: Full suite** — still green.
- [ ] **Step 6: Commit** — `git add switchboard/sensors/github.py tests/test_sensor_github.py && git commit -m "feat(sensors): GitHubSensor — webhook -> observation, delivery dedup internal"`

---

## Task 6: deciders — github-notify + ping/echo

**Files:**
- Create: `switchboard/deciders/__init__.py` (empty), `switchboard/deciders/github_notify.py`, `switchboard/deciders/discord_cmds.py`
- Test: `tests/test_deciders.py`

**Interfaces (produces):**
- `build_message(name, payload) -> dict | None` — **move verbatim from `switchboard/egress/github_notify.py`** (the pure formatter; it already takes `(kind, payload)` — `name` is the same string).
- `GitHubNotifyDecider(channel_id)` — `name="github-notify"`, `subscribes(obs)` = `obs.name.startswith("github.")`, `decide` builds `build_message(obs.name, obs.payload)`; if `None`, do nothing; else `await ctx.command("discord.post", {"channel_id": channel_id, "embed": msg["embed"], "components": msg["components"]})`.
- `PingDecider` — `name="ping"`, `subscribes` = `obs.name == "discord.command.ping"`, `decide` → `await ctx.command("discord.reply", {"interaction_token": obs.payload["interaction_token"], "content": "pong (via the durable path)"})`.
- `EchoDecider` — `name="echo"`, `subscribes` = `obs.name == "discord.command.echo"`, `decide` → `await ctx.command("discord.reply", {"interaction_token": obs.payload["interaction_token"], "content": obs.payload.get("options", {}).get("message", "")})`.

- [ ] **Step 1: Write the failing test** — `tests/test_deciders.py`:
```python
from switchboard.deciders.github_notify import GitHubNotifyDecider
from switchboard.deciders.discord_cmds import PingDecider, EchoDecider
from switchboard.message import Observation, DecideCtx


def _obs(name, payload): return Observation(id=1, name=name, payload=payload)


class _Rec:
    def __init__(self): self.cmds = []
    async def __call__(self, name, args, obs_id): self.cmds.append((name, args)); return 0
def _ctx(obs, rec): return DecideCtx(obs=obs, _emit_command=rec)


async def test_github_notify_emits_discord_post():
    rec = _Rec()
    d = GitHubNotifyDecider(channel_id="chan-9")
    obs = _obs("github.home.pr.opened",
               {"repository": {"full_name": "yp/home", "html_url": "https://github.com/yp/home"},
                "sender": {"login": "alice"},
                "pull_request": {"number": 7, "title": "T", "html_url": "https://github.com/yp/home/pull/7"}})
    assert d.subscribes(obs) is True
    await d.decide(obs, _ctx(obs, rec))
    (name, args), = rec.cmds
    assert name == "discord.post"
    assert args["channel_id"] == "chan-9"
    assert args["embed"]["title"] == "🔀 PR #7 opened"
    assert args["components"][0]["components"][0]["label"] == "View PR"


async def test_github_notify_skips_unknown_kind():
    rec = _Rec()
    d = GitHubNotifyDecider(channel_id="c")
    obs = _obs("github.home.pr.locked", {})
    await d.decide(obs, _ctx(obs, rec))
    assert rec.cmds == []


async def test_ping_and_echo_emit_reply():
    rec = _Rec()
    p = PingDecider()
    obs = _obs("discord.command.ping", {"interaction_token": "tok"})
    assert p.subscribes(obs)
    await p.decide(obs, _ctx(obs, rec))
    assert rec.cmds == [("discord.reply", {"interaction_token": "tok", "content": "pong (via the durable path)"})]

    rec2 = _Rec()
    e = EchoDecider()
    obs2 = _obs("discord.command.echo", {"interaction_token": "t2", "options": {"message": "hi"}})
    await e.decide(obs2, _ctx(obs2, rec2))
    assert rec2.cmds == [("discord.reply", {"interaction_token": "t2", "content": "hi"})]
```

- [ ] **Step 2: Run to verify it fails** — modules missing.
- [ ] **Step 3: Create the three files** — `deciders/github_notify.py` (move `build_message` verbatim + add `GitHubNotifyDecider`), `deciders/discord_cmds.py` (`PingDecider`, `EchoDecider`) per interfaces above.
- [ ] **Step 4: Run to verify it passes** — tests pass.
- [ ] **Step 5: Commit** — `git add switchboard/deciders/ tests/test_deciders.py && git commit -m "feat(deciders): github-notify + ping/echo (observation -> command)"`

---

## Task 7: `taps/logger.py` — LoggerTap

**Files:**
- Create: `switchboard/taps/__init__.py` (empty), `switchboard/taps/logger.py`
- Test: `tests/test_tap_logger.py`

**Interfaces (produces):** `LoggerTap(stream=None)` — `name="logger"`, `logs=("obs", "cmd")`, `async observe(log, view)` writes one JSON line `{"log", "id", "name", "payload"}` (+ `command_id`/`observation_id` when present) and flushes.

- [ ] **Step 1: Write the failing test** — `tests/test_tap_logger.py`:
```python
import io, json
from switchboard.taps.logger import LoggerTap
from switchboard.message import Observation, Command


async def test_logs_observation_and_command_lines():
    buf = io.StringIO()
    tap = LoggerTap(stream=buf)
    assert tap.name == "logger" and tap.logs == ("obs", "cmd")
    await tap.observe("obs", Observation(id=5, name="github.home.pr.opened", payload={"n": 1}))
    await tap.observe("cmd", Command(id=3, name="discord.post", args={"c": "x"}, observation_id=5))
    lines = [json.loads(l) for l in buf.getvalue().splitlines()]
    assert lines[0] == {"log": "obs", "id": 5, "name": "github.home.pr.opened", "payload": {"n": 1}}
    assert lines[1]["log"] == "cmd" and lines[1]["name"] == "discord.post" and lines[1]["observation_id"] == 5
```

- [ ] **Step 2: Run to verify it fails** — module missing.
- [ ] **Step 3: Write `switchboard/taps/logger.py`:**
```python
import json
import sys


class LoggerTap:
    """A tap over both logs — the structured-JSON trace (obs → cmd → result)."""
    name = "logger"
    logs = ("obs", "cmd")

    def __init__(self, stream=None):
        self._stream = stream or sys.stdout

    async def observe(self, log, view) -> None:
        line = {"log": log, "id": view.id, "name": view.name,
                "payload": getattr(view, "payload", None) if log == "obs" else getattr(view, "args", None)}
        cid = getattr(view, "command_id", None)
        oid = getattr(view, "observation_id", None)
        if cid is not None:
            line["command_id"] = cid
        if oid is not None:
            line["observation_id"] = oid
        self._stream.write(json.dumps(line) + "\n")
        self._stream.flush()
```

- [ ] **Step 4: Run to verify it passes** — 1 passed.
- [ ] **Step 5: Commit** — `git add switchboard/taps/ tests/test_tap_logger.py && git commit -m "feat(taps): LoggerTap — obs+cmd trace"`

---

## Task 8: Cutover — rewrite `app.py`, rewrite the e2e + app tests

**Files:**
- Rewrite: `switchboard/app.py`
- Create: `tests/test_relay_e2e.py`
- Rewrite: `tests/test_app.py`

**Interfaces (produces):** `build(config) -> tuple[Bus, list_of_sensors]`; `run()`. Discord sensor + `discord.reply` actuator + ping/echo deciders wired when `discord_bot_token` set; `discord.post` actuator + `github-notify` decider wired when `discord_notify_channel_id` also set; github sensor + logger tap always.

- [ ] **Step 1: Write the failing e2e + app tests**

`tests/test_relay_e2e.py` — drive the *real* Bus + real deciders/actuators, fake only the httpx edge:
```python
import asyncio, json, httpx
from switchboard.bus import Bus
from switchboard.deciders.github_notify import GitHubNotifyDecider
from switchboard.deciders.discord_cmds import PingDecider
from switchboard.actuators.discord import DiscordPost, DiscordReply


async def _wait(pred, timeout=8.0):
    async def loop():
        while not pred(): await asyncio.sleep(0.01)
    await asyncio.wait_for(loop(), timeout)


async def test_github_observation_reaches_channel(tmp_path):
    seen = {}
    def h(req):
        seen["url"] = str(req.url); seen["body"] = json.loads(req.content)
        return httpx.Response(200, json={"id": "m1"})
    client = httpx.AsyncClient(transport=httpx.MockTransport(h))
    bus = Bus(str(tmp_path / "mm.db"), wait_ms=50, reaper_interval=3600.0)
    bus.add_decider(GitHubNotifyDecider(channel_id="chan-9"))
    bus.add_actuator(DiscordPost("bot", "app", client=client))
    await bus.start()
    try:
        await bus.emit_observation("github.home.pr.opened",
            {"repository": {"full_name": "yp/home", "html_url": "https://github.com/yp/home"},
             "sender": {"login": "alice"},
             "pull_request": {"number": 7, "title": "Add retry", "html_url": "https://github.com/yp/home/pull/7"}})
        await _wait(lambda: "url" in seen)
        assert seen["url"] == "https://discord.com/api/v10/channels/chan-9/messages"
        assert seen["body"]["embeds"][0]["title"] == "🔀 PR #7 opened"
        assert [b["label"] for b in seen["body"]["components"][0]["components"]] == ["View PR", "View diff"]
    finally:
        await bus.stop(); await client.aclose()


async def test_ping_observation_reaches_followup(tmp_path):
    seen = {}
    def h(req):
        seen["url"] = str(req.url); seen["body"] = json.loads(req.content)
        return httpx.Response(200, json={})
    client = httpx.AsyncClient(transport=httpx.MockTransport(h))
    bus = Bus(str(tmp_path / "mm.db"), wait_ms=50, reaper_interval=3600.0)
    bus.add_decider(PingDecider())
    bus.add_actuator(DiscordReply("bot", "app", client=client))
    await bus.start()
    try:
        await bus.emit_observation("discord.command.ping", {"interaction_token": "tok-1"})
        await _wait(lambda: "url" in seen)
        assert seen["url"] == "https://discord.com/api/v10/webhooks/app/tok-1"
        assert seen["body"] == {"content": "pong (via the durable path)"}
    finally:
        await bus.stop(); await client.aclose()
```

`tests/test_app.py` (replace entirely):
```python
from switchboard.app import build
from switchboard.sensors.github import GitHubSensor
from switchboard.sensors.discord import DiscordSensor


def _base(tmp_path):
    return {"mamamia_db_path": str(tmp_path / "mm.db"), "github_secret": "s"}


def test_build_github_only(tmp_path):
    bus, sensors = build(_base(tmp_path))
    assert any(isinstance(s, GitHubSensor) for s in sensors)
    assert not any(isinstance(s, DiscordSensor) for s in sensors)
    names = {a.name for a in bus._actuators}
    assert "discord.post" not in names and "discord.reply" not in names
    assert any(t.name == "logger" for t in bus._taps)


def test_build_wires_discord_and_relay(tmp_path):
    cfg = _base(tmp_path) | {"discord_bot_token": "bot", "discord_application_id": "app",
                             "discord_notify_channel_id": "chan-9"}
    bus, sensors = build(cfg)
    assert any(isinstance(s, DiscordSensor) for s in sensors)
    assert {"discord.post", "discord.reply"} <= {a.name for a in bus._actuators}
    assert {"ping", "echo", "github-notify"} <= {d.name for d in bus._deciders}


def test_relay_decider_absent_without_notify_channel(tmp_path):
    cfg = _base(tmp_path) | {"discord_bot_token": "bot", "discord_application_id": "app"}
    bus, _ = build(cfg)
    dnames = {d.name for d in bus._deciders}
    assert "ping" in dnames and "github-notify" not in dnames
    assert "discord.reply" in {a.name for a in bus._actuators}
    assert "discord.post" not in {a.name for a in bus._actuators}
```

- [ ] **Step 2: Run to verify they fail** — `python -m pytest tests/test_relay_e2e.py tests/test_app.py -v` (app.py still old shape / e2e wiring absent).

- [ ] **Step 3: Rewrite `switchboard/app.py`:**
```python
import asyncio
import os

from switchboard.bus import Bus
from switchboard.sensors.github import GitHubSensor
from switchboard.sensors.discord import DiscordSensor, CommandSpec, Option
from switchboard.deciders.github_notify import GitHubNotifyDecider
from switchboard.deciders.discord_cmds import PingDecider, EchoDecider
from switchboard.actuators.discord import DiscordPost, DiscordReply
from switchboard.taps.logger import LoggerTap

DISCORD_COMMANDS = [
    CommandSpec("ping", "Ping Switchboard"),
    CommandSpec("echo", "Echo a message back",
                options=(Option("message", "Text to echo back", type=str, required=True),)),
]


def build(config: dict):
    bus = Bus(config["mamamia_db_path"])
    bus.add_tap(LoggerTap())

    sensors = [GitHubSensor(secret=config["github_secret"],
                            host=config.get("host", "0.0.0.0"),
                            port=int(config.get("port", 8080)),
                            seen_db=config.get("switchboard_db_path", ":memory:"))]
    for s in sensors:
        bus.add_sensor(s)

    if config.get("discord_bot_token"):
        token = config["discord_bot_token"]
        app_id = config.get("discord_application_id")
        if not app_id:
            raise ValueError("discord_application_id is required when discord_bot_token is set")
        discord_sensor = DiscordSensor(token, commands=DISCORD_COMMANDS,
                                       guild_id=config.get("discord_guild_id"))
        bus.add_sensor(discord_sensor); sensors.append(discord_sensor)
        bus.add_decider(PingDecider()); bus.add_decider(EchoDecider())
        bus.add_actuator(DiscordReply(token, app_id))
        if config.get("discord_notify_channel_id"):
            bus.add_decider(GitHubNotifyDecider(channel_id=config["discord_notify_channel_id"]))
            bus.add_actuator(DiscordPost(token, app_id))

    return bus, sensors


async def run() -> None:
    data_dir = os.environ.get("SB_DATA_DIR", "/data")
    config = {
        "mamamia_db_path": os.path.join(data_dir, "events.db"),
        "switchboard_db_path": os.path.join(data_dir, "switchboard.db"),
        "github_secret": os.environ["GITHUB_WEBHOOK_SECRET"],
        "port": int(os.environ.get("SB_PORT", "8080")),
        "discord_bot_token": os.environ.get("DISCORD_BOT_TOKEN"),
        "discord_application_id": os.environ.get("DISCORD_APPLICATION_ID"),
        "discord_guild_id": os.environ.get("DISCORD_GUILD_ID"),
        "discord_notify_channel_id": os.environ.get("DISCORD_NOTIFY_CHANNEL_ID"),
    }
    bus, _ = build(config)
    await bus.start()
    try:
        while True:
            await asyncio.sleep(3600)
    finally:
        await bus.stop()


if __name__ == "__main__":
    asyncio.run(run())
```
(Note: sensors are started by `bus.start()` via `add_sensor`; `run()` just keeps the process alive until cancelled, then stops the bus.)

- [ ] **Step 4: Run to verify they pass** — `python -m pytest tests/test_relay_e2e.py tests/test_app.py -v` all green.
- [ ] **Step 5: Commit** — `git add switchboard/app.py tests/test_relay_e2e.py tests/test_app.py && git commit -m "feat(app): cut over to the four-role Bus stack"`

---

## Task 9: Preserve the dead-letter path — Bus `PermanentError` + migrate `cli.py`

The old `Broker` immediately DEAD-lettered on `PermanentError` and the `switchboard dead-letters` CLI listed retained DEAD deliveries. Both are current behavior to preserve. The `Bus` (Task 2) dropped `PermanentError` handling, and `cli.py` still assumes the old single log `"events"` + the old Event payload shape (`payload.id`/`payload.kind`). Fix both.

**Files:**
- Modify: `switchboard/bus.py` (add `PermanentError` → DEAD in `_consume`)
- Modify: `switchboard/cli.py` (two-log query, read `metadata.name`)
- Modify: `switchboard/errors.py` (keep `PermanentError`; drop the now-unused `ChainTooDeep`)
- Rewrite: `tests/test_cli.py`

**Interfaces:** `list_dead_letters(mamamia_db_path) -> list[dict]` returns `{"log_id","group_id","message_id","name"}` per retained DEAD delivery across BOTH logs.

- [ ] **Step 1: Rewrite `tests/test_cli.py`** (drive the new Bus; a decider that raises `PermanentError` DEAD-letters immediately):
```python
import asyncio
from switchboard.bus import Bus
from switchboard.cli import list_dead_letters
from switchboard.errors import PermanentError


class _Boom:
    name = "boom"
    def subscribes(self, obs): return True
    async def decide(self, obs, ctx): raise PermanentError("nope")


async def test_dead_letters_lists_dead(tmp_path):
    mm = str(tmp_path / "e.db")
    b = Bus(mm, wait_ms=50, reaper_interval=3600.0)
    b.add_decider(_Boom())
    await b.start()
    try:
        await b.emit_observation("github.home.pr.opened", {"n": 1})
        rows = []
        for _ in range(500):
            rows = await list_dead_letters(mm)
            if rows:
                break
            await asyncio.sleep(0.02)
        assert rows, "never dead-lettered"
    finally:
        await b.stop()
    rows = await list_dead_letters(mm)
    assert any(r["group_id"] == "decider/boom" and r["name"] == "github.home.pr.opened"
               for r in rows)
```

- [ ] **Step 2: Run to verify it fails** — `python -m pytest tests/test_cli.py -v`. Expected FAIL: `PermanentError` isn't DEAD-lettered by the Bus yet (times out / no rows) and/or `list_dead_letters` returns the wrong shape.

- [ ] **Step 3: Add `PermanentError` → DEAD in `switchboard/bus.py`**

Add the import `from switchboard.errors import PermanentError` (near the `backoff` import), and in `_consume`'s inner try/except (currently `except asyncio.CancelledError: raise` then `except Exception:` → RETRY) insert a `PermanentError` branch BEFORE the generic `except Exception`:
```python
                except asyncio.CancelledError:
                    raise
                except PermanentError:
                    await settle(msg.id, Outcome.DEAD)
                except Exception:
                    attempts = await orch.state_store.get_retry_count(log, group_id, msg.id)
                    await settle(msg.id, Outcome.RETRY, retry_after=backoff(attempts))
```

- [ ] **Step 4: Migrate `switchboard/cli.py`** — replace `list_dead_letters` and drop the `LOG_ID = "events"` constant:
```python
async def list_dead_letters(mamamia_db_path: str) -> list[dict]:
    """Return retained DEAD deliveries across all logs, joined to each message's
    stored `name` (from mamamia metadata). Reads the mamamia database directly
    (read-only) — mamamia has no query API for this; the schema is stable within
    a pinned version."""
    conn = await connect(mamamia_db_path)
    try:
        dead = conn.execute(
            "SELECT log_id, group_id, message_id FROM message_state WHERE state = ? "
            "ORDER BY message_id DESC",
            (MessageState.DEAD.value,),
        ).fetchall()
        rows = []
        for log_id, group_id, message_id in dead:
            row = conn.execute(
                "SELECT metadata FROM messages WHERE log_id = ? AND id = ?",
                (log_id, message_id),
            ).fetchone()
            meta = _decode(row[0]) if row and row[0] else {}
            rows.append({"log_id": log_id, "group_id": group_id,
                         "message_id": message_id, "name": meta.get("name")})
        return rows
    finally:
        conn.close()
```
Keep `_decode` and `main` as-is (they still print each row as JSON).

- [ ] **Step 5: Trim `switchboard/errors.py`** — keep `PermanentError`; delete the `ChainTooDeep` class (nothing uses it once the depth cap is gone; verify with `grep -rn ChainTooDeep switchboard/ tests/` → only `errors.py`).

- [ ] **Step 6: Run to verify it passes** — `python -m pytest tests/test_cli.py -v` → 1 passed. Then full suite `python -m pytest -q` → still green (old stack untouched).

- [ ] **Step 7: Commit** — `git add switchboard/bus.py switchboard/cli.py switchboard/errors.py tests/test_cli.py && git commit -m "feat(bus): PermanentError->DEAD; migrate dead-letter CLI to two-log schema"`

---

## Task 10: Delete the old stack; green suite on new only

**Files:**
- Delete: `switchboard/broker.py`, `switchboard/event.py`, `switchboard/ingress/` (whole dir), `switchboard/egress/` (whole dir). **KEEP `switchboard/dedup.py`** (now the GitHub sensor's `SeenStore` dependency) and **KEEP `switchboard/errors.py`** (now `PermanentError` only, used by `bus.py`).
- Delete tests: `tests/test_broker.py`, `tests/test_event.py`, `tests/test_egress.py`, `tests/test_discord_events.py`, `tests/test_discord_egress.py`, `tests/test_discord_ingress.py`, `tests/test_discord_sender.py`, `tests/test_discord_integration.py`, `tests/test_github_map.py`, `tests/test_github_endpoint.py`, `tests/test_github_relay_integration.py`. **KEEP `tests/test_dedup.py`** (SeenStore still exists) and **KEEP `tests/test_cli.py`** (rewritten in Task 9).
- Fix: `tests/conftest.py` (remove the `broker`/`make_broker` fixtures — they import the deleted `Broker` and are used only by now-deleted tests); `switchboard/sensors/discord.py` (drop the dead `from switchboard.event import EventInput` import left from the Task 4 move); `switchboard/errors.py` (now that `broker.py`/`test_broker.py` are deleted this same task, delete the `ChainTooDeep` class — keep only `PermanentError`).

- [ ] **Step 1: Delete old modules + their tests**
```bash
git rm switchboard/broker.py switchboard/event.py
git rm -r switchboard/ingress switchboard/egress
git rm tests/test_broker.py tests/test_event.py tests/test_egress.py \
       tests/test_discord_events.py tests/test_discord_egress.py tests/test_discord_ingress.py \
       tests/test_discord_sender.py tests/test_discord_integration.py tests/test_github_map.py \
       tests/test_github_endpoint.py tests/test_github_relay_integration.py
```

- [ ] **Step 2: Fix `conftest.py` and the dead import**

In `tests/conftest.py` remove the `broker` and `make_broker` fixtures and the `from switchboard.broker import Broker` import (confirm no kept test uses those fixtures: `grep -rn "make_broker\|def test.*broker\b" tests/` — the new tests build `Bus` directly). In `switchboard/sensors/discord.py` delete the unused `from switchboard.event import EventInput` line (and freshen the class docstring's "published" wording to "emitted" while there).

- [ ] **Step 3: Sweep for stale imports**

Run: `grep -rn "switchboard.broker\|switchboard.event\|switchboard.ingress\|switchboard.egress\|import Broker\|EventInput\|ChainTooDeep" switchboard/ tests/`
Expected: NO hits (dedup.py/errors.py references are fine — those modules are kept). Fix any straggler.

- [ ] **Step 4: Full suite** — `python -m pytest -q` → all green on the four-role stack only.

- [ ] **Step 5: Commit** — `git add -A && git commit -m "refactor: remove the pre-restructure Broker/Ingress/Egress stack"`

---

## Final verification

- [ ] **Full suite** — `. venv/bin/activate && python -m pytest -q` → all pass.
- [ ] **Import/boot check** — `python -c "import switchboard.app"` and `python -c "from switchboard.bus import Bus; from switchboard.message import Observation, Command"` succeed.
- [ ] **Manual live check (optional)** — same as before (webhook → #releases embed; `/ping`/`/echo`), now flowing obs→cmd→result; the LoggerTap prints the full trace.

## Notes for the executor

- **Coexistence until Task 9:** new files must not import old ones. Small duplication (`DiscordSender`, `SeenStore`, `build_message`, `map_event`, `Option`) is intentional — the old copies are deleted in Task 9.
- **`Command` name clash:** `message.Command` (the runtime command view) vs. the Discord typed-option spec — the spec dataclass is renamed **`CommandSpec`** in `sensors/discord.py`.
- **No dedup, no depth cap** anywhere in `bus.py`. GitHub delivery dedup lives only inside `GitHubSensor`.
- **Actuators always call `ctx.result("ok", …)` on success.** Errors: either handled internally or raised → the bus's RETRY path (mamamia). No forced `.failed` observation.
- **Behavior parity is the acceptance bar:** the embed title/buttons for `pr.opened`, the `pong (via the durable path)` followup, the `/echo` message — must match the pre-restructure output exactly.
```
