# Agent Phase 2 — Actuator Contract & Generic Actuators Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give actuators a way to declare themselves as agent tools, and build the two generic actuators the agent needs — a key-value store and an LLM executor — plus make the existing Discord channel actuator able to speak plain text.

**Architecture:** `tool_spec` is one optional attribute on the `Actuator` protocol; declaring one *is* the opt-in to being agent-callable. `kv` and `llm` are ordinary actuators with no knowledge of any agent — `llm` is a dumb `messages + tools → completion` box that any decider could drive. Every actuator here follows the established shape: build clients in `bind()` (a running loop exists there), report known failures with `result("error")` rather than raising, expose an injected `client=` seam for tests.

**Tech Stack:** Python 3.12, asyncio, `httpx` (already a dependency — no `anthropic` SDK; the Messages API is a single POST and this matches how `DiscordSender` talks to Discord). pytest with `asyncio_mode = "auto"`.

**Spec:** `docs/superpowers/specs/2026-07-23-agentic-decider-design.md` §3 (stores), §7 (the actuator contract).

## Global Constraints

- `tool_spec` is `{"description": str, "input_schema": <JSON schema>}` or `None`. **Declaring one is the opt-in** — an actuator without one is not agent-callable.
- Tool name **==** actuator name **==** command name. Identity mapping, no registry.
- `kv` is **one** actuator with **one** scope (`actuator/kv/`), dispatching on an `op` argument. Two actuators (`kv.get`/`kv.set`) would get two scopes and could never see each other's writes.
- `kv` ops are **`get` / `set` / `delete` / `list`**. `list` is capped (`LIST_MAX = 200`) and reports truncation — an agent listing a large memory would blow its own context and the token bill. The cap is the **actuator's** policy; `KeyStore.keys` itself is uncapped.
- `KeyStore` gains `async keys(prefix="") -> list[str]`. It belongs in the contract because **every backend can provide it** (Redis SCAN, sqlite LIKE, dict iteration) and callers genuinely need it — the same test `purge` failed, since Redis expires natively and would expose no purge.
- `ScopedStore.keys` **strips its own prefix** from results: a role asks for keys and gets `draft`, never `actuator/kv/draft`.
- `kv` has **no `tool_spec`**: the agent reaches it only through decider-injected virtual memory tools, never directly.
- `llm` has **no `tool_spec`**: the decider emits it directly; the agent never calls the model as a tool.
- `llm` takes `{system, messages, tools, model, max_tokens}` and returns `{stop_reason, content, usage}`. Everything about the request travels in the command args because the decider owns them — the actuator is a dumb executor.
- Known failures (HTTP 4xx/5xx, bad args) → `ctx.result("error", {...})` and return normally. Only unexpected failures raise. Raising drags a caller through the whole retry-backoff cycle before it learns of a failure the actuator already understood.
- Clients are built in `bind()`, never `__init__` — an `httpx.AsyncClient` wants a running event loop, and `bind()` runs inside `Bus.start()`.
- Every actuator exposes `client=None` for test injection and `async close()` for teardown, matching `DiscordPost`/`DiscordReply`.
- No new production dependency.

## Deliberately excluded

- **`web_search`.** It needs a provider decision (Brave, Tavily, DDG…) and an API key, and the agent loop is fully functional without it — it can think, remember, and speak. It is a clean add later against the same `tool_spec` contract.
- **Thread creation.** The reply actuator posts to a channel or thread id it is given. Creating a thread is a different Discord call; the decider can route replies to wherever the mention arrived. Phase 4's concern if it matters.

## File Structure

- **Modify:** `switchboard/message.py` — one optional attribute on the `Actuator` protocol.
- **Create:** `switchboard/actuators/kv.py` — the agent's memory backend.
- **Create:** `switchboard/actuators/llm.py` — the model executor.
- **Modify:** `switchboard/actuators/discord.py` — `DiscordPost` gains content passthrough, returns `message_id`, declares a `tool_spec`.
- **Create:** `tests/test_actuator_kv.py`, `tests/test_actuator_llm.py`.
- **Modify:** `tests/test_actuators_discord.py`.

Each task is independent — none imports another. They can be reviewed and rejected separately.

---

### Task 1: `tool_spec` + the `kv` actuator

**Files:**
- Modify: `switchboard/message.py` — `Actuator` protocol
- Modify: `switchboard/store.py` — `keys()` on the contract and all three implementations
- Create: `switchboard/actuators/kv.py`
- Test: `tests/test_actuator_kv.py`, `tests/test_store.py`

**Interfaces:**
- Consumes: `ActuatorCtx` (has `.store`), `ActCtx` (has `.result(outcome, payload)`), `Command` — all from `switchboard.message`.
- Produces: `Actuator.tool_spec: ToolSpec | None`; `KeyStore.keys(prefix="") -> list[str]` on the protocol and on `MemoryStore`/`SqliteStore`/`ScopedStore`; `KvActuator()` with `name = "kv"`, consuming `{op, key?, value?, ttl?, prefix?}` and emitting `kv.ok` with `{value}` (get), `{keys, truncated}` (list) or `{}` (set/delete). Tasks 2 and 3 rely on `tool_spec` existing on the protocol.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_actuator_kv.py`:

```python
import pytest

from switchboard.actuators.kv import KvActuator
from switchboard.message import ActCtx, ActuatorCtx, Command
from switchboard.store import MemoryStore, ScopedStore


def _cmd(args):
    class M:
        id = 1
        payload = args
        metadata = {"name": "kv", "observation_id": 7}
    return Command.from_message(M())


async def _run(act, args):
    """Drive one command through the actuator, returning the result it emitted."""
    results = []
    async def emit_result(name, payload, cmd_id):
        results.append((name, payload)); return 0
    cmd = _cmd(args)
    await act.act(cmd, ActCtx(cmd=cmd, _emit_result=emit_result))
    return results[0]


def _bound(store=None):
    a = KvActuator()
    a.bind(ActuatorCtx(store=store or MemoryStore()))
    return a


async def test_set_then_get_round_trips():
    a = _bound()
    assert await _run(a, {"op": "set", "key": "draft", "value": "hello"}) == ("kv.ok", {})
    assert await _run(a, {"op": "get", "key": "draft"}) == ("kv.ok", {"value": "hello"})


async def test_get_missing_key_returns_null_value():
    a = _bound()
    assert await _run(a, {"op": "get", "key": "nope"}) == ("kv.ok", {"value": None})


async def test_delete_removes():
    a = _bound()
    await _run(a, {"op": "set", "key": "k", "value": "v"})
    await _run(a, {"op": "delete", "key": "k"})
    assert await _run(a, {"op": "get", "key": "k"}) == ("kv.ok", {"value": None})


async def test_set_honours_ttl():
    clock = type("C", (), {"t": 1000.0, "__call__": lambda self: self.t})()
    a = _bound(MemoryStore(time_fn=clock))
    await _run(a, {"op": "set", "key": "k", "value": "v", "ttl": 60.0})
    clock.t += 61.0
    assert await _run(a, {"op": "get", "key": "k"}) == ("kv.ok", {"value": None})


async def test_unknown_op_is_an_error_result_not_a_raise():
    """A bad op is a failure the actuator understands, so it reports rather than
    raising — raising would burn the whole retry cycle first."""
    a = _bound()
    name, payload = await _run(a, {"op": "obliterate", "key": "k"})
    assert name == "kv.error"
    assert "obliterate" in payload["message"]


async def test_non_string_value_is_an_error_result():
    a = _bound()
    name, payload = await _run(a, {"op": "set", "key": "k", "value": 5})
    assert name == "kv.error"


async def test_kv_is_one_actuator_with_one_scope():
    """Two actuators (kv.get / kv.set) would get two scopes and never see each
    other's writes. One actuator, one scope, dispatched on op."""
    assert KvActuator.name == "kv"


async def test_kv_declares_no_tool_spec():
    """The agent reaches kv only through decider-injected memory tools, never
    directly — so it must not advertise itself as a tool."""
    assert getattr(KvActuator, "tool_spec", None) is None


async def test_list_returns_keys_under_a_prefix():
    a = _bound()
    await _run(a, {"op": "set", "key": "note:a", "value": "1"})
    await _run(a, {"op": "set", "key": "note:b", "value": "2"})
    await _run(a, {"op": "set", "key": "other", "value": "3"})
    name, payload = await _run(a, {"op": "list", "prefix": "note:"})
    assert name == "kv.ok"
    assert sorted(payload["keys"]) == ["note:a", "note:b"]
    assert payload["truncated"] is False


async def test_list_is_capped_and_reports_truncation():
    """An agent listing a large memory would blow its own context and the token
    bill. The cap is the actuator's policy, not the store's."""
    from switchboard.actuators.kv import LIST_MAX
    a = _bound()
    for i in range(LIST_MAX + 10):
        await _run(a, {"op": "set", "key": f"k{i:04d}", "value": "v"})
    name, payload = await _run(a, {"op": "list", "prefix": "k"})
    assert len(payload["keys"]) == LIST_MAX
    assert payload["truncated"] is True


async def test_writes_land_in_the_actuators_own_scope():
    store = MemoryStore()
    a = KvActuator()
    a.bind(ActuatorCtx(store=ScopedStore(store, "actuator/kv/")))
    await _run(a, {"op": "set", "key": "draft", "value": "hello"})
    assert await store.get("actuator/kv/draft") == "hello"
```

- [ ] **Step 2: Run to verify failure**

Run: `./scripts/dev.sh test tests/test_actuator_kv.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'switchboard.actuators.kv'`

- [ ] **Step 3: Add `tool_spec` to the protocol**

In `switchboard/message.py`, change the `Actuator` protocol to:

```python
@runtime_checkable
class Actuator(Protocol):
    name: str                          # == the command name it executes
    # Declaring a tool_spec IS the opt-in to being agent-callable: an actuator
    # without one cannot be reached by an agent at all.
    # {"description": str, "input_schema": <JSON schema>}
    tool_spec: dict | None
    def bind(self, ctx: ActuatorCtx) -> None: ...
    async def act(self, cmd: Command, ctx: ActCtx) -> None: ...
```

- [ ] **Step 4: Add `keys()` to the store contract and all three implementations**

In `switchboard/store.py`, add to the `KeyStore` protocol (and extend its docstring's second paragraph to mention `keys`):

```python
    async def keys(self, prefix: str = "") -> list[str]: ...
```

`MemoryStore`:

```python
    async def keys(self, prefix: str = "") -> list[str]:
        _check_key(prefix)
        now = self._now()
        return [k for k, (_, exp) in self._data.items()
                if k.startswith(prefix) and (exp is None or exp > now)]
```

`SqliteStore` — the prefix is escaped, or a key containing `%` or `_` would match far more than it should:

```python
    async def keys(self, prefix: str = "") -> list[str]:
        _check_key(prefix)
        like = prefix.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"
        rows = self._conn.execute(
            "SELECT key FROM kv WHERE key LIKE ? ESCAPE '\\' "
            "AND (expires_at IS NULL OR expires_at > ?)",
            (like, self._now())).fetchall()
        return [r[0] for r in rows]
```

`ScopedStore` — strips its own prefix, so a caller sees its own keyspace and never the scope:

```python
    async def keys(self, prefix: str = "") -> list[str]:
        _check_key(prefix)
        n = len(self._prefix)
        return [k[n:] for k in await self._inner.keys(self._prefix + prefix)]
```

Add to `tests/test_store.py`:

```python
async def test_keys_lists_under_a_prefix(store):
    await store.set("a:1", "x"); await store.set("a:2", "y"); await store.set("b:1", "z")
    assert sorted(await store.keys("a:")) == ["a:1", "a:2"]
    assert sorted(await store.keys()) == ["a:1", "a:2", "b:1"]


async def test_keys_omits_expired(store):
    await store.set("k", "v", ttl=60.0)
    store.clock.t += 61.0
    assert await store.keys() == []


async def test_keys_treats_sql_wildcards_literally(store):
    """A key containing % or _ must not widen the match."""
    await store.set("a%b", "1"); await store.set("axb", "2")
    assert await store.keys("a%") == ["a%b"]


async def test_scoped_keys_strip_the_scope():
    """A role asks for keys and gets its own — never the scope prefix."""
    inner = MemoryStore()
    a = ScopedStore(inner, "actuator/kv/")
    await a.set("draft", "x")
    await inner.set("decider/other/secret", "y")
    assert await a.keys() == ["draft"]          # not "actuator/kv/draft", not the sibling
```

- [ ] **Step 5: Write the kv actuator**

Create `switchboard/actuators/kv.py`:

```python
"""The agent's memory, as an actuator.

One actuator with one scope, dispatching on `op`. Two actuators (kv.get and
kv.set) would each get their own `actuator/<name>/` scope and could never see
the other's writes — a memory that cannot remember.

It carries no tool_spec: the agent reaches this only through the memory tools
its decider injects, which prefix keys per session. It never addresses kv
directly.
"""

OPS = ("get", "set", "delete", "list")
LIST_MAX = 200          # an agent listing a large memory would blow its own context


class KvActuator:
    name = "kv"
    tool_spec = None          # not agent-callable; reached via decider-injected tools

    def bind(self, ctx) -> None:
        self.ctx = ctx

    async def act(self, cmd, ctx) -> None:
        args = cmd.args or {}
        op, key = args.get("op"), args.get("key")

        if op not in OPS:
            return await ctx.result("error", {"message": f"unknown op: {op!r}"})

        if op == "list":
            prefix = args.get("prefix") or ""
            if not isinstance(prefix, str):
                return await ctx.result("error", {"message": "prefix must be a string"})
            found = await self.ctx.store.keys(prefix)
            # Capped here, not in the store: the store is a primitive, the
            # actuator carries the policy that protects the caller.
            return await ctx.result("ok", {"keys": found[:LIST_MAX],
                                           "truncated": len(found) > LIST_MAX})

        if not isinstance(key, str):
            return await ctx.result("error", {"message": "key must be a string"})

        if op == "get":
            return await ctx.result("ok", {"value": await self.ctx.store.get(key)})

        if op == "delete":
            await self.ctx.store.delete(key)
            return await ctx.result("ok", {})

        value = args.get("value")
        if not isinstance(value, str):
            # Reported, not raised: the actuator understands this failure, and
            # raising would burn the retry cycle before the caller learned of it.
            return await ctx.result("error", {"message": "value must be a string"})
        await self.ctx.store.set(key, value, ttl=args.get("ttl"))
        await ctx.result("ok", {})
```

- [ ] **Step 6: Run to verify pass**

Run: `./scripts/dev.sh test tests/test_actuator_kv.py tests/test_store.py -q`
Expected: PASS — 11 kv tests, and the store suite grows by 4 parametrized/standalone tests.

- [ ] **Step 7: Full suite**

Run: `./scripts/dev.sh test`
Expected: PASS. Adding `tool_spec` to the protocol breaks nothing: `Protocol` attributes are structural and the existing actuators simply do not declare it. Adding `keys` to the protocol likewise breaks nothing, because all three implementations gain it in this task.

- [ ] **Step 8: Commit**

```bash
git add switchboard/message.py switchboard/store.py switchboard/actuators/kv.py \
        tests/test_actuator_kv.py tests/test_store.py
git commit -m "feat: keys() on the store contract, tool_spec opt-in, kv actuator"
```

---

### Task 2: the `llm` actuator

**Files:**
- Create: `switchboard/actuators/llm.py`
- Test: `tests/test_actuator_llm.py`

**Interfaces:**
- Consumes: `ActuatorCtx`, `ActCtx`, `Command`; `httpx`.
- Produces: `LlmActuator(api_key, *, model="claude-sonnet-5", client=None)` with `name = "llm"`, consuming `{system, messages, tools, model?, max_tokens?}` and emitting `llm.ok` with `{stop_reason, content, usage}` or `llm.error` with `{message, status}`. Phase 4's decider emits these commands.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_actuator_llm.py`:

```python
import json

import httpx

from switchboard.actuators.llm import LlmActuator, ANTHROPIC_URL
from switchboard.message import ActCtx, ActuatorCtx, Command
from switchboard.store import MemoryStore


def _cmd(args):
    class M:
        id = 3
        payload = args
        metadata = {"name": "llm", "observation_id": 9}
    return Command.from_message(M())


async def _run(act, args):
    results = []
    async def emit_result(name, payload, cmd_id):
        results.append((name, payload)); return 0
    cmd = _cmd(args)
    await act.act(cmd, ActCtx(cmd=cmd, _emit_result=emit_result))
    return results[0]


def _bound(handler):
    a = LlmActuator("sk-test", client=httpx.AsyncClient(
        transport=httpx.MockTransport(handler)))
    a.bind(ActuatorCtx(store=MemoryStore()))
    return a


async def test_completion_is_returned_as_a_result():
    seen = {}
    def h(req):
        seen["url"] = str(req.url)
        seen["body"] = json.loads(req.content)
        seen["key"] = req.headers.get("x-api-key")
        seen["version"] = req.headers.get("anthropic-version")
        return httpx.Response(200, json={
            "stop_reason": "tool_use",
            "content": [{"type": "tool_use", "id": "tu_1", "name": "web_search",
                         "input": {"query": "x"}}],
            "usage": {"input_tokens": 12, "output_tokens": 3},
        })
    a = _bound(h)
    name, payload = await _run(a, {
        "system": "be brief", "messages": [{"role": "user", "content": "hi"}],
        "tools": [{"name": "web_search"}], "max_tokens": 64,
    })
    assert seen["url"] == ANTHROPIC_URL
    assert seen["key"] == "sk-test"
    assert seen["version"]                        # version header is required
    assert seen["body"]["system"] == "be brief"
    assert seen["body"]["tools"] == [{"name": "web_search"}]
    assert name == "llm.ok"
    assert payload["stop_reason"] == "tool_use"
    assert payload["content"][0]["id"] == "tu_1"
    assert payload["usage"]["input_tokens"] == 12


async def test_the_decider_chooses_the_model():
    """Model, system and tools all travel in the command: the actuator is a dumb
    executor, and any decider could drive it."""
    seen = {}
    def h(req):
        seen["body"] = json.loads(req.content)
        return httpx.Response(200, json={"stop_reason": "end_turn", "content": [],
                                         "usage": {}})
    a = _bound(h)
    await _run(a, {"messages": [], "model": "claude-opus-4-8"})
    assert seen["body"]["model"] == "claude-opus-4-8"


async def test_default_model_is_used_when_the_command_omits_it():
    seen = {}
    def h(req):
        seen["body"] = json.loads(req.content)
        return httpx.Response(200, json={"stop_reason": "end_turn", "content": [],
                                         "usage": {}})
    a = _bound(h)
    await _run(a, {"messages": []})
    assert seen["body"]["model"] == "claude-sonnet-5"


async def test_api_error_is_an_error_result_not_a_raise():
    """A 4xx is a failure the actuator understands. Reporting it lets the caller
    react immediately; raising would burn the retry cycle first."""
    def h(req):
        return httpx.Response(400, json={"error": {"message": "bad request"}})
    a = _bound(h)
    name, payload = await _run(a, {"messages": []})
    assert name == "llm.error"
    assert payload["status"] == 400
    assert "bad request" in payload["message"]


async def test_server_error_is_also_reported():
    def h(req):
        return httpx.Response(529, json={"error": {"message": "overloaded"}})
    a = _bound(h)
    name, payload = await _run(a, {"messages": []})
    assert name == "llm.error"
    assert payload["status"] == 529


async def test_llm_declares_no_tool_spec():
    """The decider emits llm commands directly; the agent never calls the model
    as a tool."""
    assert getattr(LlmActuator, "tool_spec", None) is None
```

- [ ] **Step 2: Run to verify failure**

Run: `./scripts/dev.sh test tests/test_actuator_llm.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'switchboard.actuators.llm'`

- [ ] **Step 3: Write the llm actuator**

Create `switchboard/actuators/llm.py`:

```python
"""The model, as an actuator.

Deliberately generic: it takes messages and tools and returns a completion, and
knows nothing about agents. Any decider could emit `llm` commands. Everything
about the request — system prompt, model, tools, limits — travels in the command
args, because the decider owns those choices.

Raw httpx rather than the anthropic SDK: this matches how DiscordSender talks to
Discord, adds no dependency, and the Messages API is a single POST.
"""
import httpx

ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"
ANTHROPIC_VERSION = "2023-06-01"
DEFAULT_MODEL = "claude-sonnet-5"
DEFAULT_MAX_TOKENS = 4096
TIMEOUT = 120.0                 # a long completion with tools is not fast


class LlmActuator:
    name = "llm"
    tool_spec = None            # the decider emits this directly; not a tool

    def __init__(self, api_key: str, *, model: str = DEFAULT_MODEL, client=None):
        self._key = api_key
        self._model = model
        self._client = client
        self._http = None

    def bind(self, ctx) -> None:
        self.ctx = ctx
        # Built here, not in __init__: an httpx client wants a running loop, and
        # bind() runs inside Bus.start().
        self._http = self._client or httpx.AsyncClient(timeout=TIMEOUT)

    async def act(self, cmd, ctx) -> None:
        args = cmd.args or {}
        body = {
            "model": args.get("model") or self._model,
            "max_tokens": args.get("max_tokens") or DEFAULT_MAX_TOKENS,
            "messages": args.get("messages") or [],
        }
        if args.get("system"):
            body["system"] = args["system"]
        if args.get("tools"):
            body["tools"] = args["tools"]

        resp = await self._http.post(ANTHROPIC_URL, json=body, headers={
            "x-api-key": self._key,
            "anthropic-version": ANTHROPIC_VERSION,
            "content-type": "application/json",
        })

        if resp.status_code >= 400:
            # Reported, not raised: the caller learns immediately instead of
            # after the full retry-backoff cycle.
            try:
                message = resp.json().get("error", {}).get("message", resp.text)
            except ValueError:
                message = resp.text
            return await ctx.result("error", {"status": resp.status_code,
                                              "message": message})

        data = resp.json()
        await ctx.result("ok", {
            "stop_reason": data.get("stop_reason"),
            "content": data.get("content", []),
            "usage": data.get("usage", {}),
        })

    async def close(self) -> None:
        if self._http is not None:
            await self._http.aclose()
```

- [ ] **Step 4: Run to verify pass**

Run: `./scripts/dev.sh test tests/test_actuator_llm.py -q`
Expected: PASS, 6 tests.

- [ ] **Step 5: Full suite**

Run: `./scripts/dev.sh test`
Expected: PASS — previous total + 6.

- [ ] **Step 6: Commit**

```bash
git add switchboard/actuators/llm.py tests/test_actuator_llm.py
git commit -m "feat(actuators): generic llm executor over the Messages API"
```

---

### Task 3: give `DiscordPost` a voice, named destinations, and a tool_spec

**Files:**
- Modify: `switchboard/actuators/discord.py` — `DiscordPost.act`
- Test: `tests/test_actuators_discord.py`

**Interfaces:**
- Consumes: `DiscordSender.send(channel_id, content=None, *, embed=None, components=None)` — already accepts `content`; `DiscordPost.act` simply never passed it.
- Produces: `DiscordPost(bot_token, application_id, *, channels=None, channel_id=None, client=None)` accepting `{content?, channel?, channel_id?, embed?, components?}`, returning `{channel_id, message_id}`, and declaring an **instance** `tool_spec` exposing `{content, channel}` where `channel` is an **enum of configured names**. Phase 4's decider reads the spec off the instance.

**Why names and not ids.** Letting the model emit a raw channel id would be more capable and materially less safe. The agent reads Discord messages — untrusted input — so free choice of destination turns a prompt-injection from *"the agent says something odd in its own thread"* into *"the agent broadcasts its memory anywhere the bot can reach."* A hallucinated snowflake is the lesser worry (it usually 404s); the exfiltration path is the real one. An enum of configured names makes a bad destination structurally unrepresentable, keeps the useful capability ("post that to #releases"), and lets the agent's memory hold semantics rather than brittle ids. `tool_spec` therefore has to be an **instance** attribute, since the enum comes from config.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_actuators_discord.py`:

```python
async def test_discord_post_sends_plain_content():
    """The agent speaks in plain text; the existing actuator only ever forwarded
    embeds."""
    seen = {}
    def h(req):
        seen["body"] = json.loads(req.content)
        return httpx.Response(200, json={"id": "m-7"})
    a = _bind(DiscordPost("bot", "app", channel_id="chan-9", client=_client(h)))
    results = []
    ctx = ActCtx(cmd=_cmd("discord.post", {"content": "hello there"}),
                 _emit_result=await _recorder(results))
    await a.act(ctx.cmd, ctx)
    assert seen["body"]["content"] == "hello there"


async def test_discord_post_returns_the_message_id():
    """The decider needs the id to reference the message later."""
    def h(req):
        return httpx.Response(200, json={"id": "m-7"})
    a = _bind(DiscordPost("bot", "app", channel_id="chan-9", client=_client(h)))
    results = []
    ctx = ActCtx(cmd=_cmd("discord.post", {"content": "hi"}),
                 _emit_result=await _recorder(results))
    await a.act(ctx.cmd, ctx)
    assert results[0][1] == {"channel_id": "chan-9", "message_id": "m-7"}


async def test_tool_spec_offers_only_configured_channel_names():
    """A raw id would let a prompt-injected agent broadcast anywhere the bot can
    reach. An enum of configured names makes that unrepresentable."""
    a = DiscordPost("bot", "app", channels={"releases": "111", "alerts": "222"})
    props = a.tool_spec["input_schema"]["properties"]
    assert set(props) == {"content", "channel"}
    assert sorted(props["channel"]["enum"]) == ["alerts", "releases"]
    assert "channel_id" not in props           # never an id


async def test_named_channel_resolves_to_its_id():
    seen = {}
    def h(req):
        seen["url"] = str(req.url)
        return httpx.Response(200, json={"id": "m-1"})
    a = _bind(DiscordPost("bot", "app", channels={"releases": "111"},
                          channel_id="chan-9", client=_client(h)))
    results = []
    ctx = ActCtx(cmd=_cmd("discord.post", {"content": "ship it", "channel": "releases"}),
                 _emit_result=await _recorder(results))
    await a.act(ctx.cmd, ctx)
    assert seen["url"].endswith("/channels/111/messages")


async def test_unknown_channel_name_is_an_error_not_a_post():
    posted = []
    def h(req):
        posted.append(1); return httpx.Response(200, json={"id": "m-1"})
    a = _bind(DiscordPost("bot", "app", channels={"releases": "111"},
                          channel_id="chan-9", client=_client(h)))
    results = []
    ctx = ActCtx(cmd=_cmd("discord.post", {"content": "x", "channel": "general"}),
                 _emit_result=await _recorder(results))
    await a.act(ctx.cmd, ctx)
    assert posted == []                        # nothing was sent
    assert results[0][0] == "discord.post.error"


async def test_omitting_channel_uses_the_injected_destination():
    """No name given → the conversation the decider routed it to."""
    seen = {}
    def h(req):
        seen["url"] = str(req.url); return httpx.Response(200, json={"id": "m-1"})
    a = _bind(DiscordPost("bot", "app", channels={"releases": "111"},
                          channel_id="chan-9", client=_client(h)))
    results = []
    ctx = ActCtx(cmd=_cmd("discord.post", {"content": "hi"}),
                 _emit_result=await _recorder(results))
    await a.act(ctx.cmd, ctx)
    assert seen["url"].endswith("/channels/chan-9/messages")
```

- [ ] **Step 2: Run to verify failure**

Run: `./scripts/dev.sh test tests/test_actuators_discord.py -q`
Expected: FAIL — the body has no `content` key, the result lacks `message_id`, `DiscordPost` takes no `channels=`, and no `tool_spec` exists.

- [ ] **Step 3: Update `DiscordPost`**

In `switchboard/actuators/discord.py`, replace the `DiscordPost` class header and `act` with:

```python
class DiscordPost:
    """Actuator for the `discord.post` command: post a channel or thread message.

    The tool offers channel *names* from a configured allowlist, never raw ids.
    An agent reads untrusted messages, so free choice of destination would turn
    a prompt injection from "says something odd in its own thread" into "can
    broadcast its memory anywhere the bot can reach". An enum makes a bad
    destination unrepresentable, and lets the agent remember semantics rather
    than brittle snowflakes. Omit the name and the message goes wherever the
    decider routed the conversation.
    """
    name = "discord.post"

    def __init__(self, bot_token, application_id, *, channels=None,
                 channel_id=None, client=None):
        self._token, self._app_id = bot_token, application_id
        self._channels = dict(channels or {})
        self._default_channel = channel_id
        self._client = client
        self._sender = None
        # An instance attribute, not a class one: the enum comes from config.
        props = {"content": {"type": "string", "description": "The message text."}}
        if self._channels:
            props["channel"] = {
                "type": "string", "enum": sorted(self._channels),
                "description": "Where to post. Omit for the current conversation.",
            }
        self.tool_spec = {
            "description": "Send a message to Discord.",
            "input_schema": {"type": "object", "properties": props,
                             "required": ["content"]},
        }

    def bind(self, ctx):
        self.ctx = ctx
        self._sender = DiscordSender(self._token, self._app_id, client=self._client)

    async def act(self, cmd, ctx):
        named = cmd.args.get("channel")
        if named is not None:
            if named not in self._channels:
                # Reported, not raised, and nothing is sent.
                return await ctx.result("error",
                                        {"message": f"unknown channel: {named!r}"})
            channel = self._channels[named]
        else:
            channel = cmd.args.get("channel_id") or self._default_channel
        resp = await self._sender.send(channel,
                                       content=cmd.args.get("content"),
                                       embed=cmd.args.get("embed"),
                                       components=cmd.args.get("components"))
        message_id = None
        try:
            message_id = resp.json().get("id")
        except ValueError:
            pass
        await ctx.result("ok", {"channel_id": channel, "message_id": message_id})
```

Leave `close()` and the rest of the file untouched.

- [ ] **Step 4: Run to verify pass**

Run: `./scripts/dev.sh test tests/test_actuators_discord.py -q`
Expected: PASS — the existing embed tests still pass (they assert `seen["body"]["embeds"]`, and `content=None` is omitted from the payload by `DiscordSender.send`), plus 6 new.

- [ ] **Step 5: Full suite**

Run: `./scripts/dev.sh test`
Expected: PASS — previous total + 6.

- [ ] **Step 6: Commit**

```bash
git add switchboard/actuators/discord.py tests/test_actuators_discord.py
git commit -m "feat(actuators): discord.post gains content, named destinations, message_id, tool_spec"
```

---

## Self-Review

**Spec coverage (§7):**
- §7.1 `tool_spec` opt-in, tool name == actuator name == command name → Task 1. ✓
- §7.2 result → tool_result outcomes (`ok` / `error`), errors reported not raised → Tasks 1, 2. ✓
- §7.3 `llm` generic with request shape in the command args; `kv` one actuator one scope, no `tool_spec` on either → Tasks 1, 2. ✓
- §7.4 destination control → Task 3, **tightened**: the tool offers configured channel *names* (enum), never raw ids, and omitting the name falls back to the decider-routed conversation. ✓
- §3 `actuator/kv/` scope holds the agent's memory → Task 1, pinned by `test_writes_land_in_the_actuators_own_scope`. ✓
- §7.5 curated tool list and trusted binding is Phase 4 wiring, not an actuator concern. Out of scope here by design. ✓

**Placeholder scan:** No TBD/TODO; every code step shows complete code; every test asserts real values. ✓

**Type consistency:** `ActCtx(cmd=…, _emit_result=…)` matches the current two-field dataclass (`context` was removed in the sensor-platform work). `ActuatorCtx(store=…)` is store-only. `DiscordSender.send(channel_id, content=None, *, embed=None, components=None)` matches the existing signature. `ctx.result(outcome, payload)` produces `f"{cmd.name}.{outcome}"`, so `kv.ok` / `kv.error` / `llm.ok` / `llm.error` follow from the actuator names. ✓

**Two things for the reviewer:**
1. Task 3 changes `DiscordPost.act` to pass `content` through and to read `message_id` from the response, and moves `tool_spec` to an instance attribute. The existing embed tests assert on `seen["body"]["embeds"]` and should be unaffected, but the reviewer should confirm no existing test asserts the exact shape of the `ok` payload (it gains `message_id`), and that `test_app.py` still constructs `DiscordPost` correctly now that it takes `channels=`.
2. Nothing here is wired into `app.build()`. These actuators are built and tested but unregistered — deliberate, since the agent that drives them arrives in Phase 4. Registering `llm`/`kv` earlier would mean an API key in the deployment for a feature nothing uses.

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-07-24-agent-phase2-actuators.md`. Two execution options:

**1. Subagent-Driven (recommended)** — a fresh subagent per task, spec+quality review between tasks, fast iteration.

**2. Inline Execution** — execute the three tasks in this session with checkpoints.

Which approach?
