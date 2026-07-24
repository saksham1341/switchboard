# LLM Backends Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** One `llm` actuator, several provider backends. The actuator owns the contract and the error discipline; each backend translates our canonical request into its provider's native shape and back.

**Architecture:** `LlmActuator(backend=...)` — the actuator is thin (call `complete`, emit `ok`, catch `LlmError` and emit `error`, let anything else propagate so the Bus retries). All provider knowledge lives behind the `LlmBackend` protocol. The canonical wire format is **Anthropic-shaped**, because it is the superset: assistant `content` is an ordered list of blocks, so interleaved text and `tool_use` keep their order, and N ordered `tool_result` blocks ride in one user turn. OpenAI's flat `content` + parallel `tool_calls` cannot represent that ordering, so translating ours→theirs is a well-defined flattening while the reverse would lose information. Picking the richer shape means data is only ever discarded at the edge that cannot represent it, never in our own format.

**Tech Stack:** Python 3.11+, raw `httpx` (no provider SDKs — every backend here is a single POST), pytest with `asyncio_mode = "auto"`.

## Global Constraints

- Spec is `docs/superpowers/specs/2026-07-23-agentic-decider-design.md` §7.3 (the `llm` actuator as a generic executor).
- **The decider and its tests must not change in shape.** `llm.ok` stays `{stop_reason, content, usage}`. The only decider change is that `model` is now always sent.
- **`model` is required.** The decider always supplies it; a backend never silently defaults. A missing model is a *reported* error, never a guess — the command in the log must say which model actually ran.
- Actuators report understood failures via `ctx.result("error", ...)` and **never raise** for them. Raising is reserved for failures worth a retry cycle (network, 5xx).
- Never trust the shape of a parsed body: `isinstance` guard before `.get()` or iteration, and `except Exception` (not `ValueError`) around `.json()`. This defect class has landed four times in this project.
- Run the suite with `source venv/bin/activate && pytest -q` from the repo root (note `venv/`, **not** `.venv/`, which is empty). Baseline is 304 passing.

## Canonical format (ours)

Request — the `llm` command's args:

```python
{
  "model": "llama-3.3-70b-versatile",     # REQUIRED
  "system": "...",                         # optional str
  "messages": [...],                       # Anthropic Messages shape
  "tools": [{"name", "description", "input_schema"}],   # optional
  "max_tokens": 4096,                      # optional
}
```

Result — the `llm.ok` payload:

```python
{"stop_reason": str | None, "content": [block, ...], "usage": {"input_tokens": int, "output_tokens": int}}
```

## File Structure

| file | responsibility |
|---|---|
| `switchboard/actuators/llm/__init__.py` (create) | exports `LlmActuator`, `LlmError`, `LlmBackend` |
| `switchboard/actuators/llm/actuator.py` (create) | the thin actuator + `LlmError` + the `LlmBackend` protocol |
| `switchboard/actuators/llm/backends/anthropic.py` (create) | passthrough backend (moved from the old module) |
| `switchboard/actuators/llm/backends/openai.py` (create) | the translation |
| `switchboard/actuators/llm.py` (delete) | replaced by the package |
| `switchboard/deciders/agent/decider.py` (modify) | always send `model` |
| `switchboard/app.py` (modify) | backend selection |
| `tests/test_actuator_llm.py` (modify) | retarget at the package |
| `tests/test_llm_openai.py` (create) | translation tests |

---

### Task 1: The actuator, the protocol, and the Anthropic backend

**Files:**
- Create: `switchboard/actuators/llm/__init__.py`, `actuator.py`, `backends/__init__.py`, `backends/anthropic.py`
- Delete: `switchboard/actuators/llm.py`
- Test: `tests/test_actuator_llm.py`

**Interfaces produced** (used by Tasks 2–3):

```python
class LlmError(Exception):
    def __init__(self, message: str, status: int | None = None): ...

class LlmBackend(Protocol):
    name: str
    async def complete(self, req: dict) -> dict: ...
    async def close(self) -> None: ...

class LlmActuator:
    name = "llm"
    tool_spec = None
    def __init__(self, backend: LlmBackend): ...

class AnthropicBackend:
    name = "anthropic"
    def __init__(self, api_key: str, *, client=None): ...
```

- [ ] **Step 1: Read the existing module first**

Run: `cat switchboard/actuators/llm.py && cat tests/test_actuator_llm.py`

The existing `_error_message` is shape-defensive and battle-tested (it was an Important review fix). **Move it into the Anthropic backend verbatim** — do not rewrite it.

- [ ] **Step 2: Write the failing tests**

Rewrite `tests/test_actuator_llm.py`. Keep every existing behavioural test, retargeted at the new import path, and add these:

```python
import httpx
import pytest

from switchboard.actuators.llm import LlmActuator, LlmError
from switchboard.actuators.llm.backends.anthropic import AnthropicBackend
from switchboard.message import ActCtx, Command


def _cmd(args):
    class M:
        id = 1
        payload = args
        metadata = {"name": "llm", "observation_id": 7}
    return Command.from_message(M())


async def _run(act, args):
    results = []
    async def emit_result(name, payload, cmd_id):
        results.append((name, payload)); return 0
    cmd = _cmd(args)
    await act.act(cmd, ActCtx(cmd=cmd, _emit_result=emit_result))
    return results[0]


class _Backend:
    """A backend that records its request and returns whatever it was given."""
    name = "fake"
    def __init__(self, result=None, raises=None):
        self.seen = None
        self._result = result or {"stop_reason": "end_turn", "content": [], "usage": {}}
        self._raises = raises
    async def complete(self, req):
        self.seen = req
        if self._raises:
            raise self._raises
        return self._result
    async def close(self):
        self.closed = True


def _act(backend):
    a = LlmActuator(backend)
    a.bind(object())
    return a


async def test_the_backend_result_becomes_the_ok_payload():
    b = _Backend({"stop_reason": "tool_use", "content": [{"type": "text"}],
                  "usage": {"input_tokens": 1, "output_tokens": 2}})
    name, payload = await _run(_act(b), {"model": "m", "messages": []})
    assert name == "llm.ok"
    assert payload["stop_reason"] == "tool_use"
    assert payload["usage"]["input_tokens"] == 1


async def test_the_actuator_passes_the_args_through_untouched():
    b = _Backend()
    args = {"model": "m", "messages": [{"role": "user", "content": "hi"}],
            "tools": [{"name": "t"}], "system": "s", "max_tokens": 10}
    await _run(_act(b), args)
    assert b.seen == args


async def test_an_llm_error_is_reported_not_raised():
    b = _Backend(raises=LlmError("overloaded", status=529))
    name, payload = await _run(_act(b), {"model": "m", "messages": []})
    assert name == "llm.error"
    assert payload["message"] == "overloaded" and payload["status"] == 529


async def test_any_other_exception_propagates_so_the_bus_retries():
    b = _Backend(raises=httpx.ConnectTimeout("boom"))
    with pytest.raises(httpx.ConnectTimeout):
        await _run(_act(b), {"model": "m", "messages": []})


async def test_a_missing_model_is_a_reported_error_and_never_reaches_the_backend():
    # The command in the log must say which model ran. A backend that silently
    # defaults would make an old command unreplayable the moment the default
    # changed.
    b = _Backend()
    name, payload = await _run(_act(b), {"messages": []})
    assert name == "llm.error"
    assert "model" in payload["message"]
    assert b.seen is None


async def test_a_non_string_model_is_a_reported_error():
    b = _Backend()
    name, _ = await _run(_act(b), {"model": 7, "messages": []})
    assert name == "llm.error"
    assert b.seen is None


async def test_close_closes_the_backend():
    b = _Backend()
    a = _act(b)
    await a.close()
    assert b.closed is True


# --- the Anthropic backend: a passthrough ------------------------------------

def _anthropic(handler):
    return AnthropicBackend("key", client=httpx.AsyncClient(
        transport=httpx.MockTransport(handler)))


async def test_anthropic_sends_the_canonical_request_natively():
    seen = {}
    def handler(request):
        import json as _j
        seen.update(_j.loads(request.content))
        seen["_auth"] = request.headers.get("x-api-key")
        return httpx.Response(200, json={"stop_reason": "end_turn",
                                         "content": [], "usage": {}})
    b = _anthropic(handler)
    await b.complete({"model": "claude-sonnet-5", "system": "s",
                      "messages": [{"role": "user", "content": "hi"}],
                      "tools": [{"name": "t", "description": "d",
                                 "input_schema": {"type": "object"}}]})
    assert seen["model"] == "claude-sonnet-5"
    assert seen["system"] == "s"
    assert seen["tools"][0]["name"] == "t"          # no translation at all
    assert seen["_auth"] == "key"
    await b.close()


async def test_anthropic_maps_the_response_straight_through():
    def handler(request):
        return httpx.Response(200, json={
            "stop_reason": "tool_use",
            "content": [{"type": "tool_use", "id": "t1", "name": "x", "input": {}}],
            "usage": {"input_tokens": 3, "output_tokens": 4}})
    b = _anthropic(handler)
    out = await b.complete({"model": "m", "messages": []})
    assert out["content"][0]["id"] == "t1"
    assert out["usage"] == {"input_tokens": 3, "output_tokens": 4}
    await b.close()


async def test_anthropic_4xx_raises_llm_error_with_the_api_message():
    def handler(request):
        return httpx.Response(400, json={"error": {"message": "bad request"}})
    b = _anthropic(handler)
    with pytest.raises(LlmError) as e:
        await b.complete({"model": "m", "messages": []})
    assert "bad request" in str(e.value) and e.value.status == 400
    await b.close()


async def test_anthropic_survives_an_error_body_that_is_not_an_object():
    # The defect class that has landed four times: a non-object body must not
    # turn a reported failure into a raised AttributeError.
    def handler(request):
        return httpx.Response(400, json=["nope"])
    b = _anthropic(handler)
    with pytest.raises(LlmError):
        await b.complete({"model": "m", "messages": []})
    await b.close()
```

- [ ] **Step 3: Run to verify they fail**

Run: `source venv/bin/activate && pytest tests/test_actuator_llm.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'switchboard.actuators.llm.backends'`.

- [ ] **Step 4: Implement the actuator**

Create `switchboard/actuators/llm/actuator.py`:

```python
"""The model, as an actuator — one actuator, many provider backends.

Deliberately generic: it takes messages and tools and returns a completion, and
knows nothing about agents. Any decider could emit `llm` commands.

The actuator owns the contract and the error discipline; a backend owns one
provider's wire format. That split is what lets a provider change without the
decider, the logs, or a single test moving.
"""
import logging
from typing import Protocol, runtime_checkable

logger = logging.getLogger(__name__)


class LlmError(Exception):
    """A failure the provider explained and a retry will not fix.

    Raised by a backend, reported by the actuator. Anything else a backend
    raises (a timeout, a 5xx, a socket error) propagates so the Bus retries
    with backoff.
    """

    def __init__(self, message: str, status: int | None = None):
        super().__init__(message)
        self.status = status


@runtime_checkable
class LlmBackend(Protocol):
    name: str
    async def complete(self, req: dict) -> dict: ...
    async def close(self) -> None: ...


class LlmActuator:
    name = "llm"
    tool_spec = None            # the decider emits this directly; not a tool

    def __init__(self, backend: LlmBackend):
        self._backend = backend

    def bind(self, ctx) -> None:
        self.ctx = ctx

    async def act(self, cmd, ctx) -> None:
        args = cmd.args or {}
        model = args.get("model")
        if not isinstance(model, str) or not model:
            # Reported, not defaulted. The command in the log is the record of
            # what ran; a backend that quietly substituted its own default
            # would make that record a lie the moment the default changed.
            return await ctx.result("error", {"message": "model is required",
                                              "status": None})
        try:
            result = await self._backend.complete(args)
        except LlmError as e:
            return await ctx.result("error", {"message": str(e), "status": e.status})
        await ctx.result("ok", result)

    async def close(self) -> None:
        await self._backend.close()
```

Create `switchboard/actuators/llm/__init__.py`:

```python
from switchboard.actuators.llm.actuator import LlmActuator, LlmBackend, LlmError

__all__ = ["LlmActuator", "LlmBackend", "LlmError"]
```

Create `switchboard/actuators/llm/backends/__init__.py` as an empty file.

- [ ] **Step 5: Implement the Anthropic backend**

Create `switchboard/actuators/llm/backends/anthropic.py`. The canonical format *is* Anthropic's, so this is a passthrough — build the body, POST, return the three fields.

```python
"""The Anthropic Messages API. A passthrough, because the canonical format is
this one — see the plan's Architecture note on why the superset was chosen as
canonical rather than a neutral shape nobody speaks.

Raw httpx rather than the anthropic SDK: this matches how DiscordSender talks
to Discord, adds no dependency, and the Messages API is a single POST.
"""
import httpx

from switchboard.actuators.llm.actuator import LlmError

ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"
ANTHROPIC_VERSION = "2023-06-01"
DEFAULT_MAX_TOKENS = 4096
TIMEOUT = 120.0                 # a long completion with tools is not fast


def _error_message(resp) -> str:
    """Never trust the shape of an error body. The API returns
    {"error": {"message": ...}}, but a gateway in front of it is under no such
    obligation, and a failure we were meant to report must not become one we
    raise."""
    try:
        body = resp.json()
    except Exception:
        return resp.text
    if isinstance(body, dict):
        err = body.get("error")
        if isinstance(err, dict) and isinstance(err.get("message"), str):
            return err["message"]
        if isinstance(err, str):
            return err
    return resp.text


class AnthropicBackend:
    name = "anthropic"

    def __init__(self, api_key: str, *, client=None):
        self._key = api_key
        self._http = client or httpx.AsyncClient(timeout=TIMEOUT)

    async def complete(self, req: dict) -> dict:
        body = {"model": req["model"],
                "max_tokens": req.get("max_tokens") or DEFAULT_MAX_TOKENS,
                "messages": req.get("messages") or []}
        if req.get("system"):
            body["system"] = req["system"]
        if req.get("tools"):
            body["tools"] = req["tools"]

        resp = await self._http.post(ANTHROPIC_URL, json=body, headers={
            "x-api-key": self._key,
            "anthropic-version": ANTHROPIC_VERSION,
            "content-type": "application/json",
        })
        if resp.status_code >= 500:
            resp.raise_for_status()      # transient: let the Bus retry
        if resp.status_code >= 400:
            raise LlmError(_error_message(resp), status=resp.status_code)

        try:
            data = resp.json()
        except Exception:
            raise LlmError("unreadable response body", status=resp.status_code)
        if not isinstance(data, dict):
            raise LlmError("unreadable response body", status=resp.status_code)
        content = data.get("content")
        usage = data.get("usage")
        return {"stop_reason": data.get("stop_reason"),
                "content": content if isinstance(content, list) else [],
                "usage": usage if isinstance(usage, dict) else {}}

    async def close(self) -> None:
        await self._http.aclose()
```

Delete the old module: `git rm switchboard/actuators/llm.py`

- [ ] **Step 6: Fix the import in `app.py`**

`app.py` imports `from switchboard.actuators.llm import LlmActuator`. That still resolves to the package, so only the construction changes — Task 3 handles selection. For now, keep it compiling:

```python
from switchboard.actuators.llm import LlmActuator
from switchboard.actuators.llm.backends.anthropic import AnthropicBackend
...
            bus.add_actuator(LlmActuator(AnthropicBackend(config["anthropic_api_key"])))
```

- [ ] **Step 7: Run the full suite**

Run: `source venv/bin/activate && pytest -q`
Expected: PASS. Report the count.

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "refactor: llm actuator gains pluggable provider backends"
```

---

### Task 2: The OpenAI-compatible backend

**Files:**
- Create: `switchboard/actuators/llm/backends/openai.py`
- Test: `tests/test_llm_openai.py`

**Interface produced:**

```python
class OpenAiBackend:
    name = "openai"
    def __init__(self, api_key: str, *, base_url: str, client=None): ...
```

One backend covers Groq, Gemini, Cerebras, OpenRouter, GitHub Models and Ollama — they differ only by `base_url` and model name.

**The translation, both directions.** This is the whole task; get it exactly right.

| ours (Anthropic-shaped) | theirs (OpenAI) |
|---|---|
| `system` (top-level str) | a leading `{"role": "system", "content": ...}` message |
| `tools: [{name, description, input_schema}]` | `[{"type": "function", "function": {"name", "description", "parameters"}}]` |
| user `content` as a plain str | `{"role": "user", "content": str}` |
| assistant `content: [{type: text}, {type: tool_use, id, name, input}]` | `{"role": "assistant", "content": <joined text or None>, "tool_calls": [{"id", "type": "function", "function": {"name", "arguments": <JSON string>}}]}` |
| user `content: [{type: tool_result, tool_use_id, content, is_error}]` × N | **N separate** `{"role": "tool", "tool_call_id", "content"}` messages, in order |
| — | ← `choices[0].message.tool_calls` becomes `[{type: tool_use, id, name, input: <parsed JSON>}]` |
| — | ← `choices[0].message.content` becomes `[{type: "text", "text": ...}]`, placed **before** any tool_use blocks |
| `stop_reason` | `finish_reason`: `tool_calls`→`tool_use`, `stop`→`end_turn`, `length`→`max_tokens`, anything else passes through |
| `usage.input_tokens` / `output_tokens` | `usage.prompt_tokens` / `completion_tokens` |

Two traps:
- **`arguments` is a JSON *string*, not an object** — encode on the way out, decode on the way in. A model can emit malformed JSON here; a decode failure must degrade to `{}` rather than raise, because raising would lose a whole turn.
- **`tool_result` fan-out must preserve order**, and each result needs its `tool_call_id` to match the id we sent.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_llm_openai.py`:

```python
import json

import httpx
import pytest

from switchboard.actuators.llm import LlmError
from switchboard.actuators.llm.backends.openai import OpenAiBackend

BASE = "https://api.groq.com/openai/v1"


def _backend(handler):
    return OpenAiBackend("key", base_url=BASE,
                         client=httpx.AsyncClient(transport=httpx.MockTransport(handler)))


def _capture(response=None):
    """Returns (backend, seen) where seen is filled with the request body."""
    seen = {}
    def handler(request):
        seen.update(json.loads(request.content))
        seen["_auth"] = request.headers.get("authorization")
        seen["_url"] = str(request.url)
        return response or httpx.Response(200, json={
            "choices": [{"finish_reason": "stop",
                         "message": {"role": "assistant", "content": "hi"}}],
            "usage": {"prompt_tokens": 1, "completion_tokens": 2}})
    return _backend(handler), seen


async def test_system_becomes_a_leading_system_message():
    b, seen = _capture()
    await b.complete({"model": "m", "system": "you are x",
                      "messages": [{"role": "user", "content": "hi"}]})
    assert seen["messages"][0] == {"role": "system", "content": "you are x"}
    assert seen["messages"][1]["role"] == "user"
    await b.close()


async def test_no_system_means_no_system_message():
    b, seen = _capture()
    await b.complete({"model": "m", "messages": [{"role": "user", "content": "hi"}]})
    assert seen["messages"][0]["role"] == "user"
    await b.close()


async def test_tools_are_wrapped_in_the_function_envelope():
    b, seen = _capture()
    await b.complete({"model": "m", "messages": [],
                      "tools": [{"name": "discord.post", "description": "d",
                                 "input_schema": {"type": "object",
                                                  "properties": {"a": {"type": "string"}}}}]})
    fn = seen["tools"][0]
    assert fn["type"] == "function"
    assert fn["function"]["name"] == "discord.post"
    assert fn["function"]["parameters"]["properties"]["a"]["type"] == "string"
    await b.close()


async def test_an_assistant_tool_use_becomes_a_tool_call_with_json_string_args():
    b, seen = _capture()
    await b.complete({"model": "m", "messages": [
        {"role": "user", "content": "hi"},
        {"role": "assistant", "content": [
            {"type": "text", "text": "sure"},
            {"type": "tool_use", "id": "t1", "name": "post",
             "input": {"content": "yo"}}]}]})
    a = seen["messages"][-1]
    assert a["role"] == "assistant" and a["content"] == "sure"
    call = a["tool_calls"][0]
    assert call["id"] == "t1" and call["function"]["name"] == "post"
    assert json.loads(call["function"]["arguments"]) == {"content": "yo"}
    await b.close()


async def test_tool_results_fan_out_to_separate_tool_messages_in_order():
    b, seen = _capture()
    await b.complete({"model": "m", "messages": [
        {"role": "user", "content": [
            {"type": "tool_result", "tool_use_id": "t1", "content": "one"},
            {"type": "tool_result", "tool_use_id": "t2", "content": "two"}]}]})
    tools = [m for m in seen["messages"] if m["role"] == "tool"]
    assert [t["tool_call_id"] for t in tools] == ["t1", "t2"]
    assert [t["content"] for t in tools] == ["one", "two"]
    await b.close()


async def test_a_tool_call_in_the_response_becomes_a_tool_use_block():
    def handler(request):
        return httpx.Response(200, json={"choices": [{
            "finish_reason": "tool_calls",
            "message": {"role": "assistant", "content": None,
                        "tool_calls": [{"id": "t9", "type": "function",
                                        "function": {"name": "post",
                                                     "arguments": '{"content":"hey"}'}}]}}],
            "usage": {"prompt_tokens": 5, "completion_tokens": 6}})
    b = _backend(handler)
    out = await b.complete({"model": "m", "messages": []})
    assert out["stop_reason"] == "tool_use"
    block = out["content"][0]
    assert block == {"type": "tool_use", "id": "t9", "name": "post",
                     "input": {"content": "hey"}}
    assert out["usage"] == {"input_tokens": 5, "output_tokens": 6}
    await b.close()


async def test_text_and_tool_calls_both_appear_with_text_first():
    def handler(request):
        return httpx.Response(200, json={"choices": [{
            "finish_reason": "tool_calls",
            "message": {"content": "thinking",
                        "tool_calls": [{"id": "t1", "function":
                                        {"name": "p", "arguments": "{}"}}]}}],
            "usage": {}})
    b = _backend(handler)
    out = await b.complete({"model": "m", "messages": []})
    assert [x["type"] for x in out["content"]] == ["text", "tool_use"]
    await b.close()


async def test_malformed_tool_arguments_degrade_to_an_empty_input():
    # A model can emit invalid JSON here. Raising would lose the whole turn;
    # an empty input lets the tool report its own error back in-band.
    def handler(request):
        return httpx.Response(200, json={"choices": [{
            "finish_reason": "tool_calls",
            "message": {"tool_calls": [{"id": "t1", "function":
                                        {"name": "p", "arguments": "{not json"}}]}}],
            "usage": {}})
    b = _backend(handler)
    out = await b.complete({"model": "m", "messages": []})
    assert out["content"][0]["input"] == {}
    await b.close()


@pytest.mark.parametrize("finish,expected", [
    ("tool_calls", "tool_use"), ("stop", "end_turn"),
    ("length", "max_tokens"), ("weird", "weird")])
async def test_finish_reason_maps_to_stop_reason(finish, expected):
    def handler(request):
        return httpx.Response(200, json={
            "choices": [{"finish_reason": finish, "message": {"content": "x"}}],
            "usage": {}})
    b = _backend(handler)
    out = await b.complete({"model": "m", "messages": []})
    assert out["stop_reason"] == expected
    await b.close()


async def test_the_bearer_token_and_url_are_right():
    b, seen = _capture()
    await b.complete({"model": "m", "messages": []})
    assert seen["_auth"] == "Bearer key"
    assert seen["_url"] == f"{BASE}/chat/completions"
    await b.close()


async def test_4xx_raises_llm_error_with_the_provider_message():
    def handler(request):
        return httpx.Response(429, json={"error": {"message": "rate limited"}})
    b = _backend(handler)
    with pytest.raises(LlmError) as e:
        await b.complete({"model": "m", "messages": []})
    assert "rate limited" in str(e.value) and e.value.status == 429
    await b.close()


async def test_5xx_raises_for_status_so_the_bus_retries():
    def handler(request):
        return httpx.Response(502, text="bad gateway")
    b = _backend(handler)
    with pytest.raises(httpx.HTTPStatusError):
        await b.complete({"model": "m", "messages": []})
    await b.close()


async def test_a_non_object_error_body_still_raises_llm_error():
    def handler(request):
        return httpx.Response(400, json=["nope"])
    b = _backend(handler)
    with pytest.raises(LlmError):
        await b.complete({"model": "m", "messages": []})
    await b.close()


async def test_an_empty_choices_array_is_an_llm_error_not_an_index_error():
    def handler(request):
        return httpx.Response(200, json={"choices": [], "usage": {}})
    b = _backend(handler)
    with pytest.raises(LlmError):
        await b.complete({"model": "m", "messages": []})
    await b.close()
```

- [ ] **Step 2: Run to verify they fail**

Run: `source venv/bin/activate && pytest tests/test_llm_openai.py -q`
Expected: FAIL — `ModuleNotFoundError`.

- [ ] **Step 3: Implement**

Create `switchboard/actuators/llm/backends/openai.py`. Write `_to_openai(req)` and `_from_openai(data)` as separate module-level functions so each is testable and readable, then a thin `complete` that posts between them. Guard every parsed field with `isinstance`. Follow the mapping table in this task's header exactly.

Key details the table implies but does not spell out:
- A message whose `content` is a plain `str` passes through unchanged.
- An assistant message with text blocks and no tool_use gets `content` as the joined text and **no** `tool_calls` key.
- An assistant message with tool_use and no text gets `content: None`.
- `tool_result.content` may be a str; if it is not, `json.dumps` it.
- On the way back, a `tool_calls` entry missing an `id` or a `function.name` is skipped rather than producing a malformed block.

- [ ] **Step 4: Run to verify they pass**

Run: `source venv/bin/activate && pytest tests/test_llm_openai.py -q`
Expected: PASS.

- [ ] **Step 5: Verify the round trip is real**

Write a throwaway script that takes a realistic Anthropic-shaped conversation — user turn, assistant turn with one text block and two `tool_use` blocks, user turn with two `tool_result` blocks — runs `_to_openai`, and prints the result. Confirm by eye that the two tool results became two `tool` messages in the original order, and that both `tool_call_id`s match the ids from the assistant turn. Paste the output in your report. Delete the script afterwards.

- [ ] **Step 6: Run the full suite and commit**

Run: `source venv/bin/activate && pytest -q`

```bash
git add -A
git commit -m "feat: OpenAI-compatible llm backend"
```

---

### Task 3: Backend selection and the always-sent model

**Files:**
- Modify: `switchboard/deciders/agent/decider.py`, `switchboard/app.py`, `docker-compose.yml`
- Test: `tests/test_agent_decider.py`, `tests/test_app.py`

- [ ] **Step 1: Write the failing tests**

In `tests/test_agent_decider.py`:

```python
async def test_the_llm_command_always_carries_a_model():
    # The command in the log is the record of what ran. A backend default
    # would make that record a lie as soon as the default changed.
    a = _agent(model="llama-3.3-70b-versatile")
    rec = await _deliver(a, _obs("discord.message", _message()))
    _, args, _ = rec.emitted[0]
    assert args["model"] == "llama-3.3-70b-versatile"
```

In `tests/test_app.py`:

```python
def _cfg(tmp_path, port, **kw):
    base = {"mamamia_db_path": str(tmp_path / "mm.db"),
            "switchboard_db_path": str(tmp_path / "sb.db"),
            "github_secret": "s", "port": port,
            "discord_bot_token": "t", "discord_application_id": "1"}
    base.update(kw)
    return base


def test_openai_backend_is_selected_and_carries_its_base_url(tmp_path):
    from switchboard.app import build
    from switchboard.actuators.llm.backends.openai import OpenAiBackend
    bus, _ = build(_cfg(tmp_path, 8151, llm_backend="openai", llm_api_key="k",
                        llm_base_url="https://api.groq.com/openai/v1",
                        llm_model="llama-3.3-70b-versatile"))
    llm = next(a for a in bus._actuators if a.name == "llm")
    assert isinstance(llm._backend, OpenAiBackend)


def test_anthropic_backend_is_selected(tmp_path):
    from switchboard.app import build
    from switchboard.actuators.llm.backends.anthropic import AnthropicBackend
    bus, _ = build(_cfg(tmp_path, 8152, llm_backend="anthropic", llm_api_key="k",
                        llm_model="claude-sonnet-5"))
    llm = next(a for a in bus._actuators if a.name == "llm")
    assert isinstance(llm._backend, AnthropicBackend)


def test_the_agent_gets_the_configured_model(tmp_path):
    from switchboard.app import build
    bus, _ = build(_cfg(tmp_path, 8153, llm_backend="openai", llm_api_key="k",
                        llm_base_url="http://x", llm_model="some-model"))
    agent = next(d for d in bus._deciders if d.name == "agent")
    assert agent._model == "some-model"


def test_no_llm_key_means_no_agent_and_no_llm(tmp_path):
    from switchboard.app import build
    bus, _ = build(_cfg(tmp_path, 8154))
    assert "llm" not in {a.name for a in bus._actuators}
    assert "agent" not in {d.name for d in bus._deciders}


def test_an_unknown_backend_fails_fast(tmp_path):
    # A typo must not silently produce a Switchboard with no agent - that is
    # the failure mode with no error to observe.
    from switchboard.app import build
    with pytest.raises(ValueError):
        build(_cfg(tmp_path, 8155, llm_backend="gpt5", llm_api_key="k",
                   llm_model="m"))


def test_a_missing_model_fails_fast(tmp_path):
    from switchboard.app import build
    with pytest.raises(ValueError):
        build(_cfg(tmp_path, 8156, llm_backend="openai", llm_api_key="k",
                   llm_base_url="http://x"))
```

Add `import pytest` to `tests/test_app.py` if it is not already there.

- [ ] **Step 2: Run to verify they fail**

Run: `source venv/bin/activate && pytest tests/test_app.py tests/test_agent_decider.py -q`

- [ ] **Step 3: Always send the model**

In `switchboard/deciders/agent/decider.py`, `_advance` currently adds `model` only when set. Make it unconditional:

```python
        args = {"system": self._system, "messages": s["messages"],
                "tools": self._tools, "model": self._model}
```

and require it in `__init__`:

```python
    def __init__(self, *, tools, model, system: str | None = None,
                 max_turns: int = MAX_TURNS):
```

`model` is now keyword-required (no default). Fix every construction in the tests — `_agent()` in `tests/test_agent_decider.py` should default it:

```python
def _agent(**kw):
    kw.setdefault("model", "test-model")
    a = AgentDecider(tools=[TOOL], **kw)
```

- [ ] **Step 4: Backend selection in `app.py`**

Replace the `anthropic_api_key` block with backend selection:

```python
def _llm_backend(config):
    """Construct the configured provider backend, or None if no key is set.

    Fails loudly on a bad name or a missing model rather than returning None:
    a typo that silently produced a Switchboard with no agent would be the
    failure mode with no error to observe (spec §7.5).
    """
    key = config.get("llm_api_key")
    if not key:
        return None
    name = (config.get("llm_backend") or "anthropic").lower()
    model = config.get("llm_model")
    if not model:
        raise ValueError("llm_model is required when llm_api_key is set")
    if name == "anthropic":
        return AnthropicBackend(key)
    if name == "openai":
        base = config.get("llm_base_url")
        if not base:
            raise ValueError("llm_base_url is required for the openai backend")
        return OpenAiBackend(key, base_url=base)
    raise ValueError(f"unknown llm_backend: {name!r}")
```

and in `build()`, inside the Discord block:

```python
        backend = _llm_backend(config)
        if backend is not None:
            agent_post = _discord_post()
            bus.add_actuator(LlmActuator(backend))
            bus.add_decider(AgentDecider(
                model=config["llm_model"],
                tools=[agent_post.tool_spec | {"name": agent_post.name},
                       history.tool_spec | {"name": history.name}]))
```

Call `_llm_backend(config)` **before** the Discord check too, so a bad backend name fails fast even without Discord configured — otherwise `test_an_unknown_backend_fails_fast` passes only by accident. Restructure so validation happens once, unconditionally.

In `run()`, replace `anthropic_api_key` with:

```python
        "llm_backend": os.environ.get("SB_LLM_BACKEND", "anthropic"),
        "llm_api_key": os.environ.get("SB_LLM_API_KEY"),
        "llm_base_url": os.environ.get("SB_LLM_BASE_URL"),
        "llm_model": os.environ.get("SB_LLM_MODEL"),
```

- [ ] **Step 5: Update `docker-compose.yml`**

Replace the `ANTHROPIC_API_KEY` block:

```yaml
      # --- Agent ---
      # The agentic decider is wired only when SB_LLM_API_KEY and Discord are
      # both set. SB_LLM_BACKEND picks the provider: "anthropic" (native), or
      # "openai" for any OpenAI-compatible endpoint (Groq, Gemini, Cerebras,
      # OpenRouter, Ollama), which also needs SB_LLM_BASE_URL.
      SB_LLM_BACKEND: ${SB_LLM_BACKEND:-anthropic}
      SB_LLM_API_KEY: ${SB_LLM_API_KEY:-}
      SB_LLM_BASE_URL: ${SB_LLM_BASE_URL:-}
      SB_LLM_MODEL: ${SB_LLM_MODEL:-}
```

- [ ] **Step 6: Run the full suite**

Run: `source venv/bin/activate && pytest -q`
Expected: PASS. Report the count.

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "feat: select the llm backend by config, always send the model"
```

---

## Not in scope

| deferred | why |
|---|---|
| `BedrockBackend` | Additive — auth and URL only, no translation, since Bedrock's Anthropic models use the canonical shape. Add when there is a reason to. |
| Streaming | Nothing consumes tokens incrementally; the decider reacts to a whole result. |
| Per-backend retry policy | The Bus already retries what is retryable. |
| Cost accounting from `usage` | Phase 5 (`MAX_SPEND`). The field is carried through and unused for now. |
