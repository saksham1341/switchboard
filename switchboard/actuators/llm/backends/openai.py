"""The OpenAI Chat Completions wire format. One backend for every provider
that speaks it — Groq, Gemini, Cerebras, OpenRouter, GitHub Models, Ollama —
they differ only by base_url and model name, never by branch here.

Our canonical wire format is Anthropic-shaped on purpose: it is the superset.
An assistant turn's content is an ordered list of blocks (interleaved text and
tool_use keep their order) and N tool_result blocks ride in a single user
turn. OpenAI's format is flatter — a plain content string plus a parallel
tool_calls array, and one separate {"role": "tool"} message per result — so
translating ours -> theirs is a well-defined, lossy flattening. This backend
is where that loss is allowed to happen; nothing upstream of it knows OpenAI
exists.
"""
import json

import httpx

from switchboard.actuators.llm.actuator import (
    TRANSIENT_STATUS, LlmError, parse_retry_after)
from switchboard.errors import RetryableError

DEFAULT_MAX_TOKENS = 4096
# One HTTP call, no tools run here, and max_tokens is capped by the caller —
# so generation is bounded and only provider queueing varies. A short timeout
# with retries also beats a long one: at 60s a request is far more likely dead
# than slow, and a retry gets a fresh connection. The Bus's handler timeout
# (100s) is the backstop above this, deliberately: the inner, specific timeout
# should fire first and produce a meaningful error.
TIMEOUT = 60.0

_FINISH_TO_STOP = {
    "tool_calls": "tool_use",
    "stop": "end_turn",
    "length": "max_tokens",
}


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


def _to_openai(req: dict) -> dict:
    """Anthropic-shaped request -> OpenAI-shaped request body."""
    messages = []
    if req.get("system"):
        messages.append({"role": "system", "content": req["system"]})

    for msg in req.get("messages") or []:
        if not isinstance(msg, dict):
            continue
        role = msg.get("role")
        content = msg.get("content")

        if isinstance(content, str):
            # Plain-string content (user or assistant) passes through
            # unchanged regardless of role.
            messages.append({"role": role, "content": content})
            continue

        if not isinstance(content, list):
            continue

        if role == "assistant":
            texts = []
            tool_calls = []
            for block in content:
                if not isinstance(block, dict):
                    continue
                btype = block.get("type")
                if btype == "text" and isinstance(block.get("text"), str):
                    texts.append(block["text"])
                elif btype == "tool_use":
                    tool_calls.append({
                        "id": block.get("id"),
                        "type": "function",
                        "function": {
                            "name": block.get("name"),
                            "arguments": json.dumps(block.get("input") or {}),
                        },
                    })
            out = {"role": "assistant",
                   "content": "".join(texts) if texts else None}
            if tool_calls:
                out["tool_calls"] = tool_calls
            messages.append(out)
            continue

        # user turn with block content: tool_result blocks fan out into
        # separate {"role": "tool"} messages, in order, and must come first —
        # OpenAI requires them to immediately follow the assistant message
        # carrying the tool_calls. Any remaining non-tool_result blocks (e.g.
        # a mention the decider merged into this turn as trailing text, per
        # agent/decider.py's _advance) are collapsed into a single trailing
        # {"role": "user"} message so that content is never silently dropped
        # — a list-content user turn always yields at least one message.
        texts = []
        has_other = False
        for block in content:
            if not isinstance(block, dict):
                continue
            if block.get("type") != "tool_result":
                has_other = True
                if block.get("type") == "text" and isinstance(block.get("text"), str):
                    texts.append(block["text"])
                continue
            result_content = block.get("content")
            if not isinstance(result_content, str):
                result_content = json.dumps(result_content)
            # OpenAI's tool message has no is_error slot. This ERROR: prefix
            # is a lossy-but-visible encoding forced by the target format,
            # not a stylistic preference -- without it a failed tool call is
            # indistinguishable from a successful one to the model.
            if block.get("is_error"):
                result_content = f"ERROR: {result_content}"
            messages.append({
                "role": "tool",
                "tool_call_id": block.get("tool_use_id"),
                "content": result_content,
            })
        if has_other:
            messages.append({"role": "user", "content": "".join(texts)})

    body = {"model": req["model"],
             "max_tokens": req.get("max_tokens") or DEFAULT_MAX_TOKENS,
             "messages": messages}

    tools = req.get("tools")
    if tools:
        body["tools"] = [{
            "type": "function",
            "function": {
                "name": t.get("name"),
                "description": t.get("description"),
                "parameters": t.get("input_schema"),
            },
        } for t in tools if isinstance(t, dict)]

    return body


def _from_openai(data: dict) -> dict:
    """OpenAI-shaped response body -> Anthropic-shaped result."""
    choices = data.get("choices")
    if not isinstance(choices, list) or not choices:
        raise LlmError("no choices in response")
    choice = choices[0]
    if not isinstance(choice, dict):
        raise LlmError("malformed choice in response")

    message = choice.get("message")
    if not isinstance(message, dict):
        message = {}

    content_blocks = []
    text = message.get("content")
    if isinstance(text, str) and text:
        content_blocks.append({"type": "text", "text": text})

    tool_calls = message.get("tool_calls")
    if isinstance(tool_calls, list):
        for call in tool_calls:
            if not isinstance(call, dict):
                continue
            call_id = call.get("id")
            fn = call.get("function")
            if not isinstance(fn, dict):
                continue
            name = fn.get("name")
            if not call_id or not name:
                # A tool call missing its id or name is skipped rather than
                # producing a malformed block.
                continue
            args_str = fn.get("arguments")
            try:
                args = json.loads(args_str) if isinstance(args_str, str) else {}
            except Exception:
                # A model can emit invalid JSON here. Raising would lose the
                # whole turn; an empty input lets the tool report its own
                # error back in-band where the model can react to it.
                args = {}
            if not isinstance(args, dict):
                args = {}
            content_blocks.append({"type": "tool_use", "id": call_id,
                                    "name": name, "input": args})

    finish_reason = choice.get("finish_reason")
    stop_reason = _FINISH_TO_STOP.get(finish_reason, finish_reason)

    usage = data.get("usage")
    if not isinstance(usage, dict):
        usage = {}

    return {
        "stop_reason": stop_reason,
        "content": content_blocks,
        "usage": {
            "input_tokens": usage.get("prompt_tokens"),
            "output_tokens": usage.get("completion_tokens"),
        },
    }


class OpenAiBackend:
    name = "openai"

    def __init__(self, api_key: str, *, base_url: str, client=None):
        self._key = api_key
        self._base_url = base_url.rstrip("/")
        self._http = client or httpx.AsyncClient(timeout=TIMEOUT)

    async def complete(self, req: dict) -> dict:
        body = _to_openai(req)

        resp = await self._http.post(
            f"{self._base_url}/chat/completions", json=body,
            headers={"authorization": f"Bearer {self._key}",
                     "content-type": "application/json"})
        if resp.status_code >= 500 or resp.status_code in TRANSIENT_STATUS:
            # Transient: raise RetryableError so the Bus retries. Carry the
            # provider's retry-after (a 429 usually has one); None → Bus backoff.
            raise RetryableError(_error_message(resp),
                                 retry_after=parse_retry_after(resp))
        if resp.status_code >= 400:
            raise LlmError(_error_message(resp), status=resp.status_code)

        try:
            data = resp.json()
        except Exception:
            raise LlmError("unreadable response body", status=resp.status_code)
        if not isinstance(data, dict):
            raise LlmError("unreadable response body", status=resp.status_code)

        return _from_openai(data)

    async def close(self) -> None:
        await self._http.aclose()
