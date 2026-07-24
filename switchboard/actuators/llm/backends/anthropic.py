"""The Anthropic Messages API. A passthrough, because the canonical format is
this one — see the plan's Architecture note on why the superset was chosen as
canonical rather than a neutral shape nobody speaks.

Raw httpx rather than the anthropic SDK: this matches how DiscordSender talks
to Discord, adds no dependency, and the Messages API is a single POST.
"""
import httpx

from switchboard.actuators.llm.actuator import (
    TRANSIENT_STATUS, LlmError, parse_retry_after)
from switchboard.errors import RetryableError

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
        content = data.get("content")
        usage = data.get("usage")
        return {"stop_reason": data.get("stop_reason"),
                "content": content if isinstance(content, list) else [],
                "usage": usage if isinstance(usage, dict) else {}}

    async def close(self) -> None:
        await self._http.aclose()
