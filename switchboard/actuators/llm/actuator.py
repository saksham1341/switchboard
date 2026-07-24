"""The model, as an actuator — one actuator, many provider backends.

Deliberately generic: it takes messages and tools and returns a completion, and
knows nothing about agents. Any decider could emit `llm` commands.

The actuator owns the contract and the error discipline; a backend owns one
provider's wire format. That split is what lets a provider change without the
decider, the logs, or a single test moving.
"""
import re
from typing import Protocol, runtime_checkable

# A 4xx normally means "do not retry" — the provider explained what is wrong and
# sending the same request again will not fix it. These two are the exceptions,
# and both were learned the hard way from live traffic:
#
#   429  a rate limit. The provider expects a retry and usually says when
#        ("try again in 11.92s"). Reporting it as permanent leaves a user's
#        message unanswered when a retry seconds later would have worked.
#   408  the provider timed out reading the request. Same story.
#
# On a transient status the backend raises RetryableError carrying the provider's
# own retry-after; the Bus honours it. Blind exponential backoff fires early
# retries so close together that a large call re-exhausts the very token budget
# it is waiting on — the provider's header is the delay that actually works.
TRANSIENT_STATUS = frozenset({408, 429})

_RETRY_CAP = 120.0     # never wait longer than this on a provider's say-so
_DUR_UNITS = {"ms": 0.001, "s": 1.0, "m": 60.0, "h": 3600.0}


def _duration_seconds(s):
    """Parse a plain number of seconds ("2", "2.5") or a compound duration
    string ("205ms", "1m26.4s") into float seconds, or None."""
    if not s:
        return None
    s = s.strip()
    try:
        return float(s)              # bare seconds — the standard Retry-After form
    except ValueError:
        pass
    parts = re.findall(r"([0-9.]+)\s*(ms|s|m|h)", s)
    if not parts:
        return None
    return sum(float(n) * _DUR_UNITS[u] for n, u in parts)


def parse_retry_after(resp):
    """The delay a rate-limited response tells us to wait, capped, or None.

    Prefers the standard `Retry-After` header; falls back to the token-window
    reset some providers surface (Groq's `x-ratelimit-reset-tokens`, often
    sub-second). None → the caller lets the Bus pick its own backoff.
    """
    headers = getattr(resp, "headers", {})
    val = _duration_seconds(headers.get("retry-after"))
    if val is None:
        val = _duration_seconds(headers.get("x-ratelimit-reset-tokens"))
    if val is None:
        return None
    return max(0.0, min(val, _RETRY_CAP))


class LlmError(Exception):
    """A failure the provider explained and a retry will not fix.

    Raised by a backend, reported by the actuator. Anything else a backend
    raises (a timeout, a 5xx, a 429, a socket error) propagates so the Bus
    retries with backoff.
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
