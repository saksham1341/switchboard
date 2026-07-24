"""The model, as an actuator — one actuator, many provider backends.
Deliberately generic: it takes messages and tools and returns a completion, and
knows nothing about agents. Any decider could emit `llm` commands.
The actuator owns the contract and the error discipline; a backend owns one
provider's wire format. That split is what lets a provider change without the
decider, the logs, or a single test moving.
"""
from typing import Protocol, runtime_checkable
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
