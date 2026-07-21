import json
import sys
from dataclasses import dataclass, field
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
    publish: Any     # Publish callable from the broker (None where unused)
    egress: Any      # from egress.context()


class LoggerEgress:
    """Structured-JSON debug tap. No external dependency; exercises every
    durability/retry property before Discord becomes a translation problem."""

    name = "logger"

    def __init__(self, filter: Filter | None = None, stream=None):
        self.filter = filter or (lambda e: e.source == "github")
        self._stream = stream or sys.stdout
        self.handlers = [Handler(name="log-all", filter=self.filter, handle=self._log)]

    def context(self) -> Any:
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
