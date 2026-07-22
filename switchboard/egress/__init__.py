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
