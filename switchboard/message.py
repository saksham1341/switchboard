import json
from dataclasses import dataclass
from typing import Awaitable, Callable, Protocol, runtime_checkable

from switchboard.http import HttpServer
from switchboard.scheduler import OwnerSchedule
from switchboard.store import KeyStore

OBS_LOG = "obs"
CMD_LOG = "cmd"


@dataclass(frozen=True)
class Observation:
    id: int
    name: str
    payload: dict
    command_id: int | None = None      # present ⇒ this is a result observation
    emitted_by: str | None = None      # "sensor/github", "actuator/discord.post"
    text: str | None = None            # producer's rendered form; None ⇒ none stored

    @classmethod
    def from_message(cls, msg) -> "Observation":
        md = msg.metadata or {}
        return cls(id=msg.id, name=md.get("name", ""),
                   payload=msg.payload or {}, command_id=md.get("command_id"),
                   emitted_by=md.get("emitted_by"), text=md.get("text"))

    @property
    def rendered(self) -> str:
        """The text form, always. Falls back to JSON when no producer supplied
        one — absence is the common case and costs nothing to store."""
        if self.text is not None:
            return self.text
        try:
            return json.dumps(self.payload)
        except (TypeError, ValueError):
            return str(self.payload)


@dataclass(frozen=True)
class Command:
    id: int
    name: str
    args: dict
    observation_id: int | None = None  # the observation that triggered this command
    emitted_by: str | None = None      # "decider/github_notify"
    text: str | None = None            # producer's rendered form; None ⇒ none stored

    @classmethod
    def from_message(cls, msg) -> "Command":
        md = msg.metadata or {}
        return cls(id=msg.id, name=md.get("name", ""),
                   args=msg.payload or {}, observation_id=md.get("observation_id"),
                   emitted_by=md.get("emitted_by"), text=md.get("text"))

    @property
    def rendered(self) -> str:
        """The text form, always. Falls back to JSON when no producer supplied
        one — absence is the common case and costs nothing to store."""
        if self.text is not None:
            return self.text
        try:
            return json.dumps(self.args)
        except (TypeError, ValueError):
            return str(self.args)


@dataclass
class DecideCtx:
    obs: Observation
    _emit_command: Callable[..., Awaitable[int]]

    async def command(self, name: str, args: dict, text: str | None = None) -> int:
        # text is passed only when supplied: existing _emit_command fakes across
        # the test suite implement the pre-existing 3-positional-arg contract, and
        # this keeps them valid without edits — the new 4th arg is exercised only
        # by callers that opt into it.
        if text is None:
            return await self._emit_command(name, args, self.obs.id)
        return await self._emit_command(name, args, self.obs.id, text)


@dataclass
class ActCtx:
    cmd: Command
    _emit_result: Callable[..., Awaitable[int]]

    async def result(self, outcome: str, payload: dict | None = None,
                     text: str | None = None) -> int:
        # Same reasoning as DecideCtx.command: keep the pre-existing 3-arg
        # _emit_result contract intact for callers that never pass text.
        name, payload = f"{self.cmd.name}.{outcome}", payload or {}
        if text is None:
            return await self._emit_result(name, payload, self.cmd.id)
        return await self._emit_result(name, payload, self.cmd.id, text)


@dataclass
class SensorCtx:
    """What Switchboard provides to a sensor: how to emit, how the world
    reaches it (push and pull), and what it remembers between wakings."""
    emit: Callable[..., Awaitable[int]]
    http: HttpServer
    store: KeyStore
    schedule: OwnerSchedule


@dataclass
class DeciderCtx:
    """A decider has no world access. It has memory: the store is its own
    notebook — no effects outside Switchboard, durable, inspectable."""
    store: KeyStore


@dataclass
class ActuatorCtx:
    store: KeyStore


@dataclass
class TapCtx:
    """A store and nothing else. No emit — a tap that could write to a log
    would stop being a tap — and no http: a dashboard's page is registered by
    app.build(), which owns the HttpServer, alongside bus.add_tap()."""
    store: KeyStore


@runtime_checkable
class Sensor(Protocol):
    name: str
    def bind(self, ctx: SensorCtx) -> None: ...
    async def start(self) -> None: ...
    async def stop(self) -> None: ...


@runtime_checkable
class Decider(Protocol):
    name: str
    def bind(self, ctx: DeciderCtx) -> None: ...
    def subscribes(self, obs: Observation) -> bool: ...
    async def decide(self, obs: Observation, ctx: DecideCtx) -> None: ...


@runtime_checkable
class Actuator(Protocol):
    name: str                          # == the command name it executes
    # Declaring a tool_spec IS the opt-in to being agent-callable: an actuator
    # without one cannot be reached by an agent at all.
    # {"description": str, "input_schema": <JSON schema>}
    tool_spec: dict | None
    def bind(self, ctx: ActuatorCtx) -> None: ...
    async def act(self, cmd: Command, ctx: ActCtx) -> None: ...


@runtime_checkable
class Tap(Protocol):
    name: str
    logs: tuple[str, ...]              # which logs it reads, e.g. ("obs", "cmd")
    def bind(self, ctx: TapCtx) -> None: ...
    async def observe(self, log: str, view) -> None: ...
