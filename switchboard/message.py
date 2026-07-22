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
