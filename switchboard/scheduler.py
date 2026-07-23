import asyncio
import logging
from dataclasses import dataclass
from typing import Awaitable, Callable

logger = logging.getLogger(__name__)


@dataclass
class _Timer:
    seconds: float
    fn: Callable[[], Awaitable[None]]
    first_after: float | None
    label: str


class OwnerSchedule:
    """What a role sees as ctx.schedule. Timers declared here are bound to that
    role's lifecycle: they never tick before it starts, and they are cancelled
    before it stops."""

    def __init__(self, owner: str, scheduler: "Scheduler"):
        self._owner, self._sched = owner, scheduler

    def every(self, seconds: float, fn, *, first_after: float | None = None,
              name: str | None = None) -> None:
        self._sched._declare(self._owner, _Timer(
            seconds, fn, first_after, name or getattr(fn, "__qualname__", "timer")))


class Scheduler:
    """Owns every role's timers: one place to cancel them at shutdown, and a
    callback that raises never takes its owner down with it."""

    def __init__(self):
        self._declared: dict[str, list[_Timer]] = {}
        self._tasks: dict[str, list[asyncio.Task]] = {}
        self._running: set[str] = set()

    def for_owner(self, owner: str) -> OwnerSchedule:
        self._declared.setdefault(owner, [])
        return OwnerSchedule(owner, self)

    def start(self, owner: str) -> None:
        # Idempotent: a second start() for a running owner must not launch a
        # second loop per timer. Declarations arriving while it runs are
        # launched by _declare, not by another start().
        if owner in self._running:
            return
        self._running.add(owner)
        for t in self._declared.get(owner, ()):
            self._launch(owner, t)

    async def stop(self, owner: str) -> None:
        self._running.discard(owner)
        # Declarations are per-run. Roles declare their timers in bind(), which
        # runs again on the next startup, so keeping them here would double
        # every timer across a stop/start cycle.
        self._declared.pop(owner, None)
        tasks = self._tasks.pop(owner, [])
        for t in tasks:
            t.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def stop_all(self) -> None:
        for owner in list(self._tasks):
            await self.stop(owner)

    def _declare(self, owner: str, timer: _Timer) -> None:
        self._declared.setdefault(owner, []).append(timer)
        # Declared from inside start(): the owner is already running, so this
        # timer starts now instead of waiting for a restart that never comes.
        if owner in self._running:
            self._launch(owner, timer)

    def _launch(self, owner: str, timer: _Timer) -> None:
        task = asyncio.create_task(self._loop(timer),
                                   name=f"schedule/{owner}/{timer.label}")
        self._tasks.setdefault(owner, []).append(task)

    async def _loop(self, timer: _Timer) -> None:
        # first_after defaults to a full interval: firing at t=0 would make a
        # crash-looping process hit the remote on every restart.
        delay = timer.seconds if timer.first_after is None else timer.first_after
        await asyncio.sleep(delay)
        while True:
            try:
                await timer.fn()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("scheduled callback %s failed", timer.label)
            # Fixed delay, not fixed rate: the next sleep starts when the
            # callback finishes, so a slow tick can never stack up behind itself.
            await asyncio.sleep(timer.seconds)
