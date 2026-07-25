"""The clock — the substrate's only source of "time passed".

A decider must not own a timer: it is a pure function of observations, and a
timer would let it act with no observation, which is sensor-nature in the wrong
role. So periodic work is expressed as a subscription to a tick rather than as
a scheduled callback, and the work then runs through the normal consume loop —
serially, under the same settle discipline as everything else.

This sensor knows nothing about sessions, agents, or what anyone does with a
tick. It is a clock.
"""
import time


class ClockSensor:
    name = "clock"

    def __init__(self, interval: float = 60.0):
        self._interval = interval
        self._seq = 0
        self._last: float | None = None
        self.ctx = None

    @staticmethod
    def _now() -> float:
        return time.time()

    def bind(self, ctx) -> None:
        self.ctx = ctx
        ctx.schedule.every(self._interval, self.tick)

    async def tick(self) -> None:
        now = self._now()
        # delta is MEASURED, never the configured interval: a blocked event loop
        # or a slow consumer makes the real gap larger, and a consumer reasoning
        # "have N seconds passed" must see what happened rather than what was
        # scheduled. The first tick reports null — there is no previous tick,
        # and 0.0 would be a number a consumer could act on.
        delta = None if self._last is None else now - self._last
        self._seq += 1
        self._last = now
        await self.ctx.emit("clock.tick",
                            {"at": now, "delta": delta, "seq": self._seq})

    async def start(self) -> None:
        return                       # timer-driven: no loop to supervise

    async def stop(self) -> None:
        return
