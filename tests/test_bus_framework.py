from switchboard.bus import Bus
from switchboard.message import OBS_LOG, CMD_LOG, Command, Observation
from switchboard.errors import PermanentError
from mamamia.core.models import Outcome


class _Msg:
    """Minimal stand-in for a mamamia message."""
    def __init__(self, id, name):
        self.id = id
        self.payload = {}
        self.metadata = {"name": name}


class _SS:
    def __init__(self, n): self._n = n
    async def get_retry_count(self, log, group, mid): return self._n


class _Orch:
    def __init__(self, retry_count=0):
        self.settled = []
        self.state_store = _SS(retry_count)
    async def settle(self, log, group, mid, inst, *, outcome, retry_after=0.0):
        self.settled.append((mid, outcome))


class _Reg:
    """Hands _consume a fixed list of messages, then stops the loop."""
    def __init__(self, msgs, bus, orch):
        self._msgs = list(msgs); self._bus = bus; self._orch = orch
    def get_orchestrator(self, log): return self._orch
    async def acquire_blocking(self, log, group, inst, *, duration, wait_ms):
        if self._msgs:
            return self._msgs.pop(0)
        self._bus._running = False
        return None


def _drive(msgs, orch, *, retry_count=0, max_retries=10):
    """Build a Bus wired to a fake registry, ready for _consume."""
    bus = Bus(":memory:", max_retries=max_retries)
    bus._registry = _Reg(msgs, bus, orch)
    bus._running = True
    return bus


async def test_consume_handles_a_message_once():
    orch = _Orch()
    m = _Msg(5, "boom")
    bus = _drive([m, m], orch)          # SAME message delivered twice
    calls = []
    async def handle(v): calls.append(v.id)
    await bus._consume(CMD_LOG, "actuator/boom", Command.from_message, lambda v: True, handle)
    assert calls == [5]                 # handled once despite two deliveries
    assert orch.settled == [(5, Outcome.SUCCESS), (5, Outcome.SUCCESS)]  # both settled


async def test_dedup_is_per_group():
    orch = _Orch()
    m = _Msg(5, "boom")
    bus = _drive([m], orch)
    await bus._mark_processed("actuator/a", 5)
    assert await bus._already_processed("actuator/a", 5) is True
    assert await bus._already_processed("actuator/b", 5) is False   # different group, not seen


async def test_not_kept_message_is_not_marked():
    orch = _Orch()
    m = _Msg(5, "boom")
    bus = _drive([m], orch)
    await bus._consume(CMD_LOG, "actuator/boom", Command.from_message, lambda v: False, lambda v: None)
    assert await bus._already_processed("actuator/boom", 5) is False  # skipped, never handled
    assert orch.settled == [(5, Outcome.SUCCESS)]
