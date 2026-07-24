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


async def test_failed_handler_is_not_marked_processed():
    """A message that will be RETRIED must never be marked processed — marking
    it would make the redelivery skip, silently dropping the work forever.
    This also pins the ordering: mark must come AFTER handle, not before."""
    orch = _Orch(retry_count=0)
    m = _Msg(21, "web_search")
    bus = _drive([m], orch, max_retries=10)
    async def handle(v): raise RuntimeError("boom")
    await bus._consume(CMD_LOG, "actuator/web_search", Command.from_message, lambda v: True, handle)
    assert orch.settled == [(21, Outcome.RETRY)]
    assert await bus._already_processed("actuator/web_search", 21) is False


async def test_permanent_error_is_not_marked_processed():
    """A DEAD message is terminal, but marking it would still be wrong: the
    mark belongs to the SUCCESS path alone."""
    orch = _Orch()
    m = _Msg(22, "discord.post")
    bus = _drive([m], orch)
    async def handle(v): raise PermanentError()
    await bus._consume(CMD_LOG, "actuator/discord.post", Command.from_message, lambda v: True, handle)
    assert orch.settled == [(22, Outcome.DEAD)]
    assert await bus._already_processed("actuator/discord.post", 22) is False


async def test_bus_dedup_lives_in_its_own_scope():
    """Bus machinery is namespaced like every role's, and no role can name a key
    that reaches it — every ScopedStore prepends its own "kind/name/"."""
    from switchboard.store import MemoryStore, ScopedStore
    store = MemoryStore()
    bus = Bus(":memory:", store=store)
    await bus._mark_processed("actuator/x", 5)
    assert await store.get("_bus/processed:actuator/x:5") == "1"

    role = ScopedStore(store, "actuator/x/")
    assert await role.get("_bus/processed:actuator/x:5") is None   # unreachable
