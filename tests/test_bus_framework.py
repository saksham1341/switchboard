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
        self.retry_afters = []
        self.state_store = _SS(retry_count)
    async def settle(self, log, group, mid, inst, *, outcome, retry_after=0.0):
        self.settled.append((mid, outcome))
        self.retry_afters.append(retry_after)


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
    bus = Bus(":memory:", message_max_retries=max_retries)
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


async def test_permanent_error_is_dead_not_retried():
    from switchboard.errors import PermanentError as PE
    orch = _Orch()
    bus = _drive([_Msg(5, "boom")], orch)
    async def handle(v): raise PE("nope")
    await bus._consume(CMD_LOG, "actuator/boom", Command.from_message, lambda v: True, handle)
    assert orch.settled == [(5, Outcome.DEAD)]


async def test_retryable_error_honours_its_retry_after():
    from switchboard.errors import RetryableError
    orch = _Orch(retry_count=0)
    bus = _drive([_Msg(5, "boom")], orch)
    async def handle(v): raise RetryableError("slow down", retry_after=2.5)
    await bus._consume(CMD_LOG, "actuator/boom", Command.from_message, lambda v: True, handle)
    assert orch.settled == [(5, Outcome.RETRY)]
    assert orch.retry_afters == [2.5]                 # the provider's delay, not backoff


async def test_retryable_error_without_delay_falls_back_to_backoff():
    from switchboard.errors import RetryableError
    from switchboard.backoff import backoff
    orch = _Orch(retry_count=3)                        # so backoff(3) is deterministic-ish
    bus = _drive([_Msg(5, "boom")], orch)
    async def handle(v): raise RetryableError("slow", retry_after=None)
    await bus._consume(CMD_LOG, "actuator/boom", Command.from_message, lambda v: True, handle)
    assert orch.settled == [(5, Outcome.RETRY)]
    # jittered, so just assert it's a positive Bus-chosen delay, not the 0.0 default
    assert orch.retry_afters[0] > 0


async def test_plain_exception_uses_bus_backoff():
    orch = _Orch(retry_count=2)
    bus = _drive([_Msg(5, "boom")], orch)
    async def handle(v): raise ValueError("whatever")
    await bus._consume(CMD_LOG, "actuator/boom", Command.from_message, lambda v: True, handle)
    assert orch.settled == [(5, Outcome.RETRY)]
    assert orch.retry_afters[0] > 0


async def test_append_stores_text_only_when_given(tmp_path):
    """Absence is the default. Storing json.dumps(payload) as `text` would
    duplicate the payload byte-for-byte in metadata for zero information."""
    from switchboard.bus import Bus
    bus = Bus(str(tmp_path / "mm.db"), consumer_wait_ms=50, lease_reaper_interval_s=3600.0)
    await bus.start()
    try:
        await bus.emit_observation("a.thing", {"x": 1})
        await bus.emit_observation("a.thing", {"x": 2}, text="PRETTY")
        storage = bus._registry.get_storage()
        rows = await storage.get_batch("obs", 0, 10)
        metas = [r.metadata for r in rows]
        assert "text" not in metas[0]
        assert metas[1]["text"] == "PRETTY"
    finally:
        await bus.stop()


def test_worst_case_covers_both_retry_paths_and_handler_time():
    """The two retry paths are the jittered backoff ceiling and an explicit
    retry_after; a message takes the worse, PLUS the handler's own time on
    every attempt. The handler term is the one people forget and it is
    multiplied by (retries + 1)."""
    from switchboard.bus import Bus
    bus = Bus(":memory:", message_max_retries=10, handler_timeout_s=100.0,
              retry_backoff_max_s=300.0, retry_after_max_s=120.0)
    # per-attempt max(backoff_ceiling, retry_after_max):
    #   i=0..6 -> 120 each (retry_after dominates the small early ceilings)
    #   i=7 -> 128, i=8 -> 256, i=9 -> 300 (backoff overtakes)
    # delay   = 120*7 + 128 + 256 + 300 = 1524
    # handler = 11 * 100 = 1100
    assert bus.worst_case_retry_seconds == 1524 + 1100


def test_worst_case_takes_the_backoff_path_when_it_dominates():
    from switchboard.bus import Bus
    bus = Bus(":memory:", message_max_retries=10, handler_timeout_s=1.0,
              retry_backoff_max_s=300.0, retry_after_max_s=0.0)
    assert bus.worst_case_retry_seconds == 811 + 11


def test_worst_case_moves_with_the_handler_timeout():
    """The regression this property exists to prevent: raising the handler
    timeout must move anything derived from it, automatically."""
    from switchboard.bus import Bus
    lo = Bus(":memory:", handler_timeout_s=30.0).worst_case_retry_seconds
    hi = Bus(":memory:", handler_timeout_s=100.0).worst_case_retry_seconds
    assert hi - lo == 11 * 70.0


async def test_retry_after_is_clamped_by_the_bus_not_trusted(tmp_path):
    """retry_after is a request the Bus may clamp, not a promise it obeys. A
    provider answering with a daily-quota reset (~3593s) must not pin a
    consumer for an hour."""
    from switchboard.errors import RetryableError
    orch = _Orch()
    bus = _drive([_Msg(5, "boom")], orch)
    bus._retry_after_max_s = 120.0
    async def handle(v): raise RetryableError("quota", retry_after=3593.0)
    await bus._consume(CMD_LOG, "actuator/boom", Command.from_message, lambda v: True, handle)
    assert orch.retry_afters == [120.0]


async def test_a_retry_after_under_the_cap_is_honoured_exactly():
    from switchboard.errors import RetryableError
    orch = _Orch()
    bus = _drive([_Msg(5, "boom")], orch)
    bus._retry_after_max_s = 120.0
    async def handle(v): raise RetryableError("slow", retry_after=2.5)
    await bus._consume(CMD_LOG, "actuator/boom", Command.from_message, lambda v: True, handle)
    assert orch.retry_afters == [2.5]


def test_worst_case_sums_per_attempt_maxima_not_the_larger_total():
    """_consume picks the retry path PER ATTEMPT — a plain exception backs off,
    a RetryableError uses its retry_after — so one message can alternate. Maxing
    the two TOTALS assumes a message commits to one path and undercuts the real
    ceiling; a watchdog trusting that number would free a session still
    legitimately retrying."""
    from switchboard.bus import Bus
    bus = Bus(":memory:", message_max_retries=10, handler_timeout_s=0.0,
              retry_backoff_max_s=300.0, retry_after_max_s=120.0)
    naive = max(sum(min(300.0, 2.0 ** i) for i in range(10)), 10 * 120.0)
    assert bus.worst_case_retry_seconds == 1524.0
    assert bus.worst_case_retry_seconds > naive        # 1524 > 1200


def test_worst_case_ignores_a_retry_after_smaller_than_every_backoff():
    """When retry_after_max is below the smallest backoff ceiling it can never
    dominate, and the bound collapses to the pure backoff sum."""
    from switchboard.bus import Bus
    bus = Bus(":memory:", message_max_retries=10, handler_timeout_s=0.0,
              retry_backoff_max_s=300.0, retry_after_max_s=0.5)
    assert bus.worst_case_retry_seconds == 811.0
