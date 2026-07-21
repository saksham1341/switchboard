import asyncio
import sqlite3

import pytest
from switchboard.event import EventInput
from switchboard.egress import Handler, Ctx
from switchboard.errors import ChainTooDeep, PermanentError


async def test_publish_accepts_and_assigns_id(broker):
    r = await broker.publish(EventInput(kind="github.home.pr.opened", source="github",
                                        payload={"n": 1}))
    assert r.status == "accepted"
    assert len(r.event_id) == 26


async def test_publish_dedupes_on_key(broker):
    ev = EventInput(kind="k", source="github", payload={}, dedupe_key="delivery-1")
    first = await broker.publish(ev)
    second = await broker.publish(EventInput(kind="k", source="github", payload={},
                                             dedupe_key="delivery-1"))
    assert first.status == "accepted"
    assert second.status == "duplicate"
    assert second.event_id == first.event_id


async def test_publish_without_key_never_dedupes(broker):
    a = await broker.publish(EventInput(kind="k", source="github", payload={}))
    b = await broker.publish(EventInput(kind="k", source="github", payload={}))
    assert a.status == "accepted" and b.status == "accepted"
    assert a.event_id != b.event_id


async def test_publish_rejects_over_max_depth(broker):
    ev = EventInput(kind="k", source="github", payload={}, meta={"depth": "17"})
    with pytest.raises(ChainTooDeep):
        await broker.publish(ev)


async def test_stop_is_robust_to_a_failed_consumer_task(make_broker):
    """A consumer task that ended by raising (not by cancellation) must not
    prevent stop() from closing the db handles."""
    b = make_broker()
    await b.start()

    async def _boom():
        raise RuntimeError("consumer blew up")

    b._tasks.append(asyncio.create_task(_boom()))
    await asyncio.sleep(0.01)          # let it fail
    await b.stop()                     # must NOT raise

    # the mamamia connection is closed: using it now raises
    with pytest.raises(sqlite3.ProgrammingError):
        b._conn.execute("SELECT 1")


class RecordingEgress:
    """A fake egress whose single handler records events and can be scripted to
    fail. `fail_times` transient failures, then success."""
    def __init__(self, name="rec", fail_times=0, permanent=False, hang=False):
        self.name = name
        self.filter = None
        self.seen = []
        self._fail_times = fail_times
        self._permanent = permanent
        self._hang = hang
        self.handlers = [Handler(name="h", filter=lambda e: True, handle=self._handle,
                                 timeout_s=0.2, lease_s=0.4)]

    def context(self):
        return None

    async def _handle(self, event, ctx):
        if self._hang:
            await asyncio.sleep(10)
        if self._permanent:
            raise PermanentError("nope")
        if len(self.seen) < self._fail_times:
            self.seen.append(("fail", event.id))
            raise RuntimeError("transient")
        self.seen.append(("ok", event.id))


async def _wait_for(predicate, timeout=5.0):
    async def loop():
        while not predicate():
            await asyncio.sleep(0.01)
    await asyncio.wait_for(loop(), timeout)


async def test_dispatch_delivers_once(make_broker):
    b = make_broker()
    eg = RecordingEgress()
    b.attach(eg)
    await b.start()
    try:
        r = await b.publish(EventInput(kind="k", source="github", payload={"n": 1}))
        await _wait_for(lambda: len(eg.seen) >= 1)
        assert eg.seen == [("ok", r.event_id)]
        await asyncio.sleep(0.2)                      # no redelivery
        assert eg.seen == [("ok", r.event_id)]
    finally:
        await b.stop()


async def test_transient_failure_retries_then_succeeds(make_broker):
    b = make_broker()
    eg = RecordingEgress(fail_times=2)
    b.attach(eg)
    await b.start()
    try:
        await b.publish(EventInput(kind="k", source="github", payload={}))
        await _wait_for(lambda: ("ok" in [s[0] for s in eg.seen]), timeout=15)
        outcomes = [s[0] for s in eg.seen]
        assert outcomes == ["fail", "fail", "ok"]
    finally:
        await b.stop()


async def test_permanent_error_dead_letters_without_retry(make_broker):
    b = make_broker()
    eg = RecordingEgress(permanent=True)
    dead = []
    b.attach(eg)
    b.on("dead", lambda e, g: dead.append(e.id))
    await b.start()
    try:
        await b.publish(EventInput(kind="k", source="github", payload={}))
        await _wait_for(lambda: len(dead) >= 1)
        await asyncio.sleep(0.3)
        assert len(dead) == 1                          # dead once, never retried
    finally:
        await b.stop()


async def test_hung_handler_times_out_and_isolates(make_broker):
    b = make_broker()
    slow = RecordingEgress(name="slow", hang=True)
    fast = RecordingEgress(name="fast")
    b.attach(slow)
    b.attach(fast)
    await b.start()
    try:
        await b.publish(EventInput(kind="k", source="github", payload={}))
        # fast handler delivers despite slow one being blocked/timing out
        await _wait_for(lambda: len(fast.seen) >= 1, timeout=5)
        assert fast.seen[0][0] == "ok"
    finally:
        await b.stop()


async def test_dead_letter_ceiling_is_ten(make_broker, monkeypatch):
    # make retries immediate so 10 attempts run fast
    monkeypatch.setattr("switchboard.broker.backoff", lambda *a, **k: 0.0)
    b = make_broker()
    eg = RecordingEgress(fail_times=999)   # always raises -> RETRY each time
    dead = []
    b.attach(eg)
    b.on("dead", lambda e, g: dead.append(e.id))
    await b.start()
    try:
        await b.publish(EventInput(kind="k", source="github", payload={}))
        await _wait_for(lambda: len(dead) >= 1, timeout=10)
        # 10 attempts (all "fail") before the message dead-letters; not 3
        assert len(eg.seen) == 10, f"expected 10 attempts before dead-letter, got {len(eg.seen)}"
    finally:
        await b.stop()


async def test_consumer_survives_settle_permission_error(make_broker):
    b = make_broker()
    eg = RecordingEgress()               # handler succeeds
    success = []
    b.attach(eg)
    b.on("success", lambda e, g: success.append(e.id))
    await b.start()
    try:
        orch = b._registry.get_orchestrator("events")
        real_settle = orch.settle
        calls = {"n": 0}

        async def flaky_settle(*a, **k):
            calls["n"] += 1
            if calls["n"] == 1:
                raise PermissionError("lease expired before settle")
            return await real_settle(*a, **k)

        orch.settle = flaky_settle
        await b.publish(EventInput(kind="k", source="github", payload={}))
        # first settle raises PermissionError; the loop must survive, the message
        # is redelivered after its lease lapses, and a later settle succeeds.
        await _wait_for(lambda: len(success) >= 1, timeout=10)
        assert not b._tasks[0].done(), "consumer task died after a settle PermissionError"
    finally:
        await b.stop()


async def test_durability_redelivers_after_crash_midflight(tmp_path):
    # True crash recovery: a handler takes the message (lease written, durable)
    # and is cancelled MID-DISPATCH without settling. On restart, the held lease
    # expires and mamamia redelivers to the same consumer group.
    from switchboard.broker import Broker
    from switchboard.egress import Handler
    paths = dict(mamamia_db_path=str(tmp_path / "e.db"),
                 switchboard_db_path=str(tmp_path / "s.db"))

    started = asyncio.Event()

    async def hang(event, ctx):
        started.set()
        await asyncio.sleep(30)          # never settles; cancelled by stop()

    class Hanger:
        name = "grp"; filter = None
        def context(self): return None
        handlers = [Handler(name="h", filter=lambda e: True, handle=hang,
                            timeout_s=60, lease_s=0.5)]

    b1 = Broker(wait_ms=50, reaper_interval=3600.0, **paths)
    b1.attach(Hanger()); await b1.start()
    await b1.publish(EventInput(kind="k", source="github", payload={}))
    await asyncio.wait_for(started.wait(), timeout=5)   # in-flight, lease held
    await b1.stop()                                     # crash: cancel mid-dispatch

    got = []

    async def recover(event, ctx):
        got.append(event.id)

    class Recover:
        name = "grp"; filter = None       # SAME group_id 'grp/h'
        def context(self): return None
        handlers = [Handler(name="h", filter=lambda e: True, handle=recover,
                            timeout_s=1.0, lease_s=1.0)]

    b2 = Broker(wait_ms=50, reaper_interval=3600.0, **paths)
    b2.attach(Recover()); await b2.start()
    try:
        await _wait_for(lambda: len(got) >= 1, timeout=10)   # lease (0.5s) lapses → redeliver
        assert len(got) == 1
    finally:
        await b2.stop()
