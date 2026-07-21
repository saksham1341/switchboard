import asyncio
import sqlite3

import pytest
from switchboard.event import EventInput
from switchboard.errors import ChainTooDeep


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
