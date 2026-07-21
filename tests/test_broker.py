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
