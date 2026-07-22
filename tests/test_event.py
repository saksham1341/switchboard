import time
from switchboard.event import ulid, now_iso, Event, EventInput, PublishResult


def test_ulid_is_26_char_crockford():
    u = ulid()
    assert len(u) == 26
    assert all(c in "0123456789ABCDEFGHJKMNPQRSTVWXYZ" for c in u)


def test_ulid_is_time_sortable():
    a = ulid()
    time.sleep(0.002)
    b = ulid()
    assert b > a


def test_ulid_unique():
    assert len({ulid() for _ in range(1000)}) == 1000


def test_now_iso_roundtrips():
    from datetime import datetime
    datetime.fromisoformat(now_iso())  # must not raise


def test_event_is_frozen():
    e = Event(id="x", kind="github.home.pr.opened", source="github",
              at=now_iso(), payload={"n": 1})
    import dataclasses
    try:
        e.kind = "y"
        assert False, "Event must be frozen"
    except dataclasses.FrozenInstanceError:
        pass


def test_event_input_defaults():
    ei = EventInput(kind="k", source="s", payload={})
    assert ei.at is None and ei.dedupe_key is None and ei.meta == {}


def test_publish_result():
    r = PublishResult(status="accepted", event_id="abc")
    assert r.status == "accepted" and r.event_id == "abc"
