import io
import json
import asyncio
from switchboard.event import Event, now_iso
from switchboard.egress import LoggerEgress, Ctx


def _event(**kw):
    base = dict(id="E1", kind="github.home.pr.opened", source="github",
                at=now_iso(), payload={"n": 1})
    base.update(kw)
    return Event(**base)


def test_logger_egress_shape():
    eg = LoggerEgress()
    assert eg.name == "logger"
    assert len(eg.handlers) == 1
    # LoggerEgress is truly log-all: the egress filter accepts every event,
    # any source, and the log-all handler passes everything through.
    assert eg.filter(_event()) is True
    assert eg.filter(_event(source="discord")) is True
    assert eg.handlers[0].filter(_event(source="discord")) is True


def test_logger_handler_writes_json():
    buf = io.StringIO()
    eg = LoggerEgress(stream=buf)
    h = eg.handlers[0]
    ctx = Ctx(publish=None, egress=eg.context())
    asyncio.run(h.handle(_event(), ctx))
    line = json.loads(buf.getvalue())
    assert line["event_id"] == "E1"
    assert line["kind"] == "github.home.pr.opened"
