import json
import sys

from switchboard.event import Event
from switchboard.egress import Ctx, Filter, Handler


class LoggerEgress:
    """Structured-JSON debug tap — truly log-all (accepts every event, any source)."""

    name = "logger"

    def __init__(self, filter: Filter | None = None, stream=None):
        self.filter = filter or (lambda e: True)
        self._stream = stream or sys.stdout
        self.handlers = [Handler(name="log-all", filter=lambda e: True, handle=self._log)]

    def context(self):
        return None

    async def _log(self, event: Event, ctx: Ctx) -> None:
        self._stream.write(json.dumps({
            "event_id": event.id,
            "kind": event.kind,
            "source": event.source,
            "at": event.at,
            "payload": event.payload,
        }) + "\n")
        self._stream.flush()
