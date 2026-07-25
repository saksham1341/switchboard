import json
import sys


class LoggerTap:
    """A tap over both logs — the structured-JSON trace (obs → cmd → result)."""
    name = "logger"
    logs = ("obs", "cmd")

    def __init__(self, stream=None):
        self._stream = stream or sys.stdout
        self.ctx = None

    def bind(self, ctx) -> None:
        self.ctx = ctx

    async def observe(self, log, view) -> None:
        line = {"log": log, "id": view.id, "name": view.name,
                "payload": getattr(view, "payload", None) if log == "obs" else getattr(view, "args", None)}
        cid = getattr(view, "command_id", None)
        oid = getattr(view, "observation_id", None)
        if cid is not None:
            line["command_id"] = cid
        if oid is not None:
            line["observation_id"] = oid
        text = getattr(view, "text", None)
        if text is not None:
            line["text"] = text
        self._stream.write(json.dumps(line) + "\n")
        self._stream.flush()
