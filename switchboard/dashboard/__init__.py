"""The live dashboard: a tap that exports what it reads, and a page that draws it.

Two halves, wired separately by the app:

    DashboardTap.observe()  →  bounded queue  →  POST /dashboard/ingest
                                                          ↓
                                              Dashboard broadcaster
                                                          ↓
                                          GET /dashboard/stream (SSE)

The HTTP hop between them is deliberate: it makes the dashboard a standalone
ingest surface, so a second Switchboard — or a dashboard moved off this box —
feeds the same endpoint by changing only a URL.
"""
import asyncio
import hmac
import json
import logging
import time
from pathlib import Path

import httpx
from starlette.responses import HTMLResponse, JSONResponse, StreamingResponse

from switchboard.dashboard.stats import FRAME_KEYS, backfill
from switchboard.sensors.deadletter import DEADLETTER


def _reframe(frame: dict) -> dict:
    """Keep only the allowlisted keys. Defence in depth for the ingest seam."""
    return {k: frame.get(k) for k in FRAME_KEYS}

logger = logging.getLogger(__name__)

QUEUE_MAX = 256          # tap-side buffer before the oldest frames are dropped
CLIENT_MAX = 256         # per-browser buffer
DEAD_MAX = 500           # matches Bus's log_max_dead: the poll this replaced
                         # re-derived the list from message_state and was
                         # bounded by that ceiling for free. Nothing rebuilds
                         # it now, so the bound has to be stated here.
POST_TIMEOUT = 2.0
PAGE = Path(__file__).with_name("page.html")


def project(log: str, view) -> dict:
    """Structure only. Built here, before anything leaves the process, so a
    payload cannot reach the wire even if ingest is later reused carelessly.

    Observation payloads carry GitHub bodies, Discord user ids, and a live
    interaction_token — which is a capability, not merely data.

    A `switchboard.deadletter` observation is the one exception worth naming:
    its payload IS structure (which log, which message died), never producer
    content, so `dead_log`/`dead_id` are read from it deliberately — every
    other field below stays name/id/link, same as always.
    """
    dead_log = dead_id = None
    if view.name == DEADLETTER:
        payload = getattr(view, "payload", None) or {}
        dead_log = payload.get("log")
        dead_id = payload.get("message_id")
    return {
        "log": log,
        "id": view.id,
        "name": view.name,
        "emitted_by": view.emitted_by,
        "observation_id": getattr(view, "observation_id", None),
        "command_id": getattr(view, "command_id", None),
        "dead_log": dead_log,
        "dead_id": dead_id,
        "seen_at": time.time(),
    }


class DashboardTap:
    """Reads both logs and exports a projection. It writes to no log and changes
    no world state, so it remains a tap — but it does open a socket, and that is
    stated rather than quietly assumed.

    It must never be able to slow the relay: a tap is a consumer group, so a
    blocking observe() stalls its consume loop and backs the log up.
    """

    name = "dashboard"
    logs = ("obs", "cmd")

    def __init__(self, url: str, token: str, *, client=None):
        self._url, self._token = url, token
        self._client = client
        self._q: asyncio.Queue = asyncio.Queue(maxsize=QUEUE_MAX)
        self._sender = None
        self.dropped = 0

    def bind(self, ctx) -> None:
        self.ctx = ctx
        if self._client is None:
            # Built here, not in __init__: httpx wants a running loop, and bind
            # runs inside Bus.start().
            self._client = httpx.AsyncClient(timeout=POST_TIMEOUT)

    async def observe(self, log, view) -> None:
        if self._sender is None:
            self._sender = asyncio.create_task(self._run(), name="dashboard-sender")
        try:
            self._q.put_nowait(project(log, view))
        except asyncio.QueueFull:
            # Drop the OLDEST: a browser catching up cares about recent events,
            # and dropping the newest would make the visible tail lie.
            try:
                self._q.get_nowait()
                self._q.put_nowait(project(log, view))
            except (asyncio.QueueEmpty, asyncio.QueueFull):
                pass
            self.dropped += 1

    async def _run(self) -> None:
        while True:
            batch = [await self._q.get()]
            while not self._q.empty() and len(batch) < 64:
                batch.append(self._q.get_nowait())
            try:
                await self._client.post(
                    self._url, json={"frames": batch, "dropped": self.dropped},
                    headers={"Authorization": f"Bearer {self._token}"})
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                # Never retried: a stale animation frame is worse than a missing
                # one, and a backlog would outlive whatever caused it.
                logger.debug("dashboard ingest failed, dropping %d frames: %s",
                             len(batch), exc)

    async def close(self) -> None:
        if self._sender is not None:
            self._sender.cancel()
            await asyncio.gather(self._sender, return_exceptions=True)
            self._sender = None
        if self._client is not None:
            await self._client.aclose()


class Dashboard:
    """Serves the page, accepts authenticated frames, fans them out over SSE."""

    def __init__(self, topology: dict, token: str, db_path: str):
        self._topology = topology
        self._token = token
        self._db = db_path
        self._clients: set[asyncio.Queue] = set()
        self._dead: list[dict] = []
        # Membership index for _dead, trimmed with it. The list is what the
        # browser gets (order matters); this is only so the dedupe below is not
        # a linear scan of 500 entries per frame.
        self._dead_seen: set[tuple] = set()
        self._dropped = 0

    # --- routes ----------------------------------------------------------
    async def page(self, request):
        # Read per request rather than cached: it costs nothing at this traffic
        # and means editing the page on the box takes effect on reload.
        return HTMLResponse(PAGE.read_text())

    async def ingest(self, request):
        header = request.headers.get("Authorization", "")
        expected = f"Bearer {self._token}"
        if not hmac.compare_digest(header, expected):
            logger.warning("dashboard ingest rejected: bad or missing bearer token")
            return JSONResponse({"error": "unauthorized"}, status_code=401)
        try:
            body = await request.json()
        except ValueError:
            return JSONResponse({"error": "malformed json"}, status_code=400)

        self._dropped = body.get("dropped", self._dropped)
        new_dead = False
        for raw in body.get("frames", []):
            # Re-shape to the allowlist at the boundary too: the browser escapes
            # on render, but a frame with unexpected keys should never reach it.
            frame = _reframe(raw)
            if (frame["name"] == DEADLETTER and frame["dead_log"] is not None
                    and frame["dead_id"] is not None):
                entry = {"log": frame["dead_log"], "id": frame["dead_id"]}
                mark = (frame["dead_log"], frame["dead_id"])
                if mark not in self._dead_seen:
                    self._dead.append(entry)
                    self._dead_seen.add(mark)
                    # Bounded, oldest first. Losing pre-restart dead letters is
                    # accepted (nothing rebuilds this list any more); an
                    # unbounded one that is re-broadcast in full on every new
                    # dead letter is not. The index is trimmed with the list, or
                    # an evicted entry would be remembered as "already seen"
                    # forever and could never reappear.
                    while len(self._dead) > DEAD_MAX:
                        gone = self._dead.pop(0)
                        self._dead_seen.discard((gone["log"], gone["id"]))
                    new_dead = True
            self._broadcast({"type": "event", "frame": frame})
        if new_dead:
            # Same shape refresh_dead used to push: a dead command emits no
            # result observation, so this list is the one signal the rest of
            # the event stream cannot carry on its own.
            self._broadcast({"type": "dead", "dead": self._dead})
        return JSONResponse({"status": "ok"}, status_code=202)

    async def stream(self, request):
        q: asyncio.Queue = asyncio.Queue(maxsize=CLIENT_MAX)
        self._clients.add(q)

        async def events():
            try:
                yield self._sse({"type": "hello", "topology": self._topology,
                                 "backfill": backfill(self._db),
                                 "dead": self._dead, "dropped": self._dropped})
                while True:
                    try:
                        msg = await asyncio.wait_for(q.get(), timeout=20.0)
                    except asyncio.TimeoutError:
                        yield ": keepalive\n\n"    # keeps proxies from closing it
                        continue
                    yield self._sse(msg)
            finally:
                self._clients.discard(q)

        return StreamingResponse(events(), media_type="text/event-stream",
                                 headers={"Cache-Control": "no-cache",
                                          "X-Accel-Buffering": "no"})

    # --- internals -------------------------------------------------------
    @staticmethod
    def _sse(obj) -> str:
        return f"data: {json.dumps(obj)}\n\n"

    def _broadcast(self, msg) -> None:
        for q in list(self._clients):
            try:
                q.put_nowait(msg)
            except asyncio.QueueFull:
                # One stalled browser must not stall the others.
                self._clients.discard(q)
