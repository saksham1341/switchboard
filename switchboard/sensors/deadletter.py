"""Announce dead-lettered messages as observations.

mamamia can mark a message DEAD without the Bus seeing it — its own retry cap,
the reaper, lease-expiry churn — so inline emission from the consume loop can
never be complete. Reading the DEAD table makes the table the single source of
truth, and this is exactly a sensor's job: bring a fact into the log that was
not already in it.
"""
import logging
import sqlite3

from mamamia.core.models import MessageState

logger = logging.getLogger(__name__)

DEADLETTER = "switchboard.deadletter"


def _decode(blob):
    import msgpack
    return msgpack.unpackb(blob, raw=False)


class DeadLetterSensor:
    name = "deadletter"

    def __init__(self, db_path: str, *, interval: float = 10.0):
        self._db = db_path
        self._interval = interval
        self.ctx = None

    def bind(self, ctx) -> None:
        self.ctx = ctx
        ctx.schedule.every(self._interval, self.sweep, first_after=0.0)

    async def start(self) -> None:
        return                       # timer-driven: no loop to supervise

    async def stop(self) -> None:
        return

    async def sweep(self) -> None:
        try:
            rows = self._dead_rows()
        except Exception as exc:
            logger.debug("dead-letter sweep skipped: %s", exc)
            return

        # First run establishes a baseline: existing DEAD rows are recorded as
        # seen but not announced, so a fresh store never replays history.
        baselined = await self.ctx.store.get("baselined") is not None

        for log, group, mid, name in rows:
            key = f"seen:{log}:{group}:{mid}"
            if await self.ctx.store.get(key) is not None:
                continue
            await self.ctx.store.set(key, "1")
            if not baselined:
                continue
            # Never announce our own kind: a consumer dying on a deadletter
            # observation would otherwise announce that, forever.
            if name == DEADLETTER:
                continue
            await self.ctx.emit(DEADLETTER, {
                "log": log, "group": group, "message_id": mid,
                "name": name, "reason": "dead-lettered",
            })

        if not baselined:
            await self.ctx.store.set("baselined", "1")

    def _dead_rows(self):
        # uri=True + mode=ro so a bug here can never write to the relay's db.
        conn = sqlite3.connect(f"file:{self._db}?mode=ro", uri=True, timeout=2.0)
        try:
            dead = conn.execute(
                "SELECT log_id, group_id, message_id FROM message_state WHERE state = ?",
                (MessageState.DEAD.value,),
            ).fetchall()
            out = []
            for log, group, mid in dead:
                row = conn.execute(
                    "SELECT metadata FROM messages WHERE log_id = ? AND id = ?",
                    (log, mid),
                ).fetchone()
                md = _decode(row[0]) if row and row[0] else {}
                out.append((log, group, mid, md.get("name", "")))
            return out
        finally:
            conn.close()
