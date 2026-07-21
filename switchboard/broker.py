import asyncio
import uuid
from dataclasses import asdict
from typing import Callable, Literal

from mamamia.core.models import Outcome
from mamamia.server.db import connect
from mamamia.server.storage.sqlite import SQLiteStorage
from mamamia.server.state.sqlite import SQLiteStateStore
from mamamia.server.lease.sqlite import SQLiteLeaseManager
from mamamia.server.transaction import SQLiteTransaction
from mamamia.server.registry import LogRegistry

from switchboard.backoff import backoff
from switchboard.dedup import SeenStore
from switchboard.egress import Ctx, Egress
from switchboard.errors import ChainTooDeep, PermanentError
from switchboard.event import Event, EventInput, PublishResult, now_iso, ulid

LOG_ID = "events"


class Broker:
    def __init__(
        self,
        mamamia_db_path: str,
        switchboard_db_path: str,
        *,
        max_log_messages: int = 10_000,
        max_dead: int = 500,
        default_timeout_s: float = 30.0,
        wait_ms: int = 30_000,
        max_chain_depth: int = 16,
        reaper_interval: float = 60.0,
    ):
        self._mamamia_db_path = mamamia_db_path
        self._switchboard_db_path = switchboard_db_path
        self._max_log_messages = max_log_messages
        self._max_dead = max_dead
        self._default_timeout_s = default_timeout_s
        self._wait_ms = wait_ms
        self._max_chain_depth = max_chain_depth
        self._reaper_interval = reaper_interval

        self._instance_id = f"sb-{uuid.uuid4().hex}"
        self._egresses: dict[str, Egress] = {}
        self._hooks: dict[str, list[Callable[[Event, str], None]]] = {
            "success": [], "failed": [], "dead": []
        }
        self._registry: LogRegistry | None = None
        self._seen: SeenStore | None = None
        self._conn = None
        self._tasks: list[asyncio.Task] = []
        self._running = False

    def attach(self, egress: Egress) -> None:
        self._egresses[egress.name] = egress  # idempotent by name

    def on(self, hook: Literal["success", "failed", "dead"],
           fn: Callable[[Event, str], None]) -> None:
        self._hooks[hook].append(fn)

    def _fire(self, hook: str, event: Event, group_id: str) -> None:
        for fn in self._hooks[hook]:
            try:
                fn(event, group_id)
            except Exception:
                pass  # observability must never break dispatch

    async def start(self) -> None:
        self._conn = await connect(self._mamamia_db_path)
        self._registry = LogRegistry(
            storage=SQLiteStorage(self._conn),
            state=SQLiteStateStore(self._conn),
            lease=SQLiteLeaseManager(self._conn),
            transaction=SQLiteTransaction(self._conn),
            max_log_messages=self._max_log_messages,
            max_dead=self._max_dead,
        )
        self._seen = SeenStore(self._switchboard_db_path)
        self._running = True
        self._registry.start_reaper(interval=self._reaper_interval)
        for egress in self._egresses.values():
            for handler in egress.handlers:
                self._tasks.append(asyncio.create_task(self._consume(egress, handler)))

    async def stop(self) -> None:
        self._running = False
        for t in self._tasks:
            t.cancel()
        if self._tasks:
            # return_exceptions=True so a task that ended by raising (not just by
            # cancellation) cannot abort cleanup and leak the db handles.
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        try:
            if self._seen is not None:
                self._seen.close()
        finally:
            if self._conn is not None:
                self._conn.close()

    async def publish(self, ev: EventInput) -> PublishResult:
        depth = int(ev.meta.get("depth", "0"))
        if depth > self._max_chain_depth:
            raise ChainTooDeep(f"publish depth {depth} exceeds {self._max_chain_depth}")

        if ev.dedupe_key is not None:
            existing = self._seen.get(ev.dedupe_key)
            if existing is not None:
                return PublishResult(status="duplicate", event_id=existing)

        event = Event(
            id=ulid(), kind=ev.kind, source=ev.source,
            at=ev.at or now_iso(), payload=ev.payload,
            dedupe_key=ev.dedupe_key, meta=dict(ev.meta),
        )
        # Ordered for crash-safety: append first (durable), then record seen.
        await self._registry.get_storage().append(LOG_ID, asdict(event))
        if ev.dedupe_key is not None:
            self._seen.record(ev.dedupe_key, event.id)
        self._registry.notify(LOG_ID)
        return PublishResult(status="accepted", event_id=event.id)

    async def _consume(self, egress: Egress, handler) -> None:
        raise NotImplementedError  # Task 7
