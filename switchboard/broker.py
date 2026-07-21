import asyncio
import logging
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

logger = logging.getLogger(__name__)

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
        max_retries: int = 10,
    ):
        self._mamamia_db_path = mamamia_db_path
        self._switchboard_db_path = switchboard_db_path
        self._max_log_messages = max_log_messages
        self._max_dead = max_dead
        self._default_timeout_s = default_timeout_s
        self._wait_ms = wait_ms
        self._max_chain_depth = max_chain_depth
        self._reaper_interval = reaper_interval
        self._max_retries = max_retries

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
        self._registry.get_orchestrator(LOG_ID).max_retries = self._max_retries
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

    async def _settle(self, orch, group_id, message_id, outcome, retry_after=0.0):
        try:
            await orch.settle(LOG_ID, group_id, message_id, self._instance_id,
                              outcome=outcome, retry_after=retry_after)
        except PermissionError:
            # Lease expired before we settled; the message is/will be redelivered
            # and the new owner's result is authoritative. Never let this kill the
            # consumer loop.
            logger.warning("settle skipped for %s msg %s: lease no longer owned",
                           group_id, message_id)

    async def _consume(self, egress: Egress, handler) -> None:
        group_id = f"{egress.name}/{handler.name}"
        orch = self._registry.get_orchestrator(LOG_ID)
        ctx = Ctx(publish=self.publish, egress=egress.context())
        timeout_s = handler.timeout_s or self._default_timeout_s
        lease_s = handler.lease_s or timeout_s * 2

        def passes(event: Event) -> bool:
            if egress.filter is not None and not egress.filter(event):
                return False
            return handler.filter(event)

        while self._running:
            try:
                msg = await self._registry.acquire_blocking(
                    LOG_ID, group_id, self._instance_id,
                    duration=lease_s, wait_ms=self._wait_ms,
                )
                if msg is None:
                    continue
                event = Event(**msg.payload)
                if not passes(event):
                    await self._settle(orch, group_id, msg.id, Outcome.SUCCESS)
                    continue
                try:
                    async with asyncio.timeout(timeout_s):
                        await handler.handle(event, ctx)
                    await self._settle(orch, group_id, msg.id, Outcome.SUCCESS)
                    self._fire("success", event, group_id)
                except asyncio.CancelledError:
                    raise
                except PermanentError:
                    await self._settle(orch, group_id, msg.id, Outcome.DEAD)
                    self._fire("dead", event, group_id)
                except Exception:
                    attempts = await orch.state_store.get_retry_count(LOG_ID, group_id, msg.id)
                    await self._settle(orch, group_id, msg.id, Outcome.RETRY,
                                       retry_after=backoff(attempts))
                    # orch.settle() dead-letters internally once the retry ceiling
                    # is crossed (mamamia's Orchestrator._settle); attempts is the
                    # pre-increment count, so attempts + 1 is what settle just
                    # compared against max_retries. Fire the matching hook.
                    if attempts + 1 >= self._max_retries:
                        self._fire("dead", event, group_id)
                    else:
                        self._fire("failed", event, group_id)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("consumer loop error in group %s", group_id)
                await asyncio.sleep(0.1)
