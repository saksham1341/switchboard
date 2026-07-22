import asyncio
import logging
import uuid

from mamamia.core.models import Outcome
from mamamia.server.db import connect
from mamamia.server.storage.sqlite import SQLiteStorage
from mamamia.server.state.sqlite import SQLiteStateStore
from mamamia.server.lease.sqlite import SQLiteLeaseManager
from mamamia.server.transaction import SQLiteTransaction
from mamamia.server.registry import LogRegistry

from switchboard.backoff import backoff
from switchboard.errors import PermanentError
from switchboard.message import (
    OBS_LOG, CMD_LOG, Observation, Command, DecideCtx, ActCtx,
)

logger = logging.getLogger(__name__)


class Bus:
    def __init__(self, mamamia_db_path, *, default_timeout_s=30.0, wait_ms=30_000,
                 reaper_interval=60.0, max_retries=10, max_log_messages=10_000, max_dead=500):
        self._db = mamamia_db_path
        self._default_timeout_s = default_timeout_s
        self._wait_ms = wait_ms
        self._reaper_interval = reaper_interval
        self._max_retries = max_retries
        self._max_log_messages = max_log_messages
        self._max_dead = max_dead

        self._instance = f"sb-{uuid.uuid4().hex}"
        self._sensors, self._deciders, self._actuators, self._taps = [], [], [], []
        self._registry = None
        self._conn = None
        self._tasks: list[asyncio.Task] = []
        self._running = False

    # registration
    def add_sensor(self, s): self._sensors.append(s)
    def add_decider(self, d): self._deciders.append(d)
    def add_actuator(self, a): self._actuators.append(a)
    def add_tap(self, t): self._taps.append(t)

    # emit
    async def _append(self, log, name, payload, *, command_id=None, observation_id=None) -> int:
        md = {"name": name}
        if command_id is not None:
            md["command_id"] = command_id
        if observation_id is not None:
            md["observation_id"] = observation_id
        mid = await self._registry.get_storage().append(log, payload, metadata=md)
        self._registry.notify(log)
        return mid

    async def emit_observation(self, name, payload, command_id=None) -> int:
        return await self._append(OBS_LOG, name, payload, command_id=command_id)

    async def emit_command(self, name, args, observation_id) -> int:
        return await self._append(CMD_LOG, name, args, observation_id=observation_id)

    async def start(self) -> None:
        self._conn = await connect(self._db)
        self._registry = LogRegistry(
            storage=SQLiteStorage(self._conn), state=SQLiteStateStore(self._conn),
            lease=SQLiteLeaseManager(self._conn), transaction=SQLiteTransaction(self._conn),
            max_log_messages=self._max_log_messages, max_dead=self._max_dead,
        )
        for log in (OBS_LOG, CMD_LOG):
            self._registry.get_orchestrator(log).max_retries = self._max_retries
        self._running = True
        self._registry.start_reaper(interval=self._reaper_interval)

        for d in self._deciders:
            self._tasks.append(asyncio.create_task(self._run_decider(d)))
        for a in self._actuators:
            self._tasks.append(asyncio.create_task(self._run_actuator(a)))
        for t in self._taps:
            for log in t.logs:
                self._tasks.append(asyncio.create_task(self._run_tap(t, log)))
        for s in self._sensors:
            async def _emit(name, payload, _s=s):
                return await self.emit_observation(name, payload)
            self._tasks.append(asyncio.create_task(s.start(_emit)))

    async def stop(self) -> None:
        self._running = False
        for s in self._sensors:
            try:
                await s.stop()
            except Exception:
                pass
        for t in self._tasks:
            t.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        if self._conn is not None:
            self._conn.close()

    # generic consume loop shared by all consuming roles
    async def _consume(self, log, group_id, decode, keep, handle):
        orch = self._registry.get_orchestrator(log)
        timeout_s, lease_s = self._default_timeout_s, self._default_timeout_s * 2

        async def settle(mid, outcome, retry_after=0.0):
            try:
                await orch.settle(log, group_id, mid, self._instance, outcome=outcome, retry_after=retry_after)
            except PermissionError:
                logger.warning("settle skipped for %s msg %s: lease lost", group_id, mid)

        while self._running:
            try:
                msg = await self._registry.acquire_blocking(
                    log, group_id, self._instance, duration=lease_s, wait_ms=self._wait_ms)
                if msg is None:
                    continue
                view = decode(msg)
                if not keep(view):
                    await settle(msg.id, Outcome.SUCCESS)
                    continue
                try:
                    async with asyncio.timeout(timeout_s):
                        await handle(view)
                    await settle(msg.id, Outcome.SUCCESS)
                except asyncio.CancelledError:
                    raise
                except PermanentError:
                    await settle(msg.id, Outcome.DEAD)
                except Exception:
                    attempts = await orch.state_store.get_retry_count(log, group_id, msg.id)
                    await settle(msg.id, Outcome.RETRY, retry_after=backoff(attempts))
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("consume loop error in %s", group_id)
                await asyncio.sleep(0.1)

    async def _run_decider(self, d):
        async def handle(obs):
            ctx = DecideCtx(obs=obs, _emit_command=self.emit_command)
            await d.decide(obs, ctx)
        await self._consume(OBS_LOG, f"decider/{d.name}",
                            Observation.from_message, d.subscribes, handle)

    async def _run_actuator(self, a):
        ctx_obj = a.context()
        async def handle(cmd):
            ctx = ActCtx(cmd=cmd, context=ctx_obj,
                         _emit_result=lambda name, payload, cid: self.emit_observation(name, payload, command_id=cid))
            await a.act(cmd, ctx)
        await self._consume(CMD_LOG, f"actuator/{a.name}",
                            Command.from_message, lambda c: c.name == a.name, handle)

    async def _run_tap(self, t, log):
        decode = Observation.from_message if log == OBS_LOG else Command.from_message
        await self._consume(log, f"tap/{t.name}/{log}", decode,
                            lambda v: True, lambda v: t.observe(log, v))
