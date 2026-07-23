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
from switchboard.http import HttpServer
from switchboard.message import (
    OBS_LOG, CMD_LOG, Observation, Command, DecideCtx, ActCtx,
    SensorCtx, DeciderCtx, ActuatorCtx, TapCtx,
)
from switchboard.scheduler import Scheduler
from switchboard.store import MemoryStore, ScopedStore

logger = logging.getLogger(__name__)


def _as_async(fn):
    """Wrap a sync callable so the scheduler, which awaits its callbacks, can
    drive it. SqliteStore.purge is a plain method."""
    async def call():
        return fn()
    return call


class Bus:
    def __init__(self, mamamia_db_path, *, store=None, http=None,
                 default_timeout_s=30.0, wait_ms=30_000, reaper_interval=60.0,
                 max_retries=10, max_log_messages=10_000, max_dead=500):
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
        self._started = False
        self._started_owners: list[str] = []
        self._maintenance: list[str] = []

        self._store = store if store is not None else MemoryStore()
        self._owns_store = store is None
        # serve=False by default so tests register routes and drive .app
        # without any of them binding a port.
        self._http = http if http is not None else HttpServer(serve=False)
        self._scheduler = Scheduler()

    # registration
    def add_sensor(self, s): self._sensors.append(s)
    def add_decider(self, d): self._deciders.append(d)
    def add_actuator(self, a): self._actuators.append(a)
    def add_tap(self, t): self._taps.append(t)

    def schedule_maintenance(self, owner: str, seconds: float, fn) -> None:
        """A timer owned by the bus rather than by any role. Started with the
        bus; cancelled by stop_all() in stop()."""
        self._scheduler.for_owner(owner).every(seconds, _as_async(fn), name=owner)
        self._maintenance.append(owner)

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
        if self._started:
            raise RuntimeError(
                "Bus.start() is single-use — routes and log consumers are "
                "registered once. Construct a new Bus rather than restarting "
                "this one.")
        self._started = True
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

        def scoped(kind, name):
            return ScopedStore(self._store, f"{kind}/{name}/")

        for d in self._deciders:
            d.bind(DeciderCtx(store=scoped("decider", d.name)))
        for a in self._actuators:
            a.bind(ActuatorCtx(store=scoped("actuator", a.name)))
        for t in self._taps:
            t.bind(TapCtx(store=scoped("tap", t.name)))
        for s in self._sensors:
            async def _emit(name, payload):
                return await self.emit_observation(name, payload)
            s.bind(SensorCtx(emit=_emit, http=self._http,
                             store=scoped("sensor", s.name),
                             schedule=self._scheduler.for_owner(s.name)))

        await self._http.start()          # every route is registered by now

        for d in self._deciders:
            self._tasks.append(asyncio.create_task(self._run_decider(d)))
        for a in self._actuators:
            self._tasks.append(asyncio.create_task(self._run_actuator(a)))
        for t in self._taps:
            for log in t.logs:
                self._tasks.append(asyncio.create_task(self._run_tap(t, log)))
        for s in self._sensors:
            task = asyncio.create_task(s.start())
            task.add_done_callback(lambda t, n=s.name: self._sensor_exited(n, t))
            self._tasks.append(task)
            self._scheduler.start(s.name)
            self._started_owners.append(s.name)

        for owner in self._maintenance:
            self._scheduler.start(owner)

    def _sensor_exited(self, name, task):
        # A clean return is normal for a route-driven sensor and its timers must
        # survive; only a crash takes them down.
        if task.cancelled():
            return
        if task.exception() is not None:
            logger.error("sensor %s died; stopping its timers", name,
                         exc_info=task.exception())
            self._tasks.append(asyncio.create_task(self._scheduler.stop(name)))

    async def stop(self) -> None:
        self._running = False

        # 1. No new inbound work. uvicorn drains in-flight requests itself.
        await self._http.stop()

        # 2. Sensors down, each one's timers first: a scheduled callback must
        #    never fire against a connection being torn down.
        for name in self._started_owners:
            await self._scheduler.stop(name)
        for s in self._sensors:
            try:
                await s.stop()
            except Exception:
                logger.exception("sensor %s failed to stop", s.name)
        await self._scheduler.stop_all()

        # 3. Drain the consume loops before anything they depend on goes away.
        for t in self._tasks:
            t.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

        # 4. Now nothing is using them, so the clients can close.
        for a in self._actuators:
            close = getattr(a, "close", None)
            if close is not None:
                try:
                    await close()
                except Exception:
                    logger.exception("actuator %s failed to close", a.name)

        if self._owns_store:
            store_close = getattr(self._store, "close", None)
            if store_close is not None:
                store_close()
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
        async def handle(cmd):
            ctx = ActCtx(cmd=cmd,
                         _emit_result=lambda name, payload, cid:
                             self.emit_observation(name, payload, command_id=cid))
            await a.act(cmd, ctx)
        await self._consume(CMD_LOG, f"actuator/{a.name}",
                            Command.from_message, lambda c: c.name == a.name, handle)

    async def _run_tap(self, t, log):
        decode = Observation.from_message if log == OBS_LOG else Command.from_message
        await self._consume(log, f"tap/{t.name}/{log}", decode,
                            lambda v: True, lambda v: t.observe(log, v))
