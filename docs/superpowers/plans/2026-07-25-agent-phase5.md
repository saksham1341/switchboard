# Agent Phase 5 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The agent recovers from stuck sessions, forgets idle ones, and remembers what it is told — without adding a single ceiling.

**Architecture:** A `clock` sensor emits `clock.tick`; the agent decider subscribes and sweeps its own sessions, so the sweep arrives through the same serial consumer group as every other handler and cannot race an in-flight `decide()`. The stuck threshold is *derived* from the Bus's own retry configuration rather than picked, because the handler timeout is multiplied by 11 in the worst case and a hand-picked constant silently goes stale. Session records carry a sliding TTL; memory is two decider-injected tools over the existing `kv` actuator.

**Tech Stack:** Python 3.11+, existing `KeyStore`/mamamia substrate, pytest with `asyncio_mode = "auto"`.

**Design doc:** `docs/superpowers/specs/2026-07-25-agent-phase5-design.md`. SSOT: `docs/superpowers/specs/2026-07-23-agentic-decider-design.md` (§5.3, §6.4, §7.3, §7.5, §10.2).

## Global Constraints

- **No ceilings.** `MAX_SPEND`, the cost ledger and the transcript cap are out of scope as a category. If a task starts to look like a limit, stop and ask.
- **The watchdog must never fire on live work.** `stuck_after` is derived from `bus.worst_case_retry_seconds`, never a literal, and is deliberately not an env var.
- **The decider stays silent.** The watchdog sets `idle` and logs. No reaction, no message. Everything a user sees still comes from the model.
- **The decider owns no clock.** It reacts to `clock.tick`; it must not schedule anything itself.
- Every store record is JSON-encoded — the store is `str → str`.
- `isinstance` guard before `.get()`/iteration on anything parsed. This defect class has landed six times in this project.
- Run the suite with `source venv/bin/activate && pytest -q` from the repo root (note `venv/`, **not** `.venv/`, which is empty). Baseline is **405 passing**.

## File Structure

| file | responsibility |
|---|---|
| `switchboard/bus.py` (modify) | renamed knobs, `worst_case_retry_seconds`, clamps `retry_after` |
| `switchboard/backoff.py` (modify) | cap becomes a required argument the Bus supplies |
| `switchboard/actuators/llm/actuator.py` (modify) | drops `_RETRY_CAP`; reports the provider's delay unclamped |
| `switchboard/actuators/llm/backends/*.py` (modify) | `TIMEOUT` 120 → 60 |
| `switchboard/sensors/clock.py` (create) | emits `clock.tick` |
| `switchboard/deciders/agent/decider.py` (modify) | `busy_since`, tick sweep, `/reset`, memory tool routing |
| `switchboard/deciders/agent/session.py` (modify) | sliding TTL on both keys, `delete` |
| `switchboard/deciders/agent/memory.py` (create) | the two tool specs + key rewriting |
| `switchboard/dashboard/__init__.py` (modify) | consumes `switchboard.deadletter`; `refresh_dead` deleted |
| `switchboard/dashboard/stats.py` (modify) | `FRAME_KEYS` gains the dead-letter link fields |
| `switchboard/app.py` (modify) | env for every knob, wiring |

---

### Task 1: Retry configuration — names, env, and a derivable worst case

**Files:**
- Modify: `switchboard/bus.py`, `switchboard/backoff.py`, `switchboard/actuators/llm/actuator.py`, `switchboard/actuators/llm/backends/anthropic.py`, `switchboard/actuators/llm/backends/openai.py`, `switchboard/app.py`
- Test: `tests/test_backoff.py`, `tests/test_bus_framework.py`, `tests/test_app.py`, `tests/test_actuator_llm.py`, `tests/test_llm_openai.py`

**Interfaces produced** (used by Task 3):

```python
Bus(..., message_max_retries=10, handler_timeout_s=100.0,
    retry_backoff_max_s=300.0, retry_after_max_s=120.0,
    consumer_wait_ms=30_000, lease_reaper_interval_s=60.0,
    dedup_ttl_s=3600.0, log_max_messages=100_000, log_max_dead=500)

Bus.worst_case_retry_seconds -> float
```

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_bus_framework.py`:

```python
def test_worst_case_covers_both_retry_paths_and_handler_time():
    """The two retry paths are the jittered backoff ceiling and an explicit
    retry_after; a message takes the worse, PLUS the handler's own time on
    every attempt. The handler term is the one people forget and it is
    multiplied by (retries + 1)."""
    from switchboard.bus import Bus
    bus = Bus(":memory:", message_max_retries=10, handler_timeout_s=100.0,
              retry_backoff_max_s=300.0, retry_after_max_s=120.0)
    # jittered ceiling sum = 1+2+4+...+256 capped at 300 for the last = 811
    # explicit = 10 * 120 = 1200  (the worse path)
    # handler  = 11 * 100 = 1100
    assert bus.worst_case_retry_seconds == 1200 + 1100


def test_worst_case_takes_the_backoff_path_when_it_dominates():
    from switchboard.bus import Bus
    bus = Bus(":memory:", message_max_retries=10, handler_timeout_s=1.0,
              retry_backoff_max_s=300.0, retry_after_max_s=0.0)
    assert bus.worst_case_retry_seconds == 811 + 11


def test_worst_case_moves_with_the_handler_timeout():
    """The regression this property exists to prevent: raising the handler
    timeout must move anything derived from it, automatically."""
    from switchboard.bus import Bus
    lo = Bus(":memory:", handler_timeout_s=30.0).worst_case_retry_seconds
    hi = Bus(":memory:", handler_timeout_s=100.0).worst_case_retry_seconds
    assert hi - lo == 11 * 70.0


async def test_retry_after_is_clamped_by_the_bus_not_trusted(tmp_path):
    """retry_after is a request the Bus may clamp, not a promise it obeys. A
    provider answering with a daily-quota reset (~3593s) must not pin a
    consumer for an hour."""
    from switchboard.errors import RetryableError
    orch = _Orch()
    bus = _drive([_Msg(5, "boom")], orch)
    bus._retry_after_max_s = 120.0
    async def handle(v): raise RetryableError("quota", retry_after=3593.0)
    await bus._consume(CMD_LOG, "actuator/boom", Command.from_message, lambda v: True, handle)
    assert orch.retry_afters == [120.0]


async def test_a_retry_after_under_the_cap_is_honoured_exactly():
    from switchboard.errors import RetryableError
    orch = _Orch()
    bus = _drive([_Msg(5, "boom")], orch)
    bus._retry_after_max_s = 120.0
    async def handle(v): raise RetryableError("slow", retry_after=2.5)
    await bus._consume(CMD_LOG, "actuator/boom", Command.from_message, lambda v: True, handle)
    assert orch.retry_afters == [2.5]
```

Add to `tests/test_backoff.py`:

```python
def test_backoff_cap_is_supplied_by_the_caller():
    from switchboard.backoff import backoff
    # every draw sits in [cap/2, cap] once the ceiling is reached
    for _ in range(50):
        d = backoff(20, cap=10.0)
        assert 5.0 <= d <= 10.0
```

Add to `tests/test_app.py`:

```python
def test_env_names_reach_the_bus(tmp_path, monkeypatch):
    """Every knob is env-configurable and lands where its name says."""
    from switchboard.app import build
    cfg = {"mamamia_db_path": str(tmp_path / "mm.db"),
           "switchboard_db_path": str(tmp_path / "sb.db"),
           "github_secret": "s", "port": 8161,
           "message_max_retries": 3, "handler_timeout_s": 7.0,
           "retry_backoff_max_s": 11.0, "retry_after_max_s": 13.0,
           "log_max_messages": 100_000}
    bus, _ = build(cfg)
    assert bus._message_max_retries == 3
    assert bus._handler_timeout_s == 7.0
    assert bus._retry_backoff_max_s == 11.0
    assert bus._retry_after_max_s == 13.0
    assert bus._log_max_messages == 100_000


def test_log_max_messages_defaults_to_100k(tmp_path):
    """clock.tick emits 1440/day; at the old 10k default ticks would fill the
    log in about a week and evict real history."""
    from switchboard.app import build
    bus, _ = build({"mamamia_db_path": str(tmp_path / "mm.db"),
                    "switchboard_db_path": str(tmp_path / "sb.db"),
                    "github_secret": "s", "port": 8162})
    assert bus._log_max_messages == 100_000
```

Add to `tests/test_llm_openai.py` and `tests/test_actuator_llm.py`:

```python
async def test_a_429_reports_the_providers_delay_unclamped():
    """Clamping is the Bus's policy now. The backend reports what the provider
    said; a 3593s daily-quota answer travels intact and the Bus decides."""
    def handler(request):
        return httpx.Response(429, json={"error": {"message": "quota"}},
                              headers={"retry-after": "3593"})
    b = _backend(handler)
    with pytest.raises(RetryableError) as e:
        await b.complete({"model": "m", "messages": []})
    assert e.value.retry_after == 3593.0
    await b.close()
```

- [ ] **Step 2: Run to verify they fail**

Run: `source venv/bin/activate && pytest tests/test_bus_framework.py tests/test_backoff.py -q`
Expected: FAIL — `Bus.__init__() got an unexpected keyword argument 'message_max_retries'`.

- [ ] **Step 3: Rename the Bus knobs and add the property**

In `bus.py`, rename every constructor parameter and its attribute per the table below, update the ~9 use sites, and add the property. **No behaviour changes in this step beyond the renames and the two new defaults.**

| old | new | default |
|---|---|---|
| `max_retries` | `message_max_retries` | 10 |
| `default_timeout_s` | `handler_timeout_s` | **100.0** (was 30.0) |
| — | `retry_backoff_max_s` | 300.0 |
| — | `retry_after_max_s` | 120.0 |
| `wait_ms` | `consumer_wait_ms` | 30_000 |
| `reaper_interval` | `lease_reaper_interval_s` | 60.0 |
| `processed_ttl` | `dedup_ttl_s` | 3600.0 |
| `max_log_messages` | `log_max_messages` | **100_000** (was 10_000) |
| `max_dead` | `log_max_dead` | 500 |

```python
    @property
    def worst_case_retry_seconds(self) -> float:
        """Longest a message can legitimately stay in flight before it dead-letters.

        Two retry paths exist and a message takes the worse of them: the
        jittered backoff ceiling, or an explicit retry_after. Both are then
        paid on top of the handler's own time on EVERY attempt — the term
        people forget, and it is multiplied by (retries + 1).

        Anything watching for a stuck consumer must sit above this or it will
        kill live work. Derived rather than chosen precisely because the
        handler timeout is the most leveraged knob here: an 80s change to it
        moves this by 15 minutes.
        """
        jittered = sum(min(self._retry_backoff_max_s, BACKOFF_BASE * 2 ** i)
                       for i in range(self._message_max_retries))
        explicit = self._message_max_retries * self._retry_after_max_s
        return (max(jittered, explicit)
                + (self._message_max_retries + 1) * self._handler_timeout_s)
```

Import `BACKOFF_BASE` from `switchboard.backoff` (promote the `base=1.0` default to a module constant there; it stays unconfigurable — "start at one second" is not a thing anyone tunes).

In `_consume`, pass the cap through and clamp `retry_after`:

```python
                except RetryableError as e:
                    attempts = await orch.state_store.get_retry_count(log, group_id, msg.id)
                    if e.retry_after is None:
                        delay = backoff(attempts, cap=self._retry_backoff_max_s)
                    else:
                        # A request, not a promise. A provider can honestly answer
                        # with a daily-quota reset (~3593s); honouring that would
                        # pin the consumer for an hour.
                        delay = min(e.retry_after, self._retry_after_max_s)
                    await settle(msg.id, Outcome.RETRY, retry_after=delay)
                except Exception:
                    attempts = await orch.state_store.get_retry_count(log, group_id, msg.id)
                    await settle(msg.id, Outcome.RETRY,
                                 retry_after=backoff(attempts, cap=self._retry_backoff_max_s))
```

- [ ] **Step 4: Strip the cap from the llm actuator**

In `switchboard/actuators/llm/actuator.py`, delete `_RETRY_CAP` and the clamping inside `parse_retry_after` — it now returns the provider's value as-is (still guarding against a missing or unparseable header by returning `None`). Update the docstring: the cap moved to the Bus, because *how long to defer a message* is Bus policy, not a provider detail.

In both backends, `TIMEOUT = 120.0` → `TIMEOUT = 60.0` with the reason recorded:

```python
# One HTTP call, no tools run here, and max_tokens is capped by the caller —
# so generation is bounded and only provider queueing varies. A short timeout
# with retries also beats a long one: at 60s a request is far more likely dead
# than slow, and a retry gets a fresh connection. The Bus's handler timeout
# (100s) is the backstop above this, deliberately: the inner, specific timeout
# should fire first and produce a meaningful error.
TIMEOUT = 60.0
```

- [ ] **Step 5: Env in `app.py`**

Config keys mirror the Bus parameter names. In `run()`:

```python
        "message_max_retries": int(os.environ.get("SB_MESSAGE_MAX_RETRIES", "10")),
        "handler_timeout_s": float(os.environ.get("SB_HANDLER_TIMEOUT_S", "100")),
        "retry_backoff_max_s": float(os.environ.get("SB_RETRY_BACKOFF_MAX_S", "300")),
        "retry_after_max_s": float(os.environ.get("SB_RETRY_AFTER_MAX_S", "120")),
        "consumer_wait_ms": int(os.environ.get("SB_CONSUMER_WAIT_MS", "30000")),
        "lease_reaper_interval_s": float(os.environ.get("SB_LEASE_REAPER_INTERVAL_S", "60")),
        "dedup_ttl_s": float(os.environ.get("SB_DEDUP_TTL_S", "3600")),
        "log_max_messages": int(os.environ.get("SB_LOG_MAX_MESSAGES", "100000")),
        "log_max_dead": int(os.environ.get("SB_LOG_MAX_DEAD", "500")),
```

and pass each into `Bus(...)` in `build()`. Update `docker-compose.yml` and `.env.example` with the same names and a one-line description each.

- [ ] **Step 6: Run the full suite**

Run: `source venv/bin/activate && pytest -q`
Expected: PASS. Report the count. Any test constructing `Bus(...)` with an old keyword must be updated — that is the rename landing, not a regression.

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "refactor: name every retry knob for its job, derive the worst case"
```

---

### Task 2: The `clock` sensor

**Files:**
- Create: `switchboard/sensors/clock.py`
- Modify: `switchboard/app.py`
- Test: `tests/test_sensor_clock.py` (create), `tests/test_app.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_sensor_clock.py`:

```python
from switchboard.sensors.clock import ClockSensor


class _Ctx:
    def __init__(self):
        self.emitted = []
        self.scheduled = []
        self.schedule = self
        self.store = None
    def every(self, seconds, fn, **kw):
        self.scheduled.append((seconds, fn, kw))
    async def emit(self, name, payload, text=None):
        self.emitted.append((name, payload)); return len(self.emitted)


def test_declares_its_timer_at_bind():
    s = ClockSensor(interval=30.0)
    ctx = _Ctx(); s.bind(ctx)
    assert ctx.scheduled and ctx.scheduled[0][0] == 30.0


async def test_first_tick_has_no_delta():
    """There is no previous tick. Reporting 0.0 would be a number a consumer
    could act on; null says 'unknown' honestly."""
    s = ClockSensor(); ctx = _Ctx(); s.bind(ctx)
    await s.tick()
    name, payload = ctx.emitted[0]
    assert name == "clock.tick"
    assert payload["delta"] is None
    assert payload["seq"] == 1
    assert isinstance(payload["at"], float)


async def test_delta_is_measured_not_assumed():
    """A blocked loop or slow consumer makes the real gap larger than the
    configured interval. A consumer reasoning about elapsed time must see
    what actually happened."""
    s = ClockSensor(interval=60.0); ctx = _Ctx(); s.bind(ctx)
    times = iter([1000.0, 1000.5, 1090.0])
    s._now = lambda: next(times)
    await s.tick(); await s.tick(); await s.tick()
    deltas = [p["delta"] for _, p in ctx.emitted]
    assert deltas[0] is None
    assert deltas[1] == 0.5          # not 60.0
    assert deltas[2] == 89.5


async def test_seq_is_monotonic_so_a_missed_tick_is_visible():
    s = ClockSensor(); ctx = _Ctx(); s.bind(ctx)
    for _ in range(3):
        await s.tick()
    assert [p["seq"] for _, p in ctx.emitted] == [1, 2, 3]


async def test_a_failed_emit_does_not_break_the_sequence():
    """The scheduler survives a raising callback (test_scheduler pins that), so
    the next tick must still be coherent rather than repeating a seq."""
    s = ClockSensor(); ctx = _Ctx(); s.bind(ctx)
    async def boom(name, payload, text=None): raise RuntimeError("nope")
    ctx.emit = boom
    try:
        await s.tick()
    except RuntimeError:
        pass
    ctx.emit = _Ctx.emit.__get__(ctx)
    await s.tick()
    assert ctx.emitted[0][1]["seq"] == 2
```

Add to `tests/test_app.py`:

```python
def test_the_clock_sensor_is_always_wired(tmp_path):
    """It has no dependencies and nothing to configure — like the kv actuator,
    it ships with the platform and sits idle until something subscribes."""
    from switchboard.app import build
    bus, _ = build({"mamamia_db_path": str(tmp_path / "mm.db"),
                    "switchboard_db_path": str(tmp_path / "sb.db"),
                    "github_secret": "s", "port": 8163})
    assert "clock" in bus.topology()["sensors"]
```

- [ ] **Step 2: Run to verify they fail**

Run: `source venv/bin/activate && pytest tests/test_sensor_clock.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'switchboard.sensors.clock'`.

- [ ] **Step 3: Implement**

Create `switchboard/sensors/clock.py`:

```python
"""The clock — the substrate's only source of "time passed".

A decider must not own a timer: it is a pure function of observations, and a
timer would let it act with no observation, which is sensor-nature in the wrong
role. So periodic work is expressed as a subscription to a tick rather than as
a scheduled callback, and the work then runs through the normal consume loop —
serially, under the same settle discipline as everything else.

This sensor knows nothing about sessions, agents, or what anyone does with a
tick. It is a clock.
"""
import time


class ClockSensor:
    name = "clock"

    def __init__(self, interval: float = 60.0):
        self._interval = interval
        self._seq = 0
        self._last: float | None = None
        self.ctx = None

    @staticmethod
    def _now() -> float:
        return time.time()

    def bind(self, ctx) -> None:
        self.ctx = ctx
        ctx.schedule.every(self._interval, self.tick)

    async def tick(self) -> None:
        now = self._now()
        # delta is MEASURED, never the configured interval: a blocked event loop
        # or a slow consumer makes the real gap larger, and a consumer reasoning
        # "have N seconds passed" must see what happened rather than what was
        # scheduled. The first tick reports null — there is no previous tick,
        # and 0.0 would be a number a consumer could act on.
        delta = None if self._last is None else now - self._last
        self._seq += 1
        self._last = now
        await self.ctx.emit("clock.tick",
                            {"at": now, "delta": delta, "seq": self._seq})

    async def start(self) -> None:
        return                       # timer-driven: no loop to supervise

    async def stop(self) -> None:
        return
```

Wire it unconditionally in `app.py` alongside the other always-on sensors, with `interval=config.get("clock_tick_s", 60.0)` and `"clock_tick_s": float(os.environ.get("SB_CLOCK_TICK_S", "60"))` in `run()`.

- [ ] **Step 4: Run to verify they pass, then the full suite**

Run: `source venv/bin/activate && pytest -q`
Expected: PASS. Report the count.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat: a clock sensor, so periodic work is a subscription not a timer"
```

---

### Task 3: The stuck-session watchdog

**Files:**
- Modify: `switchboard/deciders/agent/decider.py`, `switchboard/app.py`
- Test: `tests/test_agent_decider.py`, `tests/test_app.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_agent_decider.py`:

```python
def _tick(at=1000.0, delta=60.0, seq=1, oid=900):
    return _obs("clock.tick", {"at": at, "delta": delta, "seq": seq}, oid=oid)


def test_subscribes_to_the_clock():
    assert _agent().subscribes(_tick())


async def test_a_tick_frees_a_session_stuck_past_the_threshold():
    a = _agent(stuck_after=100.0)
    await _mint(a)                                   # goes busy
    s = await a._sessions.load(100)
    s["busy_since"] = 500.0
    await a._sessions.save(s)
    rec = await _deliver(a, _tick(at=1000.0))        # 500s later
    assert rec.emitted == []                         # silent: no post, no react
    s = await a._sessions.load(100)
    assert s["state"] == "idle"
    assert s["busy_since"] is None


async def test_a_tick_leaves_a_session_inside_the_threshold_alone():
    """The failure this guards: firing on live work. A session legitimately
    retrying must not be reset."""
    a = _agent(stuck_after=100.0)
    await _mint(a)
    s = await a._sessions.load(100)
    s["busy_since"] = 950.0
    await a._sessions.save(s)
    await _deliver(a, _tick(at=1000.0))              # only 50s
    assert (await a._sessions.load(100))["state"] == "busy"


async def test_an_idle_session_is_untouched_by_a_tick():
    a = _agent(stuck_after=100.0)
    cid = await _mint(a)
    await _deliver(a, _obs("llm.ok", {"stop_reason": "end_turn", "content": []},
                           oid=200, command_id=cid))     # -> idle
    await _deliver(a, _tick(at=99999.0))
    s = await a._sessions.load(100)
    assert s["state"] == "idle" and s["turn"] == 1


async def test_busy_since_is_set_on_advance_and_cleared_on_finish():
    a = _agent()
    cid = await _mint(a)
    assert isinstance((await a._sessions.load(100))["busy_since"], float)
    await _deliver(a, _obs("llm.ok", {"stop_reason": "end_turn", "content": []},
                           oid=200, command_id=cid))
    assert (await a._sessions.load(100))["busy_since"] is None


async def test_a_tick_with_no_sessions_is_harmless():
    a = _agent(stuck_after=100.0)
    rec = await _deliver(a, _tick())
    assert rec.emitted == []
```

Add to `tests/test_app.py`:

```python
def test_the_watchdog_threshold_is_derived_from_the_bus_not_hardcoded(tmp_path):
    """A literal would silently go stale: the handler timeout is multiplied by
    (retries + 1), so changing it moves the legitimate-work window by minutes.
    Derived, the watchdog follows."""
    from switchboard.app import build
    cfg = {"mamamia_db_path": str(tmp_path / "mm.db"),
           "switchboard_db_path": str(tmp_path / "sb.db"),
           "github_secret": "s", "port": 8164,
           "discord_bot_token": "t", "discord_application_id": "1",
           "llm_backend": "openai", "llm_api_key": "k",
           "llm_base_url": "http://x", "llm_model": "m",
           "handler_timeout_s": 100.0}
    bus, _ = build(cfg)
    agent = next(d for d in bus._deciders if d.name == "agent")
    assert agent._stuck_after > bus.worst_case_retry_seconds


def test_raising_the_handler_timeout_moves_the_watchdog(tmp_path):
    from switchboard.app import build
    def mk(port, t):
        bus, _ = build({"mamamia_db_path": str(tmp_path / f"mm{port}.db"),
                        "switchboard_db_path": str(tmp_path / f"sb{port}.db"),
                        "github_secret": "s", "port": port,
                        "discord_bot_token": "t", "discord_application_id": "1",
                        "llm_backend": "openai", "llm_api_key": "k",
                        "llm_base_url": "http://x", "llm_model": "m",
                        "handler_timeout_s": t})
        return next(d for d in bus._deciders if d.name == "agent")._stuck_after
    assert mk(8165, 100.0) > mk(8166, 30.0)
```

- [ ] **Step 2: Run to verify they fail**

Run: `source venv/bin/activate && pytest tests/test_agent_decider.py -q`
Expected: FAIL — `AgentDecider.__init__() got an unexpected keyword argument 'stuck_after'`.

- [ ] **Step 3: Implement**

`AgentDecider.__init__` gains `stuck_after: float` (keyword-required — there is no safe default, since the correct value depends on Bus configuration the decider cannot see).

**Widen the test helper in the same step.** `tests/test_agent_decider.py` has **59** `_agent()` call sites, and a keyword-required parameter breaks every one of them. Add the default to the helper, not to production:

```python
def _agent(**kw):
    kw.setdefault("model", "test-model")
    kw.setdefault("stuck_after", 1800.0)
    a = AgentDecider(tools=[TOOL], **kw)
```

Production keeps no default — a decider that guessed its own threshold is the failure this whole derivation exists to prevent.

`session.py`'s `new()` adds `"busy_since": None`.

In `_advance`, alongside `s["state"] = "busy"`, set `s["busy_since"] = time.time()`. In `_finish`, when setting `idle`, set `s["busy_since"] = None`.

`subscribes` gains `or obs.name == "clock.tick"`. In `decide()`, before the `command_id` handling:

```python
        if obs.name == "clock.tick":
            return await self._sweep_stuck(obs, ctx)
```

```python
    async def _sweep_stuck(self, obs, ctx) -> None:
        """Free sessions that have been busy longer than any legitimate retry
        chain could take.

        This runs on a tick rather than on a timer, and that is the point: it
        arrives as an observation through the decider's own consumer group, so
        it is serial with every other handler. A maintenance timer would run
        outside the consume loop and could interleave with an in-flight
        decide() mid-await, clobbering the very session record it is reading
        (§5.3 — the settle discipline IS the concurrency control).

        Silent by design: the session goes idle and the event is logged, but
        nothing is posted. Everything a user sees still comes from the model.
        """
        payload = obs.payload if isinstance(obs.payload, dict) else {}
        now = payload.get("at")
        if not isinstance(now, (int, float)):
            return
        for key in await self.ctx.store.keys("session:"):
            sid = key.split(":", 1)[1]
            s = await self._sessions.load(int(sid)) if sid.isdigit() else None
            if s is None or s.get("state") != "busy":
                continue
            since = s.get("busy_since")
            if not isinstance(since, (int, float)) or now - since < self._stuck_after:
                continue
            logger.warning("session %s stuck busy for %.0fs; freeing",
                           s["sid"], now - since)
            s["state"] = "idle"
            s["busy_since"] = None
            await self._sessions.save(s)
```

In `app.py`: `AgentDecider(..., stuck_after=bus.worst_case_retry_seconds * 1.2)`.

- [ ] **Step 4: Verify the guard is load-bearing**

Temporarily change the comparison to `now - since >= 0` (fire always), and run:

Run: `source venv/bin/activate && pytest tests/test_agent_decider.py::test_a_tick_leaves_a_session_inside_the_threshold_alone -q`
Expected: FAIL — a live session was reset. Restore and re-run; expected PASS.

- [ ] **Step 5: Full suite and commit**

```bash
git add -A
git commit -m "feat: stuck-session watchdog, driven by clock.tick"
```

---

### Task 4: Session TTL and `/reset`

**Files:**
- Modify: `switchboard/deciders/agent/session.py`, `switchboard/deciders/agent/decider.py`, `switchboard/app.py`
- Test: `tests/test_agent_session.py`, `tests/test_agent_decider.py`

**The trap this task exists to avoid:** the session record is rewritten every turn, but the route key (`thread:<source>:<key>`) is written **once**, at mint. Give only the session a sliding TTL and the route expires 4h after the conversation *started*, even while it is active — orphaning a live session whose next message can no longer find it. §6.4 requires them to expire *together*, so **every `save()` must refresh both**.

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_agent_session.py`:

```python
class _TtlStore:
    """Records the ttl every key was last written with."""
    def __init__(self):
        self._v = {}
        self.ttls = {}
    async def get(self, key): return self._v.get(key)
    async def set(self, key, value, *, ttl=None):
        self._v[key] = value; self.ttls[key] = ttl
    async def delete(self, key): self._v.pop(key, None); self.ttls.pop(key, None)
    async def keys(self, prefix=""):
        return [k for k in self._v if k.startswith(prefix)]


async def test_save_refreshes_the_ttl_on_both_the_session_and_its_route():
    """The route is written once at mint but the session every turn. Without
    refreshing both, an ACTIVE conversation loses its route after one TTL and
    the next message cannot find the session."""
    store = _TtlStore()
    sess = Sessions(store, ttl=3600.0)
    s = await sess.new(sid=100, source="discord", channel_id="222",
                       thread_id="222", anchor="1")
    await sess.set_route("discord", "222", 100)
    store.ttls.clear()
    await sess.save(s)
    assert store.ttls["session:100"] == 3600.0
    assert store.ttls["thread:discord:222"] == 3600.0


async def test_delete_removes_both_keys():
    store = _TtlStore()
    sess = Sessions(store, ttl=3600.0)
    s = await sess.new(sid=100, source="discord", channel_id="222",
                       thread_id="222", anchor="1")
    await sess.set_route("discord", "222", 100)
    await sess.delete(s)
    assert await sess.load(100) is None
    assert await sess.route("discord", "222") is None
```

Add to `tests/test_agent_decider.py`:

```python
async def test_reset_clears_the_session_for_its_channel():
    a = _agent()
    await _deliver(a, _obs("discord.message", _message()))
    assert await a._sessions.load(100) is not None
    rec = await _deliver(a, _obs("discord.command.reset",
                                 {"channel_id": "222", "interaction_token": "tok"},
                                 oid=300))
    assert await a._sessions.load(100) is None
    assert await a._sessions.route("discord", "222") is None
    assert [n for n, _, _ in rec.emitted] == ["discord.reply_to_command"]


async def test_reset_on_a_channel_with_no_session_still_acknowledges():
    a = _agent()
    rec = await _deliver(a, _obs("discord.command.reset",
                                 {"channel_id": "999", "interaction_token": "tok"},
                                 oid=300))
    assert [n for n, _, _ in rec.emitted] == ["discord.reply_to_command"]


def test_subscribes_to_reset():
    assert _agent().subscribes(_obs("discord.command.reset", {}))
```

- [ ] **Step 2: Run to verify they fail, then implement**

`Sessions.__init__` gains `ttl: float | None = None`. `save()` writes the session **and** re-writes the route with the same ttl — it needs the route key, so the session record already carrying `source`, `thread_id` and `channel_id` is sufficient to reconstruct it:

```python
    def _route_key(self, s) -> str:
        return f"thread:{s['source']}:{s.get('thread_id') or s['channel_id']}"

    async def save(self, s) -> None:
        await self._store.set(f"session:{s['sid']}", json.dumps(s), ttl=self._ttl)
        # The route must slide with the session. It is written once at mint but
        # the session is rewritten every turn, so refreshing only the session
        # would expire the route out from under a live conversation (§6.4:
        # tracking and conversation expire together).
        await self._store.set(self._route_key(s), str(s["sid"]), ttl=self._ttl)

    async def delete(self, s) -> None:
        await self._store.delete(f"session:{s['sid']}")
        await self._store.delete(self._route_key(s))
```

`set_route` also passes `ttl=self._ttl`.

Decider: `subscribes` gains `discord.command.reset`; `decide()` routes it to a handler that looks up the route for `channel_id`, deletes the session if found, and always emits `discord.reply_to_command` with the interaction token so the user gets an acknowledgement either way.

`app.py`: add `CommandSpec("reset", "Clear this channel's conversation")` to `DISCORD_COMMANDS`, and pass `session_ttl_s=config.get("session_ttl_s", 14400.0)` into `AgentDecider`, which forwards it to `Sessions(ctx.store, ttl=self._session_ttl_s)` in `bind()`. Env `SB_SESSION_TTL_S` default `14400`.

`_agent()` needs `kw.setdefault("session_ttl_s", None)` for the same reason as Step 3 above — a `None` ttl keeps existing tests on non-expiring sessions, so they assert what they already assert.

- [ ] **Step 3: Full suite and commit**

```bash
git add -A
git commit -m "feat: sliding session TTL on both keys, and /reset"
```

---

### Task 5: `scratchpad` and `memory`

**Files:**
- Create: `switchboard/deciders/agent/memory.py`
- Modify: `switchboard/deciders/agent/decider.py`
- Test: `tests/test_agent_memory.py` (create)

**Interfaces produced:**

```python
MEMORY_TOOLS: list[dict]            # the two tool specs the decider injects
def rewrite(tool_name, args, sid, ttl) -> dict | None   # tool args -> kv command args
```

- [ ] **Step 1: Write the failing tests**

Create `tests/test_agent_memory.py`:

```python
import pytest

from switchboard.deciders.agent.memory import MEMORY_TOOLS, rewrite


def _names(): return {t["name"] for t in MEMORY_TOOLS}


def test_the_agent_sees_two_tools_and_never_raw_kv():
    assert _names() == {"scratchpad", "memory"}


def test_every_kv_op_is_exposed():
    for t in MEMORY_TOOLS:
        assert set(t["input_schema"]["properties"]["op"]["enum"]) == {
            "get", "set", "delete", "list"}


def test_scratchpad_is_namespaced_to_its_session():
    args = rewrite("scratchpad", {"op": "set", "key": "draft", "value": "x"},
                   sid=100, ttl=3600.0)
    assert args["key"] == "session:100:draft"
    assert args["ttl"] == 3600.0


def test_memory_is_global_and_has_no_ttl():
    args = rewrite("memory", {"op": "set", "key": "prefs", "value": "x"},
                   sid=100, ttl=3600.0)
    assert args["key"] == "global:prefs"
    assert args.get("ttl") is None


def test_a_session_cannot_name_a_key_outside_its_own_scratchpad():
    """The prefix is a security boundary, not wiring: the decider applies it,
    not the model, so a prompt-injected agent cannot reach another session."""
    args = rewrite("scratchpad", {"op": "get", "key": "../../session:999:secret"},
                   sid=100, ttl=None)
    assert args["key"].startswith("session:100:")
    assert "session:999" not in args["key"].removeprefix("session:100:")


def test_list_is_scoped_to_the_namespace_not_the_whole_store():
    args = rewrite("scratchpad", {"op": "list"}, sid=100, ttl=None)
    assert args["prefix"] == "session:100:"
    args = rewrite("memory", {"op": "list"}, sid=100, ttl=None)
    assert args["prefix"] == "global:"


def test_an_unknown_op_is_rejected_before_a_command_is_emitted():
    assert rewrite("memory", {"op": "drop_everything", "key": "k"},
                   sid=100, ttl=None) is None


def test_a_non_memory_tool_is_not_ours():
    assert rewrite("discord.post", {"content": "hi"}, sid=100, ttl=None) is None
```

Add to `tests/test_agent_decider.py`:

```python
async def test_a_memory_tool_call_becomes_a_kv_command():
    a = _agent()
    cid = await _mint(a)
    rec = await _deliver(a, _llm_ok(
        [_use("toolu_A", name="memory", args={"op": "set", "key": "n", "value": "sak"})],
        command_id=cid))
    name, args, _ = rec.emitted[0]
    assert name == "kv"                       # the agent never addresses kv itself
    assert args["key"] == "global:n"


async def test_the_memory_tools_are_offered_to_the_model():
    a = _agent()
    rec = await _deliver(a, _obs("discord.message", _message()))
    _, args, _ = rec.emitted[0]
    assert {"scratchpad", "memory"} <= {t["name"] for t in args["tools"]}


async def test_a_kv_result_is_attributed_to_the_memory_tool_use():
    """The command emitted is kv, but the tool_result must carry the memory
    tool_use_id or the gather never closes."""
    a = _agent()
    cid = await _mint(a)
    rec = await _deliver(a, _llm_ok(
        [_use("toolu_A", name="memory", args={"op": "get", "key": "n"})],
        command_id=cid))
    kv_cid = rec.emitted[0][2]
    rec2 = await _deliver(a, _obs("kv.ok", {"value": "sak"}, oid=400, command_id=kv_cid))
    assert [n for n, _, _ in rec2.emitted] == ["llm"]
    s = await a._sessions.load(100)
    assert s["messages"][-1]["content"][0]["tool_use_id"] == "toolu_A"
```

- [ ] **Step 2: Implement**

Create `switchboard/deciders/agent/memory.py` with the two tool specs (both `op`-dispatched with an `enum` of the four `kv` ops, and descriptions that tell the model what each namespace is *for*: scratchpad is working notes that die with the conversation, memory is what it should still know weeks later) and `rewrite()`.

`rewrite` returns `None` for anything that is not a memory tool or carries an unknown `op`, so the decider can tell "not mine" from "mine but invalid".

**Key sanitisation matters here**: the prefix is a security boundary (§7.3), so `rewrite` must ensure the final key genuinely starts with the namespace — strip `/`, `..` and any leading namespace-looking prefix from the model-supplied key rather than trusting it.

In the decider's `_on_response` fan-out, before the `known` check: if `rewrite(name, input, sid, ttl)` returns args, emit a `kv` command with them and record the pending entry with the **original** `tool_use_id`. The existing gather machinery then resolves it unchanged. Add `MEMORY_TOOLS` to the tool list sent in `_advance`, and to the `known` set so they are never treated as hallucinated.

- [ ] **Step 3: Full suite and commit**

```bash
git add -A
git commit -m "feat: scratchpad and memory tools over the kv actuator"
```

---

### Task 6: The dashboard stops polling for dead letters

**Files:**
- Modify: `switchboard/dashboard/__init__.py`, `switchboard/dashboard/stats.py`, `switchboard/app.py`
- Test: `tests/test_dashboard.py`

**Why this is a small change:** `DashboardTap` already observes both logs, so it *already sees* every `switchboard.deadletter` observation — it just ignores them and polls `message_state` on a 5s timer instead. §10.2 already claims the sensor "lets the dashboard drop its own poll"; this makes that true.

**The one judgement call, in the security-sensitive allowlist.** The projection is structure-only, which is what makes the public page acceptable. A dead-letter frame needs to say *which* message died, so `FRAME_KEYS` gains `dead_log` and `dead_id`. These are ids and a log name — the same class as the `observation_id` and `command_id` already carried, and precisely the "causal links" the projection exists to show. No payload content crosses.

- [ ] **Step 1: Write the failing tests**

```python
def test_the_deadletter_projection_carries_the_link_and_no_payload():
    class M:
        id = 9
    m = M()
    m.payload = {"log": "cmd", "message_id": 44, "name": "llm",
                 "secret": "SHOULD NOT APPEAR"}
    m.metadata = {"name": "switchboard.deadletter", "emitted_by": "sensor/deadletter"}
    frame = project("obs", Observation.from_message(m))
    assert set(frame) == set(FRAME_KEYS)
    assert frame["dead_log"] == "cmd" and frame["dead_id"] == 44
    assert "SHOULD NOT APPEAR" not in repr(frame)


def test_an_ordinary_frame_has_no_dead_fields():
    frame = project("obs", _obs())
    assert frame["dead_log"] is None and frame["dead_id"] is None


async def test_ingest_marks_dead_from_a_deadletter_frame(tmp_path):
    dash = _dash(tmp_path)
    seen = asyncio.Queue(); dash._clients.add(seen)
    app = Starlette(routes=[Route("/dashboard/ingest", dash.ingest, methods=["POST"])])
    TestClient(app).post("/dashboard/ingest",
        json={"frames": [{"log": "obs", "id": 9, "name": "switchboard.deadletter",
                          "dead_log": "cmd", "dead_id": 44, "seen_at": 0}]},
        headers={"Authorization": "Bearer t0ken"})
    assert {"log": "cmd", "id": 44} in dash._dead


def test_the_dashboard_no_longer_polls(tmp_path):
    from switchboard.app import build
    from switchboard.dashboard import Dashboard
    assert not hasattr(Dashboard, "refresh_dead")
    bus, _ = build({"mamamia_db_path": str(tmp_path / "mm.db"),
                    "switchboard_db_path": str(tmp_path / "sb.db"),
                    "github_secret": "s", "port": 8167,
                    "dashboard_token": "t0ken",
                    "dashboard_ingest_url": "http://127.0.0.1:8167/dashboard/ingest"})
    assert "dashboard-dead" not in bus._maintenance
```

- [ ] **Step 2: Implement**

`project()` reads `payload.get("log")` / `payload.get("message_id")` **only** when the observation is named `switchboard.deadletter`, and emits `None` for both otherwise. `ingest` accumulates dead entries from frames instead of from a poll. Delete `refresh_dead`, `dead_message_ids`, and the `schedule_maintenance("dashboard-dead", ...)` wiring.

The page's existing `dead` handling is unchanged — it still receives `{"type": "dead", "dead": [...]}` on the stream; only the source of that list moves.

- [ ] **Step 3: Full suite and commit**

```bash
git add -A
git commit -m "fix: dashboard subscribes to deadletters instead of polling"
```

---

### Task 7: Spec, deploy, verify

**Files:**
- Modify: `docs/superpowers/specs/2026-07-23-agentic-decider-design.md`, `.env.example`, `docker-compose.yml`

- [ ] **Step 1: Update the SSOT**

- §3 store diagram: session record gains `busy_since`.
- §4: add `clock.tick` to the observations table, and `kv` is now reachable via the two decider-injected tools.
- §6.4: drop the "**Phase 5, none of this is built**" banner and the future-tense note — it is built. Record that `save()` refreshes both keys and why.
- §7.3: mark the memory tools built; record global-not-per-user, all four ops, on-demand recall, and the §6.2 note that a richer memory replaces the *actuator*, never `KeyStore`.
- §9: the watchdog is built; `MAX_SPEND` and the ledger remain outstanding, so the "runs watched" constraint stands.
- §10.2: the dashboard now subscribes — "one poller, many subscribers" is true.
- §12: add the persistent-memory-injection hole with its trigger (before the bot joins a guild containing anyone outside the trust boundary, or processes input from a public source), and note per-user namespacing as the known mitigation.
- §13: mark built — clock sensor, watchdog, TTL, `/reset`, memory tools.

- [ ] **Step 2: Deploy and verify**

Clear agent state (the session shape changed — `busy_since` is new, and old records have no TTL):

```bash
venv/bin/python -c "
import sqlite3
c=sqlite3.connect('.devdata/switchboard.db')
n=c.execute(\"DELETE FROM kv WHERE key LIKE 'decider/agent/%'\").rowcount
c.commit(); print(f'cleared {n} agent keys')
"
```

Restart local dev, confirm `/health` returns 200, then verify in the logs that `clock.tick` observations are landing at the configured interval and that the agent is not reacting to them.

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "docs: Phase 5 in the SSOT"
```

---

## Not in scope

| deferred | why |
|---|---|
| `MAX_SPEND`, global cost ledger, transcript cap | limits; build capability first, add ceilings with evidence |
| `pydantic-settings` | orthogonal refactor; the names land here, which is the durable part |
| per-user memory | additive later, same decider-side prefix |
| hierarchical/graph memory | a new actuator, never a wider `KeyStore` |
| mamamia message timestamps | a real gap, different repo, not needed here |
| the decider speaking | the watchdog is silent; making it a speaker is a deliberate later decision |
