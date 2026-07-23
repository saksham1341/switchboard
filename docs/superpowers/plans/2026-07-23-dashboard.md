# Dashboard Implementation Plan

**Goal:** Ship the live dashboard end to end so it can be watched running, then tuned against the real thing.

**Spec:** `docs/superpowers/specs/2026-07-23-dashboard-design.md`

**Approach:** Built in one pass rather than task-by-task — it is one new package with one consumer, and the front end is judged by eye, not by review. Tests cover the parts that can silently rot: the projection allowlist, the non-blocking tap, and ingest auth.

## Global Constraints

- Frame keys are exactly `log, id, name, emitted_by, observation_id, command_id, seen_at`. Nothing else, ever — payloads must not reach the wire.
- `observe()` never awaits the network. Bounded queue (256), drop **oldest** on overflow.
- Ingest requires `Authorization: Bearer <SB_DASHBOARD_TOKEN>`, compared with `hmac.compare_digest`.
- No token ⇒ no dashboard: no routes registered, no tap added. Never a default token.
- Log columns show no depth and no counts.
- Latency is dashboard-observed and labelled as such.
- Dead letters are the only polled data, every 5s.

## Files

- Create `switchboard/dashboard/__init__.py` — `Dashboard`, `DashboardTap`
- Create `switchboard/dashboard/stats.py` — dead-letter ids + backfill (read-only SQL)
- Create `switchboard/dashboard/page.html` — front end, adapted from `docs/dashboard-prototype.html`
- Create `tests/test_dashboard.py`
- Modify `switchboard/bus.py` — `topology()`, and close taps at shutdown alongside actuators
- Modify `switchboard/app.py` — wiring, gated on the token
- Modify `pyproject.toml` — ship `page.html` in the wheel
- Modify `.env.example`, `docker-compose.yml` — the two new env vars

## Steps

- [ ] **1. `Bus.topology()` + tap teardown.** `{"sensors": [...], "deciders": [...], "actuators": [...]}` by name. `Bus.stop()` gains a tap-close loop mirroring the actuator one, so the tap's sender task does not outlive the bus.
- [ ] **2. `stats.py`.** `dead_message_ids(db)` → `{(log_id, message_id)}`; `backfill(db, limit)` → newest-first frames across both logs. Read-only, same precedent as `cli.py`.
- [ ] **3. `DashboardTap`.** Project → `put_nowait` → drop oldest on overflow. Lazy-started sender batches and POSTs with a 2s timeout, never retries, counts drops.
- [ ] **4. `Dashboard`.** `ingest` (bearer-checked), `stream` (SSE, per-client bounded queue), `page` (file from disk), `refresh_dead` (5s timer).
- [ ] **5. `page.html`.** Prototype minus the simulator, plus `EventSource` and a backfill fetch. Same spine, traces, feed drawer.
- [ ] **6. Wiring + packaging + env.**
- [ ] **7. Run it locally, watch a real event traverse the spine.**

## Tests

Projection has no extra keys and drops payloads · `observe()` returns while the sender is hung · full queue drops oldest and counts it · ingest 401 without/with wrong token, 202 with right one · a frame reaches every stream client · `topology()` lists registered roles · no token ⇒ no routes, no tap.
