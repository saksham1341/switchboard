# Live Dashboard — Design

**Goal:** A read-only page that makes Switchboard's event flow visible — the spine of sensors, logs, deciders and actuators, with events travelling it live and the last minute of traffic still glowing after things go quiet.

**Status:** Approved design (co-designed in conversation, against a working prototype at `docs/dashboard-prototype.html`). Additive — no change to the four-role model, the two logs, or any existing role.

**Depends on:** `emitted_by` message attribution, merged in #13. Without it a command cannot be traced back to the decider that emitted it, and the spine could only draw an anonymous middle band.

---

## What it is, and what it is not

It is a **product built on the sensor platform**, not part of it. It ships as one object that the app wires in two halves — a tap that reads both logs, and routes on the shared `HttpServer`. The platform needs no changes to accommodate it.

It is **not a role**. "Dashboard" is a view; roles are the control loop. Making it a no-op sensor would buy uniform registration at the price of handing it an `emit` it must promise never to call — the same defect that kept `http` out of `TapCtx`.

It is **not an ops console**. Nothing it serves mutates anything. No retry buttons, no replay, no muting. Those need auth and a write path, and would stop the tap half being a tap.

---

## The shape

```
DashboardTap.observe(log, view)          both logs, in-process
        │  project to structure-only     payloads never leave here
        ▼
   bounded queue                          put_nowait, drop-oldest — never blocks
        │
        ▼  background sender, batched
POST /dashboard/ingest                    Authorization: Bearer <token>
        │
        ▼
   Dashboard broadcaster
        │
        ├──▶ GET /dashboard/stream        SSE, one queue per browser
        └──▶ GET /                        the page (single HTML file)
```

### Why the tap POSTs instead of fanning out in-process

An in-process broadcaster would be a dozen lines and cannot fail. The HTTP hop buys one thing: **the dashboard becomes a standalone ingest surface**. A second Switchboard, or a dashboard moved off the Pi entirely, feeds the same endpoint with no change to the tap — only its configured URL changes. Today that URL is loopback and the POST is a self-call; making it configuration is the whole point.

The cost is honest and must be designed around: an in-memory queue `put` cannot stall, an HTTP POST can. That is why the tap side is strictly fire-and-forget (below).

### A tap that opens a socket

The role model says a tap effects nothing. This one does I/O. The distinction the spec claims: it **writes to no log and changes no world state** — it exports a projection of what it already read. That is an export, not an effect.

Stated plainly rather than quietly widened, because it is the one place the dashboard bends a role definition.

---

## The tap must be unable to slow the relay

A tap is a consumer group like any other. If `observe()` blocks, its consume loop blocks, its lease expires, and the log backs up. A dashboard that can do that is worse than no dashboard.

So:

- `observe()` **only** projects and calls `put_nowait`. It never awaits the network.
- The queue is bounded (256). On overflow the **oldest** frame is dropped, not the newest — a browser catching up cares about recent events, and dropping the newest would make the visible tail lie.
- One background sender task drains the queue, batching whatever is available into a single POST.
- The POST has a short timeout (2s). On any error — timeout, connection refused, non-2xx — it logs at debug and drops the batch. It never retries: retrying a visualisation frame delivers stale animation and risks a backlog.
- A dropped-frame counter rides along on the dead-letter refresh frame, so a dashboard that is silently missing events says so rather than quietly showing an incomplete picture.

The relay must be unable to notice the dashboard is broken.

---

## Structure only — no payloads

The page is publicly reachable and unauthenticated. Observation payloads carry full GitHub webhook bodies, Discord user ids and names, and a **live `interaction_token`** — which is a capability, not merely data: anyone reading it within 15 minutes can post as the bot.

So the projection is a fixed allowlist, built in the tap **before** the queue, so a payload cannot reach the wire even if the ingest endpoint is later reused by something careless:

```python
{
  "log":         "obs" | "cmd",
  "id":          int,
  "name":        str,          # e.g. "github.switchboard.pr.merged"
  "emitted_by":  str | None,   # "sensor/github", "decider/github_notify"
  "observation_id": int | None,
  "command_id":     int | None,
  "seen_at":     float,        # dashboard clock, see "Latency" below
}
```

Nothing else. A test asserts the serialized frame contains no key outside this set, so adding a field is a deliberate act rather than an accident.

Payloads unlock when auth lands. The redaction work that would allow them is shared with the outstanding `LoggerTap` issue (it writes `interaction_token`s to persistent container logs) and belongs to that fix, not this one.

---

## Two things the prototype promises that the system cannot yet produce

Found while building the prototype. Both are real gaps, and the design resolves them by narrowing the claim rather than by adding fields to the log.

### Latency is *observed*, not measured

The prototype badges each event `+312ms`. There is no timestamp anywhere in a Switchboard message — `at` was deliberately removed in the four-roles restructure, and re-adding it to serve a visualisation would be exactly the contamination `emitted_by` was scrutinised for.

Instead the **dashboard timestamps frames on arrival** and derives deltas between causally linked frames: `cmd 88` links to `obs 412` via `observation_id`, so the gap between when the dashboard saw each is the decider's observed latency.

This is inflated by tap scheduling and the ingest hop, and it is a lower bound in the wrong direction under load. It is therefore labelled **observed**, and the page says so. Good enough to see a slow Discord call as visibly slow; not a measurement anyone should quote.

### Log depth is not shown at all

The prototype's log columns fill and drain with queue depth. True depth is "appended but not yet settled by every consumer group", which lives in mamamia's `message_state` table — not in the event stream, and not derivable from it.

**Depth is cut from this build.** The columns stay as visual structure — they are the substance between roles, which is the honest picture — but they carry no fill bar and no number. A better representation of what a log *is* comes after this ships, informed by looking at the real thing rather than at a mockup.

Nothing else depends on it: no depth query, no per-second stats timer, no `stats` frame.

### Dead letters still need one query

Failure is the only signal the event stream genuinely cannot carry. A dead-lettered command produces no result observation, so its absence is all the dashboard sees — and "no result yet" is indistinguishable from "still working" without a timeout guess. Guessing would paint traces red that are merely slow.

So `stats.py` polls `message_state` for dead message ids on a **5-second timer** via `bus.schedule_maintenance`, and the dashboard matches those ids against frames it has already seen. That turns a red trace into a fact instead of an inference, and it is one indexed query every five seconds on a box that handles a handful of events a day.

This is the only polling in the design.

---

## Components

| file | responsibility |
|---|---|
| `switchboard/dashboard/__init__.py` | `Dashboard` (routes, broadcaster, stats) and `DashboardTap` (projection, queue, sender) |
| `switchboard/dashboard/page.html` | the entire front end — inline CSS and JS, no build step, no dependencies |
| `switchboard/dashboard/stats.py` | read-only SQL for dead-letter ids and backfill |

`page.html` ships in the wheel, so `pyproject.toml` gains package data. It is read from disk per request rather than cached, which costs nothing at this traffic and means editing it on the Pi takes effect on reload.

### Routes

| method | path | owner | auth |
|---|---|---|---|
| GET | `/` | `dashboard` | none |
| GET | `/dashboard/stream` | `dashboard` | none |
| POST | `/dashboard/ingest` | `dashboard` | **bearer token** |

`/health` stays `HttpServer`'s. The page and stream are unauthenticated because they carry nothing secret — that is the whole reason for the structure-only rule.

### Token

`SB_DASHBOARD_TOKEN`, compared with `hmac.compare_digest`, the same discipline `verify_signature` already uses. **If it is unset the dashboard is not wired at all** — fail closed. There is never a default token, and no mode where ingest accepts unauthenticated writes.

`SB_DASHBOARD_INGEST_URL` defaults to `http://127.0.0.1:{SB_PORT}/dashboard/ingest`.

### Wiring

```python
if config.get("dashboard_token"):
    dash = Dashboard(topology=bus.topology(), token=config["dashboard_token"],
                     db_path=config["mamamia_db_path"])
    http.route("/", dash.page, owner="dashboard")
    http.route("/dashboard/stream", dash.stream, owner="dashboard")
    http.route("/dashboard/ingest", dash.ingest, methods=["POST"], owner="dashboard")
    bus.add_tap(DashboardTap(url=config["dashboard_ingest_url"],
                             token=config["dashboard_token"]))
    bus.schedule_maintenance("dashboard-dead", 5.0, dash.refresh_dead)
```

`Bus.topology()` is new — `{"sensors": [...], "deciders": [...], "actuators": [...]}` by name, so the spine draws what is actually wired rather than a hardcoded picture. It must be called after all roles are registered.

---

## The front end

One file, vanilla JS, canvas for the flow layer and DOM for labels. No framework, no bundler, nothing to install on a Pi.

**Layout** — spine full-bleed, a one-line status bar, and a feed that slides up over it on demand. The spine is the point; a permanently visible log tail would spend a third of the screen on something empty most of the day.

**The spine** is five bands: sensors → obs log → deciders → cmd log → actuators, with the result observation sweeping back to the obs log along the bottom. The two logs are drawn as columns — the substance between roles, not arrows between them, because in Switchboard that is what they are. The columns show no depth and no counts in this build; how a log should represent itself is a question worth answering against the running thing.

**Traces are the idle state.** Every completed journey leaves its lit path behind, fading over 60 seconds, newest brightest, failures burnt red. This replaces an earlier "ambient idle — nodes breathe" decision, which was a screensaver: it animated without saying anything. Persistent traces mean a quiet system shows *what it actually just did*. The idle state becomes the recent past — honest and interesting rather than honest and dull.

**Reconnection** uses `EventSource`'s native retry. On reconnect the page refetches backfill rather than assuming continuity, because frames dropped during the gap are gone for good.

**Backfill** on load: the last 50 messages across both logs, from the same read-only SQL, so the page opens with the recent past already drawn instead of an empty stage.

---

## Failure behaviour

| what breaks | what happens |
|---|---|
| No browsers connected | Sender still POSTs; broadcaster discards. Cheap and keeps one code path. |
| Dashboard route errors | Tap logs at debug, drops the batch, keeps consuming. Relay unaffected. |
| A browser stalls | Its own SSE queue fills and drops oldest. Other browsers unaffected. |
| Tap queue overflows | Oldest frames dropped, counter incremented, surfaced in stats. |
| `SB_DASHBOARD_TOKEN` unset | Dashboard not constructed, no routes registered, no tap added. |
| Token wrong | `401`, logged. Nothing enters the broadcaster. |
| mamamia DB locked | The dead-letter refresh logs and skips a tick; the stream is unaffected. |

---

## Testing

- **Projection** — the frame contains exactly the allowlisted keys and no others; a payload with an `interaction_token` produces a frame with no trace of it.
- **Non-blocking tap** — `observe()` returns promptly when the sender is hung; a full queue drops the oldest and increments the counter; `observe()` never raises.
- **Ingest auth** — no header `401`, wrong token `401`, correct token `202`. Comparison is constant-time.
- **Broadcast** — a frame POSTed to ingest reaches every connected stream client; a slow client does not stall the others.
- **Dead-letter SQL** — returns the ids of dead messages against a seeded database; a locked database degrades without raising.
- **Backfill** — returns the most recent N across both logs, newest first, structure-only.
- **Topology** — `Bus.topology()` lists every registered role by kind and name.
- **Wiring** — with no token, no dashboard routes are registered and no tap is added.

The front end is not unit tested. It is a single presentation file with no branching logic worth pinning, and the prototype is the design artifact it is checked against by eye.

---

## Deliberately out of scope

| left out | why |
|---|---|
| Auth on the page and stream | Deferred by decision. The structure-only rule is what makes that safe; revisit together, not separately. |
| Payload inspection | Needs the shared redactor, which belongs to the `LoggerTap` fix. |
| Any write action (retry, replay, mute) | Needs auth and a write path, and would stop the tap half being a tap. |
| Log depth, rates, any log-internal metric | Cut deliberately. The columns are structure only for now; a better representation of a log comes from watching the real thing. |
| Historical range beyond the log | `max_log_messages` bounds retention to a rolling window by design; a dashboard that implied more would be lying. |
| Per-event payload search | Same reason as payload inspection, plus it wants an index the log does not have. |
| Metrics export (Prometheus, etc.) | A different consumer with different needs. `stats.py` is where it would attach if it ever appears. |
