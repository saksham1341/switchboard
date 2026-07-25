import asyncio

import httpx
import pytest
from starlette.applications import Starlette
from starlette.routing import Route
from starlette.testclient import TestClient

from switchboard.dashboard import DEAD_MAX, Dashboard, DashboardTap, project
from switchboard.dashboard.stats import FRAME_KEYS
from switchboard.message import Observation, Command


def _obs(**md):
    class M:
        id = 412
        payload = {"interaction_token": "SECRET", "user": "someone", "big": "x" * 500}
        metadata = {"name": "github.switchboard.pr.merged",
                    "emitted_by": "sensor/github", **md}
    return Observation.from_message(M())


def _dash(tmp_path, token="t0ken"):
    return Dashboard(topology={"sensors": ["github"], "deciders": [], "actuators": []},
                     token=token, db_path=str(tmp_path / "nope.db"))


# --- projection: the security-critical part ---------------------------------

def test_projection_has_exactly_the_allowed_keys():
    frame = project("obs", _obs())
    assert set(frame) == set(FRAME_KEYS)


def test_projection_carries_no_payload():
    frame = project("obs", _obs())
    blob = repr(frame)
    assert "SECRET" not in blob and "someone" not in blob and "xxxx" not in blob


def test_projection_keeps_attribution_and_links():
    frame = project("cmd", Command(id=88, name="discord.post", args={},
                                   observation_id=412, emitted_by="decider/github_notify"))
    assert frame["emitted_by"] == "decider/github_notify"
    assert frame["observation_id"] == 412
    assert frame["log"] == "cmd"


# --- the tap must never be able to stall the relay ---------------------------

class _HangingClient:
    def __init__(self): self.posts = 0
    async def post(self, *a, **k):
        self.posts += 1
        await asyncio.sleep(3600)
    async def aclose(self): pass


async def test_observe_returns_while_the_sender_is_hung():
    tap = DashboardTap("http://x/ingest", "t", client=_HangingClient())
    tap.bind(object())
    await asyncio.wait_for(tap.observe("obs", _obs()), timeout=0.5)
    await tap.close()


async def test_full_queue_drops_oldest_and_counts_it():
    tap = DashboardTap("http://x/ingest", "t", client=_HangingClient())
    tap.bind(object())
    # First observe starts the sender, which immediately takes one frame.
    for _ in range(400):
        await asyncio.wait_for(tap.observe("obs", _obs()), timeout=0.5)
    assert tap.dropped > 0
    assert tap._q.qsize() <= 256
    await tap.close()


# --- ingest auth -------------------------------------------------------------

def _client(dash):
    app = Starlette(routes=[
        Route("/dashboard/ingest", dash.ingest, methods=["POST"]),
        Route("/dashboard/stream", dash.stream),
    ])
    return TestClient(app)


def test_ingest_rejects_missing_token(tmp_path):
    r = _client(_dash(tmp_path)).post("/dashboard/ingest", json={"frames": []})
    assert r.status_code == 401


def test_ingest_rejects_wrong_token(tmp_path):
    r = _client(_dash(tmp_path)).post(
        "/dashboard/ingest", json={"frames": []},
        headers={"Authorization": "Bearer wrong"})
    assert r.status_code == 401


def test_ingest_accepts_correct_token(tmp_path):
    r = _client(_dash(tmp_path)).post(
        "/dashboard/ingest", json={"frames": []},
        headers={"Authorization": "Bearer t0ken"})
    assert r.status_code == 202


def test_ingest_rejects_malformed_json(tmp_path):
    r = _client(_dash(tmp_path)).post(
        "/dashboard/ingest", content=b"not json",
        headers={"Authorization": "Bearer t0ken", "Content-Type": "application/json"})
    assert r.status_code == 400


# --- broadcast ---------------------------------------------------------------

async def test_frame_reaches_every_connected_client(tmp_path):
    dash = _dash(tmp_path)
    a, b = asyncio.Queue(), asyncio.Queue()
    dash._clients.update({a, b})
    dash._broadcast({"type": "event", "frame": {"id": 1}})
    assert (await a.get())["frame"]["id"] == 1
    assert (await b.get())["frame"]["id"] == 1


async def test_a_stalled_client_is_dropped_not_allowed_to_block(tmp_path):
    dash = _dash(tmp_path)
    slow, ok = asyncio.Queue(maxsize=1), asyncio.Queue()
    slow.put_nowait("filler")
    dash._clients.update({slow, ok})
    dash._broadcast({"type": "event", "frame": {"id": 2}})
    assert slow not in dash._clients          # evicted
    assert (await ok.get())["frame"]["id"] == 2


# --- wiring ------------------------------------------------------------------

def test_no_token_means_no_dashboard_at_all(tmp_path):
    from switchboard.app import build
    bus, _ = build({
        "mamamia_db_path": str(tmp_path / "mm.db"),
        "switchboard_db_path": str(tmp_path / "sb.db"),
        "github_secret": "s", "port": 8123,
    })
    owners = set(bus._http._owners.values())
    assert "dashboard" not in owners
    assert not any(t.name == "dashboard" for t in bus._taps)


def test_token_wires_page_stream_and_ingest(tmp_path):
    from switchboard.app import build
    bus, _ = build({
        "mamamia_db_path": str(tmp_path / "mm.db"),
        "switchboard_db_path": str(tmp_path / "sb.db"),
        "github_secret": "s", "port": 8124,
        "dashboard_token": "t0ken",
        "dashboard_ingest_url": "http://127.0.0.1:8124/dashboard/ingest",
    })
    o = bus._http._owners
    assert o[("GET", "/")] == "dashboard"
    assert o[("GET", "/dashboard/stream")] == "dashboard"
    assert o[("POST", "/dashboard/ingest")] == "dashboard"
    assert any(t.name == "dashboard" for t in bus._taps)


async def test_topology_lists_registered_roles(tmp_path):
    from switchboard.bus import Bus

    class _R:
        def __init__(self, n): self.name = n
        def bind(self, ctx): pass
        def subscribes(self, o): return False
        async def decide(self, o, c): pass
        async def act(self, c, x): pass
        async def start(self): pass
        async def stop(self): pass

    bus = Bus(str(tmp_path / "mm.db"), consumer_wait_ms=50, lease_reaper_interval_s=3600.0)
    bus.add_sensor(_R("github")); bus.add_decider(_R("notify")); bus.add_actuator(_R("discord.post"))
    assert bus.topology() == {"sensors": ["github"], "deciders": ["notify"],
                              "actuators": ["discord.post"]}


# --- ingest allowlist (defence in depth for the render seam) -----------------

async def test_ingest_strips_unexpected_keys_before_broadcast(tmp_path):
    dash = _dash(tmp_path)
    seen = asyncio.Queue()
    dash._clients.add(seen)
    app = Starlette(routes=[Route("/dashboard/ingest", dash.ingest, methods=["POST"])])
    TestClient(app).post("/dashboard/ingest",
        json={"frames": [{"log": "obs", "id": 1, "name": "<script>x</script>",
                          "evil": "<img onerror=1>", "seen_at": 0}]},
        headers={"Authorization": "Bearer t0ken"})
    frame = (await asyncio.wait_for(seen.get(), 1))["frame"]
    assert set(frame) == set(FRAME_KEYS)          # no "evil" key survives
    assert "evil" not in frame


def test_the_projection_never_carries_the_rendered_text():
    """SECURITY. The dashboard page is public and unauthenticated, which is
    only acceptable because the projection is structure-only. `text` is message
    content — names, ids and causal links may cross; content may not."""
    from switchboard.dashboard.stats import FRAME_KEYS
    assert "text" not in FRAME_KEYS

    class M:
        id = 7
    m = M()
    m.payload = {"secret": "SHOULD NOT APPEAR"}
    m.metadata = {"name": "discord.message", "emitted_by": "sensor/discord",
                  "text": "[discord.message] SHOULD NOT APPEAR EITHER"}
    frame = project("obs", Observation.from_message(m))
    assert "SHOULD NOT APPEAR" not in repr(frame)


# --- dead-letter subscription (replaces the poll) ----------------------------

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


def _dead_frames(ids):
    return [{"log": "obs", "id": 9, "name": "switchboard.deadletter",
             "dead_log": "cmd", "dead_id": i, "seen_at": 0} for i in ids]


def _post_dead(dash, ids):
    app = Starlette(routes=[Route("/dashboard/ingest", dash.ingest, methods=["POST"])])
    return TestClient(app).post("/dashboard/ingest", json={"frames": _dead_frames(ids)},
                                headers={"Authorization": "Bearer t0ken"})


async def test_the_dead_list_is_capped_and_drops_the_oldest(tmp_path):
    """The poll this replaced re-derived the list from message_state every
    time, so it was implicitly bounded by log_max_dead and self-healing. An
    in-process list that is only ever appended to grows without limit and is
    re-broadcast in full on every new dead letter."""
    dash = _dash(tmp_path)
    _post_dead(dash, range(DEAD_MAX + 5))
    assert len(dash._dead) == DEAD_MAX
    assert {"log": "cmd", "id": 0} not in dash._dead          # oldest dropped
    assert {"log": "cmd", "id": DEAD_MAX + 4} in dash._dead   # newest kept


async def test_a_repeated_dead_letter_is_not_listed_twice(tmp_path):
    dash = _dash(tmp_path)
    _post_dead(dash, [44, 44, 44])
    assert dash._dead == [{"log": "cmd", "id": 44}]


async def test_a_dead_letter_evicted_by_the_cap_can_be_listed_again(tmp_path):
    """The dedupe index has to be trimmed with the list. If it is not, an
    evicted entry is remembered forever as "already seen" and can never
    reappear -- a leak of exactly the kind the cap exists to stop."""
    dash = _dash(tmp_path)
    _post_dead(dash, range(DEAD_MAX + 1))          # id 0 is evicted
    _post_dead(dash, [0])
    assert {"log": "cmd", "id": 0} in dash._dead
    assert len(dash._dead) == DEAD_MAX


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
