import asyncio

import httpx
import pytest
from starlette.applications import Starlette
from starlette.routing import Route
from starlette.testclient import TestClient

from switchboard.dashboard import Dashboard, DashboardTap, project
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

    bus = Bus(str(tmp_path / "mm.db"), wait_ms=50, reaper_interval=3600.0)
    bus.add_sensor(_R("github")); bus.add_decider(_R("notify")); bus.add_actuator(_R("discord.post"))
    assert bus.topology() == {"sensors": ["github"], "deciders": ["notify"],
                              "actuators": ["discord.post"]}
