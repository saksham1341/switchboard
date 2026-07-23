import json
import hmac as _hmac
import hashlib as _hashlib
from pathlib import Path

import pytest
from starlette.testclient import TestClient

from switchboard.sensors.github import map_event, verify_signature

FIX = Path(__file__).parent / "fixtures" / "github"
def _load(n): return json.loads((FIX / n).read_text())


def _signed_post(client, secret, gh_event, payload_dict, delivery, path="/webhook/github"):
    import json as _json
    body = _json.dumps(payload_dict).encode()
    sig = "sha256=" + _hmac.new(secret.encode(), body, _hashlib.sha256).hexdigest()
    return client.post(path, content=body, headers={
        "X-Hub-Signature-256": sig, "X-GitHub-Event": gh_event, "X-GitHub-Delivery": delivery})


def test_map_pr_opened_returns_name_and_payload():
    got = map_event("pull_request", _load("pull_request.opened.json"))
    assert got is not None
    name, payload = got
    assert name == "github.home.pr.opened"
    assert payload["number"] == 7


def test_map_check_run_success():
    name, _ = map_event("check_run", _load("check_run.success.json"))
    assert name == "github.home.check_run.succeeded"


def test_map_unknown_ignored():
    assert map_event("star", {"repository": {"name": "home"}}) is None


def test_verify_signature_roundtrip():
    import hmac, hashlib
    body = b'{"a":1}'
    sig = "sha256=" + hmac.new(b"s", body, hashlib.sha256).hexdigest()
    assert verify_signature("s", body, sig) is True
    assert verify_signature("s", body, None) is False


def _bound(secret="s3cret", store=None):
    """A GitHubSensor bound to a fake ctx, plus the pieces to assert against."""
    from switchboard.sensors.github import GitHubSensor
    from switchboard.http import HttpServer
    from switchboard.store import MemoryStore
    from switchboard.message import SensorCtx
    from switchboard.scheduler import Scheduler

    emitted = []

    async def emit(name, payload):
        emitted.append((name, payload))
        return 4242

    http = HttpServer(serve=False)
    s = GitHubSensor(secret)
    s.bind(SensorCtx(emit=emit, http=http, store=store or MemoryStore(),
                     schedule=Scheduler().for_owner("github")))
    return s, http, emitted


def test_webhook_emits_with_real_observation_id_and_dedups():
    s, http, emitted = _bound()
    client = TestClient(http.app)
    pr = _load("pull_request.opened.json")

    r1 = _signed_post(client, "s3cret", "pull_request", pr, "d-1")
    assert r1.status_code == 200 and r1.json()["event_id"] == 4242
    assert emitted == [("github.home.pr.opened", pr)]

    r2 = _signed_post(client, "s3cret", "pull_request", pr, "d-1")
    assert r2.status_code == 200 and r2.json()["status"] == "duplicate"
    assert len(emitted) == 1


def test_only_provider_scoped_path_is_served():
    s, http, emitted = _bound()
    client = TestClient(http.app)
    pr = _load("pull_request.opened.json")

    ok = _signed_post(client, "s3cret", "pull_request", pr, "d-new", path="/webhook/github")
    assert ok.status_code == 200
    assert emitted == [("github.home.pr.opened", pr)]

    for gone in ("/webhook", "/webhook/linear"):
        r = _signed_post(client, "s3cret", "pull_request", pr, "d-x", path=gone)
        assert r.status_code == 404, f"{gone} should not be served"
    assert len(emitted) == 1


def test_health_is_served_but_the_sensor_did_not_register_it():
    """/health answers because HttpServer owns it, not because GitHubSensor
    added it. The sensor's only route is its webhook."""
    s, http, _ = _bound()
    assert TestClient(http.app).get("/health").status_code == 200
    # HttpServer records an owner per (method, path); /health is its own.
    assert http._owners == {("GET", "/health"): "switchboard",
                            ("POST", "/webhook/github"): "github"}


def test_webhook_rejects_wrong_signature():
    s, http, emitted = _bound()
    client = TestClient(http.app)
    pr = _load("pull_request.opened.json")

    r = _signed_post(client, "wrong-secret", "pull_request", pr, "d-bad-sig")
    assert r.status_code == 401
    assert emitted == []


def test_webhook_rejects_malformed_json_with_valid_signature():
    s, http, emitted = _bound()
    client = TestClient(http.app)

    body = b"not-json{"
    sig = "sha256=" + _hmac.new(b"s3cret", body, _hashlib.sha256).hexdigest()
    r = client.post("/webhook/github", content=body, headers={
        "X-Hub-Signature-256": sig, "X-GitHub-Event": "pull_request",
        "X-GitHub-Delivery": "d-malformed"})
    assert r.status_code == 400
    assert emitted == []


def test_webhook_ignores_signed_event_it_deliberately_skips():
    s, http, emitted = _bound()
    client = TestClient(http.app)
    pr = _load("pull_request.opened.json")

    r = _signed_post(client, "s3cret", "star", pr, "d-star")
    assert r.status_code == 200
    assert r.json() == {"status": "ignored"}
    assert emitted == []


async def test_dedup_records_after_emit_not_before():
    """A failing emit must leave the delivery unrecorded, so a retry still lands."""
    from switchboard.sensors.github import GitHubSensor
    from switchboard.http import HttpServer
    from switchboard.store import MemoryStore
    from switchboard.message import SensorCtx
    from switchboard.scheduler import Scheduler

    store = MemoryStore()

    async def emit(name, payload):
        raise RuntimeError("log unavailable")

    http = HttpServer(serve=False)
    s = GitHubSensor("s3cret")
    s.bind(SensorCtx(emit=emit, http=http, store=store,
                     schedule=Scheduler().for_owner("github")))
    pr = _load("pull_request.opened.json")
    with pytest.raises(RuntimeError):
        _signed_post(TestClient(http.app), "s3cret", "pull_request", pr, "d-9")
    assert await store.get("github:delivery:d-9") is None
