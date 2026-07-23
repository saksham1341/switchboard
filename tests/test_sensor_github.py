import json
import hmac as _hmac
import hashlib as _hashlib
from pathlib import Path

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


def test_webhook_emits_with_real_observation_id_and_dedups():
    from switchboard.sensors.github import GitHubSensor
    emitted = []

    async def fake_emit(name, payload):
        emitted.append((name, payload))
        return 4242

    s = GitHubSensor("s3cret", seen_db=":memory:")
    s.bind(fake_emit)
    client = TestClient(s.app)
    pr = _load("pull_request.opened.json")

    r1 = _signed_post(client, "s3cret", "pull_request", pr, "d-1")
    assert r1.status_code == 200 and r1.json()["event_id"] == 4242
    assert emitted == [("github.home.pr.opened", pr)]

    r2 = _signed_post(client, "s3cret", "pull_request", pr, "d-1")
    assert r2.status_code == 200 and len(emitted) == 1


def test_only_provider_scoped_path_is_served():
    # /webhook/github is the only GitHub route. The old unscoped /webhook is
    # gone — one hostname serves every webhook sensor, each on its own path.
    from switchboard.sensors.github import GitHubSensor
    emitted = []

    async def fake_emit(name, payload):
        emitted.append(name)
        return len(emitted)

    s = GitHubSensor("s3cret", seen_db=":memory:")
    s.bind(fake_emit)
    client = TestClient(s.app)
    pr = _load("pull_request.opened.json")

    ok = _signed_post(client, "s3cret", "pull_request", pr, "d-new", path="/webhook/github")
    assert ok.status_code == 200
    assert emitted == ["github.home.pr.opened"]

    for gone in ("/webhook", "/webhook/linear"):
        r = _signed_post(client, "s3cret", "pull_request", pr, "d-x", path=gone)
        assert r.status_code == 404, f"{gone} should not be served"
    assert emitted == ["github.home.pr.opened"]        # no extra emits
