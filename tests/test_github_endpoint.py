import hmac, hashlib, json
from pathlib import Path
import pytest
from starlette.testclient import TestClient
from switchboard.ingress.github import GitHubIngress
from switchboard.event import PublishResult

FIX = Path(__file__).parent / "fixtures" / "github"
SECRET = "s3cret"


def _sign(body: bytes) -> str:
    return "sha256=" + hmac.new(SECRET.encode(), body, hashlib.sha256).hexdigest()


class Spy:
    def __init__(self): self.calls = []
    async def publish(self, ei):
        self.calls.append(ei)
        return PublishResult(status="accepted", event_id="E1")


@pytest.fixture
def client_and_spy():
    spy = Spy()
    ingress = GitHubIngress(secret=SECRET)
    ingress.bind(spy.publish)          # inject publish without starting uvicorn
    return TestClient(ingress.app), spy


def test_health(client_and_spy):
    client, _ = client_and_spy
    assert client.get("/health").status_code == 200


def test_valid_pr_opened_publishes(client_and_spy):
    client, spy = client_and_spy
    body = (FIX / "pull_request.opened.json").read_bytes()
    r = client.post("/webhook", content=body, headers={
        "X-Hub-Signature-256": _sign(body),
        "X-GitHub-Event": "pull_request",
        "X-GitHub-Delivery": "d-123",
    })
    assert r.status_code == 200
    assert len(spy.calls) == 1
    assert spy.calls[0].kind == "github.home.pr.opened"
    assert spy.calls[0].dedupe_key == "d-123"


def test_bad_signature_401_no_publish(client_and_spy):
    client, spy = client_and_spy
    body = (FIX / "pull_request.opened.json").read_bytes()
    r = client.post("/webhook", content=body, headers={
        "X-Hub-Signature-256": "sha256=deadbeef",
        "X-GitHub-Event": "pull_request",
        "X-GitHub-Delivery": "d-1",
    })
    assert r.status_code == 401
    assert spy.calls == []


def test_ignored_event_200_no_publish(client_and_spy):
    client, spy = client_and_spy
    body = (FIX / "check_run.success.json").read_bytes()
    r = client.post("/webhook", content=body, headers={
        "X-Hub-Signature-256": _sign(body),
        "X-GitHub-Event": "check_run",
        "X-GitHub-Delivery": "d-2",
    })
    assert r.status_code == 200
    assert spy.calls == []


def test_malformed_json_400(client_and_spy):
    client, spy = client_and_spy
    body = b"{not json"
    r = client.post("/webhook", content=body, headers={
        "X-Hub-Signature-256": _sign(body),
        "X-GitHub-Event": "pull_request",
        "X-GitHub-Delivery": "d-3",
    })
    assert r.status_code == 400
    assert spy.calls == []
