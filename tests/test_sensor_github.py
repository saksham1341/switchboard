import json
from pathlib import Path
from switchboard.sensors.github import map_event, verify_signature

FIX = Path(__file__).parent / "fixtures" / "github"
def _load(n): return json.loads((FIX / n).read_text())


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
