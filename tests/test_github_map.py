import hmac
import hashlib
import json
from pathlib import Path
from switchboard.ingress.github import verify_signature, map_event

FIX = Path(__file__).parent / "fixtures" / "github"


def _load(name):
    return json.loads((FIX / name).read_text())


def test_verify_signature_ok():
    secret, body = "s3cret", b'{"a":1}'
    sig = "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    assert verify_signature(secret, body, sig) is True


def test_verify_signature_rejects_tampered():
    secret, body = "s3cret", b'{"a":1}'
    sig = "sha256=" + hmac.new(secret.encode(), b'{"a":2}', hashlib.sha256).hexdigest()
    assert verify_signature(secret, body, sig) is False


def test_verify_signature_rejects_missing_header():
    assert verify_signature("s", b"x", None) is False


def test_map_pr_opened():
    ei = map_event("pull_request", _load("pull_request.opened.json"))
    assert ei.kind == "github.home.pr.opened"
    assert ei.source == "github"
    assert ei.payload["number"] == 7


def test_map_pr_merged():
    ei = map_event("pull_request", _load("pull_request.closed_merged.json"))
    assert ei.kind == "github.home.pr.merged"


def test_map_check_run_failed():
    ei = map_event("check_run", _load("check_run.failed.json"))
    assert ei.kind == "github.home.check_run.failed"


def test_map_check_run_success_is_succeeded():
    ei = map_event("check_run", _load("check_run.success.json"))
    assert ei.kind == "github.home.check_run.succeeded"
    assert ei.source == "github"


def test_map_unknown_event_is_ignored():
    assert map_event("star", {"repository": {"name": "home"}}) is None


def test_map_pull_request_review_submitted():
    ei = map_event("pull_request_review", _load("review.submitted.json"))
    assert ei.kind == "github.home.review.approved"
    assert ei.source == "github"


def test_map_pull_request_review_non_submitted_ignored():
    assert map_event("pull_request_review",
                     {"action": "edited", "repository": {"name": "home"},
                      "review": {"state": "approved"}}) is None
