import hashlib
import hmac

from switchboard.event import EventInput


def verify_signature(secret: str, body: bytes, header: str | None) -> bool:
    """Constant-time HMAC-SHA256 check against the X-Hub-Signature-256 header
    (format 'sha256=<hex>')."""
    if not header or not header.startswith("sha256="):
        return False
    expected = "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, header)


def map_event(gh_event: str, payload: dict) -> EventInput | None:
    """Translate a GitHub webhook (event type + payload) into an EventInput, or
    None for events we deliberately ignore. `dedupe_key` is filled by the caller
    from X-GitHub-Delivery."""
    repo = payload.get("repository", {}).get("name", "unknown")

    if gh_event == "pull_request":
        action = payload.get("action")
        if action == "closed" and payload.get("pull_request", {}).get("merged"):
            action = "merged"
        if action in {"opened", "closed", "merged"}:
            return _event(f"github.{repo}.pr.{action}", payload)
        if action == "review_requested":
            return _event(f"github.{repo}.review.requested", payload)
        return None

    if gh_event == "pull_request_review" and payload.get("action") == "submitted":
        state = payload.get("review", {}).get("state", "commented")
        return _event(f"github.{repo}.review.{state}", payload)

    if gh_event == "issues" and payload.get("action") in {"opened", "closed"}:
        return _event(f"github.{repo}.issue.{payload['action']}", payload)

    if gh_event == "check_run" and payload.get("action") == "completed":
        if payload.get("check_run", {}).get("conclusion") == "failure":
            return _event(f"github.{repo}.check_run.failed", payload)
        return None

    return None


def _event(kind: str, payload: dict) -> EventInput:
    return EventInput(kind=kind, source="github", payload=payload)
