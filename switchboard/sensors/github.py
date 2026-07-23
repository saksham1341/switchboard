import hashlib
import hmac


def verify_signature(secret: str, body: bytes, header: str | None) -> bool:
    """Constant-time HMAC-SHA256 check against the X-Hub-Signature-256 header
    (format 'sha256=<hex>')."""
    if not header or not header.startswith("sha256="):
        return False
    expected = "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, header)


def map_event(gh_event: str, payload: dict) -> tuple[str, dict] | None:
    """Translate a GitHub webhook (event type + payload) into a (kind, payload)
    observation tuple, or None for events we deliberately ignore."""
    repo = payload.get("repository", {}).get("name", "unknown")

    if gh_event == "pull_request":
        action = payload.get("action")
        if action == "closed" and payload.get("pull_request", {}).get("merged"):
            action = "merged"
        if action in {"opened", "closed", "merged"}:
            return (f"github.{repo}.pr.{action}", payload)
        if action == "review_requested":
            return (f"github.{repo}.review.requested", payload)
        return None

    if gh_event == "pull_request_review" and payload.get("action") == "submitted":
        state = payload.get("review", {}).get("state", "commented")
        return (f"github.{repo}.review.{state}", payload)

    if gh_event == "issues" and payload.get("action") in {"opened", "closed"}:
        return (f"github.{repo}.issue.{payload['action']}", payload)

    if gh_event == "check_run" and payload.get("action") == "completed":
        conclusion = payload.get("check_run", {}).get("conclusion")
        if conclusion == "failure":
            return (f"github.{repo}.check_run.failed", payload)
        if conclusion == "success":
            return (f"github.{repo}.check_run.succeeded", payload)
        return None

    return None


import json as _json

from starlette.responses import JSONResponse


class GitHubSensor:
    """Webhook -> observation. Owns the /webhook/github route and nothing else:
    the port, the server, and /health belong to the app; dedup state to
    ctx.store."""

    name = "github"

    def __init__(self, secret: str, *, dedup_ttl: float = 7 * 86_400.0):
        self._secret = secret
        self._dedup_ttl = dedup_ttl
        self.ctx = None

    def bind(self, ctx) -> None:
        self.ctx = ctx
        # One hostname serves every webhook sensor, each on its own path
        # (/webhook/linear, /webhook/stripe, ...).
        ctx.http.route("/webhook/github", self._webhook,
                       methods=["POST"], owner=self.name)

    async def start(self) -> None:
        return          # route-driven: no loop of its own to supervise

    async def stop(self) -> None:
        return

    async def _webhook(self, request):
        body = await request.body()
        sig = request.headers.get("X-Hub-Signature-256")
        if not verify_signature(self._secret, body, sig):
            return JSONResponse({"error": "invalid signature"}, status_code=401)
        try:
            payload = _json.loads(body)
        except ValueError:
            return JSONResponse({"error": "malformed json"}, status_code=400)

        mapped = map_event(request.headers.get("X-GitHub-Event", ""), payload)
        if mapped is None:
            return JSONResponse({"status": "ignored"}, status_code=200)
        name, payload = mapped

        delivery_id = request.headers.get("X-GitHub-Delivery")
        key = f"github:delivery:{delivery_id}" if delivery_id else None
        if key and await self.ctx.store.get(key) is not None:
            return JSONResponse({"status": "duplicate"}, status_code=200)

        # Emit first, record second: a crash in between costs a duplicate,
        # the reverse order costs the event.
        observation_id = await self.ctx.emit(name, payload)
        if key:
            await self.ctx.store.set(key, str(observation_id), ttl=self._dedup_ttl)
        return JSONResponse({"status": "ok", "event_id": observation_id}, status_code=200)
