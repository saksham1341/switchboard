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

from starlette.applications import Starlette
from starlette.responses import JSONResponse, PlainTextResponse
from starlette.routing import Route

from switchboard.dedup import SeenStore


class GitHubSensor:
    """Sensor half of the GitHub connector: a webhook server that turns GitHub
    deliveries into observations. Delivery-id dedup (keyed by
    X-GitHub-Delivery) now lives here, in the sensor's own SeenStore, rather
    than in the core bus."""

    name = "github"

    def __init__(self, secret: str, *, host: str = "0.0.0.0", port: int = 8080,
                 seen_db: str = ":memory:"):
        self._secret = secret
        self._host = host
        self._port = port
        self._emit = None
        self._server = None
        self._seen = SeenStore(seen_db)
        self.app = Starlette(routes=[
            Route("/webhook", self._webhook, methods=["POST"]),
            Route("/health", lambda request: PlainTextResponse("ok"), methods=["GET"]),
        ])

    def bind(self, emit) -> None:
        """Inject the emit callable. Called by start(); exposed so tests can
        drive `app` without binding a port."""
        self._emit = emit

    async def _webhook(self, request):
        body = await request.body()
        sig = request.headers.get("X-Hub-Signature-256")
        if not verify_signature(self._secret, body, sig):
            return JSONResponse({"error": "invalid signature"}, status_code=401)
        try:
            payload = _json.loads(body)
        except ValueError:
            return JSONResponse({"error": "malformed json"}, status_code=400)

        gh_event = request.headers.get("X-GitHub-Event", "")
        mapped = map_event(gh_event, payload)
        if mapped is None:
            return JSONResponse({"status": "ignored"}, status_code=200)

        name, payload = mapped
        delivery_id = request.headers.get("X-GitHub-Delivery")
        if delivery_id and self._seen.get(delivery_id) is not None:
            return JSONResponse({"status": "duplicate"}, status_code=200)

        observation_id = await self._emit(name, payload)
        if delivery_id:
            self._seen.record(delivery_id, str(observation_id))
        return JSONResponse({"status": "ok", "event_id": observation_id}, status_code=200)

    async def start(self, emit) -> None:
        import uvicorn
        self.bind(emit)
        config = uvicorn.Config(self.app, host=self._host, port=self._port, log_level="info")
        self._server = uvicorn.Server(config)
        await self._server.serve()

    async def stop(self) -> None:
        if self._server is not None:
            self._server.should_exit = True
