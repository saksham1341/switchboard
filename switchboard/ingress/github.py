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


import json as _json

from starlette.applications import Starlette
from starlette.responses import JSONResponse, PlainTextResponse
from starlette.routing import Route


class GitHubIngress:
    name = "github"

    def __init__(self, secret: str, *, host: str = "0.0.0.0", port: int = 8080):
        self._secret = secret
        self._host = host
        self._port = port
        self._publish = None
        self._server = None
        self.app = Starlette(routes=[
            Route("/webhook", self._webhook, methods=["POST"]),
            Route("/health", lambda request: PlainTextResponse("ok"), methods=["GET"]),
        ])

    def bind(self, publish) -> None:
        """Inject the broker's publish callable. Called by start(); exposed so
        tests can drive `app` without binding a port."""
        self._publish = publish

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
        ei = map_event(gh_event, payload)
        if ei is None:
            return JSONResponse({"status": "ignored"}, status_code=200)

        ei.dedupe_key = request.headers.get("X-GitHub-Delivery")
        ei.meta = {"delivery": ei.dedupe_key or "", "depth": "0"}
        result = await self._publish(ei)
        return JSONResponse({"status": result.status, "event_id": result.event_id},
                            status_code=200)

    async def start(self, publish) -> None:
        import uvicorn
        self.bind(publish)
        config = uvicorn.Config(self.app, host=self._host, port=self._port, log_level="info")
        self._server = uvicorn.Server(config)
        await self._server.serve()

    async def stop(self) -> None:
        if self._server is not None:
            self._server.should_exit = True
