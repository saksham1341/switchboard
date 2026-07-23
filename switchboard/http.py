import asyncio
import logging

from starlette.applications import Starlette
from starlette.responses import PlainTextResponse
from starlette.routing import Route

logger = logging.getLogger(__name__)


class HttpServer:
    """The one HTTP server. Owned by the app, shared by every role that needs a
    route, so one hostname serves every webhook sensor — each on its own path.

    /health belongs here rather than to any sensor: it is the deployment's
    liveness probe, and it must answer in a build with no webhook sensor at all.
    """

    def __init__(self, host: str = "0.0.0.0", port: int = 8080, *, serve: bool = True):
        self._host, self._port, self._serve = host, port, serve
        self._owners: dict[tuple[str, str], str] = {}
        self._server = None
        self._task = None
        self.app = Starlette(routes=[
            Route("/health", lambda request: PlainTextResponse("ok"), methods=["GET"]),
        ])
        self._owners[("GET", "/health")] = "switchboard"

    def route(self, path: str, handler, *, owner: str, methods=("GET",)) -> None:
        """Register a handler and record who owns it.

        `owner` is diagnostic, not functional — it never affects routing. It is
        the name of the component claiming this request, and it exists so a
        collision can say *who* it collided with:

            POST /webhook/github already registered by 'github'.

        Without it the error is "already registered" and nothing more, which on
        a server shared by several sensors is the difference between a
        five-second fix and a grep. It is required rather than defaulted for
        that reason: an unattributed claim produces exactly the useless message
        the parameter exists to prevent. Use the role's `name` ("github",
        "dashboard"); anything a reader can grep for is fine.

        Ownership is keyed on (method, path), not path alone: a request is
        identified by both, and that is the granularity at which exactly one
        response exists. GET /x and POST /x never contend, so they may have
        different owners — which is what lets a provider answer a verification
        challenge over GET at the same URL it POSTs events to.
        """
        claims = [m.upper() for m in methods]
        for m in claims:
            if (m, path) in self._owners:
                raise ValueError(
                    f"{m} {path} already registered by {self._owners[(m, path)]!r}. "
                    f"One request has one response, so it has one owner. To have "
                    f"several consumers react to it, add deciders that subscribe to "
                    f"the observation it emits; to separate tenants, scope the path "
                    f"(e.g. {path}/<tenant>).")
        for m in claims:
            self._owners[(m, path)] = owner
        self.app.router.routes.append(Route(path, handler, methods=claims))

    async def start(self) -> None:
        if not self._serve:
            return
        import uvicorn
        config = uvicorn.Config(self.app, host=self._host, port=self._port, log_level="info")
        self._server = uvicorn.Server(config)
        self._task = asyncio.create_task(self._server.serve())

        async def _wait_started():
            while not self._server.started:
                await asyncio.sleep(0.01)
        # Return only once the port is actually bound, so callers never race it.
        await asyncio.wait_for(_wait_started(), timeout=10.0)
        logger.info("http listening on %s:%s", self._host, self._port)

    async def stop(self) -> None:
        if self._server is not None:
            self._server.should_exit = True
        if self._task is not None:
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None
