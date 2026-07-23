import pytest
from starlette.responses import PlainTextResponse
from starlette.testclient import TestClient

from switchboard.http import HttpServer


def test_health_is_served_with_no_routes_registered():
    s = HttpServer(serve=False)
    r = TestClient(s.app).get("/health")
    assert r.status_code == 200
    assert r.text == "ok"


def test_registered_routes_are_served():
    s = HttpServer(serve=False)

    async def a(request): return PlainTextResponse("A")
    async def b(request): return PlainTextResponse("B")

    s.route("/one", a, methods=["POST"], owner="one")
    s.route("/two", b, methods=["POST"], owner="two")
    client = TestClient(s.app)
    assert client.post("/one").text == "A"
    assert client.post("/two").text == "B"


def test_unregistered_path_is_404():
    s = HttpServer(serve=False)
    assert TestClient(s.app).post("/nope").status_code == 404


def test_duplicate_method_and_path_raises_naming_the_first_owner():
    s = HttpServer(serve=False)

    async def h(request): return PlainTextResponse("x")

    s.route("/dup", h, methods=["POST"], owner="github")
    with pytest.raises(ValueError, match="github"):
        s.route("/dup", h, methods=["POST"], owner="linear")


def test_same_path_different_methods_is_allowed():
    """Meta-style webhooks answer a GET verification challenge at the same URL
    that receives event POSTs. Ownership is per request, not per path."""
    s = HttpServer(serve=False)

    async def verify(request): return PlainTextResponse("challenge")
    async def event(request): return PlainTextResponse("received")

    s.route("/webhook/meta", verify, methods=["GET"], owner="meta")
    s.route("/webhook/meta", event, methods=["POST"], owner="meta")
    client = TestClient(s.app)
    assert client.get("/webhook/meta").text == "challenge"
    assert client.post("/webhook/meta").text == "received"


def test_overlapping_method_set_raises_on_the_shared_verb():
    s = HttpServer(serve=False)

    async def h(request): return PlainTextResponse("x")

    s.route("/multi", h, methods=["GET", "POST"], owner="first")
    with pytest.raises(ValueError, match="first"):
        s.route("/multi", h, methods=["POST"], owner="second")


async def test_start_is_a_noop_when_serve_is_false():
    s = HttpServer(serve=False)
    await s.start()          # binds nothing
    await s.stop()


def test_owner_is_required():
    """Unattributed claims produce exactly the useless collision message the
    parameter exists to prevent, so it has no default."""
    s = HttpServer(serve=False)

    async def h(request): return PlainTextResponse("x")

    with pytest.raises(TypeError):
        s.route("/x", h, methods=["POST"])
