import httpx
from switchboard.errors import RetryableError
import pytest

from switchboard.actuators.llm import LlmActuator, LlmError
from switchboard.actuators.llm.backends.anthropic import AnthropicBackend
from switchboard.message import ActCtx, Command


def _cmd(args):
    class M:
        id = 1
        payload = args
        metadata = {"name": "llm", "observation_id": 7}
    return Command.from_message(M())


async def _run(act, args):
    results = []
    async def emit_result(name, payload, cmd_id):
        results.append((name, payload)); return 0
    cmd = _cmd(args)
    await act.act(cmd, ActCtx(cmd=cmd, _emit_result=emit_result))
    return results[0]


class _Backend:
    """A backend that records its request and returns whatever it was given."""
    name = "fake"
    def __init__(self, result=None, raises=None):
        self.seen = None
        self._result = result or {"stop_reason": "end_turn", "content": [], "usage": {}}
        self._raises = raises
    async def complete(self, req):
        self.seen = req
        if self._raises:
            raise self._raises
        return self._result
    async def close(self):
        self.closed = True


def _act(backend):
    a = LlmActuator(backend)
    a.bind(object())
    return a


async def test_the_backend_result_becomes_the_ok_payload():
    b = _Backend({"stop_reason": "tool_use", "content": [{"type": "text"}],
                  "usage": {"input_tokens": 1, "output_tokens": 2}})
    name, payload = await _run(_act(b), {"model": "m", "messages": []})
    assert name == "llm.ok"
    assert payload["stop_reason"] == "tool_use"
    assert payload["usage"]["input_tokens"] == 1


async def test_the_actuator_passes_the_args_through_untouched():
    b = _Backend()
    args = {"model": "m", "messages": [{"role": "user", "content": "hi"}],
            "tools": [{"name": "t"}], "system": "s", "max_tokens": 10}
    await _run(_act(b), args)
    assert b.seen == args


async def test_an_llm_error_is_reported_not_raised():
    b = _Backend(raises=LlmError("overloaded", status=529))
    name, payload = await _run(_act(b), {"model": "m", "messages": []})
    assert name == "llm.error"
    assert payload["message"] == "overloaded" and payload["status"] == 529


async def test_any_other_exception_propagates_so_the_bus_retries():
    b = _Backend(raises=httpx.ConnectTimeout("boom"))
    with pytest.raises(httpx.ConnectTimeout):
        await _run(_act(b), {"model": "m", "messages": []})


async def test_a_missing_model_is_a_reported_error_and_never_reaches_the_backend():
    # The command in the log must say which model ran. A backend that silently
    # defaults would make an old command unreplayable the moment the default
    # changed.
    b = _Backend()
    name, payload = await _run(_act(b), {"messages": []})
    assert name == "llm.error"
    assert "model" in payload["message"]
    assert b.seen is None


async def test_a_non_string_model_is_a_reported_error():
    b = _Backend()
    name, _ = await _run(_act(b), {"model": 7, "messages": []})
    assert name == "llm.error"
    assert b.seen is None


async def test_close_closes_the_backend():
    b = _Backend()
    a = _act(b)
    await a.close()
    assert b.closed is True


# --- the Anthropic backend: a passthrough ------------------------------------

def _anthropic(handler):
    return AnthropicBackend("key", client=httpx.AsyncClient(
        transport=httpx.MockTransport(handler)))


async def test_anthropic_sends_the_canonical_request_natively():
    seen = {}
    def handler(request):
        import json as _j
        seen.update(_j.loads(request.content))
        seen["_auth"] = request.headers.get("x-api-key")
        return httpx.Response(200, json={"stop_reason": "end_turn",
                                         "content": [], "usage": {}})
    b = _anthropic(handler)
    await b.complete({"model": "claude-sonnet-5", "system": "s",
                      "messages": [{"role": "user", "content": "hi"}],
                      "tools": [{"name": "t", "description": "d",
                                 "input_schema": {"type": "object"}}]})
    assert seen["model"] == "claude-sonnet-5"
    assert seen["system"] == "s"
    assert seen["tools"][0]["name"] == "t"          # no translation at all
    assert seen["_auth"] == "key"
    await b.close()


async def test_anthropic_maps_the_response_straight_through():
    def handler(request):
        return httpx.Response(200, json={
            "stop_reason": "tool_use",
            "content": [{"type": "tool_use", "id": "t1", "name": "x", "input": {}}],
            "usage": {"input_tokens": 3, "output_tokens": 4}})
    b = _anthropic(handler)
    out = await b.complete({"model": "m", "messages": []})
    assert out["content"][0]["id"] == "t1"
    assert out["usage"] == {"input_tokens": 3, "output_tokens": 4}
    await b.close()


async def test_anthropic_4xx_raises_llm_error_with_the_api_message():
    def handler(request):
        return httpx.Response(400, json={"error": {"message": "bad request"}})
    b = _anthropic(handler)
    with pytest.raises(LlmError) as e:
        await b.complete({"model": "m", "messages": []})
    assert "bad request" in str(e.value) and e.value.status == 400
    await b.close()


async def test_anthropic_survives_an_error_body_that_is_not_an_object():
    # The defect class that has landed four times: a non-object body must not
    # turn a reported failure into a raised AttributeError.
    def handler(request):
        return httpx.Response(400, json=["nope"])
    b = _anthropic(handler)
    with pytest.raises(LlmError):
        await b.complete({"model": "m", "messages": []})
    await b.close()


# --- the 5xx/4xx split, driven through the CONCRETE backend ------------------
# The fake-backend propagation test above never reaches these branches, so it
# would still pass with the branch order swapped. These would not.

async def test_anthropic_5xx_propagates_rather_than_becoming_an_llm_error():
    # A 5xx is transient. Turning it into a reported LlmError would tell the
    # agent a permanent failure happened and burn the turn, instead of letting
    # the Bus retry with backoff.
    def handler(request):
        return httpx.Response(503, text="overloaded")
    b = _anthropic(handler)
    with pytest.raises(RetryableError):
        await b.complete({"model": "m", "messages": []})
    await b.close()


async def test_anthropic_499_is_still_a_reported_error():
    # Pins the boundary from the other side: everything below 500 reports.
    def handler(request):
        return httpx.Response(499, json={"error": {"message": "client closed"}})
    b = _anthropic(handler)
    with pytest.raises(LlmError) as e:
        await b.complete({"model": "m", "messages": []})
    assert e.value.status == 499
    await b.close()


async def test_anthropic_unreadable_2xx_body_is_reported_not_crashed():
    # A 200 whose body is not an object used to reach .get() and raise an
    # uncaught AttributeError - the fifth instance of this project's most
    # repeated defect. It must be a reported failure instead.
    def handler(request):
        return httpx.Response(200, json=["not", "an", "object"])
    b = _anthropic(handler)
    with pytest.raises(LlmError):
        await b.complete({"model": "m", "messages": []})
    await b.close()


@pytest.mark.parametrize("status", [429, 408])
async def test_anthropic_transient_4xx_propagates_for_retry(status):
    """A 429 is a 4xx the provider expects you to retry — Groq's own message
    says "try again in 11.92s". Reporting it as permanent leaves the user's
    message unanswered when a retry seconds later would have worked. Learned
    from live traffic, not theory."""
    def handler(request):
        return httpx.Response(status, json={"error": {"message": "rate limited"}})
    b = _anthropic(handler)
    with pytest.raises(RetryableError):
        await b.complete({"model": "m", "messages": []})
    await b.close()


# --- retry-after parsing ------------------------------------------------------

def test_parse_retry_after_reads_bare_seconds():
    from switchboard.actuators.llm.actuator import parse_retry_after
    class R: headers = {"retry-after": "3"}
    assert parse_retry_after(R()) == 3.0


def test_parse_retry_after_reads_a_compound_duration():
    from switchboard.actuators.llm.actuator import parse_retry_after
    class R: headers = {"x-ratelimit-reset-tokens": "1m26.4s"}
    assert abs(parse_retry_after(R()) - 86.4) < 1e-6


def test_parse_retry_after_is_not_capped():
    # Clamping is the Bus's policy now. The actuator reports what the provider
    # said, uncapped; a daily-quota answer travels intact.
    from switchboard.actuators.llm.actuator import parse_retry_after
    class R: headers = {"retry-after": "99999"}
    assert parse_retry_after(R()) == 99999.0


def test_parse_retry_after_none_when_no_header():
    from switchboard.actuators.llm.actuator import parse_retry_after
    class R: headers = {}
    assert parse_retry_after(R()) is None


async def test_a_429_reports_the_providers_delay_unclamped():
    """Clamping is the Bus's policy now. The backend reports what the provider
    said; a 3593s daily-quota answer travels intact and the Bus decides."""
    def handler(request):
        return httpx.Response(429, json={"error": {"message": "quota"}},
                              headers={"retry-after": "3593"})
    b = _anthropic(handler)
    with pytest.raises(RetryableError) as e:
        await b.complete({"model": "m", "messages": []})
    assert e.value.retry_after == 3593.0
    await b.close()
