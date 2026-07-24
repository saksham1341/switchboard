import json

import httpx

from switchboard.actuators.llm import LlmActuator, ANTHROPIC_URL
from switchboard.message import ActCtx, ActuatorCtx, Command
from switchboard.store import MemoryStore


def _cmd(args):
    class M:
        id = 3
        payload = args
        metadata = {"name": "llm", "observation_id": 9}
    return Command.from_message(M())


async def _run(act, args):
    results = []
    async def emit_result(name, payload, cmd_id):
        results.append((name, payload)); return 0
    cmd = _cmd(args)
    await act.act(cmd, ActCtx(cmd=cmd, _emit_result=emit_result))
    return results[0]


def _bound(handler):
    a = LlmActuator("sk-test", client=httpx.AsyncClient(
        transport=httpx.MockTransport(handler)))
    a.bind(ActuatorCtx(store=MemoryStore()))
    return a


async def test_completion_is_returned_as_a_result():
    seen = {}
    def h(req):
        seen["url"] = str(req.url)
        seen["body"] = json.loads(req.content)
        seen["key"] = req.headers.get("x-api-key")
        seen["version"] = req.headers.get("anthropic-version")
        return httpx.Response(200, json={
            "stop_reason": "tool_use",
            "content": [{"type": "tool_use", "id": "tu_1", "name": "web_search",
                         "input": {"query": "x"}}],
            "usage": {"input_tokens": 12, "output_tokens": 3},
        })
    a = _bound(h)
    name, payload = await _run(a, {
        "system": "be brief", "messages": [{"role": "user", "content": "hi"}],
        "tools": [{"name": "web_search"}], "max_tokens": 64,
    })
    assert seen["url"] == ANTHROPIC_URL
    assert seen["key"] == "sk-test"
    assert seen["version"]                        # version header is required
    assert seen["body"]["system"] == "be brief"
    assert seen["body"]["tools"] == [{"name": "web_search"}]
    assert name == "llm.ok"
    assert payload["stop_reason"] == "tool_use"
    assert payload["content"][0]["id"] == "tu_1"
    assert payload["usage"]["input_tokens"] == 12


async def test_the_decider_chooses_the_model():
    """Model, system and tools all travel in the command: the actuator is a dumb
    executor, and any decider could drive it."""
    seen = {}
    def h(req):
        seen["body"] = json.loads(req.content)
        return httpx.Response(200, json={"stop_reason": "end_turn", "content": [],
                                         "usage": {}})
    a = _bound(h)
    await _run(a, {"messages": [], "model": "claude-opus-4-8"})
    assert seen["body"]["model"] == "claude-opus-4-8"


async def test_default_model_is_used_when_the_command_omits_it():
    seen = {}
    def h(req):
        seen["body"] = json.loads(req.content)
        return httpx.Response(200, json={"stop_reason": "end_turn", "content": [],
                                         "usage": {}})
    a = _bound(h)
    await _run(a, {"messages": []})
    assert seen["body"]["model"] == "claude-sonnet-5"


async def test_api_error_is_an_error_result_not_a_raise():
    """A 4xx is a failure the actuator understands. Reporting it lets the caller
    react immediately; raising would burn the retry cycle first."""
    def h(req):
        return httpx.Response(400, json={"error": {"message": "bad request"}})
    a = _bound(h)
    name, payload = await _run(a, {"messages": []})
    assert name == "llm.error"
    assert payload["status"] == 400
    assert "bad request" in payload["message"]


async def test_server_error_is_also_reported():
    def h(req):
        return httpx.Response(529, json={"error": {"message": "overloaded"}})
    a = _bound(h)
    name, payload = await _run(a, {"messages": []})
    assert name == "llm.error"
    assert payload["status"] == 529


async def test_error_body_with_a_string_error_is_reported_not_raised():
    """A gateway may return {"error": "..."} rather than the API's nested shape.
    That must still be reported."""
    def h(req):
        return httpx.Response(502, json={"error": "bad gateway"})
    a = _bound(h)
    name, payload = await _run(a, {"messages": []})
    assert name == "llm.error"
    assert payload["message"] == "bad gateway"


async def test_error_body_that_is_not_json_is_reported_not_raised():
    def h(req):
        return httpx.Response(503, text="<html>upstream down</html>")
    a = _bound(h)
    name, payload = await _run(a, {"messages": []})
    assert name == "llm.error"
    assert "upstream down" in payload["message"]


async def test_error_body_with_an_unexpected_shape_is_reported_not_raised():
    """JSON, an object, but nothing we recognise — still a report, never a raise."""
    def h(req):
        return httpx.Response(500, json={"error": None, "detail": ["weird"]})
    a = _bound(h)
    name, payload = await _run(a, {"messages": []})
    assert name == "llm.error"
    assert payload["status"] == 500


async def test_error_body_that_is_a_json_array_is_reported_not_raised():
    def h(req):
        return httpx.Response(500, json=["nope"])
    a = _bound(h)
    name, payload = await _run(a, {"messages": []})
    assert name == "llm.error"


async def test_llm_declares_no_tool_spec():
    """The decider emits llm commands directly; the agent never calls the model
    as a tool."""
    assert getattr(LlmActuator, "tool_spec", None) is None
