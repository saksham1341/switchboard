import json

import httpx
import pytest

from switchboard.actuators.llm import LlmError
from switchboard.actuators.llm.backends.openai import OpenAiBackend

BASE = "https://api.groq.com/openai/v1"


def _backend(handler):
    return OpenAiBackend("key", base_url=BASE,
                         client=httpx.AsyncClient(transport=httpx.MockTransport(handler)))


def _capture(response=None):
    """Returns (backend, seen) where seen is filled with the request body."""
    seen = {}
    def handler(request):
        seen.update(json.loads(request.content))
        seen["_auth"] = request.headers.get("authorization")
        seen["_url"] = str(request.url)
        return response or httpx.Response(200, json={
            "choices": [{"finish_reason": "stop",
                         "message": {"role": "assistant", "content": "hi"}}],
            "usage": {"prompt_tokens": 1, "completion_tokens": 2}})
    return _backend(handler), seen


async def test_system_becomes_a_leading_system_message():
    b, seen = _capture()
    await b.complete({"model": "m", "system": "you are x",
                      "messages": [{"role": "user", "content": "hi"}]})
    assert seen["messages"][0] == {"role": "system", "content": "you are x"}
    assert seen["messages"][1]["role"] == "user"
    await b.close()


async def test_no_system_means_no_system_message():
    b, seen = _capture()
    await b.complete({"model": "m", "messages": [{"role": "user", "content": "hi"}]})
    assert seen["messages"][0]["role"] == "user"
    await b.close()


async def test_tools_are_wrapped_in_the_function_envelope():
    b, seen = _capture()
    await b.complete({"model": "m", "messages": [],
                      "tools": [{"name": "discord.post", "description": "d",
                                 "input_schema": {"type": "object",
                                                  "properties": {"a": {"type": "string"}}}}]})
    fn = seen["tools"][0]
    assert fn["type"] == "function"
    assert fn["function"]["name"] == "discord.post"
    assert fn["function"]["parameters"]["properties"]["a"]["type"] == "string"
    await b.close()


async def test_an_assistant_tool_use_becomes_a_tool_call_with_json_string_args():
    b, seen = _capture()
    await b.complete({"model": "m", "messages": [
        {"role": "user", "content": "hi"},
        {"role": "assistant", "content": [
            {"type": "text", "text": "sure"},
            {"type": "tool_use", "id": "t1", "name": "post",
             "input": {"content": "yo"}}]}]})
    a = seen["messages"][-1]
    assert a["role"] == "assistant" and a["content"] == "sure"
    call = a["tool_calls"][0]
    assert call["id"] == "t1" and call["function"]["name"] == "post"
    assert json.loads(call["function"]["arguments"]) == {"content": "yo"}
    await b.close()


async def test_tool_results_fan_out_to_separate_tool_messages_in_order():
    # ids are deliberately *not* already sorted (t9 before t2) so that a
    # fan-out which secretly sorted by tool_use_id instead of preserving
    # block order would fail this test.
    b, seen = _capture()
    await b.complete({"model": "m", "messages": [
        {"role": "user", "content": [
            {"type": "tool_result", "tool_use_id": "t9", "content": "one"},
            {"type": "tool_result", "tool_use_id": "t2", "content": "two"}]}]})
    tools = [m for m in seen["messages"] if m["role"] == "tool"]
    assert [t["tool_call_id"] for t in tools] == ["t9", "t2"]
    assert [t["content"] for t in tools] == ["one", "two"]
    await b.close()


async def test_a_mention_merged_into_a_tool_result_turn_is_not_dropped():
    # agent/decider.py's _advance merges a mention arriving mid-gather into
    # the trailing user turn as a text block, producing
    # [tool_result, tool_result, {"type": "text", ...}]. That text must
    # survive translation as a trailing user message, in addition to the
    # tool messages (which must come first, in block order).
    b, seen = _capture()
    await b.complete({"model": "m", "messages": [
        {"role": "user", "content": [
            {"type": "tool_result", "tool_use_id": "t9", "content": "one"},
            {"type": "tool_result", "tool_use_id": "t2", "content": "two"},
            {"type": "text", "text": "hey are you there"}]}]})
    tail = seen["messages"]
    tool_msgs = [m for m in tail if m["role"] == "tool"]
    assert [t["tool_call_id"] for t in tool_msgs] == ["t9", "t2"]
    # the tool messages come first, then exactly one trailing user message
    assert [m["role"] for m in tail] == ["tool", "tool", "user"]
    assert tail[-1]["content"] == "hey are you there"
    await b.close()


async def test_a_text_only_user_turn_with_list_content_is_not_dropped():
    b, seen = _capture()
    await b.complete({"model": "m", "messages": [
        {"role": "user", "content": [{"type": "text", "text": "hello"}]}]})
    users = [m for m in seen["messages"] if m["role"] == "user"]
    assert len(users) == 1
    assert users[0]["content"] == "hello"
    await b.close()


async def test_is_error_prefixes_the_tool_message_content():
    b, seen = _capture()
    await b.complete({"model": "m", "messages": [
        {"role": "user", "content": [
            {"type": "tool_result", "tool_use_id": "t1", "content": "boom",
             "is_error": True}]}]})
    tool_msg = [m for m in seen["messages"] if m["role"] == "tool"][0]
    assert tool_msg["content"] == "ERROR: boom"
    await b.close()


async def test_is_error_false_does_not_prefix():
    b, seen = _capture()
    await b.complete({"model": "m", "messages": [
        {"role": "user", "content": [
            {"type": "tool_result", "tool_use_id": "t1", "content": "ok",
             "is_error": False}]}]})
    tool_msg = [m for m in seen["messages"] if m["role"] == "tool"][0]
    assert tool_msg["content"] == "ok"
    await b.close()


async def test_a_non_str_tool_result_content_is_json_dumped():
    b, seen = _capture()
    await b.complete({"model": "m", "messages": [
        {"role": "user", "content": [
            {"type": "tool_result", "tool_use_id": "t1",
             "content": {"a": 1}}]}]})
    tool_msg = [m for m in seen["messages"] if m["role"] == "tool"][0]
    assert json.loads(tool_msg["content"]) == {"a": 1}
    await b.close()


async def test_max_tokens_is_forwarded():
    b, seen = _capture()
    await b.complete({"model": "m", "messages": [], "max_tokens": 777})
    assert seen["max_tokens"] == 777
    await b.close()


async def test_max_tokens_defaults_when_absent():
    b, seen = _capture()
    await b.complete({"model": "m", "messages": []})
    assert seen["max_tokens"] == 4096
    await b.close()


async def test_tool_calls_ordering_matches_block_order_not_name_sort():
    # names chosen so a descending name-sort would swap the order.
    b, seen = _capture()
    await b.complete({"model": "m", "messages": [
        {"role": "assistant", "content": [
            {"type": "tool_use", "id": "a1", "name": "alpha", "input": {}},
            {"type": "tool_use", "id": "b1", "name": "beta", "input": {}}]}]})
    a = seen["messages"][-1]
    assert [c["function"]["name"] for c in a["tool_calls"]] == ["alpha", "beta"]
    await b.close()


async def test_tool_call_missing_id_or_name_is_skipped_but_valid_ones_kept():
    def handler(request):
        return httpx.Response(200, json={"choices": [{
            "finish_reason": "tool_calls",
            "message": {"tool_calls": [
                {"id": None, "function": {"name": "p", "arguments": "{}"}},
                {"id": "t1", "function": {"name": None, "arguments": "{}"}},
                {"id": "t2", "function": {"name": "q", "arguments": "{}"}}]}}],
            "usage": {}})
    b = _backend(handler)
    out = await b.complete({"model": "m", "messages": []})
    assert [c["id"] for c in out["content"] if c["type"] == "tool_use"] == ["t2"]
    await b.close()


async def test_a_non_dict_message_does_not_crash():
    def handler(request):
        return httpx.Response(200, json={"choices": [
            {"finish_reason": "stop", "message": "not-a-dict"}],
            "usage": {}})
    b = _backend(handler)
    out = await b.complete({"model": "m", "messages": []})
    assert out["content"] == []
    await b.close()


async def test_a_non_list_tool_calls_does_not_crash():
    # tool_calls is a non-iterable (an int): without the isinstance guard,
    # `for call in tool_calls` would raise TypeError instead of degrading.
    def handler(request):
        return httpx.Response(200, json={"choices": [{
            "finish_reason": "stop",
            "message": {"content": "hi", "tool_calls": 5}}],
            "usage": {}})
    b = _backend(handler)
    out = await b.complete({"model": "m", "messages": []})
    assert [c["type"] for c in out["content"]] == ["text"]
    await b.close()


async def test_a_non_dict_usage_does_not_crash():
    def handler(request):
        return httpx.Response(200, json={
            "choices": [{"finish_reason": "stop", "message": {"content": "hi"}}],
            "usage": "not-a-dict"})
    b = _backend(handler)
    out = await b.complete({"model": "m", "messages": []})
    assert out["usage"] == {"input_tokens": None, "output_tokens": None}
    await b.close()


async def test_text_only_assistant_turn_has_no_tool_calls_key():
    b, seen = _capture()
    await b.complete({"model": "m", "messages": [
        {"role": "assistant", "content": [{"type": "text", "text": "hi"}]}]})
    a = seen["messages"][-1]
    assert "tool_calls" not in a
    await b.close()


async def test_tool_only_assistant_turn_has_none_content_not_empty_string():
    b, seen = _capture()
    await b.complete({"model": "m", "messages": [
        {"role": "assistant", "content": [
            {"type": "tool_use", "id": "t1", "name": "p", "input": {}}]}]})
    a = seen["messages"][-1]
    assert a["content"] is None
    await b.close()


async def test_a_tool_call_in_the_response_becomes_a_tool_use_block():
    def handler(request):
        return httpx.Response(200, json={"choices": [{
            "finish_reason": "tool_calls",
            "message": {"role": "assistant", "content": None,
                        "tool_calls": [{"id": "t9", "type": "function",
                                        "function": {"name": "post",
                                                     "arguments": '{"content":"hey"}'}}]}}],
            "usage": {"prompt_tokens": 5, "completion_tokens": 6}})
    b = _backend(handler)
    out = await b.complete({"model": "m", "messages": []})
    assert out["stop_reason"] == "tool_use"
    block = out["content"][0]
    assert block == {"type": "tool_use", "id": "t9", "name": "post",
                     "input": {"content": "hey"}}
    assert out["usage"] == {"input_tokens": 5, "output_tokens": 6}
    await b.close()


async def test_text_and_tool_calls_both_appear_with_text_first():
    def handler(request):
        return httpx.Response(200, json={"choices": [{
            "finish_reason": "tool_calls",
            "message": {"content": "thinking",
                        "tool_calls": [{"id": "t1", "function":
                                        {"name": "p", "arguments": "{}"}}]}}],
            "usage": {}})
    b = _backend(handler)
    out = await b.complete({"model": "m", "messages": []})
    assert [x["type"] for x in out["content"]] == ["text", "tool_use"]
    await b.close()


async def test_malformed_tool_arguments_degrade_to_an_empty_input():
    # A model can emit invalid JSON here. Raising would lose the whole turn;
    # an empty input lets the tool report its own error back in-band.
    def handler(request):
        return httpx.Response(200, json={"choices": [{
            "finish_reason": "tool_calls",
            "message": {"tool_calls": [{"id": "t1", "function":
                                        {"name": "p", "arguments": "{not json"}}]}}],
            "usage": {}})
    b = _backend(handler)
    out = await b.complete({"model": "m", "messages": []})
    assert out["content"][0]["input"] == {}
    await b.close()


@pytest.mark.parametrize("finish,expected", [
    ("tool_calls", "tool_use"), ("stop", "end_turn"),
    ("length", "max_tokens"), ("weird", "weird")])
async def test_finish_reason_maps_to_stop_reason(finish, expected):
    def handler(request):
        return httpx.Response(200, json={
            "choices": [{"finish_reason": finish, "message": {"content": "x"}}],
            "usage": {}})
    b = _backend(handler)
    out = await b.complete({"model": "m", "messages": []})
    assert out["stop_reason"] == expected
    await b.close()


async def test_the_bearer_token_and_url_are_right():
    b, seen = _capture()
    await b.complete({"model": "m", "messages": []})
    assert seen["_auth"] == "Bearer key"
    assert seen["_url"] == f"{BASE}/chat/completions"
    await b.close()


async def test_4xx_raises_llm_error_with_the_provider_message():
    # 401 rather than 429 deliberately: a bad key is permanent, and retrying it
    # only burns the retry budget. 429 is covered separately as a transient.
    def handler(request):
        return httpx.Response(401, json={"error": {"message": "invalid api key"}})
    b = _backend(handler)
    with pytest.raises(LlmError) as e:
        await b.complete({"model": "m", "messages": []})
    assert "invalid api key" in str(e.value) and e.value.status == 401
    await b.close()


async def test_5xx_raises_for_status_so_the_bus_retries():
    def handler(request):
        return httpx.Response(502, text="bad gateway")
    b = _backend(handler)
    with pytest.raises(httpx.HTTPStatusError):
        await b.complete({"model": "m", "messages": []})
    await b.close()


async def test_a_non_object_error_body_still_raises_llm_error():
    def handler(request):
        return httpx.Response(400, json=["nope"])
    b = _backend(handler)
    with pytest.raises(LlmError):
        await b.complete({"model": "m", "messages": []})
    await b.close()


async def test_an_empty_choices_array_is_an_llm_error_not_an_index_error():
    def handler(request):
        return httpx.Response(200, json={"choices": [], "usage": {}})
    b = _backend(handler)
    with pytest.raises(LlmError):
        await b.complete({"model": "m", "messages": []})
    await b.close()


@pytest.mark.parametrize("status", [429, 408])
async def test_transient_4xx_propagates_for_retry(status):
    # Same rule as the Anthropic backend: 429/408 are retryable 4xx and must
    # reach the Bus rather than being reported to the agent as permanent.
    def handler(request):
        return httpx.Response(status, json={"error": {"message": "rate limited"}})
    b = _backend(handler)
    with pytest.raises(httpx.HTTPStatusError):
        await b.complete({"model": "m", "messages": []})
    await b.close()
