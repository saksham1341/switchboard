import pytest

from switchboard.deciders.agent.memory import MEMORY_TOOLS, rewrite


def _names(): return {t["name"] for t in MEMORY_TOOLS}


def test_the_agent_sees_two_tools_and_never_raw_kv():
    assert _names() == {"scratchpad", "memory"}


def test_every_kv_op_is_exposed():
    for t in MEMORY_TOOLS:
        assert set(t["input_schema"]["properties"]["op"]["enum"]) == {
            "get", "set", "delete", "list"}


def test_scratchpad_is_namespaced_to_its_session():
    args = rewrite("scratchpad", {"op": "set", "key": "draft", "value": "x"},
                   sid=100, ttl=3600.0)
    assert args["key"] == "session:100:draft"
    assert args["ttl"] == 3600.0


def test_memory_is_global_and_has_no_ttl():
    args = rewrite("memory", {"op": "set", "key": "prefs", "value": "x"},
                   sid=100, ttl=3600.0)
    assert args["key"] == "global:prefs"
    assert args.get("ttl") is None


def test_a_session_cannot_name_a_key_outside_its_own_scratchpad():
    """The prefix is a security boundary, not wiring: the decider applies it,
    not the model, so a prompt-injected agent cannot reach another session."""
    args = rewrite("scratchpad", {"op": "get", "key": "../../session:999:secret"},
                   sid=100, ttl=None)
    assert args["key"].startswith("session:100:")
    assert "session:999" not in args["key"].removeprefix("session:100:")


def test_list_is_scoped_to_the_namespace_not_the_whole_store():
    args = rewrite("scratchpad", {"op": "list"}, sid=100, ttl=None)
    assert args["prefix"] == "session:100:"
    args = rewrite("memory", {"op": "list"}, sid=100, ttl=None)
    assert args["prefix"] == "global:"


def test_an_unknown_op_is_rejected_before_a_command_is_emitted():
    assert rewrite("memory", {"op": "drop_everything", "key": "k"},
                   sid=100, ttl=None) is None


def test_a_non_memory_tool_is_not_ours():
    assert rewrite("discord.post", {"content": "hi"}, sid=100, ttl=None) is None


# --- the DATA property: distinct keys must stay distinct ---------------------

def _key(k, tool="memory"):
    return rewrite(tool, {"op": "set", "key": k, "value": "v"},
                   sid=100, ttl=None)["key"]


def test_two_keys_differing_only_before_a_colon_do_not_collide():
    """The bug: the leading-junk regex was greedy through the LAST colon, so
    every colon-delimited key collapsed to its final segment. Two distinct
    memories landed on one stored key and the second silently destroyed the
    first — and `op: list` showed only the collapsed key, so the model got no
    signal that anything had been lost. A colon-delimited key is the single
    most likely thing an LLM types into a key/value store."""
    assert _key("project:alpha:status") != _key("project:beta:status")


def test_a_colon_key_is_not_truncated_to_its_last_segment():
    assert _key("notes:draft") != _key("draft")
    assert "notes" in _key("notes:draft")


def test_distinct_keys_stay_distinct_through_the_sanitiser():
    raw = ["a", "a:b", "a:b:c", "b:a", ":a", "a:", "..a", "a/b", "ab", "a%b"]
    assert len({_key(k) for k in raw}) == len(raw)


def test_the_scratchpad_keeps_colon_keys_apart_too():
    assert (_key("project:alpha", tool="scratchpad")
            != _key("project:beta", tool="scratchpad"))
    assert _key("project:alpha", tool="scratchpad").startswith("session:100:")


# --- the SECURITY property, unchanged ---------------------------------------

def test_no_sanitised_key_can_carry_a_namespace_tag():
    """A colon is what makes a namespace. Whatever the tail becomes, it must
    not contain one — that, not the shape of the leading strip, is what makes
    forging a namespace impossible."""
    for hostile in ["global:owned", "session:999:secret", "../session:999:x",
                    "session:999:../secret", "..:..:global:x", ":::global:x"]:
        for tool, prefix in [("memory", "global:"), ("scratchpad", "session:100:")]:
            full = _key(hostile, tool=tool)
            assert full.startswith(prefix)
            assert ":" not in full.removeprefix(prefix)


def test_a_memory_key_cannot_reach_the_scratchpad_and_back():
    assert _key("session:100:draft", tool="memory").startswith("global:")
    assert "session:100:" not in _key("session:100:draft", tool="memory")[len("global:"):]
