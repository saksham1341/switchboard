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
