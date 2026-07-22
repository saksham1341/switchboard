from switchboard.ingress.discord import build_command_event


def test_build_command_event_shape():
    ei = build_command_event(
        command="ping",
        interaction_id=42,
        token="int-tok",
        channel_id=7,
        guild_id=9,
        user_id=1,
        user_name="alice#0001",
        options={"target": "prod"},
    )
    assert ei.kind == "discord.9.command.ping"
    assert ei.source == "discord"
    assert ei.dedupe_key == "42"
    assert ei.payload == {
        "command": "ping",
        "options": {"target": "prod"},
        "user": {"id": "1", "name": "alice#0001"},
        "channel_id": "7",
        "guild_id": "9",
    }
    assert ei.meta == {
        "interaction_token": "int-tok",
        "channel_id": "7",
    }


def test_build_command_event_stringifies_ids_for_msgpack():
    ei = build_command_event(
        command="deploy", interaction_id=1, token="t",
        channel_id=3, guild_id=4, user_id=5, user_name="u", options={},
    )
    assert all(isinstance(v, str) for v in ei.meta.values())
    assert isinstance(ei.payload["user"]["id"], str)
