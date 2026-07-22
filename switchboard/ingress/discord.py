from switchboard.event import EventInput


def build_command_event(*, command, interaction_id, token,
                        channel_id, guild_id, user_id, user_name, options) -> EventInput:
    """Translate a Discord slash-command interaction into a Switchboard event.

    Ids are stringified because mamamia round-trips payloads through msgpack and
    Discord ids are 64-bit snowflakes. `meta` carries the reply address
    (interaction token + channel) so a downstream handler can reply via the
    egress, even after a restart, within Discord's 15-min window. The bot's
    application id is deployment config on the egress sender, not per-event.
    """
    return EventInput(
        kind=f"discord.{guild_id}.command.{command}",
        source="discord",
        payload={
            "command": command,
            "options": options,
            "user": {"id": str(user_id), "name": user_name},
            "channel_id": str(channel_id),
            "guild_id": str(guild_id),
        },
        dedupe_key=str(interaction_id),
        meta={
            "interaction_token": token,
            "channel_id": str(channel_id),
        },
    )
