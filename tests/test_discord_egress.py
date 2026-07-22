import asyncio
from switchboard.egress.discord import DiscordEgress
from switchboard.egress import Ctx
from switchboard.event import Event, now_iso


def _cmd_event(command="ping", token="int-tok"):
    return Event(
        id="E1", kind=f"discord.9.command.{command}", source="discord", at=now_iso(),
        payload={"command": command, "options": {}, "user": {"id": "1", "name": "u"},
                 "channel_id": "7", "guild_id": "9"},
        meta={"interaction_token": token, "channel_id": "7"},
    )


class _RecordingSender:
    def __init__(self):
        self.replies = []
        self.sends = []
    async def reply(self, token, content):
        self.replies.append((token, content))
    async def send(self, channel_id, content):
        self.sends.append((channel_id, content))


def test_discord_egress_shape():
    eg = DiscordEgress("bot", "app")
    assert eg.name == "discord"
    assert eg.filter(_cmd_event()) is True
    other = _cmd_event(); object.__setattr__(other, "source", "github")
    assert eg.filter(other) is False                  # non-discord gated out
    assert "ping" in [h.name for h in eg.handlers]


def test_ping_handler_filters_to_ping_only():
    eg = DiscordEgress("bot", "app")
    ping = next(h for h in eg.handlers if h.name == "ping")
    assert ping.filter(_cmd_event(command="ping")) is True
    assert ping.filter(_cmd_event(command="deploy")) is False


def test_ping_handler_replies_via_followup():
    eg = DiscordEgress("bot", "app")
    ping = next(h for h in eg.handlers if h.name == "ping")
    sender = _RecordingSender()
    ctx = Ctx(publish=None, egress=sender)
    asyncio.run(ping.handle(_cmd_event(token="tok-9"), ctx))
    assert sender.replies == [("tok-9", "pong (via the durable path)")]
    assert sender.sends == []
