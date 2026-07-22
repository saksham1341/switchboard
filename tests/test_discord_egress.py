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
    async def send(self, channel_id, content=None, *, embed=None, components=None):
        self.sends.append({"channel_id": channel_id, "content": content,
                           "embed": embed, "components": components})


def _gh_event(kind="github.home.pr.opened"):
    return Event(id="G1", kind=kind, source="github", at=now_iso(),
                 payload={"repository": {"full_name": "yp/home", "html_url": "https://github.com/yp/home"},
                          "sender": {"login": "alice"},
                          "pull_request": {"number": 7, "title": "T", "html_url": "https://github.com/yp/home/pull/7"}},
                 meta={})


def test_egress_has_no_coarse_filter_and_ping_echo_present():
    eg = DiscordEgress("bot", "app")
    assert eg.name == "discord"
    assert eg.filter is None                       # sink: selection is per-handler
    names = [h.name for h in eg.handlers]
    assert "ping" in names and "echo" in names
    assert "notify-github" not in names            # no channel configured


def test_ping_echo_filters_are_source_aware():
    eg = DiscordEgress("bot", "app")
    ping = next(h for h in eg.handlers if h.name == "ping")
    echo = next(h for h in eg.handlers if h.name == "echo")
    assert ping.filter(_cmd_event(command="ping")) is True
    assert ping.filter(_gh_event()) is False       # a github event must not hit ping
    assert echo.filter(_gh_event()) is False


def test_notify_github_present_only_when_channel_set():
    eg = DiscordEgress("bot", "app", notify_channel_id="chan-1")
    notify = next(h for h in eg.handlers if h.name == "notify-github")
    assert notify.filter(_gh_event()) is True
    assert notify.filter(_cmd_event(command="ping")) is False   # discord event not relayed


def test_notify_github_posts_embed_and_buttons_to_channel():
    eg = DiscordEgress("bot", "app", notify_channel_id="chan-1")
    notify = next(h for h in eg.handlers if h.name == "notify-github")
    sender = _RecordingSender()
    ctx = Ctx(publish=None, egress=sender)
    asyncio.run(notify.handle(_gh_event("github.home.pr.opened"), ctx))
    assert len(sender.sends) == 1
    sent = sender.sends[0]
    assert sent["channel_id"] == "chan-1"
    assert sent["embed"]["title"] == "🔀 PR #7 opened"
    assert sent["components"][0]["components"][0]["label"] == "View PR"
    assert sender.replies == []


def test_notify_github_acks_unknown_kind_without_posting():
    eg = DiscordEgress("bot", "app", notify_channel_id="chan-1")
    notify = next(h for h in eg.handlers if h.name == "notify-github")
    sender = _RecordingSender()
    ctx = Ctx(publish=None, egress=sender)
    asyncio.run(notify.handle(_gh_event("github.home.pr.locked"), ctx))   # unrecognized -> None
    assert sender.sends == []


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


def _echo_event(message="hi there", token="int-tok"):
    return Event(
        id="E2", kind="discord.9.command.echo", source="discord", at=now_iso(),
        payload={"command": "echo", "options": {"message": message},
                 "user": {"id": "1", "name": "u"}, "channel_id": "7", "guild_id": "9"},
        meta={"interaction_token": token, "channel_id": "7"},
    )


def test_echo_handler_filters_to_echo_only():
    eg = DiscordEgress("bot", "app")
    echo = next(h for h in eg.handlers if h.name == "echo")
    assert echo.filter(_echo_event()) is True
    assert echo.filter(_cmd_event(command="ping")) is False


def test_echo_handler_replies_with_the_message_option():
    eg = DiscordEgress("bot", "app")
    echo = next(h for h in eg.handlers if h.name == "echo")
    sender = _RecordingSender()
    ctx = Ctx(publish=None, egress=sender)
    asyncio.run(echo.handle(_echo_event(message="pong-back", token="tok-2"), ctx))
    assert sender.replies == [("tok-2", "pong-back")]
    assert sender.sends == []
