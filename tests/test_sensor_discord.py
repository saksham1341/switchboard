import inspect, discord
import pytest
from starlette.testclient import TestClient
from switchboard.sensors.discord import DiscordSensor, CommandSpec, Option
from switchboard.store import MemoryStore


async def _done():
    return 1


def _sensor(**kw):
    return DiscordSensor("bot", commands=[
        CommandSpec("ping", "Ping"),
        CommandSpec("echo", "Echo", options=(Option("message", "text", type=str, required=True),)),
    ], guild_id="456", **kw)


def test_sensor_registers_typed_commands_without_network():
    s = _sensor()
    assert s.name == "discord"
    names = {c.name for c in s._tree.get_commands()}
    assert {"ping", "echo"} <= names
    echo = next(c for c in s._tree.get_commands() if c.name == "echo")
    params = {p.name: p for p in echo.parameters}
    assert params["message"].type is discord.AppCommandOptionType.string
    assert inspect.iscoroutinefunction(s.start) and inspect.iscoroutinefunction(s.stop)


class _FakeUser:
    id = 123
    def __str__(self): return "alice#0001"


class _FakeInteraction:
    token = "int-tok"
    channel_id = 7
    guild_id = 9
    user = _FakeUser()


def test_command_observation_shaping():
    from switchboard.sensors.discord import _command_observation
    name, payload = _command_observation("echo", _FakeInteraction(), {"message": "hi"})
    assert name == "discord.command.echo"
    assert payload == {
        "interaction_token": "int-tok", "channel_id": "7", "guild_id": "9",
        "user_id": "123", "user_name": "alice#0001", "options": {"message": "hi"},
    }


def test_bind_stores_ctx_and_declares_no_routes():
    from switchboard.http import HttpServer
    from switchboard.store import MemoryStore
    from switchboard.message import SensorCtx
    from switchboard.scheduler import Scheduler

    async def emit(name, payload): return 1

    http = HttpServer(serve=False)
    s = _sensor()
    s.bind(SensorCtx(emit=emit, http=http, store=MemoryStore(),
                     schedule=Scheduler().for_owner("discord")))
    assert s.ctx is not None
    assert TestClient(http.app).post("/webhook/discord").status_code == 404


class _FakeAuthor:
    def __init__(self, uid=123, bot=False):
        self.id, self.bot = uid, bot
    def __str__(self): return "alice#0001"


class _FakeThread(discord.Thread):
    def __init__(self, cid=222, parent=111, count=23):
        self.id, self.parent_id, self.message_count = cid, parent, count


class _FakeChannel:
    def __init__(self, cid=222):
        self.id = cid


class _FakeRole:
    def __init__(self, rid, default=False):
        self.id, self._default = rid, default
    def is_default(self): return self._default


class _FakeMember:
    def __init__(self, roles): self.roles = roles


class _FakeGuild:
    def __init__(self, gid=9, bot_role_ids=()):
        self.id = gid
        # roles[0] is always @everyone (the default role, id == guild id).
        self.me = _FakeMember([_FakeRole(gid, default=True)]
                              + [_FakeRole(r) for r in bot_role_ids])


class _FakeMessage:
    def __init__(self, channel=None, mentions=(), author=None, content="hi",
                 mid=1234567890, role_mentions=(), guild=None, mention_everyone=False):
        self.id = mid
        self.channel = channel if channel is not None else _FakeChannel()
        self.guild = _FakeGuild() if guild is None else guild
        self.author = author or _FakeAuthor()
        self.content = content
        self.mentions = list(mentions)
        self.role_mentions = list(role_mentions)
        self.mention_everyone = mention_everyone


def test_message_observation_in_thread_carries_the_hint():
    from switchboard.sensors.discord import _message_observation
    bot = _FakeAuthor(uid=555, bot=True)
    name, payload = _message_observation(
        _FakeMessage(channel=_FakeThread(), mentions=[bot],
                     content="hey <@555> thoughts?"), bot_id=555)
    assert name == "discord.message"
    assert payload == {
        "message_id": "1234567890", "channel_id": "222", "parent_id": "111",
        "thread_id": "222", "guild_id": "9", "user_id": "123",
        "user_name": "alice#0001", "content": "hey <@555> thoughts?",
        "mentions": ["555"], "mentions_bot": True,
        "thread": {"is_thread": True, "message_count": 23},
    }


def test_message_observation_in_plain_channel_has_no_thread():
    from switchboard.sensors.discord import _message_observation
    _, payload = _message_observation(_FakeMessage(), bot_id=555)
    assert payload["thread_id"] is None
    assert payload["parent_id"] is None
    assert payload["thread"] == {"is_thread": False, "message_count": None}
    assert payload["mentions"] == [] and payload["mentions_bot"] is False


def test_message_observation_tolerates_a_thread_without_a_count():
    # Discord omits message_count on threads created before it was tracked.
    from switchboard.sensors.discord import _message_observation
    _, payload = _message_observation(
        _FakeMessage(channel=_FakeThread(count=None)), bot_id=555)
    assert payload["thread"] == {"is_thread": True, "message_count": None}


def test_message_observation_outside_a_guild_has_no_guild_id():
    from switchboard.sensors.discord import _message_observation
    msg = _FakeMessage()
    msg.guild = None
    _, payload = _message_observation(msg, bot_id=555)
    assert payload["guild_id"] is None


def test_message_observation_detects_a_mention_of_a_bot_role():
    # `@switchboard` often resolves to the bot's same-name ROLE, which lands in
    # role_mentions, never mentions. Missing it silently ignores the ping.
    from switchboard.sensors.discord import _message_observation
    _, payload = _message_observation(
        _FakeMessage(role_mentions=[_FakeRole(777)],
                     content="<@&777> summarize"),
        bot_id=555, bot_role_ids=[777])
    assert payload["mentions_bot"] is True
    assert payload["mentions"] == []          # a role ping is not a user mention


def test_message_observation_ignores_a_role_the_bot_does_not_hold():
    from switchboard.sensors.discord import _message_observation
    _, payload = _message_observation(
        _FakeMessage(role_mentions=[_FakeRole(999)]),
        bot_id=555, bot_role_ids=[777])
    assert payload["mentions_bot"] is False


def test_message_observation_still_detects_a_direct_user_mention_without_roles():
    from switchboard.sensors.discord import _message_observation
    bot = _FakeAuthor(uid=555, bot=True)
    _, payload = _message_observation(_FakeMessage(mentions=[bot]), bot_id=555)
    assert payload["mentions_bot"] is True


def _ctx_with_store(emit):
    return type("C", (), {"emit": staticmethod(emit), "store": MemoryStore()})()


async def test_on_message_wakes_on_a_role_ping():
    # End to end through the sensor: the bot holds role 777, someone pings that
    # role, and the emitted observation must carry mentions_bot=True so the
    # agent actually advances.
    emitted = []
    s = _sensor()
    s.ctx = _ctx_with_store(lambda n, p: emitted.append((n, p)) or _done())
    msg = _FakeMessage(guild=_FakeGuild(bot_role_ids=[777]),
                       role_mentions=[_FakeRole(777)], content="<@&777> yo")
    await s._on_message(msg, bot_id=555)
    assert emitted and emitted[0][1]["mentions_bot"] is True


async def test_on_message_does_not_match_the_default_role_in_the_role_list():
    # The bot's default role (@everyone, id == guild id) is excluded from its
    # role set, so a default role appearing in role_mentions must not match it.
    # (A true @everyone broadcast arrives via mention_everyone, tested below —
    # not through the role list.)
    emitted = []
    s = _sensor()
    s.ctx = _ctx_with_store(lambda n, p: emitted.append((n, p)) or _done())
    g = _FakeGuild(gid=9, bot_role_ids=[])          # bot has only @everyone
    msg = _FakeMessage(guild=g, role_mentions=[_FakeRole(9, default=True)],
                       content="<@&9> hey all")
    await s._on_message(msg, bot_id=555)
    assert emitted and emitted[0][1]["mentions_bot"] is False


def test_message_observation_wakes_on_an_everyone_or_here_broadcast():
    # @everyone and @here both set mention_everyone; the bot is part of everyone,
    # so it wakes and lets the model judge whether the broadcast wants a reply.
    from switchboard.sensors.discord import _message_observation
    _, payload = _message_observation(
        _FakeMessage(mention_everyone=True, content="@everyone deploy is live"),
        bot_id=555)
    assert payload["mentions_bot"] is True


async def test_on_message_emits_for_a_human():
    emitted = []
    s = _sensor()
    s.ctx = _ctx_with_store(lambda n, p: emitted.append((n, p)) or _done())
    await s._on_message(_FakeMessage(), bot_id=555)
    assert emitted and emitted[0][0] == "discord.message"


async def test_on_message_ignores_bots_including_itself():
    emitted = []
    s = _sensor()
    s.ctx = _ctx_with_store(lambda n, p: emitted.append((n, p)) or _done())
    await s._on_message(_FakeMessage(author=_FakeAuthor(uid=555, bot=True)), bot_id=555)
    await s._on_message(_FakeMessage(author=_FakeAuthor(uid=777, bot=True)), bot_id=555)
    assert emitted == []


async def test_on_message_dedupes_a_replayed_message_id():
    # Discord replays gateway events on RESUME; the same message id must not
    # become a second observation.
    emitted = []
    s = _sensor()
    s.ctx = _ctx_with_store(lambda n, p: emitted.append((n, p)) or _done())
    msg = _FakeMessage(mid=42)
    await s._on_message(msg, bot_id=555)
    await s._on_message(msg, bot_id=555)          # replay: same message id
    assert len(emitted) == 1


async def test_on_message_still_emits_a_different_message_id():
    emitted = []
    s = _sensor()
    s.ctx = _ctx_with_store(lambda n, p: emitted.append((n, p)) or _done())
    await s._on_message(_FakeMessage(mid=1), bot_id=555)
    await s._on_message(_FakeMessage(mid=2), bot_id=555)
    assert len(emitted) == 2


async def test_on_message_does_not_mark_seen_when_emit_raises():
    # The invariant whose failure loses events silently: a failed emit must
    # not write the seen-key, so a redelivered copy gets another chance.
    calls = {"n": 0}

    async def flaky_emit(n, p):
        calls["n"] += 1
        raise RuntimeError("boom")

    s = _sensor()
    s.ctx = _ctx_with_store(flaky_emit)
    msg = _FakeMessage(mid=99)
    await s._on_message(msg, bot_id=555)           # swallowed, logged
    assert await s.ctx.store.get("discord:message:99") is None
    await s._on_message(msg, bot_id=555)           # not marked seen: tries again
    assert calls["n"] == 2


async def test_on_message_emit_failure_is_logged_at_error_level(caplog):
    async def flaky_emit(n, p):
        raise RuntimeError("boom")

    s = _sensor()
    s.ctx = _ctx_with_store(flaky_emit)
    with caplog.at_level("ERROR", logger="switchboard.sensors.discord"):
        await s._on_message(_FakeMessage(mid=7), bot_id=555)
    assert any("7" in r.message and r.levelname == "ERROR" for r in caplog.records)


def test_the_sensor_always_requests_exactly_the_needed_intents():
    # Unconditional since the agent depends on hearing messages. The negative
    # half is the point: we ask for message_content because we cannot work
    # without it, and for nothing else -- members and presences are privileged
    # too and would widen the portal approval we need for no reason.
    i = _sensor()._client.intents
    assert i.guilds and i.guild_messages and i.message_content
    assert not i.members and not i.presences


def test_the_sensor_always_registers_the_message_listener():
    # @client.event setattr's the coroutine onto the client, so the attribute's
    # presence is the listener's presence.
    assert hasattr(_sensor()._client, "on_message")
