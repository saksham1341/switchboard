import inspect, discord
from starlette.testclient import TestClient
from switchboard.sensors.discord import DiscordSensor, CommandSpec, Option


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


class _FakeGuild:
    id = 9


class _FakeMessage:
    def __init__(self, channel=None, mentions=(), author=None, content="hi", mid=1234567890):
        self.id = mid
        self.channel = channel if channel is not None else _FakeChannel()
        self.guild = _FakeGuild()
        self.author = author or _FakeAuthor()
        self.content = content
        self.mentions = list(mentions)


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


async def test_on_message_emits_for_a_human():
    emitted = []
    s = _sensor(messages=True)
    s.ctx = type("C", (), {"emit": staticmethod(
        lambda n, p: emitted.append((n, p)) or _done())})()
    await s._on_message(_FakeMessage(), bot_id=555)
    assert emitted and emitted[0][0] == "discord.message"


async def test_on_message_ignores_bots_including_itself():
    emitted = []
    s = _sensor(messages=True)
    s.ctx = type("C", (), {"emit": staticmethod(
        lambda n, p: emitted.append((n, p)) or _done())})()
    await s._on_message(_FakeMessage(author=_FakeAuthor(uid=555, bot=True)), bot_id=555)
    await s._on_message(_FakeMessage(author=_FakeAuthor(uid=777, bot=True)), bot_id=555)
    assert emitted == []


def test_messages_off_keeps_intents_none_and_registers_no_listener():
    s = _sensor()
    assert s._client.intents.value == discord.Intents.none().value
    assert not s.messages


def test_messages_on_requests_exactly_the_needed_intents():
    s = _sensor(messages=True)
    i = s._client.intents
    assert i.guilds and i.guild_messages and i.message_content
    assert not i.members and not i.presences
    assert s.messages
