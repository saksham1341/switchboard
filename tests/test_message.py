from switchboard.message import Observation, Command, OBS_LOG, CMD_LOG


class _Msg:
    def __init__(self, id, payload, metadata):
        self.id, self.payload, self.metadata = id, payload, metadata


def test_logs_are_named():
    assert OBS_LOG == "obs" and CMD_LOG == "cmd"


def test_observation_from_message():
    o = Observation.from_message(_Msg(5, {"a": 1}, {"name": "github.home.pr.opened"}))
    assert (o.id, o.name, o.payload, o.command_id) == (5, "github.home.pr.opened", {"a": 1}, None)


def test_result_observation_carries_command_id():
    o = Observation.from_message(_Msg(9, {"message_id": "m1"}, {"name": "discord.post.ok", "command_id": 3}))
    assert o.command_id == 3 and o.name == "discord.post.ok"


def test_command_from_message():
    c = Command.from_message(_Msg(3, {"channel": "c"}, {"name": "discord.post", "observation_id": 5}))
    assert (c.id, c.name, c.args, c.observation_id) == (3, "discord.post", {"channel": "c"}, 5)


from switchboard.message import SensorCtx, DeciderCtx, ActuatorCtx, TapCtx
from switchboard.store import MemoryStore
from switchboard.http import HttpServer
from switchboard.scheduler import Scheduler


def test_sensor_ctx_carries_the_four_capabilities():
    async def emit(name, payload): return 1
    http, store, sched = HttpServer(serve=False), MemoryStore(), Scheduler()
    ctx = SensorCtx(emit=emit, http=http, store=store,
                    schedule=sched.for_owner("s"))
    assert ctx.emit is emit and ctx.http is http and ctx.store is store
    assert ctx.schedule is not None


def test_decider_ctx_is_store_only():
    ctx = DeciderCtx(store=MemoryStore())
    assert set(vars(ctx)) == {"store"}          # no emit, no http, no schedule


def test_actuator_ctx_is_store_only():
    ctx = ActuatorCtx(store=MemoryStore())
    assert set(vars(ctx)) == {"store"}


def test_tap_ctx_is_store_only():
    ctx = TapCtx(store=MemoryStore())
    assert set(vars(ctx)) == {"store"}          # no emit, no http: a tap reads


def test_act_ctx_has_no_context_field():
    from switchboard.message import ActCtx
    assert "context" not in ActCtx.__dataclass_fields__


def test_observation_carries_emitted_by():
    class M:
        id = 7
        payload = {"a": 1}
        metadata = {"name": "github.home.pr.opened", "emitted_by": "sensor/github"}
    obs = Observation.from_message(M())
    assert obs.emitted_by == "sensor/github"


def test_command_carries_emitted_by():
    class M:
        id = 3
        payload = {}
        metadata = {"name": "discord.post", "observation_id": 7,
                    "emitted_by": "decider/github_notify"}
    cmd = Command.from_message(M())
    assert cmd.emitted_by == "decider/github_notify"


def test_emitted_by_is_none_when_absent():
    class M:
        id = 1
        payload = {}
        metadata = {"name": "x"}
    assert Observation.from_message(M()).emitted_by is None
    assert Command.from_message(M()).emitted_by is None


import json


def _msg(payload, metadata):
    class M:
        id = 5
    m = M()
    m.payload = payload
    m.metadata = metadata
    return m


def test_observation_rendered_uses_the_stored_text_when_present():
    obs = Observation.from_message(_msg({"a": 1}, {"name": "x", "text": "PRETTY"}))
    assert obs.text == "PRETTY"
    assert obs.rendered == "PRETTY"


def test_observation_rendered_falls_back_to_json_when_absent():
    obs = Observation.from_message(_msg({"a": 1}, {"name": "x"}))
    assert obs.text is None
    assert json.loads(obs.rendered) == {"a": 1}


def test_command_rendered_falls_back_over_args():
    cmd = Command.from_message(_msg({"b": 2}, {"name": "y"}))
    assert json.loads(cmd.rendered) == {"b": 2}


def test_rendered_survives_an_unserialisable_payload():
    # Degrade, never raise: a reader asking for text must not take down a turn.
    obs = Observation.from_message(_msg({"bad": {1, 2}}, {"name": "x"}))
    assert isinstance(obs.rendered, str)
