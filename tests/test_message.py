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
