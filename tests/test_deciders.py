from switchboard.deciders.github_notify import GitHubNotifyDecider
from switchboard.deciders.discord_cmds import PingDecider, EchoDecider
from switchboard.message import Observation, DecideCtx


def _obs(name, payload): return Observation(id=1, name=name, payload=payload)


class _Rec:
    def __init__(self): self.cmds = []
    async def __call__(self, name, args, obs_id): self.cmds.append((name, args)); return 0
def _ctx(obs, rec): return DecideCtx(obs=obs, _emit_command=rec)


async def test_github_notify_emits_discord_post():
    rec = _Rec()
    d = GitHubNotifyDecider(channel_id="chan-9")
    obs = _obs("github.home.pr.opened",
               {"repository": {"full_name": "yp/home", "html_url": "https://github.com/yp/home"},
                "sender": {"login": "alice"},
                "pull_request": {"number": 7, "title": "T", "html_url": "https://github.com/yp/home/pull/7"}})
    assert d.subscribes(obs) is True
    await d.decide(obs, _ctx(obs, rec))
    (name, args), = rec.cmds
    assert name == "discord.post"
    assert args["channel_id"] == "chan-9"
    assert args["embed"]["title"] == "🔀 PR #7 opened"
    assert args["components"][0]["components"][0]["label"] == "View PR"


async def test_github_notify_skips_unknown_kind():
    rec = _Rec()
    d = GitHubNotifyDecider(channel_id="c")
    obs = _obs("github.home.pr.locked", {})
    await d.decide(obs, _ctx(obs, rec))
    assert rec.cmds == []


async def test_ping_and_echo_emit_reply():
    rec = _Rec()
    p = PingDecider()
    obs = _obs("discord.command.ping", {"interaction_token": "tok"})
    assert p.subscribes(obs)
    await p.decide(obs, _ctx(obs, rec))
    assert rec.cmds == [("discord.reply", {"interaction_token": "tok", "content": "pong (via the durable path)"})]

    rec2 = _Rec()
    e = EchoDecider()
    obs2 = _obs("discord.command.echo", {"interaction_token": "t2", "options": {"message": "hi"}})
    await e.decide(obs2, _ctx(obs2, rec2))
    assert rec2.cmds == [("discord.reply", {"interaction_token": "t2", "content": "hi"})]
