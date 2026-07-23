"""Deciders for Discord slash commands: observation (discord.command.*) -> command (discord.reply)."""
from switchboard.message import DecideCtx, Observation


class PingDecider:
    name = "ping"

    def bind(self, ctx) -> None:
        self.ctx = ctx

    def subscribes(self, obs: Observation) -> bool:
        return obs.name == "discord.command.ping"

    async def decide(self, obs: Observation, ctx: DecideCtx) -> None:
        await ctx.command("discord.reply", {
            "interaction_token": obs.payload["interaction_token"],
            "content": "pong (via the durable path)",
        })


class EchoDecider:
    name = "echo"

    def bind(self, ctx) -> None:
        self.ctx = ctx

    def subscribes(self, obs: Observation) -> bool:
        return obs.name == "discord.command.echo"

    async def decide(self, obs: Observation, ctx: DecideCtx) -> None:
        await ctx.command("discord.reply", {
            "interaction_token": obs.payload["interaction_token"],
            "content": obs.payload.get("options", {}).get("message", ""),
        })
