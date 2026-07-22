import httpx

DISCORD_API = "https://discord.com/api/v10"


class DiscordSender:
    """The Discord connector's two send paths, both plain HTTP (no gateway):

    - reply(): an interaction *followup* — POST to the interaction webhook, which
      needs only the application id + interaction token (no bot auth) and is valid
      for 15 minutes. This is how a slash command's result reaches the user.
    - send(): a channel message via the bot REST API (Bot-token auth), with no
      time window — for work that outlives the interaction, and for notifications.
    """

    def __init__(self, bot_token: str, application_id: str, *,
                 client: httpx.AsyncClient | None = None):
        self._bot_token = bot_token
        self._application_id = str(application_id)
        self._client = client or httpx.AsyncClient(timeout=10.0)

    async def reply(self, interaction_token: str, content: str) -> httpx.Response:
        resp = await self._client.post(
            f"{DISCORD_API}/webhooks/{self._application_id}/{interaction_token}",
            json={"content": content},
        )
        resp.raise_for_status()
        return resp

    async def send(self, channel_id: str, content: str | None = None, *,
                   embed: dict | None = None,
                   components: list | None = None) -> httpx.Response:
        payload: dict = {}
        if content is not None:
            payload["content"] = content
        if embed is not None:
            payload["embeds"] = [embed]
        if components is not None:
            payload["components"] = components
        resp = await self._client.post(
            f"{DISCORD_API}/channels/{channel_id}/messages",
            headers={"Authorization": f"Bot {self._bot_token}"},
            json=payload,
        )
        resp.raise_for_status()
        return resp

    async def close(self) -> None:
        await self._client.aclose()


class DiscordPost:
    """Actuator for the `discord.post` command: post a channel message."""
    name = "discord.post"

    def __init__(self, bot_token, application_id, *, channel_id=None, client=None):
        self._sender = DiscordSender(bot_token, application_id, client=client)
        self._default_channel = channel_id

    def context(self):
        return self._sender

    async def act(self, cmd, ctx):
        channel = cmd.args.get("channel_id") or self._default_channel
        await ctx.context.send(channel, embed=cmd.args.get("embed"),
                               components=cmd.args.get("components"))
        await ctx.result("ok", {"channel_id": channel})

    async def close(self):
        await self._sender.close()


class DiscordReply:
    """Actuator for the `discord.reply` command: interaction followup (model A)."""
    name = "discord.reply"

    def __init__(self, bot_token, application_id, *, client=None):
        self._sender = DiscordSender(bot_token, application_id, client=client)

    def context(self):
        return self._sender

    async def act(self, cmd, ctx):
        await ctx.context.reply(cmd.args["interaction_token"], cmd.args["content"])
        await ctx.result("ok")

    async def close(self):
        await self._sender.close()
