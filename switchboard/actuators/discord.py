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
    """Actuator for the `discord.post` command: post a channel or thread message.

    The tool exposes the destination, so an agent may post wherever the bot can
    reach. Deliberate for v1 (one private guild, trusted members); masking ids
    behind configured names is a recorded, purely additive follow-up.
    """
    name = "discord.post"
    tool_spec = {
        "description": "Send a message to Discord.",
        "input_schema": {
            "type": "object",
            "properties": {
                "content": {"type": "string", "description": "The message text."},
                "channel_id": {"type": "string",
                               "description": "Where to post. Omit for the "
                                              "current conversation."},
            },
            "required": ["content"],
        },
    }

    def __init__(self, bot_token, application_id, *, channel_id=None, client=None):
        self._token, self._app_id = bot_token, application_id
        self._default_channel = channel_id
        self._client = client
        self._sender = None

    def bind(self, ctx):
        self.ctx = ctx
        self._sender = DiscordSender(self._token, self._app_id, client=self._client)

    async def act(self, cmd, ctx):
        channel = cmd.args.get("channel_id") or self._default_channel
        resp = await self._sender.send(channel,
                                       content=cmd.args.get("content"),
                                       embed=cmd.args.get("embed"),
                                       components=cmd.args.get("components"))
        # Never trust the shape of the body, only that it parsed. A post we
        # merely could not read the id from is still a successful post.
        message_id = None
        try:
            body = resp.json()
        except Exception:
            body = None
        if isinstance(body, dict):
            message_id = body.get("id")
        await ctx.result("ok", {"channel_id": channel, "message_id": message_id})

    async def close(self):
        if self._sender is not None:
            await self._sender.close()


class DiscordReply:
    """Actuator for the `discord.reply` command: interaction followup (model A)."""
    name = "discord.reply"

    def __init__(self, bot_token, application_id, *, client=None):
        self._token, self._app_id = bot_token, application_id
        self._client = client
        self._sender = None

    def bind(self, ctx):
        self.ctx = ctx
        self._sender = DiscordSender(self._token, self._app_id, client=self._client)

    async def act(self, cmd, ctx):
        await self._sender.reply(cmd.args["interaction_token"], cmd.args["content"])
        await ctx.result("ok")

    async def close(self):
        if self._sender is not None:
            await self._sender.close()
