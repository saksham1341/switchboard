import httpx

DISCORD_API = "https://discord.com/api/v10"


class DiscordSender:
    """The Discord egress's two send paths, both plain HTTP (no gateway):

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

    async def send(self, channel_id: str, content: str) -> httpx.Response:
        resp = await self._client.post(
            f"{DISCORD_API}/channels/{channel_id}/messages",
            headers={"Authorization": f"Bot {self._bot_token}"},
            json={"content": content},
        )
        resp.raise_for_status()
        return resp

    async def close(self) -> None:
        await self._client.aclose()


from switchboard.egress import Handler


class DiscordEgress:
    """Egress half of the Discord connector. Its `context()` hands handlers a
    DiscordSender (the two HTTP send paths); this egress also hosts the /ping
    demo handler. Real command handlers are added the same way as scope grows."""

    name = "discord"

    def __init__(self, bot_token: str, application_id: str, *,
                 client: httpx.AsyncClient | None = None):
        self._sender = DiscordSender(bot_token, application_id, client=client)
        self.filter = lambda e: e.source == "discord"      # coarse gate
        self.handlers = [
            Handler(
                name="ping",
                filter=lambda e: e.payload.get("command") == "ping",
                handle=self._ping,
            ),
        ]

    def context(self) -> DiscordSender:
        return self._sender

    async def _ping(self, event, ctx) -> None:
        # model A: reply to the interaction via its stored token
        await ctx.egress.reply(event.meta["interaction_token"], "pong (via the durable path)")

    async def close(self) -> None:
        await self._sender.close()
