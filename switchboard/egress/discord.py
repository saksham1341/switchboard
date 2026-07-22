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


from switchboard.egress import Handler
from switchboard.egress.github_notify import build_message


class DiscordEgress:
    """The Discord output sink. `context()` hands handlers the one DiscordSender
    (reply + channel send). Handlers are routes into Discord, each with its own
    input filter: `ping`/`echo` react to Discord slash-command events; `notify-
    github` relays GitHub events as channel messages. The egress has no coarse
    filter — a sink serves multiple input sources, so selection is per-handler.
    """

    name = "discord"

    def __init__(self, bot_token: str, application_id: str, *,
                 notify_channel_id: str | None = None,
                 client: httpx.AsyncClient | None = None):
        self._sender = DiscordSender(bot_token, application_id, client=client)
        self._notify_channel_id = notify_channel_id
        self.filter = None                                  # sink: no coarse gate
        self.handlers = [
            Handler(name="ping",
                    filter=lambda e: e.source == "discord" and e.payload.get("command") == "ping",
                    handle=self._ping),
            Handler(name="echo",
                    filter=lambda e: e.source == "discord" and e.payload.get("command") == "echo",
                    handle=self._echo),
        ]
        if notify_channel_id:
            self.handlers.append(Handler(
                name="notify-github",
                filter=lambda e: e.source == "github",
                handle=self._notify,
            ))

    def context(self) -> DiscordSender:
        return self._sender

    async def _ping(self, event, ctx) -> None:
        await ctx.egress.reply(event.meta["interaction_token"], "pong (via the durable path)")

    async def _echo(self, event, ctx) -> None:
        message = event.payload.get("options", {}).get("message", "")
        await ctx.egress.reply(event.meta["interaction_token"], message)

    async def _notify(self, event, ctx) -> None:
        # relay a GitHub event to the notify channel as an embed + link buttons
        msg = build_message(event.kind, event.payload)
        if msg is None:
            return                                          # unrecognized kind: ack, no post
        await ctx.egress.send(self._notify_channel_id,
                              embed=msg["embed"], components=msg["components"])

    async def close(self) -> None:
        await self._sender.close()
