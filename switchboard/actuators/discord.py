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

    async def fetch_messages(self, channel_id: str, *, limit: int,
                             before: str | None = None) -> httpx.Response:
        """Read a channel's or thread's recent messages. Returns the response
        unraised — the caller decides which statuses are worth a retry, because
        a 403 is a fact to report to the agent while a 502 is worth retrying.

        No gateway intent is involved: message_content gates gateway *events*,
        not the REST API, which needs only Read Message History on the channel.
        """
        params: dict = {"limit": limit}
        if before is not None:
            params["before"] = before
        return await self._client.get(
            f"{DISCORD_API}/channels/{channel_id}/messages",
            headers={"Authorization": f"Bot {self._bot_token}"},
            params=params,
        )

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


HISTORY_DEFAULT = 50           # enough to recover a mid-thread mention's context
HISTORY_MAX = 100              # Discord's hard ceiling for this endpoint


def _error_message(resp) -> str:
    """Discord's message for a failed call, defensively. Never assume the body
    parsed into an object: a non-dict body must not turn a reported failure
    into a raised AttributeError."""
    try:
        body = resp.json()
    except Exception:
        body = None
    if isinstance(body, dict):
        message = body.get("message")
        if isinstance(message, str) and message:
            return message
    return f"discord returned {resp.status_code}"


class DiscordHistory:
    """Actuator for `discord.history`: read prior messages from a channel or thread.

    This is the answer to spec §6.5 — a mention arriving twenty messages into a
    thread gives the agent a conversation that starts at the mention. Rather than
    hydrating every new session (paying on every `@switchboard what's 2+2`), the
    fetch is a tool the agent calls when the thread hint tells it context exists.

    `before` lets it exclude messages already in its conversation. Nothing
    enforces that — the decider does not rewrite the agent's arguments — the
    schema only makes a clean fetch expressible.
    """

    name = "discord.history"
    tool_spec = {
        "description": (
            "Read earlier messages from a Discord channel or thread, oldest "
            "first. Use this when you are mentioned partway into a thread and "
            "the request refers to something you cannot see."),
        "input_schema": {
            "type": "object",
            "properties": {
                "channel_id": {"type": "string",
                               "description": "The channel or thread to read."},
                "limit": {"type": "integer",
                          "description": f"How many messages, 1-{HISTORY_MAX}. "
                                         f"Defaults to {HISTORY_DEFAULT}."},
                "before": {"type": "string",
                           "description": "Only messages older than this message "
                                          "id. Use it to skip messages you have "
                                          "already seen."},
            },
            "required": ["channel_id"],
        },
    }

    def __init__(self, bot_token, application_id, *, client=None):
        self._token, self._app_id = bot_token, application_id
        self._client = client
        self._sender = None

    def bind(self, ctx):
        self.ctx = ctx
        self._sender = DiscordSender(self._token, self._app_id, client=self._client)

    async def act(self, cmd, ctx):
        args = cmd.args or {}
        channel_id = args.get("channel_id")
        if not isinstance(channel_id, str) or not channel_id:
            return await ctx.result("error", {"message": "channel_id is required"})

        limit = args.get("limit", HISTORY_DEFAULT)
        # bool is an int subclass, and `True` as a limit is a caller bug, not a 1.
        if isinstance(limit, bool) or not isinstance(limit, int):
            return await ctx.result("error", {"message": "limit must be an integer"})
        limit = max(1, min(HISTORY_MAX, limit))

        before = args.get("before")
        if before is not None and not isinstance(before, str):
            return await ctx.result("error", {"message": "before must be a message id string"})

        resp = await self._sender.fetch_messages(channel_id, limit=limit, before=before)
        if resp.status_code >= 500:
            # Transient: raise so the bus retries with backoff rather than
            # telling the agent something permanent went wrong.
            resp.raise_for_status()
        if resp.status_code >= 400:
            return await ctx.result("error", {"message": _error_message(resp)})

        try:
            body = resp.json()
        except Exception:
            body = None
        if not isinstance(body, list):
            return await ctx.result("error", {"message": "discord returned an unreadable body"})

        messages = []
        for entry in body:
            if not isinstance(entry, dict):
                continue
            author = entry.get("author")
            author = author if isinstance(author, dict) else {}
            messages.append({
                "id": str(entry.get("id")),
                "user": author.get("username"),
                "bot": bool(author.get("bot")),
                "content": entry.get("content"),
            })
        messages.reverse()          # Discord returns newest-first; read it forwards

        await ctx.result("ok", {"channel_id": channel_id,
                                "messages": messages,
                                "count": len(messages)})

    async def close(self):
        if self._sender is not None:
            await self._sender.close()
