import inspect
import logging
from dataclasses import dataclass

import discord
from discord import app_commands

from switchboard.render import message_text

logger = logging.getLogger(__name__)


def _command_observation(command: str, interaction, options: dict) -> tuple[str, dict]:
    """Shape a slash-command interaction into a (name, payload) observation.
    Snowflake ids are stringified (msgpack round-trip); the interaction token +
    channel are the reply address a downstream decider/actuator uses."""
    return (f"discord.command.{command}", {
        "interaction_token": interaction.token,
        "channel_id": str(interaction.channel_id),
        "guild_id": str(interaction.guild_id),
        "user_id": str(interaction.user.id),
        "user_name": str(interaction.user),
        "options": dict(options),
    })


def _message_observation(message, bot_id: int, bot_role_ids=()) -> tuple[str, dict]:
    """Shape a gateway message into a `discord.message` observation.

    `channel_id` is always where the message *is* — the thread id when it is in
    a thread — so replying to it lands in place without the reader knowing which
    it got. The `thread` block is the hint from spec §6.5: a mid-thread mention
    gives the agent a conversation starting at the mention, and it cannot ask
    for context it does not know exists. `message_count` tells it there is more
    above; `discord.history` is how it reads it.

    `mentions_bot` is true for a direct user mention (`<@bot>`), a mention of
    any role the bot holds (`<@&role>`), or an `@everyone`/`@here` broadcast.
    The role case matters because a bot with a mentionable same-name role is
    very often pinged *by that role* — the user types `@switchboard` and Discord
    resolves it to the role, not the user — and discord.py keeps those in
    `role_mentions`, never in `mentions`. `@everyone`/`@here` come through
    neither list, only `mention_everyone`; the bot is part of everyone, so it
    wakes and the model decides whether a broadcast actually wants a reply.
    """
    channel = message.channel
    is_thread = isinstance(channel, discord.Thread)
    mention_ids = [str(u.id) for u in message.mentions]
    role_mention_ids = {str(r.id) for r in getattr(message, "role_mentions", [])}
    bot_roles = {str(r) for r in bot_role_ids}
    everyone = bool(getattr(message, "mention_everyone", False))
    # The specific ids in this message that refer to the bot — the renderer tags
    # (never strips) these so the model can see it was addressed and where,
    # while the raw <@id> survives so it can still mention itself.
    bot_mention_ids = ([str(bot_id)] if str(bot_id) in mention_ids else []) \
        + sorted(role_mention_ids & bot_roles)
    mentions_bot = bool(bot_mention_ids) or everyone
    return ("discord.message", {
        "message_id": str(message.id),
        "channel_id": str(channel.id),
        "parent_id": str(channel.parent_id) if is_thread else None,
        "thread_id": str(channel.id) if is_thread else None,
        "guild_id": str(message.guild.id) if message.guild else None,
        "user_id": str(message.author.id),
        "user_name": str(message.author),
        "content": message.content,
        "mentions": mention_ids,
        "mentions_bot": mentions_bot,
        "bot_mention_ids": bot_mention_ids,
        "mention_everyone": everyone,
        "thread": {"is_thread": is_thread,
                   "message_count": getattr(channel, "message_count", None) if is_thread else None},
    })


def _tag_bot_mentions(content: str, payload: dict) -> str:
    """Annotate — never remove — a mention of the bot. The model must know when
    and where it was addressed, but the raw <@id> stays so it can still mention
    itself and learn the id. The tag is a hint, not a trust boundary: the
    trusted signal is `mentions_bot` from the sensor."""
    for mid in payload.get("bot_mention_ids") or []:
        content = content.replace(f"<@{mid}>", f"<@{mid}> (you)")
        content = content.replace(f"<@&{mid}>", f"<@&{mid}> (you)")
    if payload.get("mention_everyone"):
        content = content.replace("@everyone", "@everyone (you are included)")
        content = content.replace("@here", "@here (you are included)")
    return content


def render_message(payload: dict) -> str:
    """A discord.message payload as text. The sensor owns this
    because it owns the payload shape — and DiscordHistory calls the same
    function, which is what makes history and live identical by construction."""
    payload = payload if isinstance(payload, dict) else {}
    thread = payload.get("thread")
    thread = thread if isinstance(thread, dict) else {}

    fields = {"channel_id": payload.get("channel_id"),
              "message_id": payload.get("message_id"),
              "user": payload.get("user_name"),
              "user_id": payload.get("user_id")}
    if payload.get("thread_id"):
        fields["thread_id"] = payload.get("thread_id")
        count = thread.get("message_count")
        if isinstance(count, int):
            fields["thread_messages"] = count

    body = _tag_bot_mentions(
        payload.get("content") if isinstance(payload.get("content"), str) else "",
        payload)
    return message_text("discord.message", fields, body)


@dataclass(frozen=True)
class Option:
    """A declared slash-command parameter. `type` is a plain Python type
    (str -> STRING, int -> INTEGER, bool -> BOOLEAN, float -> NUMBER); Discord
    validates the value before the interaction ever reaches us."""
    name: str
    description: str
    type: type = str
    required: bool = True


@dataclass(frozen=True)
class CommandSpec:
    name: str
    description: str
    options: tuple[Option, ...] = ()


class DiscordSensor:
    """Sensor half of the Discord connector: a discord.py bot on the Gateway.
    Registers the configured slash commands (with their declared, typed options);
    each interaction is deferred (acked within Discord's 3s window) and emitted
    as a thin command observation. The real work is a downstream Switchboard handler.
    discord.py is transport + parsing only — no application logic lives here.
    """

    name = "discord"

    def __init__(self, bot_token: str, *,
                 commands: list[CommandSpec], guild_id: str | None = None,
                 dedup_ttl: float = 7 * 86_400.0):
        self._token = bot_token
        self._guild_id = guild_id
        self._dedup_ttl = dedup_ttl
        self.ctx = None
        self._synced = False

        # message_content is a *privileged* intent, so this is a hard
        # deployment requirement rather than a preference: it must be enabled
        # in the Developer Portal or the gateway refuses login outright and
        # this sensor dies at start. It was briefly behind a flag while the
        # message path was unproven; now that the agent depends on it, a
        # Switchboard that cannot hear messages is not a useful one, and a
        # flag would only turn a loud startup failure into a silent
        # never-responds.
        intents = discord.Intents.none()
        intents.guilds = True
        intents.guild_messages = True
        intents.message_content = True

        self._client = discord.Client(intents=intents)
        self._tree = app_commands.CommandTree(self._client)
        for spec in commands:
            self._tree.add_command(self._make_command(spec))

        @self._client.event
        async def on_ready():
            await self._on_ready()

        @self._client.event
        async def on_message(message):
            await self._on_message(message, bot_id=self._client.user.id)

    async def _on_ready(self) -> None:
        # Any timer this sensor grows is declared here, not in bind(): it would
        # call the Discord API and must not tick before the gateway is up. Guard
        # such a declaration on first connect — on_ready refires on every
        # reconnect and `every` is not idempotent.
        await self._sync_commands()

    async def _sync_commands(self) -> None:
        # on_ready can refire on every (re)connect, so sync only once. The guard
        # is set AFTER a successful sync, not before: a transient failure (e.g.
        # the app not yet authorized in the guild -> 403 Missing Access) must be
        # retried on the next reconnect rather than latched off for the life of
        # the process.
        if self._synced:
            return
        if self._guild_id:
            guild = discord.Object(id=int(self._guild_id))
            self._tree.copy_global_to(guild=guild)        # instant per-guild in dev
            await self._tree.sync(guild=guild)
        else:
            await self._tree.sync()                        # global (~1h propagation)
        self._synced = True

    def _make_command(self, spec: CommandSpec) -> app_commands.Command:
        # discord.py derives a command's options from its callback's *typed
        # signature*. To keep commands data-driven (a list of specs, not a
        # hand-written function each), we build that signature dynamically from
        # `spec.options` and stamp it onto a generic `**kwargs` callback:
        #   - __signature__ declares (interaction, <opt>: <type> [= default]) so
        #     discord.py emits the correct option JSON on sync and validates input;
        #   - at invoke time discord.py passes each validated option as a keyword
        #     arg, which `**kwargs` captures and we forward verbatim as `options`.
        # The callback stays thin: defer -> emit -> return.
        async def callback(interaction: discord.Interaction, **kwargs):
            await interaction.response.defer()             # ack within 3s
            name, payload = _command_observation(spec.name, interaction, kwargs)
            await self.ctx.emit(name, payload)
            # return — no work here; a downstream handler processes and replies

        params = [inspect.Parameter("interaction",
                                    inspect.Parameter.POSITIONAL_OR_KEYWORD,
                                    annotation=discord.Interaction)]
        annotations = {"interaction": discord.Interaction}
        for opt in spec.options:
            default = inspect.Parameter.empty if opt.required else None
            params.append(inspect.Parameter(
                opt.name, inspect.Parameter.KEYWORD_ONLY,
                annotation=opt.type, default=default))
            annotations[opt.name] = opt.type
        callback.__signature__ = inspect.Signature(params)
        callback.__annotations__ = annotations
        if spec.options:
            app_commands.describe(**{o.name: o.description for o in spec.options})(callback)

        return app_commands.Command(
            name=spec.name, description=spec.description, callback=callback)

    async def _on_message(self, message, *, bot_id: int) -> None:
        # Every bot is ignored, ourselves included. Ignoring only ourselves would
        # let two Switchboard-shaped bots talk each other into an endless loop,
        # and the failure would be a live spend, not a test failure.
        if message.author.bot:
            return

        # Discord replays gateway events on RESUME; the Bus's dedup keys on the
        # *log's* message id and cannot catch a re-emitted gateway event, so we
        # dedupe on the gateway's own id the same way GitHubSensor dedupes on
        # X-GitHub-Delivery: via ctx.store, with a TTL.
        key = f"discord:message:{message.id}"
        if await self.ctx.store.get(key) is not None:
            return

        # The bot's roles in this guild, minus @everyone (the default role, which
        # every member holds — treating it as "the bot's role" would make every
        # @everyone ping wake the agent). `guild.me` is the bot's own member and
        # needs no privileged intent to read.
        me = message.guild.me if message.guild else None
        bot_role_ids = [r.id for r in me.roles if not r.is_default()] if me else []

        name, payload = _message_observation(message, bot_id, bot_role_ids)
        # Emit first, record second: a crash (or a failed emit) in between costs
        # a duplicate, the reverse order costs the event.
        try:
            await self.ctx.emit(name, payload, text=render_message(payload))
        except Exception:
            # Nothing upstream can retry a gateway event: discord.py's dispatcher
            # would otherwise swallow this into its default on_error (a bare
            # traceback to stderr). Log it ourselves with the message id so a
            # dropped message leaves a greppable, alertable trace, and do not
            # mark it seen — a redelivered copy on the next RESUME gets another
            # chance to succeed instead of being silently discarded forever.
            logger.error("failed to emit discord.message for message_id=%s",
                         message.id, exc_info=True)
            return
        await self.ctx.store.set(key, "1", ttl=self._dedup_ttl)

    def bind(self, ctx) -> None:
        self.ctx = ctx          # no routes; any timer waits for the gateway

    async def start(self) -> None:
        await self._client.start(self._token)        # runs the gateway loop

    async def stop(self) -> None:
        await self._client.close()
