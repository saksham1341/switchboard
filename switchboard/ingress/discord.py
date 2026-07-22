from switchboard.event import EventInput


def build_command_event(*, command, interaction_id, token,
                        channel_id, guild_id, user_id, user_name, options) -> EventInput:
    """Translate a Discord slash-command interaction into a Switchboard event.

    Ids are stringified because mamamia round-trips payloads through msgpack and
    Discord ids are 64-bit snowflakes. `meta` carries the reply address
    (interaction token + channel) so a downstream handler can reply via the
    egress, even after a restart, within Discord's 15-min window. The bot's
    application id is deployment config on the egress sender, not per-event.
    """
    return EventInput(
        kind=f"discord.{guild_id}.command.{command}",
        source="discord",
        payload={
            "command": command,
            "options": options,
            "user": {"id": str(user_id), "name": user_name},
            "channel_id": str(channel_id),
            "guild_id": str(guild_id),
        },
        dedupe_key=str(interaction_id),
        meta={
            "interaction_token": token,
            "channel_id": str(channel_id),
        },
    )


import inspect
from dataclasses import dataclass

import discord
from discord import app_commands


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
class Command:
    name: str
    description: str
    options: tuple[Option, ...] = ()


class DiscordIngress:
    """Ingress half of the Discord connector: a discord.py bot on the Gateway.
    Registers the configured slash commands (with their declared, typed options);
    each interaction is deferred (acked within Discord's 3s window) and published
    as a thin command event. The real work is a downstream Switchboard handler.
    discord.py is transport + parsing only — no application logic lives here.
    """

    name = "discord"

    def __init__(self, bot_token: str, *,
                 commands: list[Command], guild_id: str | None = None):
        self._token = bot_token
        self._guild_id = guild_id
        self._publish = None
        self._synced = False

        self._client = discord.Client(intents=discord.Intents.none())
        self._tree = app_commands.CommandTree(self._client)
        for spec in commands:
            self._tree.add_command(self._make_command(spec))

        @self._client.event
        async def on_ready():
            if self._synced:                              # on_ready can refire on reconnect
                return
            self._synced = True
            if self._guild_id:
                guild = discord.Object(id=int(self._guild_id))
                self._tree.copy_global_to(guild=guild)    # instant per-guild in dev
                await self._tree.sync(guild=guild)
            else:
                await self._tree.sync()                    # global (~1h propagation)

    def _make_command(self, spec: Command) -> app_commands.Command:
        # discord.py derives a command's options from its callback's *typed
        # signature*. To keep commands data-driven (a list of specs, not a
        # hand-written function each), we build that signature dynamically from
        # `spec.options` and stamp it onto a generic `**kwargs` callback:
        #   - __signature__ declares (interaction, <opt>: <type> [= default]) so
        #     discord.py emits the correct option JSON on sync and validates input;
        #   - at invoke time discord.py passes each validated option as a keyword
        #     arg, which `**kwargs` captures and we forward verbatim as `options`.
        # The callback stays thin: defer -> publish -> return.
        async def callback(interaction: discord.Interaction, **kwargs):
            await interaction.response.defer()             # ack within 3s
            await self._publish(build_command_event(
                command=spec.name,
                interaction_id=interaction.id,
                token=interaction.token,
                channel_id=interaction.channel_id,
                guild_id=interaction.guild_id,
                user_id=interaction.user.id,
                user_name=str(interaction.user),
                options=dict(kwargs),                      # only the options Discord sent
            ))
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

    async def start(self, publish) -> None:
        self._publish = publish
        await self._client.start(self._token)             # runs the gateway loop

    async def stop(self) -> None:
        await self._client.close()
