import inspect
from dataclasses import dataclass

import discord
from discord import app_commands


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
                 commands: list[CommandSpec], guild_id: str | None = None):
        self._token = bot_token
        self._guild_id = guild_id
        self.ctx = None
        self._synced = False

        self._client = discord.Client(intents=discord.Intents.none())
        self._tree = app_commands.CommandTree(self._client)
        for spec in commands:
            self._tree.add_command(self._make_command(spec))

        @self._client.event
        async def on_ready():
            await self._on_ready()

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

    def bind(self, ctx) -> None:
        self.ctx = ctx          # no routes; any timer waits for the gateway

    async def start(self) -> None:
        await self._client.start(self._token)        # runs the gateway loop

    async def stop(self) -> None:
        await self._client.close()
