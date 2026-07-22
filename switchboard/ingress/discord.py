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


import discord
from discord import app_commands


class DiscordIngress:
    """Ingress half of the Discord connector: a discord.py bot on the Gateway.
    Registers the configured slash commands; each interaction is deferred (acked
    within Discord's 3s window) and published as a thin command event. The real
    work is a downstream Switchboard handler. discord.py is transport + parsing
    only — no application logic lives here.
    """

    name = "discord"

    def __init__(self, bot_token: str, *,
                 commands: list[tuple[str, str]], guild_id: str | None = None):
        self._token = bot_token
        self._guild_id = guild_id
        self._publish = None
        self._synced = False

        self._client = discord.Client(intents=discord.Intents.none())
        self._tree = app_commands.CommandTree(self._client)
        for name, description in commands:
            self._tree.add_command(self._make_command(name, description))

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

    def _make_command(self, name: str, description: str) -> app_commands.Command:
        # The callback MUST take only `interaction` — discord.py inspects the
        # signature and treats any further parameter as a user-facing slash
        # option (requiring a type annotation). `name` is captured from this
        # method's scope, which is a fresh binding per command, so no closure
        # late-binding bug and no need for a default-arg trick.
        async def callback(interaction: discord.Interaction):
            await interaction.response.defer()             # ack within 3s
            options = {}
            for opt in (interaction.data or {}).get("options", []):
                options[opt.get("name")] = opt.get("value")
            await self._publish(build_command_event(
                command=name,
                interaction_id=interaction.id,
                token=interaction.token,
                channel_id=interaction.channel_id,
                guild_id=interaction.guild_id,
                user_id=interaction.user.id,
                user_name=str(interaction.user),
                options=options,
            ))
            # return — no work here; a downstream handler processes and replies

        return app_commands.Command(name=name, description=description, callback=callback)

    async def start(self, publish) -> None:
        self._publish = publish
        await self._client.start(self._token)             # runs the gateway loop

    async def stop(self) -> None:
        await self._client.close()
