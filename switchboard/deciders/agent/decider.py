"""The agent decider: a flat event handler, not a call stack.

Nothing here awaits a tool. `_advance` emits an `llm` command and returns; the
model's answer arrives later as a fresh observation and re-enters `decide()`.
That is what keeps this a decider like any other — deterministic, replayable,
no world access — even though the judgment inside the loop is none of those.
"""
import logging

from switchboard.deciders.agent.prompt import SYSTEM
from switchboard.deciders.agent.render import render_message
from switchboard.deciders.agent.session import Sessions

logger = logging.getLogger(__name__)

MAX_TURNS = 12


class AgentDecider:
    name = "agent"

    def __init__(self, *, tools, system: str | None = None,
                 max_turns: int = MAX_TURNS, model: str | None = None):
        self._tools = list(tools)
        self._system = system if system is not None else SYSTEM
        self._max_turns = max_turns
        self._model = model

    def bind(self, ctx) -> None:
        self.ctx = ctx
        self._sessions = Sessions(ctx.store)

    def subscribes(self, obs) -> bool:
        # Coarse and synchronous — it cannot reach the store, so it cannot know
        # whether a command_id is ours. decide() makes that call by finding (or
        # not finding) a pending entry.
        return (obs.name == "discord.message"
                or obs.name == "switchboard.deadletter"
                or obs.command_id is not None)

    # --- dispatch --------------------------------------------------------

    async def decide(self, obs, ctx) -> None:
        if obs.name == "discord.message":
            return await self._on_message(obs, ctx)
        if obs.command_id is None:
            return
        p = await self._sessions.take_pending(obs.command_id)
        if p is None:
            return                      # not ours, or already handled
        s = await self._sessions.load(p["sid"])
        if s is None:
            logger.warning("result for a session that no longer exists: %s", p["sid"])
            return
        if p["kind"] == "llm":
            await self._on_response(s, obs, ctx)

    # --- input -----------------------------------------------------------

    async def _on_message(self, obs, ctx) -> None:
        payload = obs.payload if isinstance(obs.payload, dict) else {}
        key = payload.get("thread_id") or payload.get("channel_id")
        if not key:
            return
        sid = await self._sessions.route("discord", str(key))
        is_mention = bool(payload.get("mentions_bot"))

        if sid is None:
            if not is_mention:
                return                  # no session and not addressed: not ours
            s = await self._sessions.new(
                sid=obs.id, source="discord",
                channel_id=str(payload.get("channel_id") or key),
                thread_id=payload.get("thread_id"),
                anchor=str(payload.get("message_id") or ""))
            await self._sessions.set_route("discord", str(key), obs.id)
        else:
            s = await self._sessions.load(sid)
            if s is None:
                return

        s["buffer"].append({"rendered": render_message(payload),
                            "is_mention": is_mention})
        if is_mention and s["state"] == "idle":
            await self._advance(s, ctx)
        else:
            # Buffered: either context for a turn not yet taken, or a mention
            # that landed mid-turn. `finish` drains it either way.
            await self._sessions.save(s)

    # --- the turn --------------------------------------------------------

    async def _advance(self, s, ctx) -> None:
        """The sole way a session takes a turn, and therefore the sole gate."""
        if s["turn"] >= self._max_turns:
            return await self._halt(s, "turn limit reached", ctx)
        s["turn"] += 1

        if s["buffer"]:
            combined = "\n\n".join(b["rendered"] for b in s["buffer"])
            s["messages"].append({"role": "user", "content": combined})
            s["buffer"] = []

        args = {"system": self._system, "messages": s["messages"],
                "tools": self._tools}
        if self._model:
            args["model"] = self._model
        cid = await ctx.command("llm", args)
        await self._sessions.put_pending(cid, {"kind": "llm", "sid": s["sid"]})
        s["state"] = "busy"
        await self._sessions.save(s)

    async def _halt(self, s, why: str, ctx) -> None:
        """Stop without emitting another llm. Placeholder until Phase 5 gives
        halt a user-visible message; for now it idles and logs."""
        logger.warning("session %s halted: %s", s["sid"], why)
        s["state"] = "idle"
        await self._sessions.save(s)

    async def _on_response(self, s, obs, ctx) -> None:
        raise NotImplementedError          # Task 4
