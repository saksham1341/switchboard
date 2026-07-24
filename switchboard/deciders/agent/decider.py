"""The agent decider: a flat event handler, not a call stack.

Nothing here awaits a tool. `_advance` emits an `llm` command and returns; the
model's answer arrives later as a fresh observation and re-enters `decide()`.
That is what keeps this a decider like any other — deterministic, replayable,
no world access — even though the judgment inside the loop is none of those.
"""
import json
import logging

from switchboard.deciders.agent.prompt import SYSTEM
from switchboard.deciders.agent.render import render_message
from switchboard.deciders.agent.session import Sessions
from switchboard.message import CMD_LOG

logger = logging.getLogger(__name__)

MAX_TURNS = 12


def _tool_outcome(obs) -> tuple[str, bool]:
    """A result observation -> (tool_result content, is_error).

    Convention (spec 7.2): the ok payload IS the tool content, json-serialized.
    An error carries {"message": ...}. Shape-defensive throughout — a surprising
    payload must degrade to text, never raise.
    """
    payload = obs.payload if isinstance(obs.payload, dict) else {}
    if obs.name.endswith(".error"):
        message = payload.get("message")
        if isinstance(message, str):
            return message, True
        try:
            return json.dumps(payload), True
        except (TypeError, ValueError):
            return str(payload), True
    try:
        return json.dumps(payload), False
    except (TypeError, ValueError):
        return str(payload), False


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

        if obs.name == "switchboard.deadletter":
            # A dead command emits no result. The sensor is the only signal and
            # it carries no command_id — a sensor cannot forge a result — so we
            # correlate from the payload instead.
            payload = obs.payload if isinstance(obs.payload, dict) else {}
            # obs and cmd are separate logs with independent id sequences (see
            # message.py). A message_id is only meaningful within its own log,
            # so a deadletter for the obs log must never be matched against our
            # cmd-log pending table — an obs id can collide with a live cmd id
            # and forge a death for a healthy tool.
            if payload.get("log") != CMD_LOG:
                return
            mid = payload.get("message_id")
            if mid is None:
                return
            p = await self._sessions.take_pending(mid)
            if p is None:
                return
            s = await self._sessions.load(p["sid"])
            if s is None:
                return
            if p["kind"] == "llm":
                # A dead-lettered llm command recovers exactly like an
                # llm.error result would: log it and return to idle rather
                # than leaving the session stuck busy forever.
                logger.warning("llm command dead-lettered for session %s", s["sid"])
                return await self._finish(s, ctx)
            if p["kind"] != "tool":
                return
            return await self._on_gather(s, p, "the tool died", True, ctx)

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
            return await self._on_response(s, obs, ctx)
        content, is_error = _tool_outcome(obs)
        await self._on_gather(s, p, content, is_error, ctx)

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

        # At-least-once delivery (see bus.py's _consume, which marks a message
        # processed only after the handler returns) means this same
        # observation can arrive again after a retry. Keying each buffer
        # entry on the source message id and skipping a repeat keeps the
        # append idempotent instead of duplicating content into one turn.
        message_id = payload.get("message_id")
        already_buffered = message_id is not None and any(
            b.get("message_id") == message_id for b in s["buffer"])
        if not already_buffered:
            s["buffer"].append({"rendered": render_message(payload),
                                "is_mention": is_mention,
                                "message_id": message_id})
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
            messages = s["messages"]
            # A mention arriving mid-gather is buffered, then the tool_result
            # turn that closes the gather is appended as its own user message
            # before _advance ever runs — so by the time we get here the last
            # message can already be a user turn. Anthropic's Messages API
            # rejects two consecutive user turns, so merge into it instead of
            # appending a second one. tool_result blocks must lead a user
            # turn's content, so appending a trailing text block is correct.
            if messages and messages[-1]["role"] == "user":
                last = messages[-1]
                content = last["content"]
                if isinstance(content, str):
                    content = [{"type": "text", "text": content}]
                content.append({"type": "text", "text": combined})
                last["content"] = content
            else:
                messages.append({"role": "user", "content": combined})
            s["buffer"] = []

        args = {"system": self._system, "messages": s["messages"],
                "tools": self._tools}
        if self._model:
            args["model"] = self._model
        cid = await ctx.command("llm", args)
        # The store has no transactions, so these two writes can never be made
        # atomic — a crash between them is possible either way. We choose the
        # order that fails safe: save the session (turn incremented, buffer
        # flushed, state busy) BEFORE recording the pending entry. If we crash
        # after save() but before put_pending(), the session is stuck "busy"
        # with no pending record — recoverable, and exactly what the Phase 5
        # stuck-busy watchdog exists to catch. The reverse order (pending
        # before save) risks a stale, un-flushed session receiving an assistant
        # reply with no matching user turn: a structurally invalid transcript
        # that no watchdog can repair.
        s["state"] = "busy"
        await self._sessions.save(s)
        await self._sessions.put_pending(cid, {"kind": "llm", "sid": s["sid"]})

    async def _halt(self, s, why: str, ctx) -> None:
        """Stop without emitting another llm. Placeholder until Phase 5 gives
        halt a user-visible message; for now it idles and logs."""
        logger.warning("session %s halted: %s", s["sid"], why)
        s["state"] = "idle"
        await self._sessions.save(s)

    async def _on_response(self, s, obs, ctx) -> None:
        """The model spoke."""
        if obs.name.endswith(".error"):
            # The call itself failed. Do not retry here: the Bus already retried
            # what was retryable, and looping on a hard failure burns spend.
            payload = obs.payload if isinstance(obs.payload, dict) else {}
            logger.warning("llm call failed for session %s: %s",
                           s["sid"], payload.get("message"))
            return await self._finish(s, ctx)

        payload = obs.payload if isinstance(obs.payload, dict) else {}
        blocks = payload.get("content")
        blocks = blocks if isinstance(blocks, list) else []
        s["messages"].append({"role": "assistant", "content": blocks})

        uses = [b for b in blocks
                if isinstance(b, dict) and b.get("type") == "tool_use"]
        if not uses:
            # end_turn. The decider is text-blind by design: it delivers
            # nothing. A reply reaches the user only via a reply tool.
            return await self._finish(s, ctx)

        known = {t["name"] for t in self._tools}
        # Only one gather is ever open per session: a session is "busy" from
        # advance until finish and emits one llm at a time, so the session
        # record itself is the gather key. The spec's turn_key is unnecessary
        # here. (No "remaining" counter: the real barrier is comparing
        # len(results) to len(order) in _maybe_close_gather.)
        gather = {"order": [], "results": {}}
        immediate = []
        for b in uses:
            tid = b.get("id")
            # A duplicate tool_use id must not fan out to a second command:
            # _record_result is keyed on tool_use_id, so two commands sharing
            # one id would leave `results` permanently short of `order` and
            # the session stuck busy forever. Skip it entirely here, before
            # any command is emitted.
            if not isinstance(tid, str) or tid in gather["order"]:
                continue
            gather["order"].append(tid)
            name = b.get("name")
            # A non-string name (list/dict/etc) would raise on `in known` —
            # guard with isinstance before the hashable-membership check and
            # treat it the same as an unknown tool.
            if isinstance(name, str) and name in known:
                cid = await ctx.command(name,
                                        b.get("input") if isinstance(b.get("input"), dict) else {})
                await self._sessions.put_pending(
                    cid, {"kind": "tool", "sid": s["sid"], "tool_use_id": tid})
            else:
                # Hallucinated tool (spec 8): answered immediately, in-band, so
                # the model learns from a tool_result rather than from silence.
                immediate.append((tid, f"no such tool: {name!r}"))

        # Nothing to wait for (every block had a non-string id) ⇒ gather stays
        # None, preserving the "gather is None when closed" invariant instead
        # of saving a dead, non-None empty dict that _finish never clears.
        s["gather"] = gather if gather["order"] else None
        await self._sessions.save(s)
        for tid, message in immediate:
            await self._record_result(s, tid, message, True)
        if immediate:
            await self._maybe_close_gather(s, ctx)
        if not gather["order"]:
            # Every block was unusable; nothing will ever arrive.
            await self._finish(s, ctx)

    async def _record_result(self, s, tool_use_id, content, is_error) -> None:
        gather = s["gather"]
        if gather is None or tool_use_id in gather["results"]:
            return
        gather["results"][tool_use_id] = {"type": "tool_result",
                                          "tool_use_id": tool_use_id,
                                          "content": content,
                                          "is_error": is_error}

    async def _on_gather(self, s, p, content, is_error, ctx) -> None:
        """A tool finished."""
        await self._record_result(s, p["tool_use_id"], content, is_error)
        await self._maybe_close_gather(s, ctx)

    async def _maybe_close_gather(self, s, ctx) -> None:
        gather = s["gather"]
        if gather is None:
            return
        if len(gather["results"]) < len(gather["order"]):
            return await self._sessions.save(s)
        # Anthropic requires one tool_result for every tool_use, together and in
        # the model's original order.
        s["messages"].append({"role": "user",
                              "content": [gather["results"][t] for t in gather["order"]]})
        s["gather"] = None
        await self._advance(s, ctx)

    async def _finish(self, s, ctx) -> None:
        if any(b["is_mention"] for b in s["buffer"]):
            return await self._advance(s, ctx)      # a mention landed while busy
        s["state"] = "idle"                          # keep non-mention context buffered
        await self._sessions.save(s)
