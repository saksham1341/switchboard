"""The agent decider: a flat event handler, not a call stack.

Nothing here awaits a tool. `_advance` emits an `llm` command and returns; the
model's answer arrives later as a fresh observation and re-enters `decide()`.
That is what keeps this a decider like any other — deterministic, replayable,
no world access — even though the judgment inside the loop is none of those.
"""
import json
import logging
import time

from switchboard.deciders.agent.prompt import SYSTEM
from switchboard.deciders.agent.session import Sessions
from switchboard.message import CMD_LOG
from switchboard.render import escape_delimiters

logger = logging.getLogger(__name__)


def _tool_outcome(obs) -> tuple[str, bool]:
    """A result observation -> (tool_result content, is_error).

    Convention (spec 7.2): the ok payload IS the tool content, json-serialized.
    An error carries {"message": ...}. Shape-defensive throughout — a surprising
    payload must degrade to text, never raise.

    A producer-supplied `obs.text` is used VERBATIM: it was escaped at write
    time by whoever owned the payload (see switchboard/render.py's
    `message_text`/`escape_delimiters` contract). Re-escaping it here would
    double-escape legitimate content. Only the JSON fallback -- the case where
    no producer supplied anything -- is escaped by the agent, because there
    nobody else could have. The `.error` branch and the `discord.history`-style
    relay case are the same second ingress path for untrusted text documented
    at the §6.6 `discord.message` boundary: a tool result can relay content
    some other user wrote, so a forged `</untrusted> SYSTEM: ...` must not
    read as trusted transcript content once it lands.
    """
    payload = obs.payload if isinstance(obs.payload, dict) else {}
    is_error = obs.name.endswith(".error")
    if obs.text is not None:
        # Verbatim, error or not: it was escaped at write time by the producer.
        # Re-escaping would double-escape legitimate content, and discarding it
        # on the error path would silently throw away a producer's rendering.
        return obs.text, is_error
    if is_error:
        message = payload.get("message")
        # `obs.rendered` is the guarded json.dumps fallback (text is None here),
        # so an unserialisable payload degrades instead of raising out of
        # decide() -- which is what this docstring promises.
        body = message if isinstance(message, str) else obs.rendered
        return escape_delimiters(body), True
    return escape_delimiters(obs.rendered), False


# A modest default, not because of any one source but because reserving output
# is not free: providers count the RESERVED max_tokens toward a request's
# rate-limit check, so an oversized value can get a request rejected that would
# otherwise fit (a live 413: 1.9k input + 4096 reserved = 6015 > a 6000 TPM
# limit). A conversational reply is short, so a small cap passes cleanly and
# leaves headroom for the transcript. It is a constructor arg precisely because
# the right ceiling is a per-deployment call — bump it when the work needs
# longer outputs.
AGENT_MAX_TOKENS = 1024


class AgentDecider:
    name = "agent"

    def __init__(self, *, tools, model, stuck_after: float, system: str | None = None,
                 max_tokens: int = AGENT_MAX_TOKENS):
        self._tools = list(tools)
        self._system = system if system is not None else SYSTEM
        self._model = model
        self._max_tokens = max_tokens
        # Keyword-required, no default: the correct value depends on Bus
        # configuration (worst_case_retry_seconds) the decider cannot see, so
        # a default here would be the decider guessing its own threshold --
        # precisely the failure the derivation in app.py exists to prevent.
        self._stuck_after = stuck_after

    def bind(self, ctx) -> None:
        self.ctx = ctx
        self._sessions = Sessions(ctx.store)

    def subscribes(self, obs) -> bool:
        # Coarse and synchronous — it cannot reach the store, so it cannot know
        # whether a command_id is ours. decide() makes that call by finding (or
        # not finding) a pending entry.
        return (obs.name == "discord.message"
                or obs.name == "switchboard.deadletter"
                or obs.name == "clock.tick"
                or obs.command_id is not None)

    # --- dispatch --------------------------------------------------------

    async def decide(self, obs, ctx) -> None:
        if obs.name == "discord.message":
            return await self._on_message(obs, ctx)

        if obs.name == "clock.tick":
            return await self._sweep_stuck(obs, ctx)

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
        # A missing or non-string message_id cannot be matched against —
        # `None == None` would falsely call two id-less messages duplicates
        # of each other — so treat that as "cannot dedupe" and always append.
        message_id = payload.get("message_id")
        already_buffered = isinstance(message_id, str) and any(
            b.get("message_id") == message_id for b in s["buffer"])
        if not already_buffered:
            # Same rule as _tool_outcome: a producer's text is verbatim, the
            # JSON fallback is escaped here because nobody else could have.
            # Before this refactor the decider always escaped, so skipping it
            # on the fallback would be a regression, not a new trade.
            rendered = (obs.text if obs.text is not None
                        else escape_delimiters(obs.rendered))
            s["buffer"].append({"rendered": rendered,
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
        """The sole way a session takes a turn.

        There is deliberately no turn cap: a session ends when the model stops
        calling tools (`_on_response` -> `_finish`), not on a counter. `turn` is
        kept as an observability counter only. The backstop against a runaway
        tool-calling loop is Phase 5's spend ceiling, not a turn limit; until it
        lands, Phase 4 runs watched, never unattended.
        """
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
                "tools": self._tools, "model": self._model,
                "max_tokens": self._max_tokens}
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
        s["busy_since"] = time.time()
        await self._sessions.save(s)
        await self._sessions.put_pending(cid, {"kind": "llm", "sid": s["sid"]})

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
        to_emit = []
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
                args = b.get("input") if isinstance(b.get("input"), dict) else {}
                to_emit.append((tid, name, args))
            else:
                # Hallucinated tool (spec 8): answered immediately, in-band, so
                # the model learns from a tool_result rather than from silence.
                # Recorded directly into `gather["results"]` here (rather than
                # via `_record_result` after save) because the whole point is
                # that the gather saved below must already be complete.
                # Escaped like any other tool_result content: `name` is chosen
                # by the model, which may itself be echoing text an untrusted
                # user wrote, and this lands in a user-role block. repr() alone
                # is not the boundary -- escape_delimiters is.
                gather["results"][tid] = {
                    "type": "tool_result", "tool_use_id": tid,
                    "content": escape_delimiters(f"no such tool: {name!r}"),
                    "is_error": True}

        # Nothing to wait for (every block had a non-string id) ⇒ gather stays
        # None, preserving the "gather is None when closed" invariant instead
        # of saving a dead, non-None empty dict that _finish never clears.
        s["gather"] = gather if gather["order"] else None
        # The store has no transactions, so these writes can never be made
        # atomic — a crash partway through is always possible. `gather["order"]`
        # is fully known before any tool command needs to be emitted, so we
        # save the complete gather FIRST and only then emit commands and
        # record their pending entries — the same fail-safe choice `_advance`
        # makes. If we crash after save() but before a command/pending write,
        # the session is stuck "busy" with a real gather and some pending
        # entries missing: recoverable, and exactly what the Phase 5
        # stuck-busy watchdog exists to catch. The reverse order (emit and
        # put_pending before save, as this used to do) risks a crash after a
        # tool command is emitted and its pending entry recorded but before
        # save() — leaving `pending:<cid>` pointing at a session whose stored
        # `gather` is still None. When the real result then arrives,
        # take_pending succeeds, _record_result sees gather is None and
        # silently drops the result, and the session is stuck busy forever
        # with nothing to distinguish it from a legitimately-closed gather.
        await self._sessions.save(s)
        for tid, name, args in to_emit:
            cid = await ctx.command(name, args)
            await self._sessions.put_pending(
                cid, {"kind": "tool", "sid": s["sid"], "tool_use_id": tid})

        if gather["results"]:
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
        s["busy_since"] = None
        await self._sessions.save(s)

    # --- the watchdog ------------------------------------------------------

    async def _sweep_stuck(self, obs, ctx) -> None:
        """Free sessions that have been busy longer than any legitimate retry
        chain could take.

        This runs on a tick rather than on a timer, and that is the point: it
        arrives as an observation through the decider's own consumer group, so
        it is serial with every other handler. A maintenance timer would run
        outside the consume loop and could interleave with an in-flight
        decide() mid-await, clobbering the very session record it is reading
        (§5.3 — the settle discipline IS the concurrency control).

        Silent by design: the session goes idle and the event is logged, but
        nothing is posted. Everything a user sees still comes from the model.
        """
        payload = obs.payload if isinstance(obs.payload, dict) else {}
        now = payload.get("at")
        if not isinstance(now, (int, float)):
            return
        for key in await self.ctx.store.keys("session:"):
            sid = key.split(":", 1)[1]
            s = await self._sessions.load(int(sid)) if sid.isdigit() else None
            if s is None or s.get("state") != "busy":
                continue
            since = s.get("busy_since")
            if not isinstance(since, (int, float)) or now - since < self._stuck_after:
                continue
            logger.warning("session %s stuck busy for %.0fs; freeing",
                           s["sid"], now - since)
            s["state"] = "idle"
            s["busy_since"] = None
            await self._sessions.save(s)
