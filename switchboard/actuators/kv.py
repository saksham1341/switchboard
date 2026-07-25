"""The agent's memory, as an actuator.

One actuator with one scope, dispatching on `op`. Two actuators (kv.get and
kv.set) would each get their own `actuator/<name>/` scope and could never see
the other's writes — a memory that cannot remember.

It carries no tool_spec: the agent reaches this only through the memory tools
its decider injects, which prefix keys per session. It never addresses kv
directly.
"""

# The model-facing ops: what the two memory tools advertise in their schema.
# `delete_prefix` is deliberately NOT here. It empties a whole namespace, and a
# tool enum is a list of things a model can hallucinate its way into calling.
# The drain is the decider's to emit when it ends a session, never the model's.
OPS = ("get", "set", "delete", "list")
DELETE_PREFIX = "delete_prefix"
_ALL_OPS = OPS + (DELETE_PREFIX,)

LIST_MAX = 200          # an agent listing a large memory would blow its own context
# Bounded work per drain command, the same policy shape as LIST_MAX and for the
# same reason: the store is a primitive that will delete whatever it is told to,
# so the ceiling belongs to the actuator. A truncated drain is REPORTED rather
# than silently partial; nothing re-drains the remainder (the drain's result has
# no pending entry — see AgentDecider._end_session), so this is the one place
# the leak would start, and it is why the cap is generous rather than tight.
DELETE_PREFIX_MAX = 1000


class KvActuator:
    name = "kv"
    tool_spec = None          # not agent-callable; reached via decider-injected tools

    def bind(self, ctx) -> None:
        self.ctx = ctx

    async def act(self, cmd, ctx) -> None:
        args = cmd.args or {}
        op, key = args.get("op"), args.get("key")

        if op not in _ALL_OPS:
            return await ctx.result("error", {"message": f"unknown op: {op!r}"})

        if op == DELETE_PREFIX:
            prefix = args.get("prefix")
            # Three separate rejections, not one truthiness check, because they
            # fail differently and only one of them is merely untidy:
            #  - a non-string (including a MISSING prefix, which arrives as
            #    None) would raise out of the store's _check_key and burn the
            #    retry cycle before anyone learned why;
            #  - "" is the dangerous one: every key starts with the empty
            #    string, so an empty prefix drains the ENTIRE store. There is
            #    no legitimate caller for that, so it is rejected rather than
            #    honoured.
            #  - whitespace-only is "" that got past a caller's own strip: it
            #    can only ever match keys that begin with a space, so it is a
            #    bug at the call site every time, and answering it silently
            #    with removed=0 would hide that.
            if not isinstance(prefix, str):
                return await ctx.result("error", {"message": "prefix must be a string"})
            if not prefix.strip():
                return await ctx.result(
                    "error", {"message": "prefix must not be empty"})
            found = await self.ctx.store.keys(prefix)
            doomed = found[:DELETE_PREFIX_MAX]
            for k in doomed:
                await self.ctx.store.delete(k)
            # Deleting a key that is already gone is a no-op in every backend,
            # so a redelivered drain reports removed=0 and succeeds — which is
            # what at-least-once delivery requires of it.
            return await ctx.result("ok", {"removed": len(doomed),
                                           "truncated": len(found) > DELETE_PREFIX_MAX})

        if op == "list":
            prefix = args.get("prefix") or ""
            if not isinstance(prefix, str):
                return await ctx.result("error", {"message": "prefix must be a string"})
            found = await self.ctx.store.keys(prefix)
            # Capped here, not in the store: the store is a primitive, the
            # actuator carries the policy that protects the caller.
            return await ctx.result("ok", {"keys": found[:LIST_MAX],
                                           "truncated": len(found) > LIST_MAX})

        if not isinstance(key, str):
            return await ctx.result("error", {"message": "key must be a string"})

        if op == "get":
            return await ctx.result("ok", {"value": await self.ctx.store.get(key)})

        if op == "delete":
            await self.ctx.store.delete(key)
            return await ctx.result("ok", {})

        value = args.get("value")
        if not isinstance(value, str):
            # Reported, not raised: the actuator understands this failure, and
            # raising would burn the retry cycle before the caller learned of it.
            return await ctx.result("error", {"message": "value must be a string"})
        await self.ctx.store.set(key, value, ttl=args.get("ttl"))
        await ctx.result("ok", {})
