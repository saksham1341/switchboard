"""The agent's memory, as an actuator.

One actuator with one scope, dispatching on `op`. Two actuators (kv.get and
kv.set) would each get their own `actuator/<name>/` scope and could never see
the other's writes — a memory that cannot remember.

It carries no tool_spec: the agent reaches this only through the memory tools
its decider injects, which prefix keys per session. It never addresses kv
directly.
"""

OPS = ("get", "set", "delete", "list")
LIST_MAX = 200          # an agent listing a large memory would blow its own context


class KvActuator:
    name = "kv"
    tool_spec = None          # not agent-callable; reached via decider-injected tools

    def bind(self, ctx) -> None:
        self.ctx = ctx

    async def act(self, cmd, ctx) -> None:
        args = cmd.args or {}
        op, key = args.get("op"), args.get("key")

        if op not in OPS:
            return await ctx.result("error", {"message": f"unknown op: {op!r}"})

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
