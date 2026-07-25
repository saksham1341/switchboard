"""Session state for the agent decider, over the plain str->str KeyStore.

The decider holds no instance state that outlives a decide() call: everything
here is read from and written back to the store, so a process restart mid-turn
loses nothing but the in-flight call itself.
"""
import json


class Sessions:
    def __init__(self, store):
        self._store = store

    # --- the session record ---------------------------------------------

    async def load(self, sid: int) -> dict | None:
        raw = await self._store.get(f"session:{sid}")
        return json.loads(raw) if raw is not None else None

    async def save(self, s: dict) -> None:
        await self._store.set(f"session:{s['sid']}", json.dumps(s))

    async def new(self, *, sid, source, channel_id, thread_id, anchor) -> dict:
        s = {"sid": sid, "source": source, "channel_id": channel_id,
             "thread_id": thread_id, "anchor": anchor,
             "state": "idle", "turn": 0,
             "messages": [], "buffer": [], "gather": None}
        await self.save(s)
        return s

    # --- the route map ---------------------------------------------------

    # Source-qualified, per spec 6.1: extracting the routing id is per-source
    # work, and the qualifier keeps two sources' id spaces out of one namespace.
    # The generalization that pays is a MORE specific key, not an abstract one.
    async def route(self, source: str, key: str) -> int | None:
        raw = await self._store.get(f"thread:{source}:{key}")
        return int(raw) if raw is not None else None

    async def set_route(self, source: str, key: str, sid: int) -> None:
        await self._store.set(f"thread:{source}:{key}", str(sid))

    # --- the pending map -------------------------------------------------

    async def put_pending(self, command_id: int, entry: dict) -> None:
        await self._store.set(f"pending:{command_id}", json.dumps(entry))

    async def take_pending(self, command_id: int) -> dict | None:
        """Read and delete. Deleting on read is what makes a redelivered result
        a no-op: the second decide() finds nothing pending and returns."""
        key = f"pending:{command_id}"
        raw = await self._store.get(key)
        if raw is None:
            return None
        await self._store.delete(key)
        return json.loads(raw)
