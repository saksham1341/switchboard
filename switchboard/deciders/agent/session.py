"""Session state for the agent decider, over the plain str->str KeyStore.

The decider holds no instance state that outlives a decide() call: everything
here is read from and written back to the store, so a process restart mid-turn
loses nothing but the in-flight call itself.
"""
import json
import time


class Sessions:
    def __init__(self, store, *, ttl: float | None = None):
        self._store = store
        # None keeps a session immortal (existing tests, and any caller that
        # does not opt in to expiry). §6.4 requires the record and its route
        # to expire together — see save()/delete() below.
        self._ttl = ttl

    # --- the session record ---------------------------------------------

    async def load(self, sid: int) -> dict | None:
        raw = await self._store.get(f"session:{sid}")
        return json.loads(raw) if raw is not None else None

    def _route_key(self, s: dict) -> str:
        return f"thread:{s['source']}:{s.get('thread_id') or s['channel_id']}"

    async def save(self, s: dict) -> None:
        await self._store.set(f"session:{s['sid']}", json.dumps(s), ttl=self._ttl)
        # The route must slide with the session. It is written once at mint
        # (set_route, below) but the session is rewritten every turn, so
        # refreshing only the session record would let the route expire out
        # from under a live conversation — the trap this task exists to avoid.
        # §6.4: tracking and conversation expire together.
        await self._store.set(self._route_key(s), str(s["sid"]), ttl=self._ttl)

    async def new(self, *, sid, source, channel_id, thread_id, anchor) -> dict:
        # `last_seen` is stamped at mint and refreshed by the decider on every
        # genuine bit of progress. It exists because `busy_since` is None
        # whenever the session is idle -- so an idle session, the only kind that
        # can be expired, had nothing to measure its idleness against.
        s = {"sid": sid, "source": source, "channel_id": channel_id,
             "thread_id": thread_id, "anchor": anchor,
             "state": "idle", "turn": 0, "busy_since": None,
             "last_seen": time.time(),
             "messages": [], "buffer": [], "gather": None}
        await self.save(s)
        return s

    async def delete(self, s: dict) -> None:
        """Remove both the record and its route. Leaving the route behind
        after a delete means the next message resolves to a session id that
        no longer loads -- the decider handles that (it returns early), but
        it is a leak and a confusing state."""
        await self._store.delete(f"session:{s['sid']}")
        await self._store.delete(self._route_key(s))

    # --- the route map ---------------------------------------------------

    # Source-qualified, per spec 6.1: extracting the routing id is per-source
    # work, and the qualifier keeps two sources' id spaces out of one namespace.
    # The generalization that pays is a MORE specific key, not an abstract one.
    async def route(self, source: str, key: str) -> int | None:
        raw = await self._store.get(f"thread:{source}:{key}")
        return int(raw) if raw is not None else None

    async def set_route(self, source: str, key: str, sid: int) -> None:
        # Ttl'd from mint: without it, a freshly minted session's route starts
        # life immortal while the record expires (the mirror trap of the one
        # above, at the other end of the session's life).
        await self._store.set(f"thread:{source}:{key}", str(sid), ttl=self._ttl)

    # --- the pending map -------------------------------------------------

    async def put_pending(self, command_id: int, entry: dict) -> None:
        # The session TTL, for the same reason the route carries it: a pending
        # entry whose result never arrives (a command dropped on the floor, a
        # crash between emit and result) would otherwise outlive the session it
        # points at forever. Harmless -- load() returns None and decide() gives
        # up -- but permanent, and sqlite keeps every one of them.
        await self._store.set(f"pending:{command_id}", json.dumps(entry),
                              ttl=self._ttl)

    async def take_pending(self, command_id: int) -> dict | None:
        """Read and delete. Deleting on read is what makes a redelivered result
        a no-op: the second decide() finds nothing pending and returns."""
        key = f"pending:{command_id}"
        raw = await self._store.get(key)
        if raw is None:
            return None
        await self._store.delete(key)
        return json.loads(raw)

    async def clear_pending(self, sid: int) -> int:
        """Drop every pending entry this session owns; returns how many.

        The pending map is keyed by command id, so there is no index from a
        session back to its commands and this has to scan. It lives here rather
        than in the decider because the decider reaching into `pending:` keys
        directly would put knowledge of this layout in two places.

        Only the watchdog needs it: every ordinary path consumes its entries
        one at a time through take_pending. Abandoning a turn is the one case
        where results are expected that must never be honoured -- their gather
        is already closed, and letting one back in restarts a turn that has
        been replaced.
        """
        dropped = 0
        for key in await self._store.keys("pending:"):
            raw = await self._store.get(key)
            if raw is None:
                continue                # expired between keys() and get()
            try:
                entry = json.loads(raw)
            except ValueError:
                continue
            if isinstance(entry, dict) and entry.get("sid") == sid:
                await self._store.delete(key)
                dropped += 1
        return dropped
