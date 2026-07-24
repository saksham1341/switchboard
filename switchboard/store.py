import sqlite3
import time
from typing import Protocol, runtime_checkable


def _check_key(key) -> None:
    """Keys are str. No implicit coercion: str(5) would make
    set(k, 5) followed by get(k) == 5 evaluate False, discovered in production."""
    if not isinstance(key, str):
        raise TypeError(f"KeyStore keys must be str, got {type(key).__name__}")


def _check_value(value) -> None:
    """Values are str. None is a real value, not a sentinel for "no value
    supplied" — it must be rejected the same as any other non-str value."""
    if not isinstance(value, str):
        raise TypeError(f"KeyStore values must be str, got {type(value).__name__}. "
                        f"Serialize structured values yourself (json.dumps).")


@runtime_checkable
class KeyStore(Protocol):
    """get / set / delete / keys, and nothing else.

    `purge` is deliberately absent: whether expiry needs a periodic sweep is an
    implementation detail. Sqlite and memory reclaim by sweeping; a Redis-backed
    store expires natively and would expose no purge. Callers ask for it, they
    do not assume it. `keys`, unlike purge, belongs on the contract: every
    backend can list by prefix (dict iteration, sqlite LIKE, Redis SCAN), and
    callers need it — an agent's memory has to be enumerable to be useful.
    """
    async def get(self, key: str) -> str | None: ...
    async def set(self, key: str, value: str, *, ttl: float | None = None) -> None: ...
    async def delete(self, key: str) -> None: ...
    async def keys(self, prefix: str = "") -> list[str]: ...


class MemoryStore:
    """Volatile KeyStore. Written to the same contract as SqliteStore rather
    than the one a dict gives naturally — expiry on read and type checks are
    explicit — so behaviour cannot diverge between tests and production."""

    def __init__(self, *, time_fn=time.time):
        self._now = time_fn
        self._data: dict[str, tuple[str, float | None]] = {}

    async def get(self, key):
        _check_key(key)
        entry = self._data.get(key)
        if entry is None:
            return None
        value, expires_at = entry
        if expires_at is not None and expires_at <= self._now():
            del self._data[key]
            return None
        return value

    async def set(self, key, value, *, ttl=None):
        _check_key(key)
        _check_value(value)
        self._data[key] = (value, None if ttl is None else self._now() + ttl)

    async def delete(self, key):
        _check_key(key)
        self._data.pop(key, None)

    async def keys(self, prefix: str = "") -> list[str]:
        _check_key(prefix)
        now = self._now()
        return [k for k, (_, exp) in self._data.items()
                if k.startswith(prefix) and (exp is None or exp > now)]

    def purge(self) -> int:
        now = self._now()
        expired = [k for k, (_, exp) in self._data.items()
                   if exp is not None and exp <= now]
        for k in expired:
            del self._data[k]
        return len(expired)


class SqliteStore:
    """Durable KeyStore. Synchronous sqlite3 driven on the event-loop thread,
    the same approach mamamia's own backends use; operations are microseconds."""

    def __init__(self, db_path: str, *, time_fn=time.time):
        self._now = time_fn
        self._conn = sqlite3.connect(db_path, isolation_level=None, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS kv ("
            "  key        TEXT PRIMARY KEY,"
            "  value      TEXT NOT NULL,"
            "  expires_at REAL"                       # NULL = never
            ")")
        self._conn.execute("CREATE INDEX IF NOT EXISTS kv_expires ON kv (expires_at)")

    async def get(self, key):
        _check_key(key)
        # Expiry is filtered in the read, so an expired row is invisible the
        # instant it expires regardless of when purge last ran.
        row = self._conn.execute(
            "SELECT value FROM kv WHERE key = ? AND (expires_at IS NULL OR expires_at > ?)",
            (key, self._now())).fetchone()
        return row[0] if row else None

    async def set(self, key, value, *, ttl=None):
        _check_key(key)
        _check_value(value)
        self._conn.execute(
            "INSERT INTO kv (key, value, expires_at) VALUES (?, ?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value, expires_at=excluded.expires_at",
            (key, value, None if ttl is None else self._now() + ttl))

    async def delete(self, key):
        _check_key(key)
        self._conn.execute("DELETE FROM kv WHERE key = ?", (key,))

    async def keys(self, prefix: str = "") -> list[str]:
        _check_key(prefix)
        like = prefix.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"
        rows = self._conn.execute(
            "SELECT key FROM kv WHERE key LIKE ? ESCAPE '\\' "
            "AND (expires_at IS NULL OR expires_at > ?)",
            (like, self._now())).fetchall()
        return [r[0] for r in rows]

    def purge(self) -> int:
        """Delete expired rows. About disk, never about correctness."""
        return self._conn.execute(
            "DELETE FROM kv WHERE expires_at IS NOT NULL AND expires_at <= ?",
            (self._now(),)).rowcount

    def close(self) -> None:
        self._conn.close()


class ScopedStore:
    """A KeyStore view over a prefix. Roles never see the prefix and cannot
    reach another role's keys — the log is the channel between roles."""

    def __init__(self, inner, prefix: str):
        self._inner, self._prefix = inner, prefix

    async def get(self, key):
        _check_key(key)
        return await self._inner.get(self._prefix + key)

    async def set(self, key, value, *, ttl=None):
        _check_key(key)
        _check_value(value)
        await self._inner.set(self._prefix + key, value, ttl=ttl)

    async def delete(self, key):
        _check_key(key)
        await self._inner.delete(self._prefix + key)

    async def keys(self, prefix: str = "") -> list[str]:
        _check_key(prefix)
        n = len(self._prefix)
        return [k[n:] for k in await self._inner.keys(self._prefix + prefix)]
