import sqlite3

_SCHEMA_VERSION = 1


class SeenStore:
    """Switchboard's own durable dedup table, in a database mamamia never sees.
    Maps a provider idempotency key (e.g. GitHub's X-GitHub-Delivery) to the
    event id it produced. Synchronous sqlite3, driven on the event-loop thread
    like mamamia's own backends; operations are microseconds."""

    def __init__(self, db_path: str):
        self._conn = sqlite3.connect(db_path, isolation_level=None, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")  # a lost tail = a possible dup, never loss
        self._migrate()

    def _migrate(self) -> None:
        version = self._conn.execute("PRAGMA user_version").fetchone()[0]
        if version < 1:
            self._conn.execute(
                "CREATE TABLE IF NOT EXISTS seen ("
                "  key      TEXT PRIMARY KEY,"
                "  event_id TEXT NOT NULL"
                ")"
            )
            self._conn.execute(f"PRAGMA user_version={_SCHEMA_VERSION}")

    def get(self, key: str) -> str | None:
        row = self._conn.execute(
            "SELECT event_id FROM seen WHERE key = ?", (key,)
        ).fetchone()
        return row[0] if row else None

    def record(self, key: str, event_id: str) -> None:
        self._conn.execute(
            "INSERT OR IGNORE INTO seen (key, event_id) VALUES (?, ?)",
            (key, event_id),
        )

    def prune(self, keep_last: int) -> int:
        cur = self._conn.execute(
            "DELETE FROM seen WHERE rowid NOT IN ("
            "  SELECT rowid FROM seen ORDER BY rowid DESC LIMIT ?"
            ")",
            (keep_last,),
        )
        return cur.rowcount

    def close(self) -> None:
        self._conn.close()
