"""Read-only SQL over mamamia's database.

mamamia exposes no query API (append/get_batch/prune only), so the dead-letter
CLI already reads the schema directly and documents why. This follows that
precedent: read-only, never written to, and pinned to a mamamia version.
"""
import sqlite3

FRAME_KEYS = ("log", "id", "name", "emitted_by", "observation_id", "command_id",
              "dead_log", "dead_id", "seen_at")


def _decode(blob):
    import msgpack
    return msgpack.unpackb(blob, raw=False)


def _connect(db_path: str) -> sqlite3.Connection:
    # uri=True + mode=ro so a bug here can never write to the relay's database.
    return sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=2.0)


def backfill(db_path: str, limit: int = 50) -> list[dict]:
    """The most recent messages across both logs, oldest first so the page can
    replay them in order. Structure only — payloads are never read."""
    conn = _connect(db_path)
    try:
        rows = conn.execute(
            "SELECT log_id, id, metadata FROM messages ORDER BY rowid DESC LIMIT ?",
            (limit,),
        ).fetchall()
    finally:
        conn.close()

    out = []
    for log_id, mid, meta_blob in reversed(rows):
        md = _decode(meta_blob) if meta_blob else {}
        out.append({
            "log": log_id,
            "id": mid,
            "name": md.get("name", ""),
            "emitted_by": md.get("emitted_by"),
            "observation_id": md.get("observation_id"),
            "command_id": md.get("command_id"),
            "seen_at": None,          # historical: no arrival time to report
        })
    return out
