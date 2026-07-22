import argparse
import asyncio
import json
import sys

from mamamia.core.models import MessageState
from mamamia.server.db import connect

LOG_ID = "events"


async def list_dead_letters(mamamia_db_path: str) -> list[dict]:
    """Return retained DEAD deliveries joined to their stored event. Reads the
    mamamia database directly (read-only) — mamamia has no query API for this,
    and the schema is stable within a pinned version."""
    conn = await connect(mamamia_db_path)
    try:
        dead = conn.execute(
            "SELECT group_id, message_id FROM message_state WHERE state = ? "
            "ORDER BY message_id DESC",
            (MessageState.DEAD.value,),
        ).fetchall()
        rows = []
        for group_id, message_id in dead:
            payload = conn.execute(
                "SELECT payload FROM messages WHERE log_id = ? AND id = ?",
                (LOG_ID, message_id),
            ).fetchone()
            event = _decode(payload[0]) if payload else {}
            rows.append({
                "group_id": group_id,
                "message_id": message_id,
                "event_id": event.get("id"),
                "kind": event.get("kind"),
            })
        return rows
    finally:
        conn.close()


def _decode(blob):
    import msgpack
    return msgpack.unpackb(blob, raw=False)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="switchboard")
    sub = parser.add_subparsers(dest="cmd", required=True)
    dl = sub.add_parser("dead-letters", help="list retained dead-lettered events")
    dl.add_argument("--db", required=True, help="path to mamamia's events db")
    args = parser.parse_args(argv)

    if args.cmd == "dead-letters":
        rows = asyncio.run(list_dead_letters(args.db))
        for r in rows:
            sys.stdout.write(json.dumps(r) + "\n")
        return 0
    return 1
