import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Literal

_CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"


def _encode(value: int, length: int) -> str:
    chars = []
    for _ in range(length):
        chars.append(_CROCKFORD[value & 0x1F])
        value >>= 5
    return "".join(reversed(chars))


def ulid() -> str:
    """26-char Crockford base32 ULID: 48-bit ms timestamp + 80-bit randomness.
    Lexicographically sortable by time; not strictly monotonic within a
    millisecond, which is fine — mamamia's integer message id is the ordering
    of record."""
    ms = int(time.time() * 1000) & ((1 << 48) - 1)
    rand = int.from_bytes(os.urandom(10), "big")  # 80 bits
    return _encode(ms, 10) + _encode(rand, 16)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class Event:
    id: str
    kind: str
    source: str
    at: str
    payload: dict
    dedupe_key: str | None = None
    meta: dict[str, str] = field(default_factory=dict)


@dataclass
class EventInput:
    kind: str
    source: str
    payload: dict
    at: str | None = None
    dedupe_key: str | None = None
    meta: dict[str, str] = field(default_factory=dict)


@dataclass
class PublishResult:
    status: Literal["accepted", "duplicate"]
    event_id: str
