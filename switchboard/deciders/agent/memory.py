"""The agent's memory, as two tools over the `kv` actuator (SSOT §7.3).

The agent never sees `kv` directly, and never gets to choose a namespace: it
sees exactly two tools, `scratchpad` and `memory`, and the decider rewrites
whichever key it supplies into a namespaced `kv` key before a command is ever
emitted. That rewrite is the security boundary, not bookkeeping -- it is what
makes it impossible for a prompt-injected agent in one session to name a key
that lands in another session's scratchpad, or to escape into the global
namespace from inside a session-scoped one. The model-supplied key is
treated as hostile throughout: path separators, `..`, and any leading
namespace-looking segment are stripped before the real prefix is joined on,
and the joined result is re-checked to confirm it still starts with the
namespace it was supposed to land in.

- `scratchpad` is namespaced to the calling session (`session:<sid>:`) and
  expires with it (the same TTL Task 4 gives the session record itself) --
  working notes for one conversation, gone when it ends.
- `memory` is namespaced globally (`global:`) and carries no TTL -- it
  outlives any single conversation, so it is for what is worth still knowing
  weeks from now, not scratch state.

Both expose all four `kv` ops (`get`/`set`/`delete`/`list`); `list` is always
scoped to the tool's own namespace prefix, never the whole store, so a
`scratchpad.list` can only ever enumerate that session's own keys.
"""
import re

from switchboard.actuators.kv import OPS

SCRATCHPAD = "scratchpad"
MEMORY = "memory"

# A leading run of ".." path-traversal segments. Only ".." — a namespace tag
# needs a colon, and by the time this runs there are none left to find (see
# _sanitize_key). Neutralised anyway: a key that opens with ".." is a key
# trying to climb, and it should not read as one in the store.
_LEADING_DOTS = re.compile(r"^(?:\.\.)+")

# The two characters a tail must never contain literally, and what each is
# encoded as. `:` because it is what makes a namespace tag ("global:",
# "session:999:") — a tail with no colon cannot forge one, whatever else it
# says. `/` because it is the ScopedStore separator. `%` leads the list
# because it is the escape character: encoding it first is what makes this a
# reversible mapping rather than a mangling, so two distinct model keys can
# never land on one stored key.
_ENCODE = (("%", "%25"), (":", "%3A"), ("/", "%2F"))


def _sanitize_key(key: object) -> str:
    """Reduce a model-supplied key to a tail that cannot name a namespace.

    Treats the input as hostile -- a non-string collapses to the empty tail
    rather than raising, so a malformed call still lands (harmlessly) inside
    the caller's own namespace instead of blowing up the fan-out.

    Encoding, not truncating. The previous rule stripped a leading run of
    namespace-looking segments, and because that run was greedy through the
    LAST colon it silently reduced every colon-delimited key to its final
    segment: 'project:alpha:status' and 'project:beta:status' both became
    'status', so the second memory destroyed the first with no signal to the
    model -- `op: list` showed only the collapsed key. A colon-delimited key is
    the most likely thing an LLM types into a key/value store, and the tool
    description tells it not to worry about namespacing, which reads as
    "colons are harmless", not "your key is cut down to its last segment".

    The security property is unchanged and is why the encoding is chosen this
    way rather than a strip: a namespace tag is made of colons, so a tail that
    contains no literal colon cannot forge one no matter what it spells. The
    `startswith(prefix)` re-check in _namespaced_key still backs it up.
    """
    if not isinstance(key, str):
        return ""
    for raw, encoded in _ENCODE:
        key = key.replace(raw, encoded)
    # Encoded, not deleted, for the same reason: deleting a leading ".." would
    # map '..notes' and 'notes' onto one stored key, reintroducing in miniature
    # the collision this function exists to stop.
    return _LEADING_DOTS.sub(lambda m: "%2E" * len(m.group(0)), key)


def _namespaced_key(prefix: str, raw_key: object) -> str:
    tail = _sanitize_key(raw_key)
    full = f"{prefix}{tail}"
    # Belt and suspenders: sanitisation above should already guarantee this,
    # but the prefix is the one thing that must never be wrong, so re-verify
    # the join rather than trust it silently held.
    if not full.startswith(prefix):
        full = prefix
    return full


def _tool_spec(name: str, what_for: str) -> dict:
    return {
        "name": name,
        "description": what_for,
        "input_schema": {
            "type": "object",
            "properties": {
                "op": {
                    "type": "string",
                    "enum": list(OPS),
                    "description": (
                        "Required. What to do: 'get' reads one key's value, "
                        "'set' writes one key's value (creating or "
                        "overwriting it), 'delete' removes one key, 'list' "
                        "returns the keys you have stored here (no key "
                        "needed for 'list')."
                    ),
                },
                "key": {
                    "type": "string",
                    "description": (
                        "Required for get/set/delete. A short name for the "
                        "note, e.g. 'user_prefs' or 'last_topic'. You do not "
                        "need to (and cannot) namespace it yourself -- that "
                        "is handled for you."
                    ),
                },
                "value": {
                    "type": "string",
                    "description": "Required for set. The text to store.",
                },
            },
            "required": ["op"],
        },
    }


# The names the decider owns end to end. A call to one of these NEVER falls
# through to the ordinary tool path: rewrite() either produces kv args or the
# call is answered in-band as an error. Emitting a command under one of these
# names would address an actuator that does not exist, and an unconsumed
# command is never acquired, never retried, never dead-lettered (§7.5) — the
# session would simply hang until the watchdog frees it.
MEMORY_TOOL_NAMES = frozenset({"scratchpad", "memory"})

MEMORY_TOOLS = [
    _tool_spec(
        SCRATCHPAD,
        "Working notes for this conversation only. Use it to jot down "
        "something you'll want a few turns from now within this same "
        "session -- a draft you're building up, an intermediate result, a "
        "plan you're partway through -- without repeating it in every "
        "reply. Everything you store here disappears when this "
        "conversation ends or goes quiet for a while; nothing here is "
        "visible to, or shared with, any other conversation. If it needs "
        "to survive past this conversation, use `memory` instead.",
    ),
    _tool_spec(
        MEMORY,
        "Your long-term memory: things worth still knowing weeks from now, "
        "shared across every conversation you have, kept until you "
        "explicitly delete them. Use it for durable facts about the people "
        "or projects you deal with, standing preferences, or anything you'd "
        "want to recall the next time it comes up, even in a completely "
        "different conversation. Do not use it for short-lived working "
        "notes that only matter for the rest of this conversation -- that "
        "is what `scratchpad` is for.",
    ),
]


def rewrite(tool_name: str, args: dict, sid: int, ttl: float | None) -> dict | None:
    """Model-facing tool call -> kv command args, or None.

    None is returned both when `tool_name` is not one of ours (so the
    decider knows this call is somebody else's to handle) and when the op is
    missing or unrecognised (so an invalid call never reaches the `kv`
    actuator as a command). The two are indistinguishable to the caller by
    design -- both mean "rewrite produced nothing to emit".
    """
    if tool_name == SCRATCHPAD:
        prefix = f"session:{sid}:"
        item_ttl = ttl
    elif tool_name == MEMORY:
        prefix = "global:"
        item_ttl = None
    else:
        return None

    args = args if isinstance(args, dict) else {}
    op = args.get("op")
    if op not in OPS:
        return None

    if op == "list":
        # Scoped to this tool's own namespace, never the whole store -- an
        # unscoped list would hand one session every other session's keys.
        return {"op": "list", "prefix": prefix}

    out = {"op": op, "key": _namespaced_key(prefix, args.get("key"))}
    if op == "set":
        out["value"] = args.get("value")
        if item_ttl is not None:
            out["ttl"] = item_ttl
    return out
