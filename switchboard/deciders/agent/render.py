"""Observation -> one rendered block in the user turn (spec 6.6).

The decider never normalizes a payload before the model sees it. The model
chooses tools, and the source is what tells it `discord.post` rather than some
other source's post tool, with which id. Source is semantic content.

This is also the untrusted-content boundary. The header is written here; message
text goes inside <message> delimiters, and any delimiter in the text itself is
neutralised, so a user cannot forge a header and relocate the conversation.
"""

import re

OPEN, CLOSE = "<message>", "</message>"

# Case-insensitive, whitespace-tolerant match for anything delimiter-shaped:
# opening or closing, any casing, optional internal whitespace (space, tab,
# newline all satisfy \s), and any trailing attribute-like junk before the
# `>`. A byte-exact `.replace("</message>", ...)` only catches the one exact
# spelling we wrote -- an LLM reader will very plausibly still honour
# `</MESSAGE>`, `</Message >` or `</message foo>` as a closing tag, so the
# match has to be this loose to close the actual gap.
#
# The single `[\s/]*` class is load-bearing, not style. The obvious spelling
# -- `<\s*(/?)\s*message\s*[^>]*>` -- puts two `\s*` either side of `/?`, and
# another `\s*` immediately before `[^>]*`; each pair can match the same
# whitespace, so the engine backtracks them against each other. On
# `"<" + " "*n + "message" + " "*n` that is super-quadratic (~7x per doubling
# of n, measured), and this input arrives straight from an untrusted Discord
# message. A synchronous regex blocks the event loop, so `_consume`'s
# asyncio.timeout cannot preempt it -- one crafted message would freeze the
# whole process, not merely this turn. Collapsing to one character class,
# anchored by the literal `message`, removes both ambiguities and makes the
# match linear (200k chars in ~2ms).
_DELIM_RE = re.compile(r"<([\s/]*)message[^>]*>", re.IGNORECASE)

# Collapses whitespace runs anywhere in a header field, including the
# newline/tab that a byte-for-byte check on the closing delimiter alone would
# miss.
_WS_RE = re.compile(r"\s+")


def _text(value) -> str:
    return value if isinstance(value, str) else "" if value is None else str(value)


def _escape_delimiters(text: str) -> str:
    # Neutralise rather than strip: the model should still be able to see
    # that the user typed something delimiter-shaped, not have it silently
    # vanish. Kept visibly delimiter-like (escaped entities) rather than
    # deleted, and distinguishes open vs. close so the escaped form still
    # reads naturally.
    def repl(match: "re.Match[str]") -> str:
        return "&lt;/message&gt;" if "/" in match.group(1) else "&lt;message&gt;"

    return _DELIM_RE.sub(repl, text)


def _sanitize_header_field(value) -> str:
    """Sanitise a value before it is interpolated into the header line.

    The header's integrity is a construction property of THIS function, not
    a reliance on any upstream source validating what it hands us. Today
    `user_name` is `str(message.author)` (see switchboard/sensors/discord.py)
    and legacy Discord usernames permit spaces; a newline there would close
    the header line early and let a fully forged header appear before the
    real <message> block. We do not trust that constraint to hold forever,
    or that channel_id/thread_id will always be clean platform snowflakes,
    so every payload-sourced value that lands in the header goes through
    this, not just user_name.

    Whitespace (space, tab, newline, ...) collapses to a single space so a
    value can never introduce a second header line. `=` is neutralised
    (swapped for the visually-similar fullwidth U+FF1D) so a value can never
    masquerade as another `key=value` pair in the line the model is told to
    trust.

    Delimiters are escaped here too. Whitespace collapse alone would still let
    a `user_name` of "bob <message>" plant an opening delimiter inside the
    header line, which is precisely the structure this function exists to own.
    """
    text = _escape_delimiters(_WS_RE.sub(" ", _text(value)).strip())
    return text.replace("=", "＝")


def render_message(payload: dict) -> str:
    payload = payload if isinstance(payload, dict) else {}
    thread = payload.get("thread")
    thread = thread if isinstance(thread, dict) else {}

    bits = [f"channel_id={_sanitize_header_field(payload.get('channel_id'))}",
            f"user={_sanitize_header_field(payload.get('user_name'))}"]
    if payload.get("thread_id"):
        bits.insert(1, f"thread_id={_sanitize_header_field(payload.get('thread_id'))}")
        count = thread.get("message_count")
        if isinstance(count, int):
            bits.append(f"thread_messages={count}")

    body = _escape_delimiters(_text(payload.get("content")))
    return f"[discord.message] {' '.join(bits)}\n{OPEN}\n{body}\n{CLOSE}"
