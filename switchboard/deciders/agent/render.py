"""Observation -> one rendered block in the user turn (spec 6.6).

The decider never normalizes a payload before the model sees it. The model
chooses tools, and the source is what tells it `discord.post` rather than some
other source's post tool, with which id. Source is semantic content.

This is also the untrusted-content boundary. The header is written here; message
text goes inside <message> delimiters, and any delimiter in the text itself is
neutralised, so a user cannot forge a header and relocate the conversation.
"""

OPEN, CLOSE = "<message>", "</message>"


def _text(value) -> str:
    return value if isinstance(value, str) else "" if value is None else str(value)


def render_message(payload: dict) -> str:
    payload = payload if isinstance(payload, dict) else {}
    thread = payload.get("thread")
    thread = thread if isinstance(thread, dict) else {}

    bits = [f"channel_id={_text(payload.get('channel_id'))}",
            f"user={_text(payload.get('user_name'))}"]
    if payload.get("thread_id"):
        bits.insert(1, f"thread_id={_text(payload.get('thread_id'))}")
        count = thread.get("message_count")
        if isinstance(count, int):
            bits.append(f"thread_messages={count}")

    # Neutralise the delimiters rather than stripping them: the model should
    # see that the user typed something delimiter-shaped, not have it silently
    # vanish. Zero-width-free, plain, and unambiguous to a reader.
    body = _text(payload.get("content")).replace(CLOSE, "&lt;/message&gt;") \
                                        .replace(OPEN, "&lt;message&gt;")
    return f"[discord.message] {' '.join(bits)}\n{OPEN}\n{body}\n{CLOSE}"
