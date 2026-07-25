"""Shared text rendering for messages on the bus.

A producer may attach a rendered form of its message (`text=` on emit). This
module is what it builds that form with. Two rules live here and nowhere else:

- **Untrusted content is delimited and escaped.** `message_text` escapes the
  body for the caller, so a producer using it cannot forget. The contract is
  that a stored `text` is used verbatim by readers — the escaping has to happen
  at write time, and this is the helper that makes that the easy path.
- **Header fields are sanitised.** The model is told the header is trustworthy,
  so a field value must not be able to open a second line or look like another
  key=value pair.
"""
import re

OPEN, CLOSE = "<untrusted>", "</untrusted>"

# Case-insensitive, whitespace-tolerant match for anything delimiter-shaped.
# A byte-exact `.replace("</untrusted>", …)` catches only the one spelling we
# wrote -- an LLM reader will very plausibly still honour `</UNTRUSTED>` or
# `</untrusted foo>` as a closing tag.
#
# The single `[\s/]*` class is load-bearing, not style. The obvious spelling --
# `<\s*(/?)\s*untrusted\s*[^>]*>` -- puts two `\s*` either side of `/?` and
# another before `[^>]*`; each pair can match the same whitespace, so the engine
# backtracks them against each other. That is super-quadratic on
# `"<" + " "*n + "untrusted" + " "*n`, and this input arrives straight from an
# untrusted message. A synchronous regex blocks the event loop, so `_consume`'s
# asyncio.timeout cannot preempt it -- one crafted message would freeze the
# whole process. One character class, anchored by the literal, is linear.
_DELIM_RE = re.compile(r"<([\s/]*)untrusted[^>]*>", re.IGNORECASE)
_WS_RE = re.compile(r"\s+")


def _text(value) -> str:
    return value if isinstance(value, str) else "" if value is None else str(value)


def escape_delimiters(text: str) -> str:
    """Neutralise anything delimiter-shaped. Escape rather than strip: the model
    should see that someone typed something delimiter-shaped, not have it
    silently vanish."""
    def repl(match: "re.Match[str]") -> str:
        return "&lt;/untrusted&gt;" if "/" in match.group(1) else "&lt;untrusted&gt;"
    return _DELIM_RE.sub(repl, _text(text))


def sanitize_field(value) -> str:
    """Make a value safe to interpolate into a header line. Whitespace collapses
    (so it cannot open a second line), `=` is neutralised to the fullwidth form
    (so it cannot masquerade as another key), and delimiters are escaped."""
    text = escape_delimiters(_WS_RE.sub(" ", _text(value)).strip())
    return text.replace("=", "＝")


def message_text(name: str, fields: dict, body: str | None) -> str:
    """One message as text: a header line, then the body between delimiters.

    `body` is escaped here — callers do not, and must not, pre-escape it.
    A None body renders header-only (nothing untrusted to delimit).
    """
    fields = fields if isinstance(fields, dict) else {}
    head = " ".join(f"{k}={sanitize_field(v)}" for k, v in fields.items())
    header = f"[{name}] {head}".rstrip()
    if body is None:
        return header
    return f"{header}\n{OPEN}\n{escape_delimiters(body)}\n{CLOSE}"
