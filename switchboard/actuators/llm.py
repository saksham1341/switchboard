"""The model, as an actuator.

Deliberately generic: it takes messages and tools and returns a completion, and
knows nothing about agents. Any decider could emit `llm` commands. Everything
about the request — system prompt, model, tools, limits — travels in the command
args, because the decider owns those choices.

Raw httpx rather than the anthropic SDK: this matches how DiscordSender talks to
Discord, adds no dependency, and the Messages API is a single POST.
"""
import httpx

ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"
ANTHROPIC_VERSION = "2023-06-01"
DEFAULT_MODEL = "claude-sonnet-5"
DEFAULT_MAX_TOKENS = 4096
TIMEOUT = 120.0                 # a long completion with tools is not fast


class LlmActuator:
    name = "llm"
    tool_spec = None            # the decider emits this directly; not a tool

    def __init__(self, api_key: str, *, model: str = DEFAULT_MODEL, client=None):
        self._key = api_key
        self._model = model
        self._client = client
        self._http = None

    def bind(self, ctx) -> None:
        self.ctx = ctx
        # Built here, not in __init__: an httpx client wants a running loop, and
        # bind() runs inside Bus.start().
        self._http = self._client or httpx.AsyncClient(timeout=TIMEOUT)

    async def act(self, cmd, ctx) -> None:
        args = cmd.args or {}
        body = {
            "model": args.get("model") or self._model,
            "max_tokens": args.get("max_tokens") or DEFAULT_MAX_TOKENS,
            "messages": args.get("messages") or [],
        }
        if args.get("system"):
            body["system"] = args["system"]
        if args.get("tools"):
            body["tools"] = args["tools"]

        resp = await self._http.post(ANTHROPIC_URL, json=body, headers={
            "x-api-key": self._key,
            "anthropic-version": ANTHROPIC_VERSION,
            "content-type": "application/json",
        })

        if resp.status_code >= 400:
            # Reported, not raised: the caller learns immediately instead of
            # after the full retry-backoff cycle.
            try:
                message = resp.json().get("error", {}).get("message", resp.text)
            except ValueError:
                message = resp.text
            return await ctx.result("error", {"status": resp.status_code,
                                              "message": message})

        data = resp.json()
        await ctx.result("ok", {
            "stop_reason": data.get("stop_reason"),
            "content": data.get("content", []),
            "usage": data.get("usage", {}),
        })

    async def close(self) -> None:
        if self._http is not None:
            await self._http.aclose()
