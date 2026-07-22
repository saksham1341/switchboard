# GitHub → Discord Relay Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Relay GitHub activity (PRs, reviews, issues, CI results) into a Discord channel as rich embeds with link/view buttons, riding the existing GitHub ingress and Discord egress on the durable log.

**Architecture:** Sink-oriented. `DiscordEgress` is "our Discord output surface"; the relay is a **new handler** (`notify-github`) on it — not a new egress. The handler consumes `source=="github"` events, formats each into an embed + link-button row via a pure `build_message`, and posts to a configured channel via the shared `DiscordSender`. To let one egress serve multiple input sources, its coarse `filter` relaxes to `None` and the existing `ping`/`echo` handler filters become source-aware.

**Tech Stack:** Python 3.12, asyncio, `httpx` (Discord REST), existing Switchboard v1 (`Broker`, `Egress`/`Handler`/`Ctx`, `GitHubIngress`/`map_event`, `DiscordEgress`/`DiscordSender`), mamamia `v0.2.0`.

## Global Constraints

- **Python 3.12**, asyncio; every interface boundary is a coroutine.
- **Sink-oriented egress:** an egress is one output binding (`context()` = a `DiscordSender`); handlers are routes into it, each owning its input `filter`. The relay is a handler on `DiscordEgress`, sharing the one `DiscordSender`.
- **`build_message(kind, payload)` is pure** — no I/O; reads GitHub webhook fields defensively (`.get` chains); returns `{"embed": dict, "components": list}` or `None` (unrecognized kind / unusable payload). Any button whose URL can't resolve is omitted; never emit a broken button.
- **Link buttons only** — Discord component `type: 2, style: 5` (URL button); no `custom_id`, no interaction. Wrapped in one action row (`type: 1`). Write-back actions are out of scope.
- **Discord channel send** — `POST /channels/{channel_id}/messages`, header `Authorization: Bot <token>`, body may carry `content`, `embeds`, `components`. API base `https://discord.com/api/v10`.
- **Colors (int):** blue `0x3B82F6`, purple `0x8B5CF6`, grey `0x6B7280`, green `0x22C55E`, red `0xEF4444`, yellow `0xEAB308`.
- **msgpack round-trip:** relay reads only `event.kind` (str) and `event.payload` (the full GitHub webhook dict, already msgpack-safe as stored by the ingress). No new id stringification needed.
- **Backward compatibility:** `DiscordSender.send`'s existing plain-text call (`send(channel, "text")`) must keep working. `ping`/`echo` behavior is unchanged.
- **Config/secrets from env:** new `DISCORD_NOTIFY_CHANNEL_ID`. The relay handler is wired only when it is set (and Discord is already wired, i.e. `DISCORD_BOT_TOKEN` present).
- TDD; commit after each green task; no live Discord/GitHub calls in tests (fake at the `httpx` boundary with `httpx.MockTransport`; payloads inline or from `tests/fixtures/github/`).

**Existing API this builds on (verified on `main` @ `ce96cf2`):**
```python
# switchboard/ingress/github.py
def map_event(gh_event: str, payload: dict) -> EventInput | None   # source="github", kind=f"github.{repo}.{cat}.{action}"
# switchboard/egress/discord.py
class DiscordSender:  # __init__(bot_token, application_id, *, client=None); reply(); send(); close()
class DiscordEgress:  # __init__(bot_token, application_id, *, client=None); name="discord"; filter; handlers; context()->DiscordSender; close()
# switchboard/egress/__init__.py
@dataclass class Handler: name; filter; handle; timeout_s=None; lease_s=None
# broker gate: passes(e) == (egress.filter is None or egress.filter(e)) and handler.filter(e)
```

---

## File Structure

```
switchboard/
├── ingress/
│   └── github.py              # MODIFY: map_event also emits check_run.succeeded (Task 1)
├── egress/
│   ├── discord.py             # MODIFY: DiscordSender.send +embed/+components (Task 2); DiscordEgress sink refactor + notify-github (Task 4)
│   └── github_notify.py       # CREATE: pure build_message(kind, payload) (Task 3)
└── app.py                     # MODIFY: DISCORD_NOTIFY_CHANNEL_ID wiring (Task 6)
tests/
├── test_github_map.py         # MODIFY: check_run.success now maps to succeeded (Task 1)
├── test_discord_sender.py     # MODIFY: embed+components send path (Task 2)
├── test_github_notify.py      # CREATE: build_message table-driven (Task 3)
├── test_discord_egress.py     # MODIFY: sink refactor — filter None, source-aware, notify-github (Task 4)
├── test_github_relay_integration.py  # CREATE: github event -> relay -> channel POST (Task 5)
└── test_app.py                # MODIFY: relay wired iff DISCORD_NOTIFY_CHANNEL_ID set (Task 6)
```

---

## Task 1: Ingress — emit `check_run.succeeded`

**Files:**
- Modify: `switchboard/ingress/github.py` (the `check_run` branch of `map_event`)
- Modify: `tests/test_github_map.py`

**Interfaces:**
- Produces: a new event kind `github.{repo}.check_run.succeeded` for `check_run` completed with `conclusion == "success"`. `.failed` and all other conclusions unchanged.

- [ ] **Step 1: Flip the ignored-success test and add the failed one stays**

In `tests/test_github_map.py`, replace `test_map_check_run_success_is_ignored` with:
```python
def test_map_check_run_success_is_succeeded():
    ei = map_event("check_run", _load("check_run.success.json"))
    assert ei.kind == "github.home.check_run.succeeded"
    assert ei.source == "github"
```
(Leave `test_map_check_run_failed` as-is.)

- [ ] **Step 2: Run to verify it fails**

Run: `. venv/bin/activate && python -m pytest tests/test_github_map.py -v`
Expected: FAIL — `map_event` currently returns `None` for success, so `ei.kind` raises `AttributeError`.

- [ ] **Step 3: Extend the `check_run` branch**

In `switchboard/ingress/github.py`, replace the `check_run` block in `map_event`:
```python
    if gh_event == "check_run" and payload.get("action") == "completed":
        conclusion = payload.get("check_run", {}).get("conclusion")
        if conclusion == "failure":
            return _event(f"github.{repo}.check_run.failed", payload)
        if conclusion == "success":
            return _event(f"github.{repo}.check_run.succeeded", payload)
        return None
```

- [ ] **Step 4: Run to verify it passes**

Run: `python -m pytest tests/test_github_map.py -v`
Expected: all pass (success → succeeded, failed unchanged, others still ignored).

- [ ] **Step 5: Commit**

```bash
git add switchboard/ingress/github.py tests/test_github_map.py
git commit -m "feat(github): emit check_run.succeeded on a passing check"
```

---

## Task 2: Sender — embed + components in `send()`

**Files:**
- Modify: `switchboard/egress/discord.py` (`DiscordSender.send` only)
- Modify: `tests/test_discord_sender.py`

**Interfaces:**
- Produces: `async def send(self, channel_id, content=None, *, embed=None, components=None) -> httpx.Response` — body carries only the provided keys among `content`/`embeds`/`components`.

- [ ] **Step 1: Add the failing test**

Append to `tests/test_discord_sender.py`:
```python
async def test_send_posts_embed_and_components():
    seen = {}

    def handler(request):
        seen["url"] = str(request.url)
        seen["auth"] = request.headers.get("authorization")
        seen["body"] = json.loads(request.content)
        return httpx.Response(200, json={})

    s = DiscordSender("bot-tok", "app-123", client=_client(handler))
    embed = {"title": "hi", "color": 1}
    comps = [{"type": 1, "components": [{"type": 2, "style": 5, "label": "X", "url": "https://x"}]}]
    await s.send("chan-9", embed=embed, components=comps)
    await s.close()

    assert seen["url"] == f"{DISCORD_API}/channels/chan-9/messages"
    assert seen["auth"] == "Bot bot-tok"
    assert seen["body"] == {"embeds": [embed], "components": comps}   # no "content" key


async def test_send_plain_text_still_works():
    seen = {}

    def handler(request):
        seen["body"] = json.loads(request.content)
        return httpx.Response(200, json={})

    s = DiscordSender("bot-tok", "app-123", client=_client(handler))
    await s.send("chan-9", "hello channel")
    await s.close()
    assert seen["body"] == {"content": "hello channel"}               # only content
```

- [ ] **Step 2: Run to verify it fails**

Run: `python -m pytest tests/test_discord_sender.py -v`
Expected: FAIL — `send()` doesn't accept `embed`/`components` (TypeError).

- [ ] **Step 3: Replace `DiscordSender.send`**

In `switchboard/egress/discord.py`, replace the `send` method:
```python
    async def send(self, channel_id: str, content: str | None = None, *,
                   embed: dict | None = None,
                   components: list | None = None) -> httpx.Response:
        payload: dict = {}
        if content is not None:
            payload["content"] = content
        if embed is not None:
            payload["embeds"] = [embed]
        if components is not None:
            payload["components"] = components
        resp = await self._client.post(
            f"{DISCORD_API}/channels/{channel_id}/messages",
            headers={"Authorization": f"Bot {self._bot_token}"},
            json=payload,
        )
        resp.raise_for_status()
        return resp
```

- [ ] **Step 4: Run to verify it passes**

Run: `python -m pytest tests/test_discord_sender.py -v`
Expected: all pass (new embed/components test + plain-text backward-compat + the pre-existing channel/auth test).

- [ ] **Step 5: Commit**

```bash
git add switchboard/egress/discord.py tests/test_discord_sender.py
git commit -m "feat(discord): DiscordSender.send supports embeds + components"
```

---

## Task 3: Message formatter — `build_message` (embed + link buttons)

**Files:**
- Create: `switchboard/egress/github_notify.py`
- Create: `tests/test_github_notify.py`

**Interfaces:**
- Produces: `def build_message(kind: str, payload: dict) -> dict | None` returning `{"embed": dict, "components": list}` or `None`. Pure; stdlib only; imports nothing from the `switchboard.egress` package (no import cycle).

- [ ] **Step 1: Write the failing test**

`tests/test_github_notify.py`:
```python
from switchboard.egress.github_notify import build_message

_REPO = {"full_name": "yp/home", "html_url": "https://github.com/yp/home"}
_SENDER = {"login": "alice"}


def _pr_payload(**pr):
    base = {"number": 7, "title": "Add retry backoff", "html_url": "https://github.com/yp/home/pull/7"}
    base.update(pr)
    return {"repository": _REPO, "sender": _SENDER, "pull_request": base}


def _buttons(msg):
    # flatten the single action row to (label, url) pairs
    rows = msg["components"]
    if not rows:
        return []
    return [(b["label"], b["url"], b["style"]) for b in rows[0]["components"]]


def test_pr_opened_embed_and_buttons():
    msg = build_message("github.home.pr.opened", _pr_payload())
    e = msg["embed"]
    assert e["title"] == "🔀 PR #7 opened"
    assert e["url"] == "https://github.com/yp/home/pull/7"
    assert e["description"] == "**Add retry backoff**"
    assert e["color"] == 0x3B82F6
    assert e["author"] == {"name": "yp/home · alice"}
    assert _buttons(msg) == [
        ("View PR", "https://github.com/yp/home/pull/7", 5),
        ("View diff", "https://github.com/yp/home/pull/7/files", 5),
    ]


def test_pr_merged_is_purple():
    msg = build_message("github.home.pr.merged", _pr_payload())
    assert msg["embed"]["title"] == "🟣 PR #7 merged"
    assert msg["embed"]["color"] == 0x8B5CF6


def test_review_approved():
    payload = _pr_payload()
    payload["review"] = {"html_url": "https://github.com/yp/home/pull/7#pullrequestreview-1"}
    msg = build_message("github.home.review.approved", payload)
    assert msg["embed"]["title"] == "✅ Review approved · PR #7"
    assert msg["embed"]["color"] == 0x22C55E
    assert _buttons(msg) == [
        ("View review", "https://github.com/yp/home/pull/7#pullrequestreview-1", 5),
        ("View PR", "https://github.com/yp/home/pull/7", 5),
    ]


def test_issue_opened():
    payload = {"repository": _REPO, "sender": _SENDER,
               "issue": {"number": 12, "title": "Bug", "html_url": "https://github.com/yp/home/issues/12"}}
    msg = build_message("github.home.issue.opened", payload)
    assert msg["embed"]["title"] == "📝 Issue #12 opened"
    assert msg["embed"]["color"] == 0x22C55E
    assert _buttons(msg) == [("View issue", "https://github.com/yp/home/issues/12", 5)]


def test_check_run_failed_with_commit_button():
    payload = {"repository": _REPO, "sender": _SENDER,
               "check_run": {"name": "build", "html_url": "https://github.com/yp/home/runs/9",
                             "head_sha": "abc123", "check_suite": {"head_branch": "main"}}}
    msg = build_message("github.home.check_run.failed", payload)
    assert msg["embed"]["title"] == "🔴 CI failed: build"
    assert msg["embed"]["color"] == 0xEF4444
    assert _buttons(msg) == [
        ("View workflow run", "https://github.com/yp/home/runs/9", 5),
        ("View commit", "https://github.com/yp/home/commit/abc123", 5),
    ]


def test_check_run_succeeded_is_green():
    payload = {"repository": _REPO, "check_run": {"name": "build", "html_url": "https://x/9"}}
    msg = build_message("github.home.check_run.succeeded", payload)
    assert msg["embed"]["title"] == "✅ CI passed: build"
    assert msg["embed"]["color"] == 0x22C55E
    # no head_sha/html_url for repo commit -> "View commit" omitted, only the run button
    assert _buttons(msg) == [("View workflow run", "https://x/9", 5)]


def test_unrecognized_kind_returns_none():
    assert build_message("github.home.pr.locked", _pr_payload()) is None
    assert build_message("garbage", {}) is None


def test_malformed_payload_degrades_to_no_buttons_not_raise():
    # pr.opened with no pull_request object: still an embed, but no resolvable button urls
    msg = build_message("github.home.pr.opened", {"repository": _REPO})
    assert msg is not None
    assert msg["components"] == []            # both buttons omitted (no url)
    assert msg["embed"]["author"] == {"name": "yp/home"}   # no sender -> repo only
```

- [ ] **Step 2: Run to verify it fails**

Run: `python -m pytest tests/test_github_notify.py -v`
Expected: FAIL — module `switchboard.egress.github_notify` does not exist.

- [ ] **Step 3: Write `switchboard/egress/github_notify.py`**

```python
"""Pure GitHub-webhook -> Discord message formatter for the github->discord relay.

`build_message(kind, payload)` returns {"embed": {...}, "components": [...]} or
None. No I/O; reads payload fields defensively so a shape surprise degrades to a
button-less embed (or None for an unknown kind) rather than raising. Link buttons
are Discord components type 2 / style 5 (URL only, no interaction).
"""

_BLUE, _PURPLE, _GREY = 0x3B82F6, 0x8B5CF6, 0x6B7280
_GREEN, _RED, _YELLOW = 0x22C55E, 0xEF4444, 0xEAB308


def _link_button(label: str, url: str | None) -> dict | None:
    return {"type": 2, "style": 5, "label": label, "url": url} if url else None


def _row(*buttons) -> list:
    kept = [b for b in buttons if b is not None]
    return [{"type": 1, "components": kept}] if kept else []


def _bold(text) -> str | None:
    return f"**{text}**" if text else None


def _author(payload: dict) -> dict:
    repo = payload.get("repository", {})
    name = repo.get("full_name") or repo.get("name") or ""
    actor = payload.get("sender", {}).get("login")
    return {"name": f"{name} · {actor}" if actor else name}


def _embed(emoji, title, url, color, payload, description=None) -> dict:
    e = {"title": f"{emoji} {title}", "color": color, "author": _author(payload)}
    if url:
        e["url"] = url
    if description:
        e["description"] = description
    return e


_REVIEW = {
    "review.approved": ("✅", "approved", _GREEN),
    "review.changes_requested": ("🔴", "changes requested", _RED),
    "review.commented": ("💬", "commented", _GREY),
}
_PR = {
    "pr.opened": ("🔀", "opened", _BLUE),
    "pr.merged": ("🟣", "merged", _PURPLE),
    "pr.closed": ("🚫", "closed", _GREY),
}


def build_message(kind: str, payload: dict) -> dict | None:
    parts = kind.rsplit(".", 2)                    # tolerant of dotted repo names
    if len(parts) < 3:
        return None
    _, category, action = parts
    key = f"{category}.{action}"

    pr = payload.get("pull_request", {})
    issue = payload.get("issue", {})
    review = payload.get("review", {})
    check = payload.get("check_run", {})
    repo = payload.get("repository", {})

    pr_url = pr.get("html_url")
    pr_num = pr.get("number") or payload.get("number")

    if key in _PR:
        emoji, verb, color = _PR[key]
        return {
            "embed": _embed(emoji, f"PR #{pr_num} {verb}", pr_url, color, payload, _bold(pr.get("title"))),
            "components": _row(_link_button("View PR", pr_url),
                               _link_button("View diff", f"{pr_url}/files" if pr_url else None)),
        }

    if key == "review.requested":
        return {
            "embed": _embed("👀", f"Review requested · PR #{pr_num}", pr_url, _YELLOW, payload, _bold(pr.get("title"))),
            "components": _row(_link_button("View PR", pr_url)),
        }

    if key in _REVIEW:
        emoji, verb, color = _REVIEW[key]
        rurl = review.get("html_url")
        return {
            "embed": _embed(emoji, f"Review {verb} · PR #{pr_num}", rurl or pr_url, color, payload, _bold(pr.get("title"))),
            "components": _row(_link_button("View review", rurl), _link_button("View PR", pr_url)),
        }

    if key in ("issue.opened", "issue.closed"):
        emoji, color = ("📝", _GREEN) if action == "opened" else ("📕", _GREY)
        iurl = issue.get("html_url")
        return {
            "embed": _embed(emoji, f"Issue #{issue.get('number')} {action}", iurl, color, payload, _bold(issue.get("title"))),
            "components": _row(_link_button("View issue", iurl)),
        }

    if key in ("check_run.failed", "check_run.succeeded"):
        passed = action == "succeeded"
        emoji, verb, color = ("✅", "passed", _GREEN) if passed else ("🔴", "failed", _RED)
        name = check.get("name", "check")
        branch = check.get("check_suite", {}).get("head_branch")
        run_url = check.get("html_url")
        sha = check.get("head_sha")
        commit_url = f"{repo.get('html_url')}/commit/{sha}" if repo.get("html_url") and sha else None
        return {
            "embed": _embed(emoji, f"CI {verb}: {name}", run_url, color, payload,
                            f"branch `{branch}`" if branch else None),
            "components": _row(_link_button("View workflow run", run_url),
                               _link_button("View commit", commit_url)),
        }

    return None
```

- [ ] **Step 4: Run to verify it passes**

Run: `python -m pytest tests/test_github_notify.py -v`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add switchboard/egress/github_notify.py tests/test_github_notify.py
git commit -m "feat(github-notify): pure build_message — GitHub event -> Discord embed + link buttons"
```

---

## Task 4: `DiscordEgress` — sink refactor + `notify-github` handler

Relax the coarse filter so one egress serves multiple input sources; make the existing handler filters source-aware; add the relay handler (only when a notify channel is configured).

**Files:**
- Modify: `switchboard/egress/discord.py` (`DiscordEgress` only — leave `DiscordSender` from Task 2 intact)
- Modify: `tests/test_discord_egress.py`

**Interfaces:**
- Consumes: `build_message` (Task 3); `DiscordSender.send(embed=, components=)` (Task 2); `Handler`.
- Produces: `DiscordEgress(bot_token, application_id, *, notify_channel_id: str | None = None, client=None)` — `filter = None`; handlers `ping`/`echo` (now `source=="discord"`-gated) and, iff `notify_channel_id` set, `notify-github` (`filter = e.source=="github"`, posts an embed+buttons to the channel).

- [ ] **Step 1: Update the egress tests**

In `tests/test_discord_egress.py`: (a) update `_RecordingSender.send` to accept the new kwargs; (b) replace `test_discord_egress_shape`; (c) add source-awareness + notify-github tests. Apply these edits:

Replace the `_RecordingSender` class with:
```python
class _RecordingSender:
    def __init__(self):
        self.replies = []
        self.sends = []
    async def reply(self, token, content):
        self.replies.append((token, content))
    async def send(self, channel_id, content=None, *, embed=None, components=None):
        self.sends.append({"channel_id": channel_id, "content": content,
                           "embed": embed, "components": components})
```

Replace `test_discord_egress_shape` with:
```python
def _gh_event(kind="github.home.pr.opened"):
    return Event(id="G1", kind=kind, source="github", at=now_iso(),
                 payload={"repository": {"full_name": "yp/home", "html_url": "https://github.com/yp/home"},
                          "sender": {"login": "alice"},
                          "pull_request": {"number": 7, "title": "T", "html_url": "https://github.com/yp/home/pull/7"}},
                 meta={})


def test_egress_has_no_coarse_filter_and_ping_echo_present():
    eg = DiscordEgress("bot", "app")
    assert eg.name == "discord"
    assert eg.filter is None                       # sink: selection is per-handler
    names = [h.name for h in eg.handlers]
    assert "ping" in names and "echo" in names
    assert "notify-github" not in names            # no channel configured


def test_ping_echo_filters_are_source_aware():
    eg = DiscordEgress("bot", "app")
    ping = next(h for h in eg.handlers if h.name == "ping")
    echo = next(h for h in eg.handlers if h.name == "echo")
    assert ping.filter(_cmd_event(command="ping")) is True
    assert ping.filter(_gh_event()) is False       # a github event must not hit ping
    assert echo.filter(_gh_event()) is False


def test_notify_github_present_only_when_channel_set():
    eg = DiscordEgress("bot", "app", notify_channel_id="chan-1")
    notify = next(h for h in eg.handlers if h.name == "notify-github")
    assert notify.filter(_gh_event()) is True
    assert notify.filter(_cmd_event(command="ping")) is False   # discord event not relayed


def test_notify_github_posts_embed_and_buttons_to_channel():
    eg = DiscordEgress("bot", "app", notify_channel_id="chan-1")
    notify = next(h for h in eg.handlers if h.name == "notify-github")
    sender = _RecordingSender()
    ctx = Ctx(publish=None, egress=sender)
    asyncio.run(notify.handle(_gh_event("github.home.pr.opened"), ctx))
    assert len(sender.sends) == 1
    sent = sender.sends[0]
    assert sent["channel_id"] == "chan-1"
    assert sent["embed"]["title"] == "🔀 PR #7 opened"
    assert sent["components"][0]["components"][0]["label"] == "View PR"
    assert sender.replies == []


def test_notify_github_acks_unknown_kind_without_posting():
    eg = DiscordEgress("bot", "app", notify_channel_id="chan-1")
    notify = next(h for h in eg.handlers if h.name == "notify-github")
    sender = _RecordingSender()
    ctx = Ctx(publish=None, egress=sender)
    asyncio.run(notify.handle(_gh_event("github.home.pr.locked"), ctx))   # unrecognized -> None
    assert sender.sends == []
```

(Keep the existing `test_ping_handler_replies_via_followup` and the echo tests as-is — they still pass: `_cmd_event` is `source=="discord"`.)

- [ ] **Step 2: Run to verify it fails**

Run: `python -m pytest tests/test_discord_egress.py -v`
Expected: FAIL — `DiscordEgress` has no `notify_channel_id` param, `filter` is not `None`, no `notify-github` handler.

- [ ] **Step 3: Replace `DiscordEgress`**

In `switchboard/egress/discord.py`, replace the `DiscordEgress` class (keep `DiscordSender`, `DISCORD_API`, and the `from switchboard.egress import Handler` line above it), and add the `build_message` import:
```python
from switchboard.egress import Handler
from switchboard.egress.github_notify import build_message


class DiscordEgress:
    """The Discord output sink. `context()` hands handlers the one DiscordSender
    (reply + channel send). Handlers are routes into Discord, each with its own
    input filter: `ping`/`echo` react to Discord slash-command events; `notify-
    github` relays GitHub events as channel messages. The egress has no coarse
    filter — a sink serves multiple input sources, so selection is per-handler.
    """

    name = "discord"

    def __init__(self, bot_token: str, application_id: str, *,
                 notify_channel_id: str | None = None,
                 client: httpx.AsyncClient | None = None):
        self._sender = DiscordSender(bot_token, application_id, client=client)
        self._notify_channel_id = notify_channel_id
        self.filter = None                                  # sink: no coarse gate
        self.handlers = [
            Handler(name="ping",
                    filter=lambda e: e.source == "discord" and e.payload.get("command") == "ping",
                    handle=self._ping),
            Handler(name="echo",
                    filter=lambda e: e.source == "discord" and e.payload.get("command") == "echo",
                    handle=self._echo),
        ]
        if notify_channel_id:
            self.handlers.append(Handler(
                name="notify-github",
                filter=lambda e: e.source == "github",
                handle=self._notify,
            ))

    def context(self) -> DiscordSender:
        return self._sender

    async def _ping(self, event, ctx) -> None:
        await ctx.egress.reply(event.meta["interaction_token"], "pong (via the durable path)")

    async def _echo(self, event, ctx) -> None:
        message = event.payload.get("options", {}).get("message", "")
        await ctx.egress.reply(event.meta["interaction_token"], message)

    async def _notify(self, event, ctx) -> None:
        # relay a GitHub event to the notify channel as an embed + link buttons
        msg = build_message(event.kind, event.payload)
        if msg is None:
            return                                          # unrecognized kind: ack, no post
        await ctx.egress.send(self._notify_channel_id,
                              embed=msg["embed"], components=msg["components"])

    async def close(self) -> None:
        await self._sender.close()
```

- [ ] **Step 4: Run to verify it passes**

Run: `python -m pytest tests/test_discord_egress.py -v`
Expected: all pass (new + retained tests).

- [ ] **Step 5: Full suite (nothing else regressed)**

Run: `python -m pytest -q`
Expected: all pass. (Broker treats `filter=None` as "no coarse gate"; ping/echo behavior unchanged.)

- [ ] **Step 6: Commit**

```bash
git add switchboard/egress/discord.py tests/test_discord_egress.py
git commit -m "feat(discord): sink-oriented DiscordEgress + notify-github relay handler"
```

---

## Task 5: End-to-end integration — GitHub event → relay → channel POST

**Files:**
- Create: `tests/test_github_relay_integration.py`

**Interfaces:**
- Consumes: `Broker`, `DiscordEgress`, `EventInput`.

- [ ] **Step 1: Write the test**

`tests/test_github_relay_integration.py`:
```python
import asyncio
import json
import httpx
from switchboard.broker import Broker
from switchboard.egress.discord import DiscordEgress
from switchboard.event import EventInput


async def _wait_for(predicate, timeout=8.0):
    async def loop():
        while not predicate():
            await asyncio.sleep(0.01)
    await asyncio.wait_for(loop(), timeout)


async def test_github_pr_event_reaches_channel_with_embed_and_buttons(tmp_path):
    seen = {}

    def handler(request):
        seen["url"] = str(request.url)
        seen["auth"] = request.headers.get("authorization")
        seen["body"] = json.loads(request.content)
        return httpx.Response(200, json={})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    b = Broker(
        mamamia_db_path=str(tmp_path / "events.db"),
        switchboard_db_path=str(tmp_path / "sb.db"),
        wait_ms=50, reaper_interval=3600.0,
    )
    b.attach(DiscordEgress("bot-tok", "app-123", notify_channel_id="chan-9", client=client))
    await b.start()
    try:
        await b.publish(EventInput(
            kind="github.home.pr.opened", source="github",
            payload={"repository": {"full_name": "yp/home", "html_url": "https://github.com/yp/home"},
                     "sender": {"login": "alice"},
                     "pull_request": {"number": 7, "title": "Add retry backoff",
                                      "html_url": "https://github.com/yp/home/pull/7"}},
            dedupe_key="delivery-1",
            meta={"delivery": "delivery-1", "depth": "0"},
        ))
        await _wait_for(lambda: "url" in seen)
        assert seen["url"] == "https://discord.com/api/v10/channels/chan-9/messages"
        assert seen["auth"] == "Bot bot-tok"
        assert seen["body"]["embeds"][0]["title"] == "🔀 PR #7 opened"
        buttons = seen["body"]["components"][0]["components"]
        assert [(x["label"], x["url"]) for x in buttons] == [
            ("View PR", "https://github.com/yp/home/pull/7"),
            ("View diff", "https://github.com/yp/home/pull/7/files"),
        ]
    finally:
        await b.stop()
        await client.aclose()
```

- [ ] **Step 2: Run to verify it passes**

Run: `python -m pytest tests/test_github_relay_integration.py -v`
Expected: 1 passed. (Proves publish → durable log → lease → `notify-github` handler → channel POST with embed + buttons, no live Discord.)

- [ ] **Step 3: Full suite**

Run: `python -m pytest -q`
Expected: all pass.

- [ ] **Step 4: Commit**

```bash
git add tests/test_github_relay_integration.py
git commit -m "test(github-relay): PR event end-to-end through the durable path to a channel embed"
```

---

## Task 6: App wiring — `DISCORD_NOTIFY_CHANNEL_ID`

**Files:**
- Modify: `switchboard/app.py`
- Modify: `tests/test_app.py`

**Interfaces:**
- Produces: `build()` passes `notify_channel_id=config.get("discord_notify_channel_id")` into `DiscordEgress`; `run()` reads `DISCORD_NOTIFY_CHANNEL_ID` from env. The relay handler exists iff the channel id is set (and Discord is wired).

- [ ] **Step 1: Add the failing test**

Append to `tests/test_app.py`:
```python
def test_relay_handler_present_when_notify_channel_set(tmp_path):
    cfg = _base(tmp_path) | {
        "discord_bot_token": "bot-tok",
        "discord_application_id": "app-123",
        "discord_notify_channel_id": "chan-9",
    }
    broker, _ = build(cfg)
    discord = broker._egresses["discord"]
    assert "notify-github" in [h.name for h in discord.handlers]


def test_relay_handler_absent_without_notify_channel(tmp_path):
    cfg = _base(tmp_path) | {
        "discord_bot_token": "bot-tok",
        "discord_application_id": "app-123",
    }
    broker, _ = build(cfg)
    discord = broker._egresses["discord"]
    assert "notify-github" not in [h.name for h in discord.handlers]
```

- [ ] **Step 2: Run to verify it fails**

Run: `python -m pytest tests/test_app.py -v`
Expected: FAIL — `build()` doesn't pass `notify_channel_id`, so `notify-github` is never present.

- [ ] **Step 3: Wire it in `switchboard/app.py`**

In `build()`, change the `DiscordEgress` construction to pass the channel:
```python
        broker.attach(DiscordEgress(
            config["discord_bot_token"], app_id,
            notify_channel_id=config.get("discord_notify_channel_id"),
        ))
```
In `run()`'s `config` dict, add the env read:
```python
        "discord_notify_channel_id": os.environ.get("DISCORD_NOTIFY_CHANNEL_ID"),
```

- [ ] **Step 4: Run to verify it passes**

Run: `python -m pytest tests/test_app.py -v`
Expected: all pass.

- [ ] **Step 5: Full suite**

Run: `python -m pytest -q`
Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add switchboard/app.py tests/test_app.py
git commit -m "feat(app): wire the github->discord relay via DISCORD_NOTIFY_CHANNEL_ID"
```

---

## Final verification

- [ ] **Full suite**

Run: `. venv/bin/activate && python -m pytest -q`
Expected: all pass.

- [ ] **Manual live check (optional, needs a real bot + repo)** — set `DISCORD_BOT_TOKEN`/`DISCORD_APPLICATION_ID`/`DISCORD_GUILD_ID`/`DISCORD_NOTIFY_CHANNEL_ID`, ensure the bot has **Send Messages** in that channel (channel posts require it — unlike interaction followups), point a GitHub webhook at the ingress, and open/merge a PR or run CI — expect an embed with View buttons in the channel, plus the `github.<repo>.<...>` LoggerEgress line.

---

## Notes for the executor

- **The relay is a handler, not an egress.** It lives on `DiscordEgress` and shares its one `DiscordSender`. Do not create a `GitHubNotifyEgress`.
- **Coarse filter is now `None`.** The broker gate is `(egress.filter is None or egress.filter(e)) and handler.filter(e)`, so all selection is per-handler — which is why the `ping`/`echo` filters had to become `source=="discord"`-aware. Don't reintroduce a coarse `source=="discord"` gate; it would block `notify-github`.
- **`build_message` is pure and defensive.** Unknown kind → `None` (handler acks without posting). Missing button URL → that button omitted. Never let a payload-shape surprise raise inside the handler.
- **`kind.rsplit(".", 2)`** is deliberate — repo names can contain dots, so parse the category/action from the right.
- **Bot needs Send Messages** in the target channel for the live check; the connector's earlier `permissions=0` invite is insufficient for channel posts.
- **At-least-once:** the handler posts on every delivery; a rare in-broker retry after a partial success can double-post. Within spec.
```
