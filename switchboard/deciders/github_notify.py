"""Pure GitHub-webhook -> Discord message formatter for the github->discord relay.

`build_message(kind, payload)` returns {"embed": {...}, "components": [...]} or
None. No I/O; reads payload fields defensively so a shape surprise degrades to a
button-less embed (or None for an unknown kind) rather than raising. Link buttons
are Discord components type 2 / style 5 (URL only, no interaction).
"""
from switchboard.message import DecideCtx, Observation

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


class GitHubNotifyDecider:
    name = "github-notify"

    def __init__(self, channel_id: str):
        self.channel_id = channel_id

    def subscribes(self, obs: Observation) -> bool:
        return obs.name.startswith("github.")

    async def decide(self, obs: Observation, ctx: DecideCtx) -> None:
        msg = build_message(obs.name, obs.payload)
        if msg is None:
            return
        await ctx.command("discord.post", {
            "channel_id": self.channel_id,
            "embed": msg["embed"],
            "components": msg["components"],
        })
