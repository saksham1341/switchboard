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
