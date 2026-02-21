import json
import os
import sys
import time
from typing import Any

import requests
from dotenv import load_dotenv

load_dotenv()

GITHUB_REPO = "NSC-devllm/App"
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
POLL_INTERVAL = 10

REVIEW_ISSUE_TITLE_PREFIX = os.environ.get(
    "REVIEW_ISSUE_TITLE_PREFIX", "[AI Review Request]"
)
REVIEW_ISSUE_LABELS = [
    item.strip()
    for item in os.environ.get(
        "REVIEW_ISSUE_LABELS", "ai-review,auto-generated,from-gemini"
    ).split(",")
    if item.strip()
]
REVIEW_TARGET = os.environ.get("REVIEW_TARGET", "Gemini")


def _github_headers() -> dict[str, str]:
    headers = {"Accept": "application/vnd.github.v3+json"}
    if GITHUB_TOKEN:
        headers["Authorization"] = f"token {GITHUB_TOKEN}"
    return headers


def _github_get(url: str, params: dict[str, Any] | None = None) -> dict[str, Any] | None:
    try:
        response = requests.get(
            url,
            headers=_github_headers(),
            params=params,
            timeout=10,
        )
        if response.status_code == 200:
            return response.json()
        print(f"[WARN] GitHub API error: {response.status_code}")
        return None
    except requests.exceptions.RequestException as exc:
        print(f"[WARN] Network error: {exc}")
        return None


def get_latest_commit() -> dict[str, Any] | None:
    url = f"https://api.github.com/repos/{GITHUB_REPO}/commits/main"
    return _github_get(url)


def get_commit_details(sha: str) -> dict[str, Any] | None:
    url = f"https://api.github.com/repos/{GITHUB_REPO}/commits/{sha}"
    return _github_get(url)


def get_open_review_issues() -> list[dict[str, Any]]:
    url = f"https://api.github.com/repos/{GITHUB_REPO}/issues"
    data = _github_get(
        url,
        params={
            "state": "open",
            "labels": "ai-review,from-gemini",
            "sort": "created",
            "direction": "desc",
            "per_page": 50,
        },
    )
    if isinstance(data, list):
        return data
    return []


def review_issue_exists(short_sha: str) -> bool:
    marker = f"`{short_sha}`"
    for issue in get_open_review_issues():
        body = issue.get("body") or ""
        if marker in body:
            return True
    return False


def format_file_summary(files: list[dict[str, Any]]) -> str:
    if not files:
        return "- (No file diff information)"

    lines: list[str] = []
    for changed in files:
        status = changed.get("status", "unknown")
        filename = changed.get("filename", "unknown")
        additions = changed.get("additions", 0)
        deletions = changed.get("deletions", 0)
        lines.append(f"- `{filename}` ({status}, +{additions}/-{deletions})")
    return "\n".join(lines)


def build_review_issue(commit_data: dict[str, Any], commit_details: dict[str, Any]) -> tuple[str, str]:
    sha = commit_data.get("sha", "")
    short_sha = sha[:7]
    commit_message = commit_data.get("commit", {}).get("message", "(no message)")
    author = commit_data.get("commit", {}).get("author", {}).get("name", "unknown")
    file_summary = format_file_summary(commit_details.get("files", []))

    title = f"{REVIEW_ISSUE_TITLE_PREFIX} {commit_message}"
    body = (
        "## Auto-generated code review request\n\n"
        "A new commit was pushed to `main`.\n\n"
        f"**Commit SHA:** `{short_sha}`\n"
        f"**Commit message:** {commit_message}\n"
        f"**Author:** {author}\n"
        "**source:** gemini-review-bot\n\n"
        "### Changed files\n"
        f"{file_summary}\n\n"
        "---\n"
        f"### Request to {REVIEW_TARGET} (reviewer)\n"
        "Please review the changes using the checklist below:\n"
        "1. Potential bugs or logic errors\n"
        "2. Security issues\n"
        "3. Code quality and readability\n"
        "4. Suggested next tasks\n"
    )
    return title, body


def create_github_issue(title: str, body: str, labels: list[str] | None = None) -> bool:
    if not GITHUB_TOKEN:
        print("[WARN] GITHUB_TOKEN is not set.")
        return False

    url = f"https://api.github.com/repos/{GITHUB_REPO}/issues"
    payload = {
        "title": title,
        "body": body,
        "labels": labels or REVIEW_ISSUE_LABELS,
    }

    try:
        response = requests.post(
            url,
            headers=_github_headers(),
            data=json.dumps(payload),
            timeout=10,
        )
        if response.status_code == 201:
            issue_url = response.json().get("html_url")
            print(f"[OK] Review issue created: {issue_url}")
            return True

        print(f"[WARN] Issue creation failed: {response.status_code}")
        print(response.text)
        return False
    except requests.exceptions.RequestException as exc:
        print(f"[WARN] Network error: {exc}")
        return False


def handle_commit(commit_data: dict[str, Any]) -> None:
    sha = commit_data.get("sha", "")
    if not sha:
        return

    short_sha = sha[:7]
    if review_issue_exists(short_sha):
        print(f"[SKIP] Review issue already exists for commit {short_sha}.")
        return

    details = get_commit_details(sha)
    if details is None:
        print(f"[WARN] Could not load commit details for {short_sha}.")
        return

    title, body = build_review_issue(commit_data, details)
    create_github_issue(title, body)


def main_loop() -> None:
    print("=" * 60)
    print(f"[CREATOR] Monitoring main commits in [{GITHUB_REPO}] every {POLL_INTERVAL}s.")
    print("[CREATOR] New commits will generate Gemini review issues.")
    print("=" * 60)

    latest = get_latest_commit()
    if latest is None:
        print("[WARN] Initial commit lookup failed.")
        last_seen_sha = ""
    else:
        last_seen_sha = latest.get("sha", "")
        print(f"[INIT] Latest commit: {last_seen_sha[:7] if last_seen_sha else '-'}")
        handle_commit(latest)

    while True:
        try:
            latest = get_latest_commit()
            if latest is not None:
                current_sha = latest.get("sha", "")
                if current_sha and current_sha != last_seen_sha:
                    print(f"[NEW COMMIT] {current_sha[:7]}")
                    handle_commit(latest)
                    last_seen_sha = current_sha
            time.sleep(POLL_INTERVAL)
        except KeyboardInterrupt:
            print("\n[STOP] Commit monitoring stopped by user.")
            break
        except Exception as exc:
            print(f"[WARN] Runtime error: {exc}")
            time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    if len(sys.argv) >= 3:
        issue_title = sys.argv[1]
        issue_body = sys.argv[2]
        create_github_issue(issue_title, issue_body)
    else:
        main_loop()
