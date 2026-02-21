import os
import time
from typing import Any

import pyperclip
import requests
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# =====================================================================
# Configuration
# =====================================================================
GITHUB_REPO = "NSC-devllm/App"  # Target GitHub repository: "owner/name"
# If GITHUB_TOKEN exists, private repositories can also be accessed.
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
POLL_INTERVAL = 10  # Poll interval in seconds
CLIPBOARD_VERIFY_DELAY = 10  # Verify clipboard content N seconds after copy
CLIPBOARD_VERIFY_RETRIES = 2  # Additional retries if verification fails
ONLY_GEMINI_ISSUES = os.environ.get("ONLY_GEMINI_ISSUES", "true").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}

# Gemini-issued work selectors. At least one of these should match.
GEMINI_ISSUE_LABELS = {
    item.strip().lower()
    for item in os.environ.get(
        "GEMINI_ISSUE_LABELS", "ai-review,from-gemini"
    ).split(",")
    if item.strip()
}
GEMINI_TITLE_MARKERS = [
    item.strip().lower()
    for item in os.environ.get(
        "GEMINI_TITLE_MARKERS", "[ai review request],gemini"
    ).split(",")
    if item.strip()
]
GEMINI_AUTHOR_MARKERS = [
    item.strip().lower()
    for item in os.environ.get("GEMINI_AUTHOR_MARKERS", "gemini").split(",")
    if item.strip()
]


def get_latest_issues():
    """Fetch recent open issues from the GitHub API."""
    url = f"https://api.github.com/repos/{GITHUB_REPO}/issues"
    headers = {"Accept": "application/vnd.github.v3+json"}

    if GITHUB_TOKEN:
        headers["Authorization"] = f"token {GITHUB_TOKEN}"

    # state=open, sorted by creation time (newest first)
    params = {"state": "open", "sort": "created", "direction": "desc"}

    try:
        response = requests.get(url, headers=headers, params=params, timeout=10)
        if response.status_code == 200:
            return response.json()
        print(f"[WARN] GitHub API error: {response.status_code}")
        return []
    except requests.exceptions.RequestException as e:
        print(f"[WARN] Network error: {e}")
        return []


def _normalize_text(value: Any) -> str:
    return str(value or "").strip().lower()


def _extract_label_names(issue: dict[str, Any]) -> set[str]:
    labels = issue.get("labels", [])
    names: set[str] = set()
    if not isinstance(labels, list):
        return names

    for label in labels:
        if isinstance(label, dict):
            name = _normalize_text(label.get("name"))
        else:
            name = _normalize_text(label)
        if name:
            names.add(name)
    return names


def is_gemini_issue(issue: dict[str, Any]) -> tuple[bool, str]:
    title = _normalize_text(issue.get("title"))
    author = _normalize_text(issue.get("user", {}).get("login"))
    labels = _extract_label_names(issue)

    if GEMINI_ISSUE_LABELS and labels.intersection(GEMINI_ISSUE_LABELS):
        return True, "label"

    if any(marker in title for marker in GEMINI_TITLE_MARKERS):
        return True, "title"

    if any(marker in author for marker in GEMINI_AUTHOR_MARKERS):
        return True, "author"

    return False, ""


def build_chat_prompt(issue_number, title, body):
    """Build a Codex-ready prompt for a GitHub issue."""
    issue_body = body or "No description provided."
    return (
        "Please write code to resolve the following GitHub issue.\n"
        "Workflow policy:\n"
        "- Codex (GPT) handles code changes, commit, and push to main.\n"
        "- Gemini reads pushed commits and creates follow-up review issues.\n"
        "This task should be done with direct commit/push to the main branch (no Pull Request).\n"
        "After applying the code changes, also provide terminal commands "
        "(git add, git commit, git push origin main).\n\n"
        f"## [Issue #{issue_number}] {title}\n"
        f"{issue_body}\n"
    )


def copy_and_verify_prompt(chat_prompt):
    """
    Copy prompt to clipboard, wait for verification delay, and confirm content.
    Retry a few times if the clipboard content does not match.
    """
    max_attempts = CLIPBOARD_VERIFY_RETRIES + 1

    for attempt in range(1, max_attempts + 1):
        try:
            pyperclip.copy(chat_prompt)
        except pyperclip.PyperclipException as e:
            print(f"[WARN] Clipboard copy failed: {e}")
            return False

        print(
            f"[INFO] Clipboard copy attempt {attempt}/{max_attempts}. "
            f"Verifying in {CLIPBOARD_VERIFY_DELAY} seconds..."
        )
        time.sleep(CLIPBOARD_VERIFY_DELAY)

        try:
            copied_text = pyperclip.paste()
        except pyperclip.PyperclipException as e:
            print(f"[WARN] Clipboard read failed: {e}")
            return False

        if copied_text == chat_prompt:
            return True

        print(f"[WARN] Clipboard mismatch on attempt {attempt}/{max_attempts}.")

    return False


def main_loop():
    print("=" * 60)
    print(f"[WATCHER] Monitoring [{GITHUB_REPO}] every {POLL_INTERVAL} seconds...")
    print(
        f"[WATCHER] New issue prompts are verified after "
        f"{CLIPBOARD_VERIFY_DELAY} seconds."
    )
    if ONLY_GEMINI_ISSUES:
        print("[WATCHER] Gemini-only mode is ON.")
    print("Press Ctrl+C to stop.")
    print("=" * 60)

    # Store issue numbers that were already seen.
    seen_issues = set()

    # On startup, mark currently open issues as "seen"
    # to avoid notifications for old issues.
    initial_issues = get_latest_issues()
    for issue in initial_issues:
        if "pull_request" not in issue:
            seen_issues.add(issue.get("number"))

    print(f"[OK] Initialization complete. Skipping {len(seen_issues)} existing open issues.")

    while True:
        try:
            issues = get_latest_issues()

            for issue in issues:
                issue_number = issue.get("number")

                # The issues API also includes pull requests, so filter them out.
                if "pull_request" in issue:
                    continue

                if issue_number in seen_issues:
                    continue

                seen_issues.add(issue_number)
                title = issue.get("title")
                body = issue.get("body")

                if ONLY_GEMINI_ISSUES:
                    matched, reason = is_gemini_issue(issue)
                    if not matched:
                        print(f"[SKIP] #{issue_number} - {title} (not Gemini-issued)")
                        continue
                    print(f"[MATCH] #{issue_number} - {title} (matched by {reason})")
                else:
                    print(f"[NEW ISSUE] #{issue_number} - {title}")

                # Build a Codex-ready prompt and copy it to clipboard.
                chat_prompt = build_chat_prompt(issue_number, title, body)

                # Copy prompt and verify that clipboard keeps the same content.
                if copy_and_verify_prompt(chat_prompt):
                    print(
                        "[OK] Prompt copied and verified in clipboard. "
                        "Paste it into ChatGPT Pro (Codex) with Ctrl+V."
                    )
                else:
                    print(
                        "[WARN] Prompt copy could not be verified. "
                        "You can retry or copy manually from logs."
                    )

            # Wait until the next polling cycle.
            time.sleep(POLL_INTERVAL)

        except KeyboardInterrupt:
            print("\n[STOP] Monitoring stopped by user.")
            break
        except Exception as e:
            print(f"[WARN] Runtime error: {e}")
            time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main_loop()
