from __future__ import annotations

import argparse
import json
import os
import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import requests


def send_with_smtp(subject: str, body_text: str, body_html: str) -> bool:
    user = os.getenv("FAILURE_EMAIL_USERNAME") or os.getenv("EMAIL_USERNAME")
    password = os.getenv("FAILURE_EMAIL_PASSWORD") or os.getenv("EMAIL_PASSWORD")
    recipient = os.getenv("FAILURE_EMAIL_TO") or os.getenv("EMAIL_TO")
    if not (user and password and recipient):
        return False

    message = MIMEMultipart("alternative")
    message["From"] = user
    message["To"] = recipient
    message["Subject"] = subject
    message.attach(MIMEText(body_text, "plain", "utf-8"))
    message.attach(MIMEText(body_html, "html", "utf-8"))

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(user, password)
        server.sendmail(user, [recipient], message.as_string())
    return True


def send_with_sendgrid(subject: str, body_html: str) -> bool:
    api_key = os.getenv("SENDGRID_API_KEY")
    recipient = os.getenv("FAILURE_EMAIL_TO") or os.getenv("EMAIL_TO")
    sender = os.getenv("FAILURE_EMAIL_USERNAME") or os.getenv("EMAIL_USERNAME") or "report@bot.local"
    if not (api_key and recipient):
        return False

    payload = {
        "personalizations": [{"to": [{"email": recipient}]}],
        "from": {"email": sender, "name": "00992A Tracker Bot"},
        "subject": subject,
        "content": [{"type": "text/html", "value": body_html}],
    }
    response = requests.post(
        "https://api.sendgrid.com/v3/mail/send",
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        data=json.dumps(payload).encode("utf-8"),
        timeout=30,
    )
    response.raise_for_status()
    return True


def upsert_github_issue(subject: str, body_markdown: str) -> bool:
    token = os.getenv("GITHUB_TOKEN")
    repository = os.getenv("GITHUB_REPOSITORY")
    if not (token and repository):
        return False

    session = requests.Session()
    session.headers.update(
        {
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
    )
    issues_url = f"https://api.github.com/repos/{repository}/issues"
    response = session.get(issues_url, params={"state": "open", "per_page": 100}, timeout=30)
    response.raise_for_status()
    open_issues = response.json()

    for issue in open_issues:
        if issue.get("title") == subject:
            comments_url = issue.get("comments_url")
            if comments_url:
                session.post(comments_url, json={"body": body_markdown}, timeout=30).raise_for_status()
                return True

    session.post(issues_url, json={"title": subject, "body": body_markdown}, timeout=30).raise_for_status()
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description="當自動化失敗時發送通知")
    parser.add_argument("--subject", required=True)
    parser.add_argument("--body", required=True)
    args = parser.parse_args()

    body_text = args.body
    body_html = "<br>".join(line for line in body_text.splitlines())
    body_markdown = args.body

    errors = []
    try:
        if send_with_smtp(args.subject, body_text, body_html):
            print("[notify] sent via smtp")
            return
    except Exception as exc:
        errors.append(str(exc))

    try:
        if send_with_sendgrid(args.subject, body_html):
            print("[notify] sent via sendgrid")
            return
    except Exception as exc:
        errors.append(str(exc))

    try:
        if upsert_github_issue(args.subject, body_markdown):
            print("[notify] opened or updated GitHub issue")
            return
    except Exception as exc:
        errors.append(str(exc))

    raise SystemExit("通知失敗: " + " | ".join(errors) if errors else "沒有可用的通知通道")


if __name__ == "__main__":
    main()
