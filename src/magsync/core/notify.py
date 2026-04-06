"""Notification support via Apprise."""

from __future__ import annotations

import logging
from collections import Counter

from magsync.config import NotificationSettings

logger = logging.getLogger("magsync")

# Apprise URL schemes that support HTML body (email-based transports)
_EMAIL_SCHEMES = ("mailto:", "mailtos:", "mailgun:", "ses:", "smtp:", "smtps:", "email:")


def send_download_summary(
    downloaded_issues: list[dict],
    settings: NotificationSettings,
) -> None:
    """Send a notification summarizing newly downloaded magazines.

    Uses HTML email template for email-based Apprise URLs and plain text for
    all other services (Slack, Discord, Gotify, ntfy, etc.).

    Does nothing if notifications are disabled or no Apprise URLs configured.
    Logs a warning and continues if sending fails.
    """
    if not settings.enabled or not settings.apprise_urls:
        return

    if not downloaded_issues:
        return

    try:
        import apprise
    except ImportError:
        logger.warning(
            "Apprise not installed. Install with: pip install magsync[notifications]"
        )
        return

    # Build plain text summary
    by_magazine: Counter[str] = Counter()
    for issue in downloaded_issues:
        title = issue.get("magazine_title") or issue.get("title", "Unknown")
        if " - " in title:
            title = title.split(" - ", 1)[0].strip()
        by_magazine[title] += 1

    total = len(downloaded_issues)
    breakdown = ", ".join(f"{name} ({count})" for name, count in by_magazine.most_common())
    plain_body = f"Downloaded {total} new issue{'s' if total != 1 else ''}: {breakdown}"
    title = f"magsync: {total} new download{'s' if total != 1 else ''}"

    # Split URLs into email vs non-email
    email_urls = [u for u in settings.apprise_urls if any(u.startswith(s) for s in _EMAIL_SCHEMES)]
    other_urls = [u for u in settings.apprise_urls if u not in email_urls]

    # Send plain text to non-email services
    if other_urls:
        ap = apprise.Apprise()
        for url in other_urls:
            ap.add(url)
        try:
            ap.notify(title=title, body=plain_body)
        except Exception as e:
            logger.warning(f"Failed to send notification: {e}")

    # Send HTML email to email services
    if email_urls:
        from magsync.core.email_template import render_download_email

        html_body = render_download_email(downloaded_issues)
        ap = apprise.Apprise()
        for url in email_urls:
            ap.add(url)
        try:
            ap.notify(
                title=title,
                body=html_body,
                body_format=apprise.NotifyFormat.HTML,
            )
        except Exception as e:
            logger.warning(f"Failed to send email notification: {e}")

    logger.info(f"Notification sent: {plain_body}")
