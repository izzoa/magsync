"""HTML email template for magsync download notifications."""

from __future__ import annotations

from collections import Counter
from datetime import datetime


def render_download_email(downloaded_issues: list[dict]) -> str:
    """Render an HTML email summarizing downloaded magazines."""
    # Group issues by magazine
    by_magazine: dict[str, list[dict]] = {}
    for issue in downloaded_issues:
        title = issue.get("magazine_title") or issue.get("title", "Unknown")
        if " - " in title:
            title = title.split(" - ", 1)[0].strip()
        by_magazine.setdefault(title, []).append(issue)

    total = len(downloaded_issues)
    mag_count = len(by_magazine)
    date_str = datetime.now().strftime("%B %d, %Y at %I:%M %p")

    # Build magazine rows
    magazine_rows = ""
    for mag_name, issues in sorted(by_magazine.items()):
        issue_items = ""
        for issue in issues:
            issue_title = issue.get("title", "Unknown issue")
            size = issue.get("file_size") or ""
            size_badge = f'<span style="color:#8b8fa3;font-size:12px;margin-left:8px;">{size}</span>' if size else ""
            issue_items += f"""
                <tr>
                    <td style="padding:6px 0 6px 20px;border-bottom:1px solid #f0f0f5;font-size:14px;color:#4a4a68;">
                        <span style="color:#22c55e;margin-right:6px;">&#10003;</span>
                        {issue_title}{size_badge}
                    </td>
                </tr>"""

        magazine_rows += f"""
            <tr>
                <td style="padding:16px 0 8px 0;">
                    <table width="100%" cellpadding="0" cellspacing="0" border="0">
                        <tr>
                            <td style="font-size:16px;font-weight:600;color:#1a1a2e;padding-bottom:4px;">
                                {mag_name}
                                <span style="background:#e0e7ff;color:#4338ca;font-size:11px;font-weight:600;padding:2px 8px;border-radius:10px;margin-left:8px;">
                                    {len(issues)} issue{"s" if len(issues) != 1 else ""}
                                </span>
                            </td>
                        </tr>
                        {issue_items}
                    </table>
                </td>
            </tr>"""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>magsync - New Downloads</title>
</head>
<body style="margin:0;padding:0;background-color:#f4f4f8;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,'Helvetica Neue',Arial,sans-serif;">
    <table width="100%" cellpadding="0" cellspacing="0" border="0" style="background-color:#f4f4f8;padding:24px 0;">
        <tr>
            <td align="center">
                <table width="600" cellpadding="0" cellspacing="0" border="0" style="max-width:600px;width:100%;">

                    <!-- Header -->
                    <tr>
                        <td style="background:linear-gradient(135deg,#1a1a2e 0%,#16213e 100%);padding:32px 40px;border-radius:12px 12px 0 0;">
                            <table width="100%" cellpadding="0" cellspacing="0" border="0">
                                <tr>
                                    <td>
                                        <span style="font-size:28px;font-weight:700;color:#ffffff;letter-spacing:-0.5px;">magsync</span>
                                    </td>
                                    <td align="right">
                                        <span style="font-size:12px;color:#8b8fa3;text-transform:uppercase;letter-spacing:1px;">Download Report</span>
                                    </td>
                                </tr>
                            </table>
                        </td>
                    </tr>

                    <!-- Summary Banner -->
                    <tr>
                        <td style="background-color:#ffffff;padding:28px 40px 20px;border-left:1px solid #e8e8ef;border-right:1px solid #e8e8ef;">
                            <table width="100%" cellpadding="0" cellspacing="0" border="0">
                                <tr>
                                    <td align="center" style="padding-bottom:20px;">
                                        <div style="background:#f0fdf4;border:1px solid #bbf7d0;border-radius:8px;padding:16px 24px;display:inline-block;">
                                            <span style="font-size:32px;font-weight:700;color:#15803d;">{total}</span>
                                            <span style="font-size:16px;color:#166534;margin-left:8px;">new issue{"s" if total != 1 else ""} downloaded</span>
                                        </div>
                                    </td>
                                </tr>
                                <tr>
                                    <td align="center">
                                        <table cellpadding="0" cellspacing="0" border="0">
                                            <tr>
                                                <td style="padding:0 16px;text-align:center;">
                                                    <div style="font-size:24px;font-weight:600;color:#1a1a2e;">{mag_count}</div>
                                                    <div style="font-size:12px;color:#8b8fa3;text-transform:uppercase;letter-spacing:0.5px;">Magazine{"s" if mag_count != 1 else ""}</div>
                                                </td>
                                                <td style="width:1px;background:#e8e8ef;padding:0;"></td>
                                                <td style="padding:0 16px;text-align:center;">
                                                    <div style="font-size:24px;font-weight:600;color:#1a1a2e;">{total}</div>
                                                    <div style="font-size:12px;color:#8b8fa3;text-transform:uppercase;letter-spacing:0.5px;">Issue{"s" if total != 1 else ""}</div>
                                                </td>
                                            </tr>
                                        </table>
                                    </td>
                                </tr>
                            </table>
                        </td>
                    </tr>

                    <!-- Divider -->
                    <tr>
                        <td style="background-color:#ffffff;padding:0 40px;border-left:1px solid #e8e8ef;border-right:1px solid #e8e8ef;">
                            <div style="border-top:1px solid #e8e8ef;"></div>
                        </td>
                    </tr>

                    <!-- Magazine List -->
                    <tr>
                        <td style="background-color:#ffffff;padding:12px 40px 28px;border-left:1px solid #e8e8ef;border-right:1px solid #e8e8ef;">
                            <table width="100%" cellpadding="0" cellspacing="0" border="0">
                                {magazine_rows}
                            </table>
                        </td>
                    </tr>

                    <!-- Footer -->
                    <tr>
                        <td style="background-color:#f8f8fc;padding:20px 40px;border-radius:0 0 12px 12px;border:1px solid #e8e8ef;border-top:none;">
                            <table width="100%" cellpadding="0" cellspacing="0" border="0">
                                <tr>
                                    <td style="font-size:12px;color:#8b8fa3;">
                                        {date_str}
                                    </td>
                                    <td align="right" style="font-size:12px;color:#8b8fa3;">
                                        magsync daemon
                                    </td>
                                </tr>
                            </table>
                        </td>
                    </tr>

                </table>
            </td>
        </tr>
    </table>
</body>
</html>"""
