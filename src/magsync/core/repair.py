"""Repair stored titles that still carry a legacy source format tag.

One implementation serves the standalone ``magsync repair-titles`` command and
the same command executed by a running daemon or service, so both repair the
same rows, move the same files and remove the same emptied folders.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from magsync.core.diagnostics import sanitize_external_error
from magsync.core.models import DownloadStatus
from magsync.core.organizer import (
    normalize_title,
    organize_path,
    parse_date,
    strip_accents,
    strip_format_tag,
)


@dataclass
class RepairReport:
    """What a repair did (or, for a dry run, would do), line by line."""

    candidates: int = 0
    repaired: int = 0
    moved: int = 0
    collisions: int = 0
    missing: int = 0
    pruned: list[str] = field(default_factory=list)
    removed_dirs: int = 0
    # (kind, sanitized label, detail) — kinds: renamed, correct, missing,
    # collision, moved.
    lines: list[tuple[str, str, str]] = field(default_factory=list)

    def as_dict(self) -> dict:
        return {
            "candidates": self.candidates, "repaired": self.repaired, "moved": self.moved,
            "collisions": self.collisions, "missing": self.missing, "pruned": list(self.pruned),
            "removed_dirs": self.removed_dirs, "lines": [list(line) for line in self.lines],
        }


def repair_titles(index, output_dir: str | Path, *, dry_run: bool = False) -> RepairReport:
    """Strip legacy tags, re-derive what titles determine, and relocate files.

    Never overwrites an occupied destination, never guesses between two
    files, and is safe to re-run. With ``dry_run`` nothing is written.
    """
    output_dir = Path(output_dir).expanduser()
    report = RepairReport()
    candidates = [
        row
        for row in index.get_issues_with_tagged_titles()
        if strip_format_tag(row["title"] or "") != (row["title"] or "")
    ]
    report.candidates = len(candidates)
    touched_dirs: set[Path] = set()

    for row in candidates:
        new_title = strip_format_tag(row["title"])
        parsed = parse_date(new_title, row["page_url"] or "")
        norm = normalize_title(new_title)
        label = sanitize_external_error(new_title[:56])

        if not dry_run:
            magazine_id = index.get_or_create_magazine(norm, strip_accents(norm).lower())
            index.repair_issue_title(
                row["id"],
                title=new_title,
                magazine_id=magazine_id,
                year=parsed.year,
                month=parsed.month,
                date_raw=new_title,
            )
        report.repaired += 1

        old_path = Path(row["file_path"]) if row["file_path"] else None
        if row["download_status"] != DownloadStatus.COMPLETE.value or old_path is None:
            report.lines.append(("renamed", label, norm))
            continue

        new_path = organize_path(new_title, row["page_url"] or "", str(output_dir))
        if old_path == new_path:
            report.lines.append(("correct", label, ""))
            continue
        if not old_path.exists():
            report.missing += 1
            report.lines.append(("missing", label, ""))
            continue
        if new_path.exists():
            # Never guess which of two files is canonical.
            report.collisions += 1
            report.lines.append(("collision", label, ""))
            continue

        if not dry_run:
            new_path.parent.mkdir(parents=True, exist_ok=True)
            old_path.replace(new_path)
            index.set_download_file_path(row["id"], str(new_path))
        touched_dirs.add(old_path.parent)
        report.moved += 1
        report.lines.append(("moved", label, new_path.parent.name))

    if not dry_run:
        report.pruned = index.prune_empty_tagged_magazines()
        for directory in touched_dirs:
            try:
                if directory.is_dir() and not any(directory.iterdir()):
                    directory.rmdir()
                    report.removed_dirs += 1
            except OSError:
                pass  # best effort; a non-empty or busy directory is fine
    return report
