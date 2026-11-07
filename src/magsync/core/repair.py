"""Library repairs: stored titles, and the metadata embedded in downloaded PDFs.

``repair_titles`` strips source presentation from stored titles: a legacy
leading format tag (``[PDF] ...``) or the source's trailing site suffix
(``... | Magazine PDF``).

``repair_metadata`` stamps (or re-stamps) accurate embedded metadata into
files that are already downloaded; see ``core/pdfmeta.py``.

One implementation of each serves the standalone command and the same command
executed by a running daemon or service, so both repair the same rows and
touch the same files.
"""

from __future__ import annotations

import hashlib
import os
import secrets
import shutil
import stat
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

from magsync.core import pdfmeta
from magsync.core.diagnostics import sanitize_external_error
from magsync.core.models import DownloadStatus
from magsync.core.organizer import (
    canonical_file,
    normalize_title,
    organize_path,
    parse_date,
    strip_accents,
    strip_source_decorations,
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
    """Strip source decorations, re-derive what titles determine, and relocate files.

    Never overwrites an occupied destination, never guesses between two
    files, and is safe to re-run. With ``dry_run`` nothing is written.
    """
    output_dir = Path(output_dir).expanduser()
    report = RepairReport()
    candidates = [
        row
        for row in index.get_issues_with_tagged_titles()
        if strip_source_decorations(row["title"] or "") != (row["title"] or "")
    ]
    report.candidates = len(candidates)
    touched_dirs: set[Path] = set()

    for row in candidates:
        new_title = strip_source_decorations(row["title"])
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


# --- repair-metadata -----------------------------------------------------------------

# Report kinds, in display order. Every considered file gets exactly one.
METADATA_KINDS = (
    "stamped", "current", "ambiguous", "edited", "modified", "unstampable", "missing", "busy", "failed",
)
_SKIPPED_KINDS = ("ambiguous", "edited", "modified", "unstampable", "missing", "busy")
PROGRESS_EVERY = 10
# Same length as the downloader's ".part" naming, so any name that fits on
# disk as a partial also fits as a staging file.
STAMPING_SUFFIX = ".stamp"


def new_token() -> str:
    """A per-run staging token (6 hex characters, like the downloader's)."""
    return secrets.token_hex(3)
# Headroom beyond the staged copy itself before a re-stamp may start.
_FREE_SPACE_MARGIN = 1024 * 1024


@dataclass
class MetadataRepairReport:
    """What a metadata repair did (or, for a dry run, would do)."""

    total: int = 0
    done: int = 0
    dry_run: bool = False
    interrupted: bool = False
    counts: dict[str, int] = field(default_factory=lambda: dict.fromkeys(METADATA_KINDS, 0))
    # (kind, sanitized label, detail)
    lines: list[tuple[str, str, str]] = field(default_factory=list)

    @property
    def outcome(self) -> str:
        return "partial" if self.counts["failed"] or self.interrupted else "succeeded"

    def add(self, kind: str, label: str, detail: str = "") -> None:
        self.counts[kind] += 1
        self.done += 1
        self.lines.append((kind, label, detail))

    def progress_line(self) -> str:
        skipped = sum(self.counts[kind] for kind in _SKIPPED_KINDS)
        verb = "would stamp" if self.dry_run else "stamped"
        return (
            f"repair-metadata: {self.done}/{self.total} files ({verb} {self.counts['stamped']}, "
            f"current {self.counts['current']}, skipped {skipped}, failed {self.counts['failed']})"
        )

    def as_dict(self) -> dict:
        return {
            "outcome": self.outcome, "total": self.total, "done": self.done, "dry_run": self.dry_run,
            "interrupted": self.interrupted, "counts": dict(self.counts),
            "lines": [list(line) for line in self.lines],
        }


@dataclass
class FileGroup:
    """One library file and every completed row that references it."""

    path: Path  # canonical
    rows: list[dict]

    @property
    def issue_ids(self) -> list[int]:
        return [row["issue_id"] for row in self.rows]

    @property
    def label(self) -> str:
        title = strip_source_decorations(self.rows[0]["title"] or "") or self.path.name
        return sanitize_external_error(title[:56])


@dataclass
class MetadataPlan:
    """The read-only outcome of examining one file, and what finalize must do.

    ``action``: ``none`` (report only), ``record`` (index fields only) or
    ``replace`` (swap in ``staging``, then record).
    """

    kind: str
    detail: str = ""
    action: str = "none"
    staging: Path | None = None
    sha256: str | None = None
    file_size_bytes: int | None = None
    source_sha256: str | None = None
    source_size: int | None = None
    fingerprint: str | None = None
    observed: tuple[int, int, int] | None = None


def metadata_groups(index, query: str | None = None) -> list[FileGroup]:
    """Candidate files, each expanded to every row that shares it.

    ``query`` selects files (by any referencing row's magazine), never rows: a
    file shared across magazines is judged and updated as one unit.
    """
    groups: dict[Path, list[dict]] = {}
    for row in index.complete_downloads_with_files():
        groups.setdefault(canonical_file(row["file_path"]), []).append(row)
    if query:
        needle = strip_accents(query).lower()
        groups = {
            path: rows for path, rows in groups.items()
            if any(needle in (row["normalized_title"] or "") for row in rows)
        }
    return [FileGroup(path, rows) for path, rows in groups.items()]


def _observe(path: Path) -> tuple[int, int, int]:
    st = path.stat()
    return st.st_size, st.st_mtime_ns, st.st_ino


def _unchanged_since_stamp(row: dict, fingerprint: str, observed: tuple[int, int, int]) -> bool:
    record = pdfmeta.parse_stamp_record(row.get("metadata_stamp"))
    return record is not None and record[0] == fingerprint and record[1:] == observed


def _single_fingerprint(rows: list[dict]) -> tuple[pdfmeta.StampSpec, str] | None:
    specs = [pdfmeta.stamp_spec_for(row) for row in rows]
    if not specs or any(spec is None for spec in specs):
        return None
    fingerprints = {pdfmeta.fingerprint(spec) for spec in specs}
    if len(fingerprints) != 1:
        return None
    return specs[0], fingerprints.pop()


def sweep_staging(path: Path) -> None:
    """Remove re-stamp staging files a crashed or cancelled run left beside ``path``.

    Matched by literal prefix and suffix: names may contain glob syntax
    (``PC Gamer [UK] ...``).
    """
    prefix = "." + path.name + "."
    for leftover in path.parent.iterdir():
        if leftover.name.startswith(prefix) and leftover.name.endswith(STAMPING_SUFFIX):
            leftover.unlink(missing_ok=True)


def prepare_file(group: FileGroup, *, verify: bool, dry_run: bool, token: str) -> MetadataPlan:
    """Examine one file and stage its re-stamp. Never touches the file itself.

    Safe to run in a worker thread: it reads the target, and at most writes a
    uniquely named hidden staging file beside it.
    """
    path = group.path
    try:
        observed = _observe(path)
    except FileNotFoundError:
        return MetadataPlan("missing")
    if not group.rows:
        return MetadataPlan("missing")
    chosen = _single_fingerprint(group.rows)
    if chosen is None:
        return MetadataPlan("ambiguous", "rows sharing this file disagree on its metadata")
    spec, fingerprint = chosen
    if not verify and all(_unchanged_since_stamp(row, fingerprint, observed) for row in group.rows):
        return MetadataPlan("current")
    if observed[0] > pdfmeta.STAMP_SIZE_LIMIT:
        return MetadataPlan("unstampable", pdfmeta.SKIP_OVERSIZED)
    data = path.read_bytes()
    if _observe(path) != observed:
        return MetadataPlan("busy", "changed while being read")

    current = hashlib.sha256(data).hexdigest()
    # Only a hash magsync recorded for bytes it stamped or fully verified
    # (the row carries a stamp record) can authorize truncating; any other
    # recorded hash (a legacy download, a file adopted as found) can only
    # confirm an unmarked file is unchanged.
    stamped_rows = [
        row for row in group.rows if row["sha256"] and pdfmeta.parse_stamp_record(row.get("metadata_stamp"))
    ]
    other_hashes = {
        row["sha256"] for row in group.rows
        if row["sha256"] and not pdfmeta.parse_stamp_record(row.get("metadata_stamp"))
    }
    trusted = {row["sha256"] for row in stamped_rows}
    source_row = next(
        (row for row in stamped_rows if row["sha256"] == current and row["source_sha256"] and row["source_size"]),
        None,
    )
    found = pdfmeta.resolve_original(
        data,
        trusted_sha256=current if current in trusted else (min(trusted) if trusted else None),
        legacy_sha256=current if current in other_hashes else (min(other_hashes) if other_hashes else None),
        recorded_source=(source_row["source_sha256"], source_row["source_size"]) if source_row else None,
        spec=spec,
    )
    if found.kind == "edited":
        return MetadataPlan("edited", "written by another tool")
    if found.kind == "modified":
        return MetadataPlan("modified", "changed outside magsync")

    def record(kind: str, detail: str = "", *, stamped: bool) -> MetadataPlan:
        return MetadataPlan(
            kind, detail, action="record", sha256=current, file_size_bytes=len(data),
            source_sha256=found.original_sha256, source_size=found.original_size,
            fingerprint=fingerprint if stamped else None, observed=observed,
        )

    if found.kind == "healed":
        return record("current", "healed", stamped=True)
    original = data[: found.original_size]
    outcome = pdfmeta.stamp_payload(original, spec, source_sha256=found.original_sha256)
    if outcome.appended is None:
        return record("unstampable", outcome.reason or "", stamped=False)
    stamped = original + outcome.appended
    if stamped == data:
        return record("current", stamped=True)
    if dry_run:
        return MetadataPlan("stamped", "would stamp")
    if shutil.disk_usage(path.parent).free < len(stamped) + _FREE_SPACE_MARGIN:
        return MetadataPlan("failed", "not enough free space")
    sweep_staging(path)
    staging = path.parent / f".{path.name}.{token}{STAMPING_SUFFIX}"
    try:
        with open(staging, "xb") as handle:
            handle.write(stamped)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(staging, stat.S_IMODE(os.stat(path).st_mode))
    except OSError:
        staging.unlink(missing_ok=True)
        raise
    return MetadataPlan(
        "stamped", action="replace", staging=staging, sha256=hashlib.sha256(stamped).hexdigest(),
        file_size_bytes=len(stamped), source_sha256=found.original_sha256,
        source_size=found.original_size, fingerprint=fingerprint, observed=observed,
    )


def _fsync_directory(directory: Path) -> None:
    try:
        fd = os.open(directory, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def finalize_file(
    index,
    group: FileGroup,
    plan: MetadataPlan,
    *,
    dry_run: bool,
    fence: Callable[[], None] | None = None,
    locked_ids: set[int] | None = None,
) -> str:
    """Apply a plan: re-check, replace, record. Contains no awaits.

    Inside a runtime owner this runs on the owner loop, so nothing can
    interleave between the re-checks, the replace and the index update; a
    cancelled task never gets here, so its staging file never replaces the
    target. ``locked_ids`` are the issues whose export publication the caller
    holds: a row that started sharing the file meanwhile is not covered, so
    the file is left for the next run rather than swapped under its export.
    """
    def discard() -> None:
        if plan.staging is not None:
            plan.staging.unlink(missing_ok=True)

    if dry_run or plan.action == "none":
        discard()
        return plan.kind
    path = group.path
    if fence is not None:
        try:
            fence()
        except BaseException:
            discard()  # ownership is gone: leave nothing behind
            raise
    try:
        if _observe(path) != plan.observed:
            discard()
            return "busy"
    except FileNotFoundError:
        discard()
        return "missing"
    # Re-query: a duplicate may have completed against this file meanwhile.
    # Also by the group's own spellings: a row may reach the file through a
    # differently named symlink.
    rows = index.downloads_for_file(path, names={Path(row["file_path"]).name for row in group.rows})
    if plan.action == "replace" and locked_ids is not None and not {row["issue_id"] for row in rows} <= locked_ids:
        discard()
        return "busy"
    if plan.fingerprint is not None:
        chosen = _single_fingerprint(rows)
        if chosen is None or chosen[1] != plan.fingerprint:
            discard()
            return "ambiguous"
    state = plan.observed
    if plan.action == "replace":
        try:
            os.replace(plan.staging, path)
        except OSError:
            discard()
            return "failed"
        _fsync_directory(path.parent)
        state = _observe(path)
    index.record_file_identity(
        [row["issue_id"] for row in rows],
        sha256=plan.sha256,
        file_size_bytes=plan.file_size_bytes,
        source_sha256=plan.source_sha256,
        source_size=plan.source_size,
        metadata_stamp=pdfmeta.stamp_record(plan.fingerprint, *state) if plan.fingerprint else None,
    )
    return plan.kind


@dataclass
class MetadataHooks:
    """Runtime-owner integration for the synchronous driver."""

    fence: Callable[[], None] | None = None
    should_stop: Callable[[], bool] | None = None


def repair_metadata(
    index,
    output_dir: str | Path,
    *,
    query: str | None = None,
    dry_run: bool = False,
    verify: bool = False,
    on_progress: Callable[[MetadataRepairReport], None] | None = None,
    hooks: MetadataHooks | None = None,
) -> MetadataRepairReport:
    """Stamp or re-stamp the embedded metadata of downloaded files.

    Safe to re-run: unchanged files are recognized as current without being
    read, a file magsync cannot prove it owns is never touched, and each
    replacement is atomic. With ``dry_run`` nothing in the library or the
    index is written. ``output_dir`` is accepted for parity with
    ``repair_titles``; files come from the index.
    """
    hooks = hooks or MetadataHooks()
    report = MetadataRepairReport(dry_run=dry_run)
    groups = metadata_groups(index, query)
    report.total = len(groups)
    token = new_token()
    for group in groups:
        if hooks.should_stop is not None and hooks.should_stop():
            report.interrupted = True
            break
        report.add(*run_one(index, group, verify=verify, dry_run=dry_run, token=token, fence=hooks.fence))
        if on_progress is not None and report.done % PROGRESS_EVERY == 0:
            on_progress(report)
    if on_progress is not None and report.done % PROGRESS_EVERY:
        on_progress(report)
    return report


def run_one(index, group: FileGroup, *, verify: bool, dry_run: bool, token: str, fence=None) -> tuple[str, str, str]:
    """Prepare and finalize one file synchronously; returns a report line."""
    try:
        plan = prepare_file(group, verify=verify, dry_run=dry_run, token=token)
        kind = finalize_file(index, group, plan, dry_run=dry_run, fence=fence)
        detail = plan.detail if kind == plan.kind else ""
    except Exception as exc:  # one file's failure must not end the run
        kind, detail = "failed", sanitize_external_error(str(exc))
    return kind, group.label, detail
