"""Stamp format version 2, and files magsync stamped as version 1 (0.10.x).

Every stamp magsync ever wrote stays provably magsync's: recognition rebuilds
the stamp in the version the file's marker names. New stamps are always the
current version, and repair-metadata upgrades anything older.
"""

from __future__ import annotations

import hashlib

import pytest
from pdf_factory import make_pdf, revise
from test_repair_metadata import TITLE, Library

from magsync.core import pdfmeta
from magsync.core.batch import _adopt_existing


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _spec():
    return pdfmeta.stamp_spec_for({"title": TITLE, "genre": "Travel"})


def _stamped(payload: bytes, version: int) -> bytes:
    outcome = pdfmeta.stamp_payload(payload, _spec(), version=version)
    assert outcome.reason is None, (outcome.reason, outcome.error)
    return payload + outcome.appended


@pytest.fixture
def lib(tmp_path):
    library = Library(tmp_path)
    yield library
    library.idx.close()


def _record_as_0_10_did(lib, issue_id, path, payload):
    """The row 0.10.x left after stamping: on-disk hash, source identity, a version-1 stamp record."""
    data = path.read_bytes()
    st = path.stat()
    record = pdfmeta.stamp_record(pdfmeta.fingerprint(_spec(), 1), st.st_size, st.st_mtime_ns, st.st_ino)
    lib.idx.conn.execute(
        "UPDATE downloads SET sha256=?, file_size_bytes=?, source_sha256=?, source_size=?, metadata_stamp=? "
        "WHERE issue_id=?",
        (_sha(data), len(data), _sha(payload), len(payload), record, issue_id),
    )
    lib.idx.conn.commit()


def test_current_version_is_2_and_markers_name_their_version():
    payload = make_pdf()
    assert pdfmeta.STAMP_VERSION == 2
    assert pdfmeta.inspect_pdf(_stamped(payload, 1)).marker.version == 1
    assert pdfmeta.inspect_pdf(_stamped(payload, 2)).marker.version == 2
    assert pdfmeta.stamp_payload(payload, _spec()).appended == _stamped(payload, 2)[len(payload):]


def test_version_1_file_is_upgraded_to_exactly_one_version_2_update(lib):
    payload = make_pdf()
    issue_id, path = lib.add(data=_stamped(payload, 1))
    _record_as_0_10_did(lib, issue_id, path, payload)
    # Unchanged size, mtime and inode: still not "current", because the record is version 1.
    report = lib.run()
    assert report.counts["stamped"] == 1, report.lines
    assert path.read_bytes() == _stamped(payload, 2)  # truncated to the original, one new update
    row = lib.row(issue_id)
    assert pdfmeta.parse_stamp_record(row["metadata_stamp"])[0] == pdfmeta.fingerprint(_spec(), 2)
    assert (row["source_sha256"], row["source_size"]) == (_sha(payload), len(payload))
    assert lib.run().counts["current"] == 1


def test_interrupted_version_1_restamp_heals_then_upgrades(lib):
    payload = make_pdf()
    issue_id, path = lib.add(data=payload)  # the index still records the legacy payload
    path.write_bytes(_stamped(payload, 1))  # 0.10.x replaced the file; its index update was lost
    report = lib.run()
    assert report.counts["stamped"] == 1, report.lines
    assert path.read_bytes() == _stamped(payload, 2)
    row = lib.row(issue_id)
    assert (row["sha256"], row["source_sha256"]) == (_sha(_stamped(payload, 2)), _sha(payload))
    assert lib.run().counts["current"] == 1


def test_interrupted_version_2_restamp_heals_as_current(lib):
    payload = make_pdf()
    _issue_id, path = lib.add(data=payload)
    path.write_bytes(_stamped(payload, 2))
    report = lib.run()
    assert report.counts["current"] == 1 and report.lines[0][2] == "healed"


def test_resolve_original_names_the_markers_version():
    payload = make_pdf()
    for version in (1, 2):
        data = _stamped(payload, version)
        found = pdfmeta.resolve_original(
            data, trusted_sha256=None, legacy_sha256=None, recorded_source=None, spec=_spec()
        )
        assert (found.kind, found.stamp_version, found.original_size) == ("healed", version, len(payload))


@pytest.mark.parametrize("version", [1, 2])
def test_adoption_records_the_fingerprint_of_the_version_on_disk(tmp_path, version):
    payload = make_pdf()
    dest = tmp_path / "AFAR.pdf"
    dest.write_bytes(_stamped(payload, version))
    result = _adopt_existing(dest, _spec())
    assert (result.source_sha256, result.source_size) == (_sha(payload), len(payload))
    assert pdfmeta.parse_stamp_record(result.metadata_stamp)[0] == pdfmeta.fingerprint(_spec(), version)


def test_unknown_marker_version_is_never_rebuilt():
    payload = make_pdf()
    data = _stamped(payload, 2).replace(
        b"<magsync:stampVersion>2</magsync:stampVersion>", b"<magsync:stampVersion>9</magsync:stampVersion>"
    )
    found = pdfmeta.resolve_original(data, trusted_sha256=None, legacy_sha256=None, recorded_source=None, spec=_spec())
    assert found.kind == "modified"


@pytest.mark.parametrize("version", [1, 2])
def test_another_tools_update_over_a_marked_file_matches_no_version(version):
    payload = make_pdf()
    edited = revise(_stamped(payload, version), {"/Producer": "some editor"})
    assert pdfmeta.inspect_pdf(edited).marker is not None  # the newest packet is still magsync's
    found = pdfmeta.resolve_original(
        edited, trusted_sha256=None, legacy_sha256=None, recorded_source=None, spec=_spec()
    )
    assert found.kind == "modified"
