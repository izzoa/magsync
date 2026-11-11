"""Stamping files that several index rows share (fix-ambiguous-shared-file-stamps).

The source posts one issue under several page URLs with cosmetically different
titles; content deduplication points every later row at the first file. Those
rows must agree when they differ only in source presentation, and still refuse
when they name different issues.
"""

from __future__ import annotations

import itertools
from pathlib import Path

import pytest
from bookorbit_emulation import bookorbit_view
from pdf_factory import make_pdf
from test_repair_metadata import Library

from magsync.core import pdfmeta
from magsync.core.organizer import (
    normalize_title,
    organize_path,
    strip_source_decorations,
)
from magsync.core.repair import choose_stamp

# --- 1. source decoration coverage -------------------------------------------------

DECORATED = (
    "Science News – July 2026 | Magazine PDF",
    "Science News – July 2026 | Download Magazine PDF",
    "Science News – July 2026 | Magazine [PDF]",
    "Science News – July 2026 | magazine (pdf)",
    "Science News – July 2026 - Magazine PDF",
    "Science News – July 2026 – Magazine PDF",
    "Science News – July 2026 | PDF",
    "Science News – July 2026 | Free Download",
    "Science News – July 2026 - | Free Download",
    "Science News – July 2026 -",
    "Science News – July 2026 | ",
    "Science News – July 2026 –",
)


@pytest.mark.parametrize("title", DECORATED)
def test_every_spelling_of_the_site_suffix_is_stripped(title):
    assert strip_source_decorations(title) == "Science News – July 2026"
    assert normalize_title(title) == "Science News"


def test_bare_titles_with_the_suffix_normalize_to_one_magazine():
    spellings = ("GQ USA Fall 2026 | Magazine PDF", "GQ USA Fall 2026 - Magazine PDF", "GQ USA Fall 2026 |")
    assert {normalize_title(title) for title in spellings} == {"GQ USA Fall"}


def test_two_spellings_of_one_issue_place_the_same_file():
    clean = organize_path("Science News – July 2026", "", "/lib")
    assert all(organize_path(title, "", "/lib") == clean for title in DECORATED)


@pytest.mark.parametrize(
    "title",
    (
        "PC Gamer [UK] – March 2026",
        "Wired [UK]",
        "Linux Format - Issue 300 - PDF Edition",
        "Spider-Man",
        "Make: Volume 90 (Summer)",
        "Mac|Life - Download",
        "Science News – Vol 208 No 05, May 2026",
        "[PDF] Airliner World – January 2024",
    ),
)
def test_publication_and_issue_detail_survive(title):
    expected = title.removeprefix("[PDF] ")
    assert strip_source_decorations(title) == expected


@pytest.fixture
def lib(tmp_path):
    library = Library(tmp_path)
    yield library
    library.idx.close()


def _named(lib, title, magazine=None):
    """Add a row whose title names its file, as a download would have placed it."""
    path = organize_path(title, "", str(lib.out))
    path.parent.mkdir(parents=True, exist_ok=True)
    data = make_pdf() if not path.exists() else None
    issue_id, _ = lib.add(title=title, magazine=magazine or normalize_title(title), data=data, path=path)
    return issue_id, path


def _parsed(path: Path) -> dict:
    return bookorbit_view(path.read_bytes())["parsed"]


def test_a_decorated_single_row_file_stamps_the_clean_title(lib):
    _, path = lib.add(title="Science News – July 2026 | Magazine [PDF]", magazine="Science News", data=make_pdf())
    assert lib.run().counts["stamped"] == 1
    parsed = _parsed(path)
    assert (parsed["title"], parsed["seriesName"]) == ("Science News – July 2026", "Science News")


# --- 2. equivalence ------------------------------------------------------------------


def _rows(*titles, genres=None, path="/lib/X/X.pdf"):
    genres = genres or [""] * len(titles)
    return [
        {"title": title, "genre": genre, "page_url": "", "year": None, "month": None, "file_path": path}
        for title, genre in zip(titles, genres)
    ]


@pytest.mark.parametrize(
    "variant",
    ("Aerospace – July 2022", "AEROSPACE - JULY 2022", "  Aerospace -  July 2022 "),
)
def test_presentation_variants_agree(variant):
    choice = choose_stamp(_rows("Aerospace - July 2022", variant), Path("/lib/X/X.pdf"))
    assert choice.spec is not None and choice.disagreement == ""


def test_an_em_dash_the_series_rule_cannot_split_on_still_refuses():
    # normalize_title has no em-dash separator, so the two rows would name
    # different series ("Aerospace" vs "Aerospace — July"): a real difference.
    choice = choose_stamp(_rows("Aerospace - July 2022", "Aerospace — July 2022"), Path("/x.pdf"))
    assert choice.disagreement == "rows sharing this file disagree on its series name"


def test_apostrophe_and_accent_variants_agree():
    rows = _rows("Cook’s Illustrated – January 2024", "Cook's Illustrated – January 2024")
    assert choose_stamp(rows, Path("/x.pdf")).spec is not None
    rows = _rows("Bon Appétit – March 2026", "Bon Appetit – March 2026")
    assert choose_stamp(rows, Path("/x.pdf")).spec is not None


def test_the_embedded_title_is_publication_text_not_the_folded_form(lib):
    _, path = _named(lib, "Aerospace – July 2022")
    lib.add(title="Aerospace - July 2022", magazine="Aerospace", path=path)
    assert lib.run().counts["stamped"] == 1
    parsed = _parsed(path)
    # Both rows name the file; the smaller exact text wins, as written.
    assert (parsed["title"], parsed["seriesName"]) == ("Aerospace - July 2022", "Aerospace")
    assert parsed["title"] != pdfmeta.comparison_key(pdfmeta.stamp_spec_for({"title": parsed["title"]}))["title"]


@pytest.mark.parametrize(
    ("other", "differs"),
    (
        ("Aerospace - August 2022", "title, date, series index"),
        ("Aerospace - July 2023", "title, date, series index"),
        ("Aerospace International - July 2022", "title, series name"),
    ),
)
def test_rows_naming_different_issues_still_refuse_and_say_why(lib, other, differs):
    payload = make_pdf()
    _, path = lib.add(title="Aerospace - July 2022", magazine="Aerospace", data=payload)
    lib.add(title=other, magazine="Aerospace", path=path)
    report = lib.run()
    assert report.counts["ambiguous"] == 1
    assert path.read_bytes() == payload
    (kind, _label, detail), = report.lines
    assert kind == "ambiguous" and detail == f"rows sharing this file disagree on its {differs}"


def test_a_row_without_a_usable_title_still_refuses():
    choice = choose_stamp(_rows("Aerospace - July 2022", ""), Path("/x.pdf"))
    assert choice.spec is None and "no usable title" in choice.disagreement


# --- 3. winner selection --------------------------------------------------------------


def test_the_row_that_names_the_file_wins(lib):
    _, path = _named(lib, "Cook’s Illustrated – January 2024")
    lib.add(title="Cook's Illustrated – January 2024", magazine="Cook's Illustrated", path=path)
    assert path.name.startswith("Cook’s Illustrated")
    assert lib.run().counts["stamped"] == 1
    parsed = _parsed(path)
    assert (parsed["title"], parsed["seriesName"]) == ("Cook’s Illustrated – January 2024", "Cook’s Illustrated")


def test_the_winner_moves_with_the_file_name(lib):
    # The other spelling names this file: its row wins instead.
    _, path = _named(lib, "Cook's Illustrated – January 2024")
    lib.add(title="Cook’s Illustrated – January 2024", magazine="Cook’s Illustrated", path=path)
    lib.run()
    assert _parsed(path)["title"] == "Cook's Illustrated – January 2024"


def test_without_a_naming_row_the_choice_ignores_row_order():
    titles = ("Aerospace – July 2022", "Aerospace - July 2022", "Aerospace – July 2022 | Magazine PDF")
    genres = ("", "Magazines and Newspapers", "Aviation")
    rows = _rows(*titles, genres=genres, path="/lib/elsewhere.pdf")
    choices = {choose_stamp(list(order), Path("/lib/elsewhere.pdf")) for order in itertools.permutations(rows)}
    assert len(choices) == 1
    (choice,) = choices
    assert choice.spec.genres == ("Magazines and Newspapers", "Aviation")
    assert choice.spec.title == "Aerospace - July 2022"  # smallest exact text, decoration stripped


def test_the_naming_choice_ignores_row_order_too():
    path = organize_path("Cook’s Illustrated – January 2024", "", "/lib")
    rows = _rows("Cook's Illustrated – January 2024", "Cook’s Illustrated – January 2024", path=str(path))
    picks = {choose_stamp(list(order), path).spec.title for order in itertools.permutations(rows)}
    assert picks == {"Cook’s Illustrated – January 2024"}


def test_a_second_run_over_a_shared_file_is_current(lib):
    _, path = _named(lib, "Aerospace – July 2022")
    lib.add(title="Aerospace - July 2022", magazine="Aerospace", genre="", path=path)
    assert lib.run().counts["stamped"] == 1
    stamped = path.read_bytes()
    assert lib.run().counts["current"] == 1
    assert lib.run(verify=True).counts["current"] == 1
    assert path.read_bytes() == stamped


# --- 4. genres ------------------------------------------------------------------------


def test_an_absent_genre_does_not_conflict(lib):
    _, path = _named(lib, "Aerospace – July 2022")
    lib.add(title="Aerospace – July 2022", magazine="Aerospace", genre="", path=path)
    assert lib.run().counts["stamped"] == 1
    assert _parsed(path)["genres"] == ["Travel"]


def test_different_genres_are_combined_without_duplicates():
    rows = _rows(
        "Aerospace - July 2022", "Aerospace - July 2022", "Aerospace - July 2022",
        genres=["Science, Aviation", "aviation, Business", ""],
    )
    choice = choose_stamp(rows, Path("/x.pdf"))
    # The winner's own order first (smallest exact text; ties keep the
    # smallest genres), then the rest sorted; "aviation" folds into "Aviation".
    assert choice.spec.genres == ("Aviation", "Business", "Science")


def test_no_genre_anywhere_stamps_with_none(lib):
    _, path = _named(lib, "Aerospace – July 2022")
    lib.idx.conn.execute("UPDATE issues SET genre=''")
    lib.idx.conn.commit()
    lib.add(title="Aerospace - July 2022", magazine="Aerospace", genre="", path=path)
    assert lib.run().counts["stamped"] == 1
    assert _parsed(path)["genres"] == []


def test_a_single_row_keeps_its_own_genre_order():
    choice = choose_stamp(_rows("AFAR – Spring 2025", genres=["Travel, Lifestyle"]), Path("/x.pdf"))
    assert choice.spec == pdfmeta.stamp_spec_for({"title": "AFAR – Spring 2025", "genre": "Travel, Lifestyle"})


# --- 5. regression shapes from the production index ------------------------------------


def test_production_hyphen_vs_en_dash_with_one_genre_absent(lib):
    _, path = _named(lib, "Aerospace - July 2022")
    lib.idx.conn.execute("UPDATE issues SET genre='Magazines and Newspapers'")
    lib.idx.conn.commit()
    lib.add(title="Aerospace – July 2022", magazine="Aerospace", genre="", path=path)
    assert lib.run().counts["stamped"] == 1
    parsed = _parsed(path)
    assert (parsed["title"], parsed["genres"]) == ("Aerospace - July 2022", ["Magazines and Newspapers"])


def test_production_identical_titles_differing_only_in_genre(lib):
    _, path = _named(lib, "Aerospace – July 2022")
    lib.add(title="Aerospace – July 2022", magazine="Aerospace", genre="Aviation", path=path)
    assert lib.run().counts["stamped"] == 1
    assert _parsed(path)["genres"] == ["Aviation", "Travel"]


def test_production_decorated_duplicate_beside_a_clean_one(lib):
    _, path = _named(lib, "Science News – July 2026")
    lib.add(title="Science News – July 2026 | Magazine [PDF]", magazine="Science News", path=path)
    assert lib.run().counts["stamped"] == 1
    assert _parsed(path)["title"] == "Science News – July 2026"


def test_production_issue_date_conflict_still_refuses(lib):
    payload = make_pdf()
    _, path = lib.add(title="Science News – July 2026", magazine="Science News", data=payload)
    lib.add(title="Science News – August 2026", magazine="Science News", path=path)
    assert lib.run().counts["ambiguous"] == 1
    assert path.read_bytes() == payload


def test_production_cross_record_name_variant_still_refuses(lib):
    payload = make_pdf()
    _, path = lib.add(title="Monocle International – May 2024", magazine="Monocle International", data=payload)
    lib.add(
        title="Monocle International Edition – May 2024", magazine="Monocle International Edition", path=path
    )
    assert lib.run().counts["ambiguous"] == 1
    assert path.read_bytes() == payload


# --- 6. version-2 vocabularies follow the winning row ------------------------------------


def _prism(path: Path) -> dict[str, str]:
    """The PRISM fields of the newest packet, read with pypdf and ElementTree (not pdfmeta)."""
    import io
    import xml.etree.ElementTree as ET

    from pypdf import PdfReader

    packet = PdfReader(io.BytesIO(path.read_bytes())).trailer["/Root"]["/Metadata"].get_data()
    prism = "{http://prismstandard.org/namespaces/basic/2.0/}"
    return {el.tag[len(prism):]: el.text for el in ET.fromstring(packet).iter() if el.tag.startswith(prism)}


@pytest.mark.parametrize(("naming", "other"), [("Aerospace – July 2022", "Aerospace – JULY 2022"),
                                               ("Aerospace – JULY 2022", "Aerospace – July 2022")])
def test_rows_differing_in_cover_text_style_stamp_once_with_the_winners(lib, naming, other):
    _, path = _named(lib, naming)
    lib.add(title=other, magazine="Aerospace", path=path)
    assert lib.run().counts["stamped"] == 1
    assert _prism(path)["coverDisplayDate"] == naming.partition(" – ")[2]
    assert lib.run().counts["current"] == 1
