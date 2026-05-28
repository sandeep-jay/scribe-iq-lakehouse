"""Tests for SOAP-note extraction: Base64 decode, section detection, null handling.

Covers both attachment paths — inline ``attachment.data`` and a separate
``Binary`` resolved via ``attachment.url`` (ADR-005) — and the heuristic mapping
of Markdown / classic headers to S/O/A/P flags.
"""

import base64


def _b64(text: str) -> str:
    return base64.b64encode(text.encode("utf-8")).decode("ascii")


def _docref(attachment: dict, note_id: str = "n1") -> dict:
    return {
        "resourceType": "DocumentReference",
        "id": note_id,
        "subject": {"reference": "urn:uuid:patient-1"},
        "date": "2021-01-01T00:00:00-05:00",
        "context": {"encounter": [{"reference": "urn:uuid:enc-1"}]},
        "content": [{"attachment": attachment}],
    }


# ----------------------------------------------------------------- inline path


def test_inline_base64_decode(parser):
    note = parser.extract_soap_note(_docref({"data": _b64("Hello clinical note.")}))
    assert note["note_text"] == "Hello clinical note."
    assert note["patient_id"] == "patient-1"
    assert note["encounter_id"] == "enc-1"
    assert note["binary_id"] == ""


def test_classic_soap_markers_all_sections(parser):
    text = "SUBJECTIVE:\nx\nOBJECTIVE:\ny\nASSESSMENT:\nz\nPLAN:\nw\n"
    note = parser.extract_soap_note(_docref({"data": _b64(text)}))
    assert note["has_subjective"] is True
    assert note["has_objective"] is True
    assert note["has_assessment"] is True
    assert note["has_plan"] is True


def test_markdown_headers_no_objective(parser):
    """Coherent-style note: headers map to S/A/P, but no Objective section exists."""
    text = (
        "# Chief Complaint\nCough\n"
        "# History of Present Illness\n3 days\n"
        "# Assessment and Plan\nViral URI\n"
        "# Plan\nRest\n"
    )
    note = parser.extract_soap_note(_docref({"data": _b64(text)}))
    assert note["has_subjective"] is True
    assert note["has_objective"] is False
    assert note["has_assessment"] is True
    assert note["has_plan"] is True
    assert "chief complaint" in note["section_headers"]


def test_char_and_word_counts(parser):
    text = "one two three"
    note = parser.extract_soap_note(_docref({"data": _b64(text)}))
    assert note["char_count"] == len(text)
    assert note["word_count"] == 3


# ----------------------------------------------------------------- binary path


def test_binary_url_resolution(parser):
    binary_index = {
        "binary-9": {
            "resourceType": "Binary",
            "id": "binary-9",
            "data": _b64("Note from Binary resource."),
        }
    }
    note = parser.extract_soap_note(
        _docref({"url": "urn:uuid:binary-9"}), binary_index=binary_index
    )
    assert note["note_text"] == "Note from Binary resource."
    assert note["binary_id"] == "binary-9"


def test_binary_url_missing_from_index_returns_none(parser):
    note = parser.extract_soap_note(_docref({"url": "urn:uuid:missing"}), binary_index={})
    assert note is None


# ------------------------------------------------------------------- nulls


def test_no_attachment_data_returns_none(parser):
    assert parser.extract_soap_note(_docref({"contentType": "text/plain"})) is None


def test_undecodable_base64_returns_none(parser):
    note = parser.extract_soap_note(_docref({"data": "!!!not-valid-base64!!!"}))
    assert note is None


# ------------------------------------------------- bundle-level convenience API


def test_extract_soap_notes_from_bundle(parser, sample_bundle):
    notes = parser.extract_soap_notes_from_bundle(sample_bundle)
    assert len(notes) == 2
    ids = {n["note_id"] for n in notes}
    assert ids == {"docref-001", "docref-002"}


def test_no_phi_in_section_detection(parser):
    """Section detection works on note text alone; patient_id never drives it."""
    note = parser.extract_soap_note(_docref({"data": _b64("SUBJECTIVE:\nfine\n")}))
    assert note["has_subjective"] is True
    # patient_id is carried through but is not part of the detected sections.
    assert note["patient_id"] == "patient-1"
