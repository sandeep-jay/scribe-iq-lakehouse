"""Tests for DICOM ingest + header extraction (ADR-006/013).

Uses tiny synthetic in-memory DICOM bytes (never a real 32 MB Coherent file, never a
committed binary). Covers: StudyInstanceUID linkage (filename <-> FHIR identifier),
placeholder normalization (UNKNOWN -> None), DA-date formatting, FHIR-authoritative
modality, the DicomIndex, the parse_bundle resolver path, and resilience to bad bytes.
"""

from __future__ import annotations

import io

import pytest

from core.ingest.dicom_index import DicomIndex, study_uid_from_filename
from core.transforms.fhir_parser import FHIRBundleParser, imaging_study_uid

pydicom = pytest.importorskip("pydicom")

# A Coherent-style DICOM file name: {given}_{family}_{patientUUID}{StudyInstanceUID}.dcm
STUDY_UID = "1.2.840.99999999.26401232.758647660200"
PATIENT_UUID = "b8dd1798-beef-094d-1be4-f90ee0e6b7d5"
DICOM_FILENAME = f"Abe604_Frami345_{PATIENT_UUID}{STUDY_UID}.dcm"


def _make_dicom(**tags) -> bytes:
    """Build minimal valid DICOM bytes with the given header tags."""
    from pydicom.dataset import FileDataset, FileMetaDataset
    from pydicom.uid import ExplicitVRLittleEndian, MRImageStorage, generate_uid

    meta = FileMetaDataset()
    meta.MediaStorageSOPClassUID = MRImageStorage
    meta.MediaStorageSOPInstanceUID = generate_uid()
    meta.TransferSyntaxUID = ExplicitVRLittleEndian
    ds = FileDataset(None, {}, file_meta=meta, preamble=b"\0" * 128)
    for key, value in tags.items():
        setattr(ds, key, value)
    buf = io.BytesIO()
    ds.save_as(buf, enforce_file_format=True)
    return buf.getvalue()


@pytest.fixture()
def dicom_bytes() -> bytes:
    """Synthetic DICOM mimicking Coherent: placeholder strings + real dimensions."""
    return _make_dicom(
        Modality="OT",
        StudyDescription="UNKNOWN",
        SeriesDescription="UNKNOWN",
        Manufacturer="UNKNOWN",
        StudyDate="19940115",
        SliceThickness=1.0,
        Rows=256,
        Columns=256,
    )


# ------------------------------------------------------------------ linkage


def test_study_uid_from_filename():
    assert study_uid_from_filename(DICOM_FILENAME) == STUDY_UID
    assert study_uid_from_filename("not_a_dicom.txt") == ""


def test_imaging_study_uid_from_resource():
    resource = {"identifier": [{"system": "urn:ietf:rfc:3986", "value": f"urn:oid:{STUDY_UID}"}]}
    assert imaging_study_uid(resource) == STUDY_UID
    assert imaging_study_uid({}) == ""


def test_filename_and_fhir_uids_match():
    resource = {"identifier": [{"value": f"urn:oid:{STUDY_UID}"}]}
    assert study_uid_from_filename(DICOM_FILENAME) == imaging_study_uid(resource)


# --------------------------------------------------------- header extraction


def test_headers_normalize_placeholders_and_dates(dicom_bytes):
    headers = FHIRBundleParser()._extract_dicom_headers(dicom_bytes)
    assert headers["study_description"] is None  # "UNKNOWN" -> None
    assert headers["series_description"] is None
    assert headers["manufacturer"] is None
    assert headers["study_date"] == "1994-01-15"  # DA YYYYMMDD -> ISO
    assert headers["rows"] == 256
    assert headers["columns"] == 256
    assert headers["slice_thickness_mm"] == 1.0
    assert "modality" not in headers  # FHIR stays authoritative for modality


def test_extract_imaging_study_merges_dicom(dicom_bytes):
    resource = {
        "id": "study-1",
        "series": [{"modality": {"code": "MR"}, "bodySite": {"code": "12738006"}}],
        "identifier": [{"value": f"urn:oid:{STUDY_UID}"}],
    }
    rec = FHIRBundleParser().extract_imaging_study(
        resource, dicom_binary=dicom_bytes, dicom_binary_id=STUDY_UID
    )
    assert rec["dicom_extracted"] is True
    assert rec["dicom_binary_id"] == STUDY_UID  # the UID, never the patient-named filename
    assert rec["modality"] == "MR"  # from FHIR, not the DICOM "OT"
    assert rec["rows"] == 256
    assert rec["study_description"] is None


def test_extract_imaging_study_fhir_only_when_no_dicom():
    resource = {"id": "study-2", "series": [{"modality": {"code": "DX"}}]}
    rec = FHIRBundleParser().extract_imaging_study(resource)
    assert rec["dicom_extracted"] is False
    assert rec["rows"] is None
    assert rec["modality"] == "DX"


def test_extract_imaging_study_survives_bad_dicom():
    resource = {"id": "study-3", "series": [{"modality": {"code": "MR"}}]}
    rec = FHIRBundleParser().extract_imaging_study(
        resource, dicom_binary=b"not a dicom file", dicom_binary_id=STUDY_UID
    )
    # Bad bytes are caught; FHIR metadata stands, DICOM flagged not-extracted.
    assert rec["dicom_extracted"] is False
    assert rec["modality"] == "MR"


# --------------------------------------------------------------- DicomIndex


def test_dicom_index_build_and_read(tmp_path, dicom_bytes):
    (tmp_path / DICOM_FILENAME).write_bytes(dicom_bytes)
    index = DicomIndex(tmp_path)
    assert len(index) == 1
    assert STUDY_UID in index
    assert index.read(STUDY_UID) == dicom_bytes
    assert index.read("9.9.9.not.present") is None


def test_dicom_index_missing_dir(tmp_path):
    index = DicomIndex(tmp_path / "nope")
    assert len(index) == 0
    assert index.read(STUDY_UID) is None


# ------------------------------------------------- parse_bundle resolver path


def test_parse_bundle_enriches_imaging_with_resolver(tmp_path, dicom_bytes, sample_bundle):
    # The fixture bundle has one ImagingStudy; give the index a matching .dcm.
    parser = FHIRBundleParser()
    studies = parser.parse_bundle(sample_bundle)["imaging_study"]
    uid = next(
        imaging_study_uid(e["resource"])
        for e in sample_bundle["entry"]
        if e["resource"].get("resourceType") == "ImagingStudy"
    )
    if not uid:
        pytest.skip("fixture ImagingStudy has no StudyInstanceUID")

    name = f"Given1_Family1_{PATIENT_UUID}{uid}.dcm"
    (tmp_path / name).write_bytes(dicom_bytes)
    index = DicomIndex(tmp_path)

    enriched = parser.parse_bundle(sample_bundle, dicom_resolver=index.read)["imaging_study"]
    assert len(enriched) == len(studies)
    matched = [r for r in enriched if r["dicom_extracted"]]
    assert len(matched) == 1
    assert matched[0]["rows"] == 256
    assert matched[0]["dicom_binary_id"] == uid


def test_parse_bundle_without_resolver_leaves_dicom_empty(sample_bundle):
    studies = FHIRBundleParser().parse_bundle(sample_bundle)["imaging_study"]
    assert all(r["dicom_extracted"] is False for r in studies)
