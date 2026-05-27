"""Tests for local.transforms.fhir_parser.FHIRBundleParser.

Covers every extract_* method plus reference-stripping edge cases. Uses the
synthetic tests/fixtures/sample_bundle.json fixture and small inline resources —
never real patient data.
"""

import pytest

from local.transforms.fhir_parser import (
    GENOMIC_DATA_LIMITATION,
    strip_reference,
)

# --------------------------------------------------------------------- helpers


@pytest.mark.parametrize(
    "reference, expected",
    [
        ("urn:uuid:abc-123", "abc-123"),
        ("Patient/abc-123", "abc-123"),
        ("abc-123", "abc-123"),
        ("Practitioner/prov-001", "prov-001"),
        ("", ""),
        (None, ""),
    ],
)
def test_strip_reference(reference, expected):
    assert strip_reference(reference) == expected


# --------------------------------------------------------------------- bundle


def test_parse_bundle_record_counts(parsed):
    assert len(parsed["patient"]) == 1
    assert len(parsed["encounter"]) == 2
    assert len(parsed["condition"]) == 1
    assert len(parsed["observation"]) == 5  # hr, bp, glucose, ecg-hr, ecg-rhythm
    assert len(parsed["medication_request"]) == 1
    assert len(parsed["procedure"]) == 1
    assert len(parsed["soap_note"]) == 2
    assert len(parsed["ecg_metadata"]) == 1
    assert len(parsed["imaging_study"]) == 1
    assert len(parsed["genomic_report"]) == 1


def test_parse_bundle_keys_present(parsed):
    expected = {
        "patient",
        "encounter",
        "condition",
        "observation",
        "medication_request",
        "procedure",
        "soap_note",
        "ecg_metadata",
        "imaging_study",
        "genomic_report",
    }
    assert set(parsed) == expected


def test_parse_empty_bundle(parser):
    out = parser.parse_bundle({"resourceType": "Bundle", "entry": []})
    assert all(records == [] for records in out.values())


# -------------------------------------------------------------------- patient


def test_extract_patient(parsed):
    p = parsed["patient"][0]
    assert p["patient_id"] == "patient-synthetic-001"
    assert p["birth_date"] == "1980-07-15"
    assert p["gender"] == "female"
    assert p["race"] == "White"
    assert p["ethnicity"] == "Hispanic or Latino"
    assert p["state"] == "MA"
    assert p["city"] == "Boston"
    assert p["zip"] == "02118"
    assert p["deceased"] is False
    assert p["deceased_date"] is None


def test_patient_deceased_boolean(parser):
    rec = parser.extract_patient({"id": "p1", "deceasedBoolean": True})
    assert rec["deceased"] is True


def test_patient_deceased_datetime_sets_flag(parser):
    rec = parser.extract_patient({"id": "p1", "deceasedDateTime": "1992-03-02T00:00:00-05:00"})
    assert rec["deceased"] is True
    assert rec["deceased_date"] == "1992-03-02T00:00:00-05:00"


def test_patient_missing_optional_fields(parser):
    """Missing address/extensions must not crash and yield None, not KeyError."""
    rec = parser.extract_patient({"id": "p1"})
    assert rec["patient_id"] == "p1"
    assert rec["state"] is None
    assert rec["race"] is None
    assert rec["deceased"] is False


# ------------------------------------------------------------------ encounter


def test_extract_encounter(parsed):
    enc = next(e for e in parsed["encounter"] if e["encounter_id"] == "encounter-001")
    assert enc["patient_id"] == "patient-synthetic-001"
    assert enc["type_code"] == "185349003"
    assert enc["type_display"] == "Encounter for check up (procedure)"
    assert enc["class_code"] == "AMB"
    assert enc["status"] == "finished"
    assert enc["provider_id"] == "prov-001"
    assert enc["reason_code"] == "162864005"
    assert enc["start_date"] == "2021-03-15T09:00:00-04:00"


def test_encounter_minimal(parsed):
    """encounter-002 has no type/participant/reason — fields default cleanly."""
    enc = next(e for e in parsed["encounter"] if e["encounter_id"] == "encounter-002")
    assert enc["type_code"] == ""
    assert enc["provider_id"] == ""
    assert enc["end_date"] is None


# ------------------------------------------------------------------ condition


def test_extract_condition(parsed):
    c = parsed["condition"][0]
    assert c["code"] == "59621000"
    assert c["display"] == "Essential hypertension (disorder)"
    assert c["clinical_status"] == "active"
    assert c["encounter_id"] == "encounter-001"
    assert c["onset_date"] == "2019-01-01T00:00:00-05:00"


def test_clinical_codes_are_strings(parsed):
    """SNOMED/LOINC/RxNorm codes must remain strings (no numeric coercion)."""
    assert isinstance(parsed["condition"][0]["code"], str)
    assert isinstance(parsed["observation"][0]["code"], str)
    assert isinstance(parsed["medication_request"][0]["code"], str)


# ---------------------------------------------------------------- observation


def test_extract_observation_scalar(parsed):
    hr = next(o for o in parsed["observation"] if o["observation_id"] == "obs-vital-hr")
    assert hr["code"] == "8867-4"
    assert hr["value"] == 72
    assert hr["unit"] == "/min"
    assert hr["category"] == "vital-signs"
    assert hr["components"] == []


def test_extract_observation_components(parsed):
    bp = next(o for o in parsed["observation"] if o["observation_id"] == "obs-bp")
    assert bp["value"] is None  # no top-level quantity; lives in components
    codes = {c["code"]: c["value"] for c in bp["components"]}
    assert codes == {"8480-6": 154, "8462-4": 85}


# ---------------------------------------------------------- medication / proc


def test_extract_medication_request(parsed):
    m = parsed["medication_request"][0]
    assert m["code"] == "243670"
    assert m["display"] == "Aspirin 81 MG Oral Tablet"
    assert m["status"] == "active"
    assert m["dosage_text"] == "Take 1 tablet daily"


def test_extract_procedure(parsed):
    pr = parsed["procedure"][0]
    assert pr["code"] == "5880005"
    assert pr["status"] == "completed"
    assert pr["performed_start"] == "2021-03-15T09:05:00-04:00"
    assert pr["performed_end"] == "2021-03-15T09:06:00-04:00"


# ------------------------------------------------------------------- ecg meta


def test_extract_ecg_metadata(parsed):
    ecg = parsed["ecg_metadata"][0]
    assert ecg["ecg_id"] == "ecg-001"
    assert ecg["heart_rate_bpm"] == 78
    assert isinstance(ecg["heart_rate_bpm"], int)
    assert ecg["rhythm"] == "Normal sinus rhythm"
    assert ecg["conclusion"].startswith("Normal sinus rhythm")
    assert ecg["pr_interval_ms"] is None  # not present in fixture


def test_ecg_not_classified_as_genomic(parsed):
    """The ECG report must not leak into genomic_report and vice versa."""
    genomic_ids = {g["report_id"] for g in parsed["genomic_report"]}
    assert "ecg-001" not in genomic_ids


# -------------------------------------------------------------- imaging study


def test_extract_imaging_study_fhir_only(parsed):
    img = parsed["imaging_study"][0]
    assert img["modality"] == "MR"
    assert img["body_site"] == "12738006"
    assert img["body_site_display"] == "Brain structure"
    assert img["series_count"] == 1
    assert img["dicom_extracted"] is False
    # DICOM-only fields are None until pixel-free header extraction runs.
    assert img["study_description"] is None
    assert img["rows"] is None


def test_imaging_study_signature_accepts_dicom_arg(parser, sample_bundle):
    """Calling with dicom_binary=None must behave as FHIR-only (no pydicom needed)."""
    study = next(
        e["resource"]
        for e in sample_bundle["entry"]
        if e["resource"]["resourceType"] == "ImagingStudy"
    )
    rec = parser.extract_imaging_study(study, dicom_binary=None)
    assert rec["dicom_extracted"] is False


# ----------------------------------------------------------- genomic report


def test_extract_genomic_report(parsed):
    g = parsed["genomic_report"][0]
    assert g["report_id"] == "genomic-001"
    assert g["gene_panel_name"] == "Genetic analysis master panel"
    assert g["has_pathogenic_variant"] is False  # "No pathogenic variants detected"
    assert g["family_history_flag"] is True
    assert g["binary_id"] == "binary-vcf-001"


def test_genomic_data_limitation_always_populated(parsed):
    """ADR-007: data_limitation is non-nullable and always the fixed note."""
    g = parsed["genomic_report"][0]
    assert g["data_limitation"] == GENOMIC_DATA_LIMITATION
    assert "Synthea simulated inheritance" in g["data_limitation"]


def test_genomic_pathogenic_positive(parser):
    report = {
        "resourceType": "DiagnosticReport",
        "id": "g2",
        "code": {"text": "Genetic analysis panel"},
        "conclusion": "Likely pathogenic variant identified in BRCA1.",
    }
    rec = parser.extract_genomic_report(report)
    assert rec["has_pathogenic_variant"] is True
    assert rec["data_limitation"] == GENOMIC_DATA_LIMITATION
