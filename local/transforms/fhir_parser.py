"""Core FHIR R4 bundle parser for Synthea Coherent (engine-agnostic).

This module is pure Python: it operates on plain ``dict`` resources and has no
platform, Spark, or Delta imports (ADR-002). Every FHIR field is read with
``.get()`` and a sensible default — Synthea bundles have optional fields
everywhere (see ``.claude/rules/transforms.md`` and the healthcare-data skill).

Design notes derived from inspecting real Coherent bundles:
    * References use the ``urn:uuid:<id>`` form, not ``Type/<id>`` — see
      :func:`strip_reference`, which handles both.
    * SOAP notes are Base64 inside ``DocumentReference.content[].attachment.data``
      (inline), not a separate ``Binary`` resource (ADR-005).
    * Coherent SOAP notes use Markdown clinical headers ("# Chief Complaint",
      "# Assessment and Plan", ...), not literal "SUBJECTIVE:/OBJECTIVE:" markers.
      Section detection maps both vocabularies to the S/O/A/P flags and is
      explicitly heuristic. Coherent notes rarely contain an Objective section,
      so ``has_objective`` is frequently ``False`` — this is honest, not a bug.

The class returns plain dicts; conversion to ``pyarrow.Table`` happens in the
Silver transforms (Session 2), keeping this layer free of schema/Arrow concerns.

Logging policy: this is a hot path (called once per bundle, per resource), so it
logs sparingly — a per-bundle DEBUG summary of extracted counts and WARNINGs for
unrecoverable decode failures. Log messages never contain ``patient_id``,
``encounter_id``, note text, or any other identifier/PHI (CLAUDE.md security rules).
"""

from __future__ import annotations

import base64
import io
import logging
import re
from typing import Any

logger = logging.getLogger(__name__)

# Genomic limitation note — first-class data contract (ADR-007). Never None.
GENOMIC_DATA_LIMITATION = "Synthea simulated inheritance — not clinical variants"

# LOINC codes for ECG-linked observations (spec section 5.4).
LOINC_HEART_RATE = "8867-4"
LOINC_ECG_RHYTHM = "8893-1"
LOINC_PR_INTERVAL = "8625-6"
LOINC_QRS_DURATION = "8625-2"

# LOINC / category hints used to classify a DiagnosticReport.
_ECG_KEYWORDS = ("ecg", "ekg", "electrocardiogram", "electrocardiographic")
_GENOMIC_KEYWORDS = ("genetic", "genomic", "gene panel", "dna", "sequencing", "variant")

# Markdown / classic section headers mapped to the four SOAP buckets.
# Headers are matched case-insensitively after stripping leading '#' and whitespace.
_SUBJECTIVE_HEADERS = frozenset(
    {
        "subjective",
        "chief complaint",
        "history of present illness",
        "hpi",
        "social history",
        "allergies",
        "medications",
        "review of systems",
        "family history",
        "past medical history",
    }
)
_OBJECTIVE_HEADERS = frozenset(
    {
        "objective",
        "physical examination",
        "physical exam",
        "examination",
        "vital signs",
        "vitals",
        "laboratory",
        "labs",
        "results",
        "diagnostic results",
    }
)
_ASSESSMENT_HEADERS = frozenset(
    {
        "assessment",
        "impression",
        "diagnosis",
        "assessment and plan",
    }
)
_PLAN_HEADERS = frozenset(
    {
        "plan",
        "treatment",
        "treatment plan",
        "assessment and plan",
    }
)

# Matches a Markdown header line, capturing the header text.
_MD_HEADER_RE = re.compile(r"^#+\s*(.+?)\s*$", re.MULTILINE)
# Matches a classic "SUBJECTIVE:" style marker at a line start.
_CLASSIC_MARKER_RE = re.compile(
    r"^\s*(subjective|objective|assessment|plan)\s*:", re.IGNORECASE | re.MULTILINE
)


def strip_reference(reference: str | None) -> str:
    """Strip a FHIR reference down to its bare resource id.

    Handles the three forms seen in Coherent bundles:
        ``"urn:uuid:abc-123"`` -> ``"abc-123"``
        ``"Patient/abc-123"``  -> ``"abc-123"``
        ``"abc-123"``          -> ``"abc-123"``

    Args:
        reference: A FHIR reference string, or ``None``.

    Returns:
        The bare id, or ``""`` if ``reference`` is falsy.
    """
    if not reference:
        return ""
    if "/" in reference:
        return reference.rsplit("/", 1)[-1]
    if ":" in reference:
        return reference.rsplit(":", 1)[-1]
    return reference


def _first_coding(codeable: dict | None) -> dict:
    """Return the first ``coding`` entry of a CodeableConcept, or ``{}``."""
    if not codeable:
        return {}
    codings = codeable.get("coding") or []
    return codings[0] if codings else {}


def _code_and_display(codeable: dict | None) -> tuple[str, str]:
    """Return ``(code, display)`` as strings from a CodeableConcept.

    Clinical codes (SNOMED/LOINC/ICD/RxNorm) are always returned as strings,
    never cast to numeric (healthcare-data skill). Falls back to the
    CodeableConcept ``text`` for the display when no coding display exists.
    """
    coding = _first_coding(codeable)
    code = str(coding.get("code", "") or "")
    display = coding.get("display") or (codeable or {}).get("text") or ""
    return code, str(display)


def _coding_fields(coding: dict | None) -> tuple[str, str]:
    """Return ``(code, display)`` from a bare ``Coding`` dict (no ``coding`` wrapper).

    FHIR ``ImagingStudy.series.modality`` and ``.bodySite`` are bare Codings, with
    ``code``/``display`` at the top level rather than nested under ``coding[]``.
    """
    coding = coding or {}
    return str(coding.get("code", "") or ""), str(coding.get("display") or "")


def _ref_from(field: Any) -> str:
    """Strip an id from a reference field that may be a dict or a list of dicts.

    FHIR fields like ``DocumentReference.context.encounter`` are lists of
    references; ``Encounter.subject`` is a single reference dict.
    """
    if isinstance(field, list):
        field = field[0] if field else {}
    if isinstance(field, dict):
        return strip_reference(field.get("reference"))
    return ""


class FHIRBundleParser:
    """Parses a Synthea Coherent FHIR R4 bundle into typed record dicts.

    Each ``extract_*`` method takes a single FHIR resource dict and returns a flat
    record dict aligned with the corresponding Silver table schema (spec section 5.4).
    :meth:`parse_bundle` orchestrates the whole bundle, resolving cross-resource
    references (DiagnosticReport -> Observation, DocumentReference -> Binary).
    """

    # ------------------------------------------------------------------ bundle

    def parse_bundle(self, bundle_json: dict) -> dict[str, list[dict]]:
        """Parse a full bundle into ``{logical_table: [records]}``.

        Args:
            bundle_json: A parsed FHIR ``Bundle`` resource (dict).

        Returns:
            A dict keyed by logical Silver table name (``"patient"``,
            ``"encounter"``, ``"condition"``, ``"observation"``,
            ``"medication_request"``, ``"procedure"``, ``"soap_note"``,
            ``"ecg_metadata"``, ``"imaging_study"``, ``"genomic_report"``),
            each holding a list of extracted record dicts.
        """
        by_type, by_id = self._index_entries(bundle_json)
        out: dict[str, list[dict]] = {
            "patient": [],
            "encounter": [],
            "condition": [],
            "observation": [],
            "medication_request": [],
            "procedure": [],
            "soap_note": [],
            "ecg_metadata": [],
            "imaging_study": [],
            "genomic_report": [],
        }

        for resource in by_type.get("Patient", []):
            out["patient"].append(self.extract_patient(resource))
        for resource in by_type.get("Encounter", []):
            out["encounter"].append(self.extract_encounter(resource))
        for resource in by_type.get("Condition", []):
            out["condition"].append(self.extract_condition(resource))
        for resource in by_type.get("Observation", []):
            out["observation"].append(self.extract_observation(resource))
        for resource in by_type.get("MedicationRequest", []):
            out["medication_request"].append(self.extract_medication_request(resource))
        for resource in by_type.get("Procedure", []):
            out["procedure"].append(self.extract_procedure(resource))

        for resource in by_type.get("DocumentReference", []):
            note = self.extract_soap_note(resource, binary_index=by_id)
            if note is not None:
                out["soap_note"].append(note)

        for resource in by_type.get("ImagingStudy", []):
            out["imaging_study"].append(self.extract_imaging_study(resource))

        # DiagnosticReports are split by kind; other report types (lab panels, H&P
        # notes, death certificates) are intentionally not extracted to Silver here.
        for report in by_type.get("DiagnosticReport", []):
            if self._is_genomic_report(report):
                out["genomic_report"].append(self.extract_genomic_report(report))
            elif self._is_ecg_report(report):
                linked = self._resolve_results(report, by_id)
                out["ecg_metadata"].append(self.extract_ecg_metadata(report, linked))

        # One DEBUG line per bundle: counts only, never identifiers (see logging policy).
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("parse_bundle extracted %s", {k: len(v) for k, v in out.items()})
        return out

    @staticmethod
    def _index_entries(bundle_json: dict) -> tuple[dict[str, list[dict]], dict[str, dict]]:
        """Index bundle entries by resourceType and by resource id."""
        by_type: dict[str, list[dict]] = {}
        by_id: dict[str, dict] = {}
        for entry in bundle_json.get("entry", []) or []:
            resource = entry.get("resource") or {}
            rtype = resource.get("resourceType")
            if not rtype:
                continue
            by_type.setdefault(rtype, []).append(resource)
            rid = resource.get("id")
            if rid:
                by_id[rid] = resource
            # Bundle entries also carry a fullUrl (urn:uuid:...) used by references.
            full_url = entry.get("fullUrl")
            if full_url:
                by_id[strip_reference(full_url)] = resource
        return by_type, by_id

    @staticmethod
    def _resolve_results(report: dict, by_id: dict[str, dict]) -> list[dict]:
        """Resolve a DiagnosticReport's ``result`` references to Observation dicts."""
        resolved: list[dict] = []
        for ref in report.get("result", []) or []:
            rid = strip_reference(ref.get("reference"))
            if rid in by_id:
                resolved.append(by_id[rid])
        return resolved

    # --------------------------------------------------------------- patient

    def extract_patient(self, resource: dict) -> dict:
        """Extract demographics from a ``Patient`` resource (spec silver.patient)."""
        address = (resource.get("address") or [{}])[0]
        deceased_bool = resource.get("deceasedBoolean")
        deceased_dt = resource.get("deceasedDateTime")
        return {
            "patient_id": str(resource.get("id", "")),
            "birth_date": resource.get("birthDate"),
            "gender": resource.get("gender"),
            "race": self._us_core_extension(resource, "us-core-race"),
            "ethnicity": self._us_core_extension(resource, "us-core-ethnicity"),
            "state": address.get("state"),
            "city": address.get("city"),
            "zip": address.get("postalCode"),
            "deceased": bool(deceased_bool) or deceased_dt is not None,
            "deceased_date": deceased_dt,
        }

    @staticmethod
    def _us_core_extension(resource: dict, url_suffix: str) -> str | None:
        """Read a US Core race/ethnicity extension's display text.

        Prefers the nested ``text`` valueString, falling back to the
        ``ombCategory`` valueCoding display.
        """
        for ext in resource.get("extension", []) or []:
            if not ext.get("url", "").endswith(url_suffix):
                continue
            text_val = None
            omb_val = None
            for sub in ext.get("extension", []) or []:
                if sub.get("url") == "text":
                    text_val = sub.get("valueString")
                elif sub.get("url") == "ombCategory":
                    omb_val = (sub.get("valueCoding") or {}).get("display")
            return text_val or omb_val
        return None

    # ------------------------------------------------------------- encounter

    def extract_encounter(self, resource: dict) -> dict:
        """Extract an ``Encounter`` resource (spec silver.encounter)."""
        type_code, type_display = _code_and_display((resource.get("type") or [{}])[0])
        reason_code, reason_display = _code_and_display((resource.get("reasonCode") or [{}])[0])
        period = resource.get("period") or {}
        return {
            "encounter_id": str(resource.get("id", "")),
            "patient_id": _ref_from(resource.get("subject")),
            "type_code": type_code,
            "type_display": type_display,
            "class_code": str((resource.get("class") or {}).get("code", "") or ""),
            "start_date": period.get("start"),
            "end_date": period.get("end"),
            "status": resource.get("status"),
            "provider_id": self._encounter_provider(resource),
            "reason_code": reason_code,
            "reason_display": reason_display,
        }

    @staticmethod
    def _encounter_provider(resource: dict) -> str:
        """Return the first participant's individual reference id, if any."""
        for participant in resource.get("participant", []) or []:
            individual = participant.get("individual") or {}
            if individual.get("reference"):
                return strip_reference(individual["reference"])
        return ""

    # ------------------------------------------------------------- condition

    def extract_condition(self, resource: dict) -> dict:
        """Extract a ``Condition`` resource (spec silver.condition)."""
        code, display = _code_and_display(resource.get("code"))
        clinical_status = _first_coding(resource.get("clinicalStatus")).get("code", "")
        return {
            "condition_id": str(resource.get("id", "")),
            "patient_id": _ref_from(resource.get("subject")),
            "encounter_id": _ref_from(resource.get("encounter")),
            "code": code,
            "display": display,
            "clinical_status": str(clinical_status or ""),
            "onset_date": resource.get("onsetDateTime"),
            "recorded_date": resource.get("recordedDate"),
        }

    # ----------------------------------------------------------- observation

    def extract_observation(self, resource: dict) -> dict:
        """Extract an ``Observation`` resource (vitals/labs; spec silver.observation).

        Handles scalar ``valueQuantity``, coded/string values, and component-based
        observations (e.g. blood pressure has systolic/diastolic components).
        """
        code, display = _code_and_display(resource.get("code"))
        category = _first_coding((resource.get("category") or [{}])[0]).get("code", "")
        quantity = resource.get("valueQuantity") or {}
        value_string = resource.get("valueString")
        if value_string is None and "valueCodeableConcept" in resource:
            _, value_string = _code_and_display(resource.get("valueCodeableConcept"))

        components = []
        for comp in resource.get("component", []) or []:
            c_code, c_display = _code_and_display(comp.get("code"))
            c_qty = comp.get("valueQuantity") or {}
            components.append(
                {
                    "code": c_code,
                    "display": c_display,
                    "value": c_qty.get("value"),
                    "unit": c_qty.get("unit"),
                }
            )

        return {
            "observation_id": str(resource.get("id", "")),
            "patient_id": _ref_from(resource.get("subject")),
            "encounter_id": _ref_from(resource.get("encounter")),
            "code": code,
            "display": display,
            "category": str(category or ""),
            "value": quantity.get("value"),
            "unit": quantity.get("unit"),
            "value_string": value_string,
            "effective_date": resource.get("effectiveDateTime"),
            "components": components,
        }

    # ------------------------------------------------------ medication_request

    def extract_medication_request(self, resource: dict) -> dict:
        """Extract a ``MedicationRequest`` resource (spec silver.medication_request)."""
        code, display = _code_and_display(resource.get("medicationCodeableConcept"))
        dosage = (resource.get("dosageInstruction") or [{}])[0]
        return {
            "medication_request_id": str(resource.get("id", "")),
            "patient_id": _ref_from(resource.get("subject")),
            "encounter_id": _ref_from(resource.get("encounter")),
            "code": code,
            "display": display,
            "status": resource.get("status"),
            "intent": resource.get("intent"),
            "authored_on": resource.get("authoredOn"),
            "dosage_text": dosage.get("text"),
        }

    # ------------------------------------------------------------- procedure

    def extract_procedure(self, resource: dict) -> dict:
        """Extract a ``Procedure`` resource (spec silver.procedure)."""
        code, display = _code_and_display(resource.get("code"))
        period = resource.get("performedPeriod") or {}
        return {
            "procedure_id": str(resource.get("id", "")),
            "patient_id": _ref_from(resource.get("subject")),
            "encounter_id": _ref_from(resource.get("encounter")),
            "code": code,
            "display": display,
            "status": resource.get("status"),
            "performed_start": period.get("start"),
            "performed_end": period.get("end"),
        }

    # ------------------------------------------------------------- soap note

    def extract_soap_note(self, doc_ref: dict, binary_index: dict | None = None) -> dict | None:
        """Decode a SOAP note from a ``DocumentReference`` (spec silver.soap_note).

        The note text is Base64 either inline in ``content[].attachment.data`` or
        in a separate ``Binary`` resource referenced by ``attachment.url`` (ADR-005).

        Args:
            doc_ref: A ``DocumentReference`` resource.
            binary_index: Optional ``{id: resource}`` map for resolving Binary
                references. Required only when the attachment uses ``url``.

        Returns:
            A silver.soap_note record dict, or ``None`` if no decodable text exists.
        """
        attachment = (doc_ref.get("content") or [{}])[0].get("attachment") or {}
        binary_id = ""
        raw_b64 = attachment.get("data")

        if raw_b64 is None and attachment.get("url"):
            binary_id = strip_reference(attachment["url"])
            binary = (binary_index or {}).get(binary_id) or {}
            raw_b64 = binary.get("data")

        if not raw_b64:
            return None

        try:
            note_text = base64.b64decode(raw_b64).decode("utf-8")
        except (ValueError, UnicodeDecodeError):
            logger.warning("Failed to Base64-decode a DocumentReference attachment")
            return None

        context = doc_ref.get("context") or {}
        sections = self._parse_soap_sections(note_text)
        return {
            "note_id": str(doc_ref.get("id", "")),
            "patient_id": _ref_from(doc_ref.get("subject")),
            "encounter_id": _ref_from(context.get("encounter")),
            "note_date": doc_ref.get("date"),
            "note_text": note_text,
            "binary_id": binary_id,
            **sections,
        }

    @staticmethod
    def _parse_soap_sections(text: str) -> dict:
        """Heuristically detect S/O/A/P sections and size of a note.

        Recognizes both Markdown headers ("# Chief Complaint") and classic
        "SUBJECTIVE:" markers, mapping each to one or more S/O/A/P flags.
        Returns the four boolean flags plus char/word counts and the raw list of
        detected section headers (for debugging / data dictionary).
        """
        headers = [h.strip().lower() for h in _MD_HEADER_RE.findall(text)]
        markers = [m.strip().lower() for m in _CLASSIC_MARKER_RE.findall(text)]
        detected = set(headers) | set(markers)

        def _hit(bucket: frozenset[str]) -> bool:
            return any(h in bucket for h in detected)

        return {
            "has_subjective": _hit(_SUBJECTIVE_HEADERS),
            "has_objective": _hit(_OBJECTIVE_HEADERS),
            "has_assessment": _hit(_ASSESSMENT_HEADERS),
            "has_plan": _hit(_PLAN_HEADERS),
            "char_count": len(text),
            "word_count": len(text.split()),
            "section_headers": headers,
        }

    def extract_soap_notes_from_bundle(self, bundle_json: dict) -> list[dict]:
        """Extract all SOAP notes from a bundle (used by the Fabric Auto Loader UDF).

        Mirrors :meth:`parse_bundle` but returns only the soap_note records, so a
        Spark UDF can ``explode`` them per file (spec notebook 05).
        """
        _, by_id = self._index_entries(bundle_json)
        notes: list[dict] = []
        for entry in bundle_json.get("entry", []) or []:
            resource = entry.get("resource") or {}
            if resource.get("resourceType") == "DocumentReference":
                note = self.extract_soap_note(resource, binary_index=by_id)
                if note is not None:
                    notes.append(note)
        return notes

    # ------------------------------------------------------------- ecg meta

    @staticmethod
    def _is_ecg_report(report: dict) -> bool:
        """True if a DiagnosticReport looks like an ECG/EKG study."""
        _, display = _code_and_display(report.get("code"))
        haystack = display.lower()
        for cat in report.get("category", []) or []:
            _, cat_display = _code_and_display(cat)
            haystack += " " + cat_display.lower()
        return any(kw in haystack for kw in _ECG_KEYWORDS)

    def extract_ecg_metadata(self, diagnostic_report: dict, observations: list[dict]) -> dict:
        """Extract ECG metadata from a DiagnosticReport + linked Observations.

        Does NOT decode the Binary waveform signal — that is roadmap Phase 3.

        Args:
            diagnostic_report: The ECG ``DiagnosticReport`` resource.
            observations: ``Observation`` resources referenced by the report's
                ``result`` field (rhythm, heart rate, intervals).

        Returns:
            A silver.ecg_metadata record dict.
        """
        obs_by_code: dict[str, dict] = {}
        for obs in observations:
            code = _first_coding(obs.get("code")).get("code")
            if code:
                obs_by_code[str(code)] = obs

        def _qty(loinc: str) -> Any:
            return (obs_by_code.get(loinc, {}).get("valueQuantity") or {}).get("value")

        def _as_int(val: Any) -> int | None:
            return int(val) if isinstance(val, int | float) else None

        rhythm_obs = obs_by_code.get(LOINC_ECG_RHYTHM, {})
        _, rhythm = _code_and_display(rhythm_obs.get("valueCodeableConcept"))

        return {
            "ecg_id": str(diagnostic_report.get("id", "")),
            "patient_id": _ref_from(diagnostic_report.get("subject")),
            "encounter_id": _ref_from(diagnostic_report.get("encounter")),
            "report_date": diagnostic_report.get("effectiveDateTime"),
            "status": diagnostic_report.get("status"),
            "conclusion": diagnostic_report.get("conclusion"),
            "rhythm": rhythm or None,
            "heart_rate_bpm": _as_int(_qty(LOINC_HEART_RATE)),
            "pr_interval_ms": _as_int(_qty(LOINC_PR_INTERVAL)),
            "qrs_duration_ms": _as_int(_qty(LOINC_QRS_DURATION)),
            "has_waveform": bool(diagnostic_report.get("presentedForm")),
            "waveform_binary_id": self._presented_form_id(diagnostic_report),
        }

    @staticmethod
    def _presented_form_id(report: dict) -> str:
        """Return a presentedForm attachment url id, if present."""
        form = (report.get("presentedForm") or [{}])[0]
        return strip_reference(form.get("url")) if form.get("url") else ""

    # --------------------------------------------------------- imaging study

    def extract_imaging_study(self, resource: dict, dicom_binary: bytes | None = None) -> dict:
        """Extract imaging metadata (spec silver.imaging_study).

        Two-pass: FHIR ``ImagingStudy`` fields always, plus DICOM header fields
        when raw DICOM bytes are supplied. Never loads pixel data (ADR-006).

        Args:
            resource: An ``ImagingStudy`` resource.
            dicom_binary: Optional raw DICOM bytes for header extraction.

        Returns:
            A merged silver.imaging_study record dict.
        """
        fhir_meta = self._extract_imaging_from_fhir(resource)
        if dicom_binary:
            fhir_meta.update(self._extract_dicom_headers(dicom_binary))
            fhir_meta["dicom_extracted"] = True
        return fhir_meta

    @staticmethod
    def _extract_imaging_from_fhir(resource: dict) -> dict:
        """Pass 1 — fields available directly on the FHIR ImagingStudy."""
        series = (resource.get("series") or [{}])[0]
        modality_code, _ = _coding_fields(series.get("modality"))
        body_code, body_display = _coding_fields(series.get("bodySite"))
        return {
            "study_id": str(resource.get("id", "")),
            "patient_id": _ref_from(resource.get("subject")),
            "encounter_id": _ref_from(resource.get("encounter")),
            "started_date": resource.get("started"),
            "status": resource.get("status"),
            "modality": modality_code,
            "body_site": body_code,
            "body_site_display": body_display,
            "series_count": resource.get("numberOfSeries"),
            "instance_count": resource.get("numberOfInstances"),
            # DICOM header fields — populated only in pass 2.
            "study_description": None,
            "series_description": None,
            "study_date": None,
            "manufacturer": None,
            "magnetic_field_strength": None,
            "slice_thickness_mm": None,
            "rows": None,
            "columns": None,
            "dicom_binary_id": None,
            "dicom_extracted": False,
        }

    @staticmethod
    def _extract_dicom_headers(binary_data: bytes) -> dict:
        """Pass 2 — read DICOM header tags with ``stop_before_pixels=True`` (ADR-006).

        pydicom is imported lazily so this module loads even when pydicom is not
        installed (Session 1 fixtures have no DICOM). Pixel data is never read.

        Args:
            binary_data: Raw DICOM file bytes.

        Returns:
            A dict of header-derived fields. Codes/dates as strings, dimensions as
            ints, physical measurements as floats.
        """
        import pydicom  # lazy import — heavy, optional at module load

        ds = pydicom.dcmread(io.BytesIO(binary_data), stop_before_pixels=True)

        def _s(tag: str) -> str | None:
            val = getattr(ds, tag, None)
            return str(val) if val is not None else None

        def _i(tag: str) -> int | None:
            val = getattr(ds, tag, None)
            return int(val) if val is not None else None

        def _f(tag: str) -> float | None:
            val = getattr(ds, tag, None)
            return float(val) if val is not None else None

        return {
            "study_description": _s("StudyDescription"),
            "series_description": _s("SeriesDescription"),
            "study_date": _s("StudyDate"),
            "modality": _s("Modality") or None,
            "manufacturer": _s("Manufacturer"),
            "magnetic_field_strength": _f("MagneticFieldStrength"),
            "slice_thickness_mm": _f("SliceThickness"),
            "rows": _i("Rows"),
            "columns": _i("Columns"),
        }

    # ------------------------------------------------------- genomic report

    @staticmethod
    def _is_genomic_report(report: dict) -> bool:
        """True if a DiagnosticReport looks like a genetic/genomic report."""
        _, display = _code_and_display(report.get("code"))
        haystack = display.lower()
        for cat in report.get("category", []) or []:
            _, cat_display = _code_and_display(cat)
            haystack += " " + cat_display.lower()
        return any(kw in haystack for kw in _GENOMIC_KEYWORDS)

    def extract_genomic_report(self, resource: dict) -> dict:
        """Extract genomic report metadata only (spec silver.genomic_report, ADR-007).

        Synthea Coherent genomics models familial inheritance, not clinically
        actionable variants. The ``data_limitation`` field is always populated and
        is non-nullable — a first-class data contract, not a hidden footnote.

        Args:
            resource: A genomic ``DiagnosticReport`` resource.

        Returns:
            A silver.genomic_report record dict with ``data_limitation`` set.

        Raises:
            ValueError: If ``data_limitation`` somehow resolves to ``None``.
        """
        _, panel = _code_and_display(resource.get("code"))
        conclusion = resource.get("conclusion") or ""
        family_flag = self._family_history_flag(resource)
        record = {
            "report_id": str(resource.get("id", "")),
            "patient_id": _ref_from(resource.get("subject")),
            "encounter_id": _ref_from(resource.get("encounter")),
            "report_date": resource.get("effectiveDateTime"),
            "status": resource.get("status"),
            "gene_panel_name": panel or None,
            "result_summary": conclusion or None,
            "has_pathogenic_variant": self._detect_pathogenic(conclusion),
            "family_history_flag": family_flag,
            "binary_id": self._presented_form_id(resource),
            "data_limitation": GENOMIC_DATA_LIMITATION,
        }
        if record["data_limitation"] is None:  # invariant guard (ADR-007)
            raise ValueError("data_limitation must always be populated for genomic_report")
        return record

    @staticmethod
    def _detect_pathogenic(conclusion: str) -> bool:
        """Heuristically decide if a report conclusion asserts a pathogenic variant.

        Negation-aware: "No pathogenic variants detected" -> ``False``, while
        "Likely pathogenic variant in BRCA1" -> ``True``. This is a deliberately
        simple text heuristic over synthetic data, not a clinical classifier
        (ADR-007 — Synthea genomics are simulated inheritance, not real variants).
        """
        text = conclusion.lower()
        if "pathogenic" not in text:
            return False
        negations = (
            "no pathogenic",
            "non-pathogenic",
            "nonpathogenic",
            "negative for pathogenic",
            "without pathogenic",
            "no clinically significant",
        )
        return not any(neg in text for neg in negations)

    @staticmethod
    def _family_history_flag(resource: dict) -> bool:
        """Detect a family-history flag from extensions or conclusion text."""
        for ext in resource.get("extension", []) or []:
            if "family" in ext.get("url", "").lower():
                return bool(ext.get("valueBoolean", True))
        return "family history" in (resource.get("conclusion") or "").lower()
