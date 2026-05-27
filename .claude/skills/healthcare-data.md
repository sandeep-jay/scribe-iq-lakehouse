# Skill: Healthcare Data Patterns

Rules for working with clinical and health data.
Apply whenever touching FHIR, clinical notes, or patient records.

## PHI and data safety
1. Never assume PHI is absent — check data_limitation fields
2. Synthea data is synthetic — label it clearly everywhere
3. data_limitation is a first-class column, never a README footnote
4. Never log patient_id or encounter_id in plain text logs
5. Real PHI requires BAA, de-identification pipeline, audit logging
   Document this as production seam, never fake compliance

## FHIR resource handling
FHIR resources have OPTIONAL fields everywhere.
Always use .get() with a sensible default — never direct access.

```python
# Good
patient_id = resource.get("id", "")
birth_date = resource.get("birthDate", None)
conditions = resource.get("condition", [])

# Bad — crashes on missing optional fields
patient_id = resource["id"]
```

- FHIR UUIDs are strings — never cast to int or hash
- FHIR dates are strings — parse explicitly with dateutil or datetime
- FHIR references look like "Patient/abc-123" — strip the type prefix:
  `patient_id = reference.split("/")[-1]`
- One FHIR bundle per patient — iterate entries by resourceType

## Clinical code handling
SNOMED, LOINC, ICD codes are STRINGS — never cast to numeric.

```python
# Good — preserves leading zeros, special chars
condition_code: str = coding.get("code", "")

# Bad — destroys codes like "0001" or "E11.9"
condition_code: int = int(coding.get("code", 0))
```

Always store both code and display name.

Common LOINC codes:
  8867-4  = heart rate       8480-6  = systolic BP
  8462-4  = diastolic BP     8310-5  = body temperature
  59408-5 = O2 saturation    8302-2  = body height
  29463-7 = body weight

## FHIR Binary and Base64
Clinical notes in Synthea Coherent live in Binary FHIR resources.
Always decode with explicit encoding:

```python
import base64
note_text = base64.b64decode(binary_data).decode("utf-8")
```

Link Binary to patient via DocumentReference:
  DocumentReference.subject → patient_id
  DocumentReference.context.encounter → encounter_id
  DocumentReference.content[0].attachment → Binary reference

## DICOM handling
Always use stop_before_pixels=True for metadata extraction (ADR-006).

```python
import pydicom, io
ds = pydicom.dcmread(io.BytesIO(binary_bytes), stop_before_pixels=True)
```

Never load pixel data without explicit intent and GPU environment.

## Synthea-specific limitations
1. SOAP notes are template-driven — more regular than real clinical notes
2. Genomics models inheritance simulation — not clinical variants (ADR-007)
   Always: data_limitation = "Synthea simulated inheritance — not clinical variants"
3. ECG data is SBML-model-generated — not real waveforms
4. All data is synthetic — document this everywhere
5. Document limitations in model card, README, and data columns

## Age calculation
Always calculate age at time of encounter, not current date.

```python
from datetime import date
def age_at_encounter(birth_date: date, encounter_date: date) -> int:
    return int((encounter_date - birth_date).days / 365.25)
```

## Null handling in clinical data
Clinical data is full of nulls — handle gracefully everywhere.
Missing vitals ≠ normal vitals. Missing medication ≠ no medications.
Always distinguish "not recorded" from "not present".