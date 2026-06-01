"""Spark ``StructType`` schemas for the FHIR R4 Bundle shape Synthea Coherent emits.

Used with ``F.from_json(value, BUNDLE_SCHEMA)`` for schema-on-read parsing of
bundle JSON into structured Spark columns. The resource schema is a **union**
of every field any Silver transform extracts — Spark's PERMISSIVE parse mode
silently null-fills fields a given resource doesn't have, which is exactly
what we want for the heterogeneous bundle.

Two fields are intentionally typed against Encounter's shape:
    - ``class``: ``Coding`` (Encounter.class is a single Coding in R4)
    - ``type``:  ``ArrayType(CodeableConcept)`` (Encounter.type is an array)

DocumentReference.type is a single CodeableConcept, not an array — but we
never extract DocumentReference.type, so PERMISSIVE null-fill on parse is fine.
"""

from __future__ import annotations

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# ---------------------------------------------------------------- building blocks

CODING_SCHEMA = StructType(
    [
        StructField("system", StringType(), True),
        StructField("code", StringType(), True),
        StructField("display", StringType(), True),
    ]
)

CODEABLE_CONCEPT_SCHEMA = StructType(
    [
        StructField("coding", ArrayType(CODING_SCHEMA), True),
        StructField("text", StringType(), True),
    ]
)

REFERENCE_SCHEMA = StructType(
    [
        StructField("reference", StringType(), True),
        StructField("display", StringType(), True),
    ]
)

PERIOD_SCHEMA = StructType(
    [
        StructField("start", StringType(), True),
        StructField("end", StringType(), True),
    ]
)

QUANTITY_SCHEMA = StructType(
    [
        StructField("value", DoubleType(), True),
        StructField("unit", StringType(), True),
        StructField("system", StringType(), True),
        StructField("code", StringType(), True),
    ]
)

ADDRESS_SCHEMA = StructType(
    [
        StructField("line", ArrayType(StringType()), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("postalCode", StringType(), True),
        StructField("country", StringType(), True),
    ]
)

ATTACHMENT_SCHEMA = StructType(
    [
        StructField("contentType", StringType(), True),
        StructField("data", StringType(), True),  # Base64
        StructField("language", StringType(), True),
        StructField("url", StringType(), True),
    ]
)

# Patient extensions (race/ethnicity) — 2-level nested extension
INNER_EXTENSION_SCHEMA = StructType(
    [
        StructField("url", StringType(), True),
        StructField("valueCoding", CODING_SCHEMA, True),
        StructField("valueString", StringType(), True),
    ]
)

OUTER_EXTENSION_SCHEMA = StructType(
    [
        StructField("url", StringType(), True),
        StructField("extension", ArrayType(INNER_EXTENSION_SCHEMA), True),
        StructField("valueString", StringType(), True),
    ]
)

# Observation component (e.g. BP systolic / diastolic)
COMPONENT_SCHEMA = StructType(
    [
        StructField("code", CODEABLE_CONCEPT_SCHEMA, True),
        StructField("valueQuantity", QUANTITY_SCHEMA, True),
        StructField("valueString", StringType(), True),
    ]
)

# DocumentReference content + context
CONTENT_SCHEMA = StructType(
    [
        StructField("attachment", ATTACHMENT_SCHEMA, True),
        StructField("format", CODING_SCHEMA, True),
    ]
)

DOC_CONTEXT_SCHEMA = StructType(
    [
        StructField("encounter", ArrayType(REFERENCE_SCHEMA), True),
        StructField("period", PERIOD_SCHEMA, True),
    ]
)

# MedicationRequest dosage
DOSAGE_SCHEMA = StructType([StructField("text", StringType(), True)])

# Encounter participant
PARTICIPANT_SCHEMA = StructType([StructField("individual", REFERENCE_SCHEMA, True)])

# ImagingStudy series (we don't drill into instances at Silver — instance_count is a sum)
SERIES_SCHEMA = StructType(
    [
        StructField("uid", StringType(), True),
        StructField("number", IntegerType(), True),
        StructField("modality", CODING_SCHEMA, True),
        StructField("description", StringType(), True),
        StructField("bodySite", CODING_SCHEMA, True),
        StructField("numberOfInstances", IntegerType(), True),
    ]
)

# DiagnosticReport.presentedForm — the binary waveform reference for ECG
PRESENTED_FORM_SCHEMA = StructType(
    [
        StructField("contentType", StringType(), True),
        StructField("url", StringType(), True),
        StructField("data", StringType(), True),
    ]
)

# ----------------------------------------------------------------- union resource

# Every field any Silver transform extracts. PERMISSIVE parse mode null-fills
# fields a given resource doesn't have. Resources we extract:
#   Patient · Encounter · Observation · Condition · Procedure ·
#   MedicationRequest · DocumentReference · DiagnosticReport · ImagingStudy
RESOURCE_SCHEMA = StructType(
    [
        StructField("resourceType", StringType(), True),
        StructField("id", StringType(), True),
        # Patient
        StructField("birthDate", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("deceasedDateTime", StringType(), True),
        StructField("deceasedBoolean", BooleanType(), True),
        StructField("address", ArrayType(ADDRESS_SCHEMA), True),
        StructField("extension", ArrayType(OUTER_EXTENSION_SCHEMA), True),
        # Encounter
        StructField("status", StringType(), True),
        StructField("class", CODING_SCHEMA, True),
        StructField("type", ArrayType(CODEABLE_CONCEPT_SCHEMA), True),
        StructField("period", PERIOD_SCHEMA, True),
        StructField("participant", ArrayType(PARTICIPANT_SCHEMA), True),
        StructField("reasonCode", ArrayType(CODEABLE_CONCEPT_SCHEMA), True),
        # Cross-resource references
        StructField("subject", REFERENCE_SCHEMA, True),
        StructField("encounter", REFERENCE_SCHEMA, True),
        # Clinical (Condition, Observation, Procedure, DiagnosticReport)
        StructField("code", CODEABLE_CONCEPT_SCHEMA, True),
        StructField("clinicalStatus", CODEABLE_CONCEPT_SCHEMA, True),
        StructField("onsetDateTime", StringType(), True),
        StructField("abatementDateTime", StringType(), True),
        StructField("recordedDate", StringType(), True),
        StructField("effectiveDateTime", StringType(), True),
        StructField("issued", StringType(), True),
        StructField("performedDateTime", StringType(), True),
        StructField("performedPeriod", PERIOD_SCHEMA, True),
        StructField("valueQuantity", QUANTITY_SCHEMA, True),
        StructField("valueString", StringType(), True),
        StructField("component", ArrayType(COMPONENT_SCHEMA), True),
        StructField("category", ArrayType(CODEABLE_CONCEPT_SCHEMA), True),
        StructField("conclusion", StringType(), True),
        StructField("result", ArrayType(REFERENCE_SCHEMA), True),
        StructField("presentedForm", ArrayType(PRESENTED_FORM_SCHEMA), True),
        # MedicationRequest
        StructField("medicationCodeableConcept", CODEABLE_CONCEPT_SCHEMA, True),
        StructField("authoredOn", StringType(), True),
        StructField("intent", StringType(), True),
        StructField("dosageInstruction", ArrayType(DOSAGE_SCHEMA), True),
        # DocumentReference (SOAP notes)
        StructField("date", StringType(), True),
        StructField("content", ArrayType(CONTENT_SCHEMA), True),
        StructField("context", DOC_CONTEXT_SCHEMA, True),
        # ImagingStudy
        StructField("started", StringType(), True),
        StructField("modality", ArrayType(CODING_SCHEMA), True),
        StructField("numberOfSeries", IntegerType(), True),
        StructField("numberOfInstances", IntegerType(), True),
        StructField("series", ArrayType(SERIES_SCHEMA), True),
        StructField("description", StringType(), True),
    ]
)

# Bundle wrapper
ENTRY_SCHEMA = StructType(
    [
        StructField("fullUrl", StringType(), True),
        StructField("resource", RESOURCE_SCHEMA, True),
    ]
)

BUNDLE_SCHEMA = StructType(
    [
        StructField("resourceType", StringType(), True),
        StructField("type", StringType(), True),
        StructField("entry", ArrayType(ENTRY_SCHEMA), True),
    ]
)
