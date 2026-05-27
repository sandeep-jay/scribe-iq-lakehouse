# ADR-007: Genomic data_limitation as first-class column

**Date:** 2026-05-27
**Status:** Accepted
**Deciders:** Sandeep Jayaprakash

## Context

Synthea Coherent genomics models familial inheritance simulation — not clinically
actionable variants. There are no BRCA1/2, CYP2D6, HLA, or pharmacogenomic variants.
A genomic pipeline built on this data would be technically complete but clinically
meaningless. Options: skip genomics entirely, extract and silently use the data, or
extract with honest documentation of the limitation.

## Decision

Extract genomic report metadata (report_date, gene_panel_name, result_summary,
has_pathogenic_variant, family_history_flag) and populate a `data_limitation` column
on every row with the value: "Synthea simulated inheritance — not clinical variants".
This column is non-nullable — every row must have it. Any downstream consumer reading
silver.genomic_report sees the constraint in the data itself, not buried in a README.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|--------|------|------|-------------|
| Skip genomics entirely | Honest | Loses the data engineering signal | Portfolio loses multimodal data story |
| Extract without limitation | Simpler schema | Misleads downstream consumers | Dishonest data modeling |
| README-only documentation | Easy | README is rarely read | Constraint must be in the data |
| data_limitation column (current) | Self-documenting data contract | One extra column | Best approach for honest data modeling |

## Consequences

**Positive:**
- Every consumer knows the limitation without reading external docs
- Demonstrates honest data modeling as a portfolio signal
- binary_id preserved for Phase 5 when real genomic data is available

**Negative:**
- Full VCF parsing deferred to Phase 5 — requires real clinical variant data
- data_limitation column adds schema overhead

**Neutral:**
- Pattern is reusable: any data with known limitations gets a data_limitation column

## Implementation notes
- `local/transforms/fhir_parser.py` — extract_genomic_report() always sets data_limitation
- `local/transforms/silver_genomics.py` — Silver table writer
- `fabric/notebooks/08_silver_genomics.ipynb` — Fabric extraction with limitation docs
- Phase 5 path: real genomic data (ClinVar, gnomAD, UK Biobank) + cyvcf2 + PharmGKB
- The data_limitation column pattern applies to any data source with known clinical constraints
