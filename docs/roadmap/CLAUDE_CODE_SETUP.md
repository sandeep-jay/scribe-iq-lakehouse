# Claude Code Setup — scribe-iq-lakehouse + clinical-bert-pipeline

**Owner:** Sandeep Jayaprakash  
**Purpose:** Complete Claude Code configuration for both projects —
skills, agents, CLAUDE.md, session handoffs, changelog discipline,
and ADR practice.

---

## 1. Claude Code Fundamentals for This Project

### What Claude Code needs to know about you

Every CLAUDE.md should open with context that persists across sessions:

```markdown
# About this project and developer

## Developer context
- Active job hunt — healthcare AI / data engineering roles
- M1 Max 32GB — MPS available, fp16=False for PyTorch
- Fabric trial: ~15-20 days remaining — Fabric work is highest priority
- Working hours: evenings + weekends

## Project system
Three interconnected repos:
  scribe-iq-lakehouse    → data platform (this repo)
  clinical-bert-pipeline → discriminative NLP
  scribe-iq              → clinical RAG app
All share the Gold corpus contract in scribe-iq-lakehouse/docs/CORPUS_CONTRACT.md

## Non-negotiables
- Every session ends with a handoff document
- Every architectural decision gets an ADR
- CHANGELOG.md updated on every meaningful commit
- Platform abstraction layer — no Fabric-specific code in transforms/
- Arrow as interchange format — transforms return pa.Table
- Tests written alongside every transform, not after
```

---

## 2. CLAUDE.md Structure

Create one CLAUDE.md per repo. Structure:

```
CLAUDE.md
  1. Project overview (2-3 sentences)
  2. Developer context (hardware, constraints, priorities)
  3. Architecture (key decisions, not full spec — link to spec)
  4. Current status (what's done, what's in progress)
  5. Non-negotiables (patterns that must be followed)
  6. Session protocol (how to start, how to end)
  7. Key files (where things live)
  8. Skills in use (which CLAUDE.md skills are active)
```

### `scribe-iq-lakehouse/CLAUDE.md`

```markdown
# scribe-iq-lakehouse — Claude Code Configuration

## Project
Production-pattern healthcare data lakehouse on Synthea Coherent.
Medallion architecture: Bronze → Silver → Gold.
Feeds scribe-iq (RAG) and clinical-bert-pipeline (NLP).

## Developer context
- M1 Max 32GB, macOS
- Fabric trial active (~15-20 days) — Fabric notebooks are priority
- LAKEHOUSE_PLATFORM env var controls execution environment
- Active job hunt — portfolio quality matters, ship > perfect

## Architecture principles
- Platform abstraction: ALL cloud I/O via local/platform/
- Arrow interchange: transforms return pa.Table, never DataFrames
- Transforms are pure: no file paths, no platform imports in transforms/
- Tests alongside: write test when writing transform, not after
- Fabric notebooks import from local/transforms/ — no duplicate logic
- Full spec: docs/SPEC.md

## Current status
See: HANDOFF.md (updated end of every session)
See: CHANGELOG.md (updated every meaningful commit)

## Non-negotiables
1. Never hardcode OneLake paths — always platform.storage_path()
2. Never import Fabric/Spark in local/transforms/
3. Every new transform gets a test in tests/
4. Every architectural decision gets an ADR in docs/adr/
5. Session ends with updated HANDOFF.md

## Session protocol
START: Read HANDOFF.md → confirm current state → read relevant spec section
END:   Update HANDOFF.md → update CHANGELOG.md → commit with message

## Key files
  docs/SPEC.md               Full implementation spec
  docs/CORPUS_CONTRACT.md    Handoff schema to downstream AI
  docs/adr/                  Architectural decision records
  HANDOFF.md                 Current session state
  CHANGELOG.md               All meaningful changes
  local/platform/base.py     Platform abstraction interface
  local/transforms/          Engine-agnostic transform logic
  fabric/notebooks/          Fabric execution + documentation

## Skills active
  @karpathy — reasoning through complex technical decisions
  @with-docs — always read relevant docs before implementing
  @matt-pocock — TypeScript patterns (frontend only if needed)
```

### `clinical-bert-pipeline/CLAUDE.md`

```markdown
# clinical-bert-pipeline — Claude Code Configuration

## Project
Production MLOps pipeline for clinical NLP.
ClinicalBERT fine-tuning on Synthea SOAP notes + MTSamples.
MLflow tracking, DVC versioning, FastAPI serving, Streamlit showcase.

## Developer context
- M1 Max 32GB — use_mps=True, fp16=False, batch_size=32
- Training time: ~90 min on M1 Max for full run
- MLflow server: Docker Compose :5000
- Active job hunt — OOD F1 on MTSamples is the showcase metric

## Architecture principles
- params.yaml is single source of truth — never hardcode hyperparams
- MultiSourceDataset: 70% SOAP notes + 30% MTSamples per batch
- Transforms return Arrow — consistent with lakehouse pattern
- eval_report.json is committed — it's a portfolio artifact
- Full spec: docs/SPEC.md

## Current status
See: HANDOFF.md

## Non-negotiables
1. params.yaml controls everything — no hardcoded values in code
2. fp16=False — MPS limitation, always
3. eval_report.json always committed — never gitignored
4. OOD F1 (MTSamples held-out) always reported alongside in-dist F1
5. Session ends with updated HANDOFF.md

## Session protocol
START: Read HANDOFF.md → docker compose up → verify MLflow accessible
END:   Update HANDOFF.md → update CHANGELOG.md → commit

## Key files
  params.yaml               Hyperparameters — source of truth
  src/data/dataset.py       MultiSourceDataset implementation
  src/training/train.py     Trainer + MLflow autolog
  outputs/eval_report.json  Committed baseline metrics
  docs/adr/                 Architectural decisions
  HANDOFF.md                Current session state
  CHANGELOG.md              All changes
```

---

## 3. Skills Configuration

### Your existing skills (from your CLAUDE.md mention)

```
@karpathy    — deep technical reasoning, "think step by step"
               Use for: complex architectural decisions, debugging,
               choosing between technical approaches
               
@with-docs   — always fetch and read docs before implementing
               Use for: every library, every API, every framework
               Prevents hallucinated APIs
               
@matt-pocock — TypeScript excellence, type safety patterns
               Use for: any TypeScript in frontend (scribe-iq Next.js)
               Less relevant for Python-heavy lakehouse work
```

### Additional skills to add to your CLAUDE.md

**`@healthcare-data`** — custom skill for this project:

```markdown
# @healthcare-data skill

When working on healthcare data code:
1. Never assume PHI is absent — always check data_limitation fields
2. FHIR resources have optional fields — always use .get() with defaults
3. Document Synthea limitations explicitly in code comments
4. Clinical codes (SNOMED, LOINC, ICD) should be preserved as strings,
   never cast to int
5. Encounter dates matter for age calculation — use period.start not now()
6. Base64 decode FHIR Binary resources — always specify encoding='utf-8'
7. SOAP section detection is heuristic — document false positive rate
```

**`@delta-patterns`** — Delta Lake best practices:

```markdown
# @delta-patterns skill

When writing Delta Lake code:
1. Always enable CDC: delta.enableChangeDataFeed = true
2. MERGE preferred over overwrite for Silver tables
3. Partition by date or cohort — never by high-cardinality columns
4. Z-ORDER on frequently filtered columns (patient_id, encounter_id)
5. Vacuum after large deletes — default retention 7 days
6. Schema evolution: set mergeSchema=true explicitly
7. Checkpoint location must be unique per stream
```

**`@mlops-patterns`** — MLflow/DVC discipline:

```markdown
# @mlops-patterns skill

When writing MLOps code:
1. Every hyperparameter comes from params.yaml — never hardcoded
2. MLflow autolog is the floor — add custom metrics on top
3. eval_report.json is a committed artifact — always write it
4. Gate threshold lives in params.yaml — not hardcoded in CI
5. Model registration only if F1 >= gate_threshold
6. DVC tracks data files — never commit large files to git
7. Training args: fp16=False on MPS, batch_size=32 on M1 Max
```

### Skill file locations

```
.claude/
  skills/
    healthcare-data.md
    delta-patterns.md
    mlops-patterns.md
  CLAUDE.md              (repo root — applies to all sessions)
```

---

## 4. Agents Configuration

### Agents useful for this project

**Sub-agent: `test-writer`**

Dedicated agent that writes tests alongside every implementation.
Tell Claude Code: *"After implementing each transform, spawn a
test-writer sub-agent to write the corresponding test file."*

Prompt to include in CLAUDE.md:
```
After every transform implementation, immediately write the
corresponding test in tests/. Tests use the 5-patient fixture
in tests/fixtures/sample_bundle.json. Never leave a transform
untested at end of session.
```

**Sub-agent: `doc-writer`**

Writes notebook markdown cells and docstrings.
Tell Claude Code: *"After each code cell in a notebook is working,
add comprehensive markdown cells explaining what it does, why,
and what the output means."*

**Sub-agent: `adr-writer`**

Writes ADRs for architectural decisions.
Trigger: any time a significant technical choice is made.

---

## 5. HANDOFF.md — Session State Protocol

### What a handoff document is

A handoff is a structured state snapshot written at the end of
every session. It answers: *"If I come back tomorrow with zero
memory of today, what do I need to know to continue?"*

Claude Code should write this automatically at session end.
Include explicit instruction in CLAUDE.md:

```markdown
## Session end protocol (MANDATORY)
At the end of every session, before stopping:
1. Update HANDOFF.md with current state
2. Update CHANGELOG.md with what changed
3. Run tests — note any failures
4. Commit with descriptive message
5. List next session's starting task explicitly
```

### HANDOFF.md template

```markdown
# HANDOFF — {date} {time}

## Session summary
What was accomplished this session in 3-5 sentences.

## Current state
**Working:**
- List of things that are fully working and tested

**In progress:**
- What was being worked on when session ended
- Exact file and line number if mid-implementation

**Blocked:**
- Anything that needs external input or decision

## Test status
\`\`\`
pytest tests/ — X passed, Y failed
\`\`\`
List any failing tests with brief reason.

## Next session — start here
**First task:** [specific task, file, function]
**Context needed:** [what to read before starting]
**Decision needed:** [any open questions to resolve first]

## Open decisions
| Decision | Options | Recommendation | Status |
|---|---|---|---|
| e.g. Use DuckDB or Polars for SQL | DuckDB / Polars | DuckDB for complex joins | Open |

## Key state
\`\`\`
LAKEHOUSE_PLATFORM=fabric
Last notebook completed: 05_silver_soap_notes
Silver tables written: patient, encounter, condition, soap_note
Gold tables written: none yet
Fabric cohort size: 20 patients (dev mode)
Next: scale to full cohort before Gold build
\`\`\`

## Files changed this session
- local/transforms/silver_soap_notes.py — implemented Base64 decode
- tests/test_silver_soap_notes.py — 8 tests, all passing
- fabric/notebooks/05_silver_soap_notes.ipynb — Fabric version
- docs/adr/004-soap-note-extraction.md — decision recorded

## ADRs written this session
- ADR-004: SOAP note extraction via Base64 Binary decode
```

### Example handoff — start of next session

Claude Code reads HANDOFF.md and says:

*"Previous session completed Silver SOAP notes transform.
20-patient dev cohort written to Fabric. Next task is
Notebook 06 ECG metadata. Tests: 8 passing, 0 failing.
No blocked items. Starting Notebook 06 now."*

That's the behaviour to expect. If Claude Code doesn't do this
automatically, paste HANDOFF.md content at session start.

---

## 6. CHANGELOG.md Discipline

### Format — Keep a Changelog standard

```markdown
# Changelog
All notable changes to scribe-iq-lakehouse.
Format: [Keep a Changelog](https://keepachangelog.com/en/1.0.0/)

## [Unreleased]

## [0.3.0] — 2026-05-28
### Added
- Silver SOAP note transform — Base64 decode from FHIR Binary
- SOAP section detection (S/O/A/P heuristic parser)
- silver.soap_note Delta table with CDC enabled
- Fabric Notebook 05 with full markdown documentation
- ADR-004: SOAP extraction approach

### Changed
- platform/base.py: added write_silver() abstract method

### Fixed
- fhir_parser.py: handle missing DocumentReference.date field

## [0.2.0] — 2026-05-27
### Added
- Silver patient and encounter transforms
- Fabric Notebooks 02-03
- Platform abstraction layer — base.py + fabric.py
- ADR-003: Arrow as interchange format

## [0.1.0] — 2026-05-26
### Added
- Initial repo structure
- Bronze ingestion — S3 shortcut to Synthea Coherent
- FHIR parser foundation — extract_patient, extract_encounter
- 5-patient test fixture
- ADR-001: Fabric-first development approach
- ADR-002: Platform abstraction layer design
```

### When to update CHANGELOG.md

```
Always update for:
  New transform implemented
  New table written
  New test file added
  Architecture decision made
  Bug fixed
  ADR written
  Notebook completed

Don't update for:
  Minor formatting changes
  Comment updates
  WIP commits
```

---

## 7. ADR Practice

### What an ADR is

An Architectural Decision Record captures a significant technical
decision — what was decided, why, what alternatives were considered,
and what the consequences are.

For this project: any time you make a non-obvious technical choice,
write an ADR. Future you (and future employers reading the repo)
will understand why the architecture looks the way it does.

### ADR template

```markdown
# ADR-{number}: {short title}

**Date:** {date}  
**Status:** Accepted | Superseded by ADR-{n} | Deprecated  
**Deciders:** Sandeep Jayaprakash  

## Context

What situation prompted this decision?
What constraints existed?
What was unclear or in tension?

## Decision

What was decided, stated clearly and directly.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| Option A | ... | ... | ... |
| Option B | ... | ... | ... |

## Consequences

**Positive:**
- What this enables
- What becomes easier

**Negative:**
- What this constrains
- What becomes harder

**Neutral:**
- Things that change but aren't clearly better or worse

## Implementation notes

Specific files, patterns, or code that implements this decision.
```

### ADRs to write immediately (session 1)

```
ADR-001: Fabric-first development approach
  Context: Trial expiring, job hunt active
  Decision: Fabric first, local Polars builds after trial
  
ADR-002: Platform abstraction layer
  Context: Need portability across Fabric, Databricks, AWS, GCP, local
  Decision: Abstract interface + factory pattern, Arrow interchange
  
ADR-003: Polars + DuckDB for local lite tier
  Context: Zero-dependency local pipeline for portfolio reviewers
  Decision: Polars transforms, DuckDB for SQL, delta-rs for Delta
  
ADR-004: Arrow as transform interchange format
  Context: Transforms need to work on Spark and Polars
  Decision: pa.Table return type, both engines accept Arrow natively
  
ADR-005: FHIR Binary Base64 decode for SOAP notes
  Context: Synthea Coherent embeds notes in Binary FHIR resources
  Decision: Decode in fhir_parser, link via DocumentReference
  
ADR-006: DICOM stop_before_pixels extraction
  Context: Need imaging metadata without pixel processing overhead
  Decision: pydicom stop_before_pixels=True, full pixel roadmap Phase 4
  
ADR-007: Genomic data_limitation as first-class column
  Context: Synthea genomics is simulated inheritance, not clinical
  Decision: Always populate data_limitation column, visible to consumers
```

### ADR file structure

```
docs/adr/
  README.md          index of all ADRs
  001-fabric-first.md
  002-platform-abstraction.md
  003-polars-duckdb-local.md
  004-arrow-interchange.md
  005-fhir-binary-decode.md
  006-dicom-stop-before-pixels.md
  007-genomic-data-limitation.md
```

---

## 8. Session-by-Session Claude Code Instructions

### How to start every session

Paste this at the start of every Claude Code session:

```
Read HANDOFF.md and tell me:
1. What was completed last session
2. What the next task is
3. Any failing tests
4. Any open decisions

Then proceed with the next task. Do not ask for permission
to start — just begin. Update HANDOFF.md, CHANGELOG.md,
and write any ADRs before ending the session.
```

### Session 1 — Fabric setup + FHIR parser foundation

**Start prompt:**
```
We are starting scribe-iq-lakehouse from scratch.
Read docs/SPEC.md sections 1-5 before writing any code.

Session goals:
1. Create full repo structure per spec section 4
2. Create CLAUDE.md, HANDOFF.md, CHANGELOG.md, docs/adr/README.md
3. Write ADRs 001-007 (listed in CLAUDE_CODE_SETUP.md)
4. Implement local/platform/base.py — abstract interface only
5. Implement local/platform/fabric.py — Fabric implementation
6. Implement local/platform/factory.py — env var router
7. Implement local/transforms/fhir_parser.py
   — FHIRBundleParser class
   — extract_patient, extract_encounter, extract_condition
   — extract_observation, extract_medication_request, extract_procedure
   — extract_soap_note (Base64 decode + SOAP section detection)
   — extract_ecg_metadata
   — extract_imaging_study (pydicom stop_before_pixels)
   — extract_genomic_report (metadata flag + data_limitation)
8. Create tests/fixtures/sample_bundle.json (5 patients from S3)
9. Write tests/test_fhir_parser.py — full coverage
10. End session: update HANDOFF.md, CHANGELOG.md, commit

Non-negotiables:
- fhir_parser.py has zero platform imports
- extract_imaging_study uses stop_before_pixels=True
- extract_genomic_report populates data_limitation always
- All tests pass before session ends
```

### Session 2 — Fabric Bronze + Silver patient/encounter

**Start prompt:**
```
Read HANDOFF.md. Confirm session 1 state before proceeding.

Session goals:
1. Fabric Notebook 00_setup.ipynb
   — OneLake path config, library installs, verify S3 shortcut
   — Full markdown documentation per notebook template
2. Fabric Notebook 01_bronze_ingest.ipynb
   — S3 → Bronze landing, 20-patient dev cohort
   — Ingest manifest written to _metadata/
3. Fabric Notebook 02_silver_patient.ipynb
   — imports from local.transforms.fhir_parser
   — Full Silver patient table written
   — display() output verified
   — CDC enabled
4. Fabric Notebook 03_silver_encounter.ipynb
   — Same pattern as patient
5. Write ADR for any decisions made
6. End session: HANDOFF.md, CHANGELOG.md, commit

Fabric notebook template (every notebook must follow):
  Cell 1: Markdown — what this notebook does, I/O, dependencies
  Cell 2: Markdown — architecture context
  Cell 3: Code — imports, platform setup (LAKEHOUSE_PLATFORM=fabric)
  Cell 4: Markdown — section explanation
  Cell 5: Code — transform
  Cell 6: Markdown — validation approach
  Cell 7: Code — display(), row counts, quality checks
  Cell 8: Code — log to ingest_log
```

### Session 3 — Silver clinical + SOAP notes (most important)

**Start prompt:**
```
Read HANDOFF.md. Confirm sessions 1-2 state.

Session goals:
1. Fabric Notebook 04_silver_clinical.ipynb
   — Condition, Observation, MedicationRequest, Procedure
2. Fabric Notebook 05_silver_soap_notes.ipynb  ← PRIORITY
   — This is the centerpiece notebook
   — Auto Loader streaming with readStream + foreachBatch
   — Base64 decode from FHIR Binary
   — SOAP section detection
   — CDC enabled on silver.soap_note
   — Full markdown explaining Base64 → decode → sections
   — display() showing decoded note text sample
3. Write tests/test_silver_soap_notes.py if not done
4. ADR for streaming approach if needed
5. HANDOFF.md, CHANGELOG.md, commit

Note: Notebook 05 must show a decoded SOAP note in display()
output. This is the demo centerpiece — a reviewer must be able
to see readable clinical text in the notebook output.
```

### Session 4 — Silver ECG, imaging, genomics

```
Read HANDOFF.md.

Session goals:
1. Fabric Notebook 06_silver_ecg.ipynb
   — DiagnosticReport + Observation (LOINC codes)
   — heart_rate_bpm, rhythm, conclusion, has_waveform flag
2. Fabric Notebook 07_silver_imaging.ipynb
   — ImagingStudy FHIR metadata
   — pydicom header extraction (stop_before_pixels=True)
   — Verify modality, body_part, study_description populated
3. Fabric Notebook 08_silver_genomics.ipynb
   — Metadata flag only
   — data_limitation column always populated
   — Document Synthea inheritance limitation in markdown
4. Enable CDC on all Silver tables not yet enabled
5. HANDOFF.md, CHANGELOG.md, commit
```

### Session 5 — Gold + pipeline orchestration

```
Read HANDOFF.md.

Session goals:
1. Fabric Notebook 09_gold_encounter_summary.ipynb
   — Denormalize all Silver tables
   — imaging struct with modality, body_part, study_description
   — ecg_finding, ecg_rhythm, has_waveform
   — genomic_summary, has_genomics
   — corpus_manifest table for lineage
2. Fabric Notebook 10_corpus_export.ipynb
   — Export Gold for Ollama generation
   — Verify CORPUS_CONTRACT.md schema
3. Master pipeline canvas in Fabric
   — All notebooks wired with dependencies
   — Parallel groups 2 and 3
   — First full pipeline run on 20-patient cohort
4. Scale to full cohort — all 1,500 patients
5. HANDOFF.md, CHANGELOG.md, commit
```

### Session 6 — DevOps, observability, production layer

```
Read HANDOFF.md.

Session goals:
1. Git integration — connect Fabric workspace to GitHub
2. silver.ingest_log — schema, writes from all notebooks
3. silver.pipeline_metrics — aggregate per-run metrics
4. silver.quality_report — quality checks per table
5. Row count drift detection in validate.py
6. Data Activator — 4 alert rules configured
7. Power BI dashboard — 4 pages, reading from metrics tables
8. HANDOFF.md, CHANGELOG.md, commit
```

### Session 7 — Demo capture + documentation

```
Read HANDOFF.md.

Session goals:
1. All screenshots from checklist 15.2 captured
2. Videos 1-5 recorded per script 15.4
3. docs/ — all markdown files complete
4. README.md — full reviewer guide, architecture diagram,
   Fabric screenshots embedded, video links
5. Sample data committed to docs/sample_data/
   — pipeline_metrics sample (10 rows CSV)
   — quality_report sample (10 rows CSV)
   — encounter_summary sample (50 rows JSON)
6. Power BI report exported as PDF → docs/
7. All notebooks exported as .ipynb → committed
8. Repo public, clean, pushed to GitHub
9. Final HANDOFF.md — marks Fabric phase complete
```

---

## 9. Notebook Documentation Template

Every Fabric notebook must follow this structure.
Include this in CLAUDE.md so Claude Code enforces it automatically.

```python
# ============================================================
# CELL 1 — MARKDOWN (required, every notebook)
# ============================================================
"""
# {Notebook number}: {Notebook title}

## What this notebook does
One paragraph description. Inputs, outputs, what transforms run.

## Inputs
- Source: {Bronze/Silver path or table name}
- Format: {FHIR JSON / Delta table}
- Expected row count: ~{N}

## Outputs
- Table: {silver/gold}.{table_name}
- Columns: {key columns listed}
- Estimated rows: ~{N}

## Dependencies
Run after: Notebook {N-1}
Required tables: {list}

## Part of
Bronze → **Silver** → Gold medallion pipeline.
This notebook builds the {name} Silver table.
"""

# ============================================================
# CELL 2 — MARKDOWN: Architecture context
# ============================================================
"""
## Architecture context

{Where this sits in the pipeline diagram}
{What the downstream consumer uses this table for}
{Why this approach was chosen — reference ADR if applicable}
"""

# ============================================================
# CELL 3 — CODE: Setup
# ============================================================
import os
os.environ["LAKEHOUSE_PLATFORM"] = "fabric"

from local.platform.factory import get_platform
from local.transforms.{module} import {transform_function}

platform = get_platform()
spark = platform.get_spark_session()

print(f"Platform: {os.environ['LAKEHOUSE_PLATFORM']}")
print(f"Spark version: {spark.version}")

# ============================================================
# CELL 4 — MARKDOWN: Transform approach
# ============================================================
"""
## Transform approach

{Explain what the transform does in plain English}
{Note any non-obvious decisions}
{Reference relevant FHIR resources}
"""

# ============================================================
# CELL 5 — CODE: Transform
# ============================================================
result_table = transform_function(platform, spark)

# ============================================================
# CELL 6 — MARKDOWN: Validation
# ============================================================
"""
## Validation

Checking:
1. Row count — minimum threshold: {N}
2. Required non-null columns: {list}
3. Referential integrity: {FK checks}
4. Business rules: {clinical range checks}
"""

# ============================================================
# CELL 7 — CODE: Validation + display
# ============================================================
row_count = spark.table(f"silver.{table_name}").count()
print(f"Rows written: {row_count:,}")
assert row_count >= MIN_ROWS, f"Row count {row_count} below minimum {MIN_ROWS}"

display(spark.table(f"silver.{table_name}").limit(5))

# ============================================================
# CELL 8 — CODE: Log to ingest_log
# ============================================================
platform.log_metric(f"silver.{table_name}", "rows_written", row_count)
print("✓ Logged to silver.ingest_log")
```

---

## 10. Git Commit Message Convention

```
feat(silver): implement SOAP note Base64 extraction
feat(fabric): add Notebook 05 with streaming Auto Loader
feat(platform): add Fabric platform implementation
fix(fhir): handle missing DocumentReference.date field
fix(ecg): null heart_rate_bpm when observation absent
docs(adr): ADR-005 FHIR Binary extraction approach
docs(notebook): add architecture context to Notebook 03
test(silver): add soap_note extraction test coverage
chore(changelog): update for v0.3.0
refactor(platform): extract storage_path to base class
```

Format: `{type}({scope}): {description}`

Types: feat, fix, docs, test, chore, refactor, perf
Scopes: silver, gold, bronze, fabric, platform, fhir, bert, streaming

---

## 11. Quick Reference — Claude Code Prompts

### To start any session
```
Read HANDOFF.md. Summarize current state in 3 sentences.
Then proceed with the next task listed.
```

### To write an ADR
```
Write an ADR for the decision to {decision}.
Use the template in CLAUDE_CODE_SETUP.md.
Save to docs/adr/{number}-{slug}.md.
Update docs/adr/README.md index.
```

### To add a new transform
```
Implement {transform_name} in local/transforms/{file}.py.
Follow the Arrow interchange pattern — return pa.Table.
No platform imports in the transform file.
Write the corresponding Fabric notebook importing this transform.
Write tests in tests/test_{file}.py using the 5-patient fixture.
Write an ADR if a non-obvious decision was made.
Update CHANGELOG.md.
```

### To end a session
```
We're ending this session. Before stopping:
1. Update HANDOFF.md with current state
2. Update CHANGELOG.md
3. Run pytest — report results
4. Write any pending ADRs
5. Commit with descriptive message
6. Tell me the first task for next session
```

### To debug a Fabric notebook
```
The notebook {name} is failing with {error}.
Read the relevant transform in local/transforms/{file}.py.
Check if the error is in the transform or the Fabric wrapper.
Fix in the transform first — the notebook imports it.
Write a test that reproduces the failure.
Fix the test, then verify the notebook.
```

---

---

## 12. claude-os — Personal Claude Code Operating System

Full file contents for the private `sandeep-jay/claude-os` repo
are in `claude-os-spec.md`. That document contains every file
written out completely and ready to commit:

```
CLAUDE.md                    Global defaults → symlink ~/.claude/CLAUDE.md
skills/handoff.md            Session handoff protocol + template
skills/adr.md                ADR practice + template
skills/changelog.md          Changelog discipline
skills/python.md             Python conventions
skills/healthcare-data.md    FHIR, clinical data, PHI rules
skills/delta-patterns.md     Delta Lake, CDC, medallion patterns
skills/mlops.md              MLflow, DVC, training patterns
templates/CLAUDE.md.project  Per-project template
templates/HANDOFF.md         Session handoff template
init.sh                      Project init script
```

Plus the two assembled project CLAUDE.mds ready to drop in:
- `scribe-iq-lakehouse/CLAUDE.md`
- `clinical-bert-pipeline/CLAUDE.md`

### How to use

```bash
# One-time setup
git clone git@github.com:sandeep-jay/claude-os.git ~/claude-os
mkdir -p ~/.claude
ln -sf ~/claude-os/CLAUDE.md ~/.claude/CLAUDE.md
chmod +x ~/claude-os/init.sh

# Per project
cd ~/scribe-iq-lakehouse
bash ~/claude-os/init.sh scribe-iq-lakehouse \
    healthcare-data delta-patterns python handoff adr changelog

# Then drop in the assembled CLAUDE.md from claude-os-spec.md
# Edit to fill in {PLACEHOLDERS}
```

---

*Document version: 2.0 — May 2026*  
*For: scribe-iq-lakehouse + clinical-bert-pipeline*  
*claude-os full file contents: claude-os-spec.md*
