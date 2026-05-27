# claude-os — Personal Claude Code Operating System

**Repo:** `sandeep-jay/claude-os` (private)
**Purpose:** Generic Claude Code foundation pulled into any project.
**Pattern:** Global defaults + skill library + project templates.
**Usage:** Pull, run init script, customize project CLAUDE.md.

---

## How It Works

```
~/.claude/CLAUDE.md           ← symlink to claude-os/CLAUDE.md
                                 Claude Code reads this globally
                                 Applies to every project automatically

project-repo/
  CLAUDE.md                   ← assembled from template + project needs
                                 Copy relevant skill content in
                                 Add project-specific non-negotiables

  HANDOFF.md                  ← updated end of every session
  CHANGELOG.md                ← updated every meaningful commit
  docs/adr/                   ← ADR per architectural decision
```

## Pull Into a New Project

```bash
# Option A — copy files you need
cp ~/claude-os/templates/CLAUDE.md.project ./CLAUDE.md
cp ~/claude-os/templates/HANDOFF.md ./HANDOFF.md
# Then paste relevant skill content into CLAUDE.md

# Option B — init script
bash ~/claude-os/init.sh my-project-name healthcare-data delta-patterns
# Creates CLAUDE.md assembled from named skills
```

---

## Repository Structure

```
claude-os/
├── README.md                    This file
├── CLAUDE.md                    Global defaults — symlink to ~/.claude/
├── init.sh                      Project init script
│
├── skills/
│   ├── handoff.md               Session handoff protocol
│   ├── adr.md                   ADR practice
│   ├── changelog.md             Changelog discipline
│   ├── python.md                Python conventions
│   ├── healthcare-data.md       FHIR, clinical data, PHI rules
│   ├── delta-patterns.md        Delta Lake, CDC, medallion
│   └── mlops.md                 MLflow, DVC, training patterns
│
└── templates/
    ├── CLAUDE.md.project        Per-project CLAUDE.md template
    └── HANDOFF.md               Session handoff template
```

---

## File: `CLAUDE.md` (Global)

> Symlink this to `~/.claude/CLAUDE.md`
> Applies to every Claude Code session automatically

```markdown
# Claude Code — Global Configuration
# Sandeep Jayaprakash

## Developer context
- M1 Max 32GB (primary), M5 Max 128GB arriving June 2026
- MPS available for PyTorch — fp16=False always on Apple Silicon
- Active job hunt — healthcare AI / data engineering roles
- Portfolio quality matters: ship working > perfect
- Private skills repo: ~/claude-os/

## Universal non-negotiables
1. Every session ends with updated HANDOFF.md — non-negotiable
2. Every architectural decision gets an ADR in docs/adr/
3. CHANGELOG.md updated on every meaningful commit
4. Tests written alongside implementation, never after
5. No hardcoded credentials, paths, or magic numbers anywhere
6. Read relevant docs before implementing any library or API
7. When in doubt about a library API — fetch the docs, don't guess

## Session protocol

### Start of every session
Read HANDOFF.md first. Summarize current state in 3 sentences.
State the next task explicitly. Then begin — no permission needed.

### End of every session (mandatory before stopping)
1. Update HANDOFF.md — current state, next task, open decisions
2. Update CHANGELOG.md — what changed this session
3. Run tests — report results, note any failures
4. Write any pending ADRs
5. Commit with conventional commit message
6. State first task for next session explicitly

## Code quality defaults
- Python: ruff for linting, black for formatting
- Type hints on all function signatures
- Docstrings on all public functions
- No print() in production code — use logging
- No TODO comments committed — file a tracked issue instead
- Functions under 40 lines — extract if longer

## Git conventions
Format: {type}({scope}): {description}
Types: feat, fix, docs, test, chore, refactor, perf
Examples:
  feat(silver): implement SOAP note Base64 extraction
  fix(ecg): handle missing heart_rate observation
  docs(adr): ADR-005 FHIR Binary extraction approach
  test(silver): add soap_note extraction coverage

## Reasoning approach
For complex technical decisions: think step by step before coding.
State the options, state the tradeoffs, state the recommendation.
Then implement. Don't jump straight to code on hard problems.

## What I care about
- Honest documentation — limitations are first-class, not hidden
- Production patterns — even portfolio work should be production-grade
- Clear architecture — a reviewer should understand the system in 5 min
- Portable code — avoid vendor lock-in by default
```

---

## File: `skills/handoff.md`

```markdown
# Skill: Session Handoff Protocol

A handoff is a structured state snapshot written at the end of
every session. It answers: "If I return tomorrow with zero memory
of today, what do I need to know to continue without losing work?"

## When to write
End of every Claude Code session. Non-negotiable.
If session is interrupted, write partial handoff before stopping.

## Template location
~/claude-os/templates/HANDOFF.md

## Handoff structure

### Header
- Date and time
- Session number (increment each session)
- Repo and branch

### Accomplished
3-5 bullet points. What was actually completed.
Be specific — file names, function names, test counts.

### Current state
**Working:** list of things fully working and tested
**In progress:** exact file + line if mid-implementation
**Blocked:** anything needing external input or decision

### Test status
Run pytest and report. List any failing tests with reason.
Never leave a session with unknown test status.

### Next session — start here
**First task:** specific, actionable, one sentence
**Read first:** any files or docs to read before starting
**Decision needed:** any open questions to resolve first

### Open decisions
Table: Decision | Options | Recommendation | Status

### Key state
Config values, env vars, last known counts, what's running.
Anything a fresh Claude Code instance needs to not repeat work.

### Files changed
List every file modified this session with one-line description.

### ADRs written
List any ADRs written this session.

## Rules
- Write to file, not just chat output — it must persist
- Atomic: either complete handoff or nothing — partial is ok
- Honest: if tests are failing, say so — don't paper over it
- Specific: "implemented Base64 decode" not "worked on FHIR"
```

---

## File: `skills/adr.md`

```markdown
# Skill: Architectural Decision Records

An ADR captures a significant technical decision permanently.
Future you and future employers understand WHY the architecture
looks the way it does — not just what it is.

## When to write an ADR
Any time you make a non-obvious technical choice:
- Choosing between two valid approaches
- Accepting a known limitation or tradeoff
- Deferring something that could be done now
- Choosing a framework, library, or pattern
- Deciding on a data model or schema

When in doubt: write the ADR. Takes 10 minutes, saves hours later.

## ADR numbering
Sequential integers: ADR-001, ADR-002, etc.
Never reuse a number. Superseded ADRs keep their number.

## File location
docs/adr/NNN-short-slug.md
docs/adr/README.md — index of all ADRs (update when adding)

## Template

---
# ADR-{NNN}: {Short descriptive title}

**Date:** {YYYY-MM-DD}
**Status:** Accepted | Superseded by ADR-{N} | Deprecated
**Deciders:** {name}

## Context
What situation prompted this decision?
What constraints exist? What is in tension?
2-4 sentences. Be specific about the actual problem.

## Decision
What was decided. State it directly and clearly.
One paragraph maximum.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|--------|------|------|-------------|
| ...    | ...  | ...  | ...         |

## Consequences

**Positive:** what this enables, what becomes easier
**Negative:** what this constrains, what becomes harder
**Neutral:** things that change but aren't clearly better/worse

## Implementation notes
Specific files, patterns, or code that implements this decision.
Link to relevant code if already written.

---

## Index template (docs/adr/README.md)

# ADR Index

| # | Title | Status | Date |
|---|-------|--------|------|
| 001 | ... | Accepted | 2026-05-24 |

## Rules
- Write ADR BEFORE implementing, not after
- "Status: Accepted" means it's the current approach
- If you change an approach, mark old ADR superseded + write new one
- ADRs are permanent history — never delete
```

---

## File: `skills/changelog.md`

```markdown
# Skill: Changelog Discipline

## Format
Keep a Changelog standard: https://keepachangelog.com/en/1.0.0/

## Structure
# Changelog

## [Unreleased]
### Added
### Changed
### Fixed
### Removed

## [X.Y.Z] — YYYY-MM-DD
### Added
- New feature or file added
### Changed
- Existing behavior changed
### Fixed
- Bug fixed
### Removed
- Something removed

## When to update
Always update for:
  New transform or function implemented
  New table or schema added
  New test file added
  Bug fixed
  ADR written
  Notebook completed
  Dependency added or removed

Don't update for:
  Minor formatting or comment changes
  WIP commits mid-feature
  README typo fixes

## Version bumping
0.1.0 → first working pipeline or feature
0.2.0 → significant addition (new data source, new task)
0.3.0 → next major feature
1.0.0 → production-ready, publicly demonstrated

## Rules
- Unreleased section always at top
- Most recent version first
- Group by type (Added/Changed/Fixed)
- Each entry is one line, specific
- Link to ADR if decision is documented there
```

---

## File: `skills/python.md`

```markdown
# Skill: Python Conventions

## Formatting and linting
- Formatter: black (line length 88)
- Linter: ruff
- Type checker: pyright (strict where practical)
- Run before every commit: ruff check . && black --check .

## Type hints
All public function signatures get type hints.
Return types always annotated.
Use | for union types (Python 3.10+).

# Good
def extract_soap_note(
    binary_resource: dict,
    doc_ref: dict
) -> dict | None:

# Bad
def extract_soap_note(binary_resource, doc_ref):

## Docstrings
Google style on all public functions and classes.

def extract_soap_note(binary_resource: dict, doc_ref: dict) -> dict:
    """Extract and decode SOAP note from FHIR Binary resource.

    Args:
        binary_resource: FHIR Binary resource dict containing
            Base64-encoded note text.
        doc_ref: FHIR DocumentReference linking note to encounter.

    Returns:
        Dict with note_id, patient_id, encounter_id, note_text,
        has_subjective, has_objective, has_assessment, has_plan,
        char_count, word_count. None if decode fails.

    Raises:
        ValueError: If binary_resource missing required fields.
    """

## Error handling
Specific exceptions, never bare except.
Log errors before raising.
Never silently swallow exceptions in production paths.

# Good
try:
    note_text = base64.b64decode(data).decode("utf-8")
except (ValueError, UnicodeDecodeError) as e:
    logger.error("Base64 decode failed for %s: %s", note_id, e)
    raise

# Bad
try:
    note_text = base64.b64decode(data).decode("utf-8")
except:
    return None

## Imports
Standard library first, third-party second, local third.
Separated by blank lines. Alphabetical within groups.
No wildcard imports.

## Constants
UPPER_SNAKE_CASE at module level.
Never hardcode magic numbers — name them.

MIN_SOAP_NOTE_CHARS = 100
MAX_SOAP_NOTE_CHARS = 50_000
SOAP_SECTIONS = ("SUBJECTIVE", "OBJECTIVE", "ASSESSMENT", "PLAN")

## File organization
One class per file for substantial classes.
Module-level __all__ for public APIs.
Keep functions under 40 lines — extract if longer.

## pyproject.toml defaults
[tool.black]
line-length = 88

[tool.ruff]
line-length = 88
select = ["E", "F", "I", "N", "W", "UP"]

[tool.pyright]
pythonVersion = "3.11"
typeCheckingMode = "basic"
```

---

## File: `skills/healthcare-data.md`

```markdown
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
1. FHIR resources have OPTIONAL fields everywhere
   Always use .get() with a sensible default — never direct access

# Good
patient_id = resource.get("id", "")
birth_date = resource.get("birthDate", None)
conditions = resource.get("condition", [])

# Bad — will crash on missing optional fields
patient_id = resource["id"]

2. FHIR UUIDs are strings — never cast to int or hash
3. FHIR dates are strings — parse explicitly with dateutil or datetime
4. FHIR references look like "Patient/abc-123" — strip the type prefix

patient_id = reference.split("/")[-1]

5. One FHIR bundle per patient — iterate entries by resourceType

for entry in bundle.get("entry", []):
    resource = entry.get("resource", {})
    rtype = resource.get("resourceType", "")
    if rtype == "Patient":
        ...

## Clinical code handling
1. SNOMED, LOINC, ICD codes are STRINGS — never cast to numeric

# Good — preserves leading zeros, special chars
condition_code: str = coding.get("code", "")

# Bad — destroys codes like "0001" or "E11.9"
condition_code: int = int(coding.get("code", 0))

2. Always store both code and display name
   Code for computation, display for human readability
3. LOINC codes for common vitals:
   8867-4  = heart rate
   8480-6  = systolic BP
   8462-4  = diastolic BP
   8310-5  = body temperature
   59408-5 = O2 saturation
   8302-2  = body height
   29463-7 = body weight

## FHIR Binary and Base64
1. Clinical notes in Synthea Coherent live in Binary FHIR resources
2. Always decode with explicit encoding:

import base64
note_text = base64.b64decode(binary_data).decode("utf-8")

3. Link Binary to patient via DocumentReference:
   DocumentReference.subject → patient_id
   DocumentReference.context.encounter → encounter_id
   DocumentReference.content[0].attachment → Binary reference

## DICOM handling
1. Always use stop_before_pixels=True for metadata extraction

import pydicom, io
ds = pydicom.dcmread(io.BytesIO(binary_bytes), stop_before_pixels=True)

2. Never load pixel data without explicit intent
3. Useful header tags:
   StudyDate, Modality, BodyPartExamined, StudyDescription
   SeriesDescription, SliceThickness, Rows, Columns
   Manufacturer, MagneticFieldStrength

## Synthea-specific limitations
1. SOAP notes are template-driven — more regular than real clinical notes
2. Genomics data models inheritance simulation — not clinical variants
   Always populate data_limitation = "Synthea simulated inheritance"
3. ECG data is SBML-model-generated — not real waveforms
4. All data is synthetic — no real patients, no PHI risk
5. Document limitations in model card, README, and data columns

## Age calculation
Always calculate age at time of encounter, not current date

from datetime import date
def age_at_encounter(birth_date: date, encounter_date: date) -> int:
    delta = encounter_date - birth_date
    return int(delta.days / 365.25)

## Null handling in clinical data
Clinical data is full of nulls — handle gracefully everywhere
Missing vitals ≠ normal vitals
Missing medication ≠ no medications
Always distinguish "not recorded" from "not present"
```

---

## File: `skills/delta-patterns.md`

```markdown
# Skill: Delta Lake Patterns

Rules for working with Delta Lake tables.
Apply for scribe-iq-lakehouse and any data engineering work.

## Core rules
1. CDC always enabled on Silver and Gold tables

ALTER TABLE silver.soap_note
SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

2. MERGE preferred over overwrite for Silver tables
   Overwrite drops history — MERGE preserves it

from delta.tables import DeltaTable

def merge_silver(spark, new_df, target_path, merge_key):
    if DeltaTable.isDeltaTable(spark, target_path):
        dt = DeltaTable.forPath(spark, target_path)
        (dt.alias("target")
           .merge(new_df.alias("source"),
                  f"target.{merge_key} = source.{merge_key}")
           .whenMatchedUpdateAll()
           .whenNotMatchedInsertAll()
           .execute())
    else:
        new_df.write.format("delta").save(target_path)

3. Never partition by high-cardinality columns
   Good partition keys: date, cohort, specialty, year
   Bad partition keys: patient_id, encounter_id, UUID

4. Z-ORDER on frequently filtered columns

OPTIMIZE silver.soap_note
ZORDER BY (patient_id, encounter_id);

5. Schema evolution — set explicitly, never rely on defaults

df.write.format("delta") \
    .option("mergeSchema", "true") \
    .mode("append") \
    .save(path)

6. Checkpoint location must be unique per stream
   Never reuse a checkpoint directory for different streams

7. Vacuum default retention is 7 days — don't reduce below that

## Medallion rules
Bronze:
  - Append-only, never modify raw data
  - Raw files as-is, no transforms
  - Partition by cohort/date for streaming simulation
  - Log file count, size, timestamp to _metadata/

Silver:
  - Validated, typed, CDC-enabled
  - MERGE not overwrite
  - Referential integrity checked before write
  - ingest_timestamp on every row

Gold:
  - Denormalized for downstream consumers
  - Corpus contract defines schema guarantee
  - validation_passed=True enforced on AI-generated rows
  - Lineage back to Silver via version columns

## Streaming simulation (Synthea Coherent)
Auto Loader watches Bronze for new cohort partitions:

df = (spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.schemaLocation", f"{BRONZE}/_schemas/")
      .load(f"{BRONZE}/fhir/"))

Simulate stream by dropping cohort partitions sequentially.
Each partition = one "batch" of patient records arriving.

## Platform paths
Never hardcode — always use platform.storage_path():

# Good
path = platform.storage_path("silver", "soap_note")

# Bad
path = "abfss://lakehouse@onelake.dfs.fabric.microsoft.com/silver/soap_note"

## delta-rs (local Polars tier)
from deltalake import write_deltalake, DeltaTable

# Read as Arrow (zero-copy to Polars)
dt = DeltaTable(path)
df = pl.from_arrow(dt.to_pyarrow())

# Write
write_deltalake(path, df.to_arrow(), mode="append")

## Quality gates
After every Silver write, validate before continuing:
  - Row count >= minimum threshold
  - Required columns non-null
  - Referential integrity (FKs resolve)
  - Clinical range checks (heart rate 30-250, age 0-130)
  - Row count drift < 10% vs last run

## Common gotchas
- Fabric uses abfss:// paths, local uses file:// or relative
- DeltaTable.isDeltaTable() before first MERGE
- Auto Loader schema inference can fail on first run — provide hint
- CDC readChangeFeed requires startingVersion or startingTimestamp
- Z-ORDER rewrites all files — expensive on large tables, run off-hours
```

---

## File: `skills/mlops.md`

```markdown
# Skill: MLOps Patterns

Rules for ML experiment tracking, model registry, and serving.
Apply for clinical-bert-pipeline and any ML work.

## params.yaml is single source of truth
Every hyperparameter comes from params.yaml — never hardcoded.
Changing a hyperparameter = one line commit, fully tracked.

# Good — reads from params
lr = config["training"]["learning_rate"]
batch_size = config["training"]["batch_size"]

# Bad — hardcoded
lr = 2e-5
batch_size = 16

## MLflow discipline
1. autolog is the floor — always enable it first

import mlflow.transformers
mlflow.transformers.autolog()

2. Add custom metrics on top of autolog

mlflow.log_metric("weighted_f1", weighted_f1, step=epoch)
mlflow.log_metric("ood_f1", ood_f1, step=epoch)  # always log OOD

3. Log artifacts: eval_report.json, confusion matrix, model card

mlflow.log_artifact("outputs/eval_report.json")

4. Log params that autolog misses

mlflow.log_params({
    "primary_data_source": config["data"]["primary"]["source"],
    "auxiliary_data_source": config["data"]["auxiliary"]["source"],
    "mixing_ratio": "70/30",
    "platform": "M1_Max_MPS",
})

## eval_report.json
Always written. Always committed. Never gitignored.
This is a portfolio artifact — it shows honest evaluation.

Required fields:
{
  "task": "...",
  "training_sources": {...},
  "base_model": "...",
  "in_distribution": {"weighted_f1": 0.0, "dataset": "..."},
  "out_of_distribution": {"weighted_f1": 0.0, "dataset": "..."},
  "gate_threshold": 0.75,
  "gate_passed": true,
  "eval_date": "YYYY-MM-DD",
  "mlflow_run_id": "...",
  "model_note": "honest limitation statement"
}

OOD F1 is always reported alongside in-distribution F1.
This is the number that proves generalization.

## Model registration
Only register if F1 >= gate_threshold.

if metrics["weighted_f1"] >= config["evaluation"]["f1_gate"]:
    mlflow.register_model(
        f"runs:/{run_id}/model",
        config["model_name"]
    )
else:
    logger.warning(
        "F1 %.3f below gate %.3f — not registering",
        metrics["weighted_f1"],
        config["evaluation"]["f1_gate"]
    )

## CI eval gate
Every PR runs eval against registered Production model.
Fails if F1 drops below threshold.
Gate threshold lives in params.yaml — not hardcoded in CI.

## DVC
Track data files, never commit them to git.
dvc.yaml defines pipeline stages with deps and outs.
params.yaml is tracked by DVC for hyperparameter versioning.

dvc add data/raw/mtsamples.csv
git add data/raw/mtsamples.csv.dvc .gitignore
git commit -m "chore(dvc): track mtsamples dataset"

## M1 Max Apple Silicon
fp16=False — MPS does not support fp16
bf16=False — also unsupported
use_mps_device=True — explicit MPS
batch_size=32 — 32GB unified memory handles it

training_args = TrainingArguments(
    fp16=False,
    bf16=False,
    use_mps_device=torch.backends.mps.is_available(),
    per_device_train_batch_size=32,
    ...
)

## Multi-source training
When mixing datasets, use weighted sampling per batch.
Never just concatenate — the model sees imbalanced distributions.

Primary source (70%): Synthea SOAP notes — strong labels
Auxiliary source (30%): MTSamples — linguistic variety

Document mixing ratio in params.yaml and eval_report.json.

## Model card discipline
Every trained model gets a model_card.md committed to the repo.
Required sections:
  1. Model description and task
  2. Training data and sources
  3. Evaluation results (both in-dist and OOD)
  4. Intended use
  5. Out-of-scope use
  6. Known limitations (honest, specific)
  7. Roadmap (what improves it)
  8. Data governance

Limitations are not weaknesses — they're honest engineering.
A reviewer who finds an undocumented limitation loses trust.
A reviewer who finds a documented limitation gains it.
```

---

## File: `templates/CLAUDE.md.project`

> Copy this to project root as CLAUDE.md
> Fill in {PLACEHOLDERS} and paste relevant skill content

```markdown
# {PROJECT_NAME} — Claude Code Configuration

## Project
{One paragraph: what this project does, what it feeds, what it produces.}

## Developer context
- M1 Max 32GB (M5 Max 128GB arriving June 2026)
- MPS: fp16=False, batch_size=32 for PyTorch
- Fabric trial: ~{N} days remaining — Fabric work is priority
- Active job hunt — portfolio quality matters, ship > perfect
- claude-os: ~/claude-os/

## System context
{How this repo connects to other repos in the system.}
{What it consumes, what it produces, what depends on it.}

## Architecture principles
{3-5 key architectural decisions for this project.}
{Reference ADRs where applicable.}
- Full spec: docs/SPEC.md

## Current status
See: HANDOFF.md (updated end of every session)
See: CHANGELOG.md (updated every meaningful commit)

## Non-negotiables
{Project-specific rules that must never be violated.}
1. {Rule 1}
2. {Rule 2}
3. Session ends with updated HANDOFF.md — always

## Session protocol
START: Read HANDOFF.md → state current status → begin first task
END:   HANDOFF.md → CHANGELOG.md → tests → ADRs → commit

## Key files
  docs/SPEC.md               Full implementation spec
  docs/adr/                  Architectural decision records
  HANDOFF.md                 Current session state
  CHANGELOG.md               All meaningful changes
  {key file 1}               {description}
  {key file 2}               {description}

---

## Skills

{PASTE RELEVANT SKILL CONTENT BELOW}
{Select from: handoff, adr, changelog, python,}
{healthcare-data, delta-patterns, mlops}

{Paste full content of each relevant skill file here}
{The project CLAUDE.md is self-contained — no external references}
```

---

## File: `templates/HANDOFF.md`

```markdown
# HANDOFF — Session {N}
**Date:** {YYYY-MM-DD HH:MM}
**Repo:** {repo-name}
**Branch:** {branch}

---

## Session summary
{3-5 sentences: what was accomplished, what state the system is in.}

---

## Current state

**Working:**
- {thing that is fully implemented and tested}
- {another working thing}

**In progress:**
- {what was mid-flight when session ended}
- File: {path/to/file.py}, function: {function_name}, line: {N}

**Blocked:**
- {anything needing external input, decision, or dependency}

---

## Test status
```
pytest tests/ — {N} passed, {N} failed, {N} skipped
```

Failing tests:
- {test_name}: {reason}

---

## Next session — start here

**First task:** {specific, one sentence, actionable}
**Read first:** {file or doc to read before starting}
**Decision needed:** {any open question to resolve before coding}

---

## Open decisions

| Decision | Options | Recommendation | Status |
|----------|---------|----------------|--------|
| {decision} | {A / B} | {recommendation} | Open |

---

## Key state

```
{env vars, config values, last known counts}
{e.g. LAKEHOUSE_PLATFORM=fabric}
{e.g. Last notebook completed: 05_silver_soap_notes}
{e.g. Silver tables written: patient, encounter, condition, soap_note}
{e.g. Fabric cohort size: 20 patients (dev mode)}
```

---

## Files changed this session
- {path/to/file.py} — {one-line description of change}
- {path/to/notebook.ipynb} — {description}

## ADRs written this session
- {ADR-NNN: title}
```

---

## File: `init.sh`

```bash
#!/bin/bash
# claude-os/init.sh
# Usage: bash ~/claude-os/init.sh [project-name] [skill1] [skill2] ...
# Example: bash ~/claude-os/init.sh my-project healthcare-data delta-patterns mlops

PROJECT_NAME=${1:-"my-project"}
CLAUDE_OS_DIR="$(dirname "$0")"
SKILLS_DIR="$CLAUDE_OS_DIR/skills"
TEMPLATES_DIR="$CLAUDE_OS_DIR/templates"

echo "Initializing Claude Code for: $PROJECT_NAME"

# Create docs/adr directory
mkdir -p docs/adr

# Copy HANDOFF.md template
if [ ! -f HANDOFF.md ]; then
    cp "$TEMPLATES_DIR/HANDOFF.md" HANDOFF.md
    echo "Created HANDOFF.md"
fi

# Create CHANGELOG.md if missing
if [ ! -f CHANGELOG.md ]; then
    cat > CHANGELOG.md << EOF
# Changelog

## [Unreleased]

## [0.1.0] — $(date +%Y-%m-%d)
### Added
- Initial project setup
EOF
    echo "Created CHANGELOG.md"
fi

# Create docs/adr/README.md
if [ ! -f docs/adr/README.md ]; then
    cat > docs/adr/README.md << EOF
# ADR Index

| # | Title | Status | Date |
|---|-------|--------|------|
EOF
    echo "Created docs/adr/README.md"
fi

# Assemble CLAUDE.md from template + selected skills
CLAUDE_MD="CLAUDE.md"
cp "$TEMPLATES_DIR/CLAUDE.md.project" "$CLAUDE_MD"
sed -i "s/{PROJECT_NAME}/$PROJECT_NAME/g" "$CLAUDE_MD"

# Append selected skills
shift  # remove project name from args
for SKILL in "$@"; do
    SKILL_FILE="$SKILLS_DIR/$SKILL.md"
    if [ -f "$SKILL_FILE" ]; then
        echo "" >> "$CLAUDE_MD"
        echo "---" >> "$CLAUDE_MD"
        cat "$SKILL_FILE" >> "$CLAUDE_MD"
        echo "Appended skill: $SKILL"
    else
        echo "Warning: skill not found: $SKILL"
    fi
done

echo ""
echo "Done. Next steps:"
echo "  1. Edit CLAUDE.md — fill in {PLACEHOLDERS}"
echo "  2. Add project-specific non-negotiables"
echo "  3. Run: git add CLAUDE.md HANDOFF.md CHANGELOG.md docs/"
echo "  4. Start Claude Code session: 'Read HANDOFF.md and begin'"
```

---

## Assembled Project CLAUDE.mds

### `scribe-iq-lakehouse/CLAUDE.md` (complete, ready to use)

```markdown
# scribe-iq-lakehouse — Claude Code Configuration

## Project
Production-pattern healthcare data lakehouse on Synthea Coherent
(~1,500 synthetic patients). Fabric-first medallion: Bronze → Silver
→ Gold. Feeds scribe-iq (RAG) and clinical-bert-pipeline (NLP).
Ollama generation pipeline produces patient-linked dialogues from Gold.

## Developer context
- M1 Max 32GB (M5 Max 128GB arriving June 2026)
- Fabric trial: ~15 days remaining — Fabric notebooks are priority
- LAKEHOUSE_PLATFORM env var controls execution environment
- Active job hunt — capture Fabric screenshots before trial expires
- claude-os: ~/claude-os/

## System context
Consumes: Synthea Coherent S3 (s3://synthea-open-data/coherent/)
Produces: gold.encounter_summary (corpus contract)
          gold.synthetic_dialogue (Ollama generation)
Feeds: scribe-iq (RAG corpus), clinical-bert-pipeline (NLP training)
Current scribe-iq corpus: 19 patients (dev) → replacing with 1,500

## Architecture principles
- Platform abstraction: ALL cloud I/O via local/platform/
- Arrow interchange: transforms return pa.Table, never DataFrames
- Transforms are pure: no file paths, no platform imports in transforms/
- Notebooks import from local/transforms/ — zero duplicate logic
- Every notebook follows the 8-cell documentation template
- Full spec: docs/SPEC.md, MASTER_PLAN.md

## Non-negotiables
1. Never hardcode OneLake paths — always platform.storage_path()
2. Never import Fabric/Spark in local/transforms/
3. Every new transform gets a test in tests/
4. Every architectural decision gets an ADR in docs/adr/
5. CDC enabled on all Silver tables (delta.enableChangeDataFeed=true)
6. data_limitation column always populated in silver.genomic_report
7. pydicom stop_before_pixels=True — never load pixel data
8. fhir_parser.py has zero platform imports
9. Session ends with updated HANDOFF.md

## Session protocol
START: Read HANDOFF.md → confirm current state → read relevant spec
END:   HANDOFF.md → CHANGELOG.md → tests → ADRs → commit

## Key files
  docs/SPEC.md                    Full implementation spec
  MASTER_PLAN.md                  Cross-repo weekend plan
  docs/CORPUS_CONTRACT.md         Handoff schema to downstream AI
  docs/adr/                       ADRs — read before touching architecture
  HANDOFF.md                      Current session state
  local/platform/base.py          Platform abstraction interface
  local/transforms/               Engine-agnostic transform logic
  fabric/notebooks/               Fabric execution + documentation
  gold/generation/                Ollama corpus generation pipeline

## Screenshot capture (before trial expires)
If pressed for time: capture screenshots BEFORE polishing.
Evidence of running system > polished code not captured.
See docs/SPEC.md section 15.2 for full checklist.

---

# SKILL: Handoff Protocol
[paste skills/handoff.md content here]

---

# SKILL: ADR Practice
[paste skills/adr.md content here]

---

# SKILL: Changelog
[paste skills/changelog.md content here]

---

# SKILL: Python Conventions
[paste skills/python.md content here]

---

# SKILL: Healthcare Data Patterns
[paste skills/healthcare-data.md content here]

---

# SKILL: Delta Lake Patterns
[paste skills/delta-patterns.md content here]
```

---

### `clinical-bert-pipeline/CLAUDE.md` (complete, ready to use)

```markdown
# clinical-bert-pipeline — Claude Code Configuration

## Project
Production MLOps pipeline for clinical NLP. ClinicalBERT fine-tune
on Synthea Coherent SOAP notes (from scribe-iq-lakehouse Silver) +
MTSamples. MLflow tracking, DVC versioning, FastAPI serving, Streamlit
showcase. Ships complete this weekend.

## Developer context
- M1 Max 32GB — fp16=False, bf16=False, batch_size=32, use_mps=True
- Training time: ~90 min on M1 Max MPS
- MLflow server: Docker Compose :5000
- Active job hunt — OOD F1 on MTSamples is the showcase metric
- claude-os: ~/claude-os/

## System context
Consumes: silver.soap_note from scribe-iq-lakehouse (primary)
          MTSamples from HuggingFace (auxiliary)
Fallback: MTSamples-only if Silver not ready (document in model card)
Produces: trained ClinicalBERT + eval_report.json
          FastAPI /predict endpoint
          Streamlit demo

## Architecture principles
- params.yaml is single source of truth — no hardcoded values
- MultiSourceDataset: 70% SOAP + 30% MTSamples per batch
- eval_report.json committed — it's a portfolio artifact
- OOD F1 (MTSamples held-out) always reported alongside in-dist
- Full spec: docs/SPEC.md, MASTER_PLAN.md

## Non-negotiables
1. fp16=False, bf16=False — MPS limitation, always
2. Every hyperparameter from params.yaml — never hardcoded
3. eval_report.json committed and never gitignored
4. OOD F1 always in eval_report.json and Streamlit metrics tab
5. Model registration only if F1 >= gate_threshold
6. DVC tracks data files — never commit large files to git
7. Session ends with updated HANDOFF.md

## Session protocol
START: Read HANDOFF.md → docker compose up → verify MLflow at :5000
END:   HANDOFF.md → CHANGELOG.md → pytest → commit

## Key files
  docs/SPEC.md               Full implementation spec
  MASTER_PLAN.md             Cross-repo weekend plan
  params.yaml                Hyperparameters — source of truth
  src/data/dataset.py        MultiSourceDataset
  src/training/train.py      Trainer + MLflow autolog
  outputs/eval_report.json   Committed baseline metrics
  docs/adr/                  Architectural decisions
  HANDOFF.md                 Current session state

---

# SKILL: Handoff Protocol
[paste skills/handoff.md content here]

---

# SKILL: ADR Practice
[paste skills/adr.md content here]

---

# SKILL: Changelog
[paste skills/changelog.md content here]

---

# SKILL: Python Conventions
[paste skills/python.md content here]

---

# SKILL: Healthcare Data Patterns
[paste skills/healthcare-data.md content here]

---

# SKILL: MLOps Patterns
[paste skills/mlops.md content here]
```

---

## Setup Instructions

### First time — link global CLAUDE.md

```bash
# Clone private repo
git clone git@github.com:sandeep-jay/claude-os.git ~/claude-os

# Create Claude Code config directory
mkdir -p ~/.claude

# Symlink global CLAUDE.md
ln -sf ~/claude-os/CLAUDE.md ~/.claude/CLAUDE.md

# Make init script executable
chmod +x ~/claude-os/init.sh
```

### For scribe-iq-lakehouse

```bash
cd ~/scribe-iq-lakehouse

# Assemble CLAUDE.md with relevant skills
bash ~/claude-os/init.sh scribe-iq-lakehouse \
    healthcare-data delta-patterns python handoff adr changelog

# Then edit CLAUDE.md — fill in project-specific non-negotiables
# The skill content is already appended
```

### For clinical-bert-pipeline

```bash
cd ~/clinical-bert-pipeline

bash ~/claude-os/init.sh clinical-bert-pipeline \
    mlops healthcare-data python handoff adr changelog
```

### For any future project

```bash
cd ~/new-project
bash ~/claude-os/init.sh new-project [skill1] [skill2] ...
```

---

## Growing claude-os Over Time

When you learn a new pattern that should be universal:

```bash
# Add a new skill
cat > ~/claude-os/skills/new-skill.md << 'EOF'
# Skill: {Name}
{content}
EOF

git -C ~/claude-os add skills/new-skill.md
git -C ~/claude-os commit -m "feat(skills): add {name} skill"
```

When a project teaches you something reusable:
1. Extract it into a skill file in claude-os
2. Reference it in the project CLAUDE.md
3. Pull it into future projects via init.sh

The repo grows with you. Every project adds to it.

---

*Document version: 1.0 — May 2026*
*Status: READY FOR EXECUTION*
*Create repo: sandeep-jay/claude-os (private)*
*Run init.sh for each project before first Claude Code session*
