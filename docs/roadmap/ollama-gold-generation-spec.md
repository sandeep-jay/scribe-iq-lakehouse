# Ollama Gold Generation Pipeline — Project Spec

**Portfolio project:** Sandeep Jayaprakash
**Repo:** `scribe-iq-lakehouse` — `gold/generation/` module
**Status:** Implementation-ready
**Implementation:** Claude Code
**Hardware:** M1 Max 32GB — Ollama + local models
**Depends on:** scribe-iq-lakehouse Silver + Gold encounter_summary

---

## 1. Purpose and Portfolio Narrative

Takes the validated `gold.encounter_summary` from the lakehouse
and generates two synthetic artifacts per encounter:

1. **Doctor-patient dialogue** — NoteChat-style clinical conversation
   grounded strictly in the SOAP note and encounter facts
2. **Unstructured progress note** — free-text clinical note in a
   different format from the structured SOAP, adding corpus diversity

These outputs feed:
- `scribe-iq` — richer, more diverse RAG corpus
- `clinical-bert-pipeline` — training data variety beyond SOAP templates
- `silver.soap_note` enrichment — BERT NER annotations (Phase 3)

### What this signals to employers

| Signal | How it shows |
|---|---|
| Clinical NLP depth | Multi-stage pipeline with clinical grounding, fact validation |
| LLM orchestration | Three-stage generate → polish → validate pattern |
| Production discipline | Resumable pipeline, checkpointing, quality gates, audit trail |
| Healthcare AI judgment | Hallucination detection, clinical fact preservation, honest limitations |
| System design | Reads from lakehouse contract, writes back to Gold, full lineage |

---

## 2. Generation Tasks

### Task A — Synthetic Dialogue

**Input:** `gold.encounter_summary` row (patient facts + SOAP note)
**Output:** Doctor-patient dialogue, 8-15 turns
**Model pipeline:** qwen3-medical (plan + roleplay) → gemma4-polish (refine)
**Validation:** qwq-checker (clinical fact accuracy)
**Table:** `gold.synthetic_dialogue`

**Why this adds value over SOAP notes alone:**
- Dialogue is the natural format for ACI-Bench style NLP tasks
- Tests model's ability to translate clinical jargon into patient language
- Adds conversational register to corpus — SOAP notes are one-voice only
- Enables dialogue-grounded RAG queries (*"What did the doctor say about my kidneys?"*)

### Task B — Unstructured Progress Note

**Input:** `gold.encounter_summary` row (same input as Task A)
**Output:** Free-text progress note in different format from SOAP
            (narrative style, not structured S/O/A/P headers)
**Model pipeline:** qwen3-medical or medgemma-dialogue (single stage)
**Validation:** structural check + entity overlap with source facts
**Table:** `gold.synthetic_note_unstructured`

**Why this adds value:**
- SOAP notes are template-driven — too regular for BERT training
- Unstructured notes add linguistic variety for NER and classification
- Reflects real clinical note diversity (not all notes are SOAP format)
- Progress notes, discharge summaries, referral letters — different registers

---

## 3. Runtime Reality — M1 Max 32GB

### Time estimates per note

```
Stage               Model               Time/note   Memory
──────────────────  ──────────────────  ──────────  ──────────────
Task A Stage 1      qwen3:8b (fast)     ~2 sec      ~5 GB
Task A Stage 2      qwen3-medical       ~25 sec     ~20 GB
Task A Stage 3      gemma4-polish-small ~20 sec     +9.6 GB = ~25 GB
Task A Validation   Layer 1+2 only      ~0 sec      no model call
──────────────────────────────────────────────────────────────────
Task A total (portfolio run)            ~47 sec/note

Full pipeline with QwQ validation:     ~62 sec/note
Reserved for M5 Max full corpus run.
```

### Portfolio corpus target — 200 notes

```
Context: scribe-iq currently has 19 patients (dev corpus)
         Synthea Coherent has ~1,500 patients
         200 notes = stratified sample from 1,500-patient cohort

200 encounters × 47 sec = ~2.5 hours
Runs Sunday afternoon — not overnight
~150-160 unique patients (some contribute 2 encounters)

This is enough for:
  Pipeline validation ✓
  Quality baseline committed ✓
  5 sample outputs committed ✓
  BERT dialogue training augmentation ✓
  Scribe IQ patient-linked layer (thin but working) ✓

NOT enough for:
  Full RAG demo without existing corpus ✗
  Cross-patient aggregation queries ✗
  → These wait for M5 full corpus run (June)
```

### M5 Max 128GB — full corpus plan (June)

```
Available: June 2, 2026 (one setup day)
Start generation: June 3

M5 advantages:
  qwen3:32b + gemma4:31b + qwq:32b all in memory (~58 GB)
  No model swapping — all stages cached
  ~23 sec/note vs ~47 sec on M1 Max

Full corpus timeline on M5:
  6,300 notes × 23 sec = ~40 hours
  Both tasks: ~31 sec/note × 6,300 = ~55 hours
  2-3 overnight runs — done June 3-6

After M5 run:
  scribe-iq corpus migration (replace data_prep/)
  BERT retrain on full corpus
  Full RAG demo with 1,500 patients
```

### Two-machine strategy (after M5 arrives)

```
M5 Max 128GB         Primary dev machine — BERT, Fabric, code
M1 Max 32GB          Dedicated generation server
                     Always-on Ollama, SSH-triggered runs
                     No competing workloads

From M5:
  ssh m1max "bash ~/scribe-iq-lakehouse/gold/generation/scripts/run_overnight.sh"
  ssh m1max "tail -f logs/generation/latest.log"
```

---

## 4. Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│              INPUT — gold.encounter_summary                     │
│  patient facts + SOAP note + ECG + imaging + medications        │
└──────────────────────────┬──────────────────────────────────────┘
                           │
              ┌────────────▼────────────┐
              │   Corpus Builder        │
              │   gold/generation/      │
              │   pipeline.py           │
              │                         │
              │   Reads checkpoint      │
              │   Skips completed IDs   │
              │   Processes in batches  │
              └────────┬────────────────┘
                       │
        ┌──────────────┴──────────────┐
        │                             │
┌───────▼──────────┐         ┌───────▼──────────┐
│   TASK A         │         │   TASK B          │
│   Dialogue       │         │   Unstructured    │
│   Generation     │         │   Note            │
└───────┬──────────┘         └───────┬──────────┘
        │                             │
   ┌────▼────┐                   ┌────▼────┐
   │Stage 1  │                   │Generate │
   │Plan     │                   │qwen3 or │
   │qwen3    │                   │medgemma │
   └────┬────┘                   └────┬────┘
        │                             │
   ┌────▼────┐                   ┌────▼────┐
   │Stage 2  │                   │Validate │
   │Roleplay │                   │Entity   │
   │qwen3    │                   │overlap  │
   └────┬────┘                   └────┬────┘
        │                             │
   ┌────▼────┐                        │
   │Stage 3  │                        │
   │Polish   │                        │
   │gemma4   │                        │
   └────┬────┘                        │
        │                             │
   ┌────▼────┐                        │
   │Validate │                        │
   │qwq fact │                        │
   │checker  │                        │
   └────┬────┘                        │
        │                             │
        └──────────────┬──────────────┘
                       │
        ┌──────────────▼──────────────┐
        │   Write to Gold             │
        │   gold.synthetic_dialogue   │
        │   gold.synthetic_note_      │
        │      unstructured           │
        │   gold.generation_audit     │
        │   Update checkpoint         │
        └─────────────────────────────┘
```

---

## 5. Repository Structure

```
scribe-iq-lakehouse/
│
└── gold/
    └── generation/
        │
        ├── pipeline.py              # Main entrypoint — orchestrates all tasks
        ├── checkpoint.py            # Resumable state management
        ├── config.py                # Generation config, model selection
        │
        ├── tasks/
        │   ├── __init__.py
        │   ├── dialogue.py          # Task A — three-stage dialogue generation
        │   └── unstructured_note.py # Task B — free-text note generation
        │
        ├── stages/
        │   ├── __init__.py
        │   ├── planner.py           # Stage 1 — extract keywords + turn order
        │   ├── roleplay.py          # Stage 2 — generate raw dialogue
        │   ├── polisher.py          # Stage 3 — naturalness refinement
        │   └── validator.py         # QwQ clinical fact validation
        │
        ├── models/
        │   ├── __init__.py
        │   ├── client.py            # Ollama API client wrapper
        │   ├── manager.py           # Model loading, memory management
        │   └── prompts.py           # All prompt templates, versioned
        │
        ├── io/
        │   ├── __init__.py
        │   ├── reader.py            # Read gold.encounter_summary
        │   └── writer.py            # Write to Gold tables + audit
        │
        ├── few_shots/
        │   ├── README.md            # Attribution: ACI-Bench, NoteChat
        │   ├── dialogue_examples.json   # 5 hand-curated dialogue examples
        │   └── note_examples.json       # 5 hand-curated note examples
        │
        ├── quality/
        │   ├── __init__.py
        │   ├── gates.py             # Quality thresholds and gate logic
        │   └── reporter.py          # Quality report generation
        │
        ├── tests/
        │   ├── fixtures/
        │   │   └── sample_encounter.json  # Mrs. Sarah Chen test case
        │   ├── test_dialogue.py
        │   ├── test_unstructured_note.py
        │   ├── test_validator.py
        │   └── test_checkpoint.py
        │
        ├── scripts/
        │   ├── run_sample.sh        # Quick 10-note test run
        │   └── run_overnight.sh     # Full overnight run with logging
        │
        └── reports/
            ├── quality_baseline.json    # Committed quality benchmark
            └── sample_outputs/
                ├── sample_dialogue.txt  # Example output — committed
                └── sample_note.txt      # Example output — committed
```

---

## 6. Component Specifications

### 6.1 `config.py` — Generation Configuration

```python
from dataclasses import dataclass
from enum import Enum

class ModelProfile(Enum):
    COMFORTABLE  = "comfortable"   # qwen3:30b-a3b + gemma4:e4b (~26.6 GB)
    FULL         = "full"          # qwen3:32b + gemma4:e4b (~29.6 GB)
    MEDGEMMA     = "medgemma"      # medgemma:27b solo (~18 GB)
    FAST         = "fast"          # qwen3:8b + gemma4:e4b (~15 GB)

@dataclass
class GenerationConfig:
    # Model selection
    profile: ModelProfile = ModelProfile.COMFORTABLE

    # Task control
    run_task_a: bool = True         # dialogue generation
    run_task_b: bool = True         # unstructured note generation
    run_validation: bool = True     # QwQ fact checking

    # Scale
    max_notes: int = None           # None = all available
    batch_size: int = 10            # notes per batch before checkpoint write

    # Quality gates
    min_dialogue_turns: int = 8
    max_dialogue_turns: int = 15
    min_note_words: int = 150
    max_note_words: int = 800
    max_validation_issues: int = 2  # max issues before flagging for review

    # Generation params
    dialogue_temperature: float = 0.7
    polish_temperature: float = 0.8
    note_temperature: float = 0.75

    # Few-shot
    use_few_shots: bool = True
    few_shot_count: int = 2

    # Output
    gold_path: str = "data/gold"
    checkpoint_path: str = "gold/generation/.checkpoint"

    # Model names (per runbook)
    planner_model: str = "qwen3-medical"
    roleplay_model: str = "qwen3-medical"
    polish_model_small: str = "gemma4-polish-small"
    polish_model_large: str = "gemma4-polish-large"
    validator_model: str = "qwq-checker"
    note_model: str = "qwen3-medical"
```

---

### 6.2 `checkpoint.py` — Resumable State

The single most important component. Every run reads checkpoint
before processing and writes after each batch. Zero work lost
on interruption.

```python
import json
import os
from pathlib import Path
from datetime import datetime

class GenerationCheckpoint:
    """
    Tracks generation state per encounter_id.
    Atomic writes — no checkpoint corruption on interrupt.

    State per encounter:
        pending     — not yet processed
        dialogue_done — Task A complete
        note_done   — Task B complete
        complete    — both tasks done, written to Gold
        failed      — exceeded retry limit, logged for review
        flagged     — quality gate failed, needs human review
    """

    def __init__(self, checkpoint_path: str):
        self.path = Path(checkpoint_path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.state = self._load()

    def _load(self) -> dict:
        if self.path.exists():
            with open(self.path) as f:
                return json.load(f)
        return {
            "created": datetime.now().isoformat(),
            "encounters": {},
            "stats": {
                "total": 0, "complete": 0,
                "failed": 0, "flagged": 0
            }
        }

    def _save(self):
        # Atomic write — write to temp, rename
        tmp = self.path.with_suffix(".tmp")
        with open(tmp, "w") as f:
            json.dump(self.state, f, indent=2)
        tmp.rename(self.path)

    def is_complete(self, encounter_id: str) -> bool:
        return (self.state["encounters"]
                .get(encounter_id, {})
                .get("status") == "complete")

    def mark_dialogue_done(self, encounter_id: str,
                            dialogue_id: str):
        self._update(encounter_id, {
            "status": "dialogue_done",
            "dialogue_id": dialogue_id,
            "dialogue_ts": datetime.now().isoformat()
        })

    def mark_complete(self, encounter_id: str,
                       note_id: str):
        self._update(encounter_id, {
            "status": "complete",
            "note_id": note_id,
            "completed_ts": datetime.now().isoformat()
        })
        self.state["stats"]["complete"] += 1
        self._save()

    def mark_failed(self, encounter_id: str,
                     reason: str, attempt: int):
        self._update(encounter_id, {
            "status": "failed" if attempt >= 3 else "pending",
            "failure_reason": reason,
            "attempts": attempt
        })
        if attempt >= 3:
            self.state["stats"]["failed"] += 1
        self._save()

    def mark_flagged(self, encounter_id: str,
                      issues: list[str]):
        self._update(encounter_id, {
            "status": "flagged",
            "validation_issues": issues,
            "flagged_ts": datetime.now().isoformat()
        })
        self.state["stats"]["flagged"] += 1
        self._save()

    def get_pending(self,
                     encounter_ids: list[str]) -> list[str]:
        """Return encounter IDs not yet complete."""
        return [
            eid for eid in encounter_ids
            if not self.is_complete(eid)
            and self.state["encounters"]
                .get(eid, {}).get("status") != "failed"
        ]

    def print_progress(self):
        stats = self.state["stats"]
        total = stats["total"] or 1
        pct = stats["complete"] / total * 100
        print(
            f"Progress: {stats['complete']}/{total} "
            f"({pct:.1f}%) | "
            f"Failed: {stats['failed']} | "
            f"Flagged: {stats['flagged']}"
        )

    def _update(self, encounter_id: str, updates: dict):
        if encounter_id not in self.state["encounters"]:
            self.state["encounters"][encounter_id] = {}
        self.state["encounters"][encounter_id].update(updates)
```

---

### 6.3 `models/client.py` — Ollama API Client

```python
import requests
import json
from typing import Generator

OLLAMA_URL = "http://localhost:11434"

class OllamaClient:

    def __init__(self, base_url: str = OLLAMA_URL,
                  timeout: int = 300):
        self.base_url = base_url
        self.timeout = timeout

    def generate(self, model: str, prompt: str,
                  think: bool = False,
                  temperature: float = 0.7,
                  num_ctx: int = 32768) -> str:
        """Blocking generation — returns full response."""
        payload = {
            "model": model,
            "messages": [{"role": "user", "content": prompt}],
            "stream": False,
            "options": {
                "temperature": temperature,
                "num_ctx": num_ctx,
                "think": think
            }
        }
        resp = requests.post(
            f"{self.base_url}/api/chat",
            json=payload,
            timeout=self.timeout
        )
        resp.raise_for_status()
        return resp.json()["message"]["content"]

    def generate_stream(self, model: str,
                         prompt: str) -> Generator[str, None, None]:
        """Streaming generation — yields tokens."""
        payload = {
            "model": model,
            "messages": [{"role": "user", "content": prompt}],
            "stream": True
        }
        with requests.post(
            f"{self.base_url}/api/chat",
            json=payload,
            stream=True,
            timeout=self.timeout
        ) as resp:
            for line in resp.iter_lines():
                if line:
                    chunk = json.loads(line)
                    if not chunk.get("done"):
                        yield chunk["message"]["content"]

    def loaded_models(self) -> list[dict]:
        """Returns currently loaded models and memory usage."""
        resp = requests.get(f"{self.base_url}/api/ps")
        return resp.json().get("models", [])

    def health_check(self) -> bool:
        try:
            resp = requests.get(self.base_url, timeout=5)
            return resp.status_code == 200
        except:
            return False
```

---

### 6.4 `models/prompts.py` — All Prompt Templates

All prompts are versioned. Changing a prompt is a tracked commit.
The `prompt_hash` is logged in the audit table — every output
is traceable to the exact prompt that generated it.

```python
import hashlib
from dataclasses import dataclass

@dataclass
class Prompt:
    name: str
    version: str
    template: str

    @property
    def hash(self) -> str:
        return hashlib.sha256(
            self.template.encode()
        ).hexdigest()[:12]

    def render(self, **kwargs) -> str:
        return self.template.format(**kwargs)


PROMPTS = {

    "stage1_plan": Prompt(
        name="stage1_plan",
        version="1.0",
        template="""You are preparing a doctor-patient consultation dialogue.

Given this clinical encounter summary, extract the structured information
needed to write a realistic dialogue.

ENCOUNTER SUMMARY:
Patient: {patient_age}yo {patient_gender}
Encounter type: {encounter_type}
Active conditions: {active_conditions}
Current medications: {active_medications}
Recent vitals: {recent_vitals}
ECG finding: {ecg_finding}
Imaging: {imaging_description}

SOAP NOTE:
{soap_note_text}

Output ONLY valid JSON with this structure:
{{
  "chief_complaint": "...",
  "key_symptoms": ["..."],
  "key_findings": ["..."],
  "diagnosis_summary": "...",
  "plan_items": ["..."],
  "medications_to_discuss": ["..."],
  "patient_concerns_likely": ["..."],
  "consultation_flow": ["chief complaint", "symptom history", ...]
}}"""
    ),

    "stage2_roleplay": Prompt(
        name="stage2_roleplay",
        version="1.0",
        template="""Generate a realistic doctor-patient dialogue for this clinical encounter.

CONSULTATION PLAN:
{plan_json}

CLINICAL GROUND TRUTH (never invent facts not in this summary):
{encounter_summary}

FEW-SHOT EXAMPLES:
{few_shot_examples}

Rules:
- 8-12 dialogue turns
- Doctor: professional, guides consultation, explains in plain language
- Patient: natural, sometimes anxious, uses lay terms not medical jargon
- Ground every clinical fact in the plan above
- Never invent diagnoses, medications, or findings not listed
- Format: "Doctor: [text]\\nPatient: [text]"
- Start with greeting and chief complaint

Generate the dialogue:"""
    ),

    "stage3_polish": Prompt(
        name="stage3_polish",
        version="1.0",
        template="""Polish this doctor-patient dialogue to sound natural and human.

Preserve ALL clinical facts exactly. Improve:
- Patient voice: warm, sometimes worried, uses everyday language
- Doctor voice: reassuring, clear, empathetic, avoids jargon with patients
- Natural conversation flow — no robotic or repetitive phrasing
- Distinct voices — doctor and patient sound like different people

RAW DIALOGUE:
{raw_dialogue}

Output ONLY the polished dialogue, no commentary:"""
    ),

    "validate_dialogue": Prompt(
        name="validate_dialogue",
        version="1.0",
        template="""You are a clinical accuracy validator.

Compare this generated dialogue against the source clinical facts.
Identify any factual errors, hallucinations, or inconsistencies.

SOURCE FACTS:
Conditions: {active_conditions}
Medications: {active_medications}
Key findings: {key_findings}
Diagnosis: {diagnosis_summary}
Plan: {plan_items}

GENERATED DIALOGUE:
{dialogue}

Output ONLY valid JSON:
{{
  "issues": [
    {{"type": "hallucination|contradiction|omission",
      "description": "...",
      "severity": "critical|minor"}}
  ],
  "overall_accuracy": "pass|fail",
  "critical_issue_count": 0
}}

If no issues found: {{"issues": [], "overall_accuracy": "pass", "critical_issue_count": 0}}"""
    ),

    "generate_unstructured_note": Prompt(
        name="generate_unstructured_note",
        version="1.0",
        template="""Write a free-text clinical progress note for this patient encounter.

This should be a narrative-style note, NOT structured SOAP format.
Write as a clinician would in a real hospital system — flowing prose
that covers the key clinical points without explicit S/O/A/P headers.

ENCOUNTER FACTS:
Patient: {patient_age}yo {patient_gender}
Encounter: {encounter_type} on {encounter_date}
Conditions: {active_conditions}
Medications: {active_medications}
Vitals: {recent_vitals}
Labs: {recent_labs}
ECG: {ecg_finding}
Imaging: {imaging_description}

SOAP NOTE (use as ground truth — do not add facts not present here):
{soap_note_text}

FEW-SHOT EXAMPLES:
{few_shot_examples}

Rules:
- 150-600 words
- Narrative prose — no bullet points, no section headers
- Clinical but readable — write as a hospitalist would
- Ground every fact in the SOAP note above
- Include assessment and plan naturally in the narrative
- Never invent medications, diagnoses, or findings not listed

Write the progress note:"""
    ),

}
```

---

### 6.5 `few_shots/dialogue_examples.json`

Five hand-curated examples covering:
- Simple primary care encounter
- Complex multi-problem (like Mrs. Sarah Chen)
- Emergency presentation
- Chronic disease management
- Mental health screening

These are written by hand, not LLM-generated. They teach
the model the register and format, not specific clinical content.

```json
[
  {
    "id": "fs_dialogue_001",
    "source": "ACI-Bench inspired",
    "encounter_type": "AMB",
    "complexity": "simple",
    "example": "Doctor: Good morning. What brings you in today?\nPatient: ..."
  }
]
```

**Attribution note in `few_shots/README.md`:**
Dialogue style inspired by ACI-Bench and NoteChat published datasets.
No actual examples from those datasets are reproduced here.
All few-shot examples are original and hand-written for this project.

---

### 6.6 `stages/validator.py` — Clinical Fact Validation

```python
import json
from models.client import OllamaClient
from models.prompts import PROMPTS
from quality.gates import ValidationResult

class ClinicalValidator:

    def __init__(self, client: OllamaClient, config):
        self.client = client
        self.config = config

    def validate_dialogue(self, dialogue: str,
                           encounter: dict) -> ValidationResult:
        """
        Three-layer validation:
        Layer 1 — Structural: turn count, format check (fast, no model)
        Layer 2 — Entity overlap: do medications/conditions appear? (fast)
        Layer 3 — QwQ semantic check: factual accuracy (slow, model call)

        Only run Layer 3 if Layers 1-2 pass.
        Saves ~15 seconds per note when structural issues caught early.
        """
        # Layer 1 — structural
        turns = self._count_turns(dialogue)
        if turns < self.config.min_dialogue_turns:
            return ValidationResult(
                passed=False,
                issues=[f"Too few turns: {turns}"],
                layer_failed=1
            )

        # Layer 2 — entity overlap
        missing = self._check_entity_overlap(dialogue, encounter)
        if len(missing) > 2:
            return ValidationResult(
                passed=False,
                issues=[f"Missing entities: {missing}"],
                layer_failed=2
            )

        # Layer 3 — QwQ semantic validation (only if layers 1-2 pass)
        if self.config.run_validation:
            return self._semantic_validate(dialogue, encounter)

        return ValidationResult(passed=True, issues=[], layer_failed=None)

    def _check_entity_overlap(self, dialogue: str,
                               encounter: dict) -> list[str]:
        """
        Check that key clinical entities from the encounter
        appear somewhere in the dialogue text.
        Medications, primary condition, and key plan items.
        Fast string matching — no model call needed.
        """
        dialogue_lower = dialogue.lower()
        missing = []

        # Check primary condition
        conditions = encounter.get("active_conditions", [])
        if conditions:
            primary = conditions[0].lower()
            # Allow partial match — "heart failure" matches "cardiac failure"
            if not any(word in dialogue_lower
                       for word in primary.split()):
                missing.append(f"condition: {conditions[0]}")

        # Check key medications (at least 50% should appear)
        meds = encounter.get("active_medications", [])
        med_names = [m.split()[0].lower() for m in meds]
        appearing = sum(1 for m in med_names if m in dialogue_lower)
        if meds and appearing / len(meds) < 0.5:
            missing.append(f"medications: only {appearing}/{len(meds)} mentioned")

        return missing

    def _semantic_validate(self, dialogue: str,
                            encounter: dict) -> ValidationResult:
        """QwQ-based semantic fact checking."""
        prompt = PROMPTS["validate_dialogue"].render(
            active_conditions=", ".join(
                encounter.get("active_conditions", [])),
            active_medications=", ".join(
                encounter.get("active_medications", [])),
            key_findings=encounter.get("recent_vitals", ""),
            diagnosis_summary=encounter.get("encounter_type", ""),
            plan_items="See encounter summary",
            dialogue=dialogue
        )

        raw = self.client.generate(
            self.config.validator_model, prompt,
            temperature=0.3
        )

        try:
            result = json.loads(raw)
            critical = result.get("critical_issue_count", 0)
            issues = [i["description"]
                      for i in result.get("issues", [])]
            return ValidationResult(
                passed=critical == 0,
                issues=issues,
                layer_failed=3 if critical > 0 else None
            )
        except json.JSONDecodeError:
            # QwQ returned non-JSON — treat as inconclusive
            return ValidationResult(
                passed=True,
                issues=["validator returned non-JSON — skipped"],
                layer_failed=None
            )
```

---

### 6.7 `io/reader.py` — Portfolio Subset Selection

```python
def select_portfolio_subset(
        encounter_summaries: list[dict],
        n: int = 200,
        seed: int = 42) -> list[dict]:
    """
    Stratified sample for maximum clinical diversity.
    Committed as gold/generation/portfolio_subset.json
    — reproducible, same 200 every run.

    Strata:
      has_soap_and_ecg:       30  richest records
      has_soap_and_imaging:   30  imaging context
      inpatient_complex:      40  multi-problem (3+ conditions)
      ambulatory_simple:      30  straightforward encounters
      emergency:              30  ER presentations
      chronic_disease:        40  diabetes/HF/CKD management
      ─────────────────────────────────────────────
      Total:                 200
    """
    random.seed(seed)
    strata = {
        "has_soap_and_ecg":     30,
        "has_soap_and_imaging": 30,
        "inpatient_complex":    40,
        "ambulatory_simple":    30,
        "emergency":            30,
        "chronic_disease":      40,
    }
    selected = []
    for stratum, count in strata.items():
        pool = _filter_stratum(encounter_summaries, stratum)
        n_select = min(count, len(pool))
        selected.extend(random.sample(pool, n_select))

    # Deduplicate, pad with random if strata underpopulated
    seen = set()
    unique = []
    for e in selected:
        if e["encounter_id"] not in seen:
            seen.add(e["encounter_id"])
            unique.append(e)

    if len(unique) < n:
        remaining = [e for e in encounter_summaries
                     if e["encounter_id"] not in seen]
        unique.extend(random.sample(remaining,
                                     min(n - len(unique),
                                         len(remaining))))
    result = unique[:n]

    # Commit subset IDs for reproducibility
    subset_ids = [e["encounter_id"] for e in result]
    with open("gold/generation/portfolio_subset.json", "w") as f:
        json.dump({"encounter_ids": subset_ids,
                   "count": len(subset_ids),
                   "seed": seed,
                   "created": datetime.now().isoformat()}, f, indent=2)

    return result
```

`portfolio_subset.json` is committed to the repo.
Anyone can reproduce exactly the 200 notes generated.
This is the audit trail.

### 6.8 `io/writer.py` — Gold Table Writer

```python
import pyarrow as pa
from deltalake import write_deltalake
from datetime import datetime
import uuid

class GoldWriter:

    def __init__(self, gold_path: str, config):
        self.gold_path = gold_path
        self.config = config

    def write_dialogue(self, encounter_id: str,
                        dialogue: str,
                        plan_json: dict,
                        validation_result,
                        prompt_hashes: dict,
                        model_config: dict) -> str:
        """
        Write synthetic dialogue to gold.synthetic_dialogue.
        Returns dialogue_id for checkpoint.
        """
        dialogue_id = str(uuid.uuid4())

        record = {
            "dialogue_id": dialogue_id,
            "encounter_id": encounter_id,
            "dialogue_text": dialogue,
            "turn_count": self._count_turns(dialogue),
            "plan_json": str(plan_json),
            "validation_passed": validation_result.passed,
            "validation_issues": str(validation_result.issues),
            "validation_layer_failed": (
                validation_result.layer_failed),
            # Audit fields — full lineage
            "planner_model": model_config["planner"],
            "roleplay_model": model_config["roleplay"],
            "polish_model": model_config["polish"],
            "validator_model": model_config["validator"],
            "planner_prompt_hash": prompt_hashes["plan"],
            "roleplay_prompt_hash": prompt_hashes["roleplay"],
            "polish_prompt_hash": prompt_hashes["polish"],
            "generation_config": str(self.config.profile),
            "generated_at": datetime.now().isoformat(),
            "spec_version": "1.0",
        }

        table = pa.Table.from_pylist(
            [record], schema=DIALOGUE_SCHEMA
        )
        write_deltalake(
            f"{self.gold_path}/synthetic_dialogue",
            table, mode="append"
        )
        return dialogue_id

    def write_unstructured_note(self, encounter_id: str,
                                 note_text: str,
                                 prompt_hash: str,
                                 model_name: str) -> str:
        note_id = str(uuid.uuid4())
        record = {
            "note_id": note_id,
            "encounter_id": encounter_id,
            "note_text": note_text,
            "word_count": len(note_text.split()),
            "note_model": model_name,
            "note_prompt_hash": prompt_hash,
            "generated_at": datetime.now().isoformat(),
            "spec_version": "1.0",
        }
        table = pa.Table.from_pylist(
            [record], schema=NOTE_SCHEMA
        )
        write_deltalake(
            f"{self.gold_path}/synthetic_note_unstructured",
            table, mode="append"
        )
        return note_id
```

---

### 6.8 `pipeline.py` — Main Entrypoint

```python
#!/usr/bin/env python3
"""
Ollama Gold Generation Pipeline.

Generates synthetic dialogues and unstructured notes
from gold.encounter_summary, grounded in SOAP notes.

Usage:
    # Full overnight run
    python gold/generation/pipeline.py

    # 10-note sample for quality validation
    python gold/generation/pipeline.py --max-notes 10 --verbose

    # Resume interrupted run
    python gold/generation/pipeline.py  # checkpoint auto-detected

    # Task A only (dialogues)
    python gold/generation/pipeline.py --task dialogue

    # Task B only (unstructured notes)
    python gold/generation/pipeline.py --task note

    # Use specific model profile
    python gold/generation/pipeline.py --profile medgemma
"""

import argparse
from rich.console import Console
from rich.progress import (Progress, SpinnerColumn,
                            TextColumn, BarColumn,
                            TimeElapsedColumn, TimeRemainingColumn)
from rich.table import Table

from config import GenerationConfig, ModelProfile
from checkpoint import GenerationCheckpoint
from models.client import OllamaClient
from models.manager import ModelManager
from io.reader import EncounterReader
from io.writer import GoldWriter
from tasks.dialogue import DialogueGenerator
from tasks.unstructured_note import NoteGenerator
from quality.reporter import QualityReporter

console = Console()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-notes", type=int, default=None)
    parser.add_argument("--task",
        choices=["dialogue", "note", "both"],
        default="both")
    parser.add_argument("--profile",
        choices=["comfortable", "full", "medgemma", "fast"],
        default="comfortable")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--batch-size", type=int, default=10)
    args = parser.parse_args()

    config = GenerationConfig(
        profile=ModelProfile(args.profile),
        max_notes=args.max_notes,
        batch_size=args.batch_size,
        run_task_a=args.task in ["dialogue", "both"],
        run_task_b=args.task in ["note", "both"],
    )

    # Health check
    client = OllamaClient()
    if not client.health_check():
        console.print("[red]✗ Ollama server not running.[/red]")
        console.print("Run: brew services start ollama")
        return 1

    console.print("[bold green]scribe-iq Gold Generation Pipeline[/bold green]")
    console.print(f"Profile: {config.profile.value}")
    console.print(f"Tasks: {'Dialogue' if config.run_task_a else ''}"
                  f"{'+ Note' if config.run_task_b else ''}\n")

    # Load checkpoint
    checkpoint = GenerationCheckpoint(config.checkpoint_path)
    checkpoint.print_progress()

    # Load encounters
    reader = EncounterReader(config.gold_path)
    all_encounters = reader.load_with_soap_notes(
        max_rows=config.max_notes
    )
    pending = checkpoint.get_pending(
        [e["encounter_id"] for e in all_encounters]
    )

    console.print(f"\nTotal encounters: {len(all_encounters)}")
    console.print(f"Pending: {len(pending)}")
    console.print(f"Estimated time: "
                  f"{len(pending) * 93 / 3600:.1f} hours\n")

    if not pending:
        console.print("[green]✓ All encounters complete![/green]")
        QualityReporter(config).print_summary(checkpoint)
        return 0

    # Initialize generators
    writer = GoldWriter(config.gold_path, config)
    dialogue_gen = DialogueGenerator(client, config, writer)
    note_gen = NoteGenerator(client, config, writer)

    # Process with progress bar
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        console=console
    ) as progress:

        task = progress.add_task(
            "Generating...", total=len(pending)
        )

        for i, encounter_id in enumerate(pending):
            encounter = next(
                e for e in all_encounters
                if e["encounter_id"] == encounter_id
            )

            try:
                if config.run_task_a:
                    dialogue_gen.generate(encounter, checkpoint)

                if config.run_task_b:
                    note_gen.generate(encounter, checkpoint)

                checkpoint.mark_complete(
                    encounter_id,
                    note_id=encounter_id + "_note"
                )

            except Exception as e:
                attempts = (checkpoint.state["encounters"]
                            .get(encounter_id, {})
                            .get("attempts", 0) + 1)
                checkpoint.mark_failed(
                    encounter_id, str(e), attempts
                )
                if args.verbose:
                    console.print(
                        f"[yellow]Failed {encounter_id}: {e}[/yellow]"
                    )

            # Checkpoint every batch
            if (i + 1) % config.batch_size == 0:
                checkpoint._save()

            progress.advance(task)

    # Final quality report
    QualityReporter(config).generate(checkpoint)
    console.print("\n[bold green]✓ Run complete[/bold green]")
    checkpoint.print_progress()
    return 0

if __name__ == "__main__":
    exit(main())
```

---

## 7. Gold Table Schemas

### `gold.synthetic_dialogue`

| Column | Type | Notes |
|---|---|---|
| dialogue_id | string | UUID |
| encounter_id | string | FK to encounter_summary |
| dialogue_text | string | Full formatted dialogue |
| turn_count | integer | Doctor + Patient turns |
| plan_json | string | Stage 1 structured plan |
| validation_passed | boolean | QwQ fact check result |
| validation_issues | string | JSON list of issues |
| validation_layer_failed | integer | 1/2/3 or null |
| planner_model | string | e.g. qwen3-medical |
| roleplay_model | string | |
| polish_model | string | |
| validator_model | string | |
| planner_prompt_hash | string | 12-char SHA256 of prompt |
| roleplay_prompt_hash | string | |
| polish_prompt_hash | string | |
| generation_config | string | ModelProfile used |
| generated_at | timestamp | |
| spec_version | string | e.g. "1.0" |

### `gold.synthetic_note_unstructured`

| Column | Type | Notes |
|---|---|---|
| note_id | string | UUID |
| encounter_id | string | FK to encounter_summary |
| note_text | string | Free-text progress note |
| word_count | integer | |
| note_model | string | Model used |
| note_prompt_hash | string | 12-char SHA256 |
| generated_at | timestamp | |
| spec_version | string | |

### `gold.generation_audit`

Per-run summary written at pipeline completion:

| Column | Type | Notes |
|---|---|---|
| run_id | string | UUID |
| run_timestamp | timestamp | |
| profile | string | ModelProfile |
| notes_attempted | integer | |
| dialogues_generated | integer | |
| dialogues_passed_validation | integer | |
| notes_generated | integer | |
| failed_count | integer | |
| flagged_count | integer | |
| avg_turn_count | float | |
| avg_note_words | float | |
| validation_pass_rate | float | |
| runtime_seconds | integer | |
| spec_version | string | |

---

## 8. Quality Gates

### Gate thresholds

```python
QUALITY_GATES = {
    "dialogue": {
        "min_turns": 8,
        "max_turns": 15,
        "min_validation_pass_rate": 0.85,
        "max_critical_issues": 0,
        "min_entity_overlap": 0.50,
    },
    "note": {
        "min_words": 150,
        "max_words": 800,
        "min_entity_overlap": 0.60,
    },
    "corpus": {
        "min_dialogues": 1000,
        "min_notes": 1000,
        "min_pass_rate": 0.85,
    }
}
```

### What to do with flagged outputs

```
passed=True, issues=[]       → Write to Gold, complete
passed=True, issues=[minor]  → Write to Gold, log issues
passed=False, critical > 0   → Mark flagged, do NOT write to Gold
failed > 3 attempts          → Mark failed, log for manual review
```

Flagged outputs are never written to Gold — they don't contaminate
the training corpus. Failed encounters are logged with the reason.
A `reports/quality_baseline.json` is committed after the first
100-note sample run — same discipline as RAGAS baseline in campus-rag.

---

## 9. Few-Shot Strategy

### What goes in `few_shots/`

Five hand-written dialogue examples, five hand-written note examples.
These are original — not copied from ACI-Bench or NoteChat.
Attribution note explains the format inspiration.

### Why hand-written, not sampled

- Sampled examples may have copyright issues if from published datasets
- Hand-written examples can be tuned specifically for the register wanted
- Control over complexity — ensure examples cover simple and complex cases
- No risk of circular training (model trained on its own outputs)

### Few-shot selection strategy

For each encounter, select the 2 most similar examples by:
- Encounter type match (AMB/IMP/ER)
- Condition count similarity
- Age group similarity (pediatric/adult/geriatric)

```python
def select_few_shots(encounter: dict,
                      examples: list[dict],
                      n: int = 2) -> list[dict]:
    """Simple similarity scoring for few-shot selection."""
    scored = []
    for ex in examples:
        score = 0
        if ex["encounter_type"] == encounter["encounter_type"]:
            score += 3
        ex_conditions = len(ex.get("conditions", []))
        enc_conditions = len(encounter.get("active_conditions", []))
        score -= abs(ex_conditions - enc_conditions)
        scored.append((score, ex))
    scored.sort(reverse=True)
    return [ex for _, ex in scored[:n]]
```

---

## 10. Overnight Run Setup

### `scripts/run_overnight.sh`

```bash
#!/bin/bash
# Overnight generation run with logging and auto-restart on failure

LOG_DIR="logs/generation"
mkdir -p "$LOG_DIR"
LOGFILE="$LOG_DIR/run_$(date +%Y%m%d_%H%M%S).log"

echo "Starting overnight generation run: $(date)" | tee "$LOGFILE"
echo "Log: $LOGFILE"

# Verify Ollama is running
if ! curl -s http://localhost:11434 > /dev/null; then
    echo "ERROR: Ollama not running. Start with: brew services start ollama"
    exit 1
fi

# Run pipeline with auto-restart on failure (max 3 retries)
MAX_RETRIES=3
RETRY=0

while [ $RETRY -lt $MAX_RETRIES ]; do
    python gold/generation/pipeline.py \
        --profile comfortable \
        --batch-size 10 \
        2>&1 | tee -a "$LOGFILE"

    EXIT_CODE=$?

    if [ $EXIT_CODE -eq 0 ]; then
        echo "✓ Pipeline completed successfully: $(date)" | tee -a "$LOGFILE"
        break
    else
        RETRY=$((RETRY + 1))
        echo "Pipeline failed (attempt $RETRY/$MAX_RETRIES). Restarting..." \
            | tee -a "$LOGFILE"
        sleep 30
    fi
done

echo "Run finished: $(date)" | tee -a "$LOGFILE"
```

### `scripts/run_sample.sh`

```bash
#!/bin/bash
# Quick 10-note quality validation run

python gold/generation/pipeline.py \
    --max-notes 10 \
    --profile comfortable \
    --verbose \
    --batch-size 10

echo "\nSample outputs:"
echo "Dialogue: reports/sample_outputs/sample_dialogue.txt"
echo "Note:     reports/sample_outputs/sample_note.txt"
```

---

## 11. Integration with Scribe IQ and BERT

### → scribe-iq

```python
# scribe-iq corpus loader reads three Gold tables:
# 1. gold.encounter_summary  → structured context
# 2. gold.synthetic_dialogue → conversational RAG
# 3. gold.synthetic_note_unstructured → note-style RAG

# Combined corpus gives diverse retrieval:
# Query "what did the doctor recommend?" matches dialogue
# Query "patient with heart failure on furosemide" matches note
# Query "BNP 1840" matches structured summary
```

### → clinical-bert-pipeline

```python
# Training data hierarchy:
# Tier 1: gold.synthetic_note_unstructured (most diverse)
# Tier 2: silver.soap_note (structured, template-driven)
# Tier 3: mtsamples (real, varied, weakly labeled)

# MultiSourceDataset weights update:
TRAINING_SOURCES = {
    "synthetic_unstructured": 0.40,  # most linguistically varied
    "synthea_soap":           0.30,  # structured, well-labeled
    "mtsamples":              0.30,  # real clinical language
}
```

---

## 12. Corpus Contract Update

The corpus contract in `docs/CORPUS_CONTRACT.md` gains two
new tables from this pipeline:

```
gold.synthetic_dialogue
  Required fields: dialogue_id, encounter_id, dialogue_text,
                   turn_count, validation_passed
  Guarantee: validation_passed=True on all rows
  Note: flagged rows never written to Gold

gold.synthetic_note_unstructured
  Required fields: note_id, encounter_id, note_text, word_count
  Guarantee: word_count >= 150, entity overlap >= 60%
  Note: free-text narrative format, not SOAP structured
```

---

## 13. Sample Output Targets

After 100-note sample run, commit these to `reports/sample_outputs/`:

**`sample_dialogue.txt`** — best output from Mrs. Sarah Chen equivalent
**`sample_note.txt`** — best unstructured note from same encounter
**`quality_baseline.json`** — pass rates, avg turn count, avg word count

These are the portfolio artifacts. A reviewer can read them
without running anything. Same philosophy as eval_report.json
in clinical-bert-pipeline.

---

## 14. Implementation Sequence for Claude Code

**Context:** Ollama sessions run Sunday only.
BERT ships Sunday morning. Ollama starts Sunday afternoon.
Generation runs Sunday afternoon (~2.5 hrs) while Fabric
screenshots are being captured.

### Session 1 — Foundation + Dialogue Task (Sunday morning)

```
Read docs (lakehouse CORPUS_CONTRACT.md, this spec sections 1-6).

1. Create module structure per section 5
2. Implement config.py — GenerationConfig with portfolio defaults:
   max_notes=200, fast_plan=True, skip_validation=True,
   run_task_b=False (Task B reserved for M5)
3. Implement checkpoint.py — atomic write pattern (section 6.2)
4. Implement models/client.py — OllamaClient
5. Implement models/prompts.py — all 5 prompts versioned
6. Implement stages/ — planner, roleplay, polisher
7. Implement stages/validator.py — Layer 1+2 only (no QwQ for portfolio run)
8. Implement tasks/dialogue.py
9. Create tests/fixtures/sample_encounter.json (Sarah Chen)
10. Write tests/ — test_checkpoint.py, test_dialogue.py
11. ADRs: portfolio-subset-strategy, skip-validation-rationale
12. HANDOFF.md, CHANGELOG.md
```

### Session 2 — IO + Pipeline + Subset (Sunday afternoon)

```
Read HANDOFF.md.

1. Implement io/reader.py — read Gold encounter_summary + subset selector
2. Implement io/writer.py — write to Gold tables with full audit
3. Implement quality/gates.py + quality/reporter.py
4. Implement pipeline.py — full entrypoint per section 6.9
5. Write few_shots/ — 5 hand-written dialogue examples
6. Write scripts/run_sample.sh, scripts/run_overnight.sh
7. END-TO-END TEST:
   python gold/generation/pipeline.py --max-notes 3 --verbose
   Verify: checkpoint written, dialogue appears in Gold
8. START PORTFOLIO RUN (background):
   python gold/generation/pipeline.py --max-notes 200
   ~2.5 hours — runs while Fabric screenshots captured
9. HANDOFF.md noting generation in progress
```

### Session 3 — Quality review + commit (Sunday evening)

```
Generation finishes (~2.5 hrs after start).

1. Review 20 random outputs manually
2. Run quality check:
   python gold/generation/pipeline.py --validate-sample 20
3. Verify quality_baseline.json written
4. Select 5 best outputs → save to reports/sample_outputs/
5. Commit: portfolio_subset.json, quality_baseline.json,
           5 sample dialogues, CHANGELOG.md
6. Update README.md — pipeline description, sample output links,
   M5 corpus roadmap, honest limitations
7. Final push ✓
```

---

## 15. CLAUDE.md Addition for This Module

Add to `scribe-iq-lakehouse/CLAUDE.md`:

```markdown
## Gold Generation module — additional rules

Location: gold/generation/

Non-negotiables:
1. Checkpoint writes are ATOMIC — always write to .tmp then rename
2. Prompts are versioned — changing a prompt = new version string
3. Flagged outputs NEVER written to Gold — quality gate is hard
4. Few-shot examples are hand-written — never LLM-generated
5. validation_passed=True is a guarantee on all Gold rows
6. prompt_hash is logged on every generation — full audit trail
7. Pipeline must survive Ctrl+C — next run picks up from checkpoint
8. Run --max-notes 3 --verbose to verify before overnight runs

Session end: always run a 3-note test before marking session done.
```

---

*Document version: 2.0 — May 2026*
*Final: 200-note portfolio corpus, M5 full corpus June,*
*Task B deferred, fast-plan mode, Sunday-only sessions,*
*stratified subset selection, scribe-iq 19-patient context*
*Status: READY FOR EXECUTION*
