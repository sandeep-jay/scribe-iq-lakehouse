# Master Execution Plan
**Owner:** Sandeep Jayaprakash  
**Last updated:** May 2026  
**Status:** READY FOR EXECUTION

---

## System Overview

Four repos. One coherent healthcare AI system.

```
fabric-lakehouse-hls-readmission   Existing — polish + publish
scribe-iq-lakehouse                New — Fabric FHIR medallion
clinical-bert-pipeline             New — MLOps NLP harness
scribe-iq                          Existing — narrative update only
```

Ollama generation lives inside `scribe-iq-lakehouse/gold/generation/`
— not a separate repo.

---

## Critical Context

```
scribe-iq current corpus    19 patients — dev corpus only
                            NOT enough for meaningful RAG demo
                            Needs lakehouse corpus to be useful

Synthea Coherent            ~1,500 patients on S3
                            ~800-1,000 with SOAP notes
                            This is the production corpus foundation

M1 Max 32GB                 Available now
M5 Max 128GB                Available June 2, 2026
                            Full 6,300-note corpus generation on M5
```

---

## What Ships This Weekend

### 1. `fabric-lakehouse-hls-readmission`
**Work:** README polish, docs cleanup, push public  
**Effort:** 1-2 hours Friday night  
**Goal:** Existing work made reviewable

### 2. `scribe-iq-lakehouse`
**Work:** Fabric Bronze + Silver FHIR pipeline, Gold encounter_summary  
**Scale:** All 1,500 Synthea Coherent patients  
**Effort:** Saturday + Sunday  
**Goal:** Medallion running, screenshots captured, trial evidence preserved

### 3. `clinical-bert-pipeline`
**Work:** Full MLOps harness — train, serve, demo  
**Data:** Synthea SOAP notes (from lakehouse Silver) + MTSamples  
**Effort:** Saturday (trains overnight background)  
**Goal:** Ships complete Sunday — end-to-end MLOps, Streamlit demo live

### 4. Ollama Generation (inside scribe-iq-lakehouse)
**Work:** Pipeline working end-to-end  
**Scale:** 200 notes from 1,500-patient cohort (stratified sample)  
**Runtime:** ~2.5 hours — runs Sunday afternoon  
**Goal:** Pipeline complete, 200 notes generated, quality validated

### 5. `scribe-iq`
**Work:** README update only — honest corpus status + roadmap  
**Effort:** 30 minutes  
**Goal:** Narrative updated, no new features

---

## What is Explicitly Deferred

```
Full 6,300-note corpus         M5 Max — June 3-5 overnight runs
Unstructured note generation   After M5 arrives
scribe-iq corpus migration     After M5 full corpus run completes
BERT retrain on n2c2           After credentialing
Platform Polars lite           After Fabric trial expires
Databricks/AWS/GCP migration   Long-term roadmap
Scribe IQ RAG upgrade          June — after full corpus ready
```

---

## Dependency Order

```
1. scribe-iq-lakehouse Silver    → produces SOAP notes for BERT
2. clinical-bert-pipeline        → consumes Silver SOAP notes
3. Ollama pipeline               → consumes Gold encounter_summary
4. scribe-iq corpus migration    → DEFERRED to June
```

**BERT cannot use Synthea SOAP notes until Silver is built.**
If Silver is not ready by Saturday morning, BERT falls back to
MTSamples-only training (still valid, documented in model card).
Silver SOAP notes are the upgrade — not a blocker.

---

## Weekend Schedule — Locked

```
FRIDAY NIGHT (2-3 hrs)
  ├── fabric-lakehouse-hls: README + docs polish → push public ✓
  ├── BERT: repo scaffold, pyproject.toml, CLAUDE.md, params.yaml
  └── Fabric: workspace created, 3 lakehouses, S3 shortcut verified
      Notebook 00_setup running clean

SATURDAY MORNING (4-5 hrs)
  ├── BERT: MultiSourceDataset, training pipeline, MLflow autolog
  ├── START BERT TRAINING (~90 min background, M1 Max MPS)
  └── Fabric: Notebooks 01-04 (Bronze ingest, patient, encounter, clinical)
      Dev cohort: 20 patients to verify pipeline

SATURDAY AFTERNOON (4 hrs)
  ├── BERT training finishes → eval_report.json written
  │   Verify OOD F1 on MTSamples held-out set
  ├── BERT: FastAPI serving + loader
  └── Fabric: Notebooks 05-07 (SOAP notes ← PRIORITY, ECG, imaging)
      silver.soap_note populated — feed to BERT if ready

SATURDAY EVENING (3 hrs)
  ├── BERT: Streamlit app (3 tabs — inference, metrics, model card)
  └── Fabric: Notebooks 08-09 (genomics, Gold encounter_summary)
      Enable CDC on all Silver tables

SUNDAY MORNING (4 hrs)
  ├── BERT: CI + model card + README → push to GitHub ✓ BERT SHIPS
  ├── Fabric: Master pipeline canvas
      Scale to full cohort — all 1,500 patients
      First full pipeline run
  └── Ollama: Sessions 1-2 (scaffold, checkpoint, prompts, dialogue task)

SUNDAY AFTERNOON (4 hrs)
  ├── Ollama: Session 3 (pipeline entrypoint + subset selector)
  ├── START 200-NOTE GENERATION (~2.5 hrs background)
  ├── Fabric: DevOps layer (observability, quality checks)
  ├── CAPTURE: All screenshots (checklist 15.2 in lakehouse spec)
  ├── CAPTURE: 3 key videos
  └── Generation finishes → quality check 20 samples
      Commit quality_baseline.json + 5 sample outputs

SUNDAY NIGHT
  ├── scribe-iq: README update (honest corpus status)
  └── All repos pushed, public, pinned on GitHub profile ✓
```

---

## What Done Looks Like Sunday Night

```
github.com/sandeep-jay/fabric-lakehouse-hls-readmission  ✓
github.com/sandeep-jay/scribe-iq-lakehouse               ✓ Fabric medallion
github.com/sandeep-jay/clinical-bert-pipeline            ✓ MLOps complete
github.com/sandeep-jay/scribe-iq                         ✓ narrative updated

Ollama pipeline: working, 200 notes generated, quality validated
5 sample dialogues committed, quality_baseline.json committed
Full corpus roadmap documented for M5 (June 3-5)
```

---

## June Plan — M5 Max 128GB

```
June 2      M5 arrives, 1 day setup
June 3      Full corpus generation starts
            qwen3:32b + gemma4:31b + qwq:32b all loaded simultaneously
            ~23 sec/note vs ~47 sec on M1 Max
June 3-5    6,300 dialogues + 6,300 unstructured notes
            Both tasks, full validation, 2-3 overnight runs

June 5+
  ├── scribe-iq corpus migration
  │   Replace data_prep/ with lakehouse Gold
  │   Re-embed all 6,300 notes
  │   Full RAG demo — 1,500 patients
  ├── BERT retrain on full corpus + Ollama dialogues
  └── BERT Phase 2: n2c2 medication NER (if credentialed)
```

---

## M5 Max — Model Upgrades

```bash
# These run on M5, too big for M1 Max
ollama pull qwen3:72b          # ~45 GB — better dialogue quality
ollama pull gemma4:83b         # ~50 GB — best polish available locally
ollama pull deepseek-r1:70b    # ~40 GB — better fact checking

# Concurrent on M5 (128GB):
qwen3:32b + gemma4:31b + qwq:32b = ~58 GB — all in memory, no swapping
```

---

## Portfolio Narrative — What to Say

```
"I built a production healthcare AI system across four repos:
 a Fabric FHIR medallion pipeline ingesting Synthea Coherent,
 a ClinicalBERT MLOps pipeline with experiment tracking and
 governed serving, and an Ollama-based synthetic corpus
 generator producing patient-linked clinical dialogues.
 The system currently runs on a 1,500-patient synthetic cohort
 with a 200-note portfolio corpus. Full 6,300-note generation
 is planned for June on M5 Max 128GB."
```

---

## scribe-iq README Update (Sunday night — 30 min)

```markdown
## Corpus Status

The current RAG corpus uses a 19-patient development dataset
built via the legacy `data_prep/` pipeline.

**Migration in progress:**
`scribe-iq-lakehouse` ingests all 1,500 Synthea Coherent patients
into a production Fabric medallion. The Ollama generation pipeline
produces patient-linked clinical dialogues grounded in SOAP notes.
Full corpus migration planned for June 2026 following M5 Max
128GB generation runs.

See: [scribe-iq-lakehouse](../scribe-iq-lakehouse)
```

---

*Document version: 1.0 — May 2026*  
*Status: READY FOR EXECUTION*  
*Start: Friday night with fabric-lakehouse-hls polish*
