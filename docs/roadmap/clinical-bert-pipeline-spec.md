# Clinical BERT MLOps Pipeline — Project Spec

**Portfolio project:** Sandeep Jayaprakash
**Status:** Implementation-ready
**Planned implementation:** Claude Code
**Weekend target:** End-to-end pipeline running, Streamlit demo live

---

## 1. Purpose and Portfolio Narrative

This project demonstrates production MLOps discipline applied to clinical NLP.
It is the discriminative NLP complement to Scribe IQ's generative RAG layer —
trained on real annotated data, benchmarked against published baselines, and
served through a governed inference pipeline.

**Phase 1 (this weekend):** Multi-source specialty classification.
Primary: Synthea Coherent SOAP notes from scribe-iq-lakehouse Silver
(70% of training batches). Auxiliary: MTSamples for linguistic variety
(30%). If Silver SOAP notes not ready by Saturday morning, falls back
to MTSamples-only — still valid, documented honestly in model card.
Pipeline infrastructure ships complete regardless of data source.

**Phase 2 (post n2c2 credentialing):** Swap primary to n2c2 medication
NER. One params.yaml change. Pipeline unchanged.

**Phase 3 (post Scribe IQ lakehouse):** Run inference against Scribe IQ Gold
corpus to enrich synthetic notes with structured entity annotations.

### What this signals to employers

| Signal | How it shows |
|---|---|
| ML in production | MLflow experiment tracking, model registry, promotion gates |
| Data governance | DVC versioning, data validation, swap-ready pipeline |
| CI discipline | F1 gate on every PR, same pattern as RAGAS gate in campus-rag |
| Clinical NLP depth | ClinicalBERT, benchmark comparison, honest model card |
| Full stack | FastAPI serving + Streamlit showcase + Docker Compose |
| Senior judgment | Roadmap documents what is deferred and why |

---

## 2. Task Definition

### Phase 1 — Specialty Classification (MTSamples)

- **Task:** Multiclass text classification
- **Input:** Free-text clinical transcription
- **Output:** Medical specialty label (e.g. Cardiology, Orthopedics, Neurology)
- **Base model:** `emilyalsentzer/Bio_ClinicalBERT`
- **Dataset:** `harishnair04/mtsamples` via HuggingFace datasets
- **Classes:** Top 15 specialties by frequency (filter long-tail classes < 50 samples)
- **Metric:** Weighted F1 (primary), per-class F1 (reported), accuracy

### Phase 2 — Medication NER (n2c2, post-credentialing)

- **Task:** Token classification / NER
- **Input:** Clinical note text
- **Output:** BIO-tagged entities — Drug, Dosage, Route, Frequency, Duration
- **Base model:** `emilyalsentzer/Bio_ClinicalBERT`
- **Dataset:** n2c2 2018 ADE and Medication Extraction challenge
- **Metric:** Entity-level F1 (strict match), compare against n2c2 leaderboard

Same pipeline, different task head and dataset. No structural changes.

---

## 3. Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        Data Layer                           │
│  DVC ──► params.yaml ──► dvc.yaml pipeline stages          │
│  MTSamples (HuggingFace) ──► data/raw/  (DVC tracked)      │
│  Validated splits ──► data/processed/   (DVC tracked)      │
└──────────────────────────┬──────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────┐
│                     Training Layer                          │
│  train_pipeline.py                                          │
│    validate → preprocess → train → evaluate → register      │
│                                                             │
│  HuggingFace Trainer + MLflow autolog                       │
│  Every run: params, metrics, artifacts → MLflow server      │
│  Best run promoted to Production in MLflow Model Registry   │
└──────────────────────────┬──────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────┐
│                      Serving Layer                          │
│                                                             │
│  FastAPI (/predict /health /model-info)                     │
│    └── loads Production model from MLflow registry          │
│                                                             │
│  Streamlit (showcase UI)                                    │
│    └── calls FastAPI /predict                               │
│    └── reads eval_report.json for metrics tab               │
│    └── renders model_card.md                                │
│                                                             │
│  MLflow server (tracking + registry)                        │
└──────────────────────────┬──────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────┐
│                         CI Layer                            │
│  GitHub Actions                                             │
│    ci.yml    — lint, unit tests, eval gate on every PR      │
│    train.yml — manual trigger: full training run            │
└─────────────────────────────────────────────────────────────┘
```

### Docker Compose services

| Service | Port | Purpose |
|---|---|---|
| `mlflow` | 5000 | Tracking server + model registry |
| `api` | 8000 | FastAPI inference serving |
| `app` | 8501 | Streamlit showcase |

All three start with `docker compose up`.

---

## 4. Repository Structure

```
clinical-bert-pipeline/
│
├── data/
│   ├── .gitignore             # exclude raw data files
│   ├── raw/                   # DVC tracked
│   │   └── mtsamples.csv
│   └── processed/             # DVC tracked
│       ├── train.jsonl
│       ├── val.jsonl
│       └── test.jsonl
│
├── src/
│   ├── data/
│   │   ├── __init__.py
│   │   ├── dataset.py         # HF dataset loading, tokenization, splits
│   │   ├── validate.py        # schema checks, class distribution guard
│   │   └── preprocess.py      # cleaning, label encoding, filtering
│   │
│   ├── models/
│   │   ├── __init__.py
│   │   ├── model.py           # ClinicalBERT + classification head
│   │   └── registry.py        # MLflow register, promote, load helpers
│   │
│   ├── training/
│   │   ├── __init__.py
│   │   ├── train.py           # HuggingFace Trainer + MLflow autolog
│   │   └── evaluate.py        # F1, confusion matrix, eval_report.json
│   │
│   └── serving/
│       ├── __init__.py
│       ├── api.py             # FastAPI app — /predict /health /model-info
│       └── loader.py          # loads Production model from MLflow registry
│
├── app/
│   ├── streamlit_app.py       # main entry point
│   └── components/
│       ├── __init__.py
│       ├── inference.py       # text input → highlighted output
│       ├── metrics.py         # loads eval_report.json, renders charts
│       └── model_card.py      # renders model_card.md
│
├── pipelines/
│   └── train_pipeline.py      # orchestrates: validate→preprocess→train→eval→register
│
├── notebooks/
│   └── 01_eda.ipynb           # class distribution, text length, label balance
│
├── tests/
│   ├── test_dataset.py
│   ├── test_model.py
│   ├── test_api.py
│   └── conftest.py
│
├── .github/
│   └── workflows/
│       ├── ci.yml             # lint + test + eval gate
│       └── train.yml          # manual training trigger
│
├── outputs/
│   ├── model/                 # saved weights (gitignored)
│   └── eval_report.json       # metrics artifact, committed
│
├── docker-compose.yml
├── Dockerfile.api
├── Dockerfile.app
├── dvc.yaml                   # pipeline stages
├── params.yaml                # versioned hyperparameters
├── model_card.md
├── requirements.txt
├── requirements-dev.txt
├── pyproject.toml
└── README.md
```

---

## 5. Component Specifications

### 5.1 `params.yaml` — versioned hyperparameters

```yaml
base_model: emilyalsentzer/Bio_ClinicalBERT
task: classification        # swap to ner for Phase 2

data:
  dataset: harishnair04/mtsamples
  min_class_samples: 50
  max_length: 512
  test_size: 0.15
  val_size: 0.15
  seed: 42

training:
  epochs: 4
  batch_size: 16
  learning_rate: 2e-5
  warmup_ratio: 0.1
  weight_decay: 0.01
  fp16: true

evaluation:
  primary_metric: weighted_f1
  f1_gate: 0.75              # CI gate threshold — PR fails below this
```

All values flow from `params.yaml` into training via DVC. Changing a
hyperparameter is a one-line commit, fully tracked.

---

### 5.2 `src/data/dataset.py`

Responsibilities:
- Load `harishnair04/mtsamples` from HuggingFace datasets
- Filter classes below `min_class_samples` threshold
- Encode labels to integer ids, persist label mapping
- Tokenize with ClinicalBERT tokenizer, `max_length=512`, truncation
- Stratified train/val/test split
- Return HuggingFace `DatasetDict`

---

### 5.3 `src/data/validate.py`

Runs before training. Fails fast on:
- Missing required columns (`transcription`, `medical_specialty`)
- Class count below minimum threshold
- Tokenization producing zero-length sequences
- Train/val/test overlap check

Logs validation report to MLflow as a run artifact.

---

### 5.4 `src/models/model.py`

```python
# ClinicalBERT with classification head
# AutoModelForSequenceClassification loaded from params.base_model
# num_labels from dataset label map
# Phase 2: swap to AutoModelForTokenClassification, no other changes
```

---

### 5.5 `src/training/train.py`

- HuggingFace `Trainer` with `TrainingArguments` loaded from `params.yaml`
- `mlflow.transformers.autolog()` enabled — logs all params and metrics automatically
- Custom `compute_metrics` function — weighted F1 logged per epoch
- Best checkpoint saved by `eval_weighted_f1`
- On completion: calls `registry.py` to register model

---

### 5.6 `src/training/evaluate.py`

Runs on test set after training. Outputs:

```json
{
  "task": "specialty_classification",
  "dataset": "mtsamples",
  "base_model": "emilyalsentzer/Bio_ClinicalBERT",
  "weighted_f1": 0.83,
  "accuracy": 0.84,
  "per_class_f1": {
    "Cardiology": 0.91,
    "Orthopedic": 0.88,
    ...
  },
  "confusion_matrix": [...],
  "eval_date": "2026-05-24",
  "mlflow_run_id": "abc123",
  "gate_threshold": 0.75,
  "gate_passed": true,
  "note": "MTSamples baseline. Retraining on n2c2 medication NER in progress."
}
```

`eval_report.json` is committed to the repo. Streamlit metrics tab reads this
file directly. Same honest baseline discipline as campus-rag RAGAS baseline.

---

### 5.7 `src/models/registry.py`

```python
# promote_to_production(run_id, model_name)
#   - registers model from run
#   - compares F1 against current Production model
#   - promotes only if improvement or no current Production model exists
#   - logs promotion decision as MLflow tag

# load_production_model(model_name)
#   - loads latest Production stage model
#   - falls back to latest Staging if no Production exists
#   - raises clear error if registry is empty (no silent failures)
```

---

### 5.8 `src/serving/api.py`

```
GET  /health        — service status, model version loaded, task type
GET  /model-info    — full model metadata from MLflow registry
POST /predict       — inference endpoint
```

**`/predict` request:**
```json
{
  "text": "Patient presents with chest pain and shortness of breath...",
  "top_k": 3
}
```

**`/predict` response:**
```json
{
  "prediction": "Cardiovascular / Pulmonary",
  "confidence": 0.94,
  "top_k": [
    {"label": "Cardiovascular / Pulmonary", "score": 0.94},
    {"label": "Emergency Room Reports", "score": 0.04},
    {"label": "General Medicine", "score": 0.01}
  ],
  "model_version": "2",
  "model_stage": "Production",
  "task": "specialty_classification"
}
```

Model is loaded once at startup via `loader.py`, cached in memory.
`/health` returns 503 if model failed to load — no silent degradation.

---

### 5.9 Streamlit App

**Tab 1 — Live Inference**

- Text area pre-filled with sample clinical note (one per specialty, rotatable)
- "Analyze" button → POST to FastAPI `/predict`
- Top prediction displayed prominently with confidence bar
- Top-3 predictions shown as ranked list with scores
- Model version and stage shown in footer of result card
- "Try another example" button cycles through pre-loaded samples

**Tab 2 — Model Performance**

- Loads `outputs/eval_report.json` at startup
- Weighted F1 displayed as hero metric with gate threshold shown
- Per-class F1 as horizontal bar chart (Altair/Plotly via st.altair_chart)
- Confusion matrix as heatmap
- Dataset source, eval date, base model listed
- Roadmap callout: *"Retraining on n2c2 medication NER in progress"*

**Tab 3 — Model Card**

- `st.markdown(model_card.md content)` — rendered directly
- No additional UI, just clean rendered markdown

**Sidebar (all tabs):**

- Model status badge (Production / Staging / Unavailable)
- Link to GitHub repo
- Link to Scribe IQ (sister project)
- Dataset attribution

---

### 5.10 `model_card.md`

Sections:
1. Model description — task, base model, fine-tuning approach
2. Training data — MTSamples attribution, limitations of synthetic-free real data
3. Evaluation results — links to `eval_report.json`, comparison note vs published baselines
4. Intended use — portfolio demonstration, NLP pipeline showcase
5. Out-of-scope use — not for clinical decision-making, not validated on real EHR
6. Known limitations — MTSamples is cleaner than real clinical text, specialty-only phase 1
7. Roadmap — n2c2 medication NER, Scribe IQ inference integration
8. Data governance — credentialing process noted, PHI-free

---

## 6. CI/CD

### `ci.yml` — runs on every PR and push to main

```yaml
jobs:
  lint:
    - ruff check src/ app/ pipelines/
    - black --check src/ app/ pipelines/

  test:
    - pytest tests/ -v --cov=src

  eval-gate:
    - python pipelines/train_pipeline.py --fast   # small subset, skip full train
    - python -c "
        import json
        report = json.load(open('outputs/eval_report.json'))
        assert report['weighted_f1'] >= report['gate_threshold'], \
          f'F1 gate failed: {report[\"weighted_f1\"]} < {report[\"gate_threshold\"]}'
      "
```

The eval gate runs on a held-out test set using the registered Production model,
not a fresh training run. Fast, cheap, catches regressions.

### `train.yml` — manual dispatch only

```yaml
on:
  workflow_dispatch:
    inputs:
      reason:
        description: 'Reason for training run'
        required: true

jobs:
  train:
    - dvc repro                    # runs full pipeline
    - git add outputs/eval_report.json dvc.lock
    - git commit -m "chore: update eval baseline [skip ci]"
    - git push
```

Training is never automatic. Human decision, tracked reason, committed artifact.

---

## 7. Docker Compose

```yaml
services:
  mlflow:
    image: ghcr.io/mlflow/mlflow:latest
    ports: ["5000:5000"]
    volumes:
      - mlflow-data:/mlflow
    command: >
      mlflow server
      --host 0.0.0.0
      --port 5000
      --backend-store-uri sqlite:///mlflow/mlflow.db
      --default-artifact-root /mlflow/artifacts

  api:
    build:
      context: .
      dockerfile: Dockerfile.api
    ports: ["8000:8000"]
    environment:
      MLFLOW_TRACKING_URI: http://mlflow:5000
      MODEL_NAME: clinical-bert-classifier
      MODEL_STAGE: Production
    depends_on: [mlflow]

  app:
    build:
      context: .
      dockerfile: Dockerfile.app
    ports: ["8501:8501"]
    environment:
      API_URL: http://api:8000
    depends_on: [api]
    volumes:
      - ./outputs:/app/outputs:ro
      - ./model_card.md:/app/model_card.md:ro

volumes:
  mlflow-data:
```

---

## 8. DVC Pipeline

```yaml
# dvc.yaml
stages:
  validate:
    cmd: python -m src.data.validate
    deps: [src/data/validate.py, data/raw/mtsamples.csv]
    params: [params.yaml]
    outs: [data/processed/validation_report.json]

  preprocess:
    cmd: python -m src.data.preprocess
    deps: [src/data/preprocess.py, data/raw/mtsamples.csv,
           data/processed/validation_report.json]
    params: [params.yaml]
    outs: [data/processed/train.jsonl, data/processed/val.jsonl,
           data/processed/test.jsonl, data/processed/label_map.json]

  train:
    cmd: python -m src.training.train
    deps: [src/training/train.py, src/models/model.py,
           data/processed/train.jsonl, data/processed/val.jsonl]
    params: [params.yaml]
    outs: [outputs/model/]

  evaluate:
    cmd: python -m src.training.evaluate
    deps: [src/training/evaluate.py, outputs/model/,
           data/processed/test.jsonl]
    params: [params.yaml]
    outs: [outputs/eval_report.json]
```

`dvc repro` runs the full pipeline. DVC tracks which stages need rerunning
when params or data change. Swapping MTSamples for n2c2 is a data-layer change
— only `validate`, `preprocess`, and `train` stages rerun, `evaluate` format
is unchanged.

---

## 9. Implementation Sequence for Claude Code

### Session 1 — Scaffold and data pipeline

```
1. Init repo, pyproject.toml, requirements.txt, .gitignore
2. params.yaml with all hyperparameters
3. src/data/dataset.py — load MTSamples, filter, tokenize, split
4. src/data/validate.py — schema checks, class distribution
5. src/data/preprocess.py — cleaning, label encoding
6. dvc.yaml — validate and preprocess stages
7. notebooks/01_eda.ipynb — class distribution, text length plots
8. tests/test_dataset.py
```

### Session 2 — Training pipeline and MLflow

```
1. Docker Compose with MLflow service
2. src/models/model.py — ClinicalBERT classification head
3. src/training/train.py — Trainer + MLflow autolog
4. src/training/evaluate.py — F1, confusion matrix, eval_report.json
5. src/models/registry.py — register, promote, load helpers
6. pipelines/train_pipeline.py — orchestrate all stages
7. dvc.yaml — train and evaluate stages
8. tests/test_model.py
```

### Session 3 — Serving layer

```
1. src/serving/loader.py — load Production model from MLflow registry
2. src/serving/api.py — FastAPI /predict /health /model-info
3. Dockerfile.api
4. Add api service to docker-compose.yml
5. tests/test_api.py
6. Verify docker compose up — mlflow + api both running
```

### Session 4 — Streamlit app

```
1. app/components/inference.py — call /predict, format response
2. app/components/metrics.py — load eval_report.json, charts
3. app/components/model_card.py — render markdown
4. app/streamlit_app.py — three tabs, sidebar
5. Dockerfile.app
6. Add app service to docker-compose.yml
7. Verify full stack: mlflow + api + streamlit
```

### Session 5 — CI, model card, documentation

```
1. .github/workflows/ci.yml — lint, test, eval gate
2. .github/workflows/train.yml — manual dispatch
3. model_card.md — all sections
4. README.md — architecture diagram, quick start, reviewer guide
5. outputs/eval_report.json — committed baseline
6. Final docker compose up smoke test
7. Push to GitHub
```

---

## 10. README Structure

Mirrors campus-rag pattern — built for hiring reviewers.

```
# Clinical BERT Pipeline

Badge row: CI | MLflow | Python | Docker | HuggingFace

One-line description.

## Reviewer guide table (same pattern as campus-rag)

## What this shows (MLOps signals table)

## Architecture diagram

## Quick start (docker compose up → three services)

## Evaluation baseline (F1 scores, gate threshold, honest limitations)

## Stack table

## Phase roadmap
  Phase 1 — MTSamples classification (complete)
  Phase 2 — n2c2 medication NER (in progress, n2c2 credentialing underway)
  Phase 3 — Scribe IQ Gold corpus inference (planned)

## Scribe IQ integration note
```

---

## 11. Production Seams (document, don't build)

Add a `PRODUCTION_NOTES.md` that honestly states:

| Capability | Current | Production path |
|---|---|---|
| Model registry | MLflow local SQLite | MLflow on managed Postgres or SageMaker Model Registry |
| Data versioning | DVC local | DVC with S3 remote or Azure Blob |
| Serving | FastAPI single instance | FastAPI behind load balancer, autoscaling |
| Monitoring | None | Evidently for data drift, prediction drift |
| Retraining | Manual `workflow_dispatch` | Triggered on drift detection or scheduled |
| Auth | None | API key or OAuth2 on /predict |
| PHI | Synthetic/public data only | BAA-backed deployment, audit logging, de-identification |

This is the same pattern as Scribe IQ's "Demo readiness" table — honest,
specific, shows you know what production actually requires.

---

## 12. Connecting to Scribe IQ

Once Phase 3 begins, the integration point is clean:

```python
# In Scribe IQ serving layer
# Load production ClinicalBERT from clinical-bert-pipeline MLflow registry
# Run NER inference on Gold corpus notes
# Write entity annotations back to Gold layer with model version + run_id
# Enables entity-filtered search and structured extraction in Scribe IQ RAG
```

Document this integration in both repos. Reviewers who look at both projects
see a coherent system, not two disconnected demos.

---

*Document version: 1.0 — May 2026*
*Status: Ready for implementation via Claude Code*

---

## 13. M1 Max Training Configuration

### Hardware

```
Device:    Apple M1 Max
RAM:       32GB unified memory
Backend:   MPS (Metal Performance Shaders)
Estimated training time: 90 minutes - 2 hours
```

### params.yaml — M1 Max config

```yaml
base_model: emilyalsentzer/Bio_ClinicalBERT
task: classification

hardware:
  use_mps: true
  fp16: false        # MPS does not support fp16
  bf16: false        # bf16 also unsupported on MPS

data:
  primary:
    source: synthea_coherent_soap
    path: gold/encounter_summary     # from scribe-iq-lakehouse
    weight: 0.70
  auxiliary:
    source: mtsamples
    dataset: harishnair04/mtsamples
    weight: 0.30
  min_class_samples: 50
  max_length: 512
  test_size: 0.15
  val_size: 0.15
  seed: 42
  ood_eval_set: mtsamples_held_out   # out-of-distribution evaluation

training:
  epochs: 4
  batch_size: 32      # M1 Max 32GB handles batch 32 comfortably
  learning_rate: 2e-5
  warmup_ratio: 0.1
  weight_decay: 0.01
  fp16: false
  use_mps: true

evaluation:
  primary_metric: weighted_f1
  f1_gate: 0.75
  report_ood_f1: true   # report separately on mtsamples held-out set
```

### train.py — MPS device setup

```python
import torch
from transformers import TrainingArguments

def get_device():
    if torch.backends.mps.is_available():
        return torch.device("mps")
    elif torch.cuda.is_available():
        return torch.device("cuda")
    return torch.device("cpu")

def get_training_args(params):
    return TrainingArguments(
        output_dir="outputs/model",
        num_train_epochs=params.training.epochs,
        per_device_train_batch_size=params.training.batch_size,
        per_device_eval_batch_size=params.training.batch_size,
        learning_rate=params.training.learning_rate,
        warmup_ratio=params.training.warmup_ratio,
        weight_decay=params.training.weight_decay,
        fp16=False,        # MPS limitation
        bf16=False,
        use_mps_device=torch.backends.mps.is_available(),
        evaluation_strategy="epoch",
        save_strategy="epoch",
        load_best_model_at_end=True,
        metric_for_best_model="weighted_f1",
        report_to="mlflow",
    )
```

### MLflow — fully local on M1 Max

No DagsHub or remote tracking needed.
Docker Compose MLflow server runs natively on M1 Max ARM.
Training writes directly to local MLflow server.
Full stack runs on one machine:

```
docker compose up         # MLflow :5000, FastAPI :8000, Streamlit :8501
python pipelines/train_pipeline.py   # ~90 min on M1 Max MPS
```

Start training run Saturday afternoon.
Done before dinner. Eval report ready for Streamlit Sunday morning.

---

## 14. Multi-Source Training Strategy

### Why multi-source

Synthea Coherent SOAP notes are template-driven — more regular than
real clinical notes. MTSamples transcriptions are messier, more natural.
Together they produce better generalization than either alone.

This is **domain-adaptive multi-source training** — a production pattern
used in clinical NLP when training data has distribution mismatch.

### Data sources

| Source | Size | Strengths | Weaknesses |
|---|---|---|---|
| Synthea Coherent SOAP | ~6,300 notes | Coherent per patient, Silver-labeled | Template-driven, too regular |
| MTSamples | ~5,000 transcriptions | Natural clinical language, diverse | Weakly labeled (specialty only) |
| **Combined** | **~11,000** | Complementary distributions | Requires weighted sampling |

### Training approach — Option 2 (recommended)

Mixed batch training: 70% SOAP notes + 30% MTSamples per batch.
Model sees both distributions every epoch.

```python
# src/data/dataset.py — weighted multi-source dataset

class MultiSourceDataset(Dataset):
    def __init__(self, soap_dataset, mtsamples_dataset,
                 soap_weight=0.70, seed=42):
        self.soap = soap_dataset
        self.mtsamples = mtsamples_dataset
        self.soap_weight = soap_weight
        self.rng = random.Random(seed)

    def __getitem__(self, idx):
        # Sample from SOAP notes 70% of the time
        if self.rng.random() < self.soap_weight:
            source_idx = idx % len(self.soap)
            item = self.soap[source_idx]
            item["source"] = "synthea_soap"
        else:
            source_idx = idx % len(self.mtsamples)
            item = self.mtsamples[source_idx]
            item["source"] = "mtsamples"
        return item

    def __len__(self):
        return len(self.soap) + len(self.mtsamples)
```

### Evaluation strategy

Report F1 on three sets:

```
1. In-distribution test set    SOAP notes held-out split
                               Validates task performance on primary data

2. Out-of-distribution set     MTSamples held-out set
                               The honest number — generalizes beyond training?

3. Cross-dataset delta         OOD F1 - In-distribution F1
                               Negative = overfitting to SOAP templates
                               Near zero = good generalization
```

The OOD F1 on MTSamples is the number that goes in the model card
and README. Most portfolio BERT projects don't have a cross-dataset
evaluation. This is the differentiator.

### eval_report.json — updated schema

```json
{
  "task": "specialty_classification",
  "training_sources": {
    "primary": "synthea_coherent_soap",
    "auxiliary": "mtsamples",
    "mixing_ratio": "70/30"
  },
  "base_model": "emilyalsentzer/Bio_ClinicalBERT",
  "in_distribution": {
    "dataset": "synthea_soap_test_split",
    "weighted_f1": 0.87,
    "accuracy": 0.88
  },
  "out_of_distribution": {
    "dataset": "mtsamples_held_out",
    "weighted_f1": 0.79,
    "accuracy": 0.81,
    "note": "Primary generalization metric — trained on SOAP, evaluated on MTSamples"
  },
  "cross_dataset_delta": -0.08,
  "per_class_f1": { "Cardiology": 0.91, "...": "..." },
  "eval_date": "2026-05-25",
  "gate_threshold": 0.75,
  "gate_passed": true,
  "model_note": "Template-driven SOAP corpus — n2c2 retraining planned"
}
```

### Phase 2 data swap — n2c2

When n2c2 credentialing completes, swap primary source:

```yaml
# params.yaml change only — pipeline unchanged
data:
  primary:
    source: n2c2_2018_medication_ner
    path: data/raw/n2c2/
    weight: 0.70
  auxiliary:
    source: mtsamples
    weight: 0.30
task: ner    # swap from classification to NER
```

`dvc repro` reruns only affected stages.
Same training pipeline, different task head, better data.

### Phase 3 — Scribe IQ inference integration

```python
# After Phase 3: run NER inference on Gold corpus
# Fine-tuned ClinicalBERT annotates silver.soap_note_text
# Writes to gold.entity_annotations in scribe-iq-lakehouse

gold_entity_annotations schema:
  note_id         → links to silver.soap_note
  entity_text     → matched span
  entity_type     → DRUG / CONDITION / PROCEDURE
  start_char      → span offset
  end_char        → span offset
  confidence      → model score
  model_version   → from MLflow registry
  model_run_id    → full lineage
```

---

## 15. Final Weekend Plan — Locked

See MASTER_PLAN.md for full cross-repo schedule.
BERT-specific milestones:

```
FRIDAY NIGHT
  └── BERT: repo scaffold, pyproject.toml, CLAUDE.md
      params.yaml with M1 Max config (fp16=False, batch=32)
      Verify MPS: python -c "import torch; print(torch.backends.mps.is_available())"

SATURDAY MORNING
  ├── BERT: MultiSourceDataset, training pipeline + MLflow autolog
  └── START TRAINING (~90 min background, M1 Max MPS)

SATURDAY AFTERNOON
  ├── Training done → eval_report.json written
  │   Check: weighted_f1 >= 0.75 gate passes
  │   Check: OOD F1 on MTSamples held-out set
  └── BERT: FastAPI serving + model loader

SATURDAY EVENING
  └── BERT: Streamlit app (3 tabs: inference, metrics, model card)
      Metrics tab shows BOTH in-dist and OOD F1 scores

SUNDAY MORNING
  └── BERT: CI + model card + README → push GitHub
      ✓ BERT SHIPS — done by noon Sunday
```

### Lakehouse dependency note

BERT prefers Synthea SOAP notes from scribe-iq-lakehouse Silver.
Do not block BERT on lakehouse. If Silver not ready Saturday morning:

```python
# params.yaml fallback
data:
  primary:
    source: mtsamples
    weight: 1.0
  auxiliary: null
# Model card: "MTSamples only — Synthea SOAP integration
# pending lakehouse Silver. Multi-source retrain planned."
```

Upgrade to multi-source after Silver ready — 90 min retrain.

---

*Document version: 3.0 — May 2026*
*Final: multi-source training (SOAP + MTSamples), lakehouse dependency,*
*fallback to MTSamples-only if Silver not ready, locked weekend plan*
*Status: READY FOR EXECUTION*
