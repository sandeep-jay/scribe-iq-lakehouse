# Skill: MLOps Patterns

Rules for ML experiment tracking, model registry, and serving.
Apply for clinical-bert-pipeline work invoked from this repo.

## params.yaml is single source of truth
Every hyperparameter from params.yaml — never hardcoded in code.

## MLflow discipline
- autolog is the floor — always enable before training
- Custom metrics on top: weighted_f1, ood_f1 always logged
- eval_report.json always written and committed — never gitignored
- OOD F1 (held-out set) always reported alongside in-distribution F1

## eval_report.json required fields
```json
{
  "task": "...",
  "in_distribution": {"weighted_f1": 0.0, "dataset": "..."},
  "out_of_distribution": {"weighted_f1": 0.0, "dataset": "..."},
  "gate_threshold": 0.75,
  "gate_passed": true,
  "model_note": "honest limitation statement"
}
```

## Model registration
Only register if F1 >= gate_threshold. Never register a failing model.

## M1 Max Apple Silicon
fp16=False, bf16=False — MPS does not support these.
use_mps_device=True, per_device_train_batch_size=32.

## Model card discipline
Every trained model gets model_card.md committed to the repo.
Limitations are not weaknesses — they're honest engineering.
A reviewer who finds an undocumented limitation loses trust.