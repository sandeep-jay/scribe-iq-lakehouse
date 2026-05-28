# Demo Playbook — recording the scribe-iq-lakehouse video

Operational guide for producing a video demo of the lakehouse.
Three artifacts compose the story; the playbook below sequences them.

```
artifacts                                       beat in the video
─────────────────────────────────────────       ─────────────────
docs/demo/notebooks/demo.duckdb (DuckDB UI) →   "look at the data shape, in SQL"
orchestration/  (Dagster UI)                →   "look at the asset graph + sensor"
core/scripts/demo_walkthrough.py                 →   "look at one patient end-to-end"
```

Target length: **90-second short cut** and **3-minute long-form cut**.

---

## 1. Pre-flight (do once, day-of)

```bash
.venv/bin/python -m pytest -q                        # 122 tests pass — confirm clean state
brew install duckdb                                  # if not already installed (needs ≥1.2)
mkdir -p dagster_home && export DAGSTER_HOME="$PWD/dagster_home"
```

Confirm the data is on disk and complete:

```bash
.venv/bin/python -c "
from deltalake import DeltaTable
for t in ['patient','encounter','observation','soap_note']:
    n = DeltaTable(f'data/silver/{t}').to_pyarrow_dataset().count_rows()
    print(f'silver.{t:14s} {n:>7}')
print('gold:', DeltaTable('data/gold/encounter_summary').to_pyarrow_dataset().count_rows())
"
```

Expected: `patient` 1,278 · `encounter` 143,946 · `soap_note` 143,946 ·
`observation` 669,898 · `gold` 143,946. If anything is short, rebuild before recording.

---

## 2. Recording stage — windows

Position so you can switch with `Cmd-Tab`. Big font (16+) on everything that
ends up on camera. Hide the dock.

| Window | Tool | Pre-positioned to |
|--------|------|-------------------|
| 1 | **DuckDB UI** browser tab (`http://localhost:4213`) | Notebook open at Cell 1; sidebar visible at start, hidden during query takes |
| 2 | **Dagster UI** browser tab (`http://localhost:3000`) | Asset graph view; all groups visible; sensor toggle ready |
| 3 | **Terminal** (large font, dark theme) | Empty prompt, cwd = repo root, venv activated |
| 4 | **VSCode** | One FHIR bundle JSON open, collapsed at top — for the "raw mess" beat |
| 5 | **VSCode** (second window or split) | `README.md` open — closing shot |

Launch services in two background terminals before recording:

```bash
# Terminal A:
DAGSTER_HOME="$PWD/dagster_home" dagster dev

# Terminal B:
duckdb docs/demo/notebooks/demo.duckdb -ui
```

---

## 3. The 5-beat video structure

| Beat | Time | What's on screen | Voiceover (script, ≤2 lines) |
|------|------|------------------|-------------------------------|
| **1. Hook** | 0:00–0:15 | Single rendered SOAP note from the DuckDB UI (Cell 14) | "1,278 synthetic patients, FHIR R4, into a clinical-RAG corpus. Production patterns, runs locally." |
| **2. The raw mess** | 0:15–0:45 | VSCode scrolling through a FHIR bundle JSON | "FHIR is verbose, nested, optional fields everywhere. Not queryable. Step one: turn it into typed tables." |
| **3. The medallion** | 0:45–1:30 | Dagster asset graph → click bronze/silver/gold assets → click an asset check | "Bronze raw, Silver typed/CDC, Gold denormalized. Dagster orchestrates locally — drop a cohort and the asset graph materializes, with validation as first-class asset checks." |
| **4. The transformation** | 1:30–2:30 | Terminal running `python -m scripts.demo_walkthrough --pause 2.5` | "Same patient, three layers — Bronze JSON, parsed records, typed Silver, denormalized Gold with the SOAP note rendered." |
| **5. The payoff** | 2:30–3:00 | DuckDB UI walking through Cells 1 → 5 → 13 → 15 | "Versioned corpus contract — 143,946 encounters, queryable SQL, real clinical signal: anemia, hypertension, diabetes. Feeds scribe-iq, BERT, and Ollama." |

---

## 4. The DuckDB UI cell highlight reel (~2 min raw)

For Beat 5, do **NOT** show all 20 cells. Six tell the story:

| Order | Cell | What it shows | Voiceover (≤15s) |
|-------|------|---------------|-------------------|
| 1 | **Cell 1** — Corpus at a glance | Headline numbers | "1,278 patients, 143,946 encounters, 669k observations — all from raw FHIR via a pure-Python medallion." |
| 2 | **Cell 5** — Top conditions | Anemia · Hypertension · Diabetes | "Clinically coherent. Synthea generates realistic-shaped data." |
| 3 | **Cell 10** — Imaging modalities | US · CR · DX | "Real imaging modalities — extracted from DICOM headers when the binary's present, FHIR-authoritative when it isn't." |
| 4 | **Cell 13** — As-of-date evolution | One patient's `active_conditions` growing over time | "The killer query — watch this patient's active conditions grow over time as chronic disease accumulates. ADR-014, problem-list-as-of-date." |
| 5 | **Cell 14** — One SOAP note | Real, readable clinical text | "And here's the demo centerpiece — a synthetic SOAP note. Subjective. Assessment. Plan. This is what RAG retrieves against." |
| 6 | **Cell 15** — Keyword search | 4,836 encounters with diabetes AND hypertension | "Cohort definition via SQL — instant. The corpus is queryable, not just trained-on." |

**For each cell**: focus on the cell → click Run → 1-second pause → speak the line while results render. Cells 1 + 5 + 10 return in <100ms; cells 13–15 take ~500ms. Pre-warm by running each cell once before recording.

---

## 5. Recording sequence (chronological, ~45 min)

Each take is a separate clip — splice in post.

### Take 1 — The hook (~10 sec)
```bash
clear
.venv/bin/python -m scripts.demo_walkthrough --pause 0 | grep -A 60 "SOAP note"
```
Capture **only** the SOAP note rendering. Reuse a screen-grab as the video thumbnail.

### Take 2 — The raw mess (~30 sec)
- Window 4 (VSCode) showing `data/bronze/fhir/cohort=A/<patient>.json`
- Slow scroll through Patient → Encounter → Observation → MedicationRequest sections
- No mouse clicks — just scroll wheel, steady speed

### Take 3 — The asset graph (~45 sec)
- Window 2 (Dagster UI)
- Open asset graph, zoom so all groups visible
- **Click `bronze_fhir`** → right pane shows file count + FHIR resource breakdown (the `sample_bundle` markdown)
- **Click `encounter`** → right pane shows schema + sample 5 rows
- **Click `gold_encounter_summary`** → right pane shows the rendered encounter card with SOAP note
- **Click any asset check** → right pane shows the rule-by-rule pass/fail Markdown table
- Total: ~30 sec of clicks, 4 distinct panels

### Take 4 — Live sensor demo (~60 sec, **OPTIONAL** — only if sensor is reliable)
```bash
# In Terminal 3, with sensor STOPPED in Dagster UI:
mkdir -p data/bronze/fhir/cohort=DEMO_$(date +%s)
cp data/bronze/fhir/cohort=A/$(ls data/bronze/fhir/cohort=A | head -1) data/bronze/fhir/cohort=DEMO_*/
```
Then switch to Window 2, toggle the sensor ON, wait up to 30s.
The graph cascades: `bronze_fhir[DEMO_*]` → all 10 Silver assets fill green for that partition.

**Skip if flaky** — Take 3 alone carries the orchestration story.

### Take 5 — Walkthrough script (~90 sec)
```bash
clear
.venv/bin/python -m scripts.demo_walkthrough --pause 2.5
```
Let it run end-to-end without touching the keyboard. 2.5-second pauses give
time to narrate each section: "Bronze JSON" → "parsed records" → "typed Silver" → "Gold with SOAP".

### Take 6 — DuckDB UI highlight reel (~2 min)
- Window 1 (DuckDB UI)
- Hide sidebar (`Cmd-\` or click the icon) for cleaner frames
- Walk through cells 1, 5, 10, 13, 14, 15 in order
- For Cell 14, **click the cell to expand** — the SOAP note becomes full-frame readable text

### Take 7 — Closing (~15 sec)
- Window 5 (VSCode README) or final Dagster graph state
- Show the data contract version + "feeds scribe-iq, BERT, Ollama" line from the README

---

## 6. Editing (Descript / CapCut, ~30 min)

Sequence: **Take 1 (hook) → Take 2 (raw) → architecture diagram still (5s) → Take 3 (Dagster) → Take 4 (sensor, optional) → Take 5 (walkthrough) → Take 6 (DuckDB UI) → Take 7 (closing)**.

- Burn captions in — most video hosts auto-play muted
- Voiceover recorded separately (USB mic, QuickTime), sync to picture
- Cut hard — 90-sec short cut drops Takes 2, 4, 7; 3-min long cut keeps all

---

## 7. Publishing checklist

- [ ] Commit `docs/demo/notebooks/demo_notebook.sql` (the .duckdb binary stays gitignored)
- [ ] Push to GitHub; tag the commit (e.g. `v0.4-demo`)
- [ ] Pin the commit hash in the video description
- [ ] Upload short cut + long cut to a video host
- [ ] Cross-link: README has a "Demo video" link; video description links back to the README

---

## 8. Contingencies

| Problem | Fallback |
|---------|----------|
| Dagster sensor isn't firing | Skip Take 4; Take 3 (clicking assets) carries it |
| `demo.duckdb` paths broken after a `cd` change | Re-run Cell 0 from `docs/demo/notebooks/demo_notebook.sql` — recreates views |
| SOAP note content looks bland for the anchor patient | Re-pick: `.venv/bin/python -m scripts.demo_walkthrough --patient-id <other-uuid>` (try ones from Cell 11) |
| Out of recording time | Single-take Take 5 (walkthrough) alone — self-contained 90-sec demo |
| Asset checks all show "Never run" | Materialize one Silver asset once; that triggers all 10 checks |

---

## What this playbook references

| File | Purpose |
|------|---------|
| `docs/demo/notebooks/demo_notebook.sql` | 20-cell SQL source |
| `docs/demo/notebooks/demo.duckdb` | Pre-loaded DuckDB UI notebook (local convenience) |
| `docs/demo/notebooks/README.md` | How to open / regenerate |
| `core/scripts/demo_walkthrough.py` | One-patient CLI walkthrough (rich-formatted) |
| `local/preview.py` | Shared preview helpers (same renderings as Dagster UI) |
| `core/orchestration/dagster/` | Dagster asset graph + checks (ADR-015, ADR-016) |
| `docs/RUNBOOK.md` | Ops procedures (build, verify, troubleshoot) |
| `docs/CORPUS_CONTRACT.md` | The data contract this demo proves |
