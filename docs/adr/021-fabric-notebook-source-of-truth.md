# ADR-021: Fabric `.Notebook/` as the notebook source of truth (drop `.ipynb`)

**Date:** 2026-05-29
**Status:** Accepted
**Deciders:** Sandeep Jayaprakash

## Context

The first Fabric notebook authoring pass used `.ipynb` (Jupyter) as the
source format. To make Fabric Git Integration recognize the notebooks (it
only sees items in Fabric's native folder format), we wrote a converter
(`core/scripts/convert_ipynb_to_fabric.py`) and a pre-commit hook that
regenerated `<name>.Notebook/notebook-content.py` + `.platform` from each
`.ipynb` on every commit.

Maintaining two formats for the same notebook introduces drift:

- If a user edits a notebook in the Fabric UI and Git Integration pushes
  back, the `.Notebook/` file gets the change but the `.ipynb` stays stale.
  Next pre-commit regen silently overwrites the Fabric edit.
- The converter has to faithfully round-trip cell metadata, magic-comment
  cell delimiters, the `.platform` JSON, and stable `logicalId` GUIDs.
  Every divergence is a footgun.
- Reviewers see two files per notebook in `git diff` — added noise.
- The build of the converter + hook + format-converter logic was ~150 lines
  for no real ongoing benefit beyond Jupyter editor support.

## Decision

**Single source of truth: Fabric's native `<name>.Notebook/` folder format.**
Delete all `.ipynb` files, the converter script, and the pre-commit hook.

Notebooks live as:

```
fabric/notebooks/
└── <NN>_<name>.Notebook/
    ├── notebook-content.py    # Python source with # CELL / # MARKDOWN magic comments
    └── .platform              # JSON metadata: type=Notebook, displayName, logicalId (UUIDv5)
```

`notebook-content.py` is editable in any text editor — cells are delimited by
`# CELL ********************` / `# MARKDOWN ********************` blocks
followed by per-cell `# METADATA ********************` blocks. The Fabric
notebook UI round-trips this format via Git Integration without conversion.

Discipline rule: for substantive notebook changes, edit `notebook-content.py`
locally and `git push`. Fabric pulls via Git Integration → Sync. Use the
Fabric notebook UI for exploratory runs + screenshots only; if you do edit
substantively in Fabric, commit back via Git Integration source-control UI
so the change lands in git.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| `.ipynb` + generated `.Notebook/` (pre-commit hook regenerates) | Best Jupyter UX | Drift on Fabric-UI edits; 2× diff noise; converter maintenance | Tried it; the drift risk wasn't worth the editor gain |
| `.Notebook/` only (chosen) | Single source of truth; Fabric Git Integration round-trips natively; no converter | `notebook-content.py` is less ergonomic than `.ipynb` in some editors (no native cell runner) | Editing as plain Python with magic comments works in VS Code, Cursor, vim, anywhere |
| `.ipynb` only, no `.Notebook/` in git, deploy via `fabric-cicd` | Cleanest repo; one format | Fabric Git Integration becomes useless; needs Service Principal + CI for every deploy; loses Fabric→git round-trip | Foreclosed the Git Integration option entirely; brittle for solo dev |

## Consequences

**Positive:**
- **No drift risk** — Fabric edits round-trip to git via Git Integration;
  local edits push via git. Single canonical representation.
- **No converter to maintain** — `core/scripts/convert_ipynb_to_fabric.py`
  and the `fabric-notebooks-regen` pre-commit hook deleted. ~150 lines
  removed.
- **Fabric Git Integration works natively** — `.Notebook/` is the format
  Fabric expects; sync just works.
- **Smaller diffs** — one file per notebook change, not two.
- **Future Fabric items follow the same pattern** — Lakehouses, Pipelines,
  Semantic Models all use `<Name>.<Type>/` folder format. Consistent.

**Negative:**
- **Local notebook editing UX downgrades** — no native cell-run-restart in
  VSCode / Jupyter UI for `notebook-content.py`. Mitigation: edit in any
  text editor; run in Fabric UI for interactive iteration; use Git
  Integration for round-tripping Fabric-side edits.
- **Format is Fabric-specific** — `notebook-content.py` only runs as-is in
  Fabric. Locally, the cells can be copy-pasted into a Python shell or
  Jupyter cell, but the file isn't directly runnable.

**Neutral:**
- The `.platform` JSON files use deterministic `uuid.uuid5` for `logicalId`
  derived from the notebook stem, so logical identity is stable across
  re-creation and Fabric never sees a "new item" for the same notebook
  name.

## Implementation notes

- Deleted: all `fabric/notebooks/*.ipynb`; `core/scripts/convert_ipynb_to_fabric.py`;
  the `fabric-notebooks-regen` entry in `.pre-commit-config.yaml`.
- Retained: every `fabric/notebooks/<name>.Notebook/` folder with its
  `notebook-content.py` + `.platform`.
- Updated: `.claude/rules/notebooks.md` to document the new pattern;
  `CLAUDE.md` key files list reflects the format change.

## Related

- ADR-018 (CI/CD monorepo) — Fabric Git Integration is part of the deploy
  story; this ADR makes it work
- ADR-020 (Fabric distributed parsing) — sibling decision adopted in the
  same refactor; the rewritten notebooks live in this format
- `.claude/rules/notebooks.md` — operational rules
