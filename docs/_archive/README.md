# Archive

Historical planning docs that are no longer load-bearing live here.

## When to archive vs delete vs keep

| Doc type | Action |
|---|---|
| Live planning (current/future) | Keep in `docs/roadmap/` |
| Decision record | Keep in `docs/adr/` (never delete; mark "Superseded by ADR-N" instead) |
| Operational doc (current) | Keep in `docs/` |
| Planning doc for **done** work — load-bearing rationale | Move here (`docs/_archive/`) |
| Planning doc for **done** work — replaced by ADRs / CHANGELOG entirely | Delete (git history is the archive) |
| Planning doc for a **different repo** | Delete (belongs in that repo) |
| Speculation / aspirational design — unbuilt | Delete; re-add when work starts |

## Naming

Preserve the original path inside the archive — e.g., if you archive
`docs/roadmap/foo.md`, place it at `docs/_archive/roadmap/foo.md`.

This preserves the "where did this come from" signal without breaking
the original location-based grouping.

## Don't drift

The archive is for genuinely-historical-but-still-referenced docs only.
If a doc isn't referenced from anywhere current, just delete it —
`git log --diff-filter=D --name-only` will find it later if needed.

A growing `_archive/` is a smell. Empty (or near-empty) is the goal.
