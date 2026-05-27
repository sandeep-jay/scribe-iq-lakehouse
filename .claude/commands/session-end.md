We are ending this session. Before stopping, complete all of the following in order:

1. **Update HANDOFF.md** with:
   - Session summary (3-5 sentences: what was accomplished, current state)
   - Working / In progress / Blocked sections
   - Test status (run pytest tests/ and report results)
   - Next session first task (specific and actionable)
   - Open decisions table
   - Key state (env vars, table counts, what's running)
   - Files changed this session
   - ADRs written this session

2. **Update CHANGELOG.md** — add entries for everything meaningful that changed

3. **Sync the docs** (do this before pytest so the doc-as-test passes):
   - **Regenerate generated docs**: `python scripts/gen_data_dictionary.py` (writes ONLY
     docs/DATA_DICTIONARY.md) and `python scripts/gen_corpus_schema.py` (writes ONLY
     schemas/gold_encounter_summary.json from GOLD_SCHEMA — ADR-012). Never edit either by
     hand; durable per-column prose goes in the generator's `_COLUMN_NOTES`. The doc-as-test
     (data dictionary) and the corpus contract test will fail if you skip these.
   - **ARCHITECTURE.md** — update if module/layer structure changed (keep the Mermaid +
     status table current). Hand-maintained; never auto-overwritten.
   - **BENCHMARKS.md** — update if scale/perf/row counts changed.
   - **CORPUS_CONTRACT.md** — update if the Gold/corpus schema changed (once it exists).
   - Only the generator writes a file (one file, marked generated); everything else is a
     judgment edit — never bulldoze hand-written content, surface conflicts instead.

4. **Run pytest tests/** — report results. If tests are failing, note them explicitly. Do not paper over failures.

5. **Write any pending ADRs** — if a non-obvious architectural decision was made this session and no ADR exists, write it now. Update docs/adr/README.md index.

6. **Commit** with a conventional commit message:
   Format: {type}({scope}): {description}
   Stage specific files — not git add -A

7. **Tell me the first task for next session** — one sentence, specific, actionable.

Do not skip any of these steps. If something can't be completed, note why in HANDOFF.md.
