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

3. **Run pytest tests/** — report results. If tests are failing, note them explicitly. Do not paper over failures.

4. **Write any pending ADRs** — if a non-obvious architectural decision was made this session and no ADR exists, write it now. Update docs/adr/README.md index.

5. **Commit** with a conventional commit message:
   Format: {type}({scope}): {description}
   Stage specific files — not git add -A

6. **Tell me the first task for next session** — one sentence, specific, actionable.

Do not skip any of these steps. If something can't be completed, note why in HANDOFF.md.