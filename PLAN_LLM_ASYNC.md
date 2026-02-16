# LLM Query Async Spinner Plan

## Goal
When `/llm query <natural language query>` runs, keep TUI responsive and show spinner in Query pane until response is applied.

## Plan

1. Move query execution off UI thread.
- File: `src/sqlexplore/ui/tui.py`
- Keep `action_run_query` as trigger.
- Start async background coroutine.
- In coroutine, run `engine.run_input(query)` via `await asyncio.to_thread(...)`.

2. Show spinner while query runs.
- File: `src/sqlexplore/ui/tui.py`
- Use Textual built-in loading state on query editor (`editor.loading = True/False`).
- Set `loading = True` before background call.
- Set `loading = False` in `finally`, then apply response.

3. Prevent overlapping runs.
- File: `src/sqlexplore/ui/tui.py`
- Add small guard (`_query_running` or `_query_task`).
- If a query is already running, ignore repeated run actions.

4. Keep response handling unchanged.
- File: `src/sqlexplore/ui/tui.py`
- Reuse existing `_apply_response(...)` after background call completes.
- Preserve current logs, result rendering, status handling.

5. Add focused tests.
- File: `tests/sqlexplore/test_app.py`
- Test with stubbed slow `engine.run_input`:
  - Trigger run.
  - Assert query editor shows loading while pending.
  - Assert loading clears after completion.
  - Assert response is rendered/logged.
- Optional: assert repeated run keypress while pending does not start second request.

## Notes
- Keep implementation minimal: async thread handoff + built-in loading flag.
- Avoid custom spinner timers/state machines.
