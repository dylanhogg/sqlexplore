# PLAN: Minimal stdin support

Goal: let `sqlexplore` accept piped terminal output as text input.

## Scope (intentionally small)

- Support stdin as **text only**.
- Reuse current `.txt` ingestion path in engine.
- No multi-format parsing (`csv/tsv/parquet`) for stdin.

## Implementation

1. Update CLI arg in `/Users/dylan/_gitdrh/sqlexplore/src/sqlexplore/app.py`:
   - make `data` optional (`str | None`).
2. Resolve data source with one simple rule set:
   - if `data == "-"`: read stdin.
   - else if `data` missing and `not sys.stdin.isatty()`: read stdin.
   - else use existing local/URL logic unchanged.
3. For stdin mode:
   - stream stdin into temp file `stdin.txt` (UTF-8 text).
   - pass temp path into existing `SqlExplorerEngine`.
   - cleanup temp file in `finally`.
4. Default behavior for stdin mode:
   - if user did not pass `--execute`, `--file`, or `--no-ui`, force `--no-ui` behavior.
   - avoids launching TUI on transient piped input.
5. Errors:
   - if stdin selected but empty, raise `typer.BadParameter("No stdin input received.")`.

## Tests

Add focused tests in `/Users/dylan/_gitdrh/sqlexplore/tests/sqlexplore/test_main_data_source.py`:

1. `echo "a" | sqlexplore -` path uses temp `.txt`.
2. `echo "a" | sqlexplore` (no data arg) auto-uses stdin.
3. no data arg + TTY stdin -> clear usage error.
4. local file and URL behavior unchanged.

## CLI examples

```bash
ls -lha | sqlexplore -
```

```bash
ps aux | sqlexplore - --execute "SELECT * FROM data LIMIT 20"
```

```bash
cat /var/log/system.log | sqlexplore - --execute "SELECT line FROM data WHERE line ILIKE '%error%'"
```
