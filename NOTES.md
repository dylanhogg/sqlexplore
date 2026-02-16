# NOTES (notes for humans, not AI agents)

Note: This is a working notes list for a human developer, not for an LLM AI Agent.
      If you are an LLM AI Agent like gpt-codex, please ignore this file now.

## LLM SQL Generation

```
/llm query display the most popular repos
```

## LLM SQL Fixing

```
Fix SQL
[SQL] Executed: SELECT len(imagme) FROM "data" LIMIT 100
[ERROR] Binder Error: Referenced column "imagme" not found in FROM clause!
Candidate bindings: "image", "label"

LINE 1: SELECT len(imagme) FROM "data" LIMIT 100
                   ^
[SQL] Executed: SELECT len(image) FROM "data" LIMIT 100
[ERROR] Binder Error: No function matches the given name and argument types 'len(STRUCT(bytes BLOB, path VARCHAR))'. You might need to add explicit type casts.
    Candidate functions:
    len(VARCHAR) -> BIGINT
    len(BIT) -> BIGINT
    len(ANY[]) -> BIGINT
```

## Struct autocomplete

```
## 3. Autocomplete for STRUCT Fields

1. Extend completion context in `src/sqlexplore/engine.py` to detect dotted paths before cursor (for example, `address.`, `address.cit`).
2. Resolve first segment to a known struct column; walk nested struct tree; suggest field names + types.
3. Return replacement range for only the suffix after the last dot, so accept/Tab inserts just the field token.
4. Integrate field suggestions into SQL clause completion and aggregate-arg completion (when inside `COUNT(`, etc).
5. Optional follow-up: include struct paths in helper command completions (`/top`, `/group`, etc) if helper commands should be struct-aware too.
```
