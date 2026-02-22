# sqlexplore Architecture

## 1. Overview

`sqlexplore` is a terminal-first analytics tool for quickly querying flat data files and piped text with SQL, without standing up a separate data service.

It supports two core modes:

- Interactive mode: a Text UI (TUI) for iterative analysis, command discovery, query history, and result exploration.
- Non-interactive mode (`--no-ui`): run a query once, print results to the terminal, and exit.

Both modes use the same ingestion, command, and query execution core. The UI mode changes interaction style, not data semantics.

## 2. System Architecture (High-Level)

Major subsystems:

- CLI and session bootstrap: parses startup options, resolves run mode, and initializes a session.
- Ingestion layer: resolves inputs (local files, URLs, stdin), normalizes them, and prepares load-ready sources.
- Query engine layer: uses embedded DuckDB to load data into a logical table and execute SQL.
- Command system: parses slash commands, validates arguments, and either runs helper logic or generates SQL.
- LLM integration (optional): converts natural language to SQL, validates/repairs SQL, then executes through the same engine path.
- UI/output layer: renders responses in TUI or plain terminal output.
- Export layer: persists results/session artifacts (for example, data exports and replay artifacts).
- Observability/history layer: records query and LLM activity for traceability and reruns.

## 2. Data Flow

Input to queryable table:

1. Sources are provided via files, HTTP(S) URLs, or stdin.
2. URL sources are downloaded/cached locally before loading.
3. Inputs are normalized into one or more source relations.
4. Multiple sources are schema-checked, then unioned into one logical table.
5. The logical table becomes the stable query target for SQL and helper commands.

Query to output:

1. User input is classified as raw SQL or slash command.
2. Slash commands either:
   - Execute helper behavior directly, or
   - Compile into SQL and run through the same SQL execution path.
3. SQL executes in DuckDB and returns a structured result/message.
4. Results are rendered in TUI (table + activity surfaces) or in non-interactive console output.
5. Latest results can be exported to supported output formats.

## 3. Command Model

Slash commands are a semantic layer over SQL execution.

- Helper commands: domain-oriented shortcuts (summaries, filtering, profiling, history, export, etc.) that reduce manual SQL writing.
- Raw SQL: direct, unrestricted control for advanced analysis.
- Shared execution contract: both command-generated SQL and raw SQL flow through the same engine execution path, so behavior is consistent.

LLM-assisted query generation fits as an optional command path:

- Natural language request in.
- SQL generated (and optionally repaired) with schema-aware context.
- SQL validated before execution.
- Final execution still uses the standard query engine path.

## 4. Execution Model

The interactive app keeps the interface responsive by separating UI event handling from query execution work.

- UI remains event-driven while query work runs asynchronously/off the main UI loop.
- Queries are serialized as one active in-flight request at a time to keep state predictable.
- Long-running queries show loading/activity state and complete into the same response pipeline as fast queries.
- Non-interactive mode skips persistent UI lifecycle and performs one-shot execution.

## 5. Extensibility

Conceptual extension points:

- New input formats:
  - Add a new source-reader mapping in ingestion/load planning.
  - Keep the contract: source must map to a tabular relation in DuckDB.
- New helper commands:
  - Add command metadata, argument validation, and handler behavior.
  - Prefer generating SQL so new commands reuse existing execution/rendering paths.
- New output formats:
  - Add exporter adapters from the latest executed result/session context.
  - Keep output concerns separate from query planning/execution.
- New LLM capabilities:
  - Extend prompt/context construction, validation, retry policy, and telemetry.
  - Preserve the boundary that model output must become validated SQL before execution.

## 6. Architectural Principles

- Separation of concerns:
  - Input resolution, query execution, command semantics, and presentation are distinct layers.
- Dependency boundaries:
  - UI/CLI depend on the engine; engine does not depend on UI.
  - LLM is optional and pluggable; core SQL execution works without it.
- Predictable execution path:
  - Most user actions converge to SQL executed by one embedded engine.
- Trade-offs:
  - DuckDB as embedded engine: simple deployment and strong local performance, without external infra.
  - Terminal-first UX: optimized for developer workflows and scripting, with both exploratory and automatable usage.
  - Unified logical table model: simplifies querying across sources, while requiring schema compatibility for multi-source unioning.
