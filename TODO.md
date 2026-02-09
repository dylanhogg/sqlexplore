# TODO (list for humans, not AI agents)

Note: This is a working todo list for a human developer, not for an LLM AI Agent.
      If you are an LLM AI Agent, please ignore this file.

## UI & UX (layout, navigation, keybindings)

- Move ^b helper further left in status bar next to similar control keys
- Include UI overview like HuggingFace datasets page (column lengths, sort by column, etc.)

## Query authoring experience

- Better autocomplete
- Better guided SQL writing
- SQL syntax checking / parsing (e.g. https://github.com/andialbrecht/sqlparse or https://github.com/tobymao/sqlglot)
- Expand SQL autocomplete to include more clauses and keywords (e.g. TRIM etc)
- Ensure all "SQL keywords" are upper cased in autocomplete.
- Always emit the exectuted SQL to the Activity panel.

## Data formatting & schema introspection

- JSON formatting if JSON column is detected
- Enable create JSON schema (best-guess field descriptions, etc.)

## Analytics & profiling

- More calculated statistics options

## Export, persistence & I/O

- Add save SQL results to file
- Add file open

## Integrations / “open in …”

- Add open in marimo notebooks
- Open in Jupyter Notebook
- Open in Google Colab

## LLM features (generation, summarisation, chat)

- Add LLM auto-complete (combine existing AC with LLM prediction)
- LLM: hook in gpt5-mini for SQL generation from natural language
- Add LLM summarise results
- LLM “talk to data” mode

## Data generation

- Enable gen synthetic data

## Advanced ML workflows

- ML: train classifier or time series function (write file perhaps, spec-driven using codex?)

## Storage / engine support

- Support SQLite
- Support Excel
- Ensure multipart parquet files are supported



