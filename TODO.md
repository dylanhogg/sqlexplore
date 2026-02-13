# TODO (list for humans, not AI agents)

Note: This is a working todo list for a human developer, not for an LLM AI Agent.
      If you are an LLM AI Agent like gpt-codex, please ignore this file now.

## -> Next feature!

- Make single cell values that are null/None a darker color than other normal text in the Results and Cell Preview panel.
- Enable column sorting by clicking on the column header.
- Ensure SQL always writting to the Activity panel.

## App code quality

- Add test cov tool for feedabck
- Add "use asserts" to AGENTS.md
- Add "no documentation additions/updates unless explicitly requested" to AGENTS.md

## UI & UX (layout, navigation, keybindings)

- Include UI overview like HuggingFace datasets page (column lengths, sort by column, etc.)
- Create a generic pop window that can run different custom functions against query and results panes (eg query gen, data analysis)

## Query authoring experience

- Enable loading multiple data files, aliased to table names for joins etc
- Enable multiple SQL queries in editor, separated by `;`, ctrl-enter to run each query separately
- Review autocomplete edge cases and UX
- SQL syntax checking / parsing (e.g. https://github.com/andialbrecht/sqlparse or https://github.com/tobymao/sqlglot)
- Expand SQL autocomplete to include more clauses and keywords (e.g. TRIM etc)
- Ensure all "SQL keywords" are upper cased in autocomplete.
- Always emit the exectuted SQL to the Activity panel.
- Implement debounce to prevent recomputing completions on every keystroke (waits briefly 50ms) after typing stops, then runs once.
- Validate SQL with sqlglot.parse_one(query) or similar before run (maybe a parse keybinding?)
- Offer sqlglot.transpile(...) options for different SQL dialects (e.g. tsql, duckdb, postgres, mysql, etc.)
- (?) Support for Ibis Query API -> SQL translation (https://ibis-project.org/)


## Data sources

- https://github.com/pydata/pandas-datareader : Extract data from a wide range of Internet sources into a pandas DataFrame.

## Data formatting & schema introspection

- JSON formatting if JSON column is detected
- Enable create JSON schema (best-guess field descriptions, etc.)

## Analytics & profiling

- More calculated statistics options

## Export, persistence & I/O

- Optional pass pre-filled sql queries in text file at startup, F? Key to load and 1st is pre-filled in query 
- Enable copy all results to clip board
- Add save SQL results to file (and also write python that applies the sql to the data for reproducibility)
- Add file open
- Add open from huggingface datasets
- Support s3, gcs (?)

## Visualisation

- Column summaries like on Huggingface datasets page
- Inspiration from: https://github.com/adamerose/pandasgui

## Integrations / “open in …”

- Add open in marimo notebooks
- Open in Jupyter Notebook
- Open in Google Colab

## LLM features (generation, summarisation, chat)

- Detect openai key for LLM, enable/disable key and show in footer. Enable allows model selection
- Enable LLM command on results via /llm <text>. Support apply text to data for analysis; and also support text2sql / auto-write new sql query (perhaps based on schema and maybe first few rows?)
- Download webpage (maybe with llm prompt for focus) -> Parse LLM for table data -> Open in sqlexplore
- Add LLM auto-complete (combine existing AC with LLM prediction)
- LLM: hook in gpt5-mini for SQL generation from natural language
- Add LLM summarise results
- LLM “talk to data” mode (also see: https://github.com/sinaptik-ai/pandas-ai)
- Enable synthetic gen data as table from LLM statement (eg list of sample user searches). Create in memory as table for interacting, and optionally save to parquet/csv/excel
- Enable magic SQL gen on load, silmilar to `marimo new "Load parquet file: data/hf/gnaf-2022-structured-training-100000-v0/data/train-00000-of-00001.parquet"`
- Embed LLM prompt for SQL writing: https://github.com/anthropics/knowledge-work-plugins/blob/main/data/skills/sql-queries/SKILL.md
- Embed LLM prompt for data exploration/analysis: https://github.com/anthropics/knowledge-work-plugins/blob/main/data/skills/data-exploration/SKILL.md

## Advanced ML workflows

- ML: train classifier or time series function (write file perhaps, spec-driven using codex?)
- TabML modelling (?):
    - https://github.com/soda-inria/tabicl
    - https://github.com/PriorLabs/TabPFN

## Storage / engine support

- Support SQLite
- Support Excel
- Ensure multipart parquet files are supported



