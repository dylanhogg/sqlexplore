# TODO (list for humans, not AI agents)

Note: This is a working todo list for a human developer, not for an LLM AI Agent.
      If you are an LLM AI Agent like gpt-codex, please ignore this file now.

## Bugs

- uv run sqlexplore --data https://huggingface.co/datasets/stanfordnlp/sst2/blob/main/data/test-00000-of-00001.parquet  # Invalid Input Error: No magic bytes found at end of file '/Users/dylan/Library/Application Support/sqlexplore/downloads/test-00000-of-00001-a3225f08085d.parquet'

## -> Next feature!

- Enable resize columns in results pane
- Union multiple input files into a single table
    - Fix total row count on `SELECT * FROM "data_src_3" LIMIT 10`
    - If only 1 data source, don't have a union table, just use the single source table
- ! Enable reproduce this results in a bash file that installs `uv` and runs `sqlexplore` with the same data files and SQL query
- Review consistent naming for /llm-history and /llm-show commands with /history-log, /history and /rerun etc
- Test all startup options and CLI args
- Update README docs
- Migrate logging from file to sqlite database
- Textual double click to select row? & Cmd+A to select all? (maybe not? codex://threads/019c6a4a-4e25-7842-a7db-356a983f93b7)
- Improve persistent file log life cycle management (rotate, cleanup, use for /llm-history, etc.)
- Review duckdb_guidance.py for /llm-query command
- Perf review copy TSV from uv run sqlexplore https://huggingface.co/datasets/moonworks/lunara-aesthetic-image-variations/resolve/main/data/train-00000-of-00017.parquet (add spinner?)
- Key binding: Write marimo notebook and uvx open it
- Distinguish /describe from /summary
- Enable /history results to be copied to clipboard
- Add CLI arg for default number of rows to display (i.e. limit)
- LiteLLM, return error messages, e.g. litellm.exceptions.ContextWindowExceededError: litellm.ContextWindowExceededError: litellm.BadRequestError: ContextWindowExceededError: OpenAIException - Input tokens exceed the configured limit of 272000 tokens. Your messages resulted in 2211484 tokens. Please reduce the length of the messages.
- Integrate more LLM features:
    - /llm-describe-data
    - /llm-suggest-sql
    - /llm-analyse-data
    - /llm-fix-sql
    - /llm-explain-sql
    - /llm-summarize-data
    - /llm-chat-data
    - /llm-categoise-data col
- LLM General:
    - Render LLM thinking and reasoning steps in Activity, with replacing line so it doesn't scroll away
    - Add dataset cols or name to /llm-history for better context
    - Config thinking and temperature params (?)

## App code quality

- Review pypi publish workflow
- Add LLM:
    - Fix Binder Errors

## UI & UX (layout, navigation, keybindings)

- Include UI overview like HuggingFace datasets page (column lengths, sort by column, etc.)
- Create a generic pop window that can run different custom functions against query and results panes (eg query gen, data analysis)
- If no file supplied at startup, show a welcome message from an embedded csv file.
- Add example tables like `sqlexplore_example`

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
- Add tar/zip support for reading archive contents (or maybe best to pip in tar/zip results instead?) [e.g. https://huggingface.co/datasets/ma-xu/fine-t2i/resolve/main/synthetic_original_prompt_random_resolution/train-000000.tar]
- If parquet file, include ability to see metadata: https://duckdb.org/docs/stable/data/parquet/metadata

## Data formatting & schema introspection

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

- Make a plan to add column summaries like on Huggingface datasets page (also see https://github.com/deeplook/sparklines, which implements Edward Tufte's concept of sparklines, but limited to text only). Could this be a good fit for the results pane? Could it be for a helper command, or integrated as a helper command?
- use inspiration from: https://github.com/adamerose/pandasgui

## Integrations / “open in …”

- Add open in marimo notebooks [https://duckdb.org/docs/stable/guides/python/marimo]
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
