BASE_DUCKDB_GUIDANCE = (
    "DuckDB basics:\n"
    "- Use DuckDB SQL functions and syntax only.\n"
    "- Use only the provided table and columns.\n"
    "- Prefer explicit casts when mixing text, JSON, and temporal values."
)

REGEX_DUCKDB_GUIDANCE = (
    "Regex:\n"
    "- Use regexp_matches(text, pattern) for boolean regex checks.\n"
    "- Use regexp_extract(text, pattern[, group]) for extraction.\n"
    "- Use regexp_replace(text, pattern, replacement[, options]) for substitution.\n"
    "- For simple matching, LIKE/ILIKE/SIMILAR TO can be better than regex."
)

JSON_DUCKDB_GUIDANCE = (
    "JSON:\n"
    "- JSON functions expect JSON values; cast text with CAST(col AS JSON) when needed.\n"
    "- Use json_extract(json, path) for JSON values.\n"
    "- Use json_extract_string(json, path) for scalar text.\n"
    "- Use json_type/json_array_length/json_keys to inspect JSON shape."
)

STRUCT_DUCKDB_GUIDANCE = (
    "STRUCT:\n"
    "- For STRUCT values use col.field or struct_extract(col, 'field').\n"
    "- Do not apply JSON functions to STRUCT unless first converted to JSON."
)

TEMPORAL_DUCKDB_GUIDANCE = (
    "Date/time:\n"
    "- Use strptime(text, format) to parse date/timestamp strings.\n"
    "- Use strftime(value, format) to format date/timestamp values.\n"
    "- Use date_trunc/date_part for bucketing and extraction.\n"
    "- Cast explicitly between DATE, TIMESTAMP, and TIMESTAMPTZ when needed."
)

# DUCKDB_DOC_LINKS = (
#     "https://duckdb.org/docs/stable/sql/functions/overview",
#     "https://duckdb.org/docs/stable/sql/functions/text",
#     "https://duckdb.org/docs/stable/sql/functions/pattern_matching",
#     "https://duckdb.org/docs/stable/sql/functions/regular_expressions",
#     "https://duckdb.org/docs/stable/data/json/json_type",
#     "https://duckdb.org/docs/stable/data/json/json_functions",
#     "https://duckdb.org/docs/stable/sql/functions/struct",
#     "https://duckdb.org/docs/stable/sql/functions/date",
#     "https://duckdb.org/docs/stable/sql/functions/time",
#     "https://duckdb.org/docs/stable/sql/functions/timestamp",
#     "https://duckdb.org/docs/stable/sql/dialect/sql_quirks",
# )
