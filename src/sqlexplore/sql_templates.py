from __future__ import annotations

DEFAULT_LOAD_QUERY_TEMPLATE = "SELECT * FROM {source_sql}"

TXT_LOAD_QUERY_TEMPLATE = """
WITH source AS (
    SELECT
        row_number() OVER () AS line_number,
        line
    FROM {source_sql}
),
words AS (
    SELECT
        source.line_number,
        source.line,
        unnest(
            CASE
                WHEN trim(source.line) = '' THEN []::VARCHAR[]
                ELSE regexp_split_to_array(trim(source.line), '\\s+')
            END
        ) AS word
    FROM source
),
word_stats AS (
    SELECT
        source.line_number,
        source.line,
        COUNT(words.word) AS word_count,
        COALESCE(AVG(length(words.word)), 0.0) AS mean_word_length,
        COALESCE(MEDIAN(length(words.word)), 0.0) AS median_word_length,
        COALESCE(MAX(length(words.word)), 0) AS max_word_length,
        COALESCE(MIN(length(words.word)), 0) AS min_word_length
    FROM source
    LEFT JOIN words ON words.line_number = source.line_number
    GROUP BY source.line_number, source.line
)
SELECT
    word_stats.line,
    word_stats.line_number,
    length(word_stats.line) AS line_length,
    hash(word_stats.line) AS line_hash,
    word_stats.word_count,
    word_stats.mean_word_length,
    word_stats.median_word_length,
    word_stats.max_word_length,
    word_stats.min_word_length
FROM word_stats
ORDER BY word_stats.line_number
"""


def render_load_query(source_sql: str, template: str = DEFAULT_LOAD_QUERY_TEMPLATE) -> str:
    return template.format(source_sql=source_sql)
