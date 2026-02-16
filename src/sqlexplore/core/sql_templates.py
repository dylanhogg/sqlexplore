DEFAULT_LOAD_QUERY_TEMPLATE = "SELECT * FROM {source_sql}"

TXT_LOAD_QUERY_TEMPLATE = """
WITH source_text AS (
    SELECT
        content,
        regexp_split_to_array(content, '\\r?\\n') AS line_array,
        right(content, 1) = chr(10) AS has_trailing_newline
    FROM {source_sql}
),
source AS (
    SELECT
        lines.line_number,
        lines.line
    FROM source_text
    CROSS JOIN unnest(source_text.line_array) WITH ORDINALITY AS lines(line, line_number)
    WHERE source_text.content <> ''
    AND NOT (
        source_text.has_trailing_newline
        AND lines.line = ''
        AND lines.line_number = array_length(source_text.line_array)
    )
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
