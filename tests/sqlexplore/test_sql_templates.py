from sqlexplore.sql_templates import DEFAULT_LOAD_QUERY_TEMPLATE, TXT_LOAD_QUERY_TEMPLATE, render_load_query


def test_render_load_query_uses_default_template() -> None:
    source_sql = "read_parquet('/tmp/data.parquet')"
    assert render_load_query(source_sql) == f"SELECT * FROM {source_sql}"
    assert DEFAULT_LOAD_QUERY_TEMPLATE == "SELECT * FROM {source_sql}"


def test_render_load_query_injects_source_sql_for_txt_template() -> None:
    source_sql = "read_csv('/tmp/data.txt', columns={'line':'VARCHAR'})"
    query = render_load_query(source_sql, TXT_LOAD_QUERY_TEMPLATE)
    assert source_sql in query
    assert "word_count" in query
    assert "line_hash" in query
