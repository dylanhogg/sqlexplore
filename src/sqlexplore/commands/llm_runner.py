from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal

from sqlexplore.core.engine_models import EngineResponse
from sqlexplore.core.logging_utils import get_logger, log_event
from sqlexplore.llm.llm_sql import (
    SampleRows,
    build_prompt,
    build_repair_prompt,
    build_schema_context,
    fetch_sample_rows,
    generate_sql,
    llm_trace_context,
    validate_generated_sql,
)

from .protocols import CommandEngine

logger = get_logger(__name__)
DEFAULT_RETRYABLE_DUCKDB_ERROR_MARKERS = (
    "catalog error:",
    "parser error:",
    "binder error:",
    "catalogexception",
    "parserexception",
    "binderexception",
)

type LlmRunStatus = Literal["response", "provider_error", "invalid_sql"]
type BuildSchemaContextFn = Callable[[CommandEngine], str]
type FetchSampleRowsFn = Callable[[CommandEngine], SampleRows]
type BuildPromptFn = Callable[[str, str, str, SampleRows], str]
type BuildRepairPromptFn = Callable[[str, str, str, str, str, SampleRows], str]
type GenerateSqlFn = Callable[[str, str], str]
type ValidateGeneratedSqlFn = Callable[[str, str], str | None]


@dataclass(frozen=True, slots=True)
class LlmRunnerConfig:
    max_retries: int = 1
    retryable_error_markers: tuple[str, ...] = DEFAULT_RETRYABLE_DUCKDB_ERROR_MARKERS


@dataclass(frozen=True, slots=True)
class LlmRunnerDeps:
    build_schema_context: BuildSchemaContextFn
    fetch_sample_rows: FetchSampleRowsFn
    build_prompt: BuildPromptFn
    build_repair_prompt: BuildRepairPromptFn
    generate_sql: GenerateSqlFn
    validate_generated_sql: ValidateGeneratedSqlFn


@dataclass(slots=True)
class LlmRunResult:
    status: LlmRunStatus
    retry_count: int
    response: EngineResponse | None = None
    invalid_sql_detail: str | None = None
    generated_sql: str | None = None

    @classmethod
    def provider_error(cls, retry_count: int) -> LlmRunResult:
        return cls(status="provider_error", retry_count=retry_count)

    @classmethod
    def invalid_sql(cls, retry_count: int, detail: str, sql: str) -> LlmRunResult:
        return cls(status="invalid_sql", retry_count=retry_count, invalid_sql_detail=detail, generated_sql=sql)

    @classmethod
    def response_result(cls, retry_count: int, response: EngineResponse) -> LlmRunResult:
        return cls(status="response", retry_count=retry_count, response=response)


def is_retryable_duckdb_sql_error(message: str, retryable_error_markers: tuple[str, ...]) -> bool:
    payload = message.strip().casefold()
    return any(marker in payload for marker in retryable_error_markers)


def _run_generated_sql(engine: CommandEngine, sql: str) -> EngineResponse:
    out = engine.run_sql(sql, query_type="llm_generated_sql")
    out.generated_sql = sql
    return out


def _build_repair_prompt(
    deps: LlmRunnerDeps,
    user_query: str,
    previous_sql: str,
    error_message: str,
    table_name: str,
    schema_context: str,
    sample_rows: SampleRows,
) -> str:
    return deps.build_repair_prompt(
        user_query,
        previous_sql,
        error_message,
        table_name,
        schema_context,
        sample_rows,
    )


def _log_retry_event(
    trace_id: str | None,
    retry_count: int,
    reason: str,
    error: str,
    previous_sql: str,
) -> None:
    if trace_id is None:
        return
    log_event(
        "llm.retry",
        {
            "trace_id": trace_id,
            "retry_count": retry_count,
            "reason": reason,
            "error": error,
            "previous_sql": previous_sql,
        },
        logger=logger,
    )


def _log_sql_execution_event(
    trace_id: str | None,
    retry_count: int,
    sql: str,
    result: EngineResponse,
) -> None:
    if trace_id is None:
        return
    log_event(
        "llm.sql_execution",
        {
            "trace_id": trace_id,
            "retry_count": retry_count,
            "status": result.status,
            "sql": sql,
            "message": result.message,
        },
        logger=logger,
    )


def _default_runner_deps() -> LlmRunnerDeps:
    return LlmRunnerDeps(
        build_schema_context=build_schema_context,
        fetch_sample_rows=fetch_sample_rows,
        build_prompt=build_prompt,
        build_repair_prompt=build_repair_prompt,
        generate_sql=generate_sql,
        validate_generated_sql=validate_generated_sql,
    )


def run_llm_query_with_retry(
    engine: CommandEngine,
    user_query: str,
    model: str,
    config: LlmRunnerConfig | None = None,
    deps: LlmRunnerDeps | None = None,
    trace_id: str | None = None,
) -> LlmRunResult:
    active_config = config or LlmRunnerConfig()
    if active_config.max_retries < 0:
        raise ValueError(f"Expected non-negative retries, got {active_config.max_retries=}")
    active_deps = deps or _default_runner_deps()

    schema_context = active_deps.build_schema_context(engine)
    sample_rows = active_deps.fetch_sample_rows(engine)
    current_prompt = active_deps.build_prompt(
        user_query,
        engine.table_name,
        schema_context,
        sample_rows,
    )
    retry_count = 0
    sql: str
    with llm_trace_context(trace_id):
        while True:
            try:
                sql = active_deps.generate_sql(current_prompt, model)
            except Exception:  # noqa: BLE001
                logger.exception("llm command provider error model=%s retry_count=%s", model, retry_count)
                return LlmRunResult.provider_error(retry_count)

            sql_error = active_deps.validate_generated_sql(sql, engine.table_name)
            if sql_error is not None:
                if retry_count >= active_config.max_retries:
                    logger.error("llm command invalid sql reason=%s", sql_error)
                    return LlmRunResult.invalid_sql(retry_count, sql_error, sql)
                retry_count += 1
                logger.info("llm command retry=%s reason=validation error=%s", retry_count, sql_error)
                _log_retry_event(trace_id, retry_count, "validation_error", sql_error, sql)
                current_prompt = _build_repair_prompt(
                    active_deps,
                    user_query,
                    sql,
                    sql_error,
                    engine.table_name,
                    schema_context,
                    sample_rows,
                )
                continue

            logger.info("llm command generated sql chars=%s retry_count=%s", len(sql), retry_count)
            out = _run_generated_sql(engine, sql)
            _log_sql_execution_event(trace_id, retry_count, sql, out)
            if out.status != "error":
                return LlmRunResult.response_result(retry_count, out)
            if retry_count >= active_config.max_retries or not is_retryable_duckdb_sql_error(
                out.message,
                active_config.retryable_error_markers,
            ):
                return LlmRunResult.response_result(retry_count, out)
            retry_count += 1
            logger.info("llm command retry=%s reason=duckdb_error error=%s", retry_count, out.message)
            _log_retry_event(trace_id, retry_count, "duckdb_error", out.message, sql)
            current_prompt = _build_repair_prompt(
                active_deps,
                user_query,
                sql,
                out.message,
                engine.table_name,
                schema_context,
                sample_rows,
            )
