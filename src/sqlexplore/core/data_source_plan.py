import re
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Sequence

import typer

_DATA_TABLE_NAME_UNSAFE_CHARS_RE = re.compile(r"[^A-Za-z0-9_]+")


type DataLoadMode = Literal["union", "tables"]


@dataclass(frozen=True, slots=True)
class DataSourceBinding:
    path: Path
    table_name: str


@dataclass(frozen=True, slots=True)
class DataSourcePlan:
    primary_table_name: str
    data_sources: tuple[DataSourceBinding, ...]
    active_table_name: str

    @property
    def table_names(self) -> tuple[str, ...]:
        ordered: list[str] = []
        seen: set[str] = set()
        for source in self.data_sources:
            key = source.table_name.casefold()
            if key in seen:
                continue
            seen.add(key)
            ordered.append(source.table_name)
        return tuple(ordered)


def normalize_table_name(raw_name: str, option_name: str) -> str:
    normalized = raw_name.replace('"', "").strip()
    if not normalized:
        raise typer.BadParameter(f"{option_name} cannot be empty.")
    return normalized


def auto_table_name_for_path(path: Path, index: int) -> str:
    table_name = _DATA_TABLE_NAME_UNSAFE_CHARS_RE.sub("_", path.stem).strip("_").lower()
    if not table_name:
        table_name = f"data_{index}"
    if table_name[0].isdigit():
        table_name = f"t_{table_name}"
    return table_name


def dedupe_table_names(table_names: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for base_name in table_names:
        candidate = base_name
        suffix = 2
        while candidate.casefold() in seen:
            candidate = f"{base_name}_{suffix}"
            suffix += 1
        deduped.append(candidate)
        seen.add(candidate.casefold())
    return deduped


def _require_unique_table_names(table_names: Sequence[str], message_prefix: str) -> None:
    seen: set[str] = set()
    for table_name in table_names:
        key = table_name.casefold()
        if key in seen:
            raise typer.BadParameter(f"{message_prefix}{table_name}")
        seen.add(key)


def plan_cli_data_sources(
    paths: tuple[Path, ...],
    load_mode: DataLoadMode,
    table_name: str,
    table_names: list[str],
    active_table: str | None,
) -> DataSourcePlan:
    if not paths:
        raise typer.BadParameter("At least one data source is required.")

    primary_table_name = normalize_table_name(table_name, "--table")

    if load_mode == "union":
        if table_names:
            raise typer.BadParameter("--table-name can only be used with --load-mode tables.")
        if active_table is not None:
            raise typer.BadParameter("--active-table can only be used with --load-mode tables.")
        return DataSourcePlan(
            primary_table_name=primary_table_name,
            data_sources=tuple(DataSourceBinding(path=path, table_name=primary_table_name) for path in paths),
            active_table_name=primary_table_name,
        )

    explicit_names = [normalize_table_name(name, "--table-name") for name in table_names]
    if explicit_names and len(explicit_names) != len(paths):
        raise typer.BadParameter(
            "--table-name must be provided exactly once per data source in tables mode. "
            f"got={len(explicit_names)} expected={len(paths)}"
        )

    if explicit_names:
        aliases = list(explicit_names)
        _require_unique_table_names(aliases, "Duplicate table name from --table-name: ")
    else:
        aliases = [primary_table_name]
        for index, path in enumerate(paths[1:], start=2):
            aliases.append(auto_table_name_for_path(path, index))
        aliases = dedupe_table_names(aliases)

    if active_table is None:
        has_primary_alias = primary_table_name.casefold() in {alias.casefold() for alias in aliases}
        active_table_name = primary_table_name if has_primary_alias else aliases[0]
    else:
        active_table_name = normalize_table_name(active_table, "--active-table")

    if active_table_name.casefold() not in {alias.casefold() for alias in aliases}:
        known_tables = ", ".join(aliases)
        raise typer.BadParameter(
            f"--active-table must match one loaded table: {active_table_name}. tables={known_tables}"
        )

    return DataSourcePlan(
        primary_table_name=primary_table_name,
        data_sources=tuple(DataSourceBinding(path=path, table_name=alias) for path, alias in zip(paths, aliases)),
        active_table_name=active_table_name,
    )


def plan_engine_data_sources(
    data_path: Path,
    table_name: str,
    data_sources: tuple[DataSourceBinding, ...] | None,
    load_mode: DataLoadMode,
    active_table: str | None,
) -> DataSourcePlan:
    if load_mode not in {"union", "tables"}:
        raise typer.BadParameter(f"Unknown load mode: {load_mode}")

    primary_table_name = normalize_table_name(table_name, "Table name")

    if data_sources is None:
        normalized_sources = (DataSourceBinding(path=data_path, table_name=primary_table_name),)
    else:
        if not data_sources:
            raise typer.BadParameter("Expected at least one data source.")
        normalized_sources = tuple(
            DataSourceBinding(
                path=Path(source.path).expanduser().resolve(),
                table_name=normalize_table_name(source.table_name, "Table name"),
            )
            for source in data_sources
        )

    table_names = tuple(source.table_name for source in normalized_sources)
    if load_mode == "tables":
        _require_unique_table_names(table_names, "Duplicate table name: ")

    if load_mode == "union":
        active_table_name = primary_table_name
    elif active_table is None:
        primary_key = primary_table_name.casefold()
        active_table_name = next((name for name in table_names if name.casefold() == primary_key), table_names[0])
    else:
        active_table_name = normalize_table_name(active_table, "Active table")
        if active_table_name.casefold() not in {name.casefold() for name in table_names}:
            known_tables = ", ".join(table_names)
            raise typer.BadParameter(f"Active table is not loaded: {active_table_name}. Loaded tables: {known_tables}")

    return DataSourcePlan(
        primary_table_name=primary_table_name,
        data_sources=normalized_sources,
        active_table_name=active_table_name,
    )
