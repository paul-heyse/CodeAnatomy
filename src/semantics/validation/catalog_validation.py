"""Information-schema-driven validation for semantic inputs."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final, cast

import pyarrow as pa
from datafusion import col, lit

if TYPE_CHECKING:
    from datafusion import SessionContext

    from semantics.specs import SemanticTableSpec


@dataclass(frozen=True)
class ColumnValidationSpec:
    """Required column specification for a semantic input table."""

    canonical_name: str
    required_columns: tuple[str, ...]


@dataclass(frozen=True)
class SemanticInputValidationResult:
    """Validation result for semantic inputs."""

    valid: bool
    missing_tables: tuple[str, ...]
    missing_columns: Mapping[str, tuple[str, ...]]
    resolved_tables: Mapping[str, str]


SEMANTIC_INPUT_COLUMN_SPECS: Final[tuple[ColumnValidationSpec, ...]] = ()


def _information_schema_tables(ctx: SessionContext) -> set[str]:
    """Return table names from information_schema.

    Parameters
    ----------
    ctx
        DataFusion session context.

    Returns:
    -------
    set[str]
        Table names available in information_schema.
    """
    rows = _collect_info_schema_column(ctx, "information_schema.tables", "table_name")
    return {value for value in rows if isinstance(value, str)}


def _information_schema_columns(ctx: SessionContext, table_name: str) -> set[str]:
    """Return column names for a table from information_schema.

    Parameters
    ----------
    ctx
        DataFusion session context.
    table_name
        Table name to inspect.

    Returns:
    -------
    set[str]
        Column names for the table.
    """
    rows = _collect_info_schema_column(
        ctx,
        "information_schema.columns",
        "column_name",
        table_filter=table_name,
    )
    return {value for value in rows if isinstance(value, str)}


def _collect_info_schema_column(
    ctx: SessionContext,
    table: str,
    column: str,
    *,
    table_filter: str | None = None,
) -> list[object]:
    try:
        df = ctx.table(table)
        if table_filter is not None:
            df = df.filter(col("table_name") == lit(table_filter))
        batches = df.select(column).collect()
    except (RuntimeError, TypeError, ValueError) as exc:
        msg = f"Failed to query {table}."
        raise ValueError(msg) from exc
    if not batches:
        return []
    first = batches[0]
    if isinstance(first, pa.RecordBatch):
        table_view = pa.Table.from_batches(cast("Sequence[pa.RecordBatch]", batches))
        if column not in table_view.column_names:
            return []
        return table_view[column].to_pylist()
    values: list[object] = []
    for row in batches:
        if isinstance(row, Mapping):
            values.append(row.get(column))
            continue
        if isinstance(row, Sequence) and not isinstance(row, (str, bytes, bytearray)):
            if row:
                values.append(row[0])
            continue
        values.append(row)
    return values


def validate_semantic_input_columns(
    ctx: SessionContext,
    *,
    input_mapping: Mapping[str, str],
    specs: Sequence[ColumnValidationSpec] | None = None,
) -> SemanticInputValidationResult:
    """Validate required columns for semantic input tables.

    Parameters
    ----------
    ctx
        DataFusion session context with information_schema enabled.
    input_mapping
        Mapping from canonical input names to registered table names.
    specs
        Optional column validation specs to override defaults.

    Returns:
    -------
    SemanticInputValidationResult
        Validation result with missing tables/columns details.
    """
    resolved_specs = tuple(specs) if specs is not None else _semantic_input_column_specs()
    table_names = _information_schema_tables(ctx)
    missing_tables: list[str] = []
    missing_columns: dict[str, tuple[str, ...]] = {}
    resolved_tables: dict[str, str] = {}

    for spec in resolved_specs:
        table_name = input_mapping.get(spec.canonical_name, spec.canonical_name)
        resolved_tables[spec.canonical_name] = table_name
        if table_name not in table_names:
            missing_tables.append(table_name)
            continue
        available = _information_schema_columns(ctx, table_name)
        missing = tuple(col_name for col_name in spec.required_columns if col_name not in available)
        if missing:
            missing_columns[table_name] = missing

    return SemanticInputValidationResult(
        valid=not missing_tables and not missing_columns,
        missing_tables=tuple(missing_tables),
        missing_columns=missing_columns,
        resolved_tables=resolved_tables,
    )


def _semantic_input_column_specs() -> tuple[ColumnValidationSpec, ...]:
    from datafusion_engine.schema import extract_schema_for
    from semantics.registry import SEMANTIC_TABLE_SPECS

    specs: list[ColumnValidationSpec] = []
    for name, table_spec in SEMANTIC_TABLE_SPECS.items():
        try:
            schema = extract_schema_for(name)
        except KeyError:
            continue
        required = _required_columns_from_table_spec(table_spec, schema.names)
        specs.append(ColumnValidationSpec(canonical_name=name, required_columns=required))
    specs.append(
        ColumnValidationSpec(
            canonical_name="scip_occurrences",
            required_columns=(
                "path",
                "symbol",
                "start_line",
                "end_line",
                "start_char",
                "end_char",
                "line_base",
                "col_unit",
            ),
        )
    )
    specs.append(
        ColumnValidationSpec(
            canonical_name="file_line_index_v1",
            required_columns=("path", "line_no", "line_start_byte", "line_text"),
        )
    )
    return tuple(specs)


def _required_columns_from_table_spec(
    table_spec: SemanticTableSpec,
    schema_names: Sequence[str],
) -> tuple[str, ...]:
    required: list[str] = []

    def _add(name: str | None) -> None:
        if not name or name not in schema_names:
            return
        if name not in required:
            required.append(name)

    _add("file_id")
    _add(table_spec.path_col)
    _add(table_spec.primary_span.start_col)
    _add(table_spec.primary_span.end_col)
    for col_name in table_spec.text_cols:
        _add(col_name)
    return tuple(required)


__all__ = [
    "SEMANTIC_INPUT_COLUMN_SPECS",
    "ColumnValidationSpec",
    "SemanticInputValidationResult",
    "validate_semantic_input_columns",
]
