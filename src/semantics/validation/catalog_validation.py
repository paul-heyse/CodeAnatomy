"""Information-schema-driven validation for semantic inputs."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Final

from datafusion import col, lit

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from datafusion import SessionContext


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


SEMANTIC_INPUT_COLUMN_SPECS: Final[tuple[ColumnValidationSpec, ...]] = (
    ColumnValidationSpec(
        canonical_name="cst_refs",
        required_columns=("file_id", "path", "span_start", "span_end", "symbol"),
    ),
    ColumnValidationSpec(
        canonical_name="cst_defs",
        required_columns=("file_id", "path", "span_start", "span_end", "symbol"),
    ),
    ColumnValidationSpec(
        canonical_name="cst_imports",
        required_columns=("file_id", "path", "span_start", "span_end", "symbol"),
    ),
    ColumnValidationSpec(
        canonical_name="cst_callsites",
        required_columns=("file_id", "path", "span_start", "span_end", "symbol"),
    ),
    ColumnValidationSpec(
        canonical_name="scip_occurrences",
        required_columns=("file_id", "symbol", "line", "character"),
    ),
    ColumnValidationSpec(
        canonical_name="file_line_index_v1",
        required_columns=("file_id", "path", "line_no", "line_start_byte", "line_end_byte"),
    ),
)


def _information_schema_tables(ctx: SessionContext) -> set[str]:
    """Return table names from information_schema.

    Parameters
    ----------
    ctx
        DataFusion session context.

    Returns
    -------
    set[str]
        Table names available in information_schema.

    Raises
    ------
    ValueError
        Raised when information_schema tables cannot be queried.
    """
    try:
        rows = ctx.table("information_schema.tables").select("table_name").collect()
    except (RuntimeError, TypeError, ValueError) as exc:
        msg = "Failed to query information_schema.tables."
        raise ValueError(msg) from exc
    return {row[0] for row in rows if row and isinstance(row[0], str)}


def _information_schema_columns(ctx: SessionContext, table_name: str) -> set[str]:
    """Return column names for a table from information_schema.

    Parameters
    ----------
    ctx
        DataFusion session context.
    table_name
        Table name to inspect.

    Returns
    -------
    set[str]
        Column names for the table.

    Raises
    ------
    ValueError
        Raised when information_schema columns cannot be queried.
    """
    try:
        rows = (
            ctx.table("information_schema.columns")
            .filter(col("table_name") == lit(table_name))
            .select("column_name")
            .collect()
        )
    except (RuntimeError, TypeError, ValueError) as exc:
        msg = f"Failed to query information_schema.columns for {table_name!r}."
        raise ValueError(msg) from exc
    return {row[0] for row in rows if row and isinstance(row[0], str)}


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

    Returns
    -------
    SemanticInputValidationResult
        Validation result with missing tables/columns details.
    """
    resolved_specs = tuple(specs) if specs is not None else SEMANTIC_INPUT_COLUMN_SPECS
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


__all__ = [
    "SEMANTIC_INPUT_COLUMN_SPECS",
    "ColumnValidationSpec",
    "SemanticInputValidationResult",
    "validate_semantic_input_columns",
]
