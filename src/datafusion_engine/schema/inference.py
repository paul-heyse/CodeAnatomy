"""Schema inference from DataFusion plans.

This module provides schema inference capabilities that extract output schemas
and column-level lineage directly from DataFusion logical plans and DataFrames.
By deriving schemas from plan metadata, manual schema declarations become
unnecessary for most view and task definitions.

Key capabilities:
- Infer Arrow schemas from DataFrame.schema() or LogicalPlan metadata
- Trace column-level lineage through projections, aliases, and casts
- Identify source tables referenced by each output column
- Build SchemaInferenceResult combining schema and lineage
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyarrow as pa

from datafusion_engine.lineage.datafusion import (
    ExprInfo,
    LineageReport,
    extract_lineage,
)

if TYPE_CHECKING:
    from datafusion.dataframe import DataFrame


@dataclass(frozen=True)
class ColumnLineage:
    """Column-level lineage for a single output column.

    Attributes:
    ----------
    column_name : str
        Name of the output column.
    source_columns : tuple[tuple[str, str], ...]
        Source columns as (table_name, column_name) pairs.
    source_tables : tuple[str, ...]
        Tables that contribute to this output column.
    transformations : tuple[str, ...]
        Expression transformations applied to derive this column.
    is_derived : bool
        True if the column results from an expression, not a direct reference.
    """

    column_name: str
    source_columns: tuple[tuple[str, str], ...] = ()
    source_tables: tuple[str, ...] = ()
    transformations: tuple[str, ...] = ()
    is_derived: bool = False


@dataclass(frozen=True)
class SchemaInferenceResult:
    """Complete schema inference result from a DataFusion plan.

    Attributes:
    ----------
    output_schema : pa.Schema
        Arrow schema of the plan output.
    column_lineage : Mapping[str, ColumnLineage]
        Column-level lineage for each output column.
    source_tables : frozenset[str]
        All source tables referenced by the plan.
    lineage_report : LineageReport | None
        Full lineage report when available.
    """

    output_schema: pa.Schema
    column_lineage: Mapping[str, ColumnLineage]
    source_tables: frozenset[str]
    lineage_report: LineageReport | None = None


def infer_schema_from_dataframe(df: DataFrame) -> pa.Schema:
    """Extract Arrow schema from a DataFusion DataFrame.

    Args:
        df: Description.

    Returns:
        pa.Schema: Result.

    Raises:
        TypeError: If the operation cannot be completed.
    """
    schema = df.schema()
    if isinstance(schema, pa.Schema):
        return schema
    to_arrow = getattr(schema, "to_arrow", None)
    if callable(to_arrow):
        try:
            resolved = to_arrow()
            if isinstance(resolved, pa.Schema):
                return resolved
        except (RuntimeError, TypeError, ValueError):
            pass
    msg = "Unable to resolve DataFrame schema to Arrow schema."
    raise TypeError(msg)


def infer_schema_from_logical_plan(plan: object) -> pa.Schema:
    """Extract Arrow schema from a DataFusion LogicalPlan.

    Args:
        plan: Description.

    Returns:
        pa.Schema: Result.

    Raises:
        TypeError: If the operation cannot be completed.
    """
    schema_method = getattr(plan, "schema", None)
    if not callable(schema_method):
        msg = "LogicalPlan does not expose a schema() method."
        raise TypeError(msg)
    try:
        schema = schema_method()
    except (RuntimeError, TypeError, ValueError) as exc:
        msg = "Failed to extract schema from LogicalPlan."
        raise TypeError(msg) from exc
    if isinstance(schema, pa.Schema):
        return schema
    to_arrow = getattr(schema, "to_arrow", None)
    if callable(to_arrow):
        try:
            resolved = to_arrow()
            if isinstance(resolved, pa.Schema):
                return resolved
        except (RuntimeError, TypeError, ValueError):
            pass
    fields = _extract_fields_from_dfschema(schema)
    if fields:
        return pa.schema(fields)
    msg = "Unable to resolve LogicalPlan schema to Arrow schema."
    raise TypeError(msg)


def _extract_fields_from_dfschema(schema: object) -> list[pa.Field]:
    """Extract Arrow fields from a DataFusion DFSchema when direct conversion fails.

    Parameters
    ----------
    schema : object
        DFSchema object to extract fields from.

    Returns:
    -------
    list[pa.Field]
        Extracted Arrow fields, or empty list if extraction fails.
    """
    fields: list[pa.Field] = []
    fields_method = getattr(schema, "fields", None)
    if not callable(fields_method):
        return fields
    try:
        schema_fields = fields_method()
    except (RuntimeError, TypeError, ValueError):
        return fields
    if not isinstance(schema_fields, Sequence):
        return fields
    for entry in schema_fields:
        name = _safe_field_name(entry)
        dtype = _safe_field_type(entry)
        nullable = _safe_field_nullable(entry)
        if name is None or dtype is None:
            continue
        fields.append(pa.field(name, dtype, nullable=nullable))
    return fields


def _safe_field_name(entry: object) -> str | None:
    """Extract field name from a DFField.

    Returns:
    -------
    str | None
        Field name or None if unavailable.
    """
    name_method = getattr(entry, "name", None)
    if callable(name_method):
        try:
            name = name_method()
            if isinstance(name, str):
                return name
        except (RuntimeError, TypeError, ValueError):
            pass
    name_attr = getattr(entry, "name", None)
    if isinstance(name_attr, str):
        return name_attr
    return None


def _safe_field_type(entry: object) -> pa.DataType | None:
    """Extract Arrow data type from a DFField.

    Returns:
    -------
    pa.DataType | None
        Arrow data type or None if unavailable.
    """
    dtype_method = getattr(entry, "data_type", None)
    if callable(dtype_method):
        try:
            dtype = dtype_method()
            if isinstance(dtype, pa.DataType):
                return dtype
        except (RuntimeError, TypeError, ValueError):
            pass
    dtype_attr = getattr(entry, "data_type", None)
    if isinstance(dtype_attr, pa.DataType):
        return dtype_attr
    arrow_type = getattr(entry, "arrow_type", None)
    if callable(arrow_type):
        try:
            resolved = arrow_type()
            if isinstance(resolved, pa.DataType):
                return resolved
        except (RuntimeError, TypeError, ValueError):
            pass
    return None


def _safe_field_nullable(entry: object) -> bool:
    """Extract nullability from a DFField.

    Returns:
    -------
    bool
        True if the field is nullable (defaults to True).
    """
    nullable_method = getattr(entry, "is_nullable", None)
    if callable(nullable_method):
        try:
            result = nullable_method()
            if isinstance(result, bool):
                return result
        except (RuntimeError, TypeError, ValueError):
            pass
    nullable_attr = getattr(entry, "nullable", None)
    if isinstance(nullable_attr, bool):
        return nullable_attr
    return True


def infer_column_lineage(
    df: DataFrame,
    *,
    udf_snapshot: Mapping[str, object] | None = None,
) -> dict[str, ColumnLineage]:
    """Trace column-level lineage for each output column in a DataFrame.

    This function analyzes the DataFrame's optimized logical plan to determine
    which source columns contribute to each output column. It handles:
    - Direct column references
    - Aliased expressions
    - Aggregations and window functions
    - Casts and type coercions

    Parameters
    ----------
    df : DataFrame
        DataFusion DataFrame to analyze.
    udf_snapshot : Mapping[str, object] | None
        Optional UDF registry snapshot for function resolution.

    Returns:
    -------
    dict[str, ColumnLineage]
        Mapping from output column name to its lineage.

    Examples:
    --------
    >>> lineage = infer_column_lineage(df)
    >>> for col_name, col_lineage in lineage.items():
    ...     print(f"{col_name}: sources={col_lineage.source_tables}")
    """
    schema = infer_schema_from_dataframe(df)
    plan = _optimized_logical_plan(df)
    if plan is None:
        return _empty_column_lineage(schema)
    report = extract_lineage(plan, udf_snapshot=udf_snapshot)
    return _column_lineage_from_report(schema, report)


def _optimized_logical_plan(df: DataFrame) -> object | None:
    """Extract optimized logical plan from a DataFrame.

    Returns:
    -------
    object | None
        Optimized logical plan or None if unavailable.
    """
    method = getattr(df, "optimized_logical_plan", None)
    if not callable(method):
        method = getattr(df, "logical_plan", None)
        if not callable(method):
            return None
    try:
        return method()
    except (RuntimeError, TypeError, ValueError):
        return None


def _empty_column_lineage(schema: pa.Schema) -> dict[str, ColumnLineage]:
    """Create empty column lineage for all schema fields.

    Returns:
    -------
    dict[str, ColumnLineage]
        Mapping with empty lineage for each column.
    """
    return {field.name: ColumnLineage(column_name=field.name) for field in schema}


def _column_lineage_from_report(
    schema: pa.Schema,
    report: LineageReport,
) -> dict[str, ColumnLineage]:
    """Build column lineage from a LineageReport.

    Parameters
    ----------
    schema : pa.Schema
        Output schema with column names.
    report : LineageReport
        Lineage report from plan analysis.

    Returns:
    -------
    dict[str, ColumnLineage]
        Column lineage for each output column.
    """
    all_source_tables = frozenset(report.referenced_tables)
    column_refs_by_name = _column_refs_by_output_name(report)
    result: dict[str, ColumnLineage] = {}
    for schema_field in schema:
        col_name = schema_field.name
        refs = column_refs_by_name.get(col_name, ())
        source_columns = tuple(sorted(refs))
        source_tables = tuple(sorted({table for table, _ in refs if table}))
        if not source_tables:
            source_tables = tuple(sorted(all_source_tables))
        transformations = _transformations_for_column(col_name, report)
        is_derived = bool(transformations) or len(source_columns) > 1
        result[col_name] = ColumnLineage(
            column_name=col_name,
            source_columns=source_columns,
            source_tables=source_tables,
            transformations=transformations,
            is_derived=is_derived,
        )
    return result


def _column_refs_by_output_name(
    report: LineageReport,
) -> dict[str, set[tuple[str, str]]]:
    """Group column references by output column name.

    Returns:
    -------
    dict[str, set[tuple[str, str]]]
        Mapping from output column to source (table, column) pairs.
    """
    refs_by_name: dict[str, set[tuple[str, str]]] = {}
    for expr_info in report.exprs:
        if expr_info.kind == "Projection":
            _add_projection_refs(expr_info, refs_by_name)
        else:
            _add_general_refs(expr_info, refs_by_name)
    return refs_by_name


def _add_projection_refs(
    expr_info: ExprInfo,
    refs_by_name: dict[str, set[tuple[str, str]]],
) -> None:
    """Add column references from a projection expression.

    Parameters
    ----------
    expr_info : ExprInfo
        Expression info from a Projection node.
    refs_by_name : dict[str, set[tuple[str, str]]]
        Accumulator for column references.
    """
    text = expr_info.text
    if text is None:
        return
    output_name = _extract_alias_or_column_name(text)
    if not output_name:
        return
    refs_by_name.setdefault(output_name, set()).update(expr_info.referenced_columns)


def _add_general_refs(
    expr_info: ExprInfo,
    refs_by_name: dict[str, set[tuple[str, str]]],
) -> None:
    """Add column references from non-projection expressions.

    For aggregations, filters, and joins, associate references with all
    columns from the same dataset.

    Parameters
    ----------
    expr_info : ExprInfo
        Expression info from the plan.
    refs_by_name : dict[str, set[tuple[str, str]]]
        Accumulator for column references.
    """
    for table, col in expr_info.referenced_columns:
        refs_by_name.setdefault(col, set()).add((table, col))


def _extract_alias_or_column_name(text: str) -> str | None:
    """Extract output column name from expression text.

    Handles patterns like:
    - "column_name" -> column_name
    - "expr AS alias" -> alias
    - "table.column" -> column

    Returns:
    -------
    str | None
        Extracted column name or None.
    """
    text = text.strip()
    if not text:
        return None
    upper = text.upper()
    if " AS " in upper:
        parts = text.split(" AS ", 1)
        if len(parts) > 1:
            alias = parts[1].strip().strip('"').strip("'")
            return alias or None
    if "." in text and " " not in text and "(" not in text:
        return text.split(".")[-1]
    if " " not in text and "(" not in text:
        return text
    return None


def _transformations_for_column(
    col_name: str,
    report: LineageReport,
) -> tuple[str, ...]:
    """Extract transformation expressions for a column.

    Parameters
    ----------
    col_name : str
        Output column name.
    report : LineageReport
        Lineage report from plan analysis.

    Returns:
    -------
    tuple[str, ...]
        Transformation expressions that contributed to this column.
    """
    transformations: list[str] = []
    for expr_info in report.exprs:
        text = expr_info.text
        if text is None:
            continue
        if col_name not in text:
            continue
        if expr_info.kind in {"Aggregate", "Window"}:
            transformations.append(text)
        elif " AS " in text.upper() and col_name in text:
            alias = _extract_alias_or_column_name(text)
            if alias == col_name and "(" in text:
                transformations.append(text)
    return tuple(sorted(set(transformations)))


def infer_schema_with_lineage(
    df: DataFrame,
    *,
    udf_snapshot: Mapping[str, object] | None = None,
) -> SchemaInferenceResult:
    """Perform complete schema inference with column-level lineage.

    This is the primary entry point for schema inference. It combines schema
    extraction and column lineage analysis into a single result that can be
    used for contract generation and dependency tracking.

    Parameters
    ----------
    df : DataFrame
        DataFusion DataFrame to analyze.
    udf_snapshot : Mapping[str, object] | None
        Optional UDF registry snapshot for function resolution.

    Returns:
    -------
    SchemaInferenceResult
        Complete inference result with schema and lineage.

    Examples:
    --------
    >>> result = infer_schema_with_lineage(df)
    >>> print(f"Output columns: {result.output_schema.names}")
    >>> print(f"Source tables: {result.source_tables}")
    >>> for col, lineage in result.column_lineage.items():
    ...     print(f"  {col}: {lineage.source_tables}")
    """
    schema = infer_schema_from_dataframe(df)
    plan = _optimized_logical_plan(df)
    if plan is None:
        return SchemaInferenceResult(
            output_schema=schema,
            column_lineage=_empty_column_lineage(schema),
            source_tables=frozenset(),
            lineage_report=None,
        )
    report = extract_lineage(plan, udf_snapshot=udf_snapshot)
    column_lineage = _column_lineage_from_report(schema, report)
    source_tables = frozenset(report.referenced_tables)
    return SchemaInferenceResult(
        output_schema=schema,
        column_lineage=column_lineage,
        source_tables=source_tables,
        lineage_report=report,
    )


def infer_schema_from_plan_bundle(
    bundle: object,
    *,
    udf_snapshot: Mapping[str, object] | None = None,
) -> SchemaInferenceResult:
    """Extract schema inference from a DataFusionPlanBundle.

    Args:
        bundle: Description.
            udf_snapshot: Description.

    Returns:
        SchemaInferenceResult: Result.

    Raises:
        TypeError: If the operation cannot be completed.
    """
    df = getattr(bundle, "df", None)
    if df is None:
        msg = "PlanBundle missing 'df' attribute."
        raise TypeError(msg)
    optimized_plan = getattr(bundle, "optimized_logical_plan", None)
    schema = infer_schema_from_dataframe(df)
    if optimized_plan is None:
        return SchemaInferenceResult(
            output_schema=schema,
            column_lineage=_empty_column_lineage(schema),
            source_tables=frozenset(),
            lineage_report=None,
        )
    report = extract_lineage(optimized_plan, udf_snapshot=udf_snapshot)
    column_lineage = _column_lineage_from_report(schema, report)
    source_tables = frozenset(report.referenced_tables)
    return SchemaInferenceResult(
        output_schema=schema,
        column_lineage=column_lineage,
        source_tables=source_tables,
        lineage_report=report,
    )


def schema_fingerprint_from_inference(result: SchemaInferenceResult) -> str:
    """Compute a stable fingerprint for a schema inference result.

    The fingerprint includes schema structure and source table identity,
    making it useful for cache invalidation and contract validation.

    Parameters
    ----------
    result : SchemaInferenceResult
        Schema inference result to fingerprint.

    Returns:
    -------
    str
        Stable hash of the inference result.
    """
    from datafusion_engine.identity import schema_identity_hash

    schema_hash = schema_identity_hash(result.output_schema)
    source_tables_hash = ",".join(sorted(result.source_tables))
    return f"{schema_hash}:{source_tables_hash}"


def validate_inferred_schema(
    result: SchemaInferenceResult,
    expected_schema: pa.Schema,
    *,
    strict: bool = True,
) -> list[str]:
    """Validate inferred schema against an expected schema.

    Parameters
    ----------
    result : SchemaInferenceResult
        Schema inference result to validate.
    expected_schema : pa.Schema
        Expected schema to compare against.
    strict : bool
        If True, report extra columns as violations.

    Returns:
    -------
    list[str]
        List of violation messages (empty if valid).
    """
    violations: list[str] = []
    inferred_fields = {field.name: field for field in result.output_schema}
    expected_fields = {field.name: field for field in expected_schema}
    for name, expected_field in expected_fields.items():
        if name not in inferred_fields:
            violations.append(f"Missing column: {name}")
            continue
        inferred_field = inferred_fields[name]
        if inferred_field.type != expected_field.type:
            violations.append(
                f"Type mismatch for {name}: "
                f"expected {expected_field.type}, got {inferred_field.type}"
            )
        if expected_field.nullable is False and inferred_field.nullable is True:
            violations.append(f"Nullability mismatch for {name}: expected non-nullable")
    if strict:
        violations.extend(
            f"Extra column: {name}" for name in inferred_fields if name not in expected_fields
        )
    return violations


__all__ = [
    "ColumnLineage",
    "SchemaInferenceResult",
    "infer_column_lineage",
    "infer_schema_from_dataframe",
    "infer_schema_from_logical_plan",
    "infer_schema_from_plan_bundle",
    "infer_schema_with_lineage",
    "schema_fingerprint_from_inference",
    "validate_inferred_schema",
]
