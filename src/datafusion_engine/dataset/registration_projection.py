"""Projection-focused helpers for dataset registration."""

from __future__ import annotations

import time
from collections.abc import Mapping, Sequence
from contextlib import suppress
from typing import TYPE_CHECKING

import msgspec
import pyarrow as pa
from datafusion import SessionContext, SQLOptions, col
from datafusion.dataframe import DataFrame
from datafusion.expr import Expr

from datafusion_engine.dataset.registry import DatasetLocationOverrides
from datafusion_engine.io.adapter import DataFusionIOAdapter

if TYPE_CHECKING:
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from semantics.program_manifest import ManifestDatasetResolver


def _apply_projection_exprs(
    ctx: SessionContext,
    *,
    table_name: str,
    projection_exprs: Sequence[str],
    sql_options: SQLOptions,
    runtime_profile: DataFusionRuntimeProfile | None,
) -> DataFrame:
    if not projection_exprs:
        return ctx.table(table_name)
    _ = sql_options
    df = ctx.table(table_name)
    resolved_exprs = _resolve_projection_exprs(df, projection_exprs)
    if not resolved_exprs:
        msg = "Projection expressions are required for dynamic projection."
        raise ValueError(msg)
    projected = df.select(*resolved_exprs)
    adapter = DataFusionIOAdapter(ctx=ctx, profile=runtime_profile)
    adapter.register_view(table_name, projected, overwrite=True, temporary=False)
    if runtime_profile is not None:
        from serde_artifact_specs import PROJECTION_VIEW_ARTIFACT_SKIPPED_SPEC

        runtime_profile.record_artifact(
            PROJECTION_VIEW_ARTIFACT_SKIPPED_SPEC,
            {
                "event_time_unix_ms": int(time.time() * 1000),
                "table_name": table_name,
                "reason": "disabled_due_to_datafusion_panics",
                "projection_expr_count": len(projection_exprs),
            },
        )
    return ctx.table(table_name)


def _resolve_projection_exprs(
    df: DataFrame,
    projection_exprs: Sequence[str],
) -> list[Expr | str]:
    schema_fields = list(df.schema().names)
    schema_names = set(schema_fields)
    resolved_exprs: list[Expr | str] = []
    expanded_star = False
    for expr_text in projection_exprs:
        if expr_text.strip() == "*":
            if not expanded_star:
                resolved_exprs.extend(col(name) for name in schema_fields)
                expanded_star = True
            continue
        if expr_text in schema_names:
            resolved_exprs.append(col(expr_text))
            continue
        try:
            resolved_exprs.append(df.parse_sql_expr(expr_text))
        except (RuntimeError, TypeError, ValueError) as exc:
            msg = f"Projection expression parse failed: {expr_text!r} ({exc})"
            raise ValueError(msg) from exc
    return resolved_exprs


def _projection_exprs_for_schema(
    *,
    actual_columns: Sequence[str],
    expected_schema: pa.Schema,
) -> tuple[str, ...]:
    from datafusion_engine.dataset.registration_core import expected_column_defaults, sql_type_name

    actual = set(actual_columns)
    defaults = expected_column_defaults(expected_schema)
    projection_exprs: list[str] = []
    for schema_field in expected_schema:
        if schema_field.name in actual:
            if _supports_projection_cast(schema_field.type):
                dtype_name = sql_type_name(schema_field.type)
                cast_expr = f"cast({schema_field.name} as {dtype_name})"
                default_value = defaults.get(schema_field.name)
                if default_value is not None:
                    literal = _sql_literal_for_field(default_value, dtype=schema_field.type)
                    if literal is not None:
                        cast_expr = f"coalesce({cast_expr}, {literal})"
                projection_exprs.append(f"{cast_expr} as {schema_field.name}")
            else:
                projection_exprs.append(schema_field.name)
        elif _supports_projection_cast(schema_field.type):
            dtype_name = sql_type_name(schema_field.type)
            projection_exprs.append(f"cast(NULL as {dtype_name}) as {schema_field.name}")
        else:
            projection_exprs.append(f"NULL as {schema_field.name}")
    return tuple(projection_exprs)


def _supports_projection_cast(dtype: pa.DataType) -> bool:
    return not (
        pa.types.is_list(dtype)
        or pa.types.is_large_list(dtype)
        or pa.types.is_fixed_size_list(dtype)
        or pa.types.is_struct(dtype)
        or pa.types.is_map(dtype)
        or pa.types.is_union(dtype)
        or pa.types.is_dictionary(dtype)
        or pa.types.is_binary(dtype)
        or pa.types.is_large_binary(dtype)
        or pa.types.is_fixed_size_binary(dtype)
    )


def _sql_literal_for_field(value: str, *, dtype: pa.DataType) -> str | None:
    normalized = value.strip()
    if not normalized:
        return None
    if pa.types.is_boolean(dtype):
        return _sql_bool_literal(normalized)
    if pa.types.is_integer(dtype):
        return _sql_int_literal(normalized)
    if pa.types.is_floating(dtype):
        return _sql_float_literal(normalized)
    escaped = normalized.replace("'", "''")
    return f"'{escaped}'"


def _sql_bool_literal(value: str) -> str | None:
    lowered = value.lower()
    if lowered in {"true", "false"}:
        return lowered
    return None


def _sql_int_literal(value: str) -> str | None:
    try:
        return str(int(value))
    except ValueError:
        return None


def _sql_float_literal(value: str) -> str | None:
    try:
        return str(float(value))
    except ValueError:
        return None


def apply_projection_overrides(
    ctx: SessionContext,
    *,
    projection_map: Mapping[str, Sequence[str]],
    sql_options: SQLOptions,
    runtime_profile: DataFusionRuntimeProfile | None = None,
) -> None:
    """Apply projection overrides for table names when available."""
    if not projection_map:
        return
    for table_name, columns in projection_map.items():
        if not columns:
            continue
        projection_exprs = [str(name) for name in columns if str(name)]
        try:
            _apply_projection_exprs(
                ctx,
                table_name=table_name,
                projection_exprs=projection_exprs,
                sql_options=sql_options,
                runtime_profile=runtime_profile,
            )
        except (KeyError, RuntimeError, TypeError, ValueError):
            continue


def apply_projection_scan_overrides(
    ctx: SessionContext,
    *,
    projection_map: Mapping[str, Sequence[str]],
    runtime_profile: DataFusionRuntimeProfile | None,
    dataset_resolver: ManifestDatasetResolver | None = None,
) -> None:
    """Re-register datasets with projection scan overrides when possible."""
    if runtime_profile is None:
        return
    if dataset_resolver is None:
        return
    for table_name, columns in projection_map.items():
        if not columns:
            continue
        location = dataset_resolver.location(table_name)
        if location is None:
            continue
        scan = location.resolved.datafusion_scan
        if scan is None:
            continue
        projection_exprs = tuple(str(name) for name in columns if str(name))
        if scan.projection_exprs == projection_exprs:
            continue
        updated_scan = msgspec.structs.replace(scan, projection_exprs=projection_exprs)
        overrides = location.overrides
        if overrides is None:
            overrides = DatasetLocationOverrides(datafusion_scan=updated_scan)
        else:
            overrides = msgspec.structs.replace(overrides, datafusion_scan=updated_scan)
        updated_location = msgspec.structs.replace(location, overrides=overrides)
        adapter = DataFusionIOAdapter(ctx=ctx, profile=runtime_profile)
        with suppress(KeyError, RuntimeError, TypeError, ValueError):
            adapter.deregister_table(table_name)
        try:
            from datafusion_engine.session.facade import DataFusionExecutionFacade

            facade = DataFusionExecutionFacade(ctx=ctx, runtime_profile=runtime_profile)
            facade.register_dataset(name=table_name, location=updated_location)
        except (RuntimeError, TypeError, ValueError):
            continue


__all__ = [
    "apply_projection_overrides",
    "apply_projection_scan_overrides",
]
