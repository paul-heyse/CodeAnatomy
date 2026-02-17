"""Routine/parameter introspection helpers for DataFusion information_schema."""

from __future__ import annotations

import contextlib
import importlib
from collections.abc import Iterable, Mapping, Sequence
from typing import TYPE_CHECKING

import pyarrow as pa
from datafusion import SessionContext, SQLOptions

from datafusion_engine.schema.introspection_common import read_only_sql_options

if TYPE_CHECKING:
    from datafusion_engine.catalog.introspection import IntrospectionCache


def _introspection_cache_for_ctx(
    ctx: SessionContext,
    *,
    sql_options: SQLOptions | None,
) -> IntrospectionCache:
    from datafusion_engine.catalog.introspection import introspection_cache_for_ctx

    return introspection_cache_for_ctx(ctx, sql_options=sql_options)


def _empty_routines_table() -> pa.Table:
    return pa.Table.from_arrays(
        [
            pa.array([], type=pa.string()),
            pa.array([], type=pa.string()),
            pa.array([], type=pa.string()),
            pa.array([], type=pa.string()),
            pa.array([], type=pa.string()),
            pa.array([], type=pa.string()),
            pa.array([], type=pa.string()),
            pa.array([], type=pa.string()),
        ],
        names=[
            "specific_catalog",
            "specific_schema",
            "specific_name",
            "routine_catalog",
            "routine_schema",
            "routine_name",
            "routine_type",
            "data_type",
        ],
    )


def _empty_parameters_table() -> pa.Table:
    return pa.Table.from_arrays(
        [
            pa.array([], type=pa.string()),
            pa.array([], type=pa.string()),
            pa.array([], type=pa.string()),
            pa.array([], type=pa.string()),
            pa.array([], type=pa.string()),
            pa.array([], type=pa.string()),
            pa.array([], type=pa.int32()),
            pa.array([], type=pa.string()),
            pa.array([], type=pa.string()),
            pa.array([], type=pa.string()),
        ],
        names=[
            "specific_catalog",
            "specific_schema",
            "specific_name",
            "routine_catalog",
            "routine_schema",
            "routine_name",
            "ordinal_position",
            "parameter_name",
            "data_type",
            "parameter_mode",
        ],
    )


def _registry_snapshot(ctx: SessionContext) -> Mapping[str, object] | None:
    try:
        from datafusion_engine.udf.extension_core import rust_udf_snapshot
    except ImportError:
        return None
    try:
        snapshot = rust_udf_snapshot(ctx)
    except (RuntimeError, TypeError, ValueError):
        return None
    if not isinstance(snapshot, Mapping):
        return None
    return snapshot


def _registry_names(snapshot: Mapping[str, object], key: str) -> list[str]:
    value = snapshot.get(key, [])
    if isinstance(value, str):
        return []
    if isinstance(value, Iterable):
        return [str(name) for name in value if name is not None]
    return []


def _routine_name_from_row(row: Mapping[str, object]) -> str | None:
    for key in ("routine_name", "function_name", "name"):
        value = row.get(key)
        if isinstance(value, str) and value:
            return value
    return None


def _normalized_name_map(value: object) -> Mapping[str, Sequence[str]]:
    if isinstance(value, Mapping):
        resolved: dict[str, Sequence[str]] = {}
        for key, names in value.items():
            if names is None or isinstance(names, str):
                resolved[str(key)] = ()
            elif isinstance(names, Iterable):
                resolved[str(key)] = tuple(str(name) for name in names if name is not None)
            else:
                resolved[str(key)] = ()
        return resolved
    return {}


def _deterministic_flag(volatility: str | None) -> bool | None:
    if volatility is None:
        return None
    lowered = volatility.lower()
    if lowered in {"immutable", "stable"}:
        return True
    if lowered == "volatile":
        return False
    return None


def _routine_row(
    *,
    name: str,
    routine_type: str,
    specific_name: str,
    volatility: str | None,
    schema_names: Sequence[str],
) -> dict[str, object]:
    row: dict[str, object] = {
        "specific_catalog": "datafusion",
        "specific_schema": "public",
        "specific_name": specific_name,
        "routine_catalog": "datafusion",
        "routine_schema": "public",
        "routine_name": name,
        "routine_type": routine_type,
        "data_type": None,
    }
    if "function_name" in schema_names:
        row["function_name"] = name
    if "function_type" in schema_names:
        row["function_type"] = routine_type
    if "volatility" in schema_names:
        row["volatility"] = volatility
    if "is_deterministic" in schema_names:
        row["is_deterministic"] = _deterministic_flag(volatility)
    return row


def _parameter_row(
    *,
    name: str,
    specific_name: str,
    ordinal: int,
    param_name: str,
    schema_names: Sequence[str],
) -> dict[str, object]:
    row: dict[str, object] = {
        "specific_catalog": "datafusion",
        "specific_schema": "public",
        "specific_name": specific_name,
        "routine_catalog": "datafusion",
        "routine_schema": "public",
        "routine_name": name,
        "ordinal_position": ordinal,
        "parameter_name": param_name,
        "data_type": "unknown",
        "parameter_mode": "IN",
    }
    if "function_name" in schema_names:
        row["function_name"] = name
    return row


def _aligned_rows(
    rows: Sequence[Mapping[str, object]],
    schema_names: Sequence[str],
) -> list[dict[str, object]]:
    return [{name: row.get(name) for name in schema_names} for row in rows]


def _merge_registry_routines(ctx: SessionContext, base: pa.Table) -> pa.Table:
    _ = ctx
    return base


def _merge_registry_parameters(ctx: SessionContext, base: pa.Table | None) -> pa.Table:
    _ = ctx
    return base or _empty_parameters_table()


def _information_schema_table(
    ctx: SessionContext,
    *,
    name: str,
    sql_options: SQLOptions | None,
) -> pa.Table | None:
    panic_exception_type: type[Exception] = RuntimeError
    with contextlib.suppress(ImportError):
        module = importlib.import_module("pyo3_runtime")
        candidate = getattr(module, "PanicException", RuntimeError)
        if isinstance(candidate, type) and issubclass(candidate, Exception):
            panic_exception_type = candidate
    query = f"select * from information_schema.{name}"
    resolved_options = sql_options or read_only_sql_options()
    try:
        df = ctx.sql_with_options(query, resolved_options)
        return df.to_arrow_table()
    except (RuntimeError, TypeError, ValueError, AttributeError, panic_exception_type):
        return None


def routines_snapshot_table(
    ctx: SessionContext,
    *,
    sql_options: SQLOptions | None = None,
) -> pa.Table:
    """Return information_schema.routines as a pyarrow.Table."""
    snapshot = _introspection_cache_for_ctx(ctx, sql_options=sql_options).snapshot
    base = snapshot.routines if snapshot.routines is not None else _empty_routines_table()
    if base.num_rows == 0:
        fallback = _information_schema_table(ctx, name="routines", sql_options=sql_options)
        if fallback is not None:
            base = fallback
    return _merge_registry_routines(ctx, base)


def parameters_snapshot_table(
    ctx: SessionContext,
    *,
    sql_options: SQLOptions | None = None,
) -> pa.Table | None:
    """Return information_schema.parameters as a pyarrow.Table when available."""
    snapshot = _introspection_cache_for_ctx(ctx, sql_options=sql_options).snapshot
    base = getattr(snapshot, "parameters", None)
    if base is None or base.num_rows == 0:
        fallback = _information_schema_table(ctx, name="parameters", sql_options=sql_options)
        if fallback is not None:
            base = fallback
    merged = _merge_registry_parameters(ctx, base)
    if base is None and merged.num_rows == 0:
        return None
    return merged


__all__ = [
    "parameters_snapshot_table",
    "routines_snapshot_table",
]
