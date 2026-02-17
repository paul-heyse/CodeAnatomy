"""Information-schema and function-registry capture helpers for plan artifacts."""

from __future__ import annotations

import contextlib
from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING

from datafusion import SessionContext

from datafusion_engine.schema.introspection_core import SchemaIntrospector
from utils.hashing import hash_msgpack_canonical

if TYPE_CHECKING:
    from datafusion_engine.session.runtime_session import SessionRuntime
    from datafusion_engine.sql.options import SQLOptions


def information_schema_sql_options(
    session_runtime: SessionRuntime,
) -> SQLOptions | None:
    """Resolve SQL options used for information_schema capture.

    Returns:
        SQLOptions | None: Planning SQL options when available.
    """
    try:
        from datafusion_engine.sql.options import planning_sql_options

        return planning_sql_options(session_runtime.profile)
    except (RuntimeError, TypeError, ValueError, ImportError):
        return None


def build_introspector(
    ctx: SessionContext,
    *,
    sql_options: SQLOptions | None,
) -> SchemaIntrospector | None:
    """Build a schema introspector and fail-open on runtime errors.

    Returns:
        SchemaIntrospector | None: Introspector instance, or ``None`` on failure.
    """
    try:
        return SchemaIntrospector(ctx, sql_options=sql_options)
    except (RuntimeError, TypeError, ValueError):
        return None


def table_definitions_snapshot(
    introspector: SchemaIntrospector,
    *,
    tables: Sequence[Mapping[str, object]],
) -> dict[str, str]:
    """Collect table-definition SQL text keyed by table name.

    Returns:
        dict[str, str]: Table definition SQL by table name.
    """
    table_definitions: dict[str, str] = {}
    for row in tables:
        name = row.get("table_name")
        if name is None:
            continue
        definition = introspector.table_definition(str(name))
        if definition:
            table_definitions[str(name)] = definition
    return table_definitions


def safe_introspection_rows(
    fetch: Callable[[], Sequence[Mapping[str, object]]],
) -> list[Mapping[str, object]]:
    """Run an introspection query and return rows or an empty payload.

    Returns:
        list[Mapping[str, object]]: Materialized introspection rows.
    """
    try:
        return list(fetch())
    except (RuntimeError, TypeError, ValueError, Warning):
        return []


def routine_metadata_snapshot(
    introspector: SchemaIntrospector,
    *,
    capture_udf_metadata: bool,
) -> tuple[list[Mapping[str, object]], list[Mapping[str, object]], list[Mapping[str, object]]]:
    """Collect routines/parameters/catalog snapshots when UDF metadata is enabled.

    Returns:
        tuple[list[Mapping[str, object]], list[Mapping[str, object]], list[Mapping[str, object]]]:
            Routines, parameters, and function-catalog rows.
    """
    if not capture_udf_metadata:
        return [], [], []
    routines = safe_introspection_rows(introspector.routines_snapshot)
    parameters = safe_introspection_rows(introspector.parameters_snapshot)
    function_catalog = safe_introspection_rows(
        lambda: introspector.function_catalog_snapshot(include_parameters=True)
    )
    return routines, parameters, function_catalog


def settings_rows_to_mapping(rows: Sequence[Mapping[str, object]]) -> dict[str, str]:
    """Normalize settings snapshot rows to string mappings.

    Returns:
        dict[str, str]: Stable string mapping for settings rows.
    """
    mapping: dict[str, str] = {}
    for row in rows:
        name = row.get("name") or row.get("setting_name") or row.get("key")
        if name is None:
            continue
        value = row.get("value")
        mapping[str(name)] = "" if value is None else str(value)
    return mapping


def df_settings_snapshot(
    ctx: SessionContext,
    *,
    session_runtime: SessionRuntime | None,
) -> Mapping[str, str]:
    """Capture stable DataFusion settings as a string mapping.

    Returns:
        Mapping[str, str]: Stable DataFusion settings payload.
    """
    if session_runtime is not None and session_runtime.ctx is ctx:
        return dict(session_runtime.df_settings)

    try:
        sql_options = None
        if session_runtime is not None:
            sql_options = information_schema_sql_options(session_runtime)
        introspector = SchemaIntrospector(ctx, sql_options=sql_options)
        rows = introspector.settings_snapshot()
        if not rows:
            return {}
        return settings_rows_to_mapping(rows)
    except (RuntimeError, TypeError, ValueError):
        return {}


def capture_udf_metadata_for_plan(session_runtime: SessionRuntime | None) -> bool:
    """Return whether UDF metadata capture is enabled for this runtime."""
    if session_runtime is None:
        return False
    profile = session_runtime.profile
    return profile.catalog.enable_information_schema and profile.features.enable_udfs


def information_schema_snapshot(
    ctx: SessionContext,
    *,
    session_runtime: SessionRuntime | None,
) -> Mapping[str, object]:
    """Capture information_schema payload used by plan artifact fingerprints.

    Returns:
        Mapping[str, object]: Information-schema snapshot payload.
    """
    if session_runtime is None:
        return {}
    sql_options = information_schema_sql_options(session_runtime)
    introspector = build_introspector(ctx, sql_options=sql_options)
    if introspector is None:
        return {}
    tables = introspector.tables_snapshot()
    table_definitions = table_definitions_snapshot(introspector, tables=tables)
    capture_udf = capture_udf_metadata_for_plan(session_runtime)
    routines, parameters, function_catalog = routine_metadata_snapshot(
        introspector,
        capture_udf_metadata=capture_udf,
    )
    return {
        "df_settings": df_settings_snapshot(ctx, session_runtime=session_runtime),
        "settings": introspector.settings_snapshot(),
        "tables": tables,
        "schemata": introspector.schemata_snapshot(),
        "columns": introspector.columns_snapshot(),
        "routines": routines,
        "parameters": parameters,
        "function_catalog": function_catalog,
        "table_definitions": table_definitions,
    }


def canonical_sort_key(value: object) -> str:
    """Build a stable sort key for normalized snapshot values.

    Returns:
        str: Stable hash key for deterministic ordering.
    """
    return hash_msgpack_canonical(value)


def canonicalize_snapshot(value: object) -> object:
    """Canonicalize mappings/sequences for stable hashing.

    Returns:
        object: Canonically ordered snapshot structure.
    """
    if isinstance(value, Mapping):
        return {key: canonicalize_snapshot(val) for key, val in sorted(value.items())}
    if isinstance(value, Sequence) and not isinstance(
        value,
        (str, bytes, bytearray, memoryview),
    ):
        normalized = [canonicalize_snapshot(item) for item in value]
        return sorted(normalized, key=canonical_sort_key)
    return value


def information_schema_hash(snapshot: Mapping[str, object]) -> str:
    """Return a stable hash for information_schema snapshots."""
    canonical = canonicalize_snapshot(snapshot)
    return hash_msgpack_canonical(canonical)


def function_registry_hash(snapshot: Mapping[str, object]) -> str:
    """Return a stable hash for function catalog payloads."""
    canonical = canonicalize_snapshot(snapshot)
    return hash_msgpack_canonical(canonical)


def function_registry_hash_for_context(
    ctx: SessionContext,
    *,
    session_runtime: SessionRuntime | None,
) -> str:
    """Capture function catalog payload and return its stable hash.

    Returns:
        str: Deterministic hash for the current function-registry snapshot.
    """
    if not capture_udf_metadata_for_plan(session_runtime):
        return function_registry_hash({"functions": []})

    functions: Sequence[Mapping[str, object]] = ()
    try:
        sql_options = None
        if session_runtime is not None:
            sql_options = information_schema_sql_options(session_runtime)
        introspector = SchemaIntrospector(ctx, sql_options=sql_options)
        functions = introspector.function_catalog_snapshot(include_parameters=True)
    except (RuntimeError, TypeError, ValueError, Warning):
        functions = ()
    snapshot: Mapping[str, object] = {"functions": list(functions)}
    return function_registry_hash(snapshot)


def suppress_introspection_errors() -> contextlib.AbstractContextManager[None]:
    """Return an error-suppression context for introspection probes."""
    return contextlib.suppress(RuntimeError, TypeError, ValueError)


__all__ = [
    "build_introspector",
    "canonicalize_snapshot",
    "capture_udf_metadata_for_plan",
    "df_settings_snapshot",
    "function_registry_hash",
    "function_registry_hash_for_context",
    "information_schema_hash",
    "information_schema_snapshot",
    "information_schema_sql_options",
    "routine_metadata_snapshot",
    "safe_introspection_rows",
    "settings_rows_to_mapping",
    "suppress_introspection_errors",
    "table_definitions_snapshot",
]
