"""Environment and catalog snapshot helpers for plan bundles."""

from __future__ import annotations

import contextlib
from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING

from datafusion import SessionContext, SQLOptions

from datafusion_engine.schema.introspection_core import SchemaIntrospector
from utils.hashing import hash_msgpack_canonical

if TYPE_CHECKING:
    from datafusion_engine.session.runtime_session import SessionRuntime


def information_schema_sql_options(
    session_runtime: SessionRuntime,
) -> SQLOptions | None:
    """Resolve planning SQL options for information-schema introspection.

    Returns:
        SQLOptions | None: SQL options for planning-mode introspection, if available.
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
    """Build a schema introspector for the session context.

    Returns:
        SchemaIntrospector | None: Introspector instance, or `None` when initialization fails.
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
    """Return table-name to DDL-definition snapshot."""
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
    """Fetch snapshot rows while converting lookup failures to empty results.

    Returns:
        list[Mapping[str, object]]: Snapshot rows, or an empty list on lookup failure.
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
    """Capture routine/parameter/function catalog metadata when enabled.

    Returns:
        tuple[list[Mapping[str, object]], list[Mapping[str, object]], list[Mapping[str, object]]]:
            Routines, parameters, and function-catalog snapshots.
    """
    if not capture_udf_metadata:
        return [], [], []
    routines = safe_introspection_rows(introspector.routines_snapshot)
    parameters = safe_introspection_rows(introspector.parameters_snapshot)
    function_catalog = safe_introspection_rows(
        lambda: introspector.function_catalog_snapshot(include_parameters=True)
    )
    return routines, parameters, function_catalog


def capture_udf_metadata_for_plan(session_runtime: SessionRuntime | None) -> bool:
    """Return whether UDF metadata should be included in bundle snapshots."""
    if session_runtime is None:
        return False
    profile = session_runtime.profile
    return profile.catalog.enable_information_schema and profile.features.enable_udfs


def information_schema_snapshot(
    ctx: SessionContext,
    *,
    session_runtime: SessionRuntime | None,
) -> Mapping[str, object]:
    """Return a full information_schema snapshot for plan artifacts."""
    if session_runtime is None:
        return {}
    sql_options = information_schema_sql_options(session_runtime)
    introspector = build_introspector(ctx, sql_options=sql_options)
    if introspector is None:
        return {}
    tables = introspector.tables_snapshot()
    table_definitions = table_definitions_snapshot(introspector, tables=tables)
    capture_udf_metadata = capture_udf_metadata_for_plan(session_runtime)
    routines, parameters, function_catalog = routine_metadata_snapshot(
        introspector,
        capture_udf_metadata=capture_udf_metadata,
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


def information_schema_hash(snapshot: Mapping[str, object]) -> str:
    """Return a stable hash for an information_schema snapshot."""
    canonical = canonicalize_snapshot(snapshot)
    return hash_msgpack_canonical(canonical)


def canonical_sort_key(value: object) -> str:
    """Return a deterministic ordering key for canonicalized values."""
    return hash_msgpack_canonical(value)


def canonicalize_snapshot(value: object) -> object:
    """Sort nested mapping/sequence payloads into canonical order.

    Returns:
        object: Canonicalized payload with deterministic nested ordering.
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


def settings_rows_to_mapping(rows: Sequence[Mapping[str, object]]) -> dict[str, str]:
    """Normalize settings snapshot rows to a flat name/value mapping.

    Returns:
        dict[str, str]: Flat settings mapping keyed by canonical setting name.
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
    """Capture DataFusion settings from runtime cache or introspector fallback.

    Returns:
        Mapping[str, str]: Resolved DataFusion settings snapshot.
    """
    if session_runtime is not None and session_runtime.ctx is ctx:
        return dict(session_runtime.df_settings)

    try:
        sql_options = None
        if session_runtime is not None:
            try:
                from datafusion_engine.sql.options import planning_sql_options

                sql_options = planning_sql_options(session_runtime.profile)
            except (RuntimeError, TypeError, ValueError, ImportError):
                sql_options = None
        introspector = SchemaIntrospector(ctx, sql_options=sql_options)
        rows = introspector.settings_snapshot()
        if not rows:
            return {}
        return settings_rows_to_mapping(rows)
    except (RuntimeError, TypeError, ValueError):
        return {}


def function_registry_hash(snapshot: Mapping[str, object]) -> str:
    """Return a stable hash for a function-catalog snapshot payload."""
    canonical = canonicalize_snapshot(snapshot)
    return hash_msgpack_canonical(canonical)


def function_registry_hash_for_context(
    ctx: SessionContext,
    *,
    session_runtime: SessionRuntime | None,
) -> str:
    """Capture function-catalog payload and return its stable hash.

    Returns:
    -------
    str
        Stable hash value for the captured function catalog payload.
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
