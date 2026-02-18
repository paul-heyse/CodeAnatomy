"""Schema registry installation, dataset registration, and diagnostics for runtime profiles."""

from __future__ import annotations

import contextlib
import logging
import time
from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING

import datafusion
import pyarrow as pa
from datafusion import SessionContext

from datafusion_engine.dataset.registry import DatasetLocation
from datafusion_engine.identity import schema_identity_hash
from datafusion_engine.schema import (
    AST_CORE_VIEW_NAMES,
    AST_OPTIONAL_VIEW_NAMES,
    CST_VIEW_NAMES,
    TREE_SITTER_CHECK_VIEWS,
    TREE_SITTER_VIEW_NAMES,
    extract_nested_dataset_names,
    extract_schema_for,
    missing_schema_names,
    relationship_schema_for,
    relationship_schema_names,
    validate_nested_types,
    validate_required_engine_functions,
    validate_semantic_types,
    validate_udf_info_schema_parity,
)
from datafusion_engine.schema.introspection_core import (
    catalogs_snapshot,
    constraint_rows,
)
from datafusion_engine.session._session_constants import create_schema_introspector
from datafusion_engine.session.runtime_config_policies import (
    _effective_catalog_autoload_for_profile,
)
from datafusion_engine.session.runtime_profile_config import (
    _AST_OPTIONAL_VIEW_FUNCTIONS,
    PreparedStatementSpec,
    ZeroRowBootstrapConfig,
)
from datafusion_engine.session.runtime_session import (
    _build_session_runtime_from_context,
    _datafusion_function_names,
    _datafusion_version,
    _ScipRegistrationSnapshot,
    _sql_with_options,
    function_catalog_snapshot_for_profile,
)
from datafusion_engine.session.runtime_telemetry import _default_value_entries
from datafusion_engine.session.runtime_udf import (
    SchemaRegistryValidationResult,
    _collect_view_sql_parse_errors,
    _constraint_drift_entries,
    _prepare_statement_sql,
    _register_schema_table,
    _relationship_constraint_errors,
    _table_dfschema_tree,
    _table_logical_plan,
)
from datafusion_engine.sql.options import (
    sql_options_for_profile,
    statement_sql_options_for_profile,
)
from utils.validation import find_missing

if TYPE_CHECKING:
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from serde_schema_registry import ArtifactSpec

logger = logging.getLogger(__name__)
_DIAGNOSTICS_CAPTURE_ERRORS = (
    AttributeError,
    KeyError,
    RuntimeError,
    TypeError,
    ValueError,
    Exception,
)


def _resolved_table_schema(ctx: SessionContext, name: str) -> pa.Schema | None:
    """Resolve table schema from session context.

    Args:
        ctx: Session context.
        name: Table name.

    Returns:
        PyArrow schema or None.
    """
    try:
        schema = ctx.table(name).schema()
    except (KeyError, RuntimeError, TypeError, ValueError):
        return None
    if isinstance(schema, pa.Schema):
        return schema
    to_arrow = getattr(schema, "to_arrow", None)
    if callable(to_arrow):
        resolved = to_arrow()
        if isinstance(resolved, pa.Schema):
            return resolved
    return None


def _safe_table_pylist(
    ctx: SessionContext, name: str
) -> tuple[list[dict[str, object]] | None, str | None]:
    """Safely read a session table into Python rows for diagnostics payloads.

    Returns:
        tuple[list[dict[str, object]] | None, str | None]:
            Rows when materialization succeeds, otherwise an error message.
    """
    if not ctx.table_exist(name):
        return None, f"table '{name}' is unavailable"
    try:
        table = ctx.table(name).to_arrow_table()
        return list(table.to_pylist()), None
    except _DIAGNOSTICS_CAPTURE_ERRORS as exc:
        return None, str(exc)


def _get_datafusion_schema_registry_validation_spec() -> ArtifactSpec:
    """Return DATAFUSION_SCHEMA_REGISTRY_VALIDATION_SPEC (deferred import).

    Returns:
    -------
    object
        The artifact spec constant.
    """
    from serde_artifact_specs import DATAFUSION_SCHEMA_REGISTRY_VALIDATION_SPEC

    return DATAFUSION_SCHEMA_REGISTRY_VALIDATION_SPEC


def _get_datafusion_catalog_autoload_spec() -> ArtifactSpec:
    """Return DATAFUSION_CATALOG_AUTOLOAD_SPEC (deferred import).

    Returns:
    -------
    object
        The artifact spec constant.
    """
    from serde_artifact_specs import DATAFUSION_CATALOG_AUTOLOAD_SPEC

    return DATAFUSION_CATALOG_AUTOLOAD_SPEC


def _get_datafusion_ast_feature_gates_spec() -> ArtifactSpec:
    """Return DATAFUSION_AST_FEATURE_GATES_SPEC (deferred import).

    Returns:
    -------
    object
        The artifact spec constant.
    """
    from serde_artifact_specs import DATAFUSION_AST_FEATURE_GATES_SPEC

    return DATAFUSION_AST_FEATURE_GATES_SPEC


def _get_datafusion_ast_span_metadata_spec() -> ArtifactSpec:
    """Return DATAFUSION_AST_SPAN_METADATA_SPEC (deferred import).

    Returns:
    -------
    object
        The artifact spec constant.
    """
    from serde_artifact_specs import DATAFUSION_AST_SPAN_METADATA_SPEC

    return DATAFUSION_AST_SPAN_METADATA_SPEC


def _get_datafusion_ast_dataset_spec() -> ArtifactSpec:
    """Return DATAFUSION_AST_DATASET_SPEC (deferred import).

    Returns:
    -------
    object
        The artifact spec constant.
    """
    from serde_artifact_specs import DATAFUSION_AST_DATASET_SPEC

    return DATAFUSION_AST_DATASET_SPEC


def _get_datafusion_bytecode_dataset_spec() -> ArtifactSpec:
    """Return DATAFUSION_BYTECODE_DATASET_SPEC (deferred import).

    Returns:
    -------
    object
        The artifact spec constant.
    """
    from serde_artifact_specs import DATAFUSION_BYTECODE_DATASET_SPEC

    return DATAFUSION_BYTECODE_DATASET_SPEC


def _get_datafusion_scip_datasets_spec() -> ArtifactSpec:
    """Return DATAFUSION_SCIP_DATASETS_SPEC (deferred import).

    Returns:
    -------
    object
        The artifact spec constant.
    """
    from serde_artifact_specs import DATAFUSION_SCIP_DATASETS_SPEC

    return DATAFUSION_SCIP_DATASETS_SPEC


def _get_datafusion_cst_schema_diagnostics_spec() -> ArtifactSpec:
    """Return DATAFUSION_CST_SCHEMA_DIAGNOSTICS_SPEC (deferred import).

    Returns:
    -------
    object
        The artifact spec constant.
    """
    from serde_artifact_specs import DATAFUSION_CST_SCHEMA_DIAGNOSTICS_SPEC

    return DATAFUSION_CST_SCHEMA_DIAGNOSTICS_SPEC


def _get_datafusion_tree_sitter_stats_spec() -> ArtifactSpec:
    """Return DATAFUSION_TREE_SITTER_STATS_SPEC (deferred import).

    Returns:
    -------
    object
        The artifact spec constant.
    """
    from serde_artifact_specs import DATAFUSION_TREE_SITTER_STATS_SPEC

    return DATAFUSION_TREE_SITTER_STATS_SPEC


def _get_datafusion_tree_sitter_plan_schema_spec() -> ArtifactSpec:
    """Return DATAFUSION_TREE_SITTER_PLAN_SCHEMA_SPEC (deferred import).

    Returns:
    -------
    object
        The artifact spec constant.
    """
    from serde_artifact_specs import DATAFUSION_TREE_SITTER_PLAN_SCHEMA_SPEC

    return DATAFUSION_TREE_SITTER_PLAN_SCHEMA_SPEC


def _get_datafusion_tree_sitter_cross_checks_spec() -> ArtifactSpec:
    """Return DATAFUSION_TREE_SITTER_CROSS_CHECKS_SPEC (deferred import).

    Returns:
    -------
    object
        The artifact spec constant.
    """
    from serde_artifact_specs import DATAFUSION_TREE_SITTER_CROSS_CHECKS_SPEC

    return DATAFUSION_TREE_SITTER_CROSS_CHECKS_SPEC


def _get_datafusion_cst_view_plans_spec() -> ArtifactSpec:
    """Return DATAFUSION_CST_VIEW_PLANS_SPEC (deferred import).

    Returns:
    -------
    object
        The artifact spec constant.
    """
    from serde_artifact_specs import DATAFUSION_CST_VIEW_PLANS_SPEC

    return DATAFUSION_CST_VIEW_PLANS_SPEC


def _get_datafusion_cst_dfschema_spec() -> ArtifactSpec:
    """Return DATAFUSION_CST_DFSCHEMA_SPEC (deferred import).

    Returns:
    -------
    object
        The artifact spec constant.
    """
    from serde_artifact_specs import DATAFUSION_CST_DFSCHEMA_SPEC

    return DATAFUSION_CST_DFSCHEMA_SPEC


def _get_datafusion_bytecode_metadata_spec() -> ArtifactSpec:
    """Return DATAFUSION_BYTECODE_METADATA_SPEC (deferred import).

    Returns:
    -------
    object
        The artifact spec constant.
    """
    from serde_artifact_specs import DATAFUSION_BYTECODE_METADATA_SPEC

    return DATAFUSION_BYTECODE_METADATA_SPEC


def _get_datafusion_schema_introspection_spec() -> ArtifactSpec:
    """Return DATAFUSION_SCHEMA_INTROSPECTION_SPEC (deferred import).

    Returns:
    -------
    object
        The artifact spec constant.
    """
    from serde_artifact_specs import DATAFUSION_SCHEMA_INTROSPECTION_SPEC

    return DATAFUSION_SCHEMA_INTROSPECTION_SPEC


def _get_schema_registry_validation_advisory_spec() -> ArtifactSpec:
    """Return SCHEMA_REGISTRY_VALIDATION_ADVISORY_SPEC (deferred import).

    Returns:
    -------
    object
        The artifact spec constant.
    """
    from serde_artifact_specs import SCHEMA_REGISTRY_VALIDATION_ADVISORY_SPEC

    return SCHEMA_REGISTRY_VALIDATION_ADVISORY_SPEC


def _get_datafusion_prepared_statements_spec() -> ArtifactSpec:
    """Return DATAFUSION_PREPARED_STATEMENTS_SPEC (deferred import).

    Returns:
    -------
    object
        The artifact spec constant.
    """
    from serde_artifact_specs import DATAFUSION_PREPARED_STATEMENTS_SPEC

    return DATAFUSION_PREPARED_STATEMENTS_SPEC


def record_schema_snapshots_for_profile(profile: DataFusionRuntimeProfile) -> None:
    """Record information_schema snapshots to diagnostics when enabled.

    Args:
        profile: Runtime profile to record snapshots for.
    """
    if profile.diagnostics.diagnostics_sink is None:
        return
    if not profile.catalog.enable_information_schema:
        return
    ctx = profile.session_context()

    introspector = create_schema_introspector(profile, ctx)
    payload: dict[str, object] = {
        "event_time_unix_ms": int(time.time() * 1000),
    }
    try:
        payload.update(
            {
                "catalogs": catalogs_snapshot(introspector),
                "schemata": introspector.schemata_snapshot(),
                "tables": introspector.tables_snapshot(),
                "columns": introspector.columns_snapshot(),
                "constraints": constraint_rows(
                    ctx,
                    sql_options=sql_options_for_profile(profile),
                ),
                "routines": introspector.routines_snapshot(),
                "parameters": introspector.parameters_snapshot(),
                "settings": introspector.settings_snapshot(),
                "functions": function_catalog_snapshot_for_profile(
                    profile,
                    ctx,
                    include_routines=profile.catalog.enable_information_schema,
                ),
            }
        )
        version = _datafusion_version(ctx)
        if version is not None:
            payload["datafusion_version"] = version
    except (RuntimeError, TypeError, ValueError) as exc:
        payload["error"] = str(exc)
    profile.record_artifact(
        _get_datafusion_schema_introspection_spec(),
        payload,
    )


def _record_schema_registry_validation(
    profile: DataFusionRuntimeProfile,
    ctx: SessionContext,
    *,
    expected_names: Sequence[str] | None = None,
    expected_schemas: Mapping[str, pa.Schema] | None = None,
    view_errors: Mapping[str, str] | None = None,
    tree_sitter_checks: Mapping[str, object] | None = None,
) -> SchemaRegistryValidationResult:
    """Record schema registry validation results.

    Args:
        profile: Runtime profile.
        ctx: Session context.
        expected_names: Expected schema names.
        expected_schemas: Expected schemas by name.
        view_errors: View validation errors.
        tree_sitter_checks: Tree-sitter cross-check results.

    Returns:
        SchemaRegistryValidationResult: Validation result.
    """
    if not profile.catalog.enable_information_schema:
        return SchemaRegistryValidationResult()
    expected = tuple(sorted(set(expected_names or ())))
    missing = missing_schema_names(ctx, expected=expected) if expected else ()
    type_errors: dict[str, str] = {}
    introspector = create_schema_introspector(profile, ctx)
    constraint_drift = _constraint_drift_entries(
        introspector,
        names=expected,
        schemas=expected_schemas,
    )
    relationship_errors = _relationship_constraint_errors(
        _build_session_runtime_from_context(ctx, profile=profile),
        sql_options=sql_options_for_profile(profile),
    )
    result = SchemaRegistryValidationResult(
        missing=tuple(missing),
        type_errors=dict(type_errors),
        view_errors=dict(view_errors) if view_errors else {},
        constraint_drift=tuple(constraint_drift),
        relationship_constraint_errors=dict(relationship_errors) if relationship_errors else None,
    )
    if profile.diagnostics.diagnostics_sink is None:
        return result
    if (
        not result.missing
        and not result.type_errors
        and not result.view_errors
        and not result.constraint_drift
        and result.relationship_constraint_errors is None
    ):
        return result
    payload: dict[str, object] = {
        "event_time_unix_ms": int(time.time() * 1000),
        "missing": list(result.missing),
        "type_errors": dict(result.type_errors),
        "view_errors": dict(result.view_errors) if result.view_errors else None,
        "constraint_drift": list(result.constraint_drift) if result.constraint_drift else None,
        "relationship_constraint_errors": dict(result.relationship_constraint_errors)
        if result.relationship_constraint_errors
        else None,
    }
    if tree_sitter_checks is not None:
        import json

        payload["tree_sitter_checks"] = json.dumps(tree_sitter_checks, default=str)
    if result.view_errors and profile.view_registry is not None:
        parse_errors = _collect_view_sql_parse_errors(
            ctx,
            profile.view_registry,
            sql_options=sql_options_for_profile(profile),
        )
        if parse_errors:
            import json

            payload["sql_parse_errors"] = json.dumps(parse_errors, default=str)
    profile.record_artifact(
        _get_datafusion_schema_registry_validation_spec(),
        payload,
    )
    return result


def _record_catalog_autoload_snapshot(
    profile: DataFusionRuntimeProfile, ctx: SessionContext
) -> None:
    """Record catalog autoload snapshot.

    Args:
        profile: Runtime profile.
        ctx: Session context.
    """
    if profile.diagnostics.diagnostics_sink is None:
        return
    if not profile.catalog.enable_information_schema:
        return
    catalog_location, catalog_format = _effective_catalog_autoload_for_profile(profile)
    if catalog_location is None and catalog_format is None:
        return
    introspector = create_schema_introspector(profile, ctx)
    template_names = sorted(profile.data_sources.dataset_templates)
    payload: dict[str, object] = {
        "event_time_unix_ms": int(time.time() * 1000),
        "catalog_auto_load_location": catalog_location,
        "catalog_auto_load_format": catalog_format,
        "dataset_templates": template_names or None,
    }
    try:
        tables = [
            row
            for row in introspector.tables_snapshot()
            if row.get("table_schema") != "information_schema"
        ]
        payload["tables"] = tables
    except (RuntimeError, TypeError, ValueError) as exc:
        payload["error"] = str(exc)
    profile.record_artifact(_get_datafusion_catalog_autoload_spec(), payload)


def _ast_feature_gates(
    ctx: SessionContext,
) -> tuple[tuple[str, ...], tuple[str, ...], dict[str, object]]:
    """Determine AST feature gates based on DataFusion version and functions.

    Args:
        ctx: Session context.

    Returns:
        Tuple of (enabled_views, disabled_views, payload).
    """
    from datafusion_engine.session._session_constants import parse_major_version

    version = _datafusion_version(ctx)
    version_source = "sql"
    if version is None:
        version = datafusion.__version__
        version_source = "package"
    major = parse_major_version(version) if version else None
    functions = _datafusion_function_names(ctx)
    function_support = {name: name in functions for name in ("map_entries", "arrow_metadata")}
    enabled_optional: list[str] = []
    blocked_by_version: list[str] = []
    missing_functions: dict[str, list[str]] = {}
    for view in AST_OPTIONAL_VIEW_NAMES:
        required = _AST_OPTIONAL_VIEW_FUNCTIONS.get(view, ())
        if major is None:
            blocked_by_version.append(view)
            continue
        missing = find_missing(required, functions)
        if missing:
            missing_functions[view] = missing
            continue
        enabled_optional.append(view)
    view_names = AST_CORE_VIEW_NAMES + tuple(enabled_optional)
    disabled_views = tuple(view for view in AST_OPTIONAL_VIEW_NAMES if view not in enabled_optional)
    payload: dict[str, object] = {
        "event_time_unix_ms": int(time.time() * 1000),
        "datafusion_version": version,
        "datafusion_version_source": version_source,
        "datafusion_version_major": major,
        "required_functions": function_support,
        "enabled_views": list(view_names),
        "disabled_views": list(disabled_views) if disabled_views else None,
        "blocked_by_version": blocked_by_version or None,
        "missing_functions": missing_functions or None,
    }
    return view_names, disabled_views, payload


def _record_ast_feature_gates(
    profile: DataFusionRuntimeProfile, payload: Mapping[str, object]
) -> None:
    """Record AST feature gates artifact.

    Args:
        profile: Runtime profile.
        payload: Feature gates payload.
    """
    if profile.diagnostics.diagnostics_sink is None:
        return
    profile.record_artifact(_get_datafusion_ast_feature_gates_spec(), payload)


def _record_ast_span_metadata(profile: DataFusionRuntimeProfile, ctx: SessionContext) -> None:
    """Record AST span metadata.

    Args:
        profile: Runtime profile.
        ctx: Session context.
    """
    if profile.diagnostics.diagnostics_sink is None:
        return
    payload: dict[str, object] = {
        "event_time_unix_ms": int(time.time() * 1000),
        "dataset": "ast_files_v1",
    }
    try:
        table = ctx.table("ast_span_metadata").to_arrow_table()
        rows = table.to_pylist()
        schema = _resolved_table_schema(ctx, "ast_files_v1")
        if schema is not None:
            payload["schema_identity_hash"] = schema_identity_hash(schema)
        payload["metadata"] = rows[0] if rows else None
    except (KeyError, RuntimeError, TypeError, ValueError) as exc:
        payload["error"] = str(exc)
    version = _datafusion_version(ctx)
    if version is not None:
        payload["datafusion_version"] = version
    profile.record_artifact(_get_datafusion_ast_span_metadata_spec(), payload)


def _dataset_template(profile: DataFusionRuntimeProfile, name: str) -> DatasetLocation | None:
    """Resolve dataset template by name.

    Args:
        profile: Runtime profile.
        name: Dataset name.

    Returns:
        Dataset location or None.
    """
    templates = profile.data_sources.dataset_templates
    if not templates:
        return None
    template = templates.get(name)
    if template is not None:
        return template
    if name.endswith("_files_v1"):
        short_name = name.removesuffix("_files_v1")
        return templates.get(short_name)
    return None


def _ast_dataset_location(profile: DataFusionRuntimeProfile) -> DatasetLocation | None:
    """Get AST dataset location from profile templates.

    Args:
        profile: Runtime profile.

    Returns:
        Dataset location or None.
    """
    return _dataset_template(profile, "ast_files_v1")


def _register_ast_dataset(profile: DataFusionRuntimeProfile, ctx: SessionContext) -> None:
    """Register AST dataset on session context.

    Args:
        profile: Runtime profile.
        ctx: Session context.
    """
    location = _ast_dataset_location(profile)
    if location is None:
        return
    from datafusion_engine.io.adapter import DataFusionIOAdapter
    from datafusion_engine.session.facade import DataFusionExecutionFacade

    adapter = DataFusionIOAdapter(ctx=ctx, profile=profile)
    with contextlib.suppress(KeyError, RuntimeError, TypeError, ValueError):
        adapter.deregister_table("ast_files_v1")
    facade = DataFusionExecutionFacade(ctx=ctx, runtime_profile=profile)
    facade.register_dataset(name="ast_files_v1", location=location)
    _record_ast_registration(profile, location=location)


def _bytecode_dataset_location(profile: DataFusionRuntimeProfile) -> DatasetLocation | None:
    """Get bytecode dataset location from profile templates.

    Args:
        profile: Runtime profile.

    Returns:
        Dataset location or None.
    """
    return _dataset_template(profile, "bytecode_files_v1")


def _register_bytecode_dataset(profile: DataFusionRuntimeProfile, ctx: SessionContext) -> None:
    """Register bytecode dataset on session context.

    Args:
        profile: Runtime profile.
        ctx: Session context.
    """
    location = _bytecode_dataset_location(profile)
    if location is None:
        return
    from datafusion_engine.io.adapter import DataFusionIOAdapter
    from datafusion_engine.session.facade import DataFusionExecutionFacade

    adapter = DataFusionIOAdapter(ctx=ctx, profile=profile)
    with contextlib.suppress(KeyError, RuntimeError, TypeError, ValueError):
        adapter.deregister_table("bytecode_files_v1")
    facade = DataFusionExecutionFacade(ctx=ctx, runtime_profile=profile)
    facade.register_dataset(name="bytecode_files_v1", location=location)
    _record_bytecode_registration(profile, location=location)


def _register_scip_datasets(profile: DataFusionRuntimeProfile, ctx: SessionContext) -> None:
    """Register SCIP datasets on session context.

    Args:
        profile: Runtime profile.
        ctx: Session context.
    """
    scip_locations = profile.data_sources.extract_output.scip_dataset_locations
    if not scip_locations:
        return
    from datafusion_engine.io.adapter import DataFusionIOAdapter
    from datafusion_engine.session.facade import DataFusionExecutionFacade

    adapter = DataFusionIOAdapter(ctx=ctx, profile=profile)
    for name, location in sorted(scip_locations.items()):
        resolved = location
        with contextlib.suppress(KeyError, RuntimeError, TypeError, ValueError):
            adapter.deregister_table(name)
        facade = DataFusionExecutionFacade(ctx=ctx, runtime_profile=profile)
        df = facade.register_dataset(name=name, location=resolved)
        actual_schema = df.schema()
        actual_fingerprint = None
        if isinstance(actual_schema, pa.Schema):
            actual_fingerprint = schema_identity_hash(actual_schema.remove_metadata())
        snapshot = _ScipRegistrationSnapshot(
            name=name,
            location=resolved,
            expected_fingerprint=None,
            actual_fingerprint=actual_fingerprint,
            schema_match=None,
        )
        _record_scip_registration(profile, snapshot=snapshot)


def _registration_artifact_payload(
    profile: DataFusionRuntimeProfile,
    *,
    name: str,
    location: DatasetLocation,
    extra: Mapping[str, object] | None = None,
) -> dict[str, object]:
    """Build registration artifact payload.

    Args:
        profile: Runtime profile.
        name: Dataset name.
        location: Dataset location.
        extra: Extra payload fields.

    Returns:
        Artifact payload.
    """
    _ = profile
    resolved = location.resolved
    scan = resolved.datafusion_scan
    payload: dict[str, object] = {
        "event_time_unix_ms": int(time.time() * 1000),
        "name": name,
        "location": str(location.path),
        "format": location.format,
        "datafusion_provider": resolved.datafusion_provider,
        "file_sort_order": (
            [list(key) for key in scan.file_sort_order] if scan is not None else None
        ),
        "partition_cols": [
            {"name": col_name, "dtype": str(dtype)}
            for col_name, dtype in (scan.partition_cols_pyarrow() if scan is not None else ())
        ],
        "schema_force_view_types": scan.schema_force_view_types if scan is not None else None,
        "skip_arrow_metadata": scan.skip_arrow_metadata if scan is not None else None,
        "listing_table_factory_infer_partitions": (
            scan.listing_table_factory_infer_partitions if scan is not None else None
        ),
        "listing_table_ignore_subdirectory": (
            scan.listing_table_ignore_subdirectory if scan is not None else None
        ),
        "collect_statistics": scan.collect_statistics if scan is not None else None,
        "meta_fetch_concurrency": scan.meta_fetch_concurrency if scan is not None else None,
        "list_files_cache_limit": scan.list_files_cache_limit if scan is not None else None,
        "list_files_cache_ttl": scan.list_files_cache_ttl if scan is not None else None,
        "unbounded": scan.unbounded if scan is not None else None,
        "delta_version": location.delta_version,
        "delta_timestamp": location.delta_timestamp,
        "delta_constraints": list(resolved.delta_constraints)
        if resolved.delta_constraints
        else None,
    }
    if extra is not None:
        payload.update(extra)
    return payload


def _record_ast_registration(
    profile: DataFusionRuntimeProfile, *, location: DatasetLocation
) -> None:
    """Record AST dataset registration artifact.

    Args:
        profile: Runtime profile.
        location: Dataset location.
    """
    if profile.diagnostics.diagnostics_sink is None:
        return
    payload = _registration_artifact_payload(profile, name="ast_files_v1", location=location)
    profile.record_artifact(_get_datafusion_ast_dataset_spec(), payload)


def _record_bytecode_registration(
    profile: DataFusionRuntimeProfile, *, location: DatasetLocation
) -> None:
    """Record bytecode dataset registration artifact.

    Args:
        profile: Runtime profile.
        location: Dataset location.
    """
    if profile.diagnostics.diagnostics_sink is None:
        return
    payload = _registration_artifact_payload(profile, name="bytecode_files_v1", location=location)
    profile.record_artifact(_get_datafusion_bytecode_dataset_spec(), payload)


def _record_scip_registration(
    profile: DataFusionRuntimeProfile,
    *,
    snapshot: _ScipRegistrationSnapshot,
) -> None:
    """Record SCIP dataset registration artifact.

    Args:
        profile: Runtime profile.
        snapshot: Registration snapshot.
    """
    if profile.diagnostics.diagnostics_sink is None:
        return
    payload = _registration_artifact_payload(
        profile,
        name=snapshot.name,
        location=snapshot.location,
        extra={
            "expected_schema_identity_hash": snapshot.expected_fingerprint,
            "observed_schema_identity_hash": snapshot.actual_fingerprint,
            "schema_match": snapshot.schema_match,
        },
    )
    profile.record_artifact(_get_datafusion_scip_datasets_spec(), payload)


def _validate_ast_catalog_autoload(profile: DataFusionRuntimeProfile, ctx: SessionContext) -> None:
    """Validate AST catalog autoload.

    Args:
        profile: Runtime profile.
        ctx: Session context.

    Raises:
        ValueError: If validation fails.
    """
    catalog_location, catalog_format = _effective_catalog_autoload_for_profile(profile)
    if catalog_location is None and catalog_format is None:
        return
    try:
        ctx.table("ast_files_v1")
    except (KeyError, RuntimeError, TypeError, ValueError) as exc:
        msg = f"AST catalog autoload failed: {exc}."
        raise ValueError(msg) from exc
    if not profile.catalog.enable_information_schema:
        return
    try:
        create_schema_introspector(profile, ctx).table_column_names("ast_files_v1")
    except (RuntimeError, TypeError, ValueError) as exc:
        msg = f"AST catalog column introspection failed: {exc}."
        raise ValueError(msg) from exc


def _validate_bytecode_catalog_autoload(
    profile: DataFusionRuntimeProfile, ctx: SessionContext
) -> None:
    """Validate bytecode catalog autoload.

    Args:
        profile: Runtime profile.
        ctx: Session context.

    Raises:
        ValueError: If validation fails.
    """
    catalog_location, catalog_format = _effective_catalog_autoload_for_profile(profile)
    if catalog_location is None and catalog_format is None:
        return
    try:
        ctx.table("bytecode_files_v1")
    except (KeyError, RuntimeError, TypeError, ValueError) as exc:
        msg = f"Bytecode catalog autoload failed: {exc}."
        raise ValueError(msg) from exc
    if not profile.catalog.enable_information_schema:
        return
    try:
        create_schema_introspector(profile, ctx).table_column_names("bytecode_files_v1")
    except (RuntimeError, TypeError, ValueError) as exc:
        msg = f"Bytecode catalog column introspection failed: {exc}."
        raise ValueError(msg) from exc


def _record_cst_schema_diagnostics(profile: DataFusionRuntimeProfile, ctx: SessionContext) -> None:
    """Record CST schema diagnostics.

    Args:
        profile: Runtime profile.
        ctx: Session context.
    """
    if profile.diagnostics.diagnostics_sink is None:
        return
    payload: dict[str, object] = {
        "event_time_unix_ms": int(time.time() * 1000),
        "dataset": "libcst_files_v1",
    }
    if not ctx.table_exist("cst_schema_diagnostics"):
        payload["available"] = False
        profile.record_artifact(
            _get_datafusion_cst_schema_diagnostics_spec(),
            payload,
        )
        return
    rows, table_error = _safe_table_pylist(ctx, "cst_schema_diagnostics")
    if table_error is not None:
        payload["error"] = table_error
        profile.record_artifact(
            _get_datafusion_cst_schema_diagnostics_spec(),
            payload,
        )
        return
    try:
        schema = _resolved_table_schema(ctx, "libcst_files_v1")
        if schema is not None:
            payload["schema_identity_hash"] = schema_identity_hash(schema)
        default_entries = _default_value_entries(schema) if schema is not None else None
        payload["default_values"] = default_entries or None
        payload["diagnostics"] = rows[0] if rows else None
        introspector = create_schema_introspector(profile, ctx)
        payload["table_definition"] = introspector.table_definition("libcst_files_v1")
        payload["table_constraints"] = (
            list(introspector.table_constraints("libcst_files_v1")) or None
        )
    except _DIAGNOSTICS_CAPTURE_ERRORS as exc:
        payload["error"] = str(exc)
    profile.record_artifact(
        _get_datafusion_cst_schema_diagnostics_spec(),
        payload,
    )


def _record_tree_sitter_stats(profile: DataFusionRuntimeProfile, ctx: SessionContext) -> None:
    """Record tree-sitter stats.

    Args:
        profile: Runtime profile.
        ctx: Session context.
    """
    if profile.diagnostics.diagnostics_sink is None:
        return
    payload: dict[str, object] = {
        "event_time_unix_ms": int(time.time() * 1000),
        "dataset": "tree_sitter_files_v1",
    }
    rows, table_error = _safe_table_pylist(ctx, "ts_stats")
    if table_error is not None:
        payload["error"] = table_error
        profile.record_artifact(_get_datafusion_tree_sitter_stats_spec(), payload)
        return
    try:
        schema = _resolved_table_schema(ctx, "tree_sitter_files_v1")
        if schema is not None:
            payload["schema_identity_hash"] = schema_identity_hash(schema)
        payload["stats"] = rows[0] if rows else None
        introspector = create_schema_introspector(profile, ctx)
        payload["table_definition"] = introspector.table_definition("tree_sitter_files_v1")
        payload["table_constraints"] = (
            list(introspector.table_constraints("tree_sitter_files_v1")) or None
        )
    except _DIAGNOSTICS_CAPTURE_ERRORS as exc:
        payload["error"] = str(exc)
    profile.record_artifact(_get_datafusion_tree_sitter_stats_spec(), payload)


def _record_tree_sitter_view_schemas(
    profile: DataFusionRuntimeProfile, ctx: SessionContext
) -> None:
    """Record tree-sitter view schemas.

    Args:
        profile: Runtime profile.
        ctx: Session context.
    """
    if profile.diagnostics.diagnostics_sink is None:
        return
    views: list[dict[str, object]] = []
    payload: dict[str, object] = {
        "event_time_unix_ms": int(time.time() * 1000),
        "dataset": "tree_sitter_files_v1",
        "views": views,
    }
    errors: dict[str, str] = {}
    introspector = create_schema_introspector(profile, ctx)
    for name in TREE_SITTER_VIEW_NAMES:
        try:
            plan = _table_logical_plan(ctx, name=name)
            dfschema_tree = _table_dfschema_tree(ctx, name=name)
        except (KeyError, RuntimeError, TypeError, ValueError) as exc:
            errors[name] = str(exc)
            continue
        views.append(
            {
                "name": name,
                "logical_plan": plan,
                "dfschema_tree": dfschema_tree,
            }
        )
    try:
        payload["df_settings"] = introspector.settings_snapshot()
    except (KeyError, RuntimeError, TypeError, ValueError) as exc:
        errors["df_settings"] = str(exc)
    if errors:
        payload["errors"] = errors
    version = _datafusion_version(ctx)
    if version is not None:
        payload["datafusion_version"] = version
    profile.record_artifact(
        _get_datafusion_tree_sitter_plan_schema_spec(),
        payload,
    )


def _record_tree_sitter_cross_checks(
    profile: DataFusionRuntimeProfile, ctx: SessionContext
) -> dict[str, object] | None:
    """Record tree-sitter cross-checks.

    Args:
        profile: Runtime profile.
        ctx: Session context.

    Returns:
        Cross-check payload or None.
    """
    if profile.diagnostics.diagnostics_sink is None:
        return None
    views: list[dict[str, object]] = []
    payload: dict[str, object] = {
        "event_time_unix_ms": int(time.time() * 1000),
        "dataset": "tree_sitter_files_v1",
        "views": views,
    }
    errors: dict[str, str] = {}
    for name in TREE_SITTER_CHECK_VIEWS:
        try:
            if not ctx.table_exist(name):
                errors[name] = "table not found"
                continue
            summary_sql = (
                "SELECT count(*) AS row_count, "
                "sum(CASE WHEN mismatch THEN 1 ELSE 0 END) AS mismatch_count "
                f"FROM {name}"
            )
            summary_rows = (
                _sql_with_options(
                    ctx,
                    summary_sql,
                    sql_options=sql_options_for_profile(profile),
                )
                .to_arrow_table()
                .to_pylist()
            )
            summary: dict[str, object] = summary_rows[0] if summary_rows else {}
            raw_row_count = summary.get("row_count")
            row_count = int(raw_row_count) if isinstance(raw_row_count, (float, int, str)) else 0
            raw_mismatch_count = summary.get("mismatch_count")
            mismatch_count = (
                int(raw_mismatch_count) if isinstance(raw_mismatch_count, (float, int, str)) else 0
            )
            entry: dict[str, object] = {
                "name": name,
                "row_count": row_count,
                "mismatch_count": mismatch_count,
            }
            if mismatch_count:
                sample_sql = f"SELECT * FROM {name} WHERE mismatch LIMIT 25"
                sample_rows = (
                    _sql_with_options(
                        ctx,
                        sample_sql,
                        sql_options=sql_options_for_profile(profile),
                    )
                    .to_arrow_table()
                    .to_pylist()
                )
                entry["sample"] = sample_rows or None
            views.append(entry)
        except (KeyError, RuntimeError, TypeError, ValueError) as exc:
            errors[name] = str(exc)
    if errors:
        payload["errors"] = errors
    profile.record_artifact(
        _get_datafusion_tree_sitter_cross_checks_spec(),
        payload,
    )
    return payload


def _record_cst_view_plans(profile: DataFusionRuntimeProfile, ctx: SessionContext) -> None:
    """Record CST view plans.

    Args:
        profile: Runtime profile.
        ctx: Session context.
    """
    if profile.diagnostics.diagnostics_sink is None:
        return
    views: list[dict[str, object]] = []
    payload: dict[str, object] = {
        "event_time_unix_ms": int(time.time() * 1000),
        "dataset": "libcst_files_v1",
        "views": views,
    }
    errors: dict[str, str] = {}
    for name in CST_VIEW_NAMES:
        try:
            plan = _table_logical_plan(ctx, name=name)
        except (KeyError, RuntimeError, TypeError, ValueError) as exc:
            errors[name] = str(exc)
            continue
        views.append({"name": name, "logical_plan": plan})
    if errors:
        payload["errors"] = errors
    version = _datafusion_version(ctx)
    if version is not None:
        payload["datafusion_version"] = version
    profile.record_artifact(_get_datafusion_cst_view_plans_spec(), payload)


def _record_cst_dfschema_snapshots(profile: DataFusionRuntimeProfile, ctx: SessionContext) -> None:
    """Record CST DFSchema snapshots.

    Args:
        profile: Runtime profile.
        ctx: Session context.
    """
    if profile.diagnostics.diagnostics_sink is None:
        return
    views: list[dict[str, object]] = []
    payload: dict[str, object] = {
        "event_time_unix_ms": int(time.time() * 1000),
        "dataset": "libcst_files_v1",
        "views": views,
    }
    errors: dict[str, str] = {}
    for name in CST_VIEW_NAMES:
        try:
            tree = _table_dfschema_tree(ctx, name=name)
        except (KeyError, RuntimeError, TypeError, ValueError) as exc:
            errors[name] = str(exc)
            continue
        views.append({"name": name, "dfschema_tree": tree})
    if errors:
        payload["errors"] = errors
    version = _datafusion_version(ctx)
    if version is not None:
        payload["datafusion_version"] = version
    profile.record_artifact(_get_datafusion_cst_dfschema_spec(), payload)


def _record_bytecode_metadata(profile: DataFusionRuntimeProfile, ctx: SessionContext) -> None:
    """Record bytecode metadata.

    Args:
        profile: Runtime profile.
        ctx: Session context.
    """
    if profile.diagnostics.diagnostics_sink is None:
        return
    if not profile.catalog.enable_information_schema:
        return
    payload: dict[str, object] = {
        "event_time_unix_ms": int(time.time() * 1000),
        "dataset": "bytecode_files_v1",
    }
    try:
        table = ctx.table("py_bc_metadata").to_arrow_table()
        rows = table.to_pylist()
        schema = _resolved_table_schema(ctx, "bytecode_files_v1")
        if schema is not None:
            payload["schema_identity_hash"] = schema_identity_hash(schema)
        payload["metadata"] = rows[0] if rows else None
        introspector = create_schema_introspector(profile, ctx)
        payload["table_definition"] = introspector.table_definition("bytecode_files_v1")
        payload["table_constraints"] = (
            list(introspector.table_constraints("bytecode_files_v1")) or None
        )
    except (KeyError, RuntimeError, TypeError, ValueError) as exc:
        payload["error"] = str(exc)
    profile.record_artifact(_get_datafusion_bytecode_metadata_spec(), payload)


def _record_schema_snapshots(profile: DataFusionRuntimeProfile, ctx: SessionContext) -> None:
    """Record schema snapshots.

    Args:
        profile: Runtime profile.
        ctx: Session context.
    """
    if profile.diagnostics.diagnostics_sink is None:
        return
    if not profile.catalog.enable_information_schema:
        return
    introspector = create_schema_introspector(profile, ctx)
    payload: dict[str, object] = {
        "event_time_unix_ms": int(time.time() * 1000),
    }
    try:
        payload.update(
            {
                "catalogs": catalogs_snapshot(introspector),
                "schemata": introspector.schemata_snapshot(),
                "tables": introspector.tables_snapshot(),
                "columns": introspector.columns_snapshot(),
                "constraints": constraint_rows(
                    ctx,
                    sql_options=sql_options_for_profile(profile),
                ),
                "routines": introspector.routines_snapshot(),
                "parameters": introspector.parameters_snapshot(),
                "settings": introspector.settings_snapshot(),
                "functions": function_catalog_snapshot_for_profile(
                    profile,
                    ctx,
                    include_routines=profile.catalog.enable_information_schema,
                ),
            }
        )
        version = _datafusion_version(ctx)
        if version is not None:
            payload["datafusion_version"] = version
    except (RuntimeError, TypeError, ValueError) as exc:
        payload["error"] = str(exc)
    profile.record_artifact(
        _get_datafusion_schema_introspection_spec(),
        payload,
    )


def _validate_catalog_autoloads(
    profile: DataFusionRuntimeProfile,
    ctx: SessionContext,
    *,
    ast_registration: bool,
    bytecode_registration: bool,
) -> None:
    """Validate catalog autoloads.

    Args:
        profile: Runtime profile.
        ctx: Session context.
        ast_registration: Whether AST was registered.
        bytecode_registration: Whether bytecode was registered.
    """
    catalog_location, catalog_format = _effective_catalog_autoload_for_profile(profile)
    if catalog_location is None and catalog_format is None:
        return
    if not ast_registration:
        from datafusion_engine.io.adapter import DataFusionIOAdapter

        adapter = DataFusionIOAdapter(ctx=ctx, profile=profile)
        with contextlib.suppress(KeyError, RuntimeError, TypeError, ValueError):
            adapter.deregister_table("ast_files_v1")
        _validate_ast_catalog_autoload(profile, ctx)
    if not bytecode_registration:
        from datafusion_engine.io.adapter import DataFusionIOAdapter

        adapter = DataFusionIOAdapter(ctx=ctx, profile=profile)
        with contextlib.suppress(KeyError, RuntimeError, TypeError, ValueError):
            adapter.deregister_table("bytecode_files_v1")
        _validate_bytecode_catalog_autoload(profile, ctx)


def _record_schema_diagnostics(
    profile: DataFusionRuntimeProfile,
    ctx: SessionContext,
    *,
    ast_view_names: Sequence[str],
) -> Mapping[str, object] | None:
    """Record schema diagnostics for all subsystems.

    Args:
        profile: Runtime profile.
        ctx: Session context.
        ast_view_names: Enabled AST view names.

    Returns:
        Tree-sitter cross-check payload or None.
    """
    _record_cst_schema_diagnostics(profile, ctx)
    _record_cst_view_plans(profile, ctx)
    _record_cst_dfschema_snapshots(profile, ctx)
    _record_tree_sitter_stats(profile, ctx)
    _record_tree_sitter_view_schemas(profile, ctx)
    tree_sitter_checks = _record_tree_sitter_cross_checks(profile, ctx)
    if "ast_span_metadata" in ast_view_names:
        _record_ast_span_metadata(profile, ctx)
    _record_bytecode_metadata(profile, ctx)
    return tree_sitter_checks


def _validate_schema_views(
    ctx: SessionContext,
    *,
    ast_view_names: Sequence[str],
    allow_semantic_row_probe_fallback: bool = True,
) -> dict[str, str]:
    """Validate schema views.

    Args:
        ctx: Session context.
        ast_view_names: Enabled AST view names.
        allow_semantic_row_probe_fallback: Allow row probe fallback.

    Returns:
        View validation errors.
    """
    from datafusion_engine.udf.extension_runtime import extension_capabilities_report

    _ = ast_view_names
    view_errors: dict[str, str] = {}
    try:
        capabilities = extension_capabilities_report()
    except (RuntimeError, TypeError, ValueError):
        capabilities = {"available": False, "compatible": False}
    if bool(capabilities.get("available")) and bool(capabilities.get("compatible")):
        for label, validator in (
            ("udf_info_schema_parity", validate_udf_info_schema_parity),
            ("engine_functions", validate_required_engine_functions),
        ):
            try:
                validator(ctx)
            except (RuntimeError, TypeError, ValueError) as exc:
                view_errors[label] = str(exc)
    for name in extract_nested_dataset_names():
        if not ctx.table_exist(name):
            continue
        try:
            validate_nested_types(ctx, name)
        except (RuntimeError, TypeError, ValueError) as exc:
            view_errors[f"nested_types:{name}"] = str(exc)
    try:
        validate_semantic_types(
            ctx,
            allow_row_probe_fallback=allow_semantic_row_probe_fallback,
        )
    except (RuntimeError, TypeError, ValueError) as exc:
        view_errors["semantic_types"] = str(exc)
    return view_errors


def _register_schema_tables(
    ctx: SessionContext,
    *,
    names: Sequence[str],
    resolver: Callable[[str], pa.Schema],
) -> None:
    """Register schema tables on context.

    Args:
        ctx: Session context.
        names: Table names to register.
        resolver: Schema resolver function.
    """
    for name in names:
        if ctx.table_exist(name):
            continue
        schema = resolver(name)
        _register_schema_table(ctx, name, schema)


def _schema_registry_issues(
    validation: SchemaRegistryValidationResult,
    *,
    zero_row_bootstrap: ZeroRowBootstrapConfig,
) -> tuple[dict[str, object], dict[str, object] | None]:
    """Extract issues and advisories from validation result.

    Args:
        validation: Validation result.
        zero_row_bootstrap: Bootstrap config.

    Returns:
        Tuple of (issues, advisory).
    """
    issues: dict[str, object] = {}
    advisory: dict[str, object] = {}
    if validation.missing:
        issues["missing"] = list(validation.missing)
    if validation.type_errors:
        issues["type_errors"] = dict(validation.type_errors)
    if validation.view_errors:
        if zero_row_bootstrap.validation_mode == "bootstrap" and not zero_row_bootstrap.strict:
            advisory["view_errors"] = dict(validation.view_errors)
        else:
            issues["view_errors"] = dict(validation.view_errors)
    if validation.constraint_drift:
        issues["constraint_drift"] = list(validation.constraint_drift)
    if validation.relationship_constraint_errors:
        issues["relationship_constraint_errors"] = dict(validation.relationship_constraint_errors)
    return issues, advisory or None


def _semantic_input_schema_names() -> tuple[str, ...]:
    """Get semantic input schema names.

    Returns:
        Tuple of schema names.
    """
    from semantics.input_registry import SEMANTIC_INPUT_SPECS

    return tuple(dict.fromkeys(spec.extraction_source for spec in SEMANTIC_INPUT_SPECS))


def _register_schema_registry_tables(
    profile: DataFusionRuntimeProfile, ctx: SessionContext
) -> None:
    """Register all schema registry tables.

    Args:
        profile: Runtime profile.
        ctx: Session context.
    """
    _ = profile
    from datafusion_engine.extract.registry import dataset_schema as extract_dataset_schema

    _register_schema_tables(
        ctx,
        names=extract_nested_dataset_names(),
        resolver=extract_schema_for,
    )
    _register_schema_tables(
        ctx,
        names=_semantic_input_schema_names(),
        resolver=extract_dataset_schema,
    )
    _register_schema_tables(
        ctx,
        names=relationship_schema_names(),
        resolver=relationship_schema_for,
    )


def _prepare_schema_registry_context(
    profile: DataFusionRuntimeProfile,
    ctx: SessionContext,
) -> tuple[Sequence[str], bool, bool]:
    """Prepare schema registry context.

    Args:
        profile: Runtime profile.
        ctx: Session context.

    Returns:
        Tuple of (ast_view_names, ast_registration, bytecode_registration).
    """
    _record_catalog_autoload_snapshot(profile, ctx)
    ast_view_names, _, ast_gate_payload = _ast_feature_gates(ctx)
    _record_ast_feature_gates(profile, ast_gate_payload)
    ast_registration = _ast_dataset_location(profile) is not None
    if ast_registration:
        _register_ast_dataset(profile, ctx)
    bytecode_registration = _bytecode_dataset_location(profile) is not None
    if bytecode_registration:
        _register_bytecode_dataset(profile, ctx)
    _register_scip_datasets(profile, ctx)
    _register_schema_registry_tables(profile, ctx)
    return ast_view_names, ast_registration, bytecode_registration


def _install_schema_registry(profile: DataFusionRuntimeProfile, ctx: SessionContext) -> None:
    """Register canonical nested schemas on the session context.

    Args:
        profile: Runtime profile.
        ctx: Session context.

    Raises:
        ValueError: If the operation cannot be completed.
    """
    if not profile.features.enable_schema_registry:
        return
    ast_view_names, ast_registration, bytecode_registration = _prepare_schema_registry_context(
        profile, ctx
    )
    _validate_catalog_autoloads(
        profile,
        ctx,
        ast_registration=ast_registration,
        bytecode_registration=bytecode_registration,
    )
    tree_sitter_checks = _record_schema_diagnostics(
        profile,
        ctx,
        ast_view_names=ast_view_names,
    )
    view_errors = _validate_schema_views(
        ctx,
        ast_view_names=ast_view_names,
        allow_semantic_row_probe_fallback=(
            profile.zero_row_bootstrap.allow_semantic_row_probe_fallback
        ),
    )
    validation = _record_schema_registry_validation(
        profile,
        ctx,
        expected_names=(),
        expected_schemas=None,
        view_errors=view_errors or None,
        tree_sitter_checks=tree_sitter_checks,
    )
    issues, advisory = _schema_registry_issues(
        validation,
        zero_row_bootstrap=profile.zero_row_bootstrap,
    )
    if advisory:
        profile.record_artifact(
            _get_schema_registry_validation_advisory_spec(),
            {
                "event_time_unix_ms": int(time.time() * 1000),
                "issues": advisory,
                "validation_mode": profile.zero_row_bootstrap.validation_mode,
                "strict": profile.zero_row_bootstrap.strict,
            },
        )
    if issues:
        msg = f"Schema registry validation failed: {issues}."
        raise ValueError(msg)
    _record_schema_snapshots(profile, ctx)


def _prepare_statements(profile: DataFusionRuntimeProfile, ctx: SessionContext) -> None:
    """Prepare SQL statements when configured.

    Args:
        profile: Runtime profile.
        ctx: Session context.
    """
    from datafusion_engine.session.runtime_profile_config import INFO_SCHEMA_STATEMENT_NAMES

    statements = list(profile.policies.prepared_statements)
    if not profile.catalog.enable_information_schema:
        statements = [
            statement
            for statement in statements
            if statement.name not in INFO_SCHEMA_STATEMENT_NAMES
        ]
    seen: set[str] = set()
    for statement in statements:
        if statement.name in seen:
            continue
        seen.add(statement.name)
        _sql_with_options(
            ctx,
            _prepare_statement_sql(statement),
            sql_options=statement_sql_options_for_profile(profile),
            allow_statements=True,
        )
        _record_prepared_statement(profile, statement)


def _record_prepared_statement(
    profile: DataFusionRuntimeProfile, statement: PreparedStatementSpec
) -> None:
    """Record prepared statement artifact.

    Args:
        profile: Runtime profile.
        statement: Statement spec.
    """
    if profile.diagnostics.diagnostics_sink is None:
        return
    profile.record_artifact(
        _get_datafusion_prepared_statements_spec(),
        {
            "name": statement.name,
            "sql": statement.sql,
            "param_types": list(statement.param_types),
        },
    )
