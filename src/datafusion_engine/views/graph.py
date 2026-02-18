"""Dependency-aware view registration for view-driven pipelines."""

from __future__ import annotations

import time
from collections import deque
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, cast

import pyarrow as pa
from datafusion import SessionContext
from datafusion.dataframe import DataFrame

from datafusion_engine.identity import schema_identity_hash
from datafusion_engine.io.adapter import DataFusionIOAdapter
from datafusion_engine.lineage.diagnostics import record_artifact
from datafusion_engine.plan.signals import extract_plan_signals
from datafusion_engine.schema.contracts import SchemaContract, ValidationViolation, ViolationType
from datafusion_engine.schema.introspection_core import SchemaIntrospector
from datafusion_engine.udf.extension_core import (
    udf_names_from_snapshot,
    validate_required_udfs,
    validate_rust_udf_snapshot,
)
from datafusion_engine.views.artifacts import (
    CachePolicy,
    ViewArtifactLineage,
    ViewArtifactRequest,
    build_view_artifact_from_bundle,
)
from datafusion_engine.views.bundle_extraction import (
    arrow_schema_from_df,
    extract_lineage_from_bundle,
    resolve_required_udfs_from_bundle,
)
from datafusion_engine.views.cache_registration import register_view_with_cache
from utils.registry_protocol import MappingRegistryAdapter
from utils.validation import validate_required_items

if TYPE_CHECKING:
    from datafusion_engine.lineage.reporting import LineageReport
    from datafusion_engine.lineage.scheduling import ScanUnit
    from datafusion_engine.plan.bundle_artifact import DataFusionPlanArtifact
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from semantics.program_manifest import ManifestDatasetResolver


@dataclass(frozen=True)
class ViewNode:
    """Declarative view definition with explicit dependencies.

    Attributes:
    ----------
    name : str
        View name for registration.
    deps : tuple[str, ...]
        Dependency names (table/view references).
    builder : Callable[[SessionContext], DataFrame]
        Function that builds the DataFrame for this view.
    contract_builder : Callable[[pa.Schema], SchemaContract] | None
        Optional schema contract builder.
    required_udfs : tuple[str, ...]
        Required UDF names for this view.
    plan_bundle : DataFusionPlanArtifact | None
        DataFusion plan bundle (preferred source of truth for lineage).
    cache_policy : CachePolicy
        Cache policy for view materialization. Legacy "memory" is treated
        as "delta_staging" to avoid in-memory caching.
    """

    name: str
    deps: tuple[str, ...]
    builder: Callable[[SessionContext], DataFrame]
    contract_builder: Callable[[pa.Schema], SchemaContract] | None = None
    required_udfs: tuple[str, ...] = ()
    plan_bundle: DataFusionPlanArtifact | None = None
    cache_policy: CachePolicy = "none"


def view_graph_registry(
    nodes: Sequence[ViewNode] = (),
) -> MappingRegistryAdapter[str, ViewNode]:
    """Return a registry adapter for view graph nodes.

    Returns:
    -------
    MappingRegistryAdapter[str, ViewNode]
        Registry adapter populated with the provided view nodes.
    """
    registry = MappingRegistryAdapter[str, ViewNode]()
    for node in nodes:
        registry.register(node.name, node)
    return registry


class SchemaContractViolationError(ValueError):
    """Raised when a schema contract fails validation."""

    def __init__(
        self,
        *,
        table_name: str,
        violations: Sequence[ValidationViolation],
    ) -> None:
        """Initialize the instance.

        Args:
            table_name: Description.
            violations: Description.
        """
        self.table_name = table_name
        self.violations = tuple(violations)
        details = [
            f"{violation.violation_type.value}:{violation.column_name}"
            for violation in self.violations
        ]
        msg = f"Schema contract violations for {table_name!r}: {details}."
        super().__init__(msg)


@dataclass(frozen=True)
class ViewGraphOptions:
    """Configuration for view graph registration."""

    overwrite: bool = True
    temporary: bool = False
    validate_schema: bool = True


@dataclass(frozen=True)
class ViewGraphRuntimeOptions:
    """Runtime options for view graph registration."""

    runtime_profile: DataFusionRuntimeProfile | None = None
    require_artifacts: bool = False
    dataset_resolver: ManifestDatasetResolver | None = None


@dataclass(frozen=True)
class ViewCacheContext:
    """Context for view cache materialization."""

    runtime: ViewGraphRuntimeOptions
    options: ViewGraphOptions


@dataclass(frozen=True)
class ViewGraphContext:
    """Shared context for view graph registration."""

    ctx: SessionContext
    snapshot: Mapping[str, object]
    runtime: ViewGraphRuntimeOptions
    options: ViewGraphOptions
    adapter: DataFusionIOAdapter
    runtime_hash: str | None
    cache_context: ViewCacheContext


@dataclass
class ViewGraphScanState:
    """Mutable scan-unit tracking state for view graphs."""

    scan_units_by_key: dict[str, ScanUnit]
    scan_keys_by_view: dict[str, tuple[str, ...]]


def _finalize_df_to_contract(
    ctx: SessionContext,
    *,
    df: DataFrame,
    contract_builder: Callable[[pa.Schema], SchemaContract],
) -> DataFrame:
    from datafusion import col, lit

    from datafusion_engine.expr.cast import safe_cast

    _ = ctx
    schema = arrow_schema_from_df(df)
    contract = contract_builder(schema)
    target_schema = contract.to_arrow_schema()
    existing = set(df.schema().names)
    selections = [
        (
            safe_cast(col(field.name), field.type).alias(field.name)
            if field.name in existing
            else safe_cast(lit(None), field.type).alias(field.name)
        )
        for field in target_schema
    ]
    return df.select(*selections)


def register_view_graph(
    ctx: SessionContext,
    *,
    nodes: Sequence[ViewNode],
    snapshot: Mapping[str, object],
    runtime_options: ViewGraphRuntimeOptions | None = None,
    options: ViewGraphOptions | None = None,
) -> None:
    """Register a dependency-sorted view graph on a SessionContext.

    Args:
        ctx: Description.
        nodes: Description.
        snapshot: Description.
        runtime_options: Description.
        options: Description.

    Raises:
        ValueError: If the operation cannot be completed.
    """
    resolved = options or ViewGraphOptions()
    runtime = runtime_options or ViewGraphRuntimeOptions()
    if runtime.require_artifacts and runtime.runtime_profile is None:
        msg = "Runtime profile is required for view artifact recording."
        raise ValueError(msg)
    validate_rust_udf_snapshot(snapshot)
    materialized = _materialize_nodes(nodes, snapshot=snapshot)
    ordered = _topo_sort_nodes(materialized)
    adapter = DataFusionIOAdapter(ctx=ctx, profile=runtime.runtime_profile)
    cache_context = ViewCacheContext(runtime=runtime, options=resolved)
    context = ViewGraphContext(
        ctx=ctx,
        snapshot=snapshot,
        runtime=runtime,
        options=resolved,
        adapter=adapter,
        runtime_hash=_runtime_hash(runtime.runtime_profile),
        cache_context=cache_context,
    )
    scan_state = ViewGraphScanState(scan_units_by_key={}, scan_keys_by_view={})
    lineage_by_view: dict[str, LineageReport | None] = {}
    for node in ordered:
        _register_view_node(
            context,
            node=node,
            materialized=materialized,
            scan_state=scan_state,
            lineage_by_view=lineage_by_view,
        )
    _record_view_udf_parity(context, nodes=ordered)
    _record_udf_audit(context)
    _record_udf_catalog(context)
    _persist_plan_artifacts(
        context,
        ordered,
        scan_state=scan_state,
        lineage_by_view=lineage_by_view,
    )


def _runtime_hash(runtime_profile: DataFusionRuntimeProfile | None) -> str | None:
    if runtime_profile is None:
        return None
    from datafusion_engine.session.runtime_session import session_runtime_hash

    return session_runtime_hash(runtime_profile.session_runtime())


def _register_view_node(
    context: ViewGraphContext,
    *,
    node: ViewNode,
    materialized: Sequence[ViewNode],
    scan_state: ViewGraphScanState,
    lineage_by_view: dict[str, LineageReport | None],
) -> None:
    _validate_deps(context.ctx, node, materialized)
    _validate_udf_calls(context.snapshot, node)
    validate_required_udfs(context.snapshot, required=node.required_udfs)
    _validate_required_functions(context.ctx, node.required_udfs)
    _maybe_capture_scan_units(context, node=node, scan_state=scan_state)
    _maybe_capture_lineage(context.runtime, node=node, lineage_by_view=lineage_by_view)
    df = node.builder(context.ctx)
    if node.contract_builder is not None:
        df = _finalize_df_to_contract(context.ctx, df=df, contract_builder=node.contract_builder)
    registered = register_view_with_cache(
        context.ctx,
        adapter=context.adapter,
        node=node,
        df=df,
        cache=context.cache_context,
    )
    schema = arrow_schema_from_df(registered)
    _maybe_validate_schema_contract(context, node=node, schema=schema)
    _maybe_validate_information_schema(context, node=node, schema=schema)
    _maybe_record_view_definition(context, node=node, schema=schema)
    _maybe_record_explain_analyze_threshold(context, node=node)


def _maybe_capture_scan_units(
    context: ViewGraphContext,
    *,
    node: ViewNode,
    scan_state: ViewGraphScanState,
) -> None:
    runtime_profile = context.runtime.runtime_profile
    if runtime_profile is None or node.plan_bundle is None:
        return
    scan_units = _plan_scan_units_for_bundle(
        context.ctx,
        bundle=node.plan_bundle,
        dataset_resolver=context.runtime.dataset_resolver,
    )
    if not scan_units:
        return
    from datafusion_engine.dataset.resolution import apply_scan_unit_overrides

    apply_scan_unit_overrides(
        context.ctx,
        scan_units=scan_units,
        runtime_profile=runtime_profile,
        dataset_resolver=context.runtime.dataset_resolver,
    )
    scan_state.scan_keys_by_view[node.name] = tuple(unit.key for unit in scan_units)
    for unit in scan_units:
        scan_state.scan_units_by_key[unit.key] = unit


def _maybe_capture_lineage(
    runtime: ViewGraphRuntimeOptions,
    *,
    node: ViewNode,
    lineage_by_view: dict[str, LineageReport | None],
) -> None:
    if runtime.runtime_profile is None or node.plan_bundle is None:
        return
    try:
        lineage_by_view[node.name] = extract_lineage_from_bundle(node.plan_bundle)
    except (RuntimeError, TypeError, ValueError):
        lineage_by_view[node.name] = None


def _maybe_validate_schema_contract(
    context: ViewGraphContext,
    *,
    node: ViewNode,
    schema: pa.Schema,
) -> None:
    if not context.options.validate_schema or node.contract_builder is None:
        return
    contract = node.contract_builder(schema)
    try:
        _validate_schema_contract(context.ctx, contract, schema=schema)
    except SchemaContractViolationError as exc:
        runtime_profile = context.runtime.runtime_profile
        if runtime_profile is None:
            return
        from datafusion_engine.lineage.diagnostics import record_artifact
        from serde_artifact_specs import SCHEMA_CONTRACT_VIOLATIONS_SPEC

        record_artifact(
            runtime_profile,
            SCHEMA_CONTRACT_VIOLATIONS_SPEC,
            {
                "event_time_unix_ms": int(time.time() * 1000),
                "table_name": exc.table_name,
                "violations": [
                    f"{violation.violation_type.value}:{violation.column_name}"
                    for violation in exc.violations
                ],
            },
        )


def _maybe_validate_information_schema(
    context: ViewGraphContext,
    *,
    node: ViewNode,
    schema: pa.Schema,
) -> None:
    runtime_profile = context.runtime.runtime_profile
    if runtime_profile is None:
        return
    if not context.options.validate_schema or context.options.temporary:
        return
    if not runtime_profile.catalog.enable_information_schema:
        return
    from datafusion_engine.schema.catalog_contracts import (
        contract_violations_for_schema,
        schema_contract_from_information_schema,
    )

    info_contract = schema_contract_from_information_schema(context.ctx, table_name=node.name)
    info_violations = contract_violations_for_schema(
        contract=info_contract,
        schema=schema,
    )
    if info_violations:
        runtime_profile = context.runtime.runtime_profile
        if runtime_profile is None:
            return
        from datafusion_engine.lineage.diagnostics import record_artifact
        from serde_artifact_specs import INFORMATION_SCHEMA_CONTRACT_VIOLATIONS_SPEC

        record_artifact(
            runtime_profile,
            INFORMATION_SCHEMA_CONTRACT_VIOLATIONS_SPEC,
            {
                "event_time_unix_ms": int(time.time() * 1000),
                "table_name": node.name,
                "violations": [
                    f"{violation.violation_type.value}:{violation.column_name}"
                    for violation in info_violations
                ],
            },
        )
        return


def _maybe_record_view_definition(
    context: ViewGraphContext,
    *,
    node: ViewNode,
    schema: pa.Schema,
) -> None:
    runtime_profile = context.runtime.runtime_profile
    if runtime_profile is None:
        return
    if node.plan_bundle is None:
        msg = f"View {node.name!r} missing plan bundle for artifact recording."
        raise ValueError(msg)
    from datafusion_engine.session.runtime_session import record_view_definition

    artifact = build_view_artifact_from_bundle(
        node.plan_bundle,
        request=ViewArtifactRequest(
            name=node.name,
            schema=schema,
            lineage=ViewArtifactLineage(
                required_udfs=node.required_udfs,
                referenced_tables=node.deps,
            ),
            runtime_hash=context.runtime_hash,
            cache_policy=node.cache_policy,
        ),
    )
    record_view_definition(runtime_profile, artifact=artifact)


def _maybe_record_explain_analyze_threshold(
    context: ViewGraphContext,
    *,
    node: ViewNode,
) -> None:
    profile = context.runtime.runtime_profile
    if profile is None:
        return
    threshold = profile.diagnostics.explain_analyze_threshold_ms
    bundle = node.plan_bundle
    if threshold is None or bundle is None:
        return
    signals = extract_plan_signals(bundle)
    duration_ms = signals.explain_analyze_duration_ms
    if duration_ms is None or duration_ms < threshold:
        return
    payload = {
        "view_name": node.name,
        "plan_fingerprint": bundle.plan_fingerprint,
        "plan_identity_hash": bundle.plan_identity_hash,
        "duration_ms": duration_ms,
        "threshold_ms": float(threshold),
        "output_rows": signals.explain_analyze_output_rows,
    }
    from serde_artifact_specs import VIEW_EXPLAIN_ANALYZE_THRESHOLD_SPEC

    record_artifact(profile, VIEW_EXPLAIN_ANALYZE_THRESHOLD_SPEC, payload)


def _persist_plan_artifacts(
    context: ViewGraphContext,
    nodes: Sequence[ViewNode],
    *,
    scan_state: ViewGraphScanState,
    lineage_by_view: Mapping[str, LineageReport | None],
) -> None:
    runtime_profile = context.runtime.runtime_profile
    if runtime_profile is None or not runtime_profile.diagnostics.capture_plan_artifacts:
        return
    from datafusion_engine.plan.artifact_store_core import (
        PlanArtifactsForViewsRequest,
        persist_plan_artifacts_for_views,
    )

    persist_plan_artifacts_for_views(
        context.ctx,
        runtime_profile,
        request=PlanArtifactsForViewsRequest(
            view_nodes=nodes,
            scan_units=tuple(scan_state.scan_units_by_key.values()),
            scan_keys_by_view=scan_state.scan_keys_by_view,
            lineage_by_view={
                name: report for name, report in lineage_by_view.items() if report is not None
            },
        ),
    )


def _record_view_udf_parity(
    context: ViewGraphContext,
    *,
    nodes: Sequence[ViewNode],
) -> None:
    profile = context.runtime.runtime_profile
    if profile is None:
        return
    from datafusion_engine.lineage.diagnostics_payloads import view_udf_parity_payload

    payload = view_udf_parity_payload(
        snapshot=context.snapshot,
        view_nodes=nodes,
        ctx=context.ctx,
    )
    from serde_artifact_specs import VIEW_UDF_PARITY_SPEC

    record_artifact(profile, VIEW_UDF_PARITY_SPEC, payload)


def _record_udf_audit(context: ViewGraphContext) -> None:
    profile = context.runtime.runtime_profile
    if profile is None:
        return
    from datafusion_engine.udf.extension_core import udf_audit_payload
    from serde_artifact_specs import UDF_AUDIT_SPEC

    payload = udf_audit_payload(context.snapshot)
    record_artifact(profile, UDF_AUDIT_SPEC, payload)


def _record_udf_catalog(context: ViewGraphContext) -> None:
    runtime_profile = context.runtime.runtime_profile
    if runtime_profile is None:
        return
    from serde_artifact_specs import UDF_CATALOG_SPEC

    introspector = SchemaIntrospector(context.ctx)
    try:
        catalog = introspector.function_catalog_snapshot(include_parameters=True)
    except (RuntimeError, TypeError, ValueError) as exc:
        record_artifact(
            runtime_profile,
            UDF_CATALOG_SPEC,
            {"error": str(exc)},
        )
        return
    record_artifact(
        runtime_profile,
        UDF_CATALOG_SPEC,
        {"functions": catalog},
    )


def _validate_deps(
    ctx: SessionContext,
    node: ViewNode,
    nodes: Sequence[ViewNode],
) -> None:
    known = {candidate.name for candidate in nodes}
    missing: list[str] = []
    for dep in node.deps:
        if dep in known:
            continue
        if not ctx.table_exist(dep):
            missing.append(dep)
    if missing:
        msg = f"Missing dependencies for view {node.name!r}: {sorted(missing)}."
        raise ValueError(msg)


def _validate_udf_calls(snapshot: Mapping[str, object], node: ViewNode) -> None:
    if node.plan_bundle is None:
        msg = f"View {node.name!r} missing plan bundle for UDF validation."
        raise ValueError(msg)
    required_udfs = node.plan_bundle.required_udfs
    if not required_udfs:
        lineage = extract_lineage_from_bundle(node.plan_bundle)
        required_udfs = lineage.required_udfs
    if not required_udfs:
        return
    available = {name.lower() for name in udf_names_from_snapshot(snapshot)}
    required_lower = [name.lower() for name in required_udfs]
    try:
        validate_required_items(
            required_lower,
            available,
            item_label=f"UDFs referenced by view {node.name!r}",
            error_type=ValueError,
        )
    except ValueError:
        missing = [name for name in required_udfs if name.lower() not in available]
        msg = f"View {node.name!r} references non-Rust UDFs: {sorted(missing)}."
        raise ValueError(msg) from None


def _materialize_nodes(
    nodes: Sequence[ViewNode],
    *,
    snapshot: Mapping[str, object],
) -> tuple[ViewNode, ...]:
    resolved: list[ViewNode] = []
    for node in nodes:
        if node.plan_bundle is None:
            msg = f"View {node.name!r} missing plan bundle for lineage extraction."
            raise ValueError(msg)
        deps = _deps_from_plan_bundle(node.plan_bundle)
        required = resolve_required_udfs_from_bundle(node.plan_bundle, snapshot=snapshot)
        normalized_policy = _normalize_cache_policy(node.cache_policy)
        resolved.append(
            replace(
                node,
                deps=deps,
                required_udfs=required,
                cache_policy=normalized_policy,
            )
        )
    return tuple(resolved)


def _normalize_cache_policy(policy: str) -> CachePolicy:
    if policy == "memory":
        return "delta_staging"
    if policy in {"none", "delta_staging", "delta_output"}:
        return cast("CachePolicy", policy)
    return "none"


def _deps_from_plan_bundle(bundle: DataFusionPlanArtifact) -> tuple[str, ...]:
    """Extract dependencies from DataFusion plan bundle (preferred path).

    Parameters
    ----------
    bundle : DataFusionPlanArtifact
        Plan bundle with optimized logical plan.

    Returns:
    -------
    tuple[str, ...]
        Dependency names inferred from the plan bundle.
    """
    lineage = extract_lineage_from_bundle(bundle)
    return lineage.referenced_tables


def _plan_scan_units_for_bundle(
    ctx: SessionContext,
    *,
    bundle: DataFusionPlanArtifact,
    dataset_resolver: ManifestDatasetResolver | None = None,
) -> tuple[ScanUnit, ...]:
    from datafusion_engine.lineage.scheduling import plan_scan_unit

    lineage = extract_lineage_from_bundle(bundle)
    if dataset_resolver is None:
        msg = "dataset_resolver is required for scan unit planning from bundle."
        raise ValueError(msg)
    scan_units: dict[str, ScanUnit] = {}
    for scan in getattr(lineage, "scans", ()):
        location = dataset_resolver.location(scan.dataset_name)
        if location is None:
            continue
        unit = plan_scan_unit(
            ctx,
            dataset_name=scan.dataset_name,
            location=location,
            lineage=scan,
        )
        scan_units[unit.key] = unit
    return tuple(sorted(scan_units.values(), key=lambda unit: unit.key))


def _validate_schema_contract(
    ctx: SessionContext,
    contract: SchemaContract,
    *,
    schema: pa.Schema | None = None,
) -> None:
    introspector = SchemaIntrospector(ctx)
    snapshot = introspector.snapshot
    if snapshot is None:
        msg = "Schema introspection snapshot unavailable for view validation."
        raise ValueError(msg)
    violations = contract.validate_against_introspection(snapshot)
    if schema is not None:
        violations.extend(_schema_metadata_violations(schema, contract))
    if violations:
        raise SchemaContractViolationError(
            table_name=contract.table_name,
            violations=violations,
        )


def _schema_metadata_violations(
    schema: pa.Schema,
    contract: SchemaContract,
) -> list[ValidationViolation]:
    expected = contract.schema_metadata or {}
    if not expected:
        return []
    from datafusion_engine.schema.contracts import SCHEMA_ABI_FINGERPRINT_META

    actual = schema.metadata or {}
    violations: list[ValidationViolation] = []
    expected_abi = expected.get(SCHEMA_ABI_FINGERPRINT_META)
    if expected_abi is not None:
        actual_abi = schema_identity_hash(schema).encode("utf-8")
        if actual_abi != expected_abi:
            violations.append(
                ValidationViolation(
                    violation_type=ViolationType.METADATA_MISMATCH,
                    table_name=contract.table_name,
                    column_name=_metadata_key_label(SCHEMA_ABI_FINGERPRINT_META),
                    expected=_format_metadata_value(expected_abi),
                    actual=_format_metadata_value(actual_abi),
                )
            )
    for key, expected_value in expected.items():
        if key == SCHEMA_ABI_FINGERPRINT_META:
            continue
        actual_value = actual.get(key)
        if actual_value is None:
            continue
        if actual_value == expected_value:
            continue
        violations.append(
            ValidationViolation(
                violation_type=ViolationType.METADATA_MISMATCH,
                table_name=contract.table_name,
                column_name=_metadata_key_label(key),
                expected=_format_metadata_value(expected_value),
                actual=_format_metadata_value(actual_value),
            )
        )
    return violations


def _metadata_key_label(key: bytes) -> str:
    try:
        return key.decode("utf-8")
    except UnicodeDecodeError:
        return key.hex()


def _format_metadata_value(value: bytes | None) -> str | None:
    if value is None:
        return None
    try:
        return value.decode("utf-8")
    except UnicodeDecodeError:
        return value.hex()


def _validate_required_functions(ctx: SessionContext, required: Sequence[str]) -> None:
    if not required:
        return
    introspector = SchemaIntrospector(ctx)
    catalog = introspector.function_catalog_snapshot(include_parameters=False)
    available: set[str] = set()
    for row in catalog:
        name = row.get("function_name") or row.get("routine_name") or row.get("name")
        if isinstance(name, str):
            available.add(name.lower())
    try:
        from datafusion_engine.udf.extension_core import (
            rust_udf_snapshot,
            udf_names_from_snapshot,
        )

        snapshot = rust_udf_snapshot(ctx)
        available.update(name.lower() for name in udf_names_from_snapshot(snapshot))
    except (RuntimeError, TypeError, ValueError):
        pass
    required_lower = [name.lower() for name in required]
    try:
        validate_required_items(
            required_lower,
            available,
            item_label="information_schema functions",
            error_type=ValueError,
        )
    except ValueError:
        missing = [name for name in required if name.lower() not in available]
        msg = f"information_schema missing required functions: {sorted(missing)}."
        raise ValueError(msg) from None


def _topo_sort_nodes(nodes: Sequence[ViewNode]) -> tuple[ViewNode, ...]:
    """Topologically sort view nodes using Kahn's algorithm.

    Parameters
    ----------
    nodes
        View nodes to sort.

    Returns:
    -------
    tuple[ViewNode, ...]
        Topologically sorted view nodes.

    Raises:
    ------
    ValueError
        If a dependency cycle is detected.
    """
    node_map = {node.name: node for node in nodes}
    return _topo_sort_nodes_kahn(node_map, nodes)


def _topo_sort_nodes_kahn(
    node_map: Mapping[str, ViewNode],
    nodes: Sequence[ViewNode],
) -> tuple[ViewNode, ...]:
    indegree: dict[str, int] = dict.fromkeys(node_map, 0)
    adjacency: dict[str, set[str]] = {name: set() for name in node_map}
    for node in nodes:
        for dep in node.deps:
            if dep not in node_map:
                continue
            adjacency[dep].add(node.name)
            indegree[node.name] += 1
    queue = deque(sorted(name for name, degree in indegree.items() if degree == 0))
    ordered: list[ViewNode] = []
    while queue:
        name = queue.popleft()
        ordered.append(node_map[name])
        for neighbor in sorted(adjacency[name]):
            indegree[neighbor] -= 1
            if indegree[neighbor] == 0:
                queue.append(neighbor)
    if len(ordered) != len(node_map):
        remaining = sorted(name for name, degree in indegree.items() if degree > 0)
        msg = f"View dependency cycle detected among: {remaining}."
        raise ValueError(msg)
    return tuple(ordered)


__all__ = [
    "SchemaContractViolationError",
    "ViewGraphOptions",
    "ViewNode",
    "register_view_graph",
    "view_graph_registry",
]
