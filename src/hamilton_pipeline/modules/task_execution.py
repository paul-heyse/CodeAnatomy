"""Hamilton nodes for executing tasks from inferred plans."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, cast

import pyarrow as pa
from hamilton.function_modifiers import inject, resolve_from_config, source
from hamilton.htypes import Collect, Parallelizable

from core_types import JsonDict
from datafusion_engine.arrow.build import empty_table
from datafusion_engine.arrow.interop import (
    RecordBatchReaderLike,
    coerce_table_like,
)
from datafusion_engine.arrow.interop import (
    TableLike as ArrowTableLike,
)
from datafusion_engine.identity import schema_identity_hash
from datafusion_engine.lineage.scan import ScanUnit
from datafusion_engine.plan.execution import (
    PlanExecutionOptions,
    PlanScanOverrides,
)
from datafusion_engine.plan.execution import (
    execute_plan_bundle as execute_plan_bundle_helper,
)
from datafusion_engine.session.facade import ExecutionResult
from datafusion_engine.views.registration import ensure_view_graph
from hamilton_pipeline.tag_policy import TagPolicy, apply_tag
from obs.otel.scopes import SCOPE_EXTRACT
from obs.otel.tracing import stage_span
from relspec.evidence import EvidenceCatalog
from relspec.runtime_artifacts import ExecutionArtifactSpec, RuntimeArtifacts, TableLike

if TYPE_CHECKING:
    from datafusion.dataframe import DataFrame

    from datafusion_engine.plan.bundle import DataFusionPlanBundle
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile, SessionRuntime
    from engine.session import EngineSession
    from extract.extractors.scip.extract import ScipExtractOptions
    from extract.session import ExtractSession
    from hamilton_pipeline.types import RepoScanConfig
    from semantics.incremental.config import SemanticIncrementalConfig
else:
    DataFrame = object
    DataFusionPlanBundle = object
    DataFusionRuntimeProfile = object
    SessionRuntime = object
    ExtractSession = object
    ScipExtractOptions = object
    RepoScanConfig = object
    SemanticIncrementalConfig = object
    try:
        from engine.session import EngineSession
    except ImportError:
        EngineSession = object


@dataclass(frozen=True)
class TaskExecutionInputs:
    """Shared inputs for task execution."""

    runtime: RuntimeArtifacts
    engine_session: EngineSession
    evidence: EvidenceCatalog
    plan_signature: str
    active_task_names: frozenset[str]
    plan_bundles_by_task: Mapping[str, DataFusionPlanBundle]
    scan_units: tuple[ScanUnit, ...]
    scan_keys_by_task: Mapping[str, tuple[str, ...]]
    scan_units_by_task_name: Mapping[str, ScanUnit]
    scan_task_name_by_key: Mapping[str, str]
    scan_unit_results_by_key: Mapping[str, TableLike]
    scan_units_hash: str | None
    repo_scan_config: RepoScanConfig
    incremental_config: SemanticIncrementalConfig | None
    cache_salt: str
    scip_index_path: str | None
    scip_extract_options: ScipExtractOptions


@dataclass(frozen=True)
class TaskExecutionRuntimeConfig:
    """Runtime configuration inputs for task execution."""

    engine_session: EngineSession
    repo_scan_config: RepoScanConfig
    incremental_config: SemanticIncrementalConfig | None
    cache_salt: str
    scip_config: ScipExecutionConfig


@dataclass(frozen=True)
class ScipExecutionConfig:
    """SCIP execution inputs for task execution."""

    scip_index_path: str | None
    scip_extract_options: ScipExtractOptions


@dataclass(frozen=True)
class PlanScanInputs:
    """Scan-unit context for deterministic execution."""

    scan_units: tuple[ScanUnit, ...]
    scan_keys_by_task: Mapping[str, tuple[str, ...]]
    scan_units_by_task_name: Mapping[str, ScanUnit]
    scan_task_name_by_key: Mapping[str, str]
    scan_units_hash: str | None


@dataclass(frozen=True)
class PlanExecutionContext:
    """Bundle execution plan context for task execution."""

    plan_signature: str
    active_task_names: frozenset[str]
    plan_bundles_by_task: Mapping[str, DataFusionPlanBundle]
    plan_scan_inputs: PlanScanInputs


@dataclass(frozen=True)
class TaskExecutionSpec:
    """Describe a task execution request."""

    task_name: str
    task_output: str
    plan_fingerprint: str
    plan_task_signature: str
    task_kind: Literal["view", "scan", "extract"]
    scan_unit_key: str | None = None


def _record_output(
    *,
    inputs: TaskExecutionInputs,
    spec: TaskExecutionSpec,
    plan_signature: str,
    table: TableLike,
) -> None:
    evidence = inputs.evidence
    runtime = inputs.runtime
    schema = table.schema
    evidence.register_schema(spec.task_output, schema)
    if spec.task_output in runtime.execution_artifacts:
        return
    runtime.register_execution(
        spec.task_output,
        ExecutionResult.from_table(table),
        spec=ExecutionArtifactSpec(
            source_task=spec.task_name,
            schema_identity_hash=schema_identity_hash(schema),
            plan_fingerprint=spec.plan_fingerprint,
            plan_task_signature=spec.plan_task_signature,
            plan_signature=plan_signature,
        ),
    )


@apply_tag(
    TagPolicy(
        layer="execution",
        kind="context",
        artifact="runtime_artifacts",
    )
)
def runtime_artifacts(
    engine_session: EngineSession,
    relspec_param_values: JsonDict,
) -> RuntimeArtifacts:
    """Return the runtime artifacts container for task execution.

    Returns
    -------
    RuntimeArtifacts
        Runtime artifacts container.
    """
    return RuntimeArtifacts(
        execution=engine_session.df_runtime(),
        determinism_tier=engine_session.surface_policy.determinism_tier,
        rulepack_param_values=relspec_param_values,
    )


@apply_tag(
    TagPolicy(
        layer="execution",
        kind="context",
        artifact="plan_scan_inputs",
    )
)
def plan_scan_inputs(
    plan_scan_units: tuple[ScanUnit, ...],
    plan_scan_keys_by_task: Mapping[str, tuple[str, ...]],
    plan_scan_units_by_task_name: Mapping[str, ScanUnit],
) -> PlanScanInputs:
    """Bundle scan-unit context for deterministic task execution.

    Returns
    -------
    PlanScanInputs
        Scan-unit context and hash for the execution run.
    """
    mapping_units = tuple(plan_scan_units_by_task_name.values())
    units = (
        plan_scan_units
        if plan_scan_units
        else tuple(sorted(mapping_units, key=lambda unit: unit.key))
    )
    scan_hash: str | None = None
    if units:
        from datafusion_engine.dataset.resolution import scan_units_hash

        scan_hash = scan_units_hash(units)
    scan_task_name_by_key = {unit.key: name for name, unit in plan_scan_units_by_task_name.items()}
    return PlanScanInputs(
        scan_units=units,
        scan_keys_by_task=plan_scan_keys_by_task,
        scan_units_by_task_name=dict(plan_scan_units_by_task_name),
        scan_task_name_by_key=scan_task_name_by_key,
        scan_units_hash=scan_hash,
    )


@apply_tag(
    TagPolicy(
        layer="execution",
        kind="dynamic",
        artifact="scan_unit_stream",
    )
)
def scan_unit_stream(plan_scan_units: tuple[ScanUnit, ...]) -> Parallelizable[ScanUnit]:
    """Yield scan units for dynamic parallel execution.

    Yields
    ------
    ScanUnit
        Scan unit payloads for dynamic execution.
    """
    yield from plan_scan_units


@apply_tag(
    TagPolicy(
        layer="execution",
        kind="dynamic",
        artifact="scan_unit_execution",
    )
)
def scan_unit_execution(
    scan_unit_stream: ScanUnit,
    runtime_artifacts: RuntimeArtifacts,
    plan_scan_inputs: PlanScanInputs,
) -> tuple[str, TableLike]:
    """Execute a single scan unit and return its table keyed by scan unit.

    Returns
    -------
    tuple[str, TableLike]
        Scan unit key and corresponding table.
    """
    scan_task_name = plan_scan_inputs.scan_task_name_by_key.get(
        scan_unit_stream.key,
        scan_unit_stream.key,
    )
    result = _execute_scan_task(
        runtime_artifacts,
        scan_task_name=scan_task_name,
        scan_unit=scan_unit_stream,
        scan_context=plan_scan_inputs,
    )
    return scan_unit_stream.key, result.require_table()


@apply_tag(
    TagPolicy(
        layer="execution",
        kind="mapping",
        artifact="scan_unit_results_by_key",
    )
)
def scan_unit_results_by_key__dynamic(
    scan_unit_execution: Collect[tuple[str, TableLike]],
) -> Mapping[str, TableLike]:
    """Collect scan unit results into a mapping.

    Returns
    -------
    Mapping[str, TableLike]
        Mapping of scan unit keys to executed tables.
    """
    items = sorted(scan_unit_execution, key=lambda item: item[0])
    return dict(items)


@apply_tag(
    TagPolicy(
        layer="execution",
        kind="mapping",
        artifact="scan_unit_results_by_key",
    )
)
def scan_unit_results_by_key__static() -> Mapping[str, TableLike]:
    """Return an empty mapping when dynamic scan units are disabled.

    Returns
    -------
    Mapping[str, TableLike]
        Empty mapping placeholder.
    """
    return {}


@resolve_from_config(
    decorate_with=lambda enable_dynamic_scan_units=True: inject(
        scan_unit_results_by_key=source(
            "scan_unit_results_by_key__dynamic"
            if enable_dynamic_scan_units
            else "scan_unit_results_by_key__static"
        )
    ),
)
@apply_tag(
    TagPolicy(
        layer="execution",
        kind="mapping",
        artifact="scan_unit_results_by_key",
    )
)
def scan_unit_results_by_key(
    scan_unit_results_by_key: Mapping[str, TableLike],
) -> Mapping[str, TableLike]:
    """Resolve scan unit results based on the dynamic execution config.

    Returns
    -------
    Mapping[str, TableLike]
        Resolved scan unit results mapping.
    """
    return scan_unit_results_by_key


@apply_tag(
    TagPolicy(
        layer="execution",
        kind="context",
        artifact="task_execution_inputs",
    )
)
def task_execution_inputs(
    runtime_artifacts: RuntimeArtifacts,
    evidence_catalog: EvidenceCatalog,
    plan_context: PlanExecutionContext,
    scan_unit_results_by_key: Mapping[str, TableLike],
    task_execution_runtime_config: TaskExecutionRuntimeConfig,
) -> TaskExecutionInputs:
    """Bundle shared execution inputs for per-task nodes.

    Parameters
    ----------
    runtime_artifacts
        Runtime artifact container for the execution run.
    evidence_catalog
        Evidence catalog used for scheduling and contract validation.
    plan_context
        Execution-plan context for signatures, tasks, and scan inputs.
    scan_unit_results_by_key
        Mapping of scan unit keys to their executed tables.
    task_execution_runtime_config
        Runtime configuration inputs for task execution.

    Returns
    -------
    TaskExecutionInputs
        Bundled inputs for task execution.
    """
    return TaskExecutionInputs(
        runtime=runtime_artifacts,
        engine_session=task_execution_runtime_config.engine_session,
        evidence=evidence_catalog,
        plan_signature=plan_context.plan_signature,
        active_task_names=plan_context.active_task_names,
        plan_bundles_by_task=plan_context.plan_bundles_by_task,
        scan_units=plan_context.plan_scan_inputs.scan_units,
        scan_keys_by_task=plan_context.plan_scan_inputs.scan_keys_by_task,
        scan_units_by_task_name=plan_context.plan_scan_inputs.scan_units_by_task_name,
        scan_task_name_by_key=plan_context.plan_scan_inputs.scan_task_name_by_key,
        scan_unit_results_by_key=scan_unit_results_by_key,
        scan_units_hash=plan_context.plan_scan_inputs.scan_units_hash,
        repo_scan_config=task_execution_runtime_config.repo_scan_config,
        incremental_config=task_execution_runtime_config.incremental_config,
        cache_salt=task_execution_runtime_config.cache_salt,
        scip_index_path=task_execution_runtime_config.scip_config.scip_index_path,
        scip_extract_options=task_execution_runtime_config.scip_config.scip_extract_options,
    )


@apply_tag(
    TagPolicy(
        layer="execution",
        kind="context",
        artifact="task_execution_runtime_config",
    )
)
def task_execution_runtime_config(
    engine_session: EngineSession,
    repo_scan_config: RepoScanConfig,
    incremental_config: SemanticIncrementalConfig | None,
    cache_salt: str,
    scip_execution_config: ScipExecutionConfig,
) -> TaskExecutionRuntimeConfig:
    """Bundle runtime configuration inputs for task execution.

    Parameters
    ----------
    engine_session
        Engine session for execution surfaces.
    repo_scan_config
        Repository scan configuration.
    incremental_config
        Incremental configuration (when enabled).
    cache_salt
        Cache salt for deterministic repo identifiers.
    scip_execution_config
        SCIP execution inputs (index path + extract options).

    Returns
    -------
    TaskExecutionRuntimeConfig
        Bundled runtime configuration inputs.
    """
    return TaskExecutionRuntimeConfig(
        engine_session=engine_session,
        repo_scan_config=repo_scan_config,
        incremental_config=incremental_config,
        cache_salt=cache_salt,
        scip_config=scip_execution_config,
    )


@apply_tag(
    TagPolicy(
        layer="execution",
        kind="context",
        artifact="scip_execution_config",
    )
)
def scip_execution_config(
    scip_index_path: str | None,
    scip_extract_options: ScipExtractOptions,
) -> ScipExecutionConfig:
    """Bundle SCIP execution inputs.

    Parameters
    ----------
    scip_index_path
        Optional path to a SCIP index file.
    scip_extract_options
        Options for SCIP extraction.

    Returns
    -------
    ScipExecutionConfig
        Bundled SCIP execution inputs.
    """
    return ScipExecutionConfig(
        scip_index_path=scip_index_path,
        scip_extract_options=scip_extract_options,
    )


def _ensure_scan_overrides(
    runtime: RuntimeArtifacts,
    *,
    scan_context: PlanScanInputs,
) -> DataFusionRuntimeProfile:
    session_runtime = runtime.execution
    if session_runtime is None:
        msg = "RuntimeArtifacts.execution must be configured for view execution."
        raise ValueError(msg)
    profile = session_runtime.profile
    session = session_runtime.ctx
    refresh_requested = (
        scan_context.scan_units_hash is not None
        and runtime.scan_override_hash != scan_context.scan_units_hash
    )
    if refresh_requested:
        from semantics.ir_pipeline import build_semantic_ir

        ensure_view_graph(
            session,
            runtime_profile=profile,
            scan_units=scan_context.scan_units,
            semantic_ir=build_semantic_ir(),
        )
        runtime.scan_override_hash = scan_context.scan_units_hash
    return profile


def _execute_view(
    runtime: RuntimeArtifacts,
    *,
    view_name: str,
    plan_bundle: DataFusionPlanBundle | None,
    scan_context: PlanScanInputs,
) -> ExecutionResult:
    if plan_bundle is None:
        msg = f"Plan bundle is required for view execution: {view_name!r}."
        raise ValueError(msg)
    profile = _ensure_scan_overrides(runtime, scan_context=scan_context)
    session = profile.session_context()
    if not session.table_exist(view_name):
        from semantics.ir_pipeline import build_semantic_ir

        ensure_view_graph(
            session,
            runtime_profile=profile,
            scan_units=scan_context.scan_units,
            semantic_ir=build_semantic_ir(),
        )
        if not session.table_exist(view_name):
            msg = f"View {view_name!r} is not registered; call ensure_view_graph first."
            raise ValueError(msg)
    execution = execute_plan_bundle_helper(
        session,
        plan_bundle,
        options=PlanExecutionOptions(
            runtime_profile=profile,
            view_name=view_name,
            scan=PlanScanOverrides(
                scan_units=scan_context.scan_units,
                scan_keys=scan_context.scan_keys_by_task.get(view_name, ()),
                apply_scan_overrides=False,
            ),
        ),
    )
    execution_result = execution.execution_result
    dataframe = execution_result.require_dataframe()
    table = dataframe.to_arrow_table()
    return ExecutionResult.from_table(table)


def _execute_scan_task(
    _runtime: RuntimeArtifacts,
    *,
    scan_task_name: str,
    scan_unit: ScanUnit,
    scan_context: PlanScanInputs,
) -> ExecutionResult:
    metadata: dict[bytes, bytes] = {
        b"scan_task_name": scan_task_name.encode("utf-8"),
        b"scan_unit_key": scan_unit.key.encode("utf-8"),
        b"scan_dataset_name": scan_unit.dataset_name.encode("utf-8"),
    }
    if scan_unit.delta_version is not None:
        metadata[b"scan_delta_version"] = str(scan_unit.delta_version).encode("utf-8")
    if scan_context.scan_units_hash is not None:
        metadata[b"scan_units_hash"] = scan_context.scan_units_hash.encode("utf-8")
    schema = pa.schema([], metadata=metadata)
    table = empty_table(schema)
    return ExecutionResult.from_table(table)


def _resolve_extract_input_table(
    inputs: TaskExecutionInputs,
    name: str,
) -> TableLike | None:
    runtime = inputs.runtime
    artifact = runtime.execution_artifacts.get(name)
    table: TableLike | None = None
    if artifact is not None and artifact.result.table is not None:
        table = artifact.result.table
    else:
        table = runtime.materialized_tables.get(name)
    if table is not None:
        return table
    session_runtime = runtime.execution
    if session_runtime is None:
        return None
    df = _resolve_dataframe_from_session(session_runtime, name)
    if df is None:
        return None
    try:
        table = cast("TableLike", df.to_arrow_table())
    except (AttributeError, RuntimeError, TypeError, ValueError):
        return None
    return table


def _resolve_dataframe_from_session(
    session_runtime: SessionRuntime,
    name: str,
) -> DataFrame | None:
    ctx = session_runtime.ctx
    try:
        return ctx.table(name)
    except (KeyError, RuntimeError, TypeError, ValueError):
        location = session_runtime.profile.catalog_ops.dataset_location(name)
        if location is None:
            return None
        from datafusion_engine.session.facade import DataFusionExecutionFacade

        facade = DataFusionExecutionFacade(ctx=ctx, runtime_profile=session_runtime.profile)
        try:
            facade.register_dataset(name=name, location=location)
        except (KeyError, RuntimeError, TypeError, ValueError):
            return None
        try:
            return ctx.table(name)
        except (KeyError, RuntimeError, TypeError, ValueError):
            return None


def _require_extract_input_table(
    inputs: TaskExecutionInputs,
    name: str,
) -> TableLike:
    table = _resolve_extract_input_table(inputs, name)
    if table is None:
        msg = f"Missing extract input table {name!r}."
        raise ValueError(msg)
    return table


def _coerce_extract_output(value: object) -> ArrowTableLike:
    if isinstance(value, RecordBatchReaderLike):
        return value.read_all()
    if isinstance(value, ArrowTableLike):
        return value
    to_arrow = getattr(value, "to_arrow_table", None)
    if callable(to_arrow):
        return cast("ArrowTableLike", to_arrow())
    coerced = coerce_table_like(value)
    if isinstance(coerced, RecordBatchReaderLike):
        return coerced.read_all()
    return coerced


def _resolve_extract_dataset_name(name: str) -> str | None:
    from datafusion_engine.extract.bundles import dataset_name_for_output

    try:
        dataset = dataset_name_for_output(name)
    except KeyError:
        return name
    return dataset


def _normalize_extract_outputs(
    outputs: Mapping[str, object],
) -> dict[str, TableLike]:
    normalized: dict[str, TableLike] = {}
    for name, value in outputs.items():
        dataset_name = _resolve_extract_dataset_name(name)
        if dataset_name is None:
            continue
        normalized[dataset_name] = _coerce_extract_output(value)
    return normalized


def _resolve_repo_files_table(inputs: TaskExecutionInputs) -> TableLike:
    repo_files_name = _resolve_extract_dataset_name("repo_files") or "repo_files"
    return _require_extract_input_table(inputs, repo_files_name)


def _extract_repo_scan(
    inputs: TaskExecutionInputs,
    extract_session: ExtractSession,
    profile_name: str,
) -> Mapping[str, object]:
    from extract.helpers import ExtractExecutionContext
    from extract.scanning.repo_scan import scan_repo_tables
    from hamilton_pipeline.io_contracts import _repo_scan_options

    options = _repo_scan_options(
        inputs.repo_scan_config,
        incremental=inputs.incremental_config,
        cache_salt=inputs.cache_salt,
    )
    exec_ctx = ExtractExecutionContext(session=extract_session, profile=profile_name)
    with stage_span(
        "extract.repo_scan",
        stage="extract.repo_scan",
        scope_name=SCOPE_EXTRACT,
        attributes={
            "codeanatomy.repo_root": inputs.repo_scan_config.repo_root,
            "codeanatomy.max_files": inputs.repo_scan_config.max_files,
            "codeanatomy.changed_only": inputs.repo_scan_config.changed_only,
        },
    ):
        return scan_repo_tables(
            inputs.repo_scan_config.repo_root,
            options=options,
            context=exec_ctx,
            prefer_reader=False,
        )


def _extract_scip(
    inputs: TaskExecutionInputs,
    extract_session: ExtractSession,
    profile_name: str,
) -> Mapping[str, object]:
    from extract.extractors.scip.extract import ScipExtractContext, extract_scip_tables

    context = ScipExtractContext(
        scip_index_path=inputs.scip_index_path,
        repo_root=inputs.repo_scan_config.repo_root,
        session=extract_session,
        profile=profile_name,
    )
    return extract_scip_tables(
        context=context,
        options=inputs.scip_extract_options,
        prefer_reader=False,
    )


def _extract_python_imports(
    inputs: TaskExecutionInputs,
    extract_session: ExtractSession,
    profile_name: str,
) -> Mapping[str, object]:
    from extract.extractors.imports_extract import extract_python_imports_tables

    return extract_python_imports_tables(
        ast_imports=_resolve_extract_input_table(inputs, "ast_imports"),
        cst_imports=_resolve_extract_input_table(inputs, "cst_imports"),
        ts_imports=_resolve_extract_input_table(inputs, "ts_imports"),
        session=extract_session,
        profile=profile_name,
        prefer_reader=False,
    )


def _extract_python_external(
    inputs: TaskExecutionInputs,
    extract_session: ExtractSession,
    profile_name: str,
) -> Mapping[str, object]:
    from extract.extractors.external_scope import extract_python_external_tables

    python_imports = _require_extract_input_table(inputs, "python_imports")
    return extract_python_external_tables(
        python_imports=python_imports,
        repo_root=inputs.repo_scan_config.repo_root,
        session=extract_session,
        profile=profile_name,
        prefer_reader=False,
    )


def _extract_ast(
    inputs: TaskExecutionInputs,
    extract_session: ExtractSession,
    profile_name: str,
) -> Mapping[str, object]:
    from extract.extractors.ast_extract import extract_ast_tables

    repo_files = _resolve_repo_files_table(inputs)
    return extract_ast_tables(
        repo_files=repo_files,
        session=extract_session,
        profile=profile_name,
        prefer_reader=False,
    )


def _extract_cst(
    inputs: TaskExecutionInputs,
    extract_session: ExtractSession,
    profile_name: str,
) -> Mapping[str, object]:
    from extract.extractors.cst_extract import extract_cst_tables

    repo_files = _resolve_repo_files_table(inputs)
    return extract_cst_tables(
        repo_files=repo_files,
        session=extract_session,
        profile=profile_name,
        prefer_reader=False,
    )


def _extract_tree_sitter(
    inputs: TaskExecutionInputs,
    extract_session: ExtractSession,
    profile_name: str,
) -> Mapping[str, object]:
    from extract.extractors.tree_sitter.extract import extract_ts_tables

    repo_files = _resolve_repo_files_table(inputs)
    return extract_ts_tables(
        repo_files=repo_files,
        session=extract_session,
        profile=profile_name,
        prefer_reader=False,
    )


def _extract_bytecode(
    inputs: TaskExecutionInputs,
    extract_session: ExtractSession,
    profile_name: str,
) -> Mapping[str, object]:
    from extract.extractors.bytecode_extract import extract_bytecode_table

    repo_files = _resolve_repo_files_table(inputs)
    table = extract_bytecode_table(
        repo_files=repo_files,
        session=extract_session,
        profile=profile_name,
        prefer_reader=False,
    )
    return {"bytecode_files": table}


def _extract_symtable(
    inputs: TaskExecutionInputs,
    extract_session: ExtractSession,
    profile_name: str,
) -> Mapping[str, object]:
    from extract.extractors.symtable_extract import extract_symtables_table

    repo_files = _resolve_repo_files_table(inputs)
    table = extract_symtables_table(
        repo_files=repo_files,
        session=extract_session,
        profile=profile_name,
        prefer_reader=False,
    )
    return {"symtable_files": table}


def _extract_outputs_for_template(
    inputs: TaskExecutionInputs,
    *,
    template: str,
    extract_session: ExtractSession,
    profile_name: str,
) -> Mapping[str, object]:
    handlers: dict[
        str, Callable[[TaskExecutionInputs, ExtractSession, str], Mapping[str, object]]
    ] = {
        "repo_scan": _extract_repo_scan,
        "scip": _extract_scip,
        "python_imports": _extract_python_imports,
        "python_external": _extract_python_external,
        "ast": _extract_ast,
        "cst": _extract_cst,
        "tree_sitter": _extract_tree_sitter,
        "bytecode": _extract_bytecode,
        "symtable": _extract_symtable,
    }
    handler = handlers.get(template)
    if handler is None:
        msg = f"Unsupported extract template {template!r}."
        raise ValueError(msg)
    return handler(inputs, extract_session, profile_name)


def _ensure_extract_output(
    *,
    inputs: TaskExecutionInputs,
    spec: TaskExecutionSpec,
    normalized: dict[str, TableLike],
) -> None:
    fallback = normalized.get(spec.task_output)
    if fallback is None:
        table = _resolve_extract_input_table(inputs, spec.task_output)
        if table is not None:
            normalized[spec.task_output] = table
    if spec.task_output not in normalized:
        from datafusion_engine.extract.registry import dataset_schema
        from engine.diagnostics import EngineEventRecorder, ExtractQualityEvent

        issue = f"Extract task {spec.task_name!r} produced no output for {spec.task_output!r}."
        schema = None
        try:
            schema = dataset_schema(spec.task_output)
        except (KeyError, TypeError, ValueError):
            schema = pa.schema([])
        normalized[spec.task_output] = empty_table(schema)
        location = inputs.engine_session.datafusion_profile.catalog_ops.dataset_location(spec.task_output)
        recorder = EngineEventRecorder(inputs.engine_session.datafusion_profile)
        recorder.record_extract_quality_events(
            [
                ExtractQualityEvent(
                    dataset=spec.task_output,
                    stage="task",
                    status="missing_output",
                    rows=0,
                    location_path=str(location.path) if location is not None else None,
                    location_format=location.format if location is not None else None,
                    issue=issue,
                    plan_fingerprint=spec.plan_fingerprint,
                    plan_signature=inputs.plan_signature,
                )
            ]
        )


def _execute_extract_task(
    inputs: TaskExecutionInputs,
    *,
    spec: TaskExecutionSpec,
) -> dict[str, TableLike]:
    runtime = inputs.runtime
    session_runtime = runtime.execution
    if session_runtime is None:
        msg = "RuntimeArtifacts.execution must be configured for extract execution."
        raise ValueError(msg)
    from extract.session import ExtractSession
    from relspec.extract_plan import extract_output_task_map

    task_map = extract_output_task_map()
    task_spec = task_map.get(spec.task_output)
    if task_spec is None:
        msg = f"Unknown extract task output {spec.task_output!r}."
        raise ValueError(msg)
    extract_session = ExtractSession(engine_session=inputs.engine_session)
    profile_name = inputs.engine_session.datafusion_profile.policies.config_policy_name or "default"
    try:
        outputs = _extract_outputs_for_template(
            inputs,
            template=task_spec.extractor,
            extract_session=extract_session,
            profile_name=profile_name,
        )
        normalized = _normalize_extract_outputs(outputs)
    except (KeyError, OSError, RuntimeError, TypeError, ValueError) as exc:
        from datafusion_engine.extract.registry import dataset_schema
        from engine.diagnostics import EngineEventRecorder, ExtractQualityEvent

        issue = f"{type(exc).__name__}: {exc}"
        schema = None
        try:
            schema = dataset_schema(spec.task_output)
        except (KeyError, TypeError, ValueError):
            schema = pa.schema([])
        normalized = {spec.task_output: empty_table(schema)}
        location = inputs.engine_session.datafusion_profile.catalog_ops.dataset_location(spec.task_output)
        recorder = EngineEventRecorder(inputs.engine_session.datafusion_profile)
        recorder.record_extract_quality_events(
            [
                ExtractQualityEvent(
                    dataset=spec.task_output,
                    stage="extract",
                    status="extract_failed",
                    rows=None,
                    location_path=str(location.path) if location is not None else None,
                    location_format=location.format if location is not None else None,
                    issue=issue,
                    extractor=task_spec.extractor,
                    plan_fingerprint=spec.plan_fingerprint,
                    plan_signature=inputs.plan_signature,
                )
            ]
        )
    _ensure_extract_output(inputs=inputs, spec=spec, normalized=normalized)
    return normalized


def execute_task_from_catalog(
    *,
    inputs: TaskExecutionInputs,
    dependencies: Sequence[TableLike],
    plan_signature: str,
    spec: TaskExecutionSpec,
) -> TableLike:
    """Execute a single view task from a catalog.

    Returns
    -------
    TableLike
        Materialized table output.

    Raises
    ------
    ValueError
        Raised when the plan signature is inconsistent or the view is missing.
    """
    _touch_dependencies(dependencies)
    runtime = inputs.runtime
    evidence = inputs.evidence
    if plan_signature != inputs.plan_signature:
        msg = "Plan signature mismatch between injected task inputs."
        raise ValueError(msg)
    existing_artifact = runtime.execution_artifacts.get(spec.task_output)
    if existing_artifact is not None and existing_artifact.result.table is not None:
        existing = existing_artifact.result.table
    else:
        existing = runtime.materialized_tables.get(spec.task_output)
    if existing is not None:
        if spec.task_output not in evidence.sources:
            evidence.register_schema(spec.task_output, existing.schema)
        return existing
    inactive = spec.task_name not in inputs.active_task_names
    if inactive:
        runtime.record_execution(f"{spec.task_name}:skipped")
        msg = f"Task {spec.task_name!r} is inactive under the current incremental plan."
        raise ValueError(msg)
    if spec.task_kind == "scan":
        scan_unit = inputs.scan_units_by_task_name.get(spec.task_name)
        if scan_unit is None:
            msg = f"Missing scan unit for scan task {spec.task_name!r}."
            raise ValueError(msg)
    return _execute_and_record(
        inputs=inputs,
        spec=spec,
        plan_signature=plan_signature,
        inactive=inactive,
    )


def _touch_dependencies(dependencies: Sequence[TableLike]) -> int:
    return len(dependencies)


def _execute_and_record(
    *,
    inputs: TaskExecutionInputs,
    spec: TaskExecutionSpec,
    plan_signature: str,
    inactive: bool,
) -> TableLike:
    runtime = inputs.runtime
    task_name = spec.task_name
    runtime.record_execution(task_name if not inactive else f"{task_name}:inactive")
    scan_context = PlanScanInputs(
        scan_units=inputs.scan_units,
        scan_keys_by_task=inputs.scan_keys_by_task,
        scan_units_by_task_name=inputs.scan_units_by_task_name,
        scan_task_name_by_key=inputs.scan_task_name_by_key,
        scan_units_hash=inputs.scan_units_hash,
    )
    if spec.task_kind == "scan":
        scan_unit = inputs.scan_units_by_task_name.get(spec.task_name)
        if scan_unit is None:
            msg = f"Scan task {spec.task_name!r} is missing a scan unit mapping."
            raise ValueError(msg)
        precomputed = inputs.scan_unit_results_by_key.get(scan_unit.key)
        if precomputed is not None:
            _record_output(
                inputs=inputs,
                spec=spec,
                plan_signature=plan_signature,
                table=precomputed,
            )
            return precomputed
        result = _execute_scan_task(
            runtime,
            scan_task_name=spec.task_name,
            scan_unit=scan_unit,
            scan_context=scan_context,
        )
    elif spec.task_kind == "extract":
        outputs = _execute_extract_task(inputs, spec=spec)
        _record_extract_outputs(
            inputs=inputs,
            outputs=outputs,
            plan_signature=plan_signature,
            default_spec=spec,
        )
        return outputs[spec.task_output]
    else:
        plan_bundle = inputs.plan_bundles_by_task.get(spec.task_output)
        result = _execute_view(
            runtime,
            view_name=spec.task_output,
            plan_bundle=plan_bundle,
            scan_context=scan_context,
        )
    table = result.require_table()
    _record_output(
        inputs=inputs,
        spec=spec,
        plan_signature=plan_signature,
        table=table,
    )
    return table


def _record_extract_outputs(
    *,
    inputs: TaskExecutionInputs,
    outputs: Mapping[str, TableLike],
    plan_signature: str,
    default_spec: TaskExecutionSpec,
) -> None:
    from relspec.extract_plan import extract_output_task_map

    task_map = extract_output_task_map()
    from engine.diagnostics import EngineEventRecorder, ExtractQualityEvent

    recorder = EngineEventRecorder(inputs.engine_session.datafusion_profile)
    quality_events: list[ExtractQualityEvent] = []
    for name, table in outputs.items():
        if name not in inputs.active_task_names:
            continue
        mapped = task_map.get(name)
        plan_fingerprint = (
            mapped.plan_fingerprint if mapped is not None else default_spec.plan_fingerprint
        )
        plan_task_signature = plan_fingerprint
        output_spec = TaskExecutionSpec(
            task_name=name,
            task_output=name,
            plan_fingerprint=plan_fingerprint,
            plan_task_signature=plan_task_signature,
            task_kind="extract",
        )
        rows = None
        if hasattr(table, "num_rows"):
            rows = int(table.num_rows)
        location = inputs.engine_session.datafusion_profile.catalog_ops.dataset_location(name)
        status = "ok"
        issue = None
        if location is None:
            status = "missing_location"
            issue = "No extract dataset location configured."
        elif rows == 0:
            status = "empty_output"
        quality_events.append(
            ExtractQualityEvent(
                dataset=name,
                stage="task",
                status=status,
                rows=rows,
                location_path=str(location.path) if location is not None else None,
                location_format=location.format if location is not None else None,
                issue=issue,
                extractor=mapped.extractor if mapped is not None else None,
                plan_fingerprint=plan_fingerprint,
                plan_signature=plan_signature,
            )
        )
        _record_output(
            inputs=inputs,
            spec=output_spec,
            plan_signature=plan_signature,
            table=table,
        )
    recorder.record_extract_quality_events(quality_events)


__all__ = [
    "PlanExecutionContext",
    "PlanScanInputs",
    "ScipExecutionConfig",
    "TaskExecutionInputs",
    "TaskExecutionRuntimeConfig",
    "TaskExecutionSpec",
    "execute_task_from_catalog",
    "plan_scan_inputs",
    "runtime_artifacts",
    "scip_execution_config",
    "task_execution_inputs",
    "task_execution_runtime_config",
]
