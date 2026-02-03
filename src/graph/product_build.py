"""Canonical graph product build entrypoints.

This is the public API that callers should use. It intentionally hides Hamilton output node
names and returns a typed result with stable fields.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Literal, cast

from core_types import DeterminismTier, JsonDict, JsonValue, PathLike, ensure_path
from cpg.schemas import SCHEMA_VERSION
from datafusion_engine.materialize_policy import WriterStrategy
from hamilton_pipeline import PipelineExecutionOptions, execute_pipeline
from hamilton_pipeline.execution import ImpactStrategy
from hamilton_pipeline.types import (
    ExecutionMode,
    ExecutorConfig,
    GraphAdapterConfig,
    ScipIdentityOverrides,
    ScipIndexConfig,
)
from obs.otel import OtelBootstrapOptions, configure_otel
from obs.otel.run_context import reset_run_id, set_run_id
from obs.otel.tracing import record_exception, root_span, set_span_attributes
from semantics.incremental import IncrementalConfig
from utils.uuid_factory import uuid7_str

GraphProduct = Literal["cpg"]


@dataclass(frozen=True)
class FinalizeDeltaPaths:
    """Paths returned by the finalize Delta write helper."""

    data: Path
    errors: Path
    stats: Path
    alignment: Path


@dataclass(frozen=True)
class FinalizeDeltaReport:
    """Finalize Delta output plus row counts."""

    paths: FinalizeDeltaPaths
    rows: int
    error_rows: int


@dataclass(frozen=True)
class TableDeltaReport:
    """Single table Delta output plus row count."""

    path: Path
    rows: int


@dataclass(frozen=True)
class GraphProductBuildResult:
    """Stable result for a graph product build."""

    product: GraphProduct
    product_version: str
    engine_versions: Mapping[str, str]
    run_id: str | None
    repo_root: Path
    output_dir: Path

    cpg_nodes: FinalizeDeltaReport
    cpg_edges: FinalizeDeltaReport
    cpg_props: FinalizeDeltaReport
    cpg_props_map: TableDeltaReport
    cpg_edges_by_src: TableDeltaReport
    cpg_edges_by_dst: TableDeltaReport

    extract_error_artifacts: JsonDict | None = None
    manifest_path: Path | None = None
    run_bundle_dir: Path | None = None

    pipeline_outputs: Mapping[str, JsonDict | None] = field(default_factory=dict)


@dataclass(frozen=True)
class GraphProductBuildRequest:
    """Specify inputs and options for a graph product build."""

    repo_root: PathLike
    product: GraphProduct = "cpg"
    execution_mode: ExecutionMode = ExecutionMode.PLAN_PARALLEL
    executor_config: ExecutorConfig | None = None
    graph_adapter_config: GraphAdapterConfig | None = None

    output_dir: PathLike | None = None
    work_dir: PathLike | None = None

    runtime_profile_name: str | None = None
    determinism_override: DeterminismTier | None = None

    scip_index_config: ScipIndexConfig | None = None
    scip_identity_overrides: ScipIdentityOverrides | None = None

    writer_strategy: WriterStrategy | None = None

    incremental_config: IncrementalConfig | None = None
    incremental_impact_strategy: ImpactStrategy | None = None

    include_extract_errors: bool = True
    include_manifest: bool = True
    include_run_bundle: bool = True

    config: Mapping[str, JsonValue] = field(default_factory=dict)
    overrides: Mapping[str, object] | None = None
    otel_options: OtelBootstrapOptions | None = None
    use_materialize: bool = True


def build_graph_product(request: GraphProductBuildRequest) -> GraphProductBuildResult:
    """Build the requested graph product and return typed outputs.

    Returns
    -------
    GraphProductBuildResult
        Typed outputs for the requested graph product.
    """
    repo_root_path = ensure_path(request.repo_root).resolve()
    _resolve_output_dir(repo_root_path, request.output_dir)

    overrides: dict[str, object] = dict(request.overrides or {})
    if request.otel_options is not None:
        overrides["otel_options"] = request.otel_options
    if request.runtime_profile_name is not None:
        overrides["runtime_profile_name"] = request.runtime_profile_name
    if request.determinism_override is not None:
        overrides["determinism_override"] = request.determinism_override
    if request.writer_strategy is not None:
        overrides["writer_strategy"] = request.writer_strategy
    run_id = overrides.get("run_id")
    if not isinstance(run_id, str) or not run_id:
        run_id = uuid7_str()
        overrides["run_id"] = run_id

    outputs = _outputs_for_request(request)
    options = PipelineExecutionOptions(
        output_dir=request.output_dir,
        work_dir=request.work_dir,
        scip_index_config=request.scip_index_config,
        scip_identity_overrides=request.scip_identity_overrides,
        incremental_config=request.incremental_config,
        incremental_impact_strategy=request.incremental_impact_strategy,
        execution_mode=request.execution_mode,
        executor_config=request.executor_config,
        graph_adapter_config=request.graph_adapter_config,
        outputs=outputs,
        config=request.config,
        overrides=overrides or None,
        use_materialize=request.use_materialize,
    )

    effective_otel = request.otel_options or OtelBootstrapOptions()
    resource_overrides = dict(effective_otel.resource_overrides or {})
    resource_overrides.update(_otel_resource_overrides(repo_root_path))
    configure_otel(
        service_name="codeanatomy",
        options=replace(effective_otel, resource_overrides=resource_overrides),
    )
    run_token = set_run_id(run_id)
    try:
        with root_span(
            "graph_product.build",
            attributes={
                "codeanatomy.product": request.product,
                "codeanatomy.execution_mode": request.execution_mode.value,
                "codeanatomy.outputs": list(outputs),
            },
        ) as span:
            try:
                raw = execute_pipeline(repo_root=repo_root_path, options=options)
            except Exception as exc:
                record_exception(span, exc)
                raise
            result = _parse_result(
                request=request,
                repo_root=repo_root_path,
                pipeline_outputs=raw,
            )
            _record_build_output_locations(result)
            set_span_attributes(
                span,
                {
                    "codeanatomy.product_version": result.product_version,
                    "codeanatomy.output_dir": str(result.output_dir),
                },
            )
            return result
    finally:
        reset_run_id(run_token)


def _outputs_for_request(request: GraphProductBuildRequest) -> Sequence[str]:
    outputs: list[str] = [
        "write_cpg_nodes_delta",
        "write_cpg_edges_delta",
        "write_cpg_props_delta",
        "write_cpg_props_map_delta",
        "write_cpg_edges_by_src_delta",
        "write_cpg_edges_by_dst_delta",
    ]
    if request.include_extract_errors:
        outputs.append("write_extract_error_artifacts_delta")
    if request.include_manifest:
        outputs.append("write_run_manifest_delta")
    if request.include_run_bundle:
        outputs.append("write_run_bundle_dir")
    return outputs


def _resolve_output_dir(repo_root: Path, output_dir: PathLike | None) -> Path:
    """Resolve the output directory for registry exports.

    Returns
    -------
    Path
        Absolute output directory path.
    """
    if output_dir is None:
        return repo_root / "build"
    if isinstance(output_dir, str) and not output_dir:
        return repo_root / "build"
    resolved = ensure_path(output_dir)
    return resolved if resolved.is_absolute() else repo_root / resolved


def _require(outputs: Mapping[str, JsonDict | None], key: str) -> JsonDict:
    value = outputs.get(key)
    if value is None:
        msg = f"Missing required pipeline output {key!r}."
        raise ValueError(msg)
    return value


def _optional(outputs: Mapping[str, JsonDict | None], key: str) -> JsonDict | None:
    return outputs.get(key)


def _int_field(report: JsonDict, key: str) -> int:
    value = report.get(key)
    if isinstance(value, bool):
        return 0
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return 0
    return 0


def _parse_finalize(report: JsonDict) -> FinalizeDeltaReport:
    paths = cast("dict[str, str]", report.get("paths") or {})
    return FinalizeDeltaReport(
        paths=FinalizeDeltaPaths(
            data=Path(paths["data"]),
            errors=Path(paths["errors"]),
            stats=Path(paths["stats"]),
            alignment=Path(paths["alignment"]),
        ),
        rows=_int_field(report, "rows"),
        error_rows=_int_field(report, "error_rows"),
    )


def _parse_table(report: JsonDict) -> TableDeltaReport:
    return TableDeltaReport(
        path=Path(cast("str", report["path"])),
        rows=_int_field(report, "rows"),
    )


def _parse_manifest_path(
    pipeline_outputs: Mapping[str, JsonDict | None],
    *,
    include_manifest: bool,
) -> Path | None:
    if not include_manifest:
        return None
    manifest = _optional(pipeline_outputs, "write_run_manifest_delta")
    if manifest is None or not manifest.get("path"):
        return None
    return Path(cast("str", manifest["path"]))


def _parse_manifest_details(
    pipeline_outputs: Mapping[str, JsonDict | None],
    *,
    include_manifest: bool,
) -> tuple[Path | None, str | None]:
    if not include_manifest:
        return None, None
    manifest = _optional(pipeline_outputs, "write_run_manifest_delta")
    if manifest is None:
        return None, None
    manifest_path = None
    if manifest.get("path"):
        manifest_path = Path(cast("str", manifest["path"]))
    manifest_run_id = None
    if manifest.get("manifest"):
        manifest_payload = cast("dict[str, object]", manifest["manifest"])
        manifest_run_id = cast("str | None", manifest_payload.get("run_id"))
    return manifest_path, manifest_run_id


def _parse_run_bundle(
    pipeline_outputs: Mapping[str, JsonDict | None],
    *,
    include_run_bundle: bool,
) -> tuple[Path | None, str | None]:
    if not include_run_bundle:
        return None, None
    bundle = _optional(pipeline_outputs, "write_run_bundle_dir")
    if bundle is None:
        return None, None
    run_bundle_dir = None
    run_id = None
    bundle_dir = bundle.get("bundle_dir")
    if bundle_dir:
        run_bundle_dir = Path(cast("str", bundle_dir))
    bundle_run_id = bundle.get("run_id")
    if bundle_run_id:
        run_id = cast("str", bundle_run_id)
    if run_id is None and run_bundle_dir is not None:
        run_id = run_bundle_dir.name
    return run_bundle_dir, run_id


_PACKED_REF_PARTS = 2


def _read_text(path: Path) -> str | None:
    try:
        value = path.read_text(encoding="utf-8").strip()
    except OSError:
        return None
    return value or None


def _resolve_packed_ref(git_dir: Path, ref_name: str) -> str | None:
    packed_text = _read_text(git_dir / "packed-refs")
    if packed_text is None:
        return None
    for line in packed_text.splitlines():
        if not line or line.startswith(("#", "^")):
            continue
        parts = line.split()
        if len(parts) != _PACKED_REF_PARTS:
            continue
        sha, ref = parts
        if ref == ref_name:
            return sha
    return None


def _resolve_repo_hash(repo_root: Path) -> str | None:
    git_dir = repo_root / ".git"
    head_value = _read_text(git_dir / "HEAD")
    if head_value is None:
        return None
    if not head_value.startswith("ref:"):
        return head_value
    ref_name = head_value.split("ref:", maxsplit=1)[-1].strip()
    if not ref_name:
        return None
    return _read_text(git_dir / ref_name) or _resolve_packed_ref(git_dir, ref_name)


def _otel_resource_overrides(repo_root: Path) -> dict[str, str]:
    overrides = {"codeanatomy.repo_root": str(repo_root)}
    repo_hash = _resolve_repo_hash(repo_root)
    if repo_hash is not None:
        overrides["codeanatomy.repo_hash"] = repo_hash
    return overrides


def _resolve_version(packages: Sequence[str]) -> str | None:
    for name in packages:
        try:
            return version(name)
        except PackageNotFoundError:
            continue
    return None


def _engine_versions() -> dict[str, str]:
    versions: dict[str, str] = {}
    pyarrow_version = _resolve_version(["pyarrow"])
    if pyarrow_version is not None:
        versions["pyarrow"] = pyarrow_version
    datafusion_version = _resolve_version(["datafusion", "datafusion-python"])
    if datafusion_version is not None:
        versions["datafusion"] = datafusion_version
    return versions


def _product_version(product: GraphProduct) -> str:
    if product == "cpg":
        return f"cpg_ultimate_v{SCHEMA_VERSION}"
    return f"{product}_v{SCHEMA_VERSION}"


def _parse_result(
    *,
    request: GraphProductBuildRequest,
    repo_root: Path,
    pipeline_outputs: Mapping[str, JsonDict | None],
) -> GraphProductBuildResult:
    nodes_report = _parse_finalize(_require(pipeline_outputs, "write_cpg_nodes_delta"))
    output_dir = nodes_report.paths.data.parent

    edges_report = _parse_finalize(_require(pipeline_outputs, "write_cpg_edges_delta"))
    props_report = _parse_finalize(_require(pipeline_outputs, "write_cpg_props_delta"))
    props_map_report = _parse_table(_require(pipeline_outputs, "write_cpg_props_map_delta"))
    edges_by_src_report = _parse_table(_require(pipeline_outputs, "write_cpg_edges_by_src_delta"))
    edges_by_dst_report = _parse_table(_require(pipeline_outputs, "write_cpg_edges_by_dst_delta"))

    manifest_path, manifest_run_id = _parse_manifest_details(
        pipeline_outputs,
        include_manifest=request.include_manifest,
    )
    run_bundle_dir, run_id = _parse_run_bundle(
        pipeline_outputs,
        include_run_bundle=request.include_run_bundle,
    )
    if run_id is None:
        run_id = manifest_run_id
    if run_id is None and run_bundle_dir is not None:
        run_id = run_bundle_dir.name

    extract_errors = (
        _optional(pipeline_outputs, "write_extract_error_artifacts_delta")
        if request.include_extract_errors
        else None
    )

    return GraphProductBuildResult(
        product=request.product,
        product_version=_product_version(request.product),
        engine_versions=_engine_versions(),
        run_id=run_id,
        repo_root=repo_root,
        output_dir=output_dir,
        cpg_nodes=nodes_report,
        cpg_edges=edges_report,
        cpg_props=props_report,
        cpg_props_map=props_map_report,
        cpg_edges_by_src=edges_by_src_report,
        cpg_edges_by_dst=edges_by_dst_report,
        extract_error_artifacts=extract_errors,
        manifest_path=manifest_path,
        run_bundle_dir=run_bundle_dir,
        pipeline_outputs=pipeline_outputs,
    )


def _record_build_output_locations(result: GraphProductBuildResult) -> None:
    from hamilton_pipeline.lifecycle import get_hamilton_diagnostics_collector

    collector = get_hamilton_diagnostics_collector()
    if collector is None:
        return
    collector.record_artifact(
        "build_output_locations_v1",
        {
            "run_id": result.run_id,
            "output_dir": str(result.output_dir),
            "run_bundle_dir": (
                str(result.run_bundle_dir) if result.run_bundle_dir is not None else None
            ),
        },
    )


__all__ = [
    "FinalizeDeltaPaths",
    "FinalizeDeltaReport",
    "GraphProductBuildRequest",
    "GraphProductBuildResult",
    "TableDeltaReport",
    "build_graph_product",
]
