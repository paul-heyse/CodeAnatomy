"""Canonical graph product build entrypoints.

This is the public API that callers should use. It intentionally hides Hamilton output node
names and returns a typed result with stable fields.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Literal, cast

from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.determinism import DeterminismTier
from core_types import JsonDict, JsonValue, PathLike, ensure_path
from cpg.schemas import SCHEMA_VERSION
from engine.plan_policy import WriterStrategy
from hamilton_pipeline import PipelineExecutionOptions, execute_pipeline
from hamilton_pipeline.execution import ImpactStrategy
from hamilton_pipeline.pipeline_types import ScipIdentityOverrides, ScipIndexConfig
from incremental.types import IncrementalConfig
from storage.deltalake.registry_runner import run_registry_exports

GraphProduct = Literal["cpg"]


@dataclass(frozen=True)
class FinalizeDeltaPaths:
    """Paths returned by write_finalize_result_delta."""

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

    cpg_nodes_quality: TableDeltaReport | None = None
    cpg_props_quality: TableDeltaReport | None = None

    extract_error_artifacts: JsonDict | None = None
    manifest_path: Path | None = None
    run_bundle_dir: Path | None = None

    pipeline_outputs: Mapping[str, JsonDict | None] = field(default_factory=dict)


@dataclass(frozen=True)
class GraphProductBuildRequest:
    """Specify inputs and options for a graph product build."""

    repo_root: PathLike
    product: GraphProduct = "cpg"

    output_dir: PathLike | None = None
    work_dir: PathLike | None = None

    runtime_profile_name: str | None = None
    determinism_override: DeterminismTier | None = None

    scip_index_config: ScipIndexConfig | None = None
    scip_identity_overrides: ScipIdentityOverrides | None = None

    ctx: ExecutionContext | None = None
    writer_strategy: WriterStrategy | None = None

    incremental_config: IncrementalConfig | None = None
    incremental_impact_strategy: ImpactStrategy | None = None

    include_quality: bool = True
    include_extract_errors: bool = True
    include_manifest: bool = True
    include_run_bundle: bool = True

    config: Mapping[str, JsonValue] = field(default_factory=dict)
    overrides: Mapping[str, object] | None = None


def build_graph_product(request: GraphProductBuildRequest) -> GraphProductBuildResult:
    """Build the requested graph product and return typed outputs.

    Returns
    -------
    GraphProductBuildResult
        Typed outputs for the requested graph product.
    """
    repo_root_path = ensure_path(request.repo_root).resolve()
    registry_output_dir = _resolve_output_dir(repo_root_path, request.output_dir)
    run_registry_exports(registry_output_dir)

    overrides: dict[str, object] = dict(request.overrides or {})
    if request.runtime_profile_name is not None:
        overrides["runtime_profile_name"] = request.runtime_profile_name
    if request.determinism_override is not None:
        overrides["determinism_override"] = request.determinism_override
    if request.writer_strategy is not None:
        overrides["writer_strategy"] = request.writer_strategy

    outputs = _outputs_for_request(request)
    options = PipelineExecutionOptions(
        output_dir=request.output_dir,
        work_dir=request.work_dir,
        scip_index_config=request.scip_index_config,
        scip_identity_overrides=request.scip_identity_overrides,
        ctx=request.ctx,
        incremental_config=request.incremental_config,
        incremental_impact_strategy=request.incremental_impact_strategy,
        outputs=outputs,
        config=request.config,
        overrides=overrides or None,
    )

    raw = execute_pipeline(repo_root=repo_root_path, options=options)
    return _parse_result(
        request=request,
        repo_root=repo_root_path,
        pipeline_outputs=raw,
    )


def _outputs_for_request(request: GraphProductBuildRequest) -> Sequence[str]:
    outputs: list[str] = [
        "write_cpg_nodes_delta",
        "write_cpg_edges_delta",
        "write_cpg_props_delta",
    ]
    if request.include_quality:
        outputs.extend(
            [
                "write_cpg_nodes_quality_delta",
                "write_cpg_props_quality_delta",
            ]
        )
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
    ibis_version = _resolve_version(["ibis-framework", "ibis"])
    if ibis_version is not None:
        versions["ibis"] = ibis_version
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

    nodes_quality = None
    if request.include_quality:
        quality = _optional(pipeline_outputs, "write_cpg_nodes_quality_delta")
        if quality is not None:
            nodes_quality = _parse_table(quality)

    props_quality = None
    if request.include_quality:
        quality = _optional(pipeline_outputs, "write_cpg_props_quality_delta")
        if quality is not None:
            props_quality = _parse_table(quality)

    manifest_path = None
    if request.include_manifest:
        manifest = _optional(pipeline_outputs, "write_run_manifest_delta")
        if manifest is not None and manifest.get("path"):
            manifest_path = Path(cast("str", manifest["path"]))

    run_bundle_dir = None
    if request.include_run_bundle:
        bundle = _optional(pipeline_outputs, "write_run_bundle_dir")
        if bundle is not None and bundle.get("bundle_dir"):
            run_bundle_dir = Path(cast("str", bundle["bundle_dir"]))
    run_id = run_bundle_dir.name if run_bundle_dir is not None else None

    extract_errors = None
    if request.include_extract_errors:
        extract_errors = _optional(pipeline_outputs, "write_extract_error_artifacts_delta")

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
        cpg_nodes_quality=nodes_quality,
        cpg_props_quality=props_quality,
        extract_error_artifacts=extract_errors,
        manifest_path=manifest_path,
        run_bundle_dir=run_bundle_dir,
        pipeline_outputs=pipeline_outputs,
    )


__all__ = [
    "FinalizeDeltaPaths",
    "FinalizeDeltaReport",
    "GraphProductBuildRequest",
    "GraphProductBuildResult",
    "TableDeltaReport",
    "build_graph_product",
]
