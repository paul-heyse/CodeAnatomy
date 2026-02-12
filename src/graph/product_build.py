"""Canonical graph product build entrypoints.

This is the public API that callers should use. It routes through the engine-native
orchestration layer and returns a typed result with stable fields.
"""

from __future__ import annotations

import signal
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import TYPE_CHECKING, Literal, cast

from core_types import JsonDict, JsonValue, PathLike, ensure_path
from obs.diagnostics_report import write_run_diagnostics_report
from obs.otel import (
    OtelBootstrapOptions,
    configure_otel,
    emit_diagnostics_event,
    snapshot_diagnostics,
    start_build_heartbeat,
    write_run_diagnostics_bundle,
)
from obs.otel.run_context import reset_run_id, set_run_id
from obs.otel.tracing import record_exception, root_span, set_span_attributes
from planning_engine.output_contracts import (
    CPG_OUTPUT_CANONICAL_TO_LEGACY,
    ENGINE_CPG_OUTPUTS,
    output_aliases,
)
from utils.uuid_factory import uuid7_str

if TYPE_CHECKING:
    from planning_engine.spec_contracts import RuntimeConfig

GraphProduct = Literal["cpg"]
CPG_PRODUCT_SCHEMA_VERSION = 1


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
    """Specify inputs and options for a graph product build.

    Routes through engine-native orchestration with engine profiles and
    rulepack profiles replacing Hamilton execution modes.
    """

    repo_root: PathLike
    product: GraphProduct = "cpg"

    engine_profile: str = "medium"
    rulepack_profile: str = "default"
    runtime_config: RuntimeConfig | None = None

    output_dir: PathLike | None = None
    work_dir: PathLike | None = None

    extraction_config: dict[str, object] | None = None

    include_extract_errors: bool = True
    include_manifest: bool = True
    include_run_bundle: bool = False

    config: Mapping[str, JsonValue] = field(default_factory=dict)
    overrides: Mapping[str, object] | None = None
    otel_options: OtelBootstrapOptions | None = None


def build_graph_product(request: GraphProductBuildRequest) -> GraphProductBuildResult:
    """Build the requested graph product and return typed outputs.

    Routes through engine-native orchestration with profiles and runtime config.

    Returns:
    -------
    GraphProductBuildResult
        Typed outputs for the requested graph product.
    """
    repo_root_path = ensure_path(request.repo_root).resolve()
    resolved_output_dir = _resolve_output_dir(repo_root_path, request.output_dir)
    resolved_work_dir = _resolve_work_dir(repo_root_path, request.work_dir)
    run_id = _resolve_run_id(request)
    _configure_otel(request, repo_root_path)

    run_token = set_run_id(run_id)
    emit_diagnostics_event(
        "build.start",
        payload={
            "run_id": run_id,
            "repo_root": str(repo_root_path),
            "output_dir": str(resolved_output_dir),
            "product": request.product,
            "engine_profile": request.engine_profile,
            "rulepack_profile": request.rulepack_profile,
        },
        event_kind="event",
    )
    heartbeat = start_build_heartbeat(run_id=run_id, interval_s=5.0)
    result: GraphProductBuildResult | None = None
    fallback_bundle_dir = resolved_output_dir / "run_bundle" / run_id
    signal_triggered = {"value": False}
    handler = _signal_handler_for_build(run_id, fallback_bundle_dir, signal_triggered)
    previous_sigterm, previous_sigint = _install_signal_handlers(handler)
    try:
        result = _execute_build(
            request=request,
            repo_root=repo_root_path,
            output_dir=resolved_output_dir,
            work_dir=resolved_work_dir,
            run_id=run_id,
        )
    except Exception as exc:
        emit_diagnostics_event(
            "build.failure",
            payload={
                "run_id": run_id,
                "error_type": type(exc).__name__,
                "error": str(exc),
            },
            event_kind="event",
        )
        raise
    else:
        _record_build_output_locations(result)
        emit_diagnostics_event(
            "build.success",
            payload={
                "run_id": run_id,
                "output_dir": str(result.output_dir),
                "run_bundle_dir": str(result.run_bundle_dir) if result.run_bundle_dir else None,
            },
            event_kind="event",
        )
        return result
    finally:
        _restore_signal_handlers(previous_sigterm, previous_sigint)
        _finalize_build_bundle(result, request, fallback_bundle_dir, run_id)
        heartbeat.stop(timeout_s=2.0)
        reset_run_id(run_token)


def _resolve_run_id(request: GraphProductBuildRequest) -> str:
    """Resolve run_id from overrides or generate a new one.

    Returns:
    -------
    str
        Run ID for this build.
    """
    if request.overrides:
        run_id = request.overrides.get("run_id")
        if isinstance(run_id, str) and run_id:
            return run_id
    return uuid7_str()


def _resolve_work_dir(repo_root: Path, work_dir: PathLike | None) -> Path:
    """Resolve work directory for intermediate artifacts.

    Returns:
    -------
    Path
        Absolute work directory path.
    """
    if work_dir is None:
        return repo_root / "work"
    if isinstance(work_dir, str) and not work_dir:
        return repo_root / "work"
    resolved = ensure_path(work_dir)
    return resolved if resolved.is_absolute() else repo_root / resolved


def _configure_otel(request: GraphProductBuildRequest, repo_root: Path) -> None:
    effective_otel = request.otel_options or OtelBootstrapOptions()
    resource_overrides = dict(effective_otel.resource_overrides or {})
    resource_overrides.update(_otel_resource_overrides(repo_root))
    configure_otel(
        service_name="codeanatomy",
        options=replace(effective_otel, resource_overrides=resource_overrides),
    )


def _signal_handler_for_build(
    run_id: str,
    run_bundle_dir: Path,
    signal_state: dict[str, bool],
) -> Callable[[int, object | None], None]:
    def _handler(signum: int, _frame: object | None) -> None:
        _ = signum
        if signal_state["value"]:
            raise SystemExit(1)
        signal_state["value"] = True
        _write_diagnostics_outputs(run_bundle_dir, run_id=run_id)
        raise SystemExit(1)

    return _handler


def _install_signal_handlers(
    handler: Callable[[int, object | None], None],
) -> tuple[signal.Handlers, signal.Handlers]:
    return (
        cast("signal.Handlers", signal.signal(signal.SIGTERM, handler)),
        cast("signal.Handlers", signal.signal(signal.SIGINT, handler)),
    )


def _restore_signal_handlers(
    previous_sigterm: signal.Handlers,
    previous_sigint: signal.Handlers,
) -> None:
    signal.signal(signal.SIGTERM, previous_sigterm)
    signal.signal(signal.SIGINT, previous_sigint)


def _execute_build(
    *,
    request: GraphProductBuildRequest,
    repo_root: Path,
    output_dir: Path,
    work_dir: Path,
    run_id: str,
) -> GraphProductBuildResult:
    """Execute the build through engine-native orchestration.

    Routes to orchestrate_build() with engine profiles and runtime config.

    Returns:
    -------
    GraphProductBuildResult
        Typed outputs for the requested graph product.
    """
    from graph.build_pipeline import orchestrate_build

    with root_span(
        "graph_product.build",
        attributes={
            "codeanatomy.product": request.product,
            "codeanatomy.engine_profile": request.engine_profile,
            "codeanatomy.rulepack_profile": request.rulepack_profile,
        },
    ) as span:
        emit_diagnostics_event(
            "build.phase.start",
            payload={
                "phase": "orchestrate.build",
                "engine_profile": request.engine_profile,
                "rulepack_profile": request.rulepack_profile,
                "run_id": run_id,
            },
            event_kind="event",
        )
        try:
            build_result = orchestrate_build(
                repo_root=repo_root,
                work_dir=work_dir,
                output_dir=output_dir,
                engine_profile=request.engine_profile,
                rulepack_profile=request.rulepack_profile,
                runtime_config=request.runtime_config,
                extraction_config=request.extraction_config,
                include_errors=request.include_extract_errors,
                include_manifest=request.include_manifest,
                include_run_bundle=request.include_run_bundle,
            )
        except Exception as exc:
            record_exception(span, exc)
            emit_diagnostics_event(
                "build.phase.end",
                payload={
                    "phase": "orchestrate.build",
                    "status": "error",
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                    "run_id": run_id,
                },
                event_kind="event",
            )
            raise

        # Map BuildResult to GraphProductBuildResult
        result = _parse_build_result(
            request=request,
            repo_root=repo_root,
            build_result=build_result,
        )

        emit_diagnostics_event(
            "build.phase.end",
            payload={
                "phase": "orchestrate.build",
                "status": "ok",
                "run_id": run_id,
            },
            event_kind="event",
        )
        set_span_attributes(
            span,
            {
                "codeanatomy.product_version": result.product_version,
                "codeanatomy.output_dir": str(result.output_dir),
            },
        )
        return result


def _resolve_bundle_dir(
    result: GraphProductBuildResult | None,
    request: GraphProductBuildRequest,
    fallback_bundle_dir: Path,
) -> Path | None:
    if result is not None and result.run_bundle_dir is not None:
        return result.run_bundle_dir
    if request.include_run_bundle:
        return fallback_bundle_dir
    return None


def _write_diagnostics_outputs(run_bundle_dir: Path, *, run_id: str | None) -> None:
    snapshot = snapshot_diagnostics()
    write_run_diagnostics_bundle(run_bundle_dir=run_bundle_dir, run_id=run_id)
    write_run_diagnostics_report(snapshot=snapshot, run_bundle_dir=run_bundle_dir)


def _finalize_build_bundle(
    result: GraphProductBuildResult | None,
    request: GraphProductBuildRequest,
    fallback_bundle_dir: Path,
    run_id: str,
) -> None:
    bundle_dir = _resolve_bundle_dir(result, request, fallback_bundle_dir)
    if bundle_dir is None:
        return
    _write_diagnostics_outputs(bundle_dir, run_id=run_id)


def _expected_outputs_for_request(request: GraphProductBuildRequest) -> set[str]:
    """Build set of expected output keys from engine contracts.

    Returns:
    -------
    set[str]
        Expected output keys for this request.
    """
    outputs = set(ENGINE_CPG_OUTPUTS)
    outputs.update(CPG_OUTPUT_CANONICAL_TO_LEGACY.values())
    if request.include_extract_errors:
        outputs.add("extract_error_artifacts_delta")
    if request.include_manifest:
        outputs.add("run_manifest_delta")
    if request.include_run_bundle:
        outputs.add("run_bundle_dir")
    return outputs


def _resolve_output_dir(repo_root: Path, output_dir: PathLike | None) -> Path:
    """Resolve the output directory for registry exports.

    Returns:
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


def _optional(outputs: Mapping[str, JsonDict | None], key: str) -> JsonDict | None:
    for alias in output_aliases(key):
        value = outputs.get(alias)
        if value is not None:
            return value
    return None


def _require_cpg_output(outputs: Mapping[str, JsonDict | None], key: str) -> JsonDict:
    for alias in output_aliases(key):
        value = outputs.get(alias)
        if value is not None:
            return value
    msg = f"Missing required CPG output {key!r}."
    raise ValueError(msg)


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
    manifest = _optional(pipeline_outputs, "run_manifest_delta")
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
    manifest = _optional(pipeline_outputs, "run_manifest_delta")
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
    bundle = _optional(pipeline_outputs, "run_bundle_dir")
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
        return f"cpg_ultimate_v{CPG_PRODUCT_SCHEMA_VERSION}"
    return f"{product}_v{CPG_PRODUCT_SCHEMA_VERSION}"


def _parse_build_result(
    *,
    request: GraphProductBuildRequest,
    repo_root: Path,
    build_result: object,
) -> GraphProductBuildResult:
    """Map BuildResult from orchestrate_build to GraphProductBuildResult.

    Parameters:
    ----------
    request
        Original build request
    repo_root
        Repository root path
    build_result
        BuildResult from orchestrate_build

    Returns:
    -------
    GraphProductBuildResult
        Public API result type
    """
    # Build result has cpg_outputs and auxiliary_outputs. Keep run_result out of the
    # public output map to avoid schema collisions with named pipeline outputs.
    all_outputs = {
        **getattr(build_result, "cpg_outputs", {}),
        **getattr(build_result, "auxiliary_outputs", {}),
    }

    # Parse CPG outputs from canonical names with legacy alias fallback.
    nodes_report = _parse_finalize(_require_cpg_output(all_outputs, "cpg_nodes"))

    # Parse auxiliary outputs
    manifest_path, manifest_run_id = _parse_manifest_details(
        all_outputs,
        include_manifest=request.include_manifest,
    )
    run_bundle_dir, run_id = _parse_run_bundle(
        all_outputs,
        include_run_bundle=request.include_run_bundle,
    )
    run_id = run_id or manifest_run_id or (run_bundle_dir.name if run_bundle_dir else None)

    return GraphProductBuildResult(
        product=request.product,
        product_version=_product_version(request.product),
        engine_versions=_engine_versions(),
        run_id=run_id,
        repo_root=repo_root,
        output_dir=nodes_report.paths.data.parent,
        cpg_nodes=nodes_report,
        cpg_edges=_parse_finalize(_require_cpg_output(all_outputs, "cpg_edges")),
        cpg_props=_parse_finalize(_require_cpg_output(all_outputs, "cpg_props")),
        cpg_props_map=_parse_table(_require_cpg_output(all_outputs, "cpg_props_map")),
        cpg_edges_by_src=_parse_table(_require_cpg_output(all_outputs, "cpg_edges_by_src")),
        cpg_edges_by_dst=_parse_table(_require_cpg_output(all_outputs, "cpg_edges_by_dst")),
        extract_error_artifacts=(
            _optional(all_outputs, "extract_error_artifacts_delta")
            if request.include_extract_errors
            else None
        ),
        manifest_path=manifest_path,
        run_bundle_dir=run_bundle_dir,
        pipeline_outputs=all_outputs,
    )


def _record_build_output_locations(result: GraphProductBuildResult) -> None:
    """Record build output locations for diagnostics.

    Stub for compatibility - engine orchestrator handles diagnostics collection.
    """
    _ = result


__all__ = [
    "FinalizeDeltaPaths",
    "FinalizeDeltaReport",
    "GraphProductBuildRequest",
    "GraphProductBuildResult",
    "TableDeltaReport",
    "build_graph_product",
]
