"""Generate a diagnostic report for pipeline outputs."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Annotated

import pyarrow as pa
from cyclopts import Parameter

from datafusion_engine.delta.control_plane_core import (
    DeltaProviderRequest,
    delta_provider_from_session,
)
from datafusion_engine.delta.service import DeltaService
from datafusion_engine.identity import schema_identity_hash
from datafusion_engine.io.adapter import DataFusionIOAdapter
from datafusion_engine.schema import extract_schema_for
from datafusion_engine.session.runtime import DataFusionRuntimeProfile
from datafusion_engine.tables.metadata import TableProviderCapsule
from utils.uuid_factory import uuid7_hex

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Sequence

    from datafusion import SessionContext


@dataclass(frozen=True)
class Issue:
    """Diagnostic issue entry."""

    severity: str
    check: str
    detail: str


def diag_command(
    output_dir: Annotated[
        str,
        Parameter(
            name="--output-dir",
            help="Output directory containing e2e artifacts.",
        ),
    ] = "build/e2e_full_pipeline",
    run_bundle: Annotated[
        str | None,
        Parameter(
            name="--run-bundle",
            help="Specific run bundle directory (default: latest under run_bundles).",
        ),
    ] = None,
    report_path: Annotated[
        str | None,
        Parameter(
            name="--report-path",
            help="Path to write JSON report (default: <output_dir>/diagnostics_report.json).",
        ),
    ] = None,
    summary_path: Annotated[
        str | None,
        Parameter(
            name="--summary-path",
            help="Path to write summary markdown (default: <output_dir>/diagnostics_report.md).",
        ),
    ] = None,
    *,
    validate_delta: Annotated[
        bool,
        Parameter(
            name="--validate-delta",
            help="Read Delta tables and validate arrays (slower, more thorough).",
        ),
    ] = False,
) -> int:
    """Generate a diagnostic report for pipeline outputs.

    Returns:
    -------
    int
        Exit status code.
    """
    resolved_output, resolved_bundle, resolved_report, resolved_summary = _resolve_paths(
        output_dir,
        run_bundle=run_bundle,
        report_path=report_path,
        summary_path=summary_path,
    )

    profile = DataFusionRuntimeProfile()
    ctx = profile.session_context()
    adapter = DataFusionIOAdapter(ctx=ctx, profile=profile)
    read_table = _build_delta_reader(ctx, adapter=adapter)

    report = _build_report(
        resolved_output,
        resolved_bundle,
        validate_delta=bool(validate_delta),
        read_table=read_table,
    )

    resolved_report.parent.mkdir(parents=True, exist_ok=True)
    resolved_summary.parent.mkdir(parents=True, exist_ok=True)
    _write_json(resolved_report, report)
    _write_summary(resolved_summary, report)

    status = report.get("status")
    return 0 if status == "ok" else 1


def _is_delta_table(path: Path) -> bool:
    try:
        profile = DataFusionRuntimeProfile()
        return DeltaService(profile=profile).table_version(path=str(path)) is not None
    except (RuntimeError, TypeError, ValueError):
        return False


def _build_delta_reader(
    ctx: SessionContext,
    *,
    adapter: DataFusionIOAdapter,
) -> Callable[[Path], pa.Table]:
    def _read(path: Path) -> pa.Table:
        bundle = delta_provider_from_session(
            ctx,
            request=DeltaProviderRequest(
                table_uri=str(path),
                storage_options=None,
                version=None,
                timestamp=None,
                delta_scan=None,
                gate=None,
            ),
        )
        table_name = f"__diagnostics_{uuid7_hex()}"
        adapter.register_table(
            table_name,
            TableProviderCapsule(bundle.provider),
            overwrite=True,
        )
        try:
            return ctx.table(table_name).to_arrow_table()
        finally:
            adapter.deregister_table(table_name)

    return _read


def _latest_dir(directories: Iterable[Path]) -> Path | None:
    candidates = [path for path in directories if path.is_dir()]
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_mtime)


def _read_json(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        return payload
    msg = f"Expected JSON object in {path}."
    raise ValueError(msg)


def _decode_metadata(schema: pa.Schema) -> dict[str, str]:
    if schema.metadata is None:
        return {}
    return {key.decode("utf-8"): value.decode("utf-8") for key, value in schema.metadata.items()}


def _delta_summary(path: Path, *, validate: bool, table: pa.Table) -> dict[str, object]:
    summary: dict[str, object] = {"path": str(path)}
    schema = table.schema
    summary["rows"] = int(table.num_rows)
    summary["columns"] = len(schema.names)
    summary["schema_identity_hash"] = schema_identity_hash(schema)
    summary["schema_metadata"] = _decode_metadata(schema)
    if validate:
        table.validate(full=True)
    return summary


def _parse_required_non_null(metadata: Mapping[str, str]) -> list[str]:
    required = metadata.get("required_non_null")
    if not required:
        return []
    return [part.strip() for part in required.split(",") if part.strip()]


def _iter_py_values(values: pa.Array | pa.ChunkedArray) -> Iterable[object]:
    for value in values:
        if isinstance(value, pa.Scalar):
            yield value.as_py()
        else:
            yield value


def _non_null_set(values: pa.Array | pa.ChunkedArray) -> set[object]:
    return {value for value in _iter_py_values(values) if value is not None}


def _count_missing(values: pa.Array | pa.ChunkedArray, present: set[object]) -> int:
    return sum(1 for value in _iter_py_values(values) if value is not None and value not in present)


def _count_invalid_span(
    bstart: pa.Array | pa.ChunkedArray,
    bend: pa.Array | pa.ChunkedArray,
) -> int:
    return sum(
        1
        for start, end in zip(_iter_py_values(bstart), _iter_py_values(bend), strict=True)
        if isinstance(start, (int, float)) and isinstance(end, (int, float)) and start > end
    )


def _count_negative(values: pa.Array | pa.ChunkedArray) -> int:
    return sum(
        1 for value in _iter_py_values(values) if isinstance(value, (int, float)) and value < 0
    )


def _count_nulls(table: pa.Table, column: str) -> int:
    array = table[column]
    return int(array.null_count)


def _check_required_non_null(
    path: Path,
    schema: pa.Schema,
    *,
    issues: list[Issue],
    read_table: Callable[[Path], pa.Table],
) -> None:
    metadata = _decode_metadata(schema)
    required = _parse_required_non_null(metadata)
    if not required:
        return
    table = read_table(path).select(required)
    for name in required:
        nulls = _count_nulls(table, name)
        if nulls > 0:
            issues.append(
                Issue(
                    severity="error",
                    check="required_non_null",
                    detail=f"{path.name}:{name} has {nulls} nulls",
                )
            )


def _check_contract_schema(
    schema: pa.Schema,
    *,
    issues: list[Issue],
) -> None:
    metadata = _decode_metadata(schema)
    schema_name = metadata.get("schema_name")
    if not schema_name:
        return
    try:
        expected = extract_schema_for(schema_name)
    except KeyError:
        return
    actual_fp = schema_identity_hash(schema)
    expected_fp = schema_identity_hash(expected)
    if actual_fp != expected_fp:
        issues.append(
            Issue(
                severity="error",
                check="schema_contract",
                detail=f"{schema_name} fingerprint mismatch ({actual_fp} != {expected_fp})",
            )
        )


def _check_edge_invariants(
    nodes_path: Path,
    edges_path: Path,
    *,
    issues: list[Issue],
    read_table: Callable[[Path], pa.Table],
) -> None:
    nodes = read_table(nodes_path).select(["node_id"])
    edges = read_table(edges_path).select(
        ["edge_id", "src_node_id", "dst_node_id", "bstart", "bend"]
    )
    node_ids = nodes["node_id"]
    src_ids = edges["src_node_id"]
    dst_ids = edges["dst_node_id"]
    node_id_set = _non_null_set(node_ids)
    src_missing_count = _count_missing(src_ids, node_id_set)
    dst_missing_count = _count_missing(dst_ids, node_id_set)
    if src_missing_count > 0:
        issues.append(
            Issue(
                severity="error",
                check="edge_ref_integrity",
                detail=f"src_node_id missing in nodes: {src_missing_count}",
            )
        )
    if dst_missing_count > 0:
        issues.append(
            Issue(
                severity="error",
                check="edge_ref_integrity",
                detail=f"dst_node_id missing in nodes: {dst_missing_count}",
            )
        )

    bstart = edges["bstart"]
    bend = edges["bend"]
    invalid_span_count = _count_invalid_span(bstart, bend)
    if invalid_span_count > 0:
        issues.append(
            Issue(
                severity="error",
                check="edge_span",
                detail=f"bstart > bend rows: {invalid_span_count}",
            )
        )

    negative_count = _count_negative(bstart)
    if negative_count > 0:
        issues.append(
            Issue(
                severity="warn",
                check="edge_span",
                detail=f"negative bstart rows: {negative_count}",
            )
        )


def _check_required_files(
    output_dir: Path,
    *,
    issues: list[Issue],
) -> dict[str, bool]:
    expected = [
        "cpg_nodes",
        "cpg_edges",
        "cpg_props",
        "cpg_edges_error_stats",
        "manifest.json",
    ]
    present: dict[str, bool] = {}
    for name in expected:
        path = output_dir / name
        exists = path.exists()
        present[name] = exists
        if not exists:
            issues.append(
                Issue(
                    severity="error",
                    check="required_file",
                    detail=f"Missing {path}",
                )
            )
    return present


def _summarize_manifest(manifest: Mapping[str, object]) -> dict[str, object]:
    outputs = manifest.get("outputs", [])
    datasets = manifest.get("datasets", [])
    extracts = manifest.get("extracts", [])
    rules = manifest.get("rules", [])
    return {
        "outputs_count": len(outputs) if isinstance(outputs, list) else 0,
        "datasets_count": len(datasets) if isinstance(datasets, list) else 0,
        "extracts_count": len(extracts) if isinstance(extracts, list) else 0,
        "rules_count": len(rules) if isinstance(rules, list) else 0,
        "outputs": outputs,
    }


def _write_json(path: Path, payload: Mapping[str, object]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _write_summary(path: Path, report: Mapping[str, object]) -> None:
    lines: list[str] = []
    lines.append("# E2E Diagnostics Report")
    lines.append("")
    lines.append(f"Status: {report.get('status')}")
    lines.append("")
    summary_value = report.get("summary", {})
    summary: Mapping[str, object] = {}
    if isinstance(summary_value, Mapping):
        summary = summary_value
    lines.append(f"Outputs checked: {summary.get('delta_tables')}")
    lines.append(f"Manifest outputs: {summary.get('manifest_outputs')}")
    lines.append(f"Extracts: {summary.get('extracts')} Rules: {summary.get('rules')}")
    lines.append(f"Run bundle datasets: {summary.get('run_bundle_datasets')}")
    lines.append(f"Incremental enabled: {summary.get('incremental_enabled')}")
    lines.append(f"Incremental datasets: {summary.get('incremental_datasets')}")
    lines.append("")
    issues_value = report.get("issues")
    issues: list[dict[str, str]]
    if isinstance(issues_value, list):
        issues = [item for item in issues_value if isinstance(item, dict)]
    else:
        issues = []
    if not issues:
        lines.append("No issues detected.")
    else:
        lines.append("Issues:")
        lines.extend(
            f"- [{issue.get('severity')}] {issue.get('check')}: {issue.get('detail')}"
            for issue in issues
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _resolve_run_bundle(output_dir: Path, run_bundle: Path | None) -> Path | None:
    if run_bundle is not None:
        return run_bundle
    run_bundles_dir = output_dir / "run_bundles"
    if not run_bundles_dir.exists():
        return None
    return _latest_dir(run_bundles_dir.iterdir())


def _resolve_paths(
    output_dir: str,
    *,
    run_bundle: str | None,
    report_path: str | None,
    summary_path: str | None,
) -> tuple[Path, Path | None, Path, Path]:
    resolved_output = Path(output_dir).resolve()
    resolved_bundle = Path(run_bundle).resolve() if run_bundle else None
    resolved_bundle = _resolve_run_bundle(resolved_output, resolved_bundle)
    resolved_report = (
        Path(report_path).resolve() if report_path else resolved_output / "diagnostics_report.json"
    )
    resolved_summary = (
        Path(summary_path).resolve() if summary_path else resolved_output / "diagnostics_report.md"
    )
    return resolved_output, resolved_bundle, resolved_report, resolved_summary


def _resolve_state_dir(run_config: Mapping[str, object]) -> Path | None:
    value = run_config.get("incremental_state_dir")
    repo_root = run_config.get("repo_root")
    if not value and not repo_root:
        return None
    state_dir = Path(value) if isinstance(value, str) else None
    if state_dir is None:
        if isinstance(repo_root, str) and repo_root:
            state_dir = Path(repo_root) / "build" / "state"
        else:
            return None
    if not state_dir.is_absolute() and isinstance(repo_root, str) and repo_root:
        state_dir = Path(repo_root) / state_dir
    return state_dir.resolve()


def _load_manifest(
    run_bundle: Path | None,
    *,
    issues: list[Issue],
) -> dict[str, object] | None:
    if run_bundle is None:
        issues.append(
            Issue(
                severity="error",
                check="manifest",
                detail="Run bundle directory not found.",
            )
        )
        return None
    manifest_path = run_bundle / "manifest.json"
    if not manifest_path.exists():
        issues.append(
            Issue(
                severity="error",
                check="manifest",
                detail="Run bundle manifest.json not found.",
            )
        )
        return None
    return _read_json(manifest_path)


def _load_run_config(
    run_bundle: Path | None,
    *,
    issues: list[Issue],
) -> dict[str, object] | None:
    if run_bundle is None:
        return None
    config_path = run_bundle / "config.json"
    if not config_path.exists():
        issues.append(
            Issue(
                severity="warn",
                check="run_config",
                detail="Run bundle config.json not found.",
            )
        )
        return None
    return _read_json(config_path)


def _check_run_bundle_datasets(
    run_bundle: Path | None,
    *,
    issues: list[Issue],
) -> dict[str, bool]:
    expected = {
        "rule_diagnostics": Path("relspec") / "rule_diagnostics",
        "datafusion_fallbacks": Path("relspec") / "datafusion_fallbacks",
        "datafusion_explains": Path("relspec") / "datafusion_explains",
        "scan_telemetry": Path("relspec") / "scan_telemetry",
        "rule_exec_events": Path("relspec") / "rule_exec_events",
    }
    present: dict[str, bool] = {}
    if run_bundle is None:
        for name in expected:
            present[name] = False
        issues.append(
            Issue(
                severity="warn",
                check="run_bundle",
                detail="Run bundle directory not found for dataset checks.",
            )
        )
        return present
    for name, rel_path in expected.items():
        path = run_bundle / rel_path
        exists = path.exists()
        present[name] = exists
        if not exists:
            issues.append(
                Issue(
                    severity="warn",
                    check="run_bundle_dataset",
                    detail=f"Missing {path}",
                )
            )
    return present


def _check_incremental_state(
    run_config: Mapping[str, object] | None,
    run_bundle: Path | None,
    *,
    issues: list[Issue],
) -> dict[str, object]:
    info: dict[str, object] = {"enabled": False}
    if run_config is None:
        return info
    enabled = bool(run_config.get("incremental_enabled"))
    info["enabled"] = enabled
    if not enabled:
        return info
    state_dir = _resolve_state_dir(run_config)
    info["state_dir"] = str(state_dir) if state_dir is not None else None
    if state_dir is None:
        issues.append(
            Issue(
                severity="error",
                check="incremental_state",
                detail="Incremental enabled but state dir is missing.",
            )
        )
        return info
    if not state_dir.exists():
        issues.append(
            Issue(
                severity="error",
                check="incremental_state",
                detail=f"Incremental state dir missing: {state_dir}",
            )
        )
        return info
    datasets_dir = state_dir / "datasets"
    info["datasets_dir"] = str(datasets_dir)
    if datasets_dir.exists():
        datasets = sorted(path.name for path in datasets_dir.iterdir() if path.is_dir())
        info["datasets_present"] = datasets
    else:
        empty_datasets: list[str] = []
        info["datasets_present"] = empty_datasets
        issues.append(
            Issue(
                severity="warn",
                check="incremental_state",
                detail=f"Incremental datasets dir missing: {datasets_dir}",
            )
        )

    snapshot_path = state_dir / "metadata" / "invalidation_snapshot.json"
    info["invalidation_snapshot"] = {
        "path": str(snapshot_path),
        "exists": snapshot_path.exists(),
    }
    if not snapshot_path.exists():
        issues.append(
            Issue(
                severity="warn",
                check="incremental_state",
                detail=f"Invalidation snapshot missing: {snapshot_path}",
            )
        )

    if run_bundle is not None:
        incremental_dir = run_bundle / "incremental"
        expected = (
            "incremental_diff",
            "inc_changed_exports_v1",
            "inc_impacted_callers_v1",
            "inc_impacted_importers_v1",
            "inc_impacted_files_v2",
        )
        present: dict[str, bool] = {}
        for name in expected:
            path = incremental_dir / name
            exists = path.exists()
            present[name] = exists
            if not exists:
                issues.append(
                    Issue(
                        severity="warn",
                        check="incremental_artifact",
                        detail=f"Missing incremental artifact: {path}",
                    )
                )
        info["incremental_artifacts"] = present
    return info


def _collect_delta_summaries(
    output_dir: Path,
    *,
    validate_delta: bool,
    issues: list[Issue],
    read_table: Callable[[Path], pa.Table],
) -> dict[str, dict[str, object]]:
    summaries: dict[str, dict[str, object]] = {}
    for path in output_dir.iterdir():
        if not path.is_dir():
            continue
        if not _is_delta_table(path):
            continue
        table = read_table(path)
        summary = _delta_summary(path, validate=validate_delta, table=table)
        summaries[path.name] = summary
        schema = table.schema
        _check_contract_schema(schema, issues=issues)
        _check_required_non_null(path, schema, issues=issues, read_table=read_table)
    return summaries


def _maybe_check_edge_invariants(
    output_dir: Path,
    *,
    issues: list[Issue],
    read_table: Callable[[Path], pa.Table],
) -> None:
    nodes_path = output_dir / "cpg_nodes"
    edges_path = output_dir / "cpg_edges"
    if nodes_path.exists() and edges_path.exists():
        _check_edge_invariants(nodes_path, edges_path, issues=issues, read_table=read_table)


def _scip_index_info(output_dir: Path, *, issues: list[Issue]) -> dict[str, object]:
    scip_index = output_dir.parent / "scip" / "index.scip"
    info: dict[str, object] = {
        "path": str(scip_index),
        "exists": scip_index.exists(),
        "size": scip_index.stat().st_size if scip_index.exists() else 0,
    }
    if not info["exists"]:
        issues.append(
            Issue(
                severity="error",
                check="scip_index",
                detail=f"Missing SCIP index at {info['path']}",
            )
        )
    return info


def _collect_extract_error_dirs(output_dir: Path) -> list[Path]:
    extract_errors = output_dir / "extract_errors"
    if not extract_errors.exists():
        return []
    return [path for path in extract_errors.iterdir() if path.is_dir()]


def _report_status(issues: Sequence[Issue]) -> str:
    if any(issue.severity == "error" for issue in issues):
        return "error"
    if issues:
        return "warn"
    return "ok"


def _build_summary(
    manifest_summary: Mapping[str, object],
    delta_summaries: Mapping[str, Mapping[str, object]],
    extract_error_dirs: Sequence[Path],
    run_bundle_datasets: Mapping[str, bool],
    incremental_info: Mapping[str, object],
) -> dict[str, object]:
    datasets_value = incremental_info.get("datasets_present")
    datasets_present: Sequence[object] = (
        datasets_value if isinstance(datasets_value, (list, tuple)) else ()
    )
    return {
        "delta_tables": len(delta_summaries),
        "manifest_outputs": manifest_summary.get("outputs_count", 0),
        "extracts": manifest_summary.get("extracts_count", 0),
        "rules": manifest_summary.get("rules_count", 0),
        "extract_error_dirs": len(extract_error_dirs),
        "run_bundle_datasets": sum(1 for present in run_bundle_datasets.values() if present),
        "incremental_enabled": bool(incremental_info.get("enabled")),
        "incremental_datasets": len(datasets_present),
    }


def _build_report(
    output_dir: Path,
    run_bundle: Path | None,
    *,
    validate_delta: bool,
    read_table: Callable[[Path], pa.Table],
) -> dict[str, object]:
    issues: list[Issue] = []
    required_files = _check_required_files(output_dir, issues=issues)
    manifest = _load_manifest(run_bundle, issues=issues)
    run_config = _load_run_config(run_bundle, issues=issues)
    run_bundle_datasets = _check_run_bundle_datasets(run_bundle, issues=issues)
    incremental_state = _check_incremental_state(run_config, run_bundle, issues=issues)
    delta_summaries = _collect_delta_summaries(
        output_dir,
        validate_delta=validate_delta,
        issues=issues,
        read_table=read_table,
    )
    _maybe_check_edge_invariants(output_dir, issues=issues, read_table=read_table)
    scip_info = _scip_index_info(output_dir, issues=issues)
    extract_error_dirs = _collect_extract_error_dirs(output_dir)
    manifest_summary = _summarize_manifest(manifest) if manifest is not None else {}
    summary = _build_summary(
        manifest_summary,
        delta_summaries,
        extract_error_dirs,
        run_bundle_datasets,
        incremental_state,
    )
    status = _report_status(issues)

    return {
        "status": status,
        "output_dir": str(output_dir),
        "run_bundle": str(run_bundle) if run_bundle is not None else None,
        "required_files": required_files,
        "manifest_summary": manifest_summary,
        "run_bundle_datasets": run_bundle_datasets,
        "incremental_state": incremental_state,
        "delta_summaries": delta_summaries,
        "scip_index": scip_info,
        "extract_errors": {
            "path": str(output_dir / "extract_errors"),
            "dir_count": len(extract_error_dirs),
        },
        "issues": [issue.__dict__ for issue in issues],
        "summary": summary,
    }


__all__ = ["diag_command"]
