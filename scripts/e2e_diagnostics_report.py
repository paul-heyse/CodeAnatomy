#!/usr/bin/env python3
"""Generate a diagnostic report for e2e_full_pipeline outputs."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from importlib import import_module
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import pyarrow as pa
from datafusion_engine.table_provider_capsule import TableProviderCapsule

from datafusion_engine.delta.control_plane import DeltaProviderRequest, delta_provider_from_session
from datafusion_engine.io.adapter import DataFusionIOAdapter
from datafusion_engine.session.runtime import DataFusionRuntimeProfile
from storage.deltalake import delta_table_version
from utils.uuid_factory import uuid7_hex

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

schema_identity_hash = cast(
    "Callable[[pa.Schema], str]",
    import_module("datafusion_engine.arrow_schema.abi").schema_identity_hash,
)
dataset_schema = cast(
    "Callable[[str], pa.Schema]",
    import_module("cpg.registry_specs").dataset_schema,
)
incremental_dataset_schema = cast(
    "Callable[[str], pa.Schema]",
    import_module("incremental.registry_specs").dataset_schema,
)


@dataclass(frozen=True)
class Issue:
    """Diagnostic issue entry."""

    severity: str
    check: str
    detail: str


if TYPE_CHECKING:
    from datafusion import SessionContext


def _is_delta_table(path: Path) -> bool:
    try:
        return delta_table_version(str(path)) is not None
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
        adapter.register_delta_table_provider(
            table_name,
            TableProviderCapsule(bundle.provider),
            overwrite=True,
        )
        try:
            return ctx.table(table_name).to_arrow_table()
        finally:
            adapter.deregister_table(table_name)

    return _read


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build a diagnostic report for e2e outputs.")
    parser.add_argument(
        "--output-dir",
        default="build/e2e_full_pipeline",
        help="Output directory containing e2e artifacts.",
    )
    parser.add_argument(
        "--run-bundle",
        default=None,
        help="Specific run bundle directory (default: latest under run_bundles).",
    )
    parser.add_argument(
        "--report-path",
        default=None,
        help="Path to write JSON report (default: <output_dir>/diagnostics_report.json).",
    )
    parser.add_argument(
        "--summary-path",
        default=None,
        help="Path to write summary markdown (default: <output_dir>/diagnostics_report.md).",
    )
    parser.add_argument(
        "--validate-delta",
        action="store_true",
        help="Read Delta tables and validate arrays (slower, more thorough).",
    )
    return parser


def _latest_dir(directories: Iterable[Path]) -> Path | None:
    candidates = [path for path in directories if path.is_dir()]
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_mtime)


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _decode_metadata(schema: pa.Schema) -> dict[str, str]:
    if schema.metadata is None:
        return {}
    return {key.decode("utf-8"): value.decode("utf-8") for key, value in schema.metadata.items()}


def _delta_summary(path: Path, *, validate: bool, table: pa.Table) -> dict[str, Any]:
    summary: dict[str, Any] = {"path": str(path)}
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
        expected = dataset_schema(schema_name)
    except KeyError:
        try:
            expected = incremental_dataset_schema(schema_name)
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
        "cpg_nodes_quality",
        "cpg_props_quality",
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


def _summarize_manifest(manifest: Mapping[str, Any]) -> dict[str, Any]:
    outputs = manifest.get("outputs", [])
    datasets = manifest.get("datasets", [])
    extracts = manifest.get("extracts", [])
    rules = manifest.get("rules", [])
    return {
        "outputs_count": len(outputs),
        "datasets_count": len(datasets),
        "extracts_count": len(extracts),
        "rules_count": len(rules),
        "outputs": outputs,
    }


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _write_summary(path: Path, report: Mapping[str, Any]) -> None:
    lines: list[str] = []
    lines.append("# E2E Diagnostics Report")
    lines.append("")
    lines.append(f"Status: {report['status']}")
    lines.append("")
    summary = report["summary"]
    lines.append(f"Outputs checked: {summary['delta_tables']}")
    lines.append(f"Manifest outputs: {summary['manifest_outputs']}")
    lines.append(f"Extracts: {summary['extracts']} Rules: {summary['rules']}")
    lines.append(f"Run bundle datasets: {summary['run_bundle_datasets']}")
    lines.append(f"Incremental enabled: {summary['incremental_enabled']}")
    lines.append(f"Incremental datasets: {summary['incremental_datasets']}")
    lines.append("")
    issues: list[dict[str, str]] = report["issues"]
    if not issues:
        lines.append("No issues detected.")
    else:
        lines.append("Issues:")
        lines.extend(
            f"- [{issue['severity']}] {issue['check']}: {issue['detail']}" for issue in issues
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
    args: argparse.Namespace,
) -> tuple[Path, Path | None, Path, Path]:
    output_dir = Path(args.output_dir).resolve()
    run_bundle = Path(args.run_bundle).resolve() if args.run_bundle else None
    run_bundle = _resolve_run_bundle(output_dir, run_bundle)
    report_path = (
        Path(args.report_path).resolve()
        if args.report_path
        else output_dir / "diagnostics_report.json"
    )
    summary_path = (
        Path(args.summary_path).resolve()
        if args.summary_path
        else output_dir / "diagnostics_report.md"
    )
    return output_dir, run_bundle, report_path, summary_path


def _resolve_state_dir(run_config: Mapping[str, Any]) -> Path | None:
    value = run_config.get("incremental_state_dir")
    repo_root = run_config.get("repo_root")
    if not value and not repo_root:
        return None
    state_dir = Path(value) if value else Path(repo_root) / "build" / "state"
    if not state_dir.is_absolute() and repo_root:
        state_dir = Path(repo_root) / state_dir
    return state_dir.resolve()


def _load_manifest(
    run_bundle: Path | None,
    *,
    issues: list[Issue],
) -> dict[str, Any] | None:
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
) -> dict[str, Any] | None:
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
    run_config: Mapping[str, Any] | None,
    run_bundle: Path | None,
    *,
    issues: list[Issue],
) -> dict[str, Any]:
    info: dict[str, Any] = {"enabled": False}
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
        info["datasets_present"] = []
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
) -> dict[str, dict[str, Any]]:
    summaries: dict[str, dict[str, Any]] = {}
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


def _scip_index_info(output_dir: Path, *, issues: list[Issue]) -> dict[str, Any]:
    scip_index = output_dir.parent / "scip" / "index.scip"
    info = {
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
    manifest_summary: Mapping[str, Any],
    delta_summaries: Mapping[str, Mapping[str, Any]],
    extract_error_dirs: Sequence[Path],
    run_bundle_datasets: Mapping[str, bool],
    incremental_info: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "delta_tables": len(delta_summaries),
        "manifest_outputs": manifest_summary.get("outputs_count", 0),
        "extracts": manifest_summary.get("extracts_count", 0),
        "rules": manifest_summary.get("rules_count", 0),
        "extract_error_dirs": len(extract_error_dirs),
        "run_bundle_datasets": sum(1 for present in run_bundle_datasets.values() if present),
        "incremental_enabled": bool(incremental_info.get("enabled")),
        "incremental_datasets": len(incremental_info.get("datasets_present", [])),
    }


def _build_report(
    output_dir: Path,
    run_bundle: Path | None,
    *,
    validate_delta: bool,
    read_table: Callable[[Path], pa.Table],
) -> dict[str, Any]:
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


def main(argv: Sequence[str] | None = None) -> int:
    """Generate a diagnostic report for e2e pipeline outputs.

    Returns
    -------
    int
        Exit status code.
    """
    parser = _build_parser()
    args = parser.parse_args(argv)
    output_dir, run_bundle, report_path, summary_path = _resolve_paths(args)

    profile = DataFusionRuntimeProfile()
    ctx = profile.session_context()
    adapter = DataFusionIOAdapter(ctx=ctx, profile=profile)
    read_table = _build_delta_reader(ctx, adapter=adapter)
    report = _build_report(
        output_dir,
        run_bundle,
        validate_delta=bool(args.validate_delta),
        read_table=read_table,
    )
    report_path.parent.mkdir(parents=True, exist_ok=True)
    _write_json(report_path, report)
    _write_summary(summary_path, report)
    return 0 if report["status"] == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
