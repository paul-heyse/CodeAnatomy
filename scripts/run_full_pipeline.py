#!/usr/bin/env python3
"""Run the graph product build against a repo."""

from __future__ import annotations

import argparse
import logging
from collections.abc import Sequence

from core_types import DeterminismTier
from graph import GraphProductBuildRequest, build_graph_product
from hamilton_pipeline.types import ExecutionMode, ExecutorConfig, ScipIndexConfig


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the CodeAnatomy graph product build.")
    parser.add_argument("--repo-root", default=".", help="Repository root to scan.")
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Output directory for artifacts (default: <repo_root>/build).",
    )
    parser.add_argument(
        "--work-dir",
        default=None,
        help="Working directory for intermediates (default: unset).",
    )
    parser.add_argument(
        "--disable-scip",
        action="store_true",
        help="Disable scip-python indexing.",
    )
    parser.add_argument(
        "--scip-output-dir",
        default=None,
        help="SCIP output directory (relative to repo root unless absolute).",
    )
    parser.add_argument(
        "--scip-index-path-override",
        default=None,
        help="Override the index.scip path.",
    )
    parser.add_argument(
        "--scip-python-bin",
        default="scip-python",
        help="scip-python executable to use.",
    )
    parser.add_argument(
        "--scip-target-only",
        default=None,
        help="Optional target file/module to index.",
    )
    parser.add_argument(
        "--scip-timeout-s",
        type=int,
        default=None,
        help="Timeout in seconds for scip-python.",
    )
    parser.add_argument(
        "--scip-env-json",
        default=None,
        help="Path to SCIP environment JSON.",
    )
    parser.add_argument(
        "--node-max-old-space-mb",
        type=int,
        default=None,
        help="Node.js memory cap for scip-python.",
    )
    parser.add_argument(
        "--runtime-profile",
        default=None,
        help="Runtime profile name (default: from environment).",
    )
    parser.add_argument(
        "--determinism-tier",
        default=None,
        choices=(
            "tier2",
            "canonical",
            "tier1",
            "stable",
            "stable_set",
            "tier0",
            "fast",
            "best_effort",
        ),
        help="Determinism tier override (default: from environment).",
    )
    parser.add_argument(
        "--writer-strategy",
        default=None,
        choices=("arrow", "datafusion"),
        help="Writer strategy override for materialization.",
    )
    parser.add_argument(
        "--incremental-impact-strategy",
        choices=("hybrid", "symbol_closure", "import_closure"),
        default=None,
        help="Impact strategy for incremental runs.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level (DEBUG, INFO, WARNING, ERROR).",
    )
    parser.add_argument(
        "--execution-mode",
        default=ExecutionMode.PLAN_PARALLEL.value,
        choices=tuple(mode.value for mode in ExecutionMode),
        help="Execution mode for Hamilton orchestration.",
    )
    parser.add_argument(
        "--executor-kind",
        default=None,
        choices=("threadpool", "multiprocessing", "dask", "ray"),
        help="Primary executor kind for dynamic execution.",
    )
    parser.add_argument(
        "--executor-max-tasks",
        type=int,
        default=None,
        help="Max tasks for the primary executor.",
    )
    parser.add_argument(
        "--executor-remote-kind",
        default=None,
        choices=("threadpool", "multiprocessing", "dask", "ray"),
        help="Remote executor kind for scan/high-cost tasks.",
    )
    parser.add_argument(
        "--executor-remote-max-tasks",
        type=int,
        default=None,
        help="Max tasks for the remote executor.",
    )
    parser.add_argument(
        "--executor-cost-threshold",
        type=float,
        default=None,
        help="Cost threshold for routing tasks to the remote executor.",
    )
    return parser


def _parse_determinism_tier(value: str | None) -> DeterminismTier | None:
    if value is None:
        return None
    normalized = value.strip().lower()
    mapping: dict[str, DeterminismTier] = {
        "tier2": DeterminismTier.CANONICAL,
        "canonical": DeterminismTier.CANONICAL,
        "tier1": DeterminismTier.STABLE_SET,
        "stable": DeterminismTier.STABLE_SET,
        "stable_set": DeterminismTier.STABLE_SET,
        "tier0": DeterminismTier.BEST_EFFORT,
        "fast": DeterminismTier.BEST_EFFORT,
        "best_effort": DeterminismTier.BEST_EFFORT,
    }
    return mapping.get(normalized)


def _build_scip_config(args: argparse.Namespace) -> ScipIndexConfig:
    output_dir = args.scip_output_dir or "build/scip"
    return ScipIndexConfig(
        enabled=not args.disable_scip,
        output_dir=output_dir,
        index_path_override=args.scip_index_path_override,
        env_json_path=args.scip_env_json,
        scip_python_bin=args.scip_python_bin,
        target_only=args.scip_target_only,
        node_max_old_space_mb=args.node_max_old_space_mb,
        timeout_s=args.scip_timeout_s,
    )


def _build_executor_config(args: argparse.Namespace) -> ExecutorConfig | None:
    if (
        args.executor_kind is None
        and args.executor_max_tasks is None
        and args.executor_remote_kind is None
        and args.executor_remote_max_tasks is None
        and args.executor_cost_threshold is None
    ):
        return None
    return ExecutorConfig(
        kind=args.executor_kind or "multiprocessing",
        max_tasks=args.executor_max_tasks or 4,
        remote_kind=args.executor_remote_kind,
        remote_max_tasks=args.executor_remote_max_tasks,
        cost_threshold=args.executor_cost_threshold,
    )


def main(argv: Sequence[str] | None = None) -> int:
    """Run the graph product build with CLI-provided options.

    Returns
    -------
    int
        Exit status code.
    """
    parser = _build_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(level=args.log_level.upper())
    logger = logging.getLogger("codeanatomy.pipeline")

    scip_config = _build_scip_config(args)
    determinism_override = _parse_determinism_tier(args.determinism_tier)
    execution_mode = ExecutionMode(args.execution_mode)
    executor_config = _build_executor_config(args)
    result = build_graph_product(
        GraphProductBuildRequest(
            repo_root=args.repo_root,
            output_dir=args.output_dir,
            work_dir=args.work_dir,
            runtime_profile_name=args.runtime_profile,
            determinism_override=determinism_override,
            writer_strategy=args.writer_strategy,
            execution_mode=execution_mode,
            executor_config=executor_config,
            scip_index_config=scip_config,
            incremental_impact_strategy=args.incremental_impact_strategy,
            include_quality=True,
            include_extract_errors=True,
            include_manifest=True,
            include_run_bundle=True,
            config={},
        )
    )
    logger.info(
        "Build complete. Output dir=%s bundle=%s",
        result.output_dir,
        result.run_bundle_dir,
    )
    logger.info("Pipeline outputs: %s", sorted(result.pipeline_outputs))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
