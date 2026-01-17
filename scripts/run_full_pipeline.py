#!/usr/bin/env python3
"""Run the full Hamilton pipeline against a repo."""

from __future__ import annotations

import argparse
import logging
from collections.abc import Sequence

from config import AdapterMode
from hamilton_pipeline import PipelineExecutionOptions, execute_pipeline
from hamilton_pipeline.pipeline_types import ScipIndexConfig


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the CodeAnatomy Hamilton pipeline.")
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
        "--use-datafusion-bridge",
        action="store_true",
        help="Enable DataFusion bridge execution.",
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
    return parser


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


def main(argv: Sequence[str] | None = None) -> int:
    """Run the pipeline with CLI-provided options.

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
    adapter_mode = AdapterMode(
        use_ibis_bridge=True,
        use_datafusion_bridge=args.use_datafusion_bridge,
    )
    options = PipelineExecutionOptions(
        output_dir=args.output_dir,
        work_dir=args.work_dir,
        scip_index_config=scip_config,
        adapter_mode=adapter_mode,
        incremental_impact_strategy=args.incremental_impact_strategy,
    )
    results = execute_pipeline(repo_root=args.repo_root, options=options)
    logger.info("Pipeline complete. Outputs: %s", sorted(results))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
