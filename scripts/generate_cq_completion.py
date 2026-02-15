"""Generate static shell completion scripts for the CQ CLI."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.cq.cli_app.app import app
from tools.cq.cli_app.completion import (
    completion_scripts_need_update,
    generate_completion_scripts,
)


def _build_parser() -> argparse.ArgumentParser:
    """Build the CQ completion generator argument parser.

    Returns:
        argparse.ArgumentParser: Configured argument parser.
    """
    parser = argparse.ArgumentParser(description="Generate cq shell completion scripts.")
    parser.add_argument(
        "--output-dir",
        default="dist/completions",
        help="Output directory for completion scripts.",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Fail if completion scripts are missing or stale.",
    )
    return parser


def generate_cq_completions(output_dir: Path, *, program_name: str = "cq") -> dict[str, Path]:
    """Generate CQ completion scripts.

    Returns:
    -------
    dict[str, Path]
        Mapping of shell name to written completion file path.
    """
    return generate_completion_scripts(app, output_dir, program_name=program_name)


def cq_completions_need_update(output_dir: Path, *, program_name: str = "cq") -> list[Path]:
    """Return completion files that are missing or stale."""
    return completion_scripts_need_update(app, output_dir, program_name=program_name)


def main() -> int:
    """Generate completion scripts.

    Returns:
        int: Exit status code.
    """
    parser = _build_parser()
    args = parser.parse_args()
    output_dir = Path(args.output_dir)
    if args.check:
        return 1 if cq_completions_need_update(output_dir, program_name="cq") else 0
    generate_cq_completions(output_dir, program_name="cq")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
