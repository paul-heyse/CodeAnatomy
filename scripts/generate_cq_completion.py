"""Generate static shell completion scripts for the CQ CLI."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.cq.cli_app.app import app
from tools.cq.cli_app.completion import generate_completion_scripts


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
    return parser


def main() -> int:
    """Generate completion scripts.

    Returns:
        int: Exit status code.
    """
    parser = _build_parser()
    args = parser.parse_args()
    output_dir = Path(args.output_dir)
    generate_completion_scripts(app, output_dir, program_name="cq")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
