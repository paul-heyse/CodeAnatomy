"""Generate static shell completion scripts for the CodeAnatomy CLI."""

from __future__ import annotations

import argparse
from pathlib import Path

from cli.app import app
from cli.completion import generate_completion_scripts


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate codeanatomy shell completion scripts.")
    parser.add_argument(
        "--output-dir",
        default="dist/completions",
        help="Output directory for completion scripts.",
    )
    return parser


def main() -> int:
    """Generate completion scripts and return an exit code.

    Returns
    -------
    int
        Exit status code.
    """
    parser = _build_parser()
    args = parser.parse_args()
    output_dir = Path(args.output_dir)
    generate_completion_scripts(app, output_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
