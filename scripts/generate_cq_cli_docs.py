"""Generate CQ CLI documentation from the live cyclopts app graph."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.cq.cli_app.app import app


def _build_parser() -> argparse.ArgumentParser:
    """Build the CLI documentation generator argument parser.

    Returns:
        argparse.ArgumentParser: Configured argument parser.
    """
    parser = argparse.ArgumentParser(description="Generate CQ CLI reference docs.")
    parser.add_argument(
        "--output",
        default="docs/reference/cq_cli.md",
        help="Output path for generated markdown docs.",
    )
    return parser


def main() -> int:
    """Render and write CQ CLI docs.

    Returns:
        int: Exit status code.
    """
    parser = _build_parser()
    args = parser.parse_args()

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    docs = app.generate_docs(output_format="markdown", recursive=True, include_hidden=False)
    output_path.write_text(docs, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
