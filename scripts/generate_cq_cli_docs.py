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
    parser.add_argument(
        "--check",
        action="store_true",
        help="Fail if the output file is missing or stale.",
    )
    return parser


def _render_cq_reference() -> str:
    return app.generate_docs(output_format="markdown", recursive=True, include_hidden=False)


def generate_cq_reference(output_path: Path) -> str:
    """Render and write CQ CLI docs.

    Returns:
    -------
    str
        Rendered markdown documentation text.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    docs = _render_cq_reference()
    output_path.write_text(docs, encoding="utf-8")
    return docs


def cq_reference_needs_update(output_path: Path) -> bool:
    """Return whether CQ docs are missing or stale."""
    if not output_path.exists():
        return True
    return output_path.read_text(encoding="utf-8") != _render_cq_reference()


def main() -> int:
    """Render, write, or freshness-check CQ CLI docs.

    Returns:
        int: Exit status code.
    """
    parser = _build_parser()
    args = parser.parse_args()

    output_path = Path(args.output)
    if args.check:
        return 1 if cq_reference_needs_update(output_path) else 0
    generate_cq_reference(output_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
