#!/usr/bin/env python3
"""Generate semantic graph documentation artifacts."""

from __future__ import annotations

import sys
from pathlib import Path

from semantics.docs.graph_docs import export_graph_documentation

DEFAULT_OUTPUT = Path("docs/architecture/semantic_pipeline_graph.md")


def main() -> None:
    """Generate semantic graph docs and write to disk."""
    output = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_OUTPUT
    output.parent.mkdir(parents=True, exist_ok=True)
    export_graph_documentation(str(output))


if __name__ == "__main__":
    main()
