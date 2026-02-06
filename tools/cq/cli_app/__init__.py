"""CQ CLI application built with cyclopts.

This package provides the cyclopts-based CLI for the cq tool.
"""

from __future__ import annotations

from tools.cq.cli_app.context import CliContext, CliResult

__all__ = ["CliContext", "CliResult", "main"]


def main() -> int:
    """Run the CLI application.

    Returns:
    -------
    int
        Exit code.
    """
    from tools.cq.cli_app.app import app

    # Use meta app to enable global option handling via launcher
    return app.meta()
