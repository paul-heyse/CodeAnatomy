"""CQ CLI application built with cyclopts.

This package provides the cyclopts-based CLI for the cq tool.
"""

from __future__ import annotations

from tools.cq.cli_app.context import CliContext, CliResult

__all__ = ["CliContext", "CliResult", "main", "main_async"]


def _normalize_exit_code(value: object) -> int:
    if isinstance(value, bool):
        return 0 if value else 1
    return value if isinstance(value, int) else 0


def main() -> int:
    """Run the CLI application.

    Returns:
    -------
    int
        Exit code.
    """
    from tools.cq.cli_app.app import app

    # Use meta app to enable global option handling via launcher
    return _normalize_exit_code(app.meta())


async def main_async(tokens: list[str] | None = None) -> int:
    """Run the CQ CLI in async contexts without creating a nested event loop.

    Returns:
    -------
    int
        Exit code.
    """
    from tools.cq.cli_app.app import app

    out = await app.meta.run_async(
        tokens=tokens,
        exit_on_error=False,
        print_error=True,
        result_action="return_int_as_exit_code_else_zero",
    )
    return _normalize_exit_code(out)
