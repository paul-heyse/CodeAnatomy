"""Result action handler for Cyclopts integration."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from cli.exit_codes import ExitCode

if TYPE_CHECKING:
    from cyclopts import App


def cli_result_action(
    app: App,
    cmd: object,
    result: Any,
) -> int:
    """Handle command results and convert to exit codes.

    This function is registered as the ``result_action`` for the CLI app.
    It normalizes different return types to integer exit codes.

    Parameters
    ----------
    app
        The Cyclopts application instance.
    cmd
        The resolved command that was executed.
    result
        The return value from the command function.

    Returns
    -------
    int
        Exit code for the process.
    """
    _ = app  # Reserved for future use
    _ = cmd  # Reserved for future use
    from rich.console import Console

    console = Console()

    if result is None:
        return ExitCode.SUCCESS

    if isinstance(result, int):
        return result

    # Import CliResult here to avoid circular imports
    from cli.result import CliResult

    if isinstance(result, CliResult):
        if result.summary:
            console.print(result.summary)
        if result.artifacts:
            console.print("Artifacts:")
            for name, path in sorted(result.artifacts.items()):
                console.print(f"  {name}: {path}")
        if result.metrics:
            duration = result.metrics.get("duration_ms")
            if duration is not None:
                console.print(f"Duration: {duration:.1f}ms")
        return int(result.exit_code)

    console.print(f"Unexpected command return type: {type(result).__name__} (value: {result!r})")
    return ExitCode.GENERAL_ERROR


__all__ = ["cli_result_action"]
