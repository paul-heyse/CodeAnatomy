"""Tests for CLI invocation telemetry helpers."""

from __future__ import annotations

from typing import Annotated

from cyclopts import App, Parameter

from cli.context import RunContext
from cli.telemetry import invoke_with_telemetry


def test_invoke_with_telemetry_injects_run_context() -> None:
    """Ensure run_context is injected into parse=False parameters."""
    app = App()
    captured: dict[str, object] = {}

    @app.command
    def hello(*, run_context: Annotated[RunContext | None, Parameter(parse=False)] = None) -> int:
        """Return success after capturing run context.

        Returns:
        -------
        int
            Exit status code.
        """
        captured["run_id"] = run_context.run_id if run_context else None
        return 0

    run_context = RunContext(run_id="run-123", log_level="INFO", config_contents={})
    exit_code, event = invoke_with_telemetry(app, ["hello"], run_context=run_context)

    assert exit_code == 0
    assert captured["run_id"] == "run-123"
    assert event.ok is True
    assert event.command is not None
