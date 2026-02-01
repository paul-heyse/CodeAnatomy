"""Telemetry wrappers for CLI invocation."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass

from cyclopts import App
from cyclopts.exceptions import CycloptsError
from opentelemetry import trace

from cli.context import RunContext
from cli.exit_codes import ExitCode
from obs.otel.run_context import reset_run_id, set_run_id

_LOGGER = logging.getLogger(__name__)
tracer = trace.get_tracer("codeanatomy.cli")


@dataclass(frozen=True)
class CliInvokeEvent:
    """Structured telemetry event for CLI invocation."""

    ok: bool
    command: str | None
    parse_ms: float
    exec_ms: float
    exit_code: int
    error_class: str | None = None
    error_stage: str | None = None
    error_message: str | None = None


def invoke_with_telemetry(
    app: App,
    tokens: list[str] | None,
    *,
    run_context: RunContext | None,
) -> tuple[int, CliInvokeEvent]:
    """Execute CLI with telemetry capture.

    Parameters
    ----------
    app
        Cyclopts application to invoke.
    tokens
        CLI tokens (without program name).
    run_context
        Optional run context for injection.

    Returns
    -------
    tuple[int, CliInvokeEvent]
        Exit code and structured telemetry payload.
    """
    t0 = time.perf_counter()
    command_name = "<unknown>"
    run_token = None
    if run_context is not None:
        run_token = set_run_id(run_context.run_id)
    try:
        command, bound, ignored = app.parse_args(
            tokens,
            exit_on_error=False,
            print_error=True,
        )
        parse_ms = (time.perf_counter() - t0) * 1000.0
        command_name = getattr(command, "__qualname__", repr(command))

        if run_context is not None and "run_context" in ignored:
            bound.arguments["run_context"] = run_context

        t1 = time.perf_counter()
        with tracer.start_as_current_span("cli.command") as span:
            span.set_attribute("cli.command", command_name)
            if run_context is not None:
                span.set_attribute("cli.run_id", run_context.run_id)
            result = command(*bound.args, **bound.kwargs)
        exec_ms = (time.perf_counter() - t1) * 1000.0

        exit_code = result if isinstance(result, int) else 0
        return (
            exit_code,
            CliInvokeEvent(
                ok=True,
                command=command_name,
                parse_ms=parse_ms,
                exec_ms=exec_ms,
                exit_code=exit_code,
            ),
        )
    except CycloptsError as exc:
        parse_ms = (time.perf_counter() - t0) * 1000.0
        exit_code = ExitCode.from_exception(exc)
        return (
            exit_code,
            CliInvokeEvent(
                ok=False,
                command=command_name,
                parse_ms=parse_ms,
                exec_ms=0.0,
                exit_code=exit_code,
                error_class=f"cyclopts.{exc.__class__.__name__}",
                error_stage=_classify_error_stage(exc),
                error_message=str(exc),
            ),
        )
    except Exception as exc:
        # General exception handler for command execution errors
        exec_ms = (time.perf_counter() - t0) * 1000.0
        exit_code = ExitCode.from_exception(exc)
        _LOGGER.exception("Command execution failed.")
        return (
            exit_code,
            CliInvokeEvent(
                ok=False,
                command=command_name,
                parse_ms=0.0,
                exec_ms=exec_ms,
                exit_code=exit_code,
                error_class=f"{exc.__class__.__module__}.{exc.__class__.__name__}",
                error_stage="execution",
                error_message=str(exc),
            ),
        )
    finally:
        if run_token is not None:
            reset_run_id(run_token)


def _classify_error_stage(exc: CycloptsError) -> str:
    """Classify Cyclopts errors into CLI pipeline stages.

    Parameters
    ----------
    exc
        Cyclopts exception instance.

    Returns
    -------
    str
        Error stage label.
    """
    name = exc.__class__.__name__
    if name == "UnknownCommandError":
        return "command_resolve"
    if name in {"UnknownOptionError", "MissingArgumentError", "RepeatArgumentError"}:
        return "binding"
    if name == "CoercionError":
        return "coercion"
    if name == "ValidationError":
        return "validation"
    return "unknown"


__all__ = ["CliInvokeEvent", "invoke_with_telemetry"]
