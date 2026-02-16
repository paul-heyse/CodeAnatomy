"""Telemetry wrappers for CLI invocation."""

from __future__ import annotations

import logging
import time
from collections.abc import Mapping
from dataclasses import dataclass, replace
from functools import lru_cache

from cyclopts import App
from cyclopts._result_action import handle_result_action
from cyclopts._run import _run_maybe_async_command
from cyclopts.bind import normalize_tokens
from cyclopts.exceptions import CycloptsError
from opentelemetry import trace

from cli.context import RunContext
from cli.exit_codes import ExitCode
from obs.otel import (
    SCOPE_OBS,
    emit_diagnostics_event,
    reset_run_id,
    resolve_otel_config,
    root_span,
    set_run_id,
    set_span_attributes,
)

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


@dataclass
class _InvokeState:
    t0: float
    command_name: str
    parse_ms: float | None = None
    exec_ms: float | None = None


def _command_name_from_tokens(tokens: list[str] | None) -> str:
    if not tokens:
        return "<unknown>"
    return tokens[0]


def _finalize_parse_ms(state: _InvokeState) -> None:
    if state.parse_ms is None:
        state.parse_ms = (time.perf_counter() - state.t0) * 1000.0


def _apply_result_action(app: App, result: object) -> int:
    processed = handle_result_action(
        result,
        app.app_stack.resolve(
            "result_action",
            fallback="print_non_int_return_int_as_exit_code",
        ),
        app.console.print,
    )
    return processed if isinstance(processed, int) else 0


def _run_with_app_stack(
    app: App,
    tokens: list[str] | None,
    *,
    run_context: RunContext | None,
    state: _InvokeState,
) -> int:
    normalized = normalize_tokens(tokens)
    overrides: dict[str, object] = {
        "print_error": True,
        "exit_on_error": False,
    }
    if app.result_action is None:
        overrides["result_action"] = "print_non_int_return_int_as_exit_code"
    with app.app_stack(normalized, overrides):
        command, bound, ignored = app.parse_args(
            normalized,
            exit_on_error=False,
            print_error=True,
        )
        state.parse_ms = (time.perf_counter() - state.t0) * 1000.0
        state.command_name = getattr(command, "__qualname__", repr(command))

        if run_context is not None and ignored:
            for name, hint in ignored.items():
                if hint is RunContext or name == "run_context":
                    bound.arguments[name] = run_context

        t1 = time.perf_counter()
        with tracer.start_as_current_span("cli.command") as span:
            span.set_attribute("cli.command", state.command_name)
            if run_context is not None:
                span.set_attribute("cli.run_id", run_context.run_id)
            result = _run_maybe_async_command(
                command,
                bound,
                app.app_stack.resolve("backend", fallback="asyncio"),
            )
        state.exec_ms = (time.perf_counter() - t1) * 1000.0
        return _apply_result_action(app, result)


def invoke_with_telemetry(
    app: App,
    tokens: list[str] | None,
    *,
    run_context: RunContext | None,
) -> tuple[int, CliInvokeEvent]:
    """Execute CLI with telemetry capture.

    Args:
        app: CLI app instance.
        tokens: Command tokens to execute.
        run_context: Optional runtime context for telemetry correlation.

    Returns:
        tuple[int, CliInvokeEvent]: Result.

    Raises:
        RuntimeError: If command dispatch fails unexpectedly.
    """
    state = _InvokeState(time.perf_counter(), _command_name_from_tokens(tokens))
    run_token = None
    if run_context is not None:
        run_token = set_run_id(run_context.run_id)
    event: CliInvokeEvent | None = None
    exit_code: int | None = None
    with root_span(
        "cli.invocation",
        scope_name=SCOPE_OBS,
        attributes={
            "cli.command": state.command_name,
            "cli.tokens": len(tokens or ()),
            "cli.run_id": run_context.run_id if run_context else None,
        },
    ) as span:
        snapshot = get_otel_config_snapshot()
        emit_diagnostics_event(
            "otel_config_snapshot_v1",
            payload={
                "diagnostic.severity": "info",
                "diagnostic.category": "telemetry_config",
                "config": dict(snapshot),
            },
            event_kind="artifact",
        )
        if run_context is not None:
            run_context = replace(run_context, span=span)
        try:
            exit_code = _run_with_app_stack(app, tokens, run_context=run_context, state=state)
            event = CliInvokeEvent(
                ok=True,
                command=state.command_name,
                parse_ms=state.parse_ms or 0.0,
                exec_ms=state.exec_ms or 0.0,
                exit_code=exit_code,
            )
        except CycloptsError as exc:
            _finalize_parse_ms(state)
            exit_code = ExitCode.from_exception(exc)
            event = CliInvokeEvent(
                ok=False,
                command=state.command_name,
                parse_ms=state.parse_ms or 0.0,
                exec_ms=0.0,
                exit_code=exit_code,
                error_class=f"cyclopts.{exc.__class__.__name__}",
                error_stage=_classify_error_stage(exc),
                error_message=str(exc),
            )
        except Exception as exc:
            _finalize_parse_ms(state)
            if state.exec_ms is None:
                state.exec_ms = (time.perf_counter() - state.t0) * 1000.0
            exit_code = ExitCode.from_exception(exc)
            _LOGGER.exception("Command execution failed.")
            event = CliInvokeEvent(
                ok=False,
                command=state.command_name,
                parse_ms=state.parse_ms or 0.0,
                exec_ms=state.exec_ms or 0.0,
                exit_code=exit_code,
                error_class=f"{exc.__class__.__module__}.{exc.__class__.__name__}",
                error_stage="execution",
                error_message=str(exc),
            )
        finally:
            if event is not None:
                set_span_attributes(
                    span,
                    {
                        "cli.exit_code": event.exit_code,
                        "cli.ok": event.ok,
                        "cli.parse_ms": event.parse_ms,
                        "cli.exec_ms": event.exec_ms,
                    },
                )
                emit_diagnostics_event(
                    "cli_invocation_v1",
                    payload={
                        "command": event.command,
                        "ok": event.ok,
                        "parse_ms": event.parse_ms,
                        "exec_ms": event.exec_ms,
                        "exit_code": event.exit_code,
                        "error_class": event.error_class,
                        "error_stage": event.error_stage,
                    },
                    event_kind="event",
                )
            if run_token is not None:
                reset_run_id(run_token)
    if exit_code is None or event is None:
        msg = "CLI invocation did not produce a result."
        raise RuntimeError(msg)
    return exit_code, event


def _classify_error_stage(exc: CycloptsError) -> str:
    """Classify Cyclopts errors into CLI pipeline stages.

    Parameters
    ----------
    exc
        Cyclopts exception instance.

    Returns:
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


def apply_telemetry_config(*_args: object, **_kwargs: object) -> None:
    """Resolve and cache OpenTelemetry config during CLI parse."""
    get_otel_config_snapshot()


@lru_cache(maxsize=1)
def get_otel_config_snapshot() -> Mapping[str, object]:
    """Return the cached OpenTelemetry config snapshot.

    Returns:
    -------
    Mapping[str, object]
        Immutable snapshot of resolved OpenTelemetry configuration.
    """
    config = resolve_otel_config(None)
    return dict(config.fingerprint_payload())


__all__ = [
    "CliInvokeEvent",
    "apply_telemetry_config",
    "get_otel_config_snapshot",
    "invoke_with_telemetry",
]
