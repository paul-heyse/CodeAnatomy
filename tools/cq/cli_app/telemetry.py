"""Telemetry wrappers for CQ CLI invocation."""

from __future__ import annotations

import time

from cyclopts import App
from cyclopts.bind import normalize_tokens
from cyclopts.exceptions import CycloptsError

from tools.cq.cli_app.context import CliContext
from tools.cq.cli_app.infrastructure import dispatch_bound_command
from tools.cq.cli_app.result_action import (
    CQ_DEFAULT_RESULT_ACTION,
    apply_result_action,
)
from tools.cq.cli_app.telemetry_events import CqInvokeEvent, build_invoke_event
from tools.cq.utils.uuid_temporal_contracts import resolve_run_identity_contract

_INVOCATION_RUNTIME_ERRORS = (OSError, RuntimeError, TypeError, ValueError)


def _apply_result_action(app: App, result: object) -> int:
    action = app.app_stack.resolve("result_action", fallback=CQ_DEFAULT_RESULT_ACTION)
    processed = apply_result_action(result, action)
    if isinstance(processed, bool):
        return 0 if processed else 1
    return processed if isinstance(processed, int) else 0


def _classify_error_stage(exc: CycloptsError) -> str:
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


def invoke_with_telemetry(
    app: App,
    tokens: list[str] | None,
    *,
    ctx: CliContext,
) -> tuple[int, CqInvokeEvent]:
    """Execute a CQ command with parse/execute timing telemetry.

    Returns:
        tuple[int, CqInvokeEvent]: exit code and telemetry event.
    """
    normalized = normalize_tokens(tokens)
    t0 = time.perf_counter()
    command_name: str | None = normalized[0] if normalized else None
    event_identity = resolve_run_identity_contract(None)

    with app.app_stack(
        normalized,
        {
            "print_error": True,
            "exit_on_error": False,
        },
    ):
        try:
            command, bound, ignored = app.parse_args(
                normalized,
                exit_on_error=False,
                print_error=True,
            )
            parse_ms = (time.perf_counter() - t0) * 1000.0
            command_name = getattr(command, "__qualname__", command_name)
            if "ctx" in ignored:
                bound.arguments["ctx"] = ctx

            t1 = time.perf_counter()
            try:
                result = dispatch_bound_command(command, bound)
            except CycloptsError as exc:
                exec_ms = (time.perf_counter() - t1) * 1000.0
                event = build_invoke_event(
                    ok=False,
                    command=command_name,
                    parse_ms=parse_ms,
                    exec_ms=exec_ms,
                    exit_code=2,
                    error_class=f"cyclopts.{exc.__class__.__name__}",
                    error_stage=_classify_error_stage(exc),
                    event_id=event_identity.run_id,
                    event_uuid_version=event_identity.run_uuid_version,
                    event_created_ms=event_identity.run_created_ms,
                )
                return 2, event
            else:
                exit_code = _apply_result_action(app, result)
                exec_ms = (time.perf_counter() - t1) * 1000.0
                event = build_invoke_event(
                    ok=True,
                    command=command_name,
                    parse_ms=parse_ms,
                    exec_ms=exec_ms,
                    exit_code=exit_code,
                    event_id=event_identity.run_id,
                    event_uuid_version=event_identity.run_uuid_version,
                    event_created_ms=event_identity.run_created_ms,
                )
                return exit_code, event
        except CycloptsError as exc:
            parse_ms = (time.perf_counter() - t0) * 1000.0
            event = build_invoke_event(
                ok=False,
                command=command_name,
                parse_ms=parse_ms,
                exec_ms=0.0,
                exit_code=2,
                error_class=f"cyclopts.{exc.__class__.__name__}",
                error_stage=_classify_error_stage(exc),
                event_id=event_identity.run_id,
                event_uuid_version=event_identity.run_uuid_version,
                event_created_ms=event_identity.run_created_ms,
            )
            return 2, event
        except _INVOCATION_RUNTIME_ERRORS as exc:
            exec_ms = (time.perf_counter() - t0) * 1000.0
            event = build_invoke_event(
                ok=False,
                command=command_name,
                parse_ms=(time.perf_counter() - t0) * 1000.0,
                exec_ms=exec_ms,
                exit_code=1,
                error_class=f"runtime.{exc.__class__.__name__}",
                error_stage="execution",
                event_id=event_identity.run_id,
                event_uuid_version=event_identity.run_uuid_version,
                event_created_ms=event_identity.run_created_ms,
            )
            return 1, event


__all__ = ["CqInvokeEvent", "invoke_with_telemetry"]
