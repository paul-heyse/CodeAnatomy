"""Telemetry wrappers for CQ CLI invocation."""

from __future__ import annotations

import time
from dataclasses import dataclass

from cyclopts import App
from cyclopts._result_action import handle_result_action
from cyclopts._run import _run_maybe_async_command
from cyclopts.bind import normalize_tokens
from cyclopts.exceptions import CycloptsError

from tools.cq.cli_app.context import CliContext
from tools.cq.cli_app.result_action import cq_result_action
from tools.cq.utils.uuid_temporal_contracts import resolve_run_identity_contract


@dataclass(frozen=True, slots=True)
class CqInvokeEvent:
    """Structured CQ invocation telemetry payload."""

    ok: bool
    command: str | None
    parse_ms: float
    exec_ms: float
    exit_code: int
    error_class: str | None = None
    error_stage: str | None = None
    event_id: str | None = None
    event_uuid_version: int | None = None
    event_created_ms: int | None = None


def _apply_result_action(app: App, result: object) -> int:
    action = app.app_stack.resolve("result_action", fallback=cq_result_action)
    processed = handle_result_action(result, action, app.console.print)
    return int(processed) if isinstance(processed, int) else 0


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
                result = _run_maybe_async_command(
                    command,
                    bound,
                    app.app_stack.resolve("backend", fallback="asyncio"),
                )
            except CycloptsError as exc:
                exec_ms = (time.perf_counter() - t1) * 1000.0
                event = CqInvokeEvent(
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
                event = CqInvokeEvent(
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
            event = CqInvokeEvent(
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
        except Exception as exc:
            exec_ms = (time.perf_counter() - t0) * 1000.0
            event = CqInvokeEvent(
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
