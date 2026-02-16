"""Interactive shell command for CQ CLI."""

from __future__ import annotations

from inspect import BoundArguments
from typing import Annotated, Any

from cyclopts import Parameter

from tools.cq.cli_app.context import CliContext
from tools.cq.cli_app.infrastructure import dispatch_bound_command, require_context


def _dispatch_with_ctx(
    ctx: CliContext,
    command: Any,
    bound: BoundArguments,
    ignored: dict[str, Any],
) -> Any:
    """Inject CQ context into parse=False params and dispatch the command.

    Returns:
        Any: Command return value.
    """
    if "ctx" in ignored:
        bound.arguments["ctx"] = ctx
    return dispatch_bound_command(command, bound)


def repl(
    *,
    ctx: Annotated[CliContext | None, Parameter(parse=False)] = None,
) -> int:
    """Launch an interactive CQ shell session.

    Args:
        ctx: Injected CLI context.

    Returns:
        int: Process exit code.
    """
    from tools.cq.cli_app.app import app

    resolved_ctx = require_context(ctx)
    app.interactive_shell(
        prompt="cq> ",
        result_action="print_non_int_return_int_as_exit_code",
        exit_on_error=False,
        dispatcher=lambda command, bound, ignored: _dispatch_with_ctx(
            resolved_ctx,
            command,
            bound,
            ignored,
        ),
    )
    return 0


def repl_help(
    *tokens: Annotated[str, Parameter(show=False, allow_leading_hyphen=True)],
    ctx: Annotated[CliContext | None, Parameter(parse=False)] = None,
) -> int:
    """Print help from inside CQ REPL sessions.

    Returns:
        int: Process exit code.
    """
    from tools.cq.cli_app.app import app

    _ = require_context(ctx)
    app.help_print(tokens=list(tokens))
    return 0


__all__ = ["repl", "repl_help"]
