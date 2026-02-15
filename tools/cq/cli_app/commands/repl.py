"""Interactive shell command for CQ CLI."""

from __future__ import annotations

from typing import Annotated

from cyclopts import Parameter

from tools.cq.cli_app.context import CliContext
from tools.cq.cli_app.decorators import require_context, require_ctx


@require_ctx
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

    require_context(ctx)
    app.interactive_shell(
        prompt="cq> ",
        result_action="print_non_int_return_int_as_exit_code",
        exit_on_error=False,
    )
    return 0


__all__ = ["repl"]
