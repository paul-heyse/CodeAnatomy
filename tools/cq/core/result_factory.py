"""Factory for building canonical CQ error results."""

from __future__ import annotations

from pathlib import Path

from tools.cq.core.run_context import RunContext
from tools.cq.core.schema import CqResult, mk_result
from tools.cq.core.toolchain import Toolchain


def build_error_result(
    *,
    macro: str,
    root: Path | str,
    argv: list[str],
    tc: Toolchain | None,
    started_ms: float,
    error: Exception | str,
) -> CqResult:
    """Build a canonical error CqResult with consistent structure.

    Parameters
    ----------
    macro : str
        The macro name (e.g., "query", "run", "report").
    root : Path | str
        Repository root path.
    argv : list[str]
        Command-line arguments.
    tc : Toolchain | None
        Toolchain snapshot.
    started_ms : float
        Start time in milliseconds since epoch.
    error : Exception | str
        Error to record in the result summary.

    Returns:
    -------
    CqResult
        Error result with run metadata and error summary.
    """
    root = Path(root) if isinstance(root, str) else root
    run_ctx = RunContext.from_parts(root=root, argv=argv, tc=tc, started_ms=started_ms)
    result = mk_result(run_ctx.to_runmeta(macro))
    result.summary["error"] = str(error)
    return result


__all__ = [
    "build_error_result",
]
