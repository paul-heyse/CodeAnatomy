"""Substrait compilation bridge for Ibis expressions.

This module provides infrastructure for compiling Ibis expressions directly to
Substrait bytes, enabling a dual-lane compilation strategy that prefers
Substrait over SQL string generation when supported.
"""

from __future__ import annotations

import importlib.util
from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING

from ibis.expr.types import Table as IbisTable

if TYPE_CHECKING:
    from datafusion_engine.diagnostics import DiagnosticsSink

PYARROW_SUBSTRAIT_AVAILABLE = importlib.util.find_spec("pyarrow.substrait") is not None
IBIS_SUBSTRAIT_AVAILABLE = importlib.util.find_spec("ibis_substrait") is not None


@dataclass(frozen=True)
class SubstraitCompilationResult:
    """Result of attempting Ibis-to-Substrait compilation.

    Attributes
    ----------
    plan_bytes : bytes | None
        Serialized Substrait plan when compilation succeeds, otherwise ``None``.
    success : bool
        Whether compilation succeeded without errors.
    errors : tuple[str, ...]
        Error messages encountered during compilation.
    expr_type : str | None
        Type name of the expression attempted (for diagnostics).
    """

    plan_bytes: bytes | None
    success: bool
    errors: tuple[str, ...]
    expr_type: str | None = None


def _coerce_diagnostics_sink(
    sink: DiagnosticsSink | None,
    *,
    session_id: str | None,
) -> DiagnosticsSink | None:
    if sink is None:
        return None
    if session_id is None:
        return sink
    from datafusion_engine.diagnostics import ensure_recorder_sink

    return ensure_recorder_sink(sink, session_id=session_id)


def ibis_to_substrait_bytes(
    expr: IbisTable,
    *,
    record_gaps: bool = False,
    diagnostics_sink: DiagnosticsSink | None = None,
    session_id: str | None = None,
) -> bytes:
    """Compile an Ibis expression to Substrait bytes.

    This function attempts direct Ibis-to-Substrait compilation as an
    alternative to SQL generation. When compilation fails, it raises
    an exception to trigger AST fallback in the dual-lane strategy.

    Parameters
    ----------
    expr : IbisTable
        Ibis table expression to compile.
    record_gaps : bool, optional
        Whether to record unsupported expressions in diagnostics.
        Default is False.
    diagnostics_sink : DiagnosticsSink | None, optional
        Diagnostics sink for recording gap information.
    session_id : str | None, optional
        Optional session identifier for recorder adaptation.

    Returns
    -------
    bytes
        Serialized Substrait plan bytes.

    Raises
    ------
    RuntimeError
        Raised when Substrait compilation fails.

    Examples
    --------
    >>> import ibis
    >>> from ibis_engine.substrait_bridge import ibis_to_substrait_bytes
    >>> backend = ibis.datafusion.connect()
    >>> expr = backend.table("my_table").select("col1", "col2")
    >>> plan_bytes = ibis_to_substrait_bytes(expr)  # doctest: +SKIP
    """
    resolved_sink = _coerce_diagnostics_sink(diagnostics_sink, session_id=session_id)
    result = _try_ibis_substrait(
        expr,
        record_gaps=record_gaps,
        diagnostics_sink=resolved_sink,
    )

    if not result.success or result.plan_bytes is None:
        errors = "; ".join(result.errors) if result.errors else "unknown compilation failure"
        msg = f"Substrait compilation failed: {errors}"
        raise RuntimeError(msg)

    return result.plan_bytes


def _try_ibis_substrait(
    expr: IbisTable,
    *,
    record_gaps: bool,
    diagnostics_sink: DiagnosticsSink | None = None,
) -> SubstraitCompilationResult:
    """Attempt Ibis-to-Substrait compilation with error capture.

    This internal function attempts direct Substrait compilation and captures
    any errors for diagnostics. It does not raise exceptions on failure.

    Parameters
    ----------
    expr : IbisTable
        Ibis table expression to compile.
    record_gaps : bool
        Whether to record unsupported expressions in diagnostics.
    diagnostics_sink : DiagnosticsSink | None
        Diagnostics sink for recording gap information.

    Returns
    -------
    SubstraitCompilationResult
        Compilation result with plan bytes or error information.
    """
    expr_result = _try_substrait_from_expr(
        expr,
        record_gaps=record_gaps,
        diagnostics_sink=diagnostics_sink,
    )
    if expr_result is not None:
        return expr_result

    ibis_substrait_result = _try_substrait_from_ibis_substrait(
        expr,
        record_gaps=record_gaps,
        diagnostics_sink=diagnostics_sink,
    )
    if ibis_substrait_result is not None:
        return ibis_substrait_result

    if not PYARROW_SUBSTRAIT_AVAILABLE:
        error_msg = "pyarrow.substrait is not available"
        return _substrait_failure(
            expr=expr,
            error_msg=error_msg,
            record_gaps=record_gaps,
            diagnostics_sink=diagnostics_sink,
        )

    backend_result = _try_substrait_from_backend(
        expr,
        record_gaps=record_gaps,
        diagnostics_sink=diagnostics_sink,
    )
    if backend_result is not None:
        return backend_result

    error_msg = "Ibis expression does not support Substrait compilation"
    return _substrait_failure(
        expr=expr,
        error_msg=error_msg,
        record_gaps=record_gaps,
        diagnostics_sink=diagnostics_sink,
    )


def _try_substrait_from_expr(
    expr: IbisTable,
    *,
    record_gaps: bool,
    diagnostics_sink: DiagnosticsSink | None,
) -> SubstraitCompilationResult | None:
    to_substrait = getattr(expr, "to_substrait", None)
    if not callable(to_substrait):
        return None
    try:
        plan_bytes = to_substrait()
    except (RuntimeError, TypeError, ValueError, AttributeError) as exc:
        return _substrait_failure(
            expr=expr,
            error_msg=str(exc),
            record_gaps=record_gaps,
            diagnostics_sink=diagnostics_sink,
        )
    if isinstance(plan_bytes, bytes) and plan_bytes:
        return SubstraitCompilationResult(
            plan_bytes=plan_bytes,
            success=True,
            errors=(),
            expr_type=type(expr).__name__,
        )
    error_msg = "to_substrait() returned empty or invalid bytes"
    return _substrait_failure(
        expr=expr,
        error_msg=error_msg,
        record_gaps=record_gaps,
        diagnostics_sink=diagnostics_sink,
    )


def _try_substrait_from_backend(
    expr: IbisTable,
    *,
    record_gaps: bool,
    diagnostics_sink: DiagnosticsSink | None,
) -> SubstraitCompilationResult | None:
    backend = getattr(expr, "_find_backend", lambda: None)()
    if backend is None:
        backend = getattr(expr, "backend", None)
    if backend is None:
        return None
    compile_substrait = getattr(backend, "compile_substrait", None)
    if not callable(compile_substrait):
        return None
    try:
        plan = compile_substrait(expr)
    except (RuntimeError, TypeError, ValueError, AttributeError) as exc:
        return _substrait_failure(
            expr=expr,
            error_msg=str(exc),
            record_gaps=record_gaps,
            diagnostics_sink=diagnostics_sink,
        )
    serialize = getattr(plan, "SerializeToString", None)
    if callable(serialize):
        plan_bytes = serialize()
        if isinstance(plan_bytes, bytes) and plan_bytes:
            return SubstraitCompilationResult(
                plan_bytes=plan_bytes,
                success=True,
                errors=(),
                expr_type=type(expr).__name__,
            )
    error_msg = "Substrait compiler returned empty or invalid bytes"
    return _substrait_failure(
        expr=expr,
        error_msg=error_msg,
        record_gaps=record_gaps,
        diagnostics_sink=diagnostics_sink,
    )


def _try_substrait_from_ibis_substrait(
    expr: IbisTable,
    *,
    record_gaps: bool,
    diagnostics_sink: DiagnosticsSink | None,
) -> SubstraitCompilationResult | None:
    if not IBIS_SUBSTRAIT_AVAILABLE:
        return None
    try:
        from ibis_substrait.compiler.core import SubstraitCompiler
    except (ImportError, RuntimeError, TypeError, ValueError):
        return None
    try:
        compiler = SubstraitCompiler()
        plan = compiler.compile(expr)
    except (RuntimeError, TypeError, ValueError) as exc:
        return _substrait_failure(
            expr=expr,
            error_msg=str(exc),
            record_gaps=record_gaps,
            diagnostics_sink=diagnostics_sink,
        )
    if isinstance(plan, bytes):
        return SubstraitCompilationResult(
            plan_bytes=plan,
            success=True,
            errors=(),
            expr_type=type(expr).__name__,
        )
    serializer = getattr(plan, "SerializeToString", None)
    if callable(serializer):
        plan_bytes = serializer()
        if isinstance(plan_bytes, bytes) and plan_bytes:
            return SubstraitCompilationResult(
                plan_bytes=plan_bytes,
                success=True,
                errors=(),
                expr_type=type(expr).__name__,
            )
    return _substrait_failure(
        expr=expr,
        error_msg="ibis-substrait returned empty or invalid bytes",
        record_gaps=record_gaps,
        diagnostics_sink=diagnostics_sink,
    )


def _substrait_failure(
    *,
    expr: IbisTable,
    error_msg: str,
    record_gaps: bool,
    diagnostics_sink: DiagnosticsSink | None,
) -> SubstraitCompilationResult:
    if record_gaps:
        record_substrait_gap(
            expr_type=type(expr).__name__,
            reason=error_msg,
            sink=diagnostics_sink,
        )
    return SubstraitCompilationResult(
        plan_bytes=None,
        success=False,
        errors=(error_msg,),
        expr_type=type(expr).__name__,
    )


def substrait_compilation_diagnostics(
    result: SubstraitCompilationResult,
) -> Mapping[str, object]:
    """Return diagnostics payload for a Substrait compilation result.

    Parameters
    ----------
    result : SubstraitCompilationResult
        Compilation result to extract diagnostics from.

    Returns
    -------
    Mapping[str, object]
        Diagnostics payload with compilation status and error information.

    Examples
    --------
    >>> result = SubstraitCompilationResult(
    ...     plan_bytes=None,
    ...     success=False,
    ...     errors=("compilation not supported",),
    ...     expr_type="Table",
    ... )
    >>> diag = substrait_compilation_diagnostics(result)
    >>> diag["success"]
    False
    >>> diag["errors"]
    ['compilation not supported']
    """
    return {
        "success": result.success,
        "has_plan_bytes": result.plan_bytes is not None,
        "plan_size_bytes": len(result.plan_bytes) if result.plan_bytes is not None else None,
        "errors": list(result.errors),
        "error_count": len(result.errors),
        "expr_type": result.expr_type,
    }


def record_substrait_gap(
    expr_type: str,
    reason: str,
    sink: DiagnosticsSink | None,
) -> None:
    """Record a Substrait compilation gap in diagnostics.

    This function emits diagnostic information when an Ibis expression
    cannot be compiled to Substrait, helping identify which operations
    need AST fallback.

    Parameters
    ----------
    expr_type : str
        Type name of the unsupported expression.
    reason : str
        Explanation for why Substrait compilation failed.
    sink : DiagnosticsSink | None
        Diagnostics sink to record the gap event, or None to skip recording.

    Examples
    --------
    >>> from datafusion_engine.diagnostics import InMemoryDiagnosticsSink
    >>> sink = InMemoryDiagnosticsSink()
    >>> record_substrait_gap("Join", "anti joins not supported", sink)
    >>> gaps = sink.artifacts.get("substrait_gaps_v1", [])
    >>> len(gaps)
    1
    >>> gaps[0]["expr_type"]
    'Join'
    """
    if sink is None:
        return

    payload: dict[str, object] = {
        "expr_type": expr_type,
        "reason": reason,
        "fallback": "ast_execution",
    }

    sink.record_artifact("substrait_gaps_v1", payload)


def try_ibis_to_substrait_bytes(
    expr: IbisTable,
    *,
    record_gaps: bool = True,
    diagnostics_sink: DiagnosticsSink | None = None,
    session_id: str | None = None,
) -> bytes | None:
    """Attempt Ibis-to-Substrait compilation without raising exceptions.

    This is a convenience wrapper that returns None on failure instead of
    raising exceptions, suitable for dual-lane compilation strategies.

    Parameters
    ----------
    expr : IbisTable
        Ibis table expression to compile.
    record_gaps : bool, optional
        Whether to record Substrait gaps in diagnostics.
    diagnostics_sink : DiagnosticsSink | None, optional
        Diagnostics sink for recording gap information.
    session_id : str | None, optional
        Optional session identifier for recorder adaptation.

    Returns
    -------
    bytes | None
        Serialized Substrait plan bytes on success, None on failure.

    Examples
    --------
    >>> import ibis
    >>> from ibis_engine.substrait_bridge import try_ibis_to_substrait_bytes
    >>> backend = ibis.datafusion.connect()
    >>> expr = backend.table("my_table").select("col1", "col2")
    >>> plan_bytes = try_ibis_to_substrait_bytes(expr)  # doctest: +SKIP
    >>> if plan_bytes is None:  # doctest: +SKIP
    ...     # Fall back to AST execution
    ...     pass
    """
    resolved_sink = _coerce_diagnostics_sink(diagnostics_sink, session_id=session_id)
    result = _try_ibis_substrait(
        expr,
        record_gaps=record_gaps,
        diagnostics_sink=resolved_sink,
    )
    return result.plan_bytes if result.success else None


__all__ = [
    "SubstraitCompilationResult",
    "ibis_to_substrait_bytes",
    "record_substrait_gap",
    "substrait_compilation_diagnostics",
    "try_ibis_to_substrait_bytes",
]
