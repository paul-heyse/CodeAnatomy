"""Call-analysis output assembly helpers."""

from __future__ import annotations

from tools.cq.macros.calls.entry import CallScanResult, CallsContext, build_calls_result

__all__ = ["build_calls_output"]


def build_calls_output(
    ctx: CallsContext,
    scan_result: CallScanResult,
    *,
    started_ms: float,
) -> object:
    """Build final calls macro output.

    Returns:
        object: `CqResult` produced by `build_calls_result`.
    """
    return build_calls_result(ctx, scan_result, started_ms=started_ms)
