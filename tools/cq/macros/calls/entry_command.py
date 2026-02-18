"""Command boundary for calls macro execution."""

from __future__ import annotations

import logging
from pathlib import Path

from tools.cq.core.cache.run_lifecycle import maybe_evict_run_cache_tag
from tools.cq.core.schema import CqResult, assign_result_finding_ids, ms
from tools.cq.macros.contracts import CallsRequest
from tools.cq.macros.rust_fallback_policy import RustFallbackPolicyV1, apply_rust_fallback_policy

logger = logging.getLogger(__name__)


def cmd_calls(request: CallsRequest) -> CqResult:
    """Census all call sites for a function.

    Returns:
        Calls macro result with front-door insight and fallback policy applied.
    """
    from tools.cq.macros.calls.entry import CallsContext, _build_calls_result, _scan_call_sites

    started = ms()
    ctx = CallsContext(
        tc=request.tc,
        root=Path(request.root),
        argv=request.argv,
        function_name=request.function_name,
    )
    logger.debug("Running calls macro function=%s root=%s", ctx.function_name, ctx.root)
    scan_result = _scan_call_sites(ctx.root, ctx.function_name)
    if scan_result.used_fallback:
        logger.warning("Calls macro used ripgrep fallback for function=%s", ctx.function_name)
    result = _build_calls_result(ctx, scan_result, started_ms=started)
    result = apply_rust_fallback_policy(
        result,
        root=ctx.root,
        policy=RustFallbackPolicyV1(
            macro_name="calls",
            pattern=ctx.function_name,
            query=ctx.function_name,
            fallback_matches_summary_key="total_sites",
        ),
    )
    result = assign_result_finding_ids(result)
    if result.run.run_id:
        maybe_evict_run_cache_tag(root=ctx.root, language="python", run_id=result.run.run_id)
        maybe_evict_run_cache_tag(root=ctx.root, language="rust", run_id=result.run.run_id)
    logger.debug(
        "Calls macro completed function=%s total_sites=%d",
        ctx.function_name,
        len(scan_result.all_sites),
    )
    return result


__all__ = ["cmd_calls"]
