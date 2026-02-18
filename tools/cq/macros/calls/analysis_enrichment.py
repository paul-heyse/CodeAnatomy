"""Enrichment-oriented wrappers for calls analysis."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import ast

    from tools.cq.macros.calls.analysis import CallAnalysis


def detect_hazards(call: ast.Call, info: CallAnalysis) -> list[str]:
    """Detect hazard labels for one call expression.

    Returns:
        Hazard labels detected for the call expression.
    """
    from tools.cq.macros.calls.analysis import _detect_hazards as detect_hazards_impl

    return detect_hazards_impl(call, info)


def enrich_call_site(
    source: str,
    containing_fn: str,
    rel_path: str,
) -> dict[str, dict[str, object] | None]:
    """Enrich one call-site with symtable and bytecode signals.

    Returns:
        Enrichment payload keyed by source plane.
    """
    from tools.cq.macros.calls.enrichment import enrich_call_site as enrich_call_site_impl

    return enrich_call_site_impl(source, containing_fn, rel_path)


__all__ = ["detect_hazards", "enrich_call_site"]
