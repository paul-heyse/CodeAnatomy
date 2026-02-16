"""Section-building helpers for Smart Search rendering."""

from __future__ import annotations

from pathlib import Path

from tools.cq.core.schema import Finding, Section
from tools.cq.search._shared.types import QueryMode
from tools.cq.search.objects.resolve import ObjectResolutionRuntime
from tools.cq.search.pipeline.smart_search_types import EnrichedMatch


def build_sections(
    matches: list[EnrichedMatch],
    root: Path,
    query: str,
    mode: QueryMode,
    *,
    include_strings: bool = False,
    object_runtime: ObjectResolutionRuntime | None = None,
) -> list[Section]:
    """Build rendered sections for Smart Search output.

    Returns:
        list[Section]: Rendered section list for Smart Search output.
    """
    from tools.cq.search.pipeline.smart_search import build_sections as _build_sections

    return _build_sections(
        matches,
        root,
        query,
        mode,
        include_strings=include_strings,
        object_runtime=object_runtime,
    )


def build_finding(match: EnrichedMatch, root: Path) -> Finding:
    """Build one finding from an enriched match.

    Returns:
        Finding: Render-ready finding derived from one enriched match.
    """
    from tools.cq.search.pipeline.smart_search import build_finding as _build_finding

    return _build_finding(match, root)


__all__ = ["build_finding", "build_sections"]
