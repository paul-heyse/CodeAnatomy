"""Render enrichment apply helpers.

Keeps report-layer enrichment orchestration imports focused on apply semantics.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from tools.cq.core.render_enrichment_orchestrator import (
    RenderEnrichmentSessionV1,
    count_render_enrichment_tasks,
    maybe_attach_render_enrichment,
    precompute_render_enrichment_cache,
    select_enrichment_target_files,
)
from tools.cq.core.schema import Finding

if TYPE_CHECKING:
    from tools.cq.core.ports import RenderEnrichmentPort

__all__ = [
    "RenderEnrichmentSessionV1",
    "count_render_enrichment_tasks",
    "maybe_attach_render_enrichment",
    "precompute_render_enrichment_cache",
    "select_enrichment_target_files",
]


def apply_render_enrichment(
    finding: Finding,
    *,
    root: Path,
    cache: dict[tuple[str, int, int, str], dict[str, object]] | None,
    allowed_files: set[str] | None,
    port: RenderEnrichmentPort | None,
) -> Finding:
    """Apply render enrichment to one finding.

    Returns:
        Finding: Finding with enrichment details applied when available.
    """
    return maybe_attach_render_enrichment(
        finding,
        root=root,
        cache=cache,
        allowed_files=allowed_files,
        port=port,
    )
