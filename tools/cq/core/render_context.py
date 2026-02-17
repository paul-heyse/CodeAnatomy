"""Typed render context for markdown and summary rendering."""

from __future__ import annotations

from typing import TYPE_CHECKING

from tools.cq.core.structs import CqStruct

if TYPE_CHECKING:
    from tools.cq.core.ports import RenderEnrichmentPort


class RenderContext(CqStruct, frozen=True):
    """Rendering context carrying optional enrichment adapter dependencies."""

    enrichment_port: RenderEnrichmentPort | None = None

    @classmethod
    def minimal(cls) -> RenderContext:
        """Return default render context with best-effort enrichment adapter."""
        try:
            from tools.cq.orchestration.render_enrichment import SmartSearchRenderEnrichmentAdapter

            return cls(enrichment_port=SmartSearchRenderEnrichmentAdapter())
        except (ImportError, OSError, RuntimeError, ValueError, TypeError):
            return cls(enrichment_port=None)


__all__ = ["RenderContext"]
