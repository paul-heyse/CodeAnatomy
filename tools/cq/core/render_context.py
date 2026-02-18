"""Typed render context for markdown and summary rendering."""

from __future__ import annotations

import threading
from collections.abc import Callable
from typing import TYPE_CHECKING

from tools.cq.core.structs import CqStruct

if TYPE_CHECKING:
    from tools.cq.core.ports import RenderEnrichmentPort

    type RenderEnrichmentFactory = Callable[[], RenderEnrichmentPort | None]
else:
    RenderEnrichmentFactory = Callable[[], object | None]
_DEFAULT_ENRICHMENT_FACTORY_LOCK = threading.Lock()
_DEFAULT_ENRICHMENT_FACTORY: RenderEnrichmentFactory | None = None


def get_default_render_enrichment_factory() -> RenderEnrichmentFactory | None:
    """Return process-default render enrichment factory."""
    with _DEFAULT_ENRICHMENT_FACTORY_LOCK:
        return _DEFAULT_ENRICHMENT_FACTORY


def set_default_render_enrichment_factory(factory: RenderEnrichmentFactory | None) -> None:
    """Install or clear process-default render enrichment factory."""
    global _DEFAULT_ENRICHMENT_FACTORY
    with _DEFAULT_ENRICHMENT_FACTORY_LOCK:
        _DEFAULT_ENRICHMENT_FACTORY = factory


class RenderContext(CqStruct, frozen=True):
    """Rendering context carrying optional enrichment adapter dependencies."""

    enrichment_port: RenderEnrichmentPort | None = None

    @classmethod
    def minimal(cls) -> RenderContext:
        """Return default render context with best-effort enrichment adapter."""
        factory = get_default_render_enrichment_factory()
        if factory is None:
            return cls(enrichment_port=None)
        try:
            return cls(enrichment_port=factory())
        except (OSError, RuntimeError, ValueError, TypeError):
            return cls(enrichment_port=None)


__all__ = [
    "RenderContext",
    "RenderEnrichmentFactory",
    "get_default_render_enrichment_factory",
    "set_default_render_enrichment_factory",
]
