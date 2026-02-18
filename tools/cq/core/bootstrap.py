"""Composition root for CQ runtime services and adapters."""

from __future__ import annotations

import atexit
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

from tools.cq.core.cache.diskcache_backend import close_cq_cache_backend, get_cq_cache_backend
from tools.cq.core.cache.interface import CqCacheBackend
from tools.cq.core.render_context import (
    get_default_render_enrichment_factory,
    set_default_render_enrichment_factory,
)
from tools.cq.core.runtime import RuntimeExecutionPolicy
from tools.cq.core.schema import CqResult
from tools.cq.core.services import (
    CallsService,
    CallsServiceRequest,
    EntityFrontDoorRequest,
    EntityService,
    SearchService,
    SearchServiceRequest,
)
from tools.cq.core.settings_factory import SettingsFactory

if TYPE_CHECKING:
    from tools.cq.core.ports import RenderEnrichmentPort


@dataclass(frozen=True)
class CqRuntimeServices:
    """Runtime service bundle."""

    search: SearchService
    entity: EntityService
    calls: CallsService
    cache: CqCacheBackend
    policy: RuntimeExecutionPolicy


@dataclass
class RuntimeServicesRegistry:
    """Thread-safe workspace-keyed runtime service registry."""

    _rows: dict[str, CqRuntimeServices] = field(default_factory=dict)
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def get(self, workspace: str) -> CqRuntimeServices | None:
        """Return cached services for a workspace, if present."""
        with self._lock:
            return self._rows.get(workspace)

    def set(self, workspace: str, services: CqRuntimeServices) -> None:
        """Store runtime services for a workspace."""
        with self._lock:
            self._rows[workspace] = services

    def clear(self) -> None:
        """Clear all cached workspace services."""
        with self._lock:
            self._rows.clear()


def _build_render_enrichment_adapter() -> RenderEnrichmentPort | None:
    from tools.cq.orchestration.render_enrichment import SmartSearchRenderEnrichmentAdapter

    return SmartSearchRenderEnrichmentAdapter()


def build_runtime_services(*, root: Path) -> CqRuntimeServices:
    """Construct CQ runtime service bundle for a workspace.

    Returns:
        Runtime services wired for the provided repository root.
    """
    from tools.cq.core.enrichment_mode import parse_incremental_enrichment_mode
    from tools.cq.macros.calls import cmd_calls
    from tools.cq.query.entity_front_door import attach_entity_front_door_insight
    from tools.cq.search._shared.types import QueryMode
    from tools.cq.search.pipeline.smart_search import smart_search
    from tools.cq.search.python.extractors import ensure_python_clear_callback_registered
    from tools.cq.search.rust.enrichment import ensure_rust_clear_callback_registered
    from tools.cq.search.tree_sitter.rust_lane.query_cache import (
        ensure_query_cache_callback_registered,
    )
    from tools.cq.search.tree_sitter.rust_lane.runtime_cache import (
        ensure_runtime_cache_callback_registered,
    )

    ensure_python_clear_callback_registered()
    ensure_rust_clear_callback_registered()
    ensure_query_cache_callback_registered()
    ensure_runtime_cache_callback_registered()

    def _attach_front_door(request: EntityFrontDoorRequest) -> CqResult:
        return attach_entity_front_door_insight(
            request.result,
            relationship_detail_max_matches=request.relationship_detail_max_matches,
        )

    def _execute_calls(request: CallsServiceRequest) -> CqResult:
        from tools.cq.macros.contracts import CallsRequest

        call_request = request.request
        return cmd_calls(
            CallsRequest(
                root=call_request.root,
                function_name=call_request.function_name,
                tc=call_request.tc,
                argv=list(call_request.argv),
            )
        )

    def _execute_search(request: SearchServiceRequest) -> CqResult:
        parsed_mode: QueryMode | None = None
        if isinstance(request.mode, QueryMode):
            parsed_mode = request.mode
        elif isinstance(request.mode, str):
            normalized_mode = request.mode.strip().lower()
            if normalized_mode in {"identifier", "regex", "literal"}:
                parsed_mode = QueryMode(normalized_mode)
        return smart_search(
            root=request.root,
            query=request.query,
            mode=parsed_mode,
            lang_scope=request.lang_scope,
            include_globs=request.include_globs,
            exclude_globs=request.exclude_globs,
            include_strings=request.include_strings,
            with_neighborhood=request.with_neighborhood,
            limits=request.limits,
            tc=request.tc,
            argv=request.argv,
            run_id=request.run_id,
            incremental_enrichment_enabled=request.incremental_enrichment_enabled,
            incremental_enrichment_mode=parse_incremental_enrichment_mode(
                request.incremental_enrichment_mode
            ),
        )

    if get_default_render_enrichment_factory() is None:
        set_default_render_enrichment_factory(_build_render_enrichment_adapter)

    return CqRuntimeServices(
        search=SearchService(execute_fn=_execute_search),
        entity=EntityService(attach_front_door_fn=_attach_front_door),
        calls=CallsService(execute_fn=_execute_calls),
        cache=get_cq_cache_backend(root=root),
        policy=SettingsFactory.runtime_policy(),
    )


_RUNTIME_SERVICES_REGISTRY = RuntimeServicesRegistry()


def resolve_runtime_services(root: Path) -> CqRuntimeServices:
    """Resolve workspace-scoped CQ runtime services.

    Services are cached per workspace root and shared across run steps.
    This ensures consistent state and avoids re-initialization overhead
    within a single workspace session.

    Returns:
        Reused runtime service bundle keyed by resolved workspace path.
    """
    workspace = str(root.resolve())
    services = _RUNTIME_SERVICES_REGISTRY.get(workspace)
    if services is not None:
        return services
    services = build_runtime_services(root=root)
    _RUNTIME_SERVICES_REGISTRY.set(workspace, services)
    return services


def clear_runtime_services() -> None:
    """Clear cached runtime service bundles."""
    _RUNTIME_SERVICES_REGISTRY.clear()
    close_cq_cache_backend()


atexit.register(clear_runtime_services)


__all__ = [
    "CqRuntimeServices",
    "RuntimeServicesRegistry",
    "build_runtime_services",
    "clear_runtime_services",
    "resolve_runtime_services",
]
