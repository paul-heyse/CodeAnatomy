"""Composition root for CQ runtime services and adapters."""

from __future__ import annotations

import atexit
import threading
from dataclasses import dataclass
from pathlib import Path

from tools.cq.core.cache.diskcache_backend import close_cq_cache_backend, get_cq_cache_backend
from tools.cq.core.cache.interface import CqCacheBackend
from tools.cq.core.runtime import RuntimeExecutionPolicy
from tools.cq.core.services import CallsService, EntityService, SearchService
from tools.cq.core.settings_factory import SettingsFactory


@dataclass(frozen=True)
class CqRuntimeServices:
    """Runtime service bundle."""

    search: SearchService
    entity: EntityService
    calls: CallsService
    cache: CqCacheBackend
    policy: RuntimeExecutionPolicy


def build_runtime_services(*, root: Path) -> CqRuntimeServices:
    """Construct CQ runtime service bundle for a workspace.

    Returns:
        Runtime services wired for the provided repository root.
    """
    return CqRuntimeServices(
        search=SearchService(),
        entity=EntityService(),
        calls=CallsService(),
        cache=get_cq_cache_backend(root=root),
        policy=SettingsFactory.runtime_policy(),
    )


_RUNTIME_SERVICES_LOCK = threading.Lock()
_RUNTIME_SERVICES: dict[str, CqRuntimeServices] = {}


def resolve_runtime_services(root: Path) -> CqRuntimeServices:
    """Resolve workspace-scoped CQ runtime services.

    Services are cached per workspace root and shared across run steps.
    This ensures consistent state and avoids re-initialization overhead
    within a single workspace session.

    Returns:
        Reused runtime service bundle keyed by resolved workspace path.
    """
    workspace = str(root.resolve())
    with _RUNTIME_SERVICES_LOCK:
        services = _RUNTIME_SERVICES.get(workspace)
        if services is not None:
            return services
        services = build_runtime_services(root=root)
        _RUNTIME_SERVICES[workspace] = services
        return services


def clear_runtime_services() -> None:
    """Clear cached runtime service bundles."""
    with _RUNTIME_SERVICES_LOCK:
        _RUNTIME_SERVICES.clear()
    close_cq_cache_backend()


atexit.register(clear_runtime_services)


__all__ = [
    "CqRuntimeServices",
    "build_runtime_services",
    "clear_runtime_services",
    "resolve_runtime_services",
]
