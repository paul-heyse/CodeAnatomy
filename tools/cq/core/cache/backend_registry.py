"""Registry object for workspace-scoped cache backends."""

from __future__ import annotations

from dataclasses import dataclass, field

from tools.cq.core.cache.interface import CqCacheBackend


@dataclass
class BackendRegistry:
    """Mutable workspace->backend registry."""

    backends: dict[str, CqCacheBackend] = field(default_factory=dict)

    def get(self, workspace: str) -> CqCacheBackend | None:
        """Return backend for workspace when registered.

        Returns:
            CqCacheBackend | None: Registered backend for `workspace`, when present.
        """
        return self.backends.get(workspace)

    def set(self, workspace: str, backend: CqCacheBackend) -> CqCacheBackend | None:
        """Register backend for workspace and return previous backend if present.

        Returns:
            CqCacheBackend | None: Previously registered backend for `workspace`.
        """
        previous = self.backends.get(workspace)
        self.backends[workspace] = backend
        return previous

    def pop(self, workspace: str) -> CqCacheBackend | None:
        """Remove and return backend for workspace when present.

        Returns:
            CqCacheBackend | None: Removed backend for `workspace`, when present.
        """
        return self.backends.pop(workspace, None)

    def items(self) -> list[tuple[str, CqCacheBackend]]:
        """Return snapshot list of workspace/backend pairs.

        Returns:
            list[tuple[str, CqCacheBackend]]: Current `(workspace, backend)` pairs.
        """
        return list(self.backends.items())

    def values(self) -> list[CqCacheBackend]:
        """Return snapshot list of registered backend instances.

        Returns:
            list[CqCacheBackend]: Current registered backend values.
        """
        return list(self.backends.values())

    def clear(self) -> None:
        """Clear all registered workspace/backend mappings."""
        self.backends.clear()


__all__ = ["BackendRegistry"]
