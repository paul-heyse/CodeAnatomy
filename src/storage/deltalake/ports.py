"""Storage-side protocol definitions for Delta control-plane interactions."""

from __future__ import annotations

from typing import Protocol


class ControlPlanePort(Protocol):
    """Abstract control-plane operations needed by storage adapters."""

    def delta_merge(self, request: object) -> object:
        """Execute a Delta merge mutation."""
        ...

    def delta_schema_guard(self, table: object) -> object:
        """Apply schema-guard validation for Delta operations."""
        ...

    def delta_observability_table(self, name: str) -> object:
        """Resolve observability table metadata by logical name."""
        ...
