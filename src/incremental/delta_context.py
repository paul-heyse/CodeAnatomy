"""Shared Delta access context helpers for incremental pipelines."""

from __future__ import annotations

from dataclasses import dataclass, field

from incremental.runtime import IncrementalRuntime
from storage.deltalake import StorageOptions


@dataclass(frozen=True)
class DeltaStorageOptions:
    """Storage options for Delta table access."""

    storage_options: StorageOptions | None = None
    log_storage_options: StorageOptions | None = None


@dataclass(frozen=True)
class DeltaAccessContext:
    """Delta access context bundling runtime and storage options."""

    runtime: IncrementalRuntime
    storage: DeltaStorageOptions = field(default_factory=DeltaStorageOptions)

    def storage_kwargs(self) -> dict[str, StorageOptions | None]:
        """Return storage option kwargs for Delta helpers.

        Returns
        -------
        dict[str, StorageOptions | None]
            Storage option kwargs for Delta helpers.
        """
        return {
            "storage_options": self.storage.storage_options,
            "log_storage_options": self.storage.log_storage_options,
        }


__all__ = ["DeltaAccessContext", "DeltaStorageOptions"]
