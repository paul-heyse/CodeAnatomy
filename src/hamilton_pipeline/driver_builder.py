"""Synchronous Hamilton driver builder wrapper."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from hamilton import driver

if TYPE_CHECKING:
    from hamilton_pipeline.cache_lineage import CacheLineageHook
    from hamilton_pipeline.graph_snapshot import GraphSnapshotHook


@dataclass(frozen=True)
class DriverBuilder:
    """Synchronous driver builder with optional hook binding."""

    builder: driver.Builder
    cache_lineage_hook: CacheLineageHook | None = None
    graph_snapshot_hook: GraphSnapshotHook | None = None

    def build(self) -> driver.Driver:
        """Build and return a Hamilton driver instance.

        Returns
        -------
        driver.Driver
            Built Hamilton driver instance.
        """
        driver_instance = self.builder.build()
        if self.cache_lineage_hook is not None:
            self.cache_lineage_hook.bind_driver(driver_instance)
        if self.graph_snapshot_hook is not None:
            self.graph_snapshot_hook.bind_driver(driver_instance)
        return driver_instance


__all__ = ["DriverBuilder"]
