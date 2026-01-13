"""Dataset factory and scan spec helpers."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyarrow.dataset as ds
from pyarrow import fs

from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import SchemaLike
from arrowdsl.plan.plan import Plan

if TYPE_CHECKING:
    from arrowdsl.plan.query import QuerySpec


@dataclass(frozen=True)
class DatasetFactorySpec:
    """Specification for deterministic dataset discovery."""

    root: str
    format: ds.FileFormat
    filesystem: fs.FileSystem | None = None
    partition_base_dir: str | None = None
    exclude_invalid_files: bool = False
    selector_ignore_prefixes: Sequence[str] | None = None
    promote_options: str = "permissive"
    recursive: bool = True

    def build(self, *, schema: SchemaLike | None = None) -> ds.Dataset:
        """Build a dataset using a factory-driven discovery path.

        Returns
        -------
        ds.Dataset
            Dataset instance discovered by the factory.
        """
        filesystem = self.filesystem or fs.LocalFileSystem()
        selector = fs.FileSelector(self.root, recursive=self.recursive)
        options = ds.FileSystemFactoryOptions(
            partition_base_dir=self.partition_base_dir,
            exclude_invalid_files=self.exclude_invalid_files,
            selector_ignore_prefixes=list(self.selector_ignore_prefixes or [".", "_"]),
        )
        factory = ds.FileSystemDatasetFactory(
            filesystem=filesystem,
            paths_or_selector=selector,
            format=self.format,
            options=options,
        )
        inspected = factory.inspect(promote_options=self.promote_options)
        return factory.finish(schema=schema or inspected)


@dataclass(frozen=True)
class ScanSpec:
    """Bundle a dataset source with a query spec."""

    dataset: ds.Dataset | DatasetFactorySpec
    query: QuerySpec

    def open_dataset(self, *, schema: SchemaLike | None = None) -> ds.Dataset:
        """Return a dataset for scanning.

        Returns
        -------
        ds.Dataset
            Dataset instance for scanning.
        """
        if isinstance(self.dataset, DatasetFactorySpec):
            return self.dataset.build(schema=schema)
        return self.dataset

    def to_plan(
        self,
        *,
        ctx: ExecutionContext,
        schema: SchemaLike | None = None,
        label: str = "",
    ) -> Plan:
        """Compile the scan spec into an Acero-backed Plan.

        Returns
        -------
        Plan
            Plan representing the scan/filter/project pipeline.
        """
        dataset = self.open_dataset(schema=schema)
        return self.query.to_plan(
            dataset=dataset,
            ctx=ctx,
            label=label,
            scan_provenance=ctx.runtime.scan.scan_provenance_columns,
        )


__all__ = ["DatasetFactorySpec", "ScanSpec"]
