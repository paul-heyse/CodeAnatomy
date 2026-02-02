"""Registry spec definitions for semantic dataset rows."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Final

from semantics.catalog.dataset_rows import DatasetCategory, DatasetRole

DEFAULT_DATASET_KIND: Final[str] = "table"


@dataclass(frozen=True)
class DatasetRegistrySpec:
    """Declarative spec for deriving SemanticDatasetRow instances."""

    name: str
    version: int
    bundles: tuple[str, ...]
    fields: tuple[str, ...]
    category: DatasetCategory
    supports_cdf: bool = True
    partition_cols: tuple[str, ...] = ()
    merge_keys: tuple[str, ...] | None = None
    join_keys: tuple[str, ...] = ()
    template: str | None = None
    view_builder: str | None = None
    kind: str = DEFAULT_DATASET_KIND
    semantic_id: str | None = None
    entity: str | None = None
    grain: str | None = None
    stability: str | None = None
    schema_ref: str | None = None
    materialization: str | None = None
    materialized_name: str | None = None
    metadata_extra: dict[bytes, bytes] = field(default_factory=dict)
    register_view: bool = True
    source_dataset: str | None = None
    role: DatasetRole = "output"


__all__ = ["DEFAULT_DATASET_KIND", "DatasetRegistrySpec"]
