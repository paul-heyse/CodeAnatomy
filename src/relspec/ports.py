"""Port protocols for relspec engine decoupling."""

from __future__ import annotations

from typing import Protocol


class LineagePort(Protocol):
    """Abstract lineage extraction used by dependency inference."""

    def extract_lineage(self, plan: object) -> object:
        """Extract lineage payload for a compiled plan."""
        ...

    def resolve_required_udfs(self, bundle: object) -> frozenset[str]:
        """Resolve required UDF names from a lineage bundle."""
        ...


class DatasetSpecProvider(Protocol):
    """Abstract dataset-spec provider used by relspec adapters."""

    def extract_dataset_spec(self, value: object) -> object | None:
        """Extract dataset spec from an arbitrary payload."""
        ...

    def normalize_dataset_spec(self, value: object) -> object | None:
        """Normalize extracted dataset spec payload."""
        ...
