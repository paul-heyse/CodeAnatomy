"""Port protocols for relspec engine decoupling."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Protocol


class LineagePort(Protocol):
    """Abstract lineage extraction used by dependency inference."""

    def extract_lineage(
        self,
        plan: object,
        *,
        udf_snapshot: Mapping[str, object] | None = None,
    ) -> object:
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


class RuntimeProfilePoliciesPort(Protocol):
    """Policy projection used by relspec policy compilation."""

    write_policy: object | None


class RuntimeProfileFeaturesPort(Protocol):
    """Feature-gates projection used by relspec policy compilation."""

    enable_delta_cdf: bool


class RuntimeProfilePort(Protocol):
    """Runtime-profile projection used by relspec policy compilation."""

    policies: RuntimeProfilePoliciesPort
    features: RuntimeProfileFeaturesPort
