"""Semantic program manifest produced by compile-context orchestration."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

from utils.hashing import hash_msgpack_canonical

if TYPE_CHECKING:
    from datafusion_engine.dataset.registry import DatasetLocation
    from semantics.ir import SemanticIR
    from semantics.validation.catalog_validation import SemanticInputValidationResult
    from semantics.validation.policy import SemanticInputValidationPolicy


@dataclass(frozen=True)
class ManifestDatasetBindings:
    """Resolved dataset bindings for a semantic program."""

    locations: Mapping[str, DatasetLocation]

    def payload(self) -> dict[str, object]:
        """Return a JSON-serializable payload for diagnostics."""
        return {
            name: {
                "path": str(location.path),
                "format": location.format,
            }
            for name, location in sorted(self.locations.items())
        }

    def to_payload(self) -> dict[str, object]:
        """Return canonical payload for compatibility callsites."""
        return self.payload()

    def location(self, name: str) -> DatasetLocation | None:
        """Return dataset location for a name, or None if not found.

        Parameters
        ----------
        name
            Dataset name to resolve.

        Returns:
        -------
        DatasetLocation | None
            Location if found, None otherwise.
        """
        return self.locations.get(name)

    def has_location(self, name: str) -> bool:
        """Check if a dataset location exists.

        Parameters
        ----------
        name
            Dataset name to check.

        Returns:
        -------
        bool
            True if location exists.
        """
        return name in self.locations

    def names(self) -> Sequence[str]:
        """Return all dataset names in the resolver.

        Returns:
        -------
        Sequence[str]
            All dataset names.
        """
        return tuple(self.locations.keys())

    def require_location(self, name: str) -> DatasetLocation:
        """Return dataset location or raise structured KeyError.

        Parameters
        ----------
        name
            Dataset name to resolve.

        Returns:
        -------
        DatasetLocation
            Location for the dataset.

        Raises:
            KeyError: If dataset location is not found.
        """
        location = self.locations.get(name)
        if location is None:
            msg = f"Required dataset location not found: {name!r}"
            raise KeyError(msg)
        return location

    def subset(self, names: Sequence[str]) -> ManifestDatasetBindings:
        """Return a new bindings object filtered to the given names.

        Parameters
        ----------
        names
            Dataset names to include.

        Returns:
        -------
        ManifestDatasetBindings
            Filtered bindings.
        """
        return ManifestDatasetBindings(
            locations={n: self.locations[n] for n in names if n in self.locations}
        )


class ManifestDatasetResolver(Protocol):
    """Read-only protocol for resolving dataset locations from manifest bindings."""

    def location(self, name: str) -> DatasetLocation | None:
        """Return dataset location for a name, or None if not found.

        Parameters
        ----------
        name
            Dataset name to resolve.

        Returns:
        -------
        DatasetLocation | None
            Location if found, None otherwise.
        """
        ...

    def has_location(self, name: str) -> bool:
        """Check if a dataset location exists.

        Parameters
        ----------
        name
            Dataset name to check.

        Returns:
        -------
        bool
            True if location exists.
        """
        ...

    def names(self) -> Sequence[str]:
        """Return all dataset names in the resolver.

        Returns:
        -------
        Sequence[str]
            All dataset names.
        """
        ...


@dataclass(frozen=True)
class SemanticProgramManifest:
    """Compiled semantic program contract consumed by runtime flows."""

    semantic_ir: SemanticIR
    requested_outputs: tuple[str, ...]
    input_mapping: Mapping[str, str]
    validation_policy: SemanticInputValidationPolicy
    dataset_bindings: ManifestDatasetBindings
    validation: SemanticInputValidationResult | None = None
    udf_snapshot: Mapping[str, object] | None = None
    fingerprint: str | None = None
    manifest_version: int = 1

    def payload(self) -> dict[str, object]:
        """Return a JSON-serializable payload for diagnostics."""
        return {
            "requested_outputs": list(self.requested_outputs),
            "input_mapping": dict(sorted(self.input_mapping.items())),
            "validation_policy": self.validation_policy,
            "dataset_bindings": self.dataset_bindings.payload(),
            "semantic_ir": {
                "view_count": len(self.semantic_ir.views),
                "join_group_count": len(self.semantic_ir.join_groups),
                "model_hash": self.semantic_ir.model_hash,
                "ir_hash": self.semantic_ir.ir_hash,
            },
            "validation": None
            if self.validation is None
            else {
                "valid": self.validation.valid,
                "missing_tables": list(self.validation.missing_tables),
                "missing_columns": {
                    name: list(columns) for name, columns in self.validation.missing_columns.items()
                },
                "resolved_tables": dict(self.validation.resolved_tables),
            },
            "fingerprint": self.fingerprint,
        }

    def to_payload(self) -> dict[str, object]:
        """Return canonical payload for compatibility callsites."""
        return self.payload()

    def with_fingerprint(self) -> SemanticProgramManifest:
        """Return manifest with deterministic fingerprint populated."""
        digest = hash_msgpack_canonical(self.payload())
        return SemanticProgramManifest(
            semantic_ir=self.semantic_ir,
            requested_outputs=self.requested_outputs,
            input_mapping=dict(self.input_mapping),
            validation_policy=self.validation_policy,
            dataset_bindings=self.dataset_bindings,
            validation=self.validation,
            udf_snapshot=self.udf_snapshot,
            fingerprint=digest,
        )


__all__ = ["ManifestDatasetBindings", "ManifestDatasetResolver", "SemanticProgramManifest"]
