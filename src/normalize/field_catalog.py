"""Field catalog helpers for normalize dataset schemas."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from schema_spec.specs import ArrowFieldSpec


@dataclass
class FieldCatalog:
    """Registry for ArrowFieldSpec instances keyed by logical field name."""

    fields: dict[str, ArrowFieldSpec] = field(default_factory=dict)

    def register(self, key: str, spec: ArrowFieldSpec) -> None:
        """Register a field spec, guarding against conflicts.

        Raises
        ------
        ValueError
            Raised when a conflicting spec is already registered.
        """
        existing = self.fields.get(key)
        if existing is not None and existing != spec:
            msg = f"Field spec conflict for key {key!r}: {existing} vs {spec}"
            raise ValueError(msg)
        self.fields[key] = spec

    def register_many(self, entries: Mapping[str, ArrowFieldSpec]) -> None:
        """Register multiple field specs."""
        for key, spec in entries.items():
            self.register(key, spec)

    def field(self, key: str) -> ArrowFieldSpec:
        """Return the ArrowFieldSpec for a field key.

        Returns
        -------
        ArrowFieldSpec
            Registered field specification.
        """
        return self.fields[key]

    def fields_for(self, keys: Sequence[str]) -> list[ArrowFieldSpec]:
        """Return ArrowFieldSpec instances for field keys.

        Returns
        -------
        list[ArrowFieldSpec]
            Registered field specifications.
        """
        return [self.field(key) for key in keys]


__all__ = ["FieldCatalog"]
