"""Core model classes for the Ultimate CPG registry."""

from __future__ import annotations

import importlib
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Literal

from cpg.kinds_registry_enums import SourceKind
from cpg.kinds_registry_props import PropSpec

DerivationStatus = Literal["implemented", "planned"]


@dataclass(frozen=True)
class NodeKindContract:
    """Contract for a node kind."""

    requires_anchor: bool
    required_props: Mapping[str, PropSpec] = field(default_factory=dict)
    optional_props: Mapping[str, PropSpec] = field(default_factory=dict)
    allowed_sources: tuple[SourceKind, ...] = ()
    description: str = ""

    def to_dict(self) -> dict[str, object]:
        """Serialize the node kind contract to a dictionary.

        Returns
        -------
        dict[str, object]
            JSON-serializable node contract.
        """
        return {
            "requires_anchor": self.requires_anchor,
            "required_props": {k: v.to_dict() for k, v in self.required_props.items()},
            "optional_props": {k: v.to_dict() for k, v in self.optional_props.items()},
            "allowed_sources": [s.value for s in self.allowed_sources],
            "description": self.description,
        }


@dataclass(frozen=True)
class EdgeKindContract:
    """Contract for an edge kind."""

    requires_evidence_anchor: bool
    required_props: Mapping[str, PropSpec] = field(default_factory=dict)
    optional_props: Mapping[str, PropSpec] = field(default_factory=dict)
    allowed_sources: tuple[SourceKind, ...] = ()
    description: str = ""

    def to_dict(self) -> dict[str, object]:
        """Serialize the edge kind contract to a dictionary.

        Returns
        -------
        dict[str, object]
            JSON-serializable edge contract.
        """
        return {
            "requires_evidence_anchor": self.requires_evidence_anchor,
            "required_props": {k: v.to_dict() for k, v in self.required_props.items()},
            "optional_props": {k: v.to_dict() for k, v in self.optional_props.items()},
            "allowed_sources": [s.value for s in self.allowed_sources],
            "description": self.description,
        }


@dataclass(frozen=True)
class DerivationSpec:
    """Derivation spec for a node_kind or edge_kind."""

    extractor: str
    provider_or_field: str
    join_keys: tuple[str, ...]
    id_recipe: str
    confidence_policy: str
    ambiguity_policy: str
    status: DerivationStatus = "implemented"
    notes: str = ""

    def to_dict(self) -> dict[str, object]:
        """Serialize the derivation spec to a dictionary.

        Returns
        -------
        dict[str, object]
            JSON-serializable derivation spec.
        """
        return {
            "extractor": self.extractor,
            "provider_or_field": self.provider_or_field,
            "join_keys": list(self.join_keys),
            "id_recipe": self.id_recipe,
            "confidence_policy": self.confidence_policy,
            "ambiguity_policy": self.ambiguity_policy,
            "status": self.status,
            "notes": self.notes,
        }


@dataclass(frozen=True)
class CallableRef:
    """Reference a callable attribute via a module path."""

    module: str
    attr: str

    def resolve(self) -> object:
        """Import and resolve the referenced attribute.

        Returns
        -------
        object
            Resolved attribute.
        """
        mod = importlib.import_module(self.module)
        return getattr(mod, self.attr)


def parse_extractor(value: str) -> CallableRef:
    """Parse an extractor string into a callable reference.

    Parameters
    ----------
    value:
        Extractor string in the form "module:attribute".

    Returns
    -------
    CallableRef
        Parsed callable reference.

    Raises
    ------
    ValueError
        Raised when the extractor string is invalid.
    """
    raw = value.strip()
    if ":" not in raw:
        msg = f"Extractor must be in module:attribute form, got {value!r}."
        raise ValueError(msg)
    module, attr = raw.split(":", 1)
    if not module or not attr:
        msg = f"Extractor must include module and attribute, got {value!r}."
        raise ValueError(msg)
    return CallableRef(module=module, attr=attr)


__all__ = [
    "CallableRef",
    "DerivationSpec",
    "DerivationStatus",
    "EdgeKindContract",
    "NodeKindContract",
    "parse_extractor",
]
