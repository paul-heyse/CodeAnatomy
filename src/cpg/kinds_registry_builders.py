"""Row-driven builders for Ultimate CPG kind contracts."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass

from cpg.kinds_registry_enums import EdgeKind, NodeKind, SourceKind
from cpg.kinds_registry_models import EdgeKindContract, NodeKindContract
from cpg.kinds_registry_props import PropSpec, resolve_prop_specs

PropSpecOverride = tuple[tuple[str, PropSpec], ...]


@dataclass(frozen=True)
class NodeContractTemplate:
    """Reusable defaults for node contracts."""

    requires_anchor: bool
    allowed_sources: tuple[SourceKind, ...] = ()
    required: tuple[str, ...] = ()
    optional: tuple[str, ...] = ()


@dataclass(frozen=True)
class EdgeContractTemplate:
    """Reusable defaults for edge contracts."""

    requires_evidence_anchor: bool
    allowed_sources: tuple[SourceKind, ...] = ()
    required: tuple[str, ...] = ()
    optional: tuple[str, ...] = ()


@dataclass(frozen=True)
class NodeContractRow:
    """Row-driven node contract definition."""

    kind: NodeKind
    required: tuple[str, ...] = ()
    optional: tuple[str, ...] = ()
    template: NodeContractTemplate | None = None
    required_overrides: PropSpecOverride = ()
    optional_overrides: PropSpecOverride = ()
    requires_anchor: bool | None = None
    allowed_sources: tuple[SourceKind, ...] | None = None
    description: str = ""


@dataclass(frozen=True)
class EdgeContractRow:
    """Row-driven edge contract definition."""

    kind: EdgeKind
    required: tuple[str, ...] = ()
    optional: tuple[str, ...] = ()
    template: EdgeContractTemplate | None = None
    required_overrides: PropSpecOverride = ()
    optional_overrides: PropSpecOverride = ()
    requires_evidence_anchor: bool | None = None
    allowed_sources: tuple[SourceKind, ...] | None = None
    description: str = ""


def _merge_keys(groups: Iterable[Sequence[str]]) -> tuple[str, ...]:
    seen: set[str] = set()
    out: list[str] = []
    for group in groups:
        for key in group:
            if key in seen:
                continue
            seen.add(key)
            out.append(key)
    return tuple(out)


def _override_map(overrides: PropSpecOverride) -> dict[str, PropSpec]:
    return dict(overrides)


def _validate_override_keys(keys: Sequence[str], overrides: Mapping[str, PropSpec]) -> None:
    extra = sorted(set(overrides) - set(keys))
    if extra:
        msg = f"Override keys not declared in contract keys: {extra}"
        raise ValueError(msg)


def _ensure_disjoint(required: Sequence[str], optional: Sequence[str]) -> None:
    overlap = sorted(set(required) & set(optional))
    if overlap:
        msg = f"Props defined as both required and optional: {overlap}"
        raise ValueError(msg)


def build_node_contracts(
    rows: Sequence[NodeContractRow],
) -> dict[NodeKind, NodeKindContract]:
    """Build node contracts from row definitions.

    Returns
    -------
    dict[NodeKind, NodeKindContract]
        Node kind contracts keyed by kind.

    Raises
    ------
    ValueError
        Raised when a contract row is missing required inputs or has invalid overrides.
    """
    out: dict[NodeKind, NodeKindContract] = {}
    for row in rows:
        template = row.template
        requires_anchor = (
            row.requires_anchor
            if row.requires_anchor is not None
            else template.requires_anchor
            if template
            else None
        )
        if requires_anchor is None:
            msg = f"Missing requires_anchor for node kind {row.kind.value}."
            raise ValueError(msg)
        allowed_sources = (
            row.allowed_sources
            if row.allowed_sources is not None
            else template.allowed_sources
            if template
            else None
        )
        if allowed_sources is None:
            msg = f"Missing allowed_sources for node kind {row.kind.value}."
            raise ValueError(msg)
        required_keys = _merge_keys([template.required if template else (), row.required])
        optional_keys = _merge_keys([template.optional if template else (), row.optional])
        _ensure_disjoint(required_keys, optional_keys)
        required_overrides = _override_map(row.required_overrides)
        optional_overrides = _override_map(row.optional_overrides)
        _validate_override_keys(required_keys, required_overrides)
        _validate_override_keys(optional_keys, optional_overrides)
        required_props = resolve_prop_specs(required_keys, overrides=required_overrides)
        optional_props = resolve_prop_specs(optional_keys, overrides=optional_overrides)
        out[row.kind] = NodeKindContract(
            requires_anchor=requires_anchor,
            required_props=required_props,
            optional_props=optional_props,
            allowed_sources=allowed_sources,
            description=row.description,
        )
    return out


def build_edge_contracts(
    rows: Sequence[EdgeContractRow],
) -> dict[EdgeKind, EdgeKindContract]:
    """Build edge contracts from row definitions.

    Returns
    -------
    dict[EdgeKind, EdgeKindContract]
        Edge kind contracts keyed by kind.

    Raises
    ------
    ValueError
        Raised when a contract row is missing required inputs or has invalid overrides.
    """
    out: dict[EdgeKind, EdgeKindContract] = {}
    for row in rows:
        template = row.template
        requires_evidence_anchor = (
            row.requires_evidence_anchor
            if row.requires_evidence_anchor is not None
            else template.requires_evidence_anchor
            if template
            else None
        )
        if requires_evidence_anchor is None:
            msg = f"Missing requires_evidence_anchor for edge kind {row.kind.value}."
            raise ValueError(msg)
        allowed_sources = (
            row.allowed_sources
            if row.allowed_sources is not None
            else template.allowed_sources
            if template
            else None
        )
        if allowed_sources is None:
            msg = f"Missing allowed_sources for edge kind {row.kind.value}."
            raise ValueError(msg)
        required_keys = _merge_keys([template.required if template else (), row.required])
        optional_keys = _merge_keys([template.optional if template else (), row.optional])
        _ensure_disjoint(required_keys, optional_keys)
        required_overrides = _override_map(row.required_overrides)
        optional_overrides = _override_map(row.optional_overrides)
        _validate_override_keys(required_keys, required_overrides)
        _validate_override_keys(optional_keys, optional_overrides)
        required_props = resolve_prop_specs(required_keys, overrides=required_overrides)
        optional_props = resolve_prop_specs(optional_keys, overrides=optional_overrides)
        out[row.kind] = EdgeKindContract(
            requires_evidence_anchor=requires_evidence_anchor,
            required_props=required_props,
            optional_props=optional_props,
            allowed_sources=allowed_sources,
            description=row.description,
        )
    return out


__all__ = [
    "EdgeContractRow",
    "EdgeContractTemplate",
    "NodeContractRow",
    "NodeContractTemplate",
    "PropSpecOverride",
    "build_edge_contracts",
    "build_node_contracts",
]
