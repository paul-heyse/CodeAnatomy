"""Helpers for mapping contracts to property field specs."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace

from cpg.kinds_ultimate import (
    EDGE_KIND_CONTRACTS,
    NODE_KIND_CONTRACTS,
    EdgeKind,
    EdgeKindContract,
    NodeKind,
    NodeKindContract,
)
from cpg.specs import PropFieldSpec, PropValueType

type PropFieldInput = PropFieldSpec | str
type Contract = NodeKindContract | EdgeKindContract


def _resolve_contract(
    *,
    node_kind: NodeKind | None,
    edge_kind: EdgeKind | None,
) -> Contract:
    if node_kind is None:
        if edge_kind is None:
            msg = "Provide exactly one of node_kind or edge_kind."
            raise ValueError(msg)
        return EDGE_KIND_CONTRACTS[edge_kind]
    if edge_kind is not None:
        msg = "Provide exactly one of node_kind or edge_kind."
        raise ValueError(msg)
    return NODE_KIND_CONTRACTS[node_kind]


def _allowed_keys(
    contract: Contract,
    *,
    include_optional: bool,
    include_required: bool,
) -> set[str]:
    allowed: set[str] = set()
    if include_required:
        allowed.update(contract.required_props.keys())
    if include_optional:
        allowed.update(contract.optional_props.keys())
    return allowed


def _missing_required(
    contract: Contract,
    source_map: Mapping[str, PropFieldInput],
    *,
    include_required: bool,
) -> set[str]:
    if not include_required:
        return set()
    return set(contract.required_props.keys()) - set(source_map.keys())


def _value_type(contract: Contract, key: str) -> PropValueType | None:
    spec = contract.required_props.get(key) or contract.optional_props.get(key)
    if spec is None:
        return None
    return spec.type


def _coerce_prop_field(
    contract: Contract,
    key: str,
    source: PropFieldInput,
) -> PropFieldSpec:
    if isinstance(source, PropFieldSpec):
        if source.prop_key != key:
            msg = f"PropFieldSpec key mismatch: {source.prop_key!r} != {key!r}"
            raise ValueError(msg)
        if source.value_type is None:
            value_type = _value_type(contract, key)
            if value_type is not None:
                return replace(source, value_type=value_type)
        return source
    return PropFieldSpec(
        prop_key=key,
        source_col=source,
        value_type=_value_type(contract, key),
    )


def _fields_from_source_map(
    *,
    contract: Contract,
    source_map: Mapping[str, PropFieldInput],
    allowed: set[str],
) -> tuple[PropFieldSpec, ...]:
    fields: list[PropFieldSpec] = []
    for key, source in source_map.items():
        if key not in allowed:
            msg = f"Prop key {key!r} not defined in contract."
            raise ValueError(msg)
        fields.append(_coerce_prop_field(contract, key, source))
    return tuple(fields)


def prop_fields_from_contract(
    *,
    node_kind: NodeKind | None = None,
    edge_kind: EdgeKind | None = None,
    source_map: Mapping[str, PropFieldInput],
    include_optional: bool = True,
    include_required: bool = True,
) -> tuple[PropFieldSpec, ...]:
    """Build PropFieldSpec entries validated against contracts.

    Returns
    -------
    tuple[PropFieldSpec, ...]
        Prop field specs validated against the contract.

    Raises
    ------
    ValueError
        Raised when the contract configuration is invalid.
    """
    contract = _resolve_contract(node_kind=node_kind, edge_kind=edge_kind)
    allowed = _allowed_keys(
        contract,
        include_optional=include_optional,
        include_required=include_required,
    )
    missing_required = _missing_required(
        contract,
        source_map,
        include_required=include_required,
    )
    if missing_required:
        msg = f"Missing required props for {contract}: {sorted(missing_required)}"
        raise ValueError(msg)
    return _fields_from_source_map(
        contract=contract,
        source_map=source_map,
        allowed=allowed,
    )


__all__ = ["PropFieldInput", "prop_fields_from_contract"]
