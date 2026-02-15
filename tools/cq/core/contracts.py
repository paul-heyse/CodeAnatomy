"""Shared helpers for CQ msgspec contract boundary serialization."""

from __future__ import annotations

from collections.abc import Mapping
from typing import cast

import msgspec

from tools.cq.core.public_serialization import to_public_dict
from tools.cq.core.serialization import to_builtins
from tools.cq.core.structs import CqOutputStruct
from tools.cq.search._shared.search_contracts import SearchSummaryContract, summary_contract_to_dict


class ContractEnvelope(CqOutputStruct, frozen=True):
    """Generic typed envelope for contract payload transport."""

    payload: dict[str, object]


def contract_to_builtins(value: object) -> object:
    """Serialize a CQ contract object into builtins recursively.

    Returns:
        object: Builtins-only representation safe for JSON rendering.
    """
    if isinstance(value, msgspec.Struct):
        return to_public_dict(value)
    if (
        isinstance(value, list)
        and value
        and all(isinstance(item, msgspec.Struct) for item in value)
    ):
        return [to_public_dict(cast("msgspec.Struct", item)) for item in value]
    return to_builtins(value)


def summary_contract_to_mapping(
    contract: SearchSummaryContract,
    *,
    common: Mapping[str, object] | None,
) -> dict[str, object]:
    """Serialize canonical search summary contract to mapping payload.

    Returns:
        dict[str, object]: Summary mapping with deterministic, renderer-ready fields.
    """
    return summary_contract_to_dict(contract, common=common)


def require_mapping(value: object) -> dict[str, object]:
    """Return mapping payload or raise a deterministic contract error.

    Returns:
        dict[str, object]: Builtins payload when the contract is mapping-shaped.

    Raises:
        TypeError: If the payload is not mapping-shaped after conversion.
    """
    payload = contract_to_builtins(value)
    if isinstance(payload, dict):
        return cast("dict[str, object]", payload)
    msg = f"Expected mapping contract payload, got {type(payload).__name__}"
    raise TypeError(msg)


__all__ = [
    "ContractEnvelope",
    "contract_to_builtins",
    "require_mapping",
    "summary_contract_to_mapping",
]
