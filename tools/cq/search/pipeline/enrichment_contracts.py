"""Typed enrichment payload contracts for smart-search matches."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TypeVar

import msgspec

_ContractT = TypeVar("_ContractT")


class RustTreeSitterEnrichmentV1(
    msgspec.Struct,
    frozen=True,
    omit_defaults=True,
    forbid_unknown_fields=True,
):
    """Typed rust enrichment payload wrapper."""

    schema_version: int = 1
    payload: dict[str, object] = msgspec.field(default_factory=dict)


class PythonEnrichmentV1(
    msgspec.Struct,
    frozen=True,
    omit_defaults=True,
    forbid_unknown_fields=True,
):
    """Typed python enrichment payload wrapper."""

    schema_version: int = 1
    payload: dict[str, object] = msgspec.field(default_factory=dict)


class PythonSemanticEnrichmentV1(
    msgspec.Struct,
    frozen=True,
    omit_defaults=True,
    forbid_unknown_fields=True,
):
    """Typed python semantic enrichment payload wrapper."""

    schema_version: int = 1
    payload: dict[str, object] = msgspec.field(default_factory=dict)


def _copy_payload(payload: Mapping[str, object]) -> dict[str, object]:
    return {key: value for key, value in payload.items() if isinstance(key, str)}


def _convert_contract(
    raw: object,
    *,
    contract: type[_ContractT],
) -> _ContractT | None:
    if isinstance(raw, contract):
        return raw
    if not isinstance(raw, Mapping):
        return None
    try:
        return msgspec.convert(
            {"schema_version": 1, "payload": _copy_payload(raw)},
            type=contract,
            strict=True,
        )
    except (msgspec.ValidationError, TypeError, ValueError):
        return None


def wrap_rust_enrichment(
    payload: Mapping[str, object] | RustTreeSitterEnrichmentV1 | None,
) -> RustTreeSitterEnrichmentV1 | None:
    """Wrap raw rust enrichment mapping into a typed contract."""
    return _convert_contract(payload, contract=RustTreeSitterEnrichmentV1)


def wrap_python_enrichment(
    payload: Mapping[str, object] | PythonEnrichmentV1 | None,
) -> PythonEnrichmentV1 | None:
    """Wrap raw python enrichment mapping into a typed contract."""
    return _convert_contract(payload, contract=PythonEnrichmentV1)


def wrap_python_semantic_enrichment(
    payload: Mapping[str, object] | PythonSemanticEnrichmentV1 | None,
) -> PythonSemanticEnrichmentV1 | None:
    """Wrap raw python-semantic enrichment mapping into a typed contract."""
    return _convert_contract(payload, contract=PythonSemanticEnrichmentV1)


def rust_enrichment_payload(payload: RustTreeSitterEnrichmentV1 | None) -> dict[str, object]:
    if payload is None:
        return {}
    return dict(payload.payload)


def python_enrichment_payload(payload: PythonEnrichmentV1 | None) -> dict[str, object]:
    if payload is None:
        return {}
    return dict(payload.payload)


def python_semantic_enrichment_payload(
    payload: PythonSemanticEnrichmentV1 | None,
) -> dict[str, object]:
    if payload is None:
        return {}
    return dict(payload.payload)


__all__ = [
    "PythonEnrichmentV1",
    "PythonSemanticEnrichmentV1",
    "RustTreeSitterEnrichmentV1",
    "python_enrichment_payload",
    "python_semantic_enrichment_payload",
    "rust_enrichment_payload",
    "wrap_python_enrichment",
    "wrap_python_semantic_enrichment",
    "wrap_rust_enrichment",
]
