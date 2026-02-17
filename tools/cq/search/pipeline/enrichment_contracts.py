"""Typed enrichment payload contracts for smart-search matches."""

from __future__ import annotations

from collections.abc import Mapping

import msgspec


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


def _convert_contract[ContractT](
    raw: object,
    *,
    contract: type[ContractT],
) -> ContractT | None:
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
    payload: object,
) -> RustTreeSitterEnrichmentV1 | None:
    """Wrap rust enrichment payload into a typed contract.

    Returns:
        Typed enrichment payload or ``None`` when payload is not mapping-like
        (or cannot be coerced).
    """
    return _convert_contract(payload, contract=RustTreeSitterEnrichmentV1)


def wrap_python_enrichment(
    payload: object,
) -> PythonEnrichmentV1 | None:
    """Wrap python enrichment payload into a typed contract.

    Returns:
        Typed enrichment payload or ``None`` when payload is not mapping-like
        (or cannot be coerced).
    """
    return _convert_contract(payload, contract=PythonEnrichmentV1)


def wrap_python_semantic_enrichment(
    payload: object,
) -> PythonSemanticEnrichmentV1 | None:
    """Wrap python-semantic enrichment payload into a typed contract.

    Returns:
        Typed enrichment payload or ``None`` when payload is not mapping-like
        (or cannot be coerced).
    """
    return _convert_contract(payload, contract=PythonSemanticEnrichmentV1)


def rust_enrichment_payload(payload: RustTreeSitterEnrichmentV1 | None) -> dict[str, object]:
    """Extract rust enrichment payload as a mutable dictionary.

    Returns:
        Plain mapping payload; empty dictionary when payload is missing.
    """
    if payload is None:
        return {}
    return dict(payload.payload)


def python_enrichment_payload(payload: PythonEnrichmentV1 | None) -> dict[str, object]:
    """Extract python enrichment payload as a mutable dictionary.

    Returns:
        Plain mapping payload; empty dictionary when payload is missing.
    """
    if payload is None:
        return {}
    return dict(payload.payload)


def python_semantic_enrichment_payload(
    payload: PythonSemanticEnrichmentV1 | None,
) -> dict[str, object]:
    """Extract python semantic enrichment payload as a mutable dictionary.

    Returns:
        Plain mapping payload; empty dictionary when payload is missing.
    """
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
