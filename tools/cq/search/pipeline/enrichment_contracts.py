"""Typed enrichment payload contracts for smart-search matches."""

from __future__ import annotations

import enum
from collections.abc import Mapping

import msgspec


class IncrementalEnrichmentModeV1(enum.StrEnum):
    """Canonical incremental enrichment mode for smart-search integration."""

    TS_ONLY = "ts_only"
    TS_SYM = "ts_sym"
    TS_SYM_DIS = "ts_sym_dis"
    FULL = "full"

    @property
    def includes_symtable(self) -> bool:
        """Return whether this mode enables symtable enrichment."""
        return self in {
            IncrementalEnrichmentModeV1.TS_SYM,
            IncrementalEnrichmentModeV1.TS_SYM_DIS,
            IncrementalEnrichmentModeV1.FULL,
        }

    @property
    def includes_dis(self) -> bool:
        """Return whether this mode enables bytecode/dis enrichment."""
        return self in {
            IncrementalEnrichmentModeV1.TS_SYM_DIS,
            IncrementalEnrichmentModeV1.FULL,
        }

    @property
    def includes_inspect(self) -> bool:
        """Return whether this mode enables inspect/runtime enrichment."""
        return self is IncrementalEnrichmentModeV1.FULL


DEFAULT_INCREMENTAL_ENRICHMENT_MODE = IncrementalEnrichmentModeV1.TS_SYM


def parse_incremental_enrichment_mode(
    value: object,
    *,
    default: IncrementalEnrichmentModeV1 = DEFAULT_INCREMENTAL_ENRICHMENT_MODE,
) -> IncrementalEnrichmentModeV1:
    """Parse user/config value into canonical incremental enrichment mode.

    Returns:
        IncrementalEnrichmentModeV1: Parsed mode or the provided default.
    """
    if isinstance(value, IncrementalEnrichmentModeV1):
        return value
    if isinstance(value, str):
        text = value.strip().lower()
        for mode in IncrementalEnrichmentModeV1:
            if mode.value == text:
                return mode
    return default


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


class IncrementalEnrichmentV1(
    msgspec.Struct,
    frozen=True,
    omit_defaults=True,
    forbid_unknown_fields=True,
):
    """Typed incremental enrichment payload wrapper."""

    schema_version: int = 1
    mode: IncrementalEnrichmentModeV1 = DEFAULT_INCREMENTAL_ENRICHMENT_MODE
    payload: dict[str, object] = msgspec.field(default_factory=dict)


def _copy_payload(payload: Mapping[str, object]) -> dict[str, object]:
    return {key: value for key, value in payload.items() if isinstance(key, str)}


def _convert_contract[ContractT](
    raw: object,
    *,
    contract: type[ContractT],
    mode: IncrementalEnrichmentModeV1 | None = None,
) -> ContractT | None:
    if isinstance(raw, contract):
        return raw
    if not isinstance(raw, Mapping):
        return None
    body: dict[str, object] = {
        "schema_version": 1,
        "payload": _copy_payload(raw),
    }
    if mode is not None:
        body["mode"] = mode
    try:
        return msgspec.convert(
            body,
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
        RustTreeSitterEnrichmentV1 | None: Normalized payload contract.
    """
    return _convert_contract(payload, contract=RustTreeSitterEnrichmentV1)


def wrap_python_enrichment(
    payload: object,
) -> PythonEnrichmentV1 | None:
    """Wrap python enrichment payload into a typed contract.

    Returns:
        PythonEnrichmentV1 | None: Normalized payload contract.
    """
    return _convert_contract(payload, contract=PythonEnrichmentV1)


def wrap_incremental_enrichment(
    payload: object,
    *,
    mode: IncrementalEnrichmentModeV1 = DEFAULT_INCREMENTAL_ENRICHMENT_MODE,
) -> IncrementalEnrichmentV1 | None:
    """Wrap incremental enrichment payload into a typed contract.

    Returns:
        IncrementalEnrichmentV1 | None: Normalized payload contract.
    """
    if isinstance(payload, IncrementalEnrichmentV1):
        return payload
    parsed_mode = mode
    if isinstance(payload, Mapping):
        parsed_mode = parse_incremental_enrichment_mode(payload.get("mode"), default=mode)
    return _convert_contract(payload, contract=IncrementalEnrichmentV1, mode=parsed_mode)


def rust_enrichment_payload(payload: RustTreeSitterEnrichmentV1 | None) -> dict[str, object]:
    """Extract rust enrichment payload as a mutable dictionary.

    Returns:
        dict[str, object]: Copy of the rust enrichment payload.
    """
    if payload is None:
        return {}
    return dict(payload.payload)


def python_enrichment_payload(payload: PythonEnrichmentV1 | None) -> dict[str, object]:
    """Extract python enrichment payload as a mutable dictionary.

    Returns:
        dict[str, object]: Copy of the python enrichment payload.
    """
    if payload is None:
        return {}
    return dict(payload.payload)


def incremental_enrichment_payload(payload: IncrementalEnrichmentV1 | None) -> dict[str, object]:
    """Extract incremental enrichment payload as a mutable dictionary.

    Returns:
        dict[str, object]: Copy of the incremental enrichment payload with metadata.
    """
    if payload is None:
        return {}
    out = dict(payload.payload)
    out.setdefault("schema_version", payload.schema_version)
    out.setdefault("mode", payload.mode.value)
    return out


__all__ = [
    "DEFAULT_INCREMENTAL_ENRICHMENT_MODE",
    "IncrementalEnrichmentModeV1",
    "IncrementalEnrichmentV1",
    "PythonEnrichmentV1",
    "RustTreeSitterEnrichmentV1",
    "incremental_enrichment_payload",
    "parse_incremental_enrichment_mode",
    "python_enrichment_payload",
    "rust_enrichment_payload",
    "wrap_incremental_enrichment",
    "wrap_python_enrichment",
    "wrap_rust_enrichment",
]
