"""Serializable contracts for Python native-resolution payloads."""

from __future__ import annotations

import msgspec

from tools.cq.core.structs import CqOutputStruct


class PythonResolutionPayloadV1(CqOutputStruct, frozen=True):
    """Normalized subset of Python native-resolution output."""

    symbol: str | None = None
    symbol_role: str | None = None
    enclosing_callable: str | None = None
    enclosing_class: str | None = None
    qualified_name_candidates: tuple[dict[str, object], ...] = msgspec.field(default_factory=tuple)


def coerce_resolution_payload(payload: dict[str, object]) -> PythonResolutionPayloadV1:
    """Coerce raw resolution mapping into typed contract payload."""
    return msgspec.convert(payload, type=PythonResolutionPayloadV1, strict=False)


__all__ = ["PythonResolutionPayloadV1", "coerce_resolution_payload"]
