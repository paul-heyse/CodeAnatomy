"""Typed summary envelope contracts for CQ output assembly."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import msgspec

from tools.cq.core.contract_codec import require_mapping
from tools.cq.core.structs import CqStrictOutputStruct


class SummaryEnvelopeV1(CqStrictOutputStruct, frozen=True):
    """Canonical summary envelope for report/render boundaries."""

    summary: dict[str, Any] = msgspec.field(default_factory=dict)
    diagnostics: list[dict[str, Any]] = msgspec.field(default_factory=list)
    telemetry: dict[str, Any] = msgspec.field(default_factory=dict)


def build_summary_envelope(
    *,
    summary: Mapping[str, Any],
    diagnostics: list[dict[str, Any]] | None = None,
    telemetry: Mapping[str, Any] | None = None,
) -> SummaryEnvelopeV1:
    """Build a typed summary envelope from mapping surfaces.

    Returns:
        SummaryEnvelopeV1: Function return value.
    """
    return SummaryEnvelopeV1(
        summary=dict(summary),
        diagnostics=[dict(row) for row in (diagnostics or [])],
        telemetry=dict(telemetry) if isinstance(telemetry, Mapping) else {},
    )


def summary_envelope_to_mapping(envelope: SummaryEnvelopeV1) -> dict[str, Any]:
    """Convert summary envelope to mapping payload.

    Returns:
        dict[str, Any]: Function return value.
    """
    return require_mapping(envelope)


__all__ = [
    "SummaryEnvelopeV1",
    "build_summary_envelope",
    "summary_envelope_to_mapping",
]
