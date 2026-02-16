"""Typed contracts for artifact-first CQ diagnostic payloads."""

from __future__ import annotations

from typing import TYPE_CHECKING

import msgspec

from tools.cq.core.structs import CqStruct
from tools.cq.core.typed_boundary import BoundaryDecodeError, convert_lax

if TYPE_CHECKING:
    from tools.cq.core.schema import CqResult


class DiagnosticsArtifactRunMetaV1(CqStruct, frozen=True):
    """Run metadata persisted with offloaded diagnostics payloads."""

    macro: str
    root: str
    run_id: str | None = None


class DiagnosticsArtifactPayloadV1(CqStruct, frozen=True):
    """Typed diagnostics payload for artifact-first rendering."""

    run_meta: DiagnosticsArtifactRunMetaV1
    enrichment_telemetry: dict[str, object] = msgspec.field(default_factory=dict)
    python_semantic_telemetry: dict[str, object] = msgspec.field(default_factory=dict)
    rust_semantic_telemetry: dict[str, object] = msgspec.field(default_factory=dict)
    semantic_planes: dict[str, object] = msgspec.field(default_factory=dict)
    python_semantic_diagnostics: list[dict[str, object]] = msgspec.field(default_factory=list)
    language_capabilities: dict[str, object] = msgspec.field(default_factory=dict)
    cross_language_diagnostics: list[dict[str, object]] = msgspec.field(default_factory=list)


def build_diagnostics_artifact_payload(result: CqResult) -> DiagnosticsArtifactPayloadV1 | None:
    """Build typed diagnostics artifact payload from result summary.

    Returns:
    -------
    DiagnosticsArtifactPayloadV1 | None
        Typed diagnostics payload, or ``None`` when no diagnostic data is present.
    """
    summary = result.summary
    payload = DiagnosticsArtifactPayloadV1(
        run_meta=DiagnosticsArtifactRunMetaV1(
            macro=result.run.macro,
            root=result.run.root,
            run_id=result.run.run_id,
        ),
        enrichment_telemetry=_safe_dict(summary.enrichment_telemetry),
        python_semantic_telemetry=_safe_dict(summary.python_semantic_telemetry),
        rust_semantic_telemetry=_safe_dict(summary.rust_semantic_telemetry),
        semantic_planes=_safe_dict(summary.semantic_planes),
        python_semantic_diagnostics=_safe_list_of_dict(summary.python_semantic_diagnostics),
        language_capabilities=_safe_dict(summary.language_capabilities),
        cross_language_diagnostics=_safe_list_of_dict(summary.cross_language_diagnostics),
    )
    has_data = any(
        (
            payload.enrichment_telemetry,
            payload.python_semantic_telemetry,
            payload.rust_semantic_telemetry,
            payload.semantic_planes,
            payload.python_semantic_diagnostics,
            payload.language_capabilities,
            payload.cross_language_diagnostics,
        )
    )
    if not has_data:
        return None
    return payload


def _safe_dict(value: object) -> dict[str, object]:
    try:
        return convert_lax(value, type_=dict[str, object])
    except BoundaryDecodeError:
        return {}


def _safe_list_of_dict(value: object) -> list[dict[str, object]]:
    try:
        return convert_lax(value, type_=list[dict[str, object]])
    except BoundaryDecodeError:
        return []


__all__ = [
    "DiagnosticsArtifactPayloadV1",
    "DiagnosticsArtifactRunMetaV1",
    "build_diagnostics_artifact_payload",
]
