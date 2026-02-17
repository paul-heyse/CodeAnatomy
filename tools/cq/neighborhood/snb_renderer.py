"""Canonical SNB-to-CqResult rendering."""

from __future__ import annotations

from collections.abc import Mapping

import msgspec

from tools.cq.core.schema import (
    Artifact,
    CqResult,
    DetailPayload,
    Finding,
    RunMeta,
    Section,
    append_result_evidence,
    extend_result_key_findings,
    mk_result,
    update_result_summary,
)
from tools.cq.core.snb_schema import DegradeEventV1, SemanticNeighborhoodBundleV1
from tools.cq.core.structs import CqStruct
from tools.cq.neighborhood.section_layout import materialize_section_layout


class RenderSnbRequest(CqStruct, frozen=True):
    """Request envelope for rendering an SNB bundle to CQ result."""

    run: RunMeta
    bundle: SemanticNeighborhoodBundleV1
    target: str
    language: str
    top_k: int
    enable_semantic_enrichment: bool
    semantic_env: Mapping[str, object] | None = None


def render_snb_result(
    request: RenderSnbRequest,
) -> CqResult:
    """Render a semantic neighborhood bundle into `CqResult`.

    Returns:
        Rendered CQ result payload for neighborhood output.
    """
    result = mk_result(request.run)
    view = materialize_section_layout(request.bundle)

    result = _populate_summary(
        result=result,
        request=request,
    )
    result = _populate_findings(
        result=result, bundle=request.bundle, view=view, semantic_env=request.semantic_env
    )
    return _populate_artifacts(result=result, bundle=request.bundle)


def _populate_summary(
    *,
    result: CqResult,
    request: RenderSnbRequest,
) -> CqResult:
    bundle = request.bundle
    updates: dict[str, object] = {
        "target": request.target,
        "language": request.language,
        "top_k": request.top_k,
        "enable_semantic_enrichment": request.enable_semantic_enrichment,
        "bundle_id": bundle.bundle_id,
        "total_slices": len(bundle.slices),
        "total_diagnostics": len(bundle.diagnostics),
    }
    if bundle.subject is not None:
        updates["target_file"] = bundle.subject.file_path
        updates["target_name"] = bundle.subject.name
    if bundle.graph is not None:
        updates["total_nodes"] = bundle.graph.node_count
        updates["total_edges"] = bundle.graph.edge_count
    if request.semantic_env:
        for key in ("semantic_health", "semantic_quiescent", "semantic_position_encoding"):
            value = request.semantic_env.get(key)
            if value is not None:
                updates[key] = value
    return update_result_summary(result, updates)


def _populate_findings(
    *,
    result: CqResult,
    bundle: SemanticNeighborhoodBundleV1,
    view: object,
    semantic_env: Mapping[str, object] | None,
) -> CqResult:
    from tools.cq.neighborhood.section_layout import BundleViewV1

    if isinstance(view, BundleViewV1):
        result = extend_result_key_findings(
            result,
            [
                Finding(
                    category=finding_v1.category,
                    message=f"{finding_v1.label}: {finding_v1.value}",
                )
                for finding_v1 in view.key_findings
            ],
        )

        rendered_sections = [
            Section(
                title=section_v1.title,
                collapsed=section_v1.collapsed,
                findings=tuple(
                    Finding(
                        category="neighborhood",
                        message=item,
                    )
                    for item in section_v1.items
                ),
            )
            for section_v1 in view.sections
        ]
        result = msgspec.structs.replace(
            result,
            sections=(*result.sections, *tuple(rendered_sections)),
        )

    enrichment_payload: dict[str, object] = {
        "neighborhood_bundle": {
            "bundle_id": bundle.bundle_id,
            "subject_label": bundle.subject_label,
            "slice_count": len(bundle.slices),
            "diagnostic_count": len(bundle.diagnostics),
            "artifact_count": len(bundle.artifacts),
        },
        "degrade_events": [_degrade_event_dict(event) for event in bundle.diagnostics],
    }
    if semantic_env:
        for key in ("semantic_health", "semantic_quiescent", "semantic_position_encoding"):
            value = semantic_env.get(key)
            if value is not None:
                enrichment_payload[key] = value

    return append_result_evidence(
        result,
        Finding(
            category="neighborhood_bundle",
            message=f"Bundle `{bundle.bundle_id}` assembled with {len(bundle.slices)} slices",
            details=DetailPayload.from_legacy({"enrichment": enrichment_payload}),
        ),
    )


def _populate_artifacts(*, result: CqResult, bundle: SemanticNeighborhoodBundleV1) -> CqResult:
    artifacts = list(result.artifacts)
    artifacts.extend(
        Artifact(path=pointer.storage_path, format="json")
        for pointer in bundle.artifacts
        if pointer.storage_path
    )
    return msgspec.structs.replace(result, artifacts=tuple(artifacts))


def _degrade_event_dict(event: DegradeEventV1) -> dict[str, object]:
    return {
        "stage": event.stage,
        "severity": event.severity,
        "category": event.category,
        "message": event.message,
        "correlation_key": event.correlation_key,
    }


__all__ = ["RenderSnbRequest", "render_snb_result"]
