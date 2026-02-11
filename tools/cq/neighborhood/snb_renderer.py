# ruff: noqa: DOC201,PLR0913
"""Canonical SNB-to-CqResult rendering."""

from __future__ import annotations

from collections.abc import Mapping

from tools.cq.core.schema import (
    Artifact,
    CqResult,
    DetailPayload,
    Finding,
    RunMeta,
    Section,
    mk_result,
)
from tools.cq.core.snb_schema import DegradeEventV1, SemanticNeighborhoodBundleV1
from tools.cq.neighborhood.section_layout import materialize_section_layout


def render_snb_result(
    *,
    run: RunMeta,
    bundle: SemanticNeighborhoodBundleV1,
    target: str,
    language: str,
    top_k: int,
    enable_lsp: bool,
    lsp_env: Mapping[str, object] | None = None,
) -> CqResult:
    """Render a semantic neighborhood bundle into `CqResult`."""
    result = mk_result(run)
    view = materialize_section_layout(bundle)

    _populate_summary(
        result=result,
        bundle=bundle,
        target=target,
        language=language,
        top_k=top_k,
        enable_lsp=enable_lsp,
        lsp_env=lsp_env,
    )
    _populate_findings(result=result, bundle=bundle, view=view, lsp_env=lsp_env)
    _populate_artifacts(result=result, bundle=bundle)
    return result


def _populate_summary(
    *,
    result: CqResult,
    bundle: SemanticNeighborhoodBundleV1,
    target: str,
    language: str,
    top_k: int,
    enable_lsp: bool,
    lsp_env: Mapping[str, object] | None,
) -> None:
    result.summary["target"] = target
    result.summary["language"] = language
    result.summary["top_k"] = top_k
    result.summary["enable_lsp"] = enable_lsp
    result.summary["bundle_id"] = bundle.bundle_id
    result.summary["total_slices"] = len(bundle.slices)
    result.summary["total_diagnostics"] = len(bundle.diagnostics)
    if bundle.subject is not None:
        result.summary["target_file"] = bundle.subject.file_path
        result.summary["target_name"] = bundle.subject.name
    if bundle.graph is not None:
        result.summary["total_nodes"] = bundle.graph.node_count
        result.summary["total_edges"] = bundle.graph.edge_count
    if lsp_env:
        for key in ("lsp_health", "lsp_quiescent", "lsp_position_encoding"):
            value = lsp_env.get(key)
            if value is not None:
                result.summary[key] = value


def _populate_findings(
    *,
    result: CqResult,
    bundle: SemanticNeighborhoodBundleV1,
    view: object,
    lsp_env: Mapping[str, object] | None,
) -> None:
    from tools.cq.neighborhood.section_layout import BundleViewV1

    if isinstance(view, BundleViewV1):
        for finding_v1 in view.key_findings:
            result.key_findings.append(
                Finding(
                    category=finding_v1.category,
                    message=f"{finding_v1.label}: {finding_v1.value}",
                )
            )

        for section_v1 in view.sections:
            result.sections.append(
                Section(
                    title=section_v1.title,
                    collapsed=section_v1.collapsed,
                    findings=[
                        Finding(
                            category="neighborhood",
                            message=item,
                        )
                        for item in section_v1.items
                    ],
                )
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
    if lsp_env:
        for key in ("lsp_health", "lsp_quiescent", "lsp_position_encoding"):
            value = lsp_env.get(key)
            if value is not None:
                enrichment_payload[key] = value

    result.evidence.append(
        Finding(
            category="neighborhood_bundle",
            message=f"Bundle `{bundle.bundle_id}` assembled with {len(bundle.slices)} slices",
            details=DetailPayload.from_legacy({"enrichment": enrichment_payload}),
        )
    )


def _populate_artifacts(*, result: CqResult, bundle: SemanticNeighborhoodBundleV1) -> None:
    for pointer in bundle.artifacts:
        if pointer.storage_path:
            result.artifacts.append(
                Artifact(
                    path=pointer.storage_path,
                    format="json",
                )
            )


def _degrade_event_dict(event: DegradeEventV1) -> dict[str, object]:
    return {
        "stage": event.stage,
        "severity": event.severity,
        "category": event.category,
        "message": event.message,
        "correlation_key": event.correlation_key,
    }


__all__ = ["render_snb_result"]
