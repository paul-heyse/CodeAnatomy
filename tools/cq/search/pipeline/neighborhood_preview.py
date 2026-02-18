"""Neighborhood preview assembly helpers for smart-search output."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

from tools.cq.core.schema import Finding
from tools.cq.core.scoring import build_detail_payload
from tools.cq.search.pipeline.contracts import SearchConfig
from tools.cq.search.pipeline.smart_search_types import (
    EnrichedMatch,
    LanguageSearchResult,
    _NeighborhoodPreviewInputs,
)
from tools.cq.search.pipeline.target_resolution import candidate_scope_paths_for_neighborhood

if TYPE_CHECKING:
    from tools.cq.core.front_door_contracts import InsightNeighborhoodV1
    from tools.cq.core.schema import Section

__all__ = ["build_tree_sitter_neighborhood_preview"]

logger = logging.getLogger(__name__)


def _normalize_neighborhood_file_path(path: str) -> str:
    normalized = path.strip()
    if normalized.startswith("./"):
        normalized = normalized[2:]
    return Path(normalized).as_posix()


def _build_structural_neighborhood_preview(
    ctx: SearchConfig,
    *,
    primary_target_finding: Finding | None,
    definition_matches: list[EnrichedMatch],
) -> tuple[InsightNeighborhoodV1 | None, list[Finding], list[str]]:
    from tools.cq.core.front_door_search import build_neighborhood_from_slices
    from tools.cq.neighborhood.collector import collect_tree_sitter_neighborhood
    from tools.cq.neighborhood.contracts import TreeSitterNeighborhoodCollectRequest

    if primary_target_finding is None or primary_target_finding.anchor is None:
        logger.debug("neighborhood.preview_skipped reason=no_anchor")
        return None, [], []

    target_name = (
        str(primary_target_finding.details.get("name", "")).strip()
        or primary_target_finding.message.split(":")[-1].strip()
        or ctx.query
    )
    target_file = _normalize_neighborhood_file_path(primary_target_finding.anchor.file)
    target_language = (
        definition_matches[0].language
        if definition_matches
        else str(primary_target_finding.details.get("language", "python"))
    )
    collector_language = "rust" if target_language == "rust" else "python"
    try:
        collect_result = collect_tree_sitter_neighborhood(
            TreeSitterNeighborhoodCollectRequest(
                root=str(ctx.root),
                target_name=target_name,
                target_file=target_file,
                language=collector_language,
                target_line=primary_target_finding.anchor.line,
                target_col=int(primary_target_finding.anchor.col or 0),
                max_per_slice=5,
            )
        )
    except (OSError, RuntimeError, TimeoutError, ValueError, TypeError) as exc:
        logger.warning(
            "neighborhood.preview_unavailable language=%s reason=%s",
            collector_language,
            type(exc).__name__,
        )
        return None, [], [f"tree_sitter_neighborhood_unavailable:{type(exc).__name__}"]

    slices = tuple(collect_result.slices)
    degrades = tuple(collect_result.diagnostics)

    neighborhood = build_neighborhood_from_slices(
        slices,
        preview_per_slice=5,
        source="structural",
    )
    findings: list[Finding] = []
    for slice_item in slices:
        labels = [
            node.display_label or node.name
            for node in slice_item.preview[:5]
            if (node.display_label or node.name)
        ]
        message = f"{slice_item.title}: {slice_item.total}"
        if labels:
            message += f" (top: {', '.join(labels)})"
        findings.append(
            Finding(
                category="neighborhood",
                message=message,
                severity="info",
                details=build_detail_payload(
                    kind="neighborhood",
                    data={
                        "slice_kind": slice_item.kind,
                        "total": slice_item.total,
                        "preview": labels,
                    },
                ),
            )
        )
    notes = [f"{degrade.stage}:{degrade.category or degrade.severity}" for degrade in degrades]
    logger.debug(
        "neighborhood.preview_built slices=%d diagnostics=%d findings=%d",
        len(slices),
        len(degrades),
        len(findings),
    )
    return neighborhood, findings, list(dict.fromkeys(notes))


def build_tree_sitter_neighborhood_preview(
    *,
    ctx: SearchConfig,
    partition_results: list[LanguageSearchResult],
    sections: list[Section],
    inputs: _NeighborhoodPreviewInputs,
) -> tuple[InsightNeighborhoodV1 | None, list[str], dict[str, object]]:
    """Build tree-sitter neighborhood preview and insert findings into sections.

    Returns:
        tuple[InsightNeighborhoodV1 | None, list[str], dict[str, object]]:
            Neighborhood payload, notes, and summary neighborhood payload.
    """
    from tools.cq.search.pipeline.orchestration import insert_neighborhood_preview

    neighborhood_summary: dict[str, object] = {
        "enabled": bool(ctx.with_neighborhood),
        "mode": "opt_in",
    }
    if not ctx.with_neighborhood:
        logger.debug("neighborhood.preview_disabled")
        return None, ["tree_sitter_neighborhood_disabled_by_default"], neighborhood_summary

    scope_paths = candidate_scope_paths_for_neighborhood(
        ctx=ctx,
        partition_results=partition_results,
    )
    neighborhood_summary["candidate_scope_files"] = str(len(scope_paths))
    insight_neighborhood, neighborhood_findings, neighborhood_notes = (
        _build_structural_neighborhood_preview(
            ctx,
            primary_target_finding=inputs.primary_target_finding,
            definition_matches=inputs.definition_matches,
        )
    )
    insert_neighborhood_preview(
        sections,
        findings=neighborhood_findings,
        has_target_candidates=inputs.has_target_candidates,
    )
    logger.debug(
        "neighborhood.preview_inserted findings=%d notes=%d",
        len(neighborhood_findings),
        len(neighborhood_notes),
    )
    return insight_neighborhood, neighborhood_notes, neighborhood_summary
