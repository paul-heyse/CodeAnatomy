"""Target-resolution helpers for smart-search assembly."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, cast

from tools.cq.core.schema import Anchor, Finding
from tools.cq.search.objects.resolve import ObjectResolutionRuntime
from tools.cq.search.pipeline.contracts import SearchConfig
from tools.cq.search.pipeline.smart_search_types import EnrichedMatch, LanguageSearchResult

if TYPE_CHECKING:
    from tools.cq.search.objects.render import SearchObjectSummaryV1

__all__ = [
    "candidate_scope_paths_for_neighborhood",
    "collect_definition_candidates",
    "resolve_primary_target_match",
]

MAX_TARGET_CANDIDATES = 3
_TARGET_CANDIDATE_KINDS: frozenset[str] = frozenset(
    {
        "function",
        "method",
        "class",
        "type",
        "module",
        "callable",
    }
)

logger = logging.getLogger(__name__)


def _normalize_target_candidate_kind(kind: str | None) -> str:
    if not isinstance(kind, str) or not kind:
        return "reference"
    lowered = kind.lower()
    if lowered in {"method", "function", "callable"}:
        return "function"
    if lowered in {"class"}:
        return "class"
    if lowered in {"module"}:
        return "module"
    if lowered in {"type"}:
        return "type"
    return lowered


def _build_object_candidate_finding(
    *,
    summary: SearchObjectSummaryV1,
    representative: EnrichedMatch | None,
) -> Finding:
    from tools.cq.core.schema import DetailPayload

    object_ref = summary.object_ref
    kind = _normalize_target_candidate_kind(object_ref.kind)
    symbol = object_ref.symbol
    anchor: Anchor | None = None
    if isinstance(object_ref.canonical_file, str) and isinstance(object_ref.canonical_line, int):
        anchor = Anchor(
            file=object_ref.canonical_file,
            line=max(1, int(object_ref.canonical_line)),
            col=representative.col if representative is not None else None,
        )
    elif representative is not None:
        anchor = Anchor.from_span(representative.span)
    details = {
        "name": symbol,
        "kind": kind,
        "qualified_name": object_ref.qualified_name,
        "signature": representative.text.strip() if representative is not None else symbol,
        "object_id": object_ref.object_id,
        "language": representative.language if representative is not None else object_ref.language,
        "occurrence_count": summary.occurrence_count,
        "resolution_quality": object_ref.resolution_quality,
    }
    return Finding(
        category="definition",
        message=f"{kind}: {symbol}",
        anchor=anchor,
        severity="info",
        details=DetailPayload(
            kind=kind,
            data_items=tuple(sorted(cast("dict[str, object]", details).items())),
        ),
    )


def collect_definition_candidates(
    ctx: SearchConfig,
    object_runtime: ObjectResolutionRuntime,
) -> tuple[list[EnrichedMatch], list[Finding]]:
    """Collect definition candidates for target-candidate rendering.

    Returns:
        tuple[list[EnrichedMatch], list[Finding]]: Definition matches and candidate findings.
    """
    _ = ctx
    definition_matches: list[EnrichedMatch] = []
    candidate_findings: list[Finding] = []

    for summary in object_runtime.view.summaries:
        object_ref = summary.object_ref
        kind = _normalize_target_candidate_kind(object_ref.kind)
        if kind not in _TARGET_CANDIDATE_KINDS:
            continue

        representative = object_runtime.representative_matches.get(object_ref.object_id)
        if representative is not None:
            definition_matches.append(representative)
        candidate_findings.append(
            _build_object_candidate_finding(
                summary=summary,
                representative=representative,
            )
        )
        if len(candidate_findings) >= MAX_TARGET_CANDIDATES:
            break

    if candidate_findings:
        logger.debug(
            "target_resolution.candidates_built count=%d",
            len(candidate_findings),
        )
        return definition_matches, candidate_findings

    if not object_runtime.view.summaries:
        return definition_matches, candidate_findings
    fallback_summary = object_runtime.view.summaries[0]
    representative = object_runtime.representative_matches.get(
        fallback_summary.object_ref.object_id
    )
    if representative is not None:
        definition_matches.append(representative)
    candidate_findings.append(
        _build_object_candidate_finding(summary=fallback_summary, representative=representative)
    )
    logger.debug("target_resolution.fallback_candidate_built")
    return definition_matches, candidate_findings


def resolve_primary_target_match(
    *,
    candidate_findings: list[Finding],
    object_runtime: ObjectResolutionRuntime,
    definition_matches: list[EnrichedMatch],
    enriched_matches: list[EnrichedMatch],
) -> EnrichedMatch | None:
    """Resolve primary target match from candidate and fallback sources.

    Returns:
        EnrichedMatch | None: Best available primary target match.
    """
    if candidate_findings:
        object_id = candidate_findings[0].details.get("object_id")
        if isinstance(object_id, str):
            representative = object_runtime.representative_matches.get(object_id)
            if representative is not None:
                logger.debug("target_resolution.primary_from_candidate")
                return representative
    if definition_matches:
        logger.debug("target_resolution.primary_from_definitions")
        return definition_matches[0]
    if object_runtime.view.summaries:
        object_id = object_runtime.view.summaries[0].object_ref.object_id
        representative = object_runtime.representative_matches.get(object_id)
        if representative is not None:
            logger.debug("target_resolution.primary_from_summary")
            return representative
    if enriched_matches:
        logger.debug("target_resolution.primary_from_enriched_fallback")
        return enriched_matches[0]
    logger.debug("target_resolution.primary_missing")
    return None


def candidate_scope_paths_for_neighborhood(
    *,
    ctx: SearchConfig,
    partition_results: list[LanguageSearchResult],
) -> tuple[Path, ...]:
    """Collect candidate scope paths from partition raw-match payloads.

    Returns:
        tuple[Path, ...]: Existing candidate file paths for neighborhood assembly.
    """
    ordered_paths: dict[str, Path] = {}
    for partition in partition_results:
        for match in partition.raw_matches:
            rel_path = str(match.file).strip()
            if not rel_path or rel_path in ordered_paths:
                continue
            candidate = (ctx.root / rel_path).resolve()
            if candidate.exists():
                ordered_paths[rel_path] = candidate
    return tuple(ordered_paths.values())
