"""Neighborhood extraction for call contexts.

Computes context windows and integrates tree-sitter-based neighborhood
slices for call-site analysis.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import msgspec

from tools.cq.core.schema import Finding, ScoreDetails
from tools.cq.core.scoring import build_detail_payload

if TYPE_CHECKING:
    from tools.cq.core.front_door_insight import InsightNeighborhoodV1

_FRONT_DOOR_PREVIEW_PER_SLICE = 5


@dataclass(frozen=True)
class CallsNeighborhoodRequest:
    """Request envelope for neighborhood construction in calls front door."""

    root: Path
    function_name: str
    target_location: tuple[str, int] | None
    target_callees: Counter[str]
    analysis: CallAnalysisSummary
    score: ScoreDetails | None
    preview_per_slice: int


@dataclass(frozen=True)
class CallAnalysisSummary:
    """Derived analysis aggregates for call sites."""

    arg_shapes: Counter[str]
    kwarg_usage: Counter[str]
    forwarding_count: int
    contexts: Counter[str]
    hazard_counts: Counter[str]


def compute_calls_context_window(
    line: int,
    def_lines: list[tuple[int, int]],
    total_lines: int,
) -> dict[str, int]:
    """Public wrapper for callsite context window calculation.

    Returns:
        Inclusive context line bounds for a callsite.
    """
    return _compute_context_window(line, def_lines, total_lines)


def _compute_context_window(
    call_line: int,
    def_lines: list[tuple[int, int]],
    total_lines: int,
) -> dict[str, int]:
    """Compute context window for a call site.

    Finds the containing def based on indentation and computes line bounds.

    Parameters
    ----------
    call_line
        Line number of the call site.
    def_lines
        List of (line_number, indent_level) for all defs in the file.
    total_lines
        Total lines in the file.

    Returns:
    -------
    dict[str, int]
        Dict with 'start_line' and 'end_line' keys.
    """
    # Find containing def (nearest preceding def at same/lesser indent).
    # Allow equality so definition matches use their own block.
    containing_def: tuple[int, int] | None = None
    for def_line, def_indent in reversed(def_lines):
        if def_line <= call_line:
            containing_def = (def_line, def_indent)
            break

    if containing_def is None:
        return {"start_line": 1, "end_line": total_lines}

    start_line, def_indent = containing_def
    end_line = total_lines

    # Find end (next def at same/lesser indent or EOF)
    for def_line, indent in def_lines:
        if def_line > start_line and indent <= def_indent:
            end_line = def_line - 1
            break

    return {"start_line": start_line, "end_line": end_line}


def _build_calls_neighborhood(
    request: CallsNeighborhoodRequest,
) -> tuple[InsightNeighborhoodV1, list[Finding], list[str]]:
    from tools.cq.core.front_door_insight import (
        InsightNeighborhoodV1,
        InsightSliceV1,
        build_neighborhood_from_slices,
    )
    from tools.cq.core.snb_schema import SemanticNodeRefV1
    from tools.cq.neighborhood.contracts import (
        TreeSitterNeighborhoodCollectRequest,
    )
    from tools.cq.neighborhood.tree_sitter_collector import collect_tree_sitter_neighborhood

    neighborhood = InsightNeighborhoodV1()
    neighborhood_findings: list[Finding] = []
    degradation_notes: list[str] = []
    if request.target_location is None:
        degradation_notes.append("target_definition_unresolved")
    else:
        target_file, target_line = request.target_location
        try:
            collect_result = collect_tree_sitter_neighborhood(
                TreeSitterNeighborhoodCollectRequest(
                    root=str(request.root),
                    target_name=request.function_name.rsplit(".", maxsplit=1)[-1],
                    target_file=target_file,
                    language="python",
                    target_line=target_line,
                    target_col=0,
                    max_per_slice=request.preview_per_slice,
                )
            )
            slices = tuple(collect_result.slices)
            degrades = tuple(collect_result.diagnostics)
            neighborhood = build_neighborhood_from_slices(
                slices,
                preview_per_slice=request.preview_per_slice,
                source="structural",
            )
            for slice_item in slices:
                labels = [
                    node.display_label or node.name
                    for node in slice_item.preview[: request.preview_per_slice]
                    if (node.display_label or node.name)
                ]
                message = f"{slice_item.title}: {slice_item.total}"
                if labels:
                    message += f" (top: {', '.join(labels)})"
                neighborhood_findings.append(
                    Finding(
                        category="neighborhood",
                        message=message,
                        severity="info",
                        details=build_detail_payload(
                            data={
                                "slice_kind": slice_item.kind,
                                "total": slice_item.total,
                                "preview": labels,
                            },
                            score=request.score,
                        ),
                    )
                )
            degradation_notes.extend(
                f"{degrade.stage}:{degrade.category or degrade.severity}" for degrade in degrades
            )
        except (OSError, RuntimeError, TimeoutError, ValueError, TypeError) as exc:
            degradation_notes.append(f"tree_sitter_neighborhood_unavailable:{type(exc).__name__}")

    if neighborhood.callers.total == 0 and request.analysis.contexts:
        preview_nodes = tuple(
            SemanticNodeRefV1(
                node_id=f"context:{name}",
                kind="function",
                name=name,
                display_label=name,
                file_path="",
            )
            for name, _count in request.analysis.contexts.most_common(request.preview_per_slice)
        )
        neighborhood = msgspec.structs.replace(
            neighborhood,
            callers=InsightSliceV1(
                total=sum(request.analysis.contexts.values()),
                preview=preview_nodes,
                availability="partial",
                source="heuristic",
            ),
        )

    if neighborhood.callees.total == 0 and request.target_callees:
        preview_nodes = tuple(
            SemanticNodeRefV1(
                node_id=f"callee:{name}",
                kind="callsite",
                name=name,
                display_label=name,
                file_path=request.target_location[0] if request.target_location is not None else "",
            )
            for name, _count in request.target_callees.most_common(request.preview_per_slice)
        )
        neighborhood = msgspec.structs.replace(
            neighborhood,
            callees=InsightSliceV1(
                total=sum(request.target_callees.values()),
                preview=preview_nodes,
                availability="partial",
                source="heuristic",
            ),
        )
    return neighborhood, neighborhood_findings, degradation_notes


__all__ = [
    "CallAnalysisSummary",
    "CallsNeighborhoodRequest",
    "compute_calls_context_window",
]
