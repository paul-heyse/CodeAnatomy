"""Section assembly helpers for smart-search front-door rendering."""

from __future__ import annotations

from tools.cq.core.schema import Finding, Section


def insert_target_candidates(
    sections: list[Section],
    *,
    candidates: list[Finding],
) -> None:
    """Insert Target Candidates section at top when candidates exist."""
    if not candidates:
        return
    sections.insert(0, Section(title="Target Candidates", findings=candidates))


def insert_neighborhood_preview(
    sections: list[Section],
    *,
    findings: list[Finding],
    has_target_candidates: bool,
) -> None:
    """Insert Neighborhood Preview section after candidates when present."""
    if not findings:
        return
    insert_idx = 1 if has_target_candidates else 0
    sections.insert(insert_idx, Section(title="Neighborhood Preview", findings=findings))


__all__ = ["insert_neighborhood_preview", "insert_target_candidates"]
