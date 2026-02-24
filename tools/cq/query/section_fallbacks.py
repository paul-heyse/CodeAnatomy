"""Section fallback helpers for query runtime outputs."""

from __future__ import annotations

from tools.cq.core.schema import CqResult, Section, append_result_section


def ensure_query_sections(result: CqResult, *, title: str = "Findings") -> CqResult:
    """Ensure one section exists when key findings are present.

    Returns:
        CqResult: Result with at least one section when findings exist.
    """
    if result.sections or not result.key_findings:
        return result
    return append_result_section(
        result,
        Section(title=title, findings=tuple(result.key_findings)),
    )


__all__ = ["ensure_query_sections"]
