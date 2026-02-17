"""Collapse policy for LDMD section metadata rendering."""

from __future__ import annotations

import msgspec

from tools.cq.core.structs import CqStruct


class LdmdCollapsePolicyV1(CqStruct, frozen=True):
    """Policy controlling default collapse state for LDMD sections."""

    uncollapsed_sections: tuple[str, ...] = (
        "target_tldr",
        "neighborhood_summary",
        "suggested_followups",
    )
    dynamic_collapse_thresholds: dict[str, int] = msgspec.field(
        default_factory=lambda: {
            "parents": 3,
            "enclosing_context": 1,
        }
    )
    default_collapsed: bool = True

    @classmethod
    def default(cls) -> LdmdCollapsePolicyV1:
        """Return default collapse policy."""
        return cls()

    def is_collapsed(self, section_id: str, *, total: int = 0) -> bool:
        """Return whether one section should be collapsed by default."""
        if section_id in self.uncollapsed_sections:
            return False
        threshold = self.dynamic_collapse_thresholds.get(section_id)
        if threshold is None:
            return self.default_collapsed
        return int(total) > threshold


__all__ = ["LdmdCollapsePolicyV1"]
