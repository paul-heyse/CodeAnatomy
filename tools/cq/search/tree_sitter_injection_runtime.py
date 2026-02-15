"""Execution helpers for tree-sitter injection plans using `included_ranges`."""

from __future__ import annotations

from typing import TYPE_CHECKING

from tools.cq.search.tree_sitter_injection_contracts import InjectionRuntimeResultV1
from tools.cq.search.tree_sitter_injections import InjectionPlanV1

if TYPE_CHECKING:
    from tree_sitter import Language

try:
    from tree_sitter import Parser as _TreeSitterParser
    from tree_sitter import Point as _TreeSitterPoint
    from tree_sitter import Range as _TreeSitterRange
except ImportError:  # pragma: no cover - optional dependency
    _TreeSitterParser = None
    _TreeSitterPoint = None
    _TreeSitterRange = None


def parse_injected_ranges(
    *,
    source_bytes: bytes,
    language: Language,
    plans: tuple[InjectionPlanV1, ...],
) -> InjectionRuntimeResultV1:
    """Parse injected ranges for one target language.

    Returns:
        InjectionRuntimeResultV1: Result describing parse success and included-range
            behavior.
    """
    combined_count = sum(1 for row in plans if bool(getattr(row, "combined", False)))
    if _TreeSitterParser is None or _TreeSitterPoint is None or _TreeSitterRange is None:
        return InjectionRuntimeResultV1(
            language=str(getattr(language, "name", "unknown")),
            plan_count=len(plans),
            combined_count=combined_count,
            parsed=False,
            included_ranges_applied=False,
            errors=("tree_sitter_bindings_unavailable",),
        )
    parser = _TreeSitterParser(language)
    ranges = [
        _TreeSitterRange(
            _TreeSitterPoint(int(row.start_row), int(row.start_col)),
            _TreeSitterPoint(int(row.end_row), int(row.end_col)),
            int(row.start_byte),
            int(row.end_byte),
        )
        for row in plans
    ]
    try:
        parser.included_ranges = ranges
    except (RuntimeError, TypeError, ValueError, AttributeError) as exc:
        return InjectionRuntimeResultV1(
            language=str(getattr(language, "name", "unknown")),
            plan_count=len(plans),
            combined_count=combined_count,
            parsed=False,
            included_ranges_applied=False,
            errors=(type(exc).__name__,),
        )

    try:
        tree = parser.parse(source_bytes)
    except (RuntimeError, TypeError, ValueError, AttributeError) as exc:
        return InjectionRuntimeResultV1(
            language=str(getattr(language, "name", "unknown")),
            plan_count=len(plans),
            combined_count=combined_count,
            parsed=False,
            included_ranges_applied=True,
            errors=(type(exc).__name__,),
        )
    return InjectionRuntimeResultV1(
        language=str(getattr(language, "name", "unknown")),
        plan_count=len(plans),
        combined_count=combined_count,
        parsed=tree is not None,
        included_ranges_applied=True,
        errors=(),
    )


__all__ = ["parse_injected_ranges"]
