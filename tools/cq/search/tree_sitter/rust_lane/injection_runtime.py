"""Execution helpers for tree-sitter injection plans using `included_ranges`."""

from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

from tree_sitter import Parser as _TreeSitterParser
from tree_sitter import Point as _TreeSitterPoint
from tree_sitter import Range as _TreeSitterRange

from tools.cq.search.tree_sitter.contracts.core_models import InjectionRuntimeResultV1
from tools.cq.search.tree_sitter.core.language_registry import load_tree_sitter_language
from tools.cq.search.tree_sitter.rust_lane.injections import InjectionPlanV1

if TYPE_CHECKING:
    from tree_sitter import Language


@dataclass(frozen=True, slots=True)
class _InjectionParseRun:
    """One parser run outcome for injection range execution."""

    parsed: bool
    included_ranges_applied: bool
    errors: tuple[str, ...] = ()


def _new_parser(language: Language) -> Any:
    try:
        return _TreeSitterParser(language)
    except TypeError:
        parser = _TreeSitterParser()
        parser.language = language
        return parser


def _build_ranges(plans: tuple[InjectionPlanV1, ...]) -> list[Any]:
    return [
        _TreeSitterRange(
            _TreeSitterPoint(int(row.start_row), int(row.start_col)),
            _TreeSitterPoint(int(row.end_row), int(row.end_col)),
            int(row.start_byte),
            int(row.end_byte),
        )
        for row in plans
    ]


def _parse_ranges_once(
    *,
    source_bytes: bytes,
    language: Language,
    plans: tuple[InjectionPlanV1, ...],
) -> _InjectionParseRun:
    parser = _new_parser(language)
    try:
        parser.included_ranges = _build_ranges(plans)
    except (RuntimeError, TypeError, ValueError, AttributeError) as exc:
        return _InjectionParseRun(
            parsed=False,
            included_ranges_applied=False,
            errors=(type(exc).__name__,),
        )
    try:
        tree = parser.parse(source_bytes)
    except (RuntimeError, TypeError, ValueError, AttributeError) as exc:
        return _InjectionParseRun(
            parsed=False,
            included_ranges_applied=True,
            errors=(type(exc).__name__,),
        )
    return _InjectionParseRun(
        parsed=tree is not None,
        included_ranges_applied=True,
    )


def _group_plans_by_language(
    plans: tuple[InjectionPlanV1, ...],
) -> tuple[tuple[str, tuple[InjectionPlanV1, ...]], ...]:
    grouped: OrderedDict[str, list[InjectionPlanV1]] = OrderedDict()
    for row in plans:
        language_name = row.language.strip().lower() if row.language else ""
        grouped.setdefault(language_name, []).append(row)
    return tuple((name, tuple(rows)) for name, rows in grouped.items())


def _resolve_language(default_language: Language, language_name: str) -> Language:
    if not language_name:
        return default_language
    try:
        loaded = load_tree_sitter_language(language_name)
    except (RuntimeError, TypeError, ValueError, AttributeError):
        loaded = None
    if loaded is None:
        return default_language
    return cast("Language", loaded)


def _parse_combined_groups(
    *,
    source_bytes: bytes,
    language: Language,
    plans: tuple[InjectionPlanV1, ...],
) -> tuple[_InjectionParseRun, ...]:
    if not plans:
        return ()
    runs: list[_InjectionParseRun] = []
    for language_name, group in _group_plans_by_language(plans):
        resolved = _resolve_language(language, language_name)
        runs.append(
            _parse_ranges_once(
                source_bytes=source_bytes,
                language=resolved,
                plans=group,
            )
        )
    return tuple(runs)


def _parse_separate_ranges(
    *,
    source_bytes: bytes,
    language: Language,
    plans: tuple[InjectionPlanV1, ...],
) -> tuple[_InjectionParseRun, ...]:
    if not plans:
        return ()
    rows: list[_InjectionParseRun] = []
    for row in plans:
        resolved = _resolve_language(language, row.language.strip().lower())
        rows.append(
            _parse_ranges_once(
                source_bytes=source_bytes,
                language=resolved,
                plans=(row,),
            )
        )
    return tuple(rows)


def _merge_runs(
    *,
    plans: tuple[InjectionPlanV1, ...],
    language: Language,
    combined_runs: tuple[_InjectionParseRun, ...],
    separate_runs: tuple[_InjectionParseRun, ...],
) -> InjectionRuntimeResultV1:
    all_runs = (*combined_runs, *separate_runs)
    combined_count = sum(1 for row in plans if bool(getattr(row, "combined", False)))
    include_children_count = sum(
        1 for row in plans if bool(getattr(row, "include_children", False))
    )
    errors = tuple(
        dict.fromkeys(
            error_name
            for run in all_runs
            for error_name in run.errors
            if isinstance(error_name, str) and error_name
        )
    )
    return InjectionRuntimeResultV1(
        language=str(getattr(language, "name", "unknown")),
        plan_count=len(plans),
        combined_count=combined_count,
        parsed=bool(all_runs) and all(run.parsed for run in all_runs),
        included_ranges_applied=bool(all_runs)
        and all(run.included_ranges_applied for run in all_runs),
        errors=errors,
        metadata={
            "include_children_count": include_children_count,
            "combined_runs": len(combined_runs),
            "separate_runs": len(separate_runs),
        },
    )


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
    combined_plans = tuple(row for row in plans if bool(getattr(row, "combined", False)))
    separate_plans = tuple(row for row in plans if not bool(getattr(row, "combined", False)))
    combined_runs = _parse_combined_groups(
        source_bytes=source_bytes,
        language=language,
        plans=combined_plans,
    )
    separate_runs = _parse_separate_ranges(
        source_bytes=source_bytes,
        language=language,
        plans=separate_plans,
    )
    return _merge_runs(
        plans=plans,
        language=language,
        combined_runs=combined_runs,
        separate_runs=separate_runs,
    )


__all__ = ["parse_injected_ranges"]
