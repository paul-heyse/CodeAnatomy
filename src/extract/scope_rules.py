"""Pathspec-backed scope rule evaluation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

from pathspec import PathSpec
from pathspec.util import detailed_match_files, normalize_file

from extract.cache_utils import stable_cache_key

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping


@dataclass(frozen=True)
class ScopeRuleSet:
    """Compiled include/exclude scope rules."""

    include_spec: PathSpec | None
    exclude_spec: PathSpec | None
    include_lines: tuple[str, ...]
    exclude_lines: tuple[str, ...]

    def signature(self) -> str:
        """Return a stable signature for the rule set.

        Returns
        -------
        str
            Stable signature hash.
        """
        return stable_cache_key(
            "scope_rules",
            {"include": list(self.include_lines), "exclude": list(self.exclude_lines)},
        )


@dataclass(frozen=True)
class ScopeRuleDecision:
    """Decision payload for a scoped path."""

    include: bool
    include_index: int | None
    exclude_index: int | None
    normalized_path: str


def build_scope_rules(
    *, include_lines: Iterable[str], exclude_lines: Iterable[str]
) -> ScopeRuleSet:
    """Compile a scope rule set from include/exclude lines.

    Returns
    -------
    ScopeRuleSet
        Compiled scope rules.
    """
    include_list = [str(line) for line in include_lines if str(line)]
    exclude_list = [str(line) for line in exclude_lines if str(line)]
    include_spec = (
        PathSpec.from_lines("gitwildmatch", cast("list[Any]", include_list))
        if include_list
        else None
    )
    exclude_spec = (
        PathSpec.from_lines("gitwildmatch", cast("list[Any]", exclude_list))
        if exclude_list
        else None
    )
    return ScopeRuleSet(
        include_spec=include_spec,
        exclude_spec=exclude_spec,
        include_lines=tuple(include_list),
        exclude_lines=tuple(exclude_list),
    )


def check_scope_path(path: str, rules: ScopeRuleSet) -> ScopeRuleDecision:
    """Return a scope decision for a path.

    Returns
    -------
    ScopeRuleDecision
        Scope decision payload.
    """
    normalized = normalize_file(path)
    include = True
    include_index: int | None = None
    exclude_index: int | None = None
    if rules.include_spec is not None:
        include_result = rules.include_spec.check_file(normalized)
        include_index = include_result.index
        include = include_result.include is True
    if include and rules.exclude_spec is not None:
        exclude_result = rules.exclude_spec.check_file(normalized)
        exclude_index = exclude_result.index
        if exclude_result.include is True:
            include = False
    return ScopeRuleDecision(
        include=include,
        include_index=include_index,
        exclude_index=exclude_index,
        normalized_path=normalized,
    )


def explain_scope_paths(paths: Iterable[str], rules: ScopeRuleSet) -> Mapping[str, object]:
    """Return detailed pathspec match diagnostics.

    Returns
    -------
    Mapping[str, object]
        Detailed pathspec match payloads.
    """
    path_list = [normalize_file(path) for path in paths]
    include_detail: Mapping[str, object]
    if rules.include_spec is not None:
        include_detail = cast(
            "Mapping[str, object]",
            detailed_match_files(rules.include_spec.patterns, path_list),
        )
    else:
        include_detail = {}
    exclude_detail: Mapping[str, object]
    if rules.exclude_spec is not None:
        exclude_detail = cast(
            "Mapping[str, object]",
            detailed_match_files(rules.exclude_spec.patterns, path_list),
        )
    else:
        exclude_detail = {}
    return {"include": include_detail, "exclude": exclude_detail}


__all__ = [
    "ScopeRuleDecision",
    "ScopeRuleSet",
    "build_scope_rules",
    "check_scope_path",
    "explain_scope_paths",
]
