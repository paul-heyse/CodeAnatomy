"""Ripgrep prefilter helpers for ast-grep batch scans."""

from __future__ import annotations

import re
from pathlib import Path
from typing import TYPE_CHECKING

from tools.cq.core.pathing import normalize_repo_relative_path
from tools.cq.query.language import ripgrep_types_for_scope
from tools.cq.search.pipeline.classifier import QueryMode
from tools.cq.search.pipeline.profiles import INTERACTIVE, SearchLimits
from tools.cq.search.rg.runner import run_rg_files_with_matches

if TYPE_CHECKING:
    from tools.cq.astgrep.sgpy_scanner import RuleSpec
    from tools.cq.query.language import QueryLanguageScope

_METAVAR_TOKEN_RE = re.compile(r"\${1,3}_?[A-Z][A-Z0-9_]*")
_MIN_PREFILTER_LITERAL_LEN = 3


def extract_literal_fragments(pattern: str) -> list[str]:
    """Return non-metavariable literal fragments sorted by selectivity."""
    fragments = _METAVAR_TOKEN_RE.split(pattern)
    cleaned = [fragment.strip() for fragment in fragments if fragment.strip()]
    return sorted(cleaned, key=len, reverse=True)


def collect_prefilter_fragments(rules: tuple[RuleSpec, ...]) -> tuple[str, ...]:
    """Collect a high-signal literal set from ast-grep rule patterns."""
    fragments: list[str] = []
    for rule in rules:
        pattern = rule.to_config().get("pattern")
        if isinstance(pattern, str):
            fragments.extend(extract_literal_fragments(pattern)[:2])
    unique = sorted(
        {value for value in fragments if len(value) >= _MIN_PREFILTER_LITERAL_LEN},
        key=len,
        reverse=True,
    )
    return tuple(unique[:8])


def _resolve_prefilter_path(root: Path, rel: str) -> Path:
    candidate = Path(rel)
    if candidate.is_absolute():
        return candidate.resolve()
    return (root / rel).resolve()


def rg_prefilter_files(
    root: Path,
    *,
    files: list[Path],
    literals: tuple[str, ...],
    lang_scope: QueryLanguageScope,
    limits: SearchLimits | None = None,
) -> list[Path]:
    """Return subset of files that contain at least one literal fragment."""
    if not literals or not files:
        return files

    resolved_root = root.resolve()
    candidate_paths = tuple(
        normalize_repo_relative_path(path.resolve(), root=resolved_root) for path in files
    )
    matched_rel = run_rg_files_with_matches(
        root=resolved_root,
        patterns=literals,
        mode=QueryMode.LITERAL,
        lang_types=tuple(ripgrep_types_for_scope(lang_scope)),
        include_globs=[],
        exclude_globs=[],
        paths=candidate_paths,
        limits=limits or INTERACTIVE,
    )
    if matched_rel is None:
        return files

    allowed = {value for value in matched_rel if value}
    if not allowed:
        return []

    selected: list[Path] = []
    for file_path in files:
        rel = normalize_repo_relative_path(file_path.resolve(), root=resolved_root)
        if rel in allowed:
            selected.append(file_path.resolve())
    if selected:
        return sorted(selected)

    # Fallback for absolute rows from non-standard rg output.
    return sorted(_resolve_prefilter_path(resolved_root, rel) for rel in matched_rel)


def maybe_prefilter_astgrep_files(
    root: Path,
    *,
    files: list[Path],
    rules: tuple[RuleSpec, ...],
    lang_scope: QueryLanguageScope,
    limits: SearchLimits | None = None,
    enabled: bool = True,
) -> list[Path]:
    """Apply rg prefilter for multi-file ast-grep scans, fail-open on no literals."""
    if not enabled or len(files) <= 1 or not rules:
        return files
    literals = collect_prefilter_fragments(rules)
    return rg_prefilter_files(
        root,
        files=files,
        literals=literals,
        lang_scope=lang_scope,
        limits=limits,
    )


__all__ = [
    "collect_prefilter_fragments",
    "extract_literal_fragments",
    "maybe_prefilter_astgrep_files",
    "rg_prefilter_files",
]
