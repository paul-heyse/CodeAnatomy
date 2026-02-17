"""Shared utility helpers for CQ macro implementations."""

from __future__ import annotations

import ast
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Protocol

from tools.cq.core.scoring import (
    ConfidenceSignals,
    ImpactSignals,
    bucket,
    confidence_score,
    impact_score,
)
from tools.cq.index.files import build_repo_file_index, tabulate_files
from tools.cq.index.repo import resolve_repo_context
from tools.cq.macros.contracts import MacroScorePayloadV1, ScoringDetailsV1
from tools.cq.search.pipeline.profiles import INTERACTIVE
from tools.cq.search.rg.adapter import find_symbol_definition_files


class _AstVisitor(Protocol):
    def visit(self, node: ast.AST) -> object: ...


# ── Scope Filtering ──


def scope_filter_applied(
    include: Sequence[str] | None,
    exclude: Sequence[str] | None,
) -> bool:
    """Return whether scope include/exclude filters are configured."""
    return bool(include or exclude)


def resolve_macro_files(
    *,
    root: Path,
    include: Sequence[str] | None,
    exclude: Sequence[str] | None,
    extensions: tuple[str, ...],
) -> list[Path]:
    """Resolve macro scan files using shared include/exclude semantics.

    Returns:
        Files selected for the macro scan.
    """
    repo_context = resolve_repo_context(root)
    repo_index = build_repo_file_index(repo_context)
    globs: list[str] = []
    if include:
        globs.extend(str(pattern) for pattern in include if str(pattern).strip())
    if exclude:
        globs.extend(
            f"!{pattern}" for pattern in (str(value).strip() for value in exclude) if pattern
        )
    result = tabulate_files(
        repo_index,
        [repo_context.repo_root],
        globs or None,
        extensions=extensions,
    )
    return result.files


# ── File Scanning ──


def iter_files(
    *,
    root: Path,
    include: list[str] | None = None,
    exclude: list[str] | None = None,
    extensions: tuple[str, ...] = (".py",),
    max_files: int | None = None,
) -> list[Path]:
    """Resolve files for macro scans with optional include/exclude globs.

    Returns:
    -------
    list[Path]
        Candidate file paths, optionally truncated by ``max_files``.
    """
    rows = resolve_macro_files(
        root=root,
        include=include,
        exclude=exclude,
        extensions=extensions,
    )
    if max_files is None:
        return rows
    return rows[: max(0, int(max_files))]


def scan_python_files[VisitorT: _AstVisitor](
    root: Path,
    *,
    include: list[str] | None,
    exclude: list[str] | None,
    visitor_factory: Callable[[str], VisitorT],
    max_files: int | None = None,
) -> tuple[list[VisitorT], int]:
    """Parse Python files and run one visitor per file.

    Returns:
        tuple[list[VisitorT], int]: Visitor instances and files successfully scanned.
    """
    repo_root = resolve_repo_context(root).repo_root
    visitors: list[VisitorT] = []
    files_scanned = 0
    for pyfile in iter_files(
        root=repo_root,
        include=include,
        exclude=exclude,
        extensions=(".py",),
        max_files=max_files,
    ):
        rel = str(pyfile.relative_to(repo_root))
        try:
            source = pyfile.read_text(encoding="utf-8")
            tree = ast.parse(source, filename=rel)
        except (SyntaxError, OSError, UnicodeDecodeError):
            continue
        visitor = visitor_factory(rel)
        visitor.visit(tree)
        visitors.append(visitor)
        files_scanned += 1
    return visitors, files_scanned


# ── Target Resolution ──


def resolve_target_files(
    *,
    root: Path,
    target: str,
    max_files: int,
    include: list[str] | None = None,
    exclude: list[str] | None = None,
    extensions: tuple[str, ...] = (".py",),
) -> list[Path]:
    """Resolve explicit path targets or symbol-hint file candidates.

    Returns:
    -------
    list[Path]
        Files matching the explicit path or symbol-hint scan.
    """
    _ = extensions
    target_path = Path(target)
    if target_path.exists() and target_path.is_file():
        return [target_path.resolve()]
    rooted_target = root / target
    if rooted_target.exists() and rooted_target.is_file():
        return [rooted_target]

    files = find_symbol_definition_files(
        root=root,
        symbol_name=target,
        include_globs=include,
        exclude_globs=exclude,
        limits=INTERACTIVE,
    )
    return files[: max(0, int(max_files))]


# ── Scoring Utilities ──


def macro_scoring_details(
    *,
    sites: int,
    files: int,
    depth: int = 0,
    breakages: int = 0,
    ambiguities: int = 0,
    evidence_kind: str = "resolved_ast",
) -> ScoringDetailsV1:
    """Build a normalized scoring-details struct for macro findings.

    Returns:
    -------
    ScoringDetailsV1
        Scoring metrics and buckets for macro findings.
    """
    impact = impact_score(
        ImpactSignals(
            sites=sites,
            files=files,
            depth=depth,
            breakages=breakages,
            ambiguities=ambiguities,
        )
    )
    confidence = confidence_score(ConfidenceSignals(evidence_kind=evidence_kind))
    return ScoringDetailsV1(
        impact_score=impact,
        impact_bucket=bucket(impact),
        confidence_score=confidence,
        confidence_bucket=bucket(confidence),
        evidence_kind=evidence_kind,
    )


def macro_score_payload(*, files: int, findings: int) -> MacroScorePayloadV1:
    """Build a normalized macro scoring payload from simple counters.

    Returns:
    -------
    MacroScorePayloadV1
        Structured scoring payload with impact/confidence metrics.
    """
    scoring = macro_scoring_details(
        sites=findings,
        files=files,
        evidence_kind="resolved_ast",
    )
    return MacroScorePayloadV1(
        impact=scoring.impact_score,
        confidence=scoring.confidence_score,
        impact_bucket=scoring.impact_bucket,
        confidence_bucket=scoring.confidence_bucket,
        details={
            "files": files,
            "findings": findings,
        },
    )


__all__ = [
    "iter_files",
    "macro_score_payload",
    "macro_scoring_details",
    "resolve_macro_files",
    "resolve_target_files",
    "scan_python_files",
    "scope_filter_applied",
]
