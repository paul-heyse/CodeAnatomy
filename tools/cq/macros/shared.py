"""Shared utility helpers for CQ macro implementations."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import cast

from tools.cq.core.scoring import (
    ConfidenceSignals,
    ImpactSignals,
    bucket,
    confidence_score,
    impact_score,
)
from tools.cq.index.files import build_repo_file_index, tabulate_files
from tools.cq.index.repo import resolve_repo_context
from tools.cq.macros.contracts import MacroScorePayloadV1

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
    target_path = Path(target)
    if target_path.exists() and target_path.is_file():
        return [target_path.resolve()]
    rooted_target = root / target
    if rooted_target.exists() and rooted_target.is_file():
        return [rooted_target]

    files: list[Path] = []
    for candidate in iter_files(
        root=root,
        include=include,
        exclude=exclude,
        extensions=extensions,
        max_files=max_files,
    ):
        try:
            source = candidate.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue
        if f"def {target}" in source or f"class {target}" in source:
            files.append(candidate)
    return files


# ── Scoring Utilities ──


def macro_scoring_details(
    *,
    sites: int,
    files: int,
    depth: int = 0,
    breakages: int = 0,
    ambiguities: int = 0,
    evidence_kind: str = "resolved_ast",
) -> dict[str, object]:
    """Build a normalized scoring-details mapping for macro findings.

    Returns:
    -------
    dict[str, object]
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
    return {
        "impact_score": impact,
        "impact_bucket": bucket(impact),
        "confidence_score": confidence,
        "confidence_bucket": bucket(confidence),
        "evidence_kind": evidence_kind,
    }


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
    impact = cast("float", scoring["impact_score"])
    confidence = cast("float", scoring["confidence_score"])
    return MacroScorePayloadV1(
        impact=impact,
        confidence=confidence,
        impact_bucket=str(scoring["impact_bucket"]),
        confidence_bucket=str(scoring["confidence_bucket"]),
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
    "scope_filter_applied",
]
