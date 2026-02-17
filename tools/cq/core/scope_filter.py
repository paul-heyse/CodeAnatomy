"""Scope filtering helpers for CQ results."""

from __future__ import annotations

import fnmatch
from collections.abc import Callable
from pathlib import Path

import msgspec

from tools.cq.core.schema import CqResult, Finding


def filter_findings_by_scope(
    result: CqResult,
    *,
    root: Path,
    in_dir: str | None = None,
    exclude: tuple[str, ...] = (),
    path_filter: Callable[[Path], bool] | None = None,
) -> CqResult:
    """Filter findings to a directory/exclude scope and optional path predicate.

    Returns:
        CqResult: Scope-filtered result payload.
    """
    if not in_dir and not exclude and path_filter is None:
        return result

    resolved_root = root.resolve()
    base = (resolved_root / in_dir).resolve() if in_dir else None
    exclude_patterns = tuple(pattern.lstrip("!") for pattern in exclude)

    def in_scope(finding: Finding) -> bool:
        anchor = finding.anchor
        if anchor is None:
            return True
        rel_path = Path(anchor.file)
        rel_posix = rel_path.as_posix()
        abs_path = (resolved_root / rel_path).resolve()
        if base is not None and not abs_path.is_relative_to(base):
            return False
        if exclude_patterns and any(
            fnmatch.fnmatch(rel_posix, pattern) for pattern in exclude_patterns
        ):
            return False
        return path_filter is None or path_filter(abs_path)

    key_findings = tuple(finding for finding in result.key_findings if in_scope(finding))
    evidence = tuple(finding for finding in result.evidence if in_scope(finding))
    section_rows = []
    for section in result.sections:
        section_findings = tuple(finding for finding in section.findings if in_scope(finding))
        if not section_findings:
            continue
        section_rows.append(msgspec.structs.replace(section, findings=section_findings))
    sections = tuple(section_rows)

    return msgspec.structs.replace(
        result,
        key_findings=key_findings,
        evidence=evidence,
        sections=sections,
    )


__all__ = ["filter_findings_by_scope"]
