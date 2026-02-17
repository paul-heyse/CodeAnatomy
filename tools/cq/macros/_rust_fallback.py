"""Shared Rust fallback search helper for CQ macros.

Provides a reusable helper that each macro can call to search Rust files
for a pattern, returning findings, diagnostics, and partition stats.
"""

from __future__ import annotations

from contextlib import suppress
from pathlib import Path

from tools.cq.core.schema import Anchor, Finding
from tools.cq.core.scoring import build_detail_payload
from tools.cq.search.semantic.diagnostics import (
    build_capability_diagnostics,
    features_from_macro,
)


def rust_fallback_search(
    root: Path,
    pattern: str,
    *,
    macro_name: str,
) -> tuple[list[Finding], list[Finding], dict[str, object]]:
    """Search Rust files for pattern, returning findings, diagnostics, and partition stats.

    Args:
        root: Repository root path.
        pattern: Search pattern (typically a function/symbol name).
        macro_name: Name of the calling macro for diagnostics.

    Returns:
        Tuple of (rust_findings, capability_diagnostics, rust_partition_stats).
    """
    from tools.cq.search.pipeline.profiles import INTERACTIVE
    from tools.cq.search.rg.adapter import search_content

    matches: list[tuple[Path, int, str]] = []
    with suppress(OSError, TimeoutError, RuntimeError, ValueError):
        matches = search_content(
            root,
            pattern,
            limits=INTERACTIVE,
            lang_scope="rust",
        )

    findings: list[Finding] = []
    for file_path, line_no, line_text in matches:
        try:
            rel_path = str(file_path.relative_to(root))
        except ValueError:
            rel_path = str(file_path)
        findings.append(
            Finding(
                category="rust_reference",
                message=(
                    f"Rust reference: {line_text.strip()}"
                    if line_text.strip()
                    else f"Rust match at {rel_path}:{line_no}"
                ),
                anchor=Anchor(file=rel_path, line=line_no),
                severity="info",
                details=build_detail_payload(
                    kind="rust_reference",
                    data={
                        "language": "rust",
                        "confidence": 0.45,
                        "evidence_kind": "rg_only",
                    },
                ),
            )
        )

    capability_diags = build_capability_diagnostics(
        features=features_from_macro(macro_name),
        lang_scope="auto",
    )

    matched_files: set[str] = set()
    for file_path, _line_no, _line_text in matches:
        try:
            matched_files.add(str(file_path.relative_to(root)))
        except ValueError:
            matched_files.add(str(file_path))

    partition_stats: dict[str, object] = {
        "matches": len(matches),
        "files_scanned": 0,
        "matched_files": len(matched_files),
        "total_matches": len(matches),
    }

    return findings, capability_diags, partition_stats
