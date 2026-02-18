"""Shared macro fallback helpers for multilang summary and diagnostics."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

import msgspec

from tools.cq.core.contracts import SummaryBuildRequest
from tools.cq.core.schema import (
    CqResult,
    extend_result_evidence,
    extend_result_key_findings,
)
from tools.cq.core.summary_types import summary_from_mapping
from tools.cq.macros._rust_fallback import rust_fallback_search
from tools.cq.orchestration.multilang_summary import (
    build_multilang_summary,
    partition_stats_from_result_summary,
)


def _derive_macro_query(
    *,
    query: str | None,
    pattern: str,
    argv: Sequence[str] | None,
) -> str:
    if query is not None and query.strip():
        return query.strip()
    if pattern.strip():
        return pattern.strip()
    if argv:
        tail = " ".join(str(arg) for arg in argv[2:] if str(arg).strip())
        if tail:
            return tail
    return "macro query"


def apply_rust_macro_fallback(
    *,
    result: CqResult,
    root: Path,
    pattern: str,
    macro_name: str,
    fallback_matches: int = 0,
    query: str | None = None,
) -> CqResult:
    """Apply shared Rust fallback behavior to a macro result.

    Returns:
        CqResult: Updated result with Rust fallback findings and summary metadata.
    """
    rust_findings, capability_diags, rust_stats = rust_fallback_search(
        root,
        pattern,
        macro_name=macro_name,
    )
    result = extend_result_evidence(result, rust_findings)
    result = extend_result_key_findings(result, capability_diags)

    existing_summary = result.summary.to_dict()
    existing_summary.setdefault("mode", f"macro:{macro_name}")
    existing_summary.setdefault(
        "query",
        _derive_macro_query(query=query, pattern=pattern, argv=result.run.argv),
    )
    py_stats = partition_stats_from_result_summary(
        existing_summary,
        fallback_matches=fallback_matches,
    )
    updated_summary = summary_from_mapping(
        build_multilang_summary(
            SummaryBuildRequest(
                common=existing_summary,
                lang_scope="auto",
                language_order=None,
                languages={"python": py_stats, "rust": rust_stats},
            )
        )
    )
    return msgspec.structs.replace(result, summary=updated_summary)


__all__ = [
    "apply_rust_macro_fallback",
]
