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
from tools.cq.core.structs import CqStruct
from tools.cq.core.summary_types import summary_from_mapping
from tools.cq.macros._rust_fallback import rust_fallback_search
from tools.cq.orchestration.multilang_summary import (
    build_multilang_summary,
    partition_stats_from_result_summary,
)
from tools.cq.search._shared.types import QueryMode
from tools.cq.search.semantic.diagnostics import (
    build_capability_diagnostics,
    diagnostics_to_summary_payload,
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


def _ensure_text_summary_field(summary: dict[str, object], *, key: str, value: str) -> None:
    current = summary.get(key)
    if isinstance(current, str) and current.strip():
        return
    summary[key] = value


class RustMacroFallbackRequestV1(CqStruct, frozen=True):
    """Input envelope for shared Rust macro fallback processing."""

    result: CqResult
    root: Path
    pattern: str
    macro_name: str
    fallback_matches: int = 0
    query: str | None = None
    mode: QueryMode = QueryMode.REGEX


def apply_rust_macro_fallback(request: RustMacroFallbackRequestV1) -> CqResult:
    """Apply shared Rust fallback behavior to a macro result.

    Returns:
        CqResult: Updated result with Rust fallback findings and summary metadata.
    """
    result = request.result
    rust_findings, capability_diags, rust_stats = rust_fallback_search(
        request.root,
        request.pattern,
        macro_name=request.macro_name,
        mode=request.mode,
    )
    result = extend_result_evidence(result, rust_findings)
    result = extend_result_key_findings(result, capability_diags)

    existing_summary = result.summary.to_dict()
    _ensure_text_summary_field(
        existing_summary,
        key="mode",
        value=f"macro:{request.macro_name}",
    )
    _ensure_text_summary_field(
        existing_summary,
        key="query",
        value=_derive_macro_query(
            query=request.query, pattern=request.pattern, argv=result.run.argv
        ),
    )
    py_stats = partition_stats_from_result_summary(
        existing_summary,
        fallback_matches=request.fallback_matches,
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


def apply_unsupported_macro_contract(
    *,
    result: CqResult,
    root: Path,
    macro_name: str,
    rust_only: bool,
    query: str | None = None,
) -> CqResult:
    """Apply deterministic unsupported-macro contract for Rust-none macros.

    Returns:
        CqResult: Updated result with capability-only Rust diagnostics.
    """
    capability_diags = build_capability_diagnostics(
        features=[f"macro:{macro_name}"],
        lang_scope="auto",
    )
    _ = root

    existing_summary = result.summary.to_dict()
    _ensure_text_summary_field(
        existing_summary,
        key="mode",
        value=f"macro:{macro_name}",
    )
    _ensure_text_summary_field(
        existing_summary,
        key="query",
        value=_derive_macro_query(query=query, pattern="", argv=result.run.argv),
    )
    py_stats = partition_stats_from_result_summary(
        existing_summary,
        fallback_matches=len(result.key_findings),
    )
    rust_stats = {
        "matches": 0,
        "files_scanned": 0,
        "scanned_files": 0,
        "scanned_files_is_estimate": False,
        "matched_files": 0,
        "total_matches": 0,
        "timed_out": False,
        "truncated": False,
        "caps_hit": "none",
    }
    updated_summary = summary_from_mapping(
        build_multilang_summary(
            SummaryBuildRequest(
                common=existing_summary,
                lang_scope="auto",
                language_order=None,
                languages={"python": py_stats, "rust": rust_stats},
                cross_language_diagnostics=diagnostics_to_summary_payload(capability_diags),
                language_capabilities=existing_summary.get("language_capabilities"),
            )
        )
    )

    if rust_only:
        return msgspec.structs.replace(
            result,
            summary=updated_summary,
            key_findings=tuple(capability_diags),
            evidence=(),
            sections=(),
        )

    return msgspec.structs.replace(
        result,
        summary=updated_summary,
        key_findings=(*result.key_findings, *tuple(capability_diags)),
    )


__all__ = [
    "RustMacroFallbackRequestV1",
    "apply_rust_macro_fallback",
    "apply_unsupported_macro_contract",
]
