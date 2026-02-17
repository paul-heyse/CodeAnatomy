"""Entity scan orchestration helpers for query execution."""

from __future__ import annotations

from pathlib import Path

from tools.cq.astgrep.sgpy_scanner import SgRecord
from tools.cq.query.execution_context import QueryExecutionContext
from tools.cq.query.fragment_cache import (
    assemble_entity_records,
    build_entity_fragment_context,
    decode_entity_fragment_payload,
    entity_fragment_entries,
    entity_records_from_hits,
    scan_entity_fragment_misses,
)
from tools.cq.query.query_cache import run_query_fragment_scan

__all__ = ["scan_entity_records"]


def scan_entity_records(
    ctx: QueryExecutionContext,
    paths: list[Path],
    scope_globs: list[str] | None,
) -> list[SgRecord]:
    """Scan entity records using fragment-cached scan orchestration.

    Returns:
        list[SgRecord]: Deterministically ordered entity scan records.
    """
    fragment_ctx = build_entity_fragment_context(ctx, paths=paths, scope_globs=scope_globs)
    if not fragment_ctx.cache_ctx.files:
        return []

    entries = entity_fragment_entries(fragment_ctx)
    scan_result = run_query_fragment_scan(
        context=fragment_ctx.cache_ctx,
        entries=entries,
        run_id=ctx.run_id,
        decode=decode_entity_fragment_payload,
        scan_misses=lambda misses: scan_entity_fragment_misses(
            ctx=ctx,
            fragment_ctx=fragment_ctx,
            misses=misses,
        ),
    )

    records_by_rel = entity_records_from_hits(scan_result.hits)
    if scan_result.miss_payload:
        records_by_rel.update(scan_result.miss_payload)

    return assemble_entity_records(
        fragment_ctx.cache_ctx.files,
        fragment_ctx.cache_ctx.root,
        records_by_rel,
    )
