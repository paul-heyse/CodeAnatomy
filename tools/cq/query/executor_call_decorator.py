"""Decorator/call query processing helpers."""

from __future__ import annotations

from pathlib import Path

from tools.cq.astgrep.sgpy_scanner import SgRecord
from tools.cq.core.schema import Finding
from tools.cq.core.summary_update_contracts import EntitySummaryUpdateV1
from tools.cq.query.executor_runtime import (
    process_call_query as process_call_query_impl,
)
from tools.cq.query.executor_runtime import (
    process_decorator_query as process_decorator_query_impl,
)
from tools.cq.query.ir import Query
from tools.cq.query.scan import ScanContext

__all__ = ["process_call_query", "process_decorator_query"]


def process_decorator_query(
    ctx: ScanContext,
    query: Query,
    root: Path,
    def_candidates: list[SgRecord] | tuple[SgRecord, ...] | None = None,
) -> tuple[list[Finding], EntitySummaryUpdateV1]:
    """Process a decorator entity query.

    Returns:
        tuple[list[Finding], EntitySummaryUpdateV1]: Decorator findings and summary metrics.
    """
    return process_decorator_query_impl(ctx, query, root, def_candidates)


def process_call_query(
    ctx: ScanContext,
    query: Query,
    root: Path,
) -> tuple[list[Finding], EntitySummaryUpdateV1]:
    """Process a callsite entity query.

    Returns:
        tuple[list[Finding], EntitySummaryUpdateV1]: Call findings and summary metrics.
    """
    return process_call_query_impl(ctx, query, root)
