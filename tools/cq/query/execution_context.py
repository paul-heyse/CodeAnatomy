"""Execution context for CQ query execution."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from tools.cq.core.structs import CqStruct

if TYPE_CHECKING:
    from tools.cq.core.bootstrap import CqRuntimeServices
    from tools.cq.core.cache.interface import CqCacheBackend
    from tools.cq.core.toolchain import Toolchain
    from tools.cq.query.enrichment import SymtableEnricher
    from tools.cq.query.ir import Query
    from tools.cq.query.planner import ToolPlan


class QueryExecutionContext(CqStruct, frozen=True):
    """Bundled execution context for query evaluation."""

    query: Query
    plan: ToolPlan
    tc: Toolchain
    root: Path
    argv: list[str]
    started_ms: float
    run_id: str
    services: CqRuntimeServices
    query_text: str | None = None
    cache_backend: CqCacheBackend | None = None
    symtable_enricher: SymtableEnricher | None = None
