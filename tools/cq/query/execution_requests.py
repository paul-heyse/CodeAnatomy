"""Request and context helpers for cq query execution."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tools.cq.astgrep.sgpy_scanner import SgRecord
    from tools.cq.core.schema import CqResult
    from tools.cq.core.toolchain import Toolchain
    from tools.cq.index.files import FileFilterDecision
    from tools.cq.query.enrichment import SymtableEnricher
    from tools.cq.query.executor import EntityExecutionState
    from tools.cq.query.ir import Query
    from tools.cq.query.planner import ToolPlan


@dataclass(frozen=True)
class EntityQueryRequest:
    """Request payload for executing an entity query using pre-scanned records."""

    plan: ToolPlan
    query: Query
    tc: Toolchain
    root: Path
    records: list[SgRecord]
    paths: list[Path]
    scope_globs: list[str] | None
    argv: list[str]
    run_id: str | None = None
    query_text: str | None = None
    match_spans: dict[str, list[tuple[int, int]]] | None = None
    symtable: SymtableEnricher | None = None


@dataclass(frozen=True)
class PatternQueryRequest:
    """Request payload for executing a pattern query with pre-tabulated files."""

    plan: ToolPlan
    query: Query
    tc: Toolchain
    root: Path
    files: list[Path]
    argv: list[str]
    run_id: str | None = None
    query_text: str | None = None
    decisions: list[FileFilterDecision] | None = None


@dataclass(frozen=True)
class DefQueryContext:
    """Context bundle for definition query handlers."""

    state: EntityExecutionState
    result: CqResult
    symtable: SymtableEnricher | None = None


__all__ = [
    "DefQueryContext",
    "EntityQueryRequest",
    "PatternQueryRequest",
]
