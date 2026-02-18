"""Entity-query execution helpers extracted from executor_runtime."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

import msgspec

from tools.cq.astgrep.sgpy_scanner import SgRecord
from tools.cq.core.entity_kinds import ENTITY_KINDS
from tools.cq.core.run_context import SymtableEnricherPort
from tools.cq.core.schema import Anchor, CqResult, Finding, Section, mk_result
from tools.cq.core.summary_update_contracts import EntitySummaryUpdateV1
from tools.cq.query.execution_requests import DefQueryContext
from tools.cq.query.executor_definitions import (
    def_to_finding as _def_to_finding,
)
from tools.cq.query.executor_definitions import (
    filter_to_matching as _filter_to_matching,
)
from tools.cq.query.executor_definitions import (
    matches_name as _matches_name,
)
from tools.cq.query.executor_definitions import (
    process_def_query as _process_def_query,
)
from tools.cq.query.executor_definitions import (
    process_import_query as _process_import_query,
)
from tools.cq.query.executor_runtime_summary import entity_summary_updates
from tools.cq.query.finding_builders import (
    build_call_evidence as _build_call_evidence,
)
from tools.cq.query.finding_builders import (
    build_def_evidence_map as _build_def_evidence_map,
)
from tools.cq.query.finding_builders import (
    call_to_finding as _call_to_finding,
)
from tools.cq.query.finding_builders import (
    extract_call_target as _extract_call_target,
)
from tools.cq.query.finding_builders import (
    record_key as _record_key,
)
from tools.cq.query.query_summary import (
    build_runmeta as _build_runmeta,
)
from tools.cq.query.scan import ScanContext
from tools.cq.query.shared_utils import extract_def_name

if TYPE_CHECKING:
    from tools.cq.query.execution_context import QueryExecutionContext
    from tools.cq.query.execution_requests import EntityQueryRequest
    from tools.cq.query.executor_runtime import EntityExecutionState
    from tools.cq.query.ir import Query

logger = logging.getLogger(__name__)


def apply_entity_handlers(
    state: EntityExecutionState,
    *,
    symtable: SymtableEnricherPort,
) -> tuple[list[Finding], list[Section], EntitySummaryUpdateV1]:
    """Apply entity handlers for one prepared query execution state.

    Returns:
        Findings, sections, and typed summary counters for one entity query.
    """
    query = state.ctx.query
    root = state.ctx.root
    candidates = state.candidates

    if query.entity == "import":
        temp_result = mk_result(_build_runmeta(state.ctx))
        temp_result = _process_import_query(
            list(candidates.import_records),
            query,
            temp_result,
            symtable=symtable,
        )
        return (
            list(temp_result.key_findings),
            list(temp_result.sections),
            entity_summary_updates(temp_result),
        )
    if query.entity == "decorator":
        findings, summary_updates = process_decorator_query(
            state.scan,
            query,
            root,
            list(candidates.def_records),
        )
        return findings, [], summary_updates
    if query.entity == "callsite":
        findings, summary_updates = process_call_query(state.scan, query, root)
        return findings, [], summary_updates

    temp_result = mk_result(_build_runmeta(state.ctx))
    def_ctx = DefQueryContext(state=state, result=temp_result, symtable=symtable)
    temp_result = _process_def_query(def_ctx, query, list(candidates.def_records))
    return (
        list(temp_result.key_findings),
        list(temp_result.sections),
        entity_summary_updates(temp_result),
    )


def process_decorator_query(
    ctx: ScanContext,
    query: Query,
    root: Path,
    def_candidates: list[SgRecord] | tuple[SgRecord, ...] | None = None,
) -> tuple[list[Finding], EntitySummaryUpdateV1]:
    """Process a decorator entity query.

    Returns:
        Decorator findings and typed summary counters.
    """
    from tools.cq.query.enrichment import enrich_with_decorators

    findings: list[Finding] = []

    candidate_records = def_candidates if def_candidates is not None else ctx.def_records
    for def_record in candidate_records:
        if def_record.kind not in ENTITY_KINDS.decorator_kinds:
            continue

        if query.name and not _matches_name(def_record, query.name):
            continue

        file_path = root / def_record.file
        try:
            source = file_path.read_text(encoding="utf-8")
        except OSError:
            logger.warning("Skipping unreadable file during decorator query: %s", file_path)
            continue

        decorator_info = enrich_with_decorators(
            Finding(
                category="definition",
                message="",
                anchor=Anchor(file=def_record.file, line=def_record.start_line),
            ),
            source,
        )

        decorators_value = decorator_info.get("decorators", [])
        decorators: list[str] = (
            [str(item) for item in decorators_value] if isinstance(decorators_value, list) else []
        )
        count = len(decorators)

        if query.decorator_filter:
            if (
                query.decorator_filter.decorated_by
                and query.decorator_filter.decorated_by not in decorators
            ):
                continue

            if (
                query.decorator_filter.decorator_count_min is not None
                and count < query.decorator_filter.decorator_count_min
            ):
                continue
            if (
                query.decorator_filter.decorator_count_max is not None
                and count > query.decorator_filter.decorator_count_max
            ):
                continue

        if count > 0:
            finding = _def_to_finding(def_record, list(ctx.calls_by_def.get(def_record, ())))
            details = finding.details.with_entry("decorators", decorators)
            details = details.with_entry("decorator_count", count)
            finding = msgspec.structs.replace(finding, details=details)
            findings.append(finding)

    return findings, EntitySummaryUpdateV1(
        matches=len(findings),
        total_defs=len(ctx.def_records),
        total_calls=0,
        total_imports=0,
    )


def process_call_query(
    ctx: ScanContext,
    query: Query,
    root: Path,
) -> tuple[list[Finding], EntitySummaryUpdateV1]:
    """Process a callsite entity query.

    Returns:
        Callsite findings and typed summary counters.
    """
    matching_calls = _filter_to_matching(list(ctx.call_records), query)
    call_contexts: list[tuple[SgRecord, SgRecord | None]] = []
    for call_record in matching_calls:
        containing = ctx.file_index.find_containing(call_record)
        call_contexts.append((call_record, containing))

    containing_defs = [containing for _, containing in call_contexts if containing is not None]
    evidence_map = _build_def_evidence_map(containing_defs, root)

    findings: list[Finding] = []
    for call_record, containing in call_contexts:
        details: dict[str, object] = {}
        call_target = _extract_call_target(call_record)
        if containing is not None:
            caller_name = extract_def_name(containing) or "<module>"
            details["caller"] = caller_name
            evidence = evidence_map.get(_record_key(containing))
            details.update(_build_call_evidence(evidence, call_target))
        finding = _call_to_finding(call_record, extra_details=details)
        findings.append(finding)

    return findings, EntitySummaryUpdateV1(
        matches=len(findings),
        total_defs=0,
        total_calls=len(ctx.call_records),
        total_imports=0,
    )


def execute_entity_query(ctx: QueryExecutionContext) -> CqResult:
    """Execute entity query for a prepared execution context.

    Returns:
        Entity query result payload.
    """
    from tools.cq.query.executor_runtime import execute_entity_query as execute_entity_query_impl

    return execute_entity_query_impl(ctx)


def execute_entity_query_from_records(request: EntityQueryRequest) -> CqResult:
    """Execute entity query over pre-scanned records.

    Returns:
        Entity query result payload.
    """
    from tools.cq.query.executor_runtime import (
        execute_entity_query_from_records as execute_entity_query_from_records_impl,
    )

    return execute_entity_query_from_records_impl(request)


__all__ = [
    "apply_entity_handlers",
    "execute_entity_query",
    "execute_entity_query_from_records",
    "process_call_query",
    "process_decorator_query",
]
