"""Definition and import query execution.

Handles entity queries for definitions, imports, decorators, and calls.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

import msgspec

from tools.cq.astgrep.sgpy_scanner import SgRecord
from tools.cq.core.entity_kinds import ENTITY_KINDS
from tools.cq.core.schema import (
    CqResult,
    Finding,
    extend_result_key_findings,
    update_result_summary,
)
from tools.cq.core.structs import CqStruct
from tools.cq.query.enrichment import filter_by_scope
from tools.cq.query.executor_definitions_findings import def_to_finding
from tools.cq.query.executor_definitions_imports import (
    dedupe_import_matches,
    extract_import_name,
    import_match_key,
    import_to_finding,
)
from tools.cq.query.scan import ScanContext
from tools.cq.query.shared_utils import extract_def_name

if TYPE_CHECKING:
    from tools.cq.core.run_context import SymtableEnricherPort
    from tools.cq.query.execution_requests import DefQueryContext
    from tools.cq.query.ir import Query

MAX_RELATIONSHIP_DETAIL_RECORDS = 50


class DefQueryRelationshipPolicyV1(CqStruct, frozen=True):
    """Policy controls for definition-query relationship detail expansion."""

    compute_relationship_details: bool


def process_import_query(
    import_records: list[SgRecord],
    query: Query,
    result: CqResult,
    *,
    symtable: SymtableEnricherPort,
) -> CqResult:
    """Process an import entity query.

    Parameters
    ----------
    import_records
        Import records to process
    query
        Query with filters
    result
        Result to populate
    symtable
        Symtable enricher

    Returns:
    -------
    CqResult
        Updated query result including import findings and summary counters.
    """
    matching_imports = dedupe_import_matches(filter_to_matching(import_records, query))

    import_findings = [import_to_finding(import_record) for import_record in matching_imports]
    result = extend_result_key_findings(result, import_findings)

    # Apply scope filter if present
    if query.scope_filter and matching_imports:
        filtered = filter_by_scope(
            result.key_findings,
            query.scope_filter,
            symtable,
            matching_imports,
        )
        result = msgspec.structs.replace(result, key_findings=tuple(filtered))

    return update_result_summary(
        result,
        {
            "total_imports": len(import_records),
            "matches": len(result.key_findings),
        },
    )


def process_def_query(
    ctx: DefQueryContext,
    query: Query,
    def_candidates: list[SgRecord] | None = None,
) -> CqResult:
    """Process a definition entity query.

    Parameters
    ----------
    ctx
        Definition query context
    query
        Query with filters
    def_candidates
        Optional candidate definitions

    Returns:
    -------
    CqResult
        Updated query result including definition findings, sections, and summary counters.
    """
    from tools.cq.query.section_builders import (
        append_def_query_sections,
        append_expander_sections,
    )

    state = ctx.state
    result = ctx.result
    scan_ctx = state.scan

    candidate_records = (
        list(def_candidates) if def_candidates is not None else list(scan_ctx.def_records)
    )
    matching_defs = filter_to_matching(candidate_records, query)
    relationship_policy = build_def_relationship_policy(query, matching_defs)
    result = append_definition_findings(
        result,
        matching_defs=matching_defs,
        scan_ctx=scan_ctx,
        policy=relationship_policy,
    )

    # Apply scope filter if present
    if query.scope_filter:
        filtered = filter_by_scope(
            result.key_findings,
            query.scope_filter,
            ctx.symtable,
            matching_defs,
        )
        result = msgspec.structs.replace(result, key_findings=tuple(filtered))

    result = append_def_query_sections(
        result=result,
        query=query,
        matching_defs=matching_defs,
        scan_ctx=scan_ctx,
        root=state.ctx.root,
        symtable=ctx.symtable,
    )
    result = append_expander_sections(
        result,
        matching_defs,
        scan_ctx,
        state.ctx.root,
        query,
        symtable=ctx.symtable,
    )
    return finalize_def_query_summary(result, scan_ctx)


def build_def_relationship_policy(
    query: Query,
    matching_records: list[SgRecord],
) -> DefQueryRelationshipPolicyV1:
    """Build relationship policy for definition query.

    Parameters
    ----------
    query
        Query with constraints
    matching_records
        Matching definition records

    Returns:
    -------
    DefQueryRelationshipPolicyV1
        Relationship policy
    """
    has_scope_constraints = bool(query.scope.in_dir or query.scope.exclude or query.scope.globs)
    compute_relationship_details = (
        len(matching_records) <= MAX_RELATIONSHIP_DETAIL_RECORDS
        or query.name is not None
        or has_scope_constraints
        or "callers" in query.fields
        or "callees" in query.fields
    )
    return DefQueryRelationshipPolicyV1(compute_relationship_details=compute_relationship_details)


def append_definition_findings(
    result: CqResult,
    *,
    matching_defs: list[SgRecord],
    scan_ctx: ScanContext,
    policy: DefQueryRelationshipPolicyV1,
) -> CqResult:
    """Append definition findings to result.

    Parameters
    ----------
    result
        Result to populate
    matching_defs
        Matching definition records
    scan_ctx
        Scan context with indexed records
    policy
        Relationship policy

    Returns:
    -------
    CqResult
        Updated result with definition findings appended.
    """
    findings: list[Finding] = []
    for def_record in matching_defs:
        calls_within, caller_count, enclosing_scope = definition_relationship_detail(
            def_record,
            scan_ctx=scan_ctx,
            policy=policy,
        )
        findings.append(
            def_to_finding(
                def_record,
                calls_within,
                caller_count=caller_count,
                callee_count=len(calls_within),
                enclosing_scope=enclosing_scope,
            )
        )
    return extend_result_key_findings(result, findings)


def definition_relationship_detail(
    def_record: SgRecord,
    *,
    scan_ctx: ScanContext,
    policy: DefQueryRelationshipPolicyV1,
) -> tuple[list[SgRecord], int, str]:
    """Compute relationship details for a definition.

    Parameters
    ----------
    def_record
        Definition record
    scan_ctx
        Scan context with indexed records
    policy
        Relationship policy

    Returns:
    -------
    tuple[list[SgRecord], int, str]
        Calls within, caller count, enclosing scope
    """
    from tools.cq.query.finding_builders import (
        count_callers_for_definition,
        resolve_enclosing_scope,
    )

    if not policy.compute_relationship_details:
        return [], 0, "<module>"
    calls_within = list(scan_ctx.calls_by_def.get(def_record, ()))
    caller_count = count_callers_for_definition(
        def_record,
        scan_ctx.call_records,
        scan_ctx.file_index,
    )
    enclosing_scope = resolve_enclosing_scope(def_record, scan_ctx.file_index)
    return calls_within, caller_count, enclosing_scope


def finalize_def_query_summary(result: CqResult, scan_ctx: ScanContext) -> CqResult:
    """Finalize definition query summary.

    Parameters
    ----------
    result
        Result to finalize
    scan_ctx
        Scan context

    Returns:
    -------
    CqResult
        Result with definition summary totals applied.
    """
    return update_result_summary(
        result,
        {
            "total_defs": len(scan_ctx.def_records),
            "total_calls": len(scan_ctx.call_records),
            "matches": len(result.key_findings),
        },
    )


def filter_to_matching(
    def_records: list[SgRecord],
    query: Query,
) -> list[SgRecord]:
    """Filter definitions to those matching the query.

    Parameters
    ----------
    def_records
        Candidate records
    query
        Query with filters

    Returns:
    -------
    list[SgRecord]
        Records that match the query filters
    """
    matching: list[SgRecord] = []

    for record in def_records:
        # Filter by entity type
        if not ENTITY_KINDS.matches(
            entity_type=query.entity,
            record_kind=record.kind,
            record_type=record.record,
        ):
            continue

        # Filter by name pattern
        if query.name and not matches_name(record, query.name):
            continue

        matching.append(record)

    return matching


def matches_name(record: SgRecord, name: str) -> bool:
    """Check if record matches name pattern.

    Parameters
    ----------
    record
        Record to check
    name
        Name pattern (supports ~regex)

    Returns:
    -------
    bool
        True if the record matches the name pattern
    """
    from tools.cq.query.finding_builders import extract_call_name

    # Extract name based on record type
    if record.record == "import":
        extracted_name = extract_import_name(record)
    elif record.record == "call":
        extracted_name = extract_call_name(record)
    else:
        extracted_name = extract_def_name(record)

    if not extracted_name:
        return False

    # Regex match if pattern starts with ~
    if name.startswith("~"):
        pattern = name[1:]
        return bool(re.search(pattern, extracted_name))

    # Exact match otherwise
    return extracted_name == name


__all__ = [
    "DefQueryRelationshipPolicyV1",
    "append_definition_findings",
    "build_def_relationship_policy",
    "dedupe_import_matches",
    "def_to_finding",
    "definition_relationship_detail",
    "extract_import_name",
    "filter_to_matching",
    "finalize_def_query_summary",
    "import_match_key",
    "import_to_finding",
    "matches_name",
    "process_def_query",
    "process_import_query",
]
