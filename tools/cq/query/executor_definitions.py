"""Definition and import query execution.

Handles entity queries for definitions, imports, decorators, and calls.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import TYPE_CHECKING

import msgspec

from tools.cq.astgrep.sgpy_scanner import SgRecord
from tools.cq.core.entity_kinds import ENTITY_KINDS
from tools.cq.core.schema import (
    Anchor,
    CqResult,
    Finding,
    extend_result_key_findings,
    update_result_summary,
)
from tools.cq.core.scoring import (
    ConfidenceSignals,
    ImpactSignals,
    build_detail_payload,
    build_score_details,
)
from tools.cq.core.structs import CqStruct
from tools.cq.query.enrichment import SymtableEnricher, filter_by_scope
from tools.cq.query.scan import ScanContext
from tools.cq.query.shared_utils import extract_def_name

if TYPE_CHECKING:
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
    root: Path,
    *,
    symtable: SymtableEnricher | None = None,
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
    root
        Repository root
    symtable
        Optional symtable enricher

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
        enricher = symtable if symtable is not None else SymtableEnricher(root)
        filtered = filter_by_scope(
            result.key_findings,
            query.scope_filter,
            enricher,
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
    root = state.ctx.root

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
        enricher = ctx.symtable if ctx.symtable is not None else SymtableEnricher(root)
        filtered = filter_by_scope(
            result.key_findings,
            query.scope_filter,
            enricher,
            matching_defs,
        )
        result = msgspec.structs.replace(result, key_findings=tuple(filtered))

    result = append_def_query_sections(
        result=result,
        query=query,
        matching_defs=matching_defs,
        scan_ctx=scan_ctx,
        root=root,
    )
    result = append_expander_sections(result, matching_defs, scan_ctx, root, query)
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


def import_match_key(import_record: SgRecord) -> tuple[str, int, int, int, int, str]:
    """Build a stable dedupe key for import-query findings.

    Parameters
    ----------
    import_record
        Import record

    Returns:
    -------
    tuple[str, int, int, int, int, str]
        Dedupe key
    """
    return (
        import_record.file,
        import_record.start_line,
        import_record.start_col,
        import_record.end_line,
        import_record.end_col,
        extract_import_name(import_record) or import_record.text.strip(),
    )


def dedupe_import_matches(import_records: list[SgRecord]) -> list[SgRecord]:
    """Drop duplicate import records emitted by overlapping ast-grep import rules.

    Parameters
    ----------
    import_records
        Import records to dedupe

    Returns:
    -------
    list[SgRecord]
        Deduped records
    """
    deduped: dict[tuple[str, int, int, int, int, str], SgRecord] = {}
    for import_record in import_records:
        key = import_match_key(import_record)
        if key not in deduped:
            deduped[key] = import_record
    return list(deduped.values())


def extract_import_name(record: SgRecord) -> str | None:
    """Extract the imported name from an import record.

    For single imports, returns the imported name or alias.
    For multi-imports (comma-separated or parenthesized), returns the module name.

    Parameters
    ----------
    record
        Import record

    Returns:
    -------
    str | None
        Imported name or module name when extractable
    """
    text = record.text.strip()
    kind = record.kind

    extractor_by_kind: dict[str, Callable[[str], str | None]] = {
        "import": extract_simple_import,
        "import_as": extract_import_alias,
        "from_import": extract_from_import,
        "from_import_as": extract_from_import_alias,
        "from_import_multi": extract_from_module,
        "from_import_paren": extract_from_module,
        "use_declaration": extract_rust_use_name,
    }
    extractor = extractor_by_kind.get(kind)
    if extractor is None:
        return None
    return extractor(text)


def extract_simple_import(text: str) -> str | None:
    """Extract name from 'import foo' or 'import foo.bar'.

    Parameters
    ----------
    text
        Import statement text

    Returns:
    -------
    str | None
        Imported module name when found
    """
    # Import statements don't contain string literals; stripping inline comments
    # prevents commas/parentheses in comments from affecting extraction.
    text = text.split("#", maxsplit=1)[0].strip()
    match = re.match(r"import\s+([\w.]+)", text)
    return match.group(1) if match else None


def extract_import_alias(text: str) -> str | None:
    """Extract alias from 'import foo as bar'.

    Parameters
    ----------
    text
        Import statement text

    Returns:
    -------
    str | None
        Alias name when found
    """
    text = text.split("#", maxsplit=1)[0].strip()
    match = re.match(r"import\s+[\w.]+\s+as\s+(\w+)", text)
    return match.group(1) if match else None


def extract_from_import(text: str) -> str | None:
    """Extract name from 'from x import y' (single import only).

    Parameters
    ----------
    text
        Import statement text

    Returns:
    -------
    str | None
        Imported name or module name when extractable
    """
    text = text.split("#", maxsplit=1)[0].strip()
    if "," not in text:
        match = re.search(r"import\s+(\w+)\s*$", text)
        if match:
            return match.group(1)
    # Fall back to module name for multi-imports
    return extract_from_module(text)


def extract_from_import_alias(text: str) -> str | None:
    """Extract alias from 'from x import y as z'.

    Parameters
    ----------
    text
        Import statement text

    Returns:
    -------
    str | None
        Alias name when found
    """
    text = text.split("#", maxsplit=1)[0].strip()
    match = re.search(r"as\s+(\w+)\s*$", text)
    return match.group(1) if match else None


def extract_from_module(text: str) -> str | None:
    """Extract module name from 'from x import ...'.

    Parameters
    ----------
    text
        Import statement text

    Returns:
    -------
    str | None
        Module name when found
    """
    text = text.split("#", maxsplit=1)[0].strip()
    match = re.match(r"from\s+([\w.]+)", text)
    return match.group(1) if match else None


def extract_rust_use_name(text: str) -> str | None:
    """Extract import target from Rust use declarations.

    Parameters
    ----------
    text
        Use declaration text

    Returns:
    -------
    str | None
        Imported Rust symbol name or alias when extractable
    """
    text = text.split("//", maxsplit=1)[0].strip()
    match = re.match(r"use\s+([^;]+);?", text)
    if not match:
        return None
    use_target = match.group(1).strip()
    if " as " in use_target:
        return use_target.rsplit(" as ", maxsplit=1)[1].strip()
    return use_target.rsplit("::", maxsplit=1)[-1].strip("{} ").strip()


def def_to_finding(
    def_record: SgRecord,
    calls_within: Sequence[SgRecord],
    *,
    caller_count: int = 0,
    callee_count: int | None = None,
    enclosing_scope: str | None = None,
) -> Finding:
    """Convert a definition record to a Finding.

    Parameters
    ----------
    def_record
        Definition record
    calls_within
        Calls within the definition
    caller_count
        Number of callers
    callee_count
        Number of callees
    enclosing_scope
        Enclosing scope name

    Returns:
    -------
    Finding
        Finding describing the definition record
    """
    def_name = extract_def_name(def_record) or "unknown"

    # Build anchor
    anchor = Anchor(
        file=def_record.file,
        line=def_record.start_line,
        col=def_record.start_col,
        end_line=def_record.end_line,
        end_col=def_record.end_col,
    )

    # Calculate scores
    effective_callee_count = len(calls_within) if callee_count is None else callee_count
    scope_label = enclosing_scope or "<module>"
    impact_signals = ImpactSignals(
        sites=max(caller_count, effective_callee_count),
        files=1,
        depth=1,
    )
    conf_signals = ConfidenceSignals(evidence_kind="resolved_ast")

    score = build_score_details(impact=impact_signals, confidence=conf_signals)
    return Finding(
        category="definition",
        message=f"{def_record.kind}: {def_name}",
        anchor=anchor,
        severity="info",
        details=build_detail_payload(
            data={
                "kind": def_record.kind,
                "name": def_name,
                "calls_within": len(calls_within),
                "caller_count": caller_count,
                "callee_count": effective_callee_count,
                "enclosing_scope": scope_label,
            },
            score=score,
        ),
    )


def import_to_finding(import_record: SgRecord) -> Finding:
    """Convert an import record to a Finding.

    Parameters
    ----------
    import_record
        Import record

    Returns:
    -------
    Finding
        Finding describing the import record
    """
    import_name = extract_import_name(import_record) or "unknown"

    anchor = Anchor(
        file=import_record.file,
        line=import_record.start_line,
        col=import_record.start_col,
        end_line=import_record.end_line,
        end_col=import_record.end_col,
    )

    # Determine category based on import kind
    if import_record.kind in {
        "from_import",
        "from_import_as",
        "from_import_multi",
        "from_import_paren",
    }:
        category = "from_import"
    else:
        category = "import"

    return Finding(
        category=category,
        message=f"{category}: {import_name}",
        anchor=anchor,
        severity="info",
        details=build_detail_payload(
            data={
                "kind": import_record.kind,
                "name": import_name,
                "text": import_record.text.strip(),
            }
        ),
    )


__all__ = [
    "DefQueryRelationshipPolicyV1",
    "append_definition_findings",
    "build_def_relationship_policy",
    "dedupe_import_matches",
    "def_to_finding",
    "definition_relationship_detail",
    "extract_from_import",
    "extract_from_import_alias",
    "extract_from_module",
    "extract_import_alias",
    "extract_import_name",
    "extract_rust_use_name",
    "extract_simple_import",
    "filter_to_matching",
    "finalize_def_query_summary",
    "import_match_key",
    "import_to_finding",
    "matches_name",
    "process_def_query",
    "process_import_query",
]
