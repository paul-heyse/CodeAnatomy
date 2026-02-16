"""Section builders for query results.

Constructs section objects for callers, callees, imports, raises, scope, and bytecode surface.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from tools.cq.astgrep.sgpy_scanner import SgRecord
from tools.cq.core.schema import Anchor, Finding, Section
from tools.cq.core.scoring import build_detail_payload
from tools.cq.query.finding_builders import (
    apply_call_evidence,
    build_def_evidence_map,
    extract_call_target,
    find_enclosing_class,
    record_key,
)
from tools.cq.query.scan import ScanContext
from tools.cq.query.shared_utils import extract_def_name

if TYPE_CHECKING:
    from tools.cq.core.schema import CqResult
    from tools.cq.query.ir import Query
    from tools.cq.utils.interval_index import FileIntervalIndex


@dataclass(frozen=True)
class CallTargetContext:
    """Resolved target names for caller expansion."""

    target_names: set[str]
    function_targets: set[str]
    method_targets: set[str]
    class_methods: dict[str, set[str]]


def append_def_query_sections(
    *,
    result: CqResult,
    query: Query,
    matching_defs: list[SgRecord],
    scan_ctx: ScanContext,
    root: Path,
) -> None:
    """Append sections to definition query result.

    Parameters
    ----------
    result
        Result to populate
    query
        Query with field requests
    matching_defs
        Matching definition records
    scan_ctx
        Scan context
    root
        Repository root
    """
    if "callers" in query.fields:
        callers_section = build_callers_section(
            matching_defs,
            scan_ctx.call_records,
            scan_ctx.file_index,
            root,
        )
        if callers_section.findings:
            result.sections.append(callers_section)
    if "callees" in query.fields:
        callees_section = build_callees_section(matching_defs, scan_ctx.calls_by_def, root)
        if callees_section.findings:
            result.sections.append(callees_section)
    if "imports" in query.fields:
        imports_section = build_imports_section(matching_defs, scan_ctx.all_records)
        if imports_section.findings:
            result.sections.append(imports_section)
    preview_section = build_entity_neighborhood_preview_section(result.key_findings)
    if preview_section.findings:
        result.sections.insert(0, preview_section)


def append_expander_sections(
    result: CqResult,
    target_defs: list[SgRecord],
    ctx: ScanContext,
    root: Path,
    query: Query,
) -> None:
    """Append sections for requested expanders.

    Parameters
    ----------
    result
        Result to populate
    target_defs
        Target definitions
    ctx
        Scan context
    root
        Repository root
    query
        Query with expand requests
    """
    if not query.expand:
        return

    expand_kinds = {expander.kind for expander in query.expand}
    field_kinds = set(query.fields)
    expander_specs: list[tuple[str, bool, Callable[[], Section]]] = [
        (
            "callers",
            True,
            lambda: build_callers_section(
                target_defs,
                ctx.call_records,
                ctx.file_index,
                root,
            ),
        ),
        ("callees", True, lambda: build_callees_section(target_defs, ctx.calls_by_def, root)),
        ("imports", True, lambda: build_imports_section(target_defs, ctx.all_records)),
        (
            "raises",
            False,
            lambda: build_raises_section(
                target_defs,
                ctx.all_records,
                ctx.file_index,
            ),
        ),
        ("scope", False, lambda: build_scope_section(target_defs, root, ctx.calls_by_def)),
        ("bytecode_surface", False, lambda: build_bytecode_surface_section(target_defs, root)),
    ]

    for kind, skip_field, builder in expander_specs:
        if kind not in expand_kinds:
            continue
        if skip_field and kind in field_kinds:
            continue
        section = builder()
        if section.findings:
            result.sections.append(section)


def build_entity_neighborhood_preview_section(
    findings: list[Finding],
) -> Section:
    """Build bounded neighborhood preview for entity query top results.

    Parameters
    ----------
    findings
        Key findings from query

    Returns:
    -------
    Section
        Section containing bounded neighborhood preview findings
    """
    preview_findings: list[Finding] = []
    definition_findings = [finding for finding in findings if finding.category == "definition"][:3]
    for finding in definition_findings:
        name = (
            str(finding.details.get("name"))
            if isinstance(finding.details.get("name"), str)
            else finding.message
        )
        caller_count = finding.details.get("caller_count")
        callee_count = finding.details.get("callee_count")
        enclosing_scope = finding.details.get("enclosing_scope")
        caller_total = caller_count if isinstance(caller_count, int) else 0
        callee_total = callee_count if isinstance(callee_count, int) else 0
        scope_name = enclosing_scope if isinstance(enclosing_scope, str) else "<module>"
        preview_findings.append(
            Finding(
                category="entity_neighborhood",
                message=(
                    f"{name}: callers={caller_total}, callees={callee_total}, scope={scope_name}"
                ),
                anchor=finding.anchor,
                severity="info",
                details=build_detail_payload(
                    data={
                        "name": name,
                        "caller_count": caller_total,
                        "callee_count": callee_total,
                        "enclosing_scope": scope_name,
                    },
                    score=finding.details.score,
                ),
            )
        )
    return Section(title="Neighborhood Preview", findings=preview_findings)


def build_callers_section(
    target_defs: list[SgRecord],
    all_calls: list[SgRecord],
    index: FileIntervalIndex,
    root: Path,
) -> Section:
    """Build section showing callers of target definitions.

    Parameters
    ----------
    target_defs
        Target definition records
    all_calls
        All call records
    index
        File interval index
    root
        Repository root

    Returns:
    -------
    Section
        Callers section for the report
    """
    target_ctx = build_call_target_context(target_defs, index)
    call_contexts = collect_call_contexts(all_calls, index, target_ctx)
    evidence_map = build_def_evidence_map(
        [containing for _, _, containing in call_contexts if containing is not None],
        root,
    )
    findings = build_caller_findings(call_contexts, evidence_map)
    return Section(title="Callers", findings=findings)


def build_call_target_context(
    target_defs: list[SgRecord],
    index: FileIntervalIndex,
) -> CallTargetContext:
    """Build call target context from definitions.

    Parameters
    ----------
    target_defs
        Target definitions
    index
        File interval index

    Returns:
    -------
    CallTargetContext
        Target context
    """
    target_names: set[str] = set()
    method_targets: set[str] = set()
    function_targets: set[str] = set()
    class_methods: dict[str, set[str]] = {}
    for def_record in target_defs:
        def_name = extract_def_name(def_record)
        if not def_name:
            continue
        target_names.add(def_name)
        enclosing_class = find_enclosing_class(def_record, index)
        if enclosing_class is None:
            function_targets.add(def_name)
            continue
        method_targets.add(def_name)
        class_name = extract_def_name(enclosing_class)
        if class_name:
            class_methods.setdefault(class_name, set()).add(def_name)
    return CallTargetContext(
        target_names=target_names,
        function_targets=function_targets,
        method_targets=method_targets,
        class_methods=class_methods,
    )


def collect_call_contexts(
    all_calls: list[SgRecord],
    index: FileIntervalIndex,
    target_ctx: CallTargetContext,
) -> list[tuple[SgRecord, str, SgRecord | None]]:
    """Collect call contexts for target functions.

    Parameters
    ----------
    all_calls
        All call records
    index
        File interval index
    target_ctx
        Target context

    Returns:
    -------
    list[tuple[SgRecord, str, SgRecord | None]]
        Call contexts (call, target, containing)
    """
    from tools.cq.query.finding_builders import extract_call_receiver

    call_contexts: list[tuple[SgRecord, str, SgRecord | None]] = []
    for call in all_calls:
        call_target = extract_call_target(call)
        if call_target not in target_ctx.target_names:
            continue
        receiver = extract_call_receiver(call)
        containing = index.find_containing(call)
        if not call_matches_target(call_target, receiver, containing, index, target_ctx):
            continue
        call_contexts.append((call, call_target, containing))
    return call_contexts


def call_matches_target(
    call_target: str | None,
    receiver: str | None,
    containing: SgRecord | None,
    index: FileIntervalIndex,
    target_ctx: CallTargetContext,
) -> bool:
    """Check if call matches target.

    Parameters
    ----------
    call_target
        Call target name
    receiver
        Call receiver
    containing
        Containing definition
    index
        File interval index
    target_ctx
        Target context

    Returns:
    -------
    bool
        True if call matches target
    """
    if call_target is None:
        return False
    if receiver in {"self", "cls"} and containing is not None:
        caller_class = find_enclosing_class(containing, index)
        if caller_class is not None:
            caller_class_name = extract_def_name(caller_class)
            if caller_class_name:
                methods = target_ctx.class_methods.get(caller_class_name, set())
                if call_target not in methods:
                    return False
    return not (
        receiver is None
        and call_target in target_ctx.method_targets
        and call_target not in target_ctx.function_targets
    )


def build_caller_findings(
    call_contexts: list[tuple[SgRecord, str, SgRecord | None]],
    evidence_map: dict[tuple[str, int, int, int, int], dict[str, object]],
) -> list[Finding]:
    """Build caller findings from call contexts.

    Parameters
    ----------
    call_contexts
        Call contexts
    evidence_map
        Evidence map

    Returns:
    -------
    list[Finding]
        Caller findings
    """
    findings: list[Finding] = []
    for call, call_target, containing in call_contexts:
        caller_name = extract_def_name(containing) if containing else "<module>"
        anchor = Anchor(file=call.file, line=call.start_line, col=call.start_col)
        details: dict[str, object] = {"caller": caller_name, "callee": call_target}
        if containing is not None:
            evidence = evidence_map.get(record_key(containing))
            apply_call_evidence(details, evidence, call_target)
        findings.append(
            Finding(
                category="caller",
                message=f"caller: {caller_name} calls {call_target}",
                anchor=anchor,
                severity="info",
                details=build_detail_payload(data=details),
            )
        )
    return findings


def build_callees_section(
    target_defs: list[SgRecord],
    calls_by_def: dict[SgRecord, list[SgRecord]],
    root: Path,
) -> Section:
    """Build section showing callees for target definitions.

    Parameters
    ----------
    target_defs
        Target definitions
    calls_by_def
        Calls by definition
    root
        Repository root

    Returns:
    -------
    Section
        Callees section for the report
    """
    findings: list[Finding] = []
    evidence_map = build_def_evidence_map(target_defs, root)

    for def_record in target_defs:
        def_name = extract_def_name(def_record) or "<unknown>"
        evidence = evidence_map.get(record_key(def_record))
        for call in calls_by_def.get(def_record, []):
            call_target = extract_call_target(call)
            if not call_target:
                continue
            anchor = Anchor(
                file=call.file,
                line=call.start_line,
                col=call.start_col,
            )
            details: dict[str, object] = {
                "caller": def_name,
                "callee": call_target,
            }
            apply_call_evidence(details, evidence, call_target)
            findings.append(
                Finding(
                    category="callee",
                    message=f"callee: {def_name} calls {call_target}",
                    anchor=anchor,
                    severity="info",
                    details=build_detail_payload(data=details),
                )
            )

    return Section(
        title="Callees",
        findings=findings,
    )


def build_imports_section(
    target_defs: list[SgRecord],
    all_records: list[SgRecord],
) -> Section:
    """Build section showing imports within target files.

    Parameters
    ----------
    target_defs
        Target definitions
    all_records
        All records

    Returns:
    -------
    Section
        Imports section for the report
    """
    from tools.cq.query.executor_definitions import import_to_finding

    target_files = {record.file for record in target_defs}
    findings: list[Finding] = []

    for record in all_records:
        if record.record != "import":
            continue
        if record.file not in target_files:
            continue
        findings.append(import_to_finding(record))

    return Section(
        title="Imports",
        findings=findings,
    )


def build_raises_section(
    target_defs: list[SgRecord],
    all_records: list[SgRecord],
    index: FileIntervalIndex,
) -> Section:
    """Build section showing raises/excepts within target definitions.

    Parameters
    ----------
    target_defs
        Target definitions
    all_records
        All records
    index
        File interval index

    Returns:
    -------
    Section
        Raises section for the report
    """
    findings: list[Finding] = []
    target_def_keys = {record_key(record) for record in target_defs}

    for record in all_records:
        if record.record not in {"raise", "except"}:
            continue
        containing = index.find_containing(record)
        if containing is None or record_key(containing) not in target_def_keys:
            continue
        category = "raise" if record.record == "raise" else "except"
        anchor = Anchor(
            file=record.file,
            line=record.start_line,
            col=record.start_col,
        )
        findings.append(
            Finding(
                category=category,
                message=f"{category}: {record.text.strip()}",
                anchor=anchor,
                severity="info",
                details=build_detail_payload(
                    data={
                        "context_def": extract_def_name(containing) or "<module>",
                    }
                ),
            )
        )

    return Section(
        title="Raises",
        findings=findings,
    )


def build_scope_section(
    target_defs: list[SgRecord],
    root: Path,
    calls_by_def: dict[SgRecord, list[SgRecord]],
) -> Section:
    """Build section showing scope details for target definitions.

    Parameters
    ----------
    target_defs
        Target definitions
    root
        Repository root
    calls_by_def
        Calls by definition

    Returns:
    -------
    Section
        Scope section for the report
    """
    from tools.cq.query.enrichment import SymtableEnricher
    from tools.cq.query.executor_definitions import def_to_finding

    findings: list[Finding] = []
    enricher = SymtableEnricher(root)

    for def_record in target_defs:
        base_finding = def_to_finding(def_record, calls_by_def.get(def_record, []))
        scope_info = enricher.enrich_function_finding(base_finding, def_record)
        if not scope_info:
            continue
        def_name = extract_def_name(def_record) or "<unknown>"
        free_vars_value = scope_info.get("free_vars", [])
        free_vars: list[str] = free_vars_value if isinstance(free_vars_value, list) else []
        cell_vars_value = scope_info.get("cell_vars", [])
        cell_vars: list[str] = cell_vars_value if isinstance(cell_vars_value, list) else []
        label = "closure" if scope_info.get("is_closure") else "toplevel"
        message = (
            f"scope: {def_name} ({label}) free_vars={len(free_vars)} cell_vars={len(cell_vars)}"
        )
        findings.append(
            Finding(
                category="scope",
                message=message,
                anchor=base_finding.anchor,
                severity="info",
                details=build_detail_payload(data=scope_info),
            )
        )

    return Section(
        title="Scope",
        findings=findings,
    )


def build_bytecode_surface_section(
    target_defs: list[SgRecord],
    root: Path,
) -> Section:
    """Build section showing bytecode surface info for target definitions.

    Parameters
    ----------
    target_defs
        Target definitions
    root
        Repository root

    Returns:
    -------
    Section
        Bytecode surface section for the report
    """
    from tools.cq.query.enrichment import BytecodeInfo, enrich_records

    findings: list[Finding] = []
    enrichment = enrich_records(target_defs, root)

    for record in target_defs:
        location = f"{record.file}:{record.start_line}:{record.start_col}"
        info = enrichment.get(location, {})
        bytecode_info = info.get("bytecode_info")
        if not isinstance(bytecode_info, BytecodeInfo):
            continue
        def_name = extract_def_name(record) or "<unknown>"
        globals_list = [str(item) for item in bytecode_info.load_globals]
        attrs_list = [str(item) for item in bytecode_info.load_attrs]
        calls_list = [str(item) for item in bytecode_info.call_functions]
        details: dict[str, object] = {
            "globals": globals_list,
            "attrs": attrs_list,
            "calls": calls_list,
        }
        anchor = Anchor(
            file=record.file,
            line=record.start_line,
            col=record.start_col,
        )
        message = (
            f"bytecode: {def_name} globals={len(globals_list)} "
            f"attrs={len(attrs_list)} calls={len(calls_list)}"
        )
        findings.append(
            Finding(
                category="bytecode_surface",
                message=message,
                anchor=anchor,
                severity="info",
                details=build_detail_payload(data=details),
            )
        )

    return Section(
        title="Bytecode Surface",
        findings=findings,
    )


__all__ = [
    "CallTargetContext",
    "append_def_query_sections",
    "append_expander_sections",
    "build_bytecode_surface_section",
    "build_call_target_context",
    "build_callees_section",
    "build_caller_findings",
    "build_callers_section",
    "build_entity_neighborhood_preview_section",
    "build_imports_section",
    "build_raises_section",
    "build_scope_section",
    "call_matches_target",
    "collect_call_contexts",
]
