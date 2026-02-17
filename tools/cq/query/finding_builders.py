"""Finding construction utilities for query execution.

Builds findings from ast-grep records with evidence enrichment.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from pathlib import Path

from tools.cq.astgrep.sgpy_scanner import SgRecord
from tools.cq.core.entity_kinds import ENTITY_KINDS
from tools.cq.core.schema import Anchor, Finding
from tools.cq.core.scoring import build_detail_payload
from tools.cq.query.shared_utils import extract_def_name
from tools.cq.utils.interval_index import FileIntervalIndex


def count_callers_for_definition(
    def_record: SgRecord,
    all_calls: Sequence[SgRecord],
    index: FileIntervalIndex,
) -> int:
    """Count callsites that target the given definition.

    Parameters
    ----------
    def_record
        Definition record
    all_calls
        All call records
    index
        File interval index

    Returns:
    -------
    int
        Number of matching callsites
    """
    target_name = extract_def_name(def_record)
    if not target_name:
        return 0
    count = 0
    for call in all_calls:
        if extract_call_target(call) != target_name:
            continue
        receiver = extract_call_receiver(call)
        containing = index.find_containing(call)
        if receiver in {"self", "cls"}:
            target_class = find_enclosing_class(def_record, index)
            caller_class = (
                find_enclosing_class(containing, index) if containing is not None else None
            )
            target_class_name = extract_def_name(target_class) if target_class is not None else None
            caller_class_name = extract_def_name(caller_class) if caller_class is not None else None
            if target_class_name and caller_class_name and target_class_name != caller_class_name:
                continue
        count += 1
    return count


def resolve_enclosing_scope(def_record: SgRecord, index: FileIntervalIndex) -> str:
    """Resolve human-readable enclosing scope for a definition.

    Parameters
    ----------
    def_record
        Definition record
    index
        File interval index

    Returns:
    -------
    str
        Enclosing scope name, or `<module>` when no parent scope exists
    """
    file_index = index.by_file.get(def_record.file)
    if file_index is None:
        return "<module>"
    parents: list[SgRecord] = []
    for start, end, candidate in file_index.intervals:
        if record_key(candidate) == record_key(def_record):
            continue
        if start <= def_record.start_line <= end:
            parents.append(candidate)
    if not parents:
        return "<module>"
    parent = min(parents, key=lambda candidate: candidate.end_line - candidate.start_line)
    name = extract_def_name(parent)
    return name or "<module>"


def record_key(record: SgRecord) -> tuple[str, int, int, int, int]:
    """Return a stable key for a record.

    Parameters
    ----------
    record
        Record to key

    Returns:
    -------
    tuple[str, int, int, int, int]
        Stable key identifying a record location
    """
    return (
        record.file,
        record.start_line,
        record.start_col,
        record.end_line,
        record.end_col,
    )


def build_def_evidence_map(
    def_records: list[SgRecord],
    root: Path,
) -> dict[tuple[str, int, int, int, int], dict[str, object]]:
    """Build a map of definition records to symtable/bytecode evidence.

    Parameters
    ----------
    def_records
        Definition records
    root
        Repository root

    Returns:
    -------
    dict[tuple[str, int, int, int, int], dict[str, object]]
        Evidence details keyed by record location
    """
    from tools.cq.query.enrichment import BytecodeInfo, SymtableInfo, enrich_records

    unique_records: dict[tuple[str, int, int, int, int], SgRecord] = {}
    for record in def_records:
        unique_records[record_key(record)] = record

    if not unique_records:
        return {}

    enrichment = enrich_records(list(unique_records.values()), root)
    evidence_map: dict[tuple[str, int, int, int, int], dict[str, object]] = {}

    for rec_key, record in unique_records.items():
        location = f"{record.file}:{record.start_line}:{record.start_col}"
        info = enrichment.get(location)
        if not info:
            continue
        details: dict[str, object] = {}
        symtable_info = info.get("symtable_info")
        if isinstance(symtable_info, SymtableInfo):
            details["resolved_globals"] = list(symtable_info.globals_used)
        bytecode_info = info.get("bytecode_info")
        if isinstance(bytecode_info, BytecodeInfo):
            details["bytecode_calls"] = list(bytecode_info.call_functions)
        if details:
            evidence_map[rec_key] = details

    return evidence_map


def apply_call_evidence(
    details: dict[str, object],
    evidence: dict[str, object] | None,
    call_target: str,
) -> None:
    """Attach call evidence details to the finding payload.

    Parameters
    ----------
    details
        Finding details dictionary (mutated)
    evidence
        Evidence map entry
    call_target
        Call target name
    """
    if not evidence:
        return
    resolved_globals = evidence.get("resolved_globals")
    if isinstance(resolved_globals, list):
        details["resolved_globals"] = resolved_globals
        if call_target:
            details["globals_has_target"] = call_target in resolved_globals

    bytecode_calls = evidence.get("bytecode_calls")
    if isinstance(bytecode_calls, list):
        details["bytecode_calls"] = bytecode_calls
        if call_target:
            details["bytecode_has_target"] = call_target in bytecode_calls


def call_to_finding(
    record: SgRecord,
    *,
    extra_details: dict[str, object] | None = None,
) -> Finding:
    """Convert a call record to a Finding.

    Parameters
    ----------
    record
        Call record
    extra_details
        Optional extra details

    Returns:
    -------
    Finding
        Finding describing the callsite
    """
    call_target = extract_call_target(record) or "<unknown>"
    anchor = Anchor(
        file=record.file,
        line=record.start_line,
        col=record.start_col,
        end_line=record.end_line,
        end_col=record.end_col,
    )
    details: dict[str, object] = {"text": record.text.strip()}
    if extra_details:
        details.update(extra_details)
    return Finding(
        category="callsite",
        message=f"call: {call_target}",
        anchor=anchor,
        severity="info",
        details=build_detail_payload(data=details),
    )


def extract_call_target(call: SgRecord) -> str:
    """Extract the target name from a call record.

    Parameters
    ----------
    call
        Call record

    Returns:
    -------
    str
        Extracted target name
    """
    text = call.text.lstrip()

    # For attribute calls (obj.method()), extract the method name
    if call.kind in {"attr_call", "attr"}:
        match = re.search(r"\.(\w+)\s*\(", text)
        if match:
            return match.group(1)

    # For name calls (func()), extract the function name
    match = re.search(r"\b(\w+)\s*\(", text)
    if match:
        return match.group(1)

    return ""


def extract_call_receiver(call: SgRecord) -> str | None:
    """Extract the receiver name for attribute calls.

    Parameters
    ----------
    call
        Call record

    Returns:
    -------
    str | None
        Receiver name when present
    """
    if call.kind not in {"attr_call", "attr"}:
        return None
    match = re.search(r"(\w+)\s*\.", call.text.lstrip())
    if match:
        return match.group(1)
    return None


def find_enclosing_class(
    record: SgRecord,
    index: FileIntervalIndex,
) -> SgRecord | None:
    """Find the innermost class containing a record.

    Parameters
    ----------
    record
        Record to search
    index
        File interval index

    Returns:
    -------
    SgRecord | None
        Enclosing class record when found
    """
    file_index = index.by_file.get(record.file)
    if file_index is None:
        return None
    candidates: list[SgRecord] = []
    for start, end, candidate in file_index.intervals:
        if candidate.kind not in ENTITY_KINDS.class_kinds:
            continue
        if record_key(candidate) == record_key(record):
            continue
        if start <= record.start_line <= end:
            candidates.append(candidate)
    if not candidates:
        return None
    return min(candidates, key=lambda candidate: candidate.end_line - candidate.start_line)


def extract_call_name(call: SgRecord) -> str | None:
    """Extract the name for callsite matching.

    Parameters
    ----------
    call
        Call record

    Returns:
    -------
    str | None
        Extracted call name when available
    """
    target = extract_call_target(call)
    return target or None


__all__ = [
    "apply_call_evidence",
    "build_def_evidence_map",
    "call_to_finding",
    "count_callers_for_definition",
    "extract_call_name",
    "extract_call_receiver",
    "extract_call_target",
    "find_enclosing_class",
    "record_key",
    "resolve_enclosing_scope",
]
