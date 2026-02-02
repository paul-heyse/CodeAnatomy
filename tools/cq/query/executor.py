"""Query executor for cq queries.

Executes ToolPlans and returns CqResult objects.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from ast_grep_py import SgRoot

from tools.cq.astgrep.sgpy_scanner import SgRecord, group_records_by_file
from tools.cq.core.schema import Anchor, CqResult, Finding, Section, mk_result, mk_runmeta, ms
from tools.cq.core.scoring import (
    ConfidenceSignals,
    ImpactSignals,
    bucket,
    confidence_score,
    impact_score,
)
from tools.cq.query.enrichment import SymtableEnricher, filter_by_scope
from tools.cq.query.planner import AstGrepRule, ToolPlan, scope_to_globs, scope_to_paths
from tools.cq.query.sg_parser import filter_records_by_kind, sg_scan
from tools.cq.search import SearchLimits, find_files_with_pattern

if TYPE_CHECKING:
    from ast_grep_py import SgNode

    from tools.cq.core.toolchain import Toolchain
from tools.cq.index.files import build_repo_file_index, tabulate_files
from tools.cq.index.query_cache import QueryCache
from tools.cq.index.repo import resolve_repo_context
from tools.cq.index.sqlite_cache import IndexCache
from tools.cq.query.ir import Query, Scope

QUERY_CACHE_VERSION = "9"


@dataclass
class ScanContext:
    """Bundled context from ast-grep scan for query processing."""

    def_records: list[SgRecord]
    call_records: list[SgRecord]
    interval_index: IntervalIndex
    file_index: FileIntervalIndex
    calls_by_def: dict[SgRecord, list[SgRecord]]
    all_records: list[SgRecord]


@dataclass
class IntervalIndex:
    """Index for efficient interval containment queries.

    Enables O(log n) lookup of which definition contains a given position.
    """

    # Sorted list of (start_line, end_line, record) tuples
    intervals: list[tuple[int, int, SgRecord]]

    @classmethod
    def from_records(cls, records: list[SgRecord]) -> IntervalIndex:
        """Build interval index from definition records."""
        intervals = [(r.start_line, r.end_line, r) for r in records]
        # Sort by start line, then by end line (larger spans first for nesting)
        intervals.sort(key=lambda x: (x[0], -x[1]))
        return cls(intervals=intervals)

    def find_containing(self, line: int) -> SgRecord | None:
        """Find the innermost definition containing the given line.

        Returns None if no definition contains the line.
        """
        # Binary search for candidates
        candidates: list[SgRecord] = []
        for start, end, record in self.intervals:
            if start <= line <= end:
                candidates.append(record)
            elif start > line:
                break

        if not candidates:
            return None

        # Return innermost (smallest span)
        return min(candidates, key=lambda r: r.end_line - r.start_line)


@dataclass(frozen=True)
class FileIntervalIndex:
    """Per-file interval indexes to avoid cross-file attribution."""

    by_file: dict[str, IntervalIndex]

    @classmethod
    def from_records(cls, records: list[SgRecord]) -> FileIntervalIndex:
        """Build per-file interval indexes."""
        grouped = group_records_by_file(records)
        return cls(
            by_file={
                file_path: IntervalIndex.from_records(recs) for file_path, recs in grouped.items()
            }
        )

    def find_containing(self, record: SgRecord) -> SgRecord | None:
        """Find the innermost definition containing the record."""
        index = self.by_file.get(record.file)
        if index is None:
            return None
        return index.find_containing(record.start_line)


def execute_plan(
    plan: ToolPlan,
    query: Query,
    tc: Toolchain,
    root: Path,
    argv: list[str] | None = None,
    index_cache: IndexCache | None = None,
    query_cache: QueryCache | None = None,
    use_cache: bool = True,
) -> CqResult:
    """Execute a ToolPlan and return results.

    Parameters
    ----------
    plan
        Compiled tool plan
    query
        Original query (for metadata)
    tc
        Toolchain with tool availability info
    root
        Repository root
    argv
        Original command line arguments

    Returns
    -------
    CqResult
        Query results
    """
    started_ms = ms()
    cache_key: str | None = None
    cache_files: list[Path] | None = None

    if use_cache and query_cache is not None:
        cache_files = _collect_cache_files(plan, root)
        if cache_files:
            cache_key = _build_query_cache_key(query, plan, root, tc)
            cached = query_cache.get(cache_key, cache_files)
            if cached is not None:
                result = CqResult.from_dict(cached)
                result.summary["cache"] = {"status": "hit", "key": cache_key}
                return result

    # Dispatch to pattern query executor if this is a pattern query
    if plan.is_pattern_query:
        result = _execute_pattern_query(
            plan,
            query,
            tc,
            root,
            argv,
            started_ms,
            index_cache,
        )
    else:
        result = _execute_entity_query(
            plan,
            query,
            tc,
            root,
            argv,
            started_ms,
            index_cache,
        )

    if use_cache and query_cache is not None and cache_key and cache_files:
        query_cache.set(cache_key, result.to_dict(), cache_files)
        result.summary["cache"] = {"status": "miss", "key": cache_key}

    return result


def _execute_entity_query(
    plan: ToolPlan,
    query: Query,
    tc: Toolchain,
    root: Path,
    argv: list[str] | None,
    started_ms: float,
    index_cache: IndexCache | None,
) -> CqResult:
    """Execute an entity-based query."""
    # Get paths to scan
    paths = scope_to_paths(plan.scope, root)
    if not paths:
        run = mk_runmeta(
            macro="q",
            argv=argv or [],
            root=str(root),
            started_ms=started_ms,
            toolchain=tc.to_dict(),
        )
        result = mk_result(run)
        result.summary["error"] = "No files match scope"
        return result

    scope_globs = scope_to_globs(plan.scope)

    # ast-grep scan
    if not tc.has_sgpy:
        run = mk_runmeta(
            macro="q",
            argv=argv or [],
            root=str(root),
            started_ms=started_ms,
            toolchain=tc.to_dict(),
        )
        result = mk_result(run)
        result.summary["error"] = "ast-grep not available"
        return result

    records = sg_scan(
        paths=paths,
        record_types=set(plan.sg_record_types),
        root=root,
        globs=scope_globs,
        index_cache=index_cache,
    )

    # Phase 3: Build scan context
    def_records = filter_records_by_kind(records, "def")
    interval_index = IntervalIndex.from_records(def_records)
    file_index = FileIntervalIndex.from_records(def_records)
    call_records = filter_records_by_kind(records, "call")
    calls_by_def = assign_calls_to_defs(interval_index, call_records)

    ctx = ScanContext(
        def_records=def_records,
        call_records=call_records,
        interval_index=interval_index,
        file_index=file_index,
        calls_by_def=calls_by_def,
        all_records=records,
    )

    # Phase 4: Build result
    run = mk_runmeta(
        macro="q",
        argv=argv or [],
        root=str(root),
        started_ms=started_ms,
        toolchain=tc.to_dict(),
    )
    result = mk_result(run)

    # Phase 5: Filter and add findings based on entity type
    def_candidates = def_records
    import_candidates = filter_records_by_kind(records, "import")
    call_candidates = call_records

    if plan.sg_rules:
        match_spans = _collect_match_spans(plan.sg_rules, paths, root, query, scope_globs)
        if match_spans:
            if query.entity in {"function", "class", "method", "decorator"}:
                def_candidates = _filter_records_by_spans(def_candidates, match_spans)
            elif query.entity == "import":
                import_candidates = _filter_records_by_spans(import_candidates, match_spans)
            elif query.entity == "callsite":
                call_candidates = _filter_records_by_spans(call_candidates, match_spans)

    if query.entity == "import":
        _process_import_query(import_candidates, query, result, root)
    elif query.entity == "decorator":
        _process_decorator_query(ctx, query, result, root, def_candidates)
    elif query.entity == "callsite":
        _process_call_query(ctx, query, result, root)
    else:
        _process_def_query(ctx, query, result, root, def_candidates)

    result.summary["files_scanned"] = len({r.file for r in records})

    if plan.explain:
        result.summary["plan"] = {
            "sg_record_types": list(plan.sg_record_types),
            "need_symtable": plan.need_symtable,
            "need_bytecode": plan.need_bytecode,
            "is_pattern_query": plan.is_pattern_query,
        }
        repo_context = resolve_repo_context(root)
        repo_index = build_repo_file_index(repo_context)
        file_result = tabulate_files(
            repo_index,
            paths,
            scope_globs,
            extensions=(".py",),
            explain=True,
        )
        result.summary["file_filters"] = [asdict(decision) for decision in file_result.decisions]

    return result


def _execute_pattern_query(
    plan: ToolPlan,
    query: Query,
    tc: Toolchain,
    root: Path,
    argv: list[str] | None,
    started_ms: float,
    _index_cache: IndexCache | None,
) -> CqResult:
    """Execute a pattern-based query using inline ast-grep rules."""
    scope_globs = scope_to_globs(plan.scope)
    if not tc.has_sgpy:
        run = mk_runmeta(
            macro="q",
            argv=argv or [],
            root=str(root),
            started_ms=started_ms,
            toolchain=tc.to_dict(),
        )
        result = mk_result(run)
        result.summary["error"] = "ast-grep not available"
        return result

    # Get paths to scan
    paths = scope_to_paths(plan.scope, root)
    if not paths:
        run = mk_runmeta(
            macro="q",
            argv=argv or [],
            root=str(root),
            started_ms=started_ms,
            toolchain=tc.to_dict(),
        )
        result = mk_result(run)
        result.summary["error"] = "No files match scope"
        return result
    repo_context = resolve_repo_context(root)
    repo_index = build_repo_file_index(repo_context)
    file_result = tabulate_files(
        repo_index,
        paths,
        scope_globs,
        extensions=(".py",),
        explain=plan.explain,
    )
    paths = file_result.files
    if not paths:
        run = mk_runmeta(
            macro="q",
            argv=argv or [],
            root=str(root),
            started_ms=started_ms,
            toolchain=tc.to_dict(),
        )
        result = mk_result(run)
        result.summary["error"] = "No files match scope after filtering"
        if plan.explain:
            result.summary["file_filters"] = [
                asdict(decision) for decision in file_result.decisions
            ]
        return result

    # Execute ast-grep rules
    findings, records, raw_matches = _execute_ast_grep_rules(
        plan.sg_rules,
        paths,
        root,
        query,
        None,
    )

    # Build result
    run = mk_runmeta(
        macro="q",
        argv=argv or [],
        root=str(root),
        started_ms=started_ms,
        toolchain=tc.to_dict(),
    )
    result = mk_result(run)
    result.key_findings.extend(findings)

    # Apply scope filter if present
    if query.scope_filter and findings:
        enricher = SymtableEnricher(root)
        result.key_findings = filter_by_scope(
            result.key_findings,
            query.scope_filter,
            enricher,
            records,
        )

    # Apply limit
    if query.limit and len(result.key_findings) > query.limit:
        result.key_findings = result.key_findings[: query.limit]

    result.summary["matches"] = len(result.key_findings)
    result.summary["files_scanned"] = len({r.file for r in records})

    if plan.explain:
        result.summary["plan"] = {
            "is_pattern_query": True,
            "pattern": query.pattern_spec.pattern if query.pattern_spec else None,
            "strictness": query.pattern_spec.strictness if query.pattern_spec else None,
            "context": query.pattern_spec.context if query.pattern_spec else None,
            "selector": query.pattern_spec.selector if query.pattern_spec else None,
            "rules_count": len(plan.sg_rules),
            "metavar_filters": len(query.metavar_filters),
        }
        result.summary["file_filters"] = [asdict(decision) for decision in file_result.decisions]

    return result


def _execute_ast_grep_rules(
    rules: tuple[AstGrepRule, ...],
    paths: list[Path],
    root: Path,
    query: Query | None = None,
    globs: list[str] | None = None,
) -> tuple[list[Finding], list[SgRecord], list[dict]]:
    """Execute ast-grep rules using ast-grep-py and return findings.

    Parameters
    ----------
    rules
        ast-grep rules to execute
    paths
        Paths to scan
    root
        Repository root
    query
        Optional query for metavar filtering
    globs
        Optional glob filters (not used with ast-grep-py, filtering done upstream)

    Returns
    -------
    tuple[list[Finding], list[SgRecord], list[dict]]
        Findings, underlying records, and raw match data.
    """
    from tools.cq.query.metavar import apply_metavar_filters

    if not rules:
        return [], [], []

    findings: list[Finding] = []
    records: list[SgRecord] = []
    raw_matches: list[dict] = []

    # Execute rules using ast-grep-py
    for file_path in paths:
        try:
            src = file_path.read_text(encoding="utf-8")
        except OSError:
            continue

        sg_root = SgRoot(src, "python")
        node = sg_root.root()
        rel_path = _normalize_match_file(str(file_path), root)

        for idx, rule in enumerate(rules):
            rule_id = f"pattern_{idx}"
            pattern = rule.pattern

            # Skip if no pattern (kind-only rules handled differently)
            if not pattern or pattern in {"$FUNC", "$METHOD", "$CLASS"}:
                # For kind-only rules, use kind matching
                if rule.kind:
                    matches = node.find_all(kind=rule.kind)
                else:
                    continue
            else:
                matches = node.find_all(pattern=pattern)

            for match in matches:
                # Build match dict for compatibility
                range_obj = match.range()
                match_data = {
                    "ruleId": rule_id,
                    "file": rel_path,
                    "text": match.text(),
                    "range": {
                        "start": {"line": range_obj.start.line, "column": range_obj.start.column},
                        "end": {"line": range_obj.end.line, "column": range_obj.end.column},
                    },
                    "metaVariables": _extract_match_metavars(match),
                }
                raw_matches.append(match_data)

                # Apply metavar filters if present
                if query and query.metavar_filters:
                    captures = _parse_sgpy_metavariables(match)
                    if not apply_metavar_filters(captures, query.metavar_filters):
                        continue

                finding, record = _match_to_finding(match_data)
                if finding:
                    if query and query.metavar_filters:
                        captures = _extract_match_metavars(match)
                        finding.details["metavar_captures"] = captures
                    findings.append(finding)
                if record:
                    records.append(record)

    return findings, records, raw_matches


def _extract_match_metavars(match: SgNode) -> dict[str, str]:
    """Extract metavariable captures from an ast-grep-py match.

    Parameters
    ----------
    match
        ast-grep-py SgNode match.

    Returns
    -------
    dict[str, str]
        Dictionary of metavariable name to captured text.
    """
    metavars: dict[str, str] = {}
    common_names = [
        "$FUNC",
        "$F",
        "$CLASS",
        "$METHOD",
        "$M",
        "$X",
        "$Y",
        "$Z",
        "$A",
        "$B",
        "$OBJ",
        "$ATTR",
        "$VAL",
        "$E",
        "$NAME",
        "$MODULE",
        "$ARGS",
        "$KWARGS",
        "$COND",
        "$VAR",
        "$P",
        "$L",
        "$DECORATOR",
    ]
    for name in common_names:
        captured = match.get_match(name)
        if captured is not None:
            metavars[name] = captured.text()
    return metavars


def _parse_sgpy_metavariables(match: SgNode) -> dict[str, object]:
    """Parse metavariables from ast-grep-py match for filter application.

    Parameters
    ----------
    match
        ast-grep-py SgNode match.

    Returns
    -------
    dict[str, object]
        Dictionary of metavariable info for filtering.
    """
    from dataclasses import dataclass

    @dataclass
    class MetavarCapture:
        text: str

    result: dict[str, object] = {}
    common_names = [
        "$FUNC",
        "$F",
        "$CLASS",
        "$METHOD",
        "$M",
        "$X",
        "$Y",
        "$Z",
        "$A",
        "$B",
        "$OBJ",
        "$ATTR",
        "$VAL",
        "$E",
        "$NAME",
        "$MODULE",
        "$ARGS",
        "$KWARGS",
        "$COND",
        "$VAR",
        "$P",
        "$L",
        "$DECORATOR",
    ]
    for name in common_names:
        captured = match.get_match(name)
        if captured is not None:
            result[name] = MetavarCapture(text=captured.text())
    return result


def _match_to_finding(data: dict) -> tuple[Finding | None, SgRecord | None]:
    """Convert ast-grep match to Finding and SgRecord."""
    if "range" not in data or "file" not in data:
        return None, None

    range_data = data["range"]
    start = range_data.get("start", {})
    end = range_data.get("end", {})

    anchor = Anchor(
        file=data["file"],
        line=start.get("line", 0) + 1,  # Convert to 1-indexed
        col=start.get("column", 0),
        end_line=end.get("line", 0) + 1,
        end_col=end.get("column", 0),
    )

    finding = Finding(
        category="pattern_match",
        message=data.get("message", "Pattern match"),
        anchor=anchor,
        severity="info",
        details={
            "text": data.get("text", ""),
            "rule_id": data.get("ruleId", "pattern_query"),
        },
    )

    record = SgRecord(
        record="def",  # Default, may not be accurate for all patterns
        kind="pattern_match",
        file=data["file"],
        start_line=start.get("line", 0) + 1,
        start_col=start.get("column", 0),
        end_line=end.get("line", 0) + 1,
        end_col=end.get("column", 0),
        text=data.get("text", ""),
        rule_id=data.get("ruleId", "pattern_query"),
    )

    return finding, record


def _collect_match_spans(
    rules: tuple[AstGrepRule, ...],
    paths: list[Path],
    root: Path,
    query: Query,
    globs: list[str] | None,
) -> dict[str, list[tuple[int, int]]]:
    """Collect matched spans for relational constraints using ast-grep-py."""
    from tools.cq.query.metavar import apply_metavar_filters

    repo_context = resolve_repo_context(root)
    repo_index = build_repo_file_index(repo_context)
    file_result = tabulate_files(
        repo_index,
        paths,
        globs,
        extensions=(".py",),
    )

    spans: dict[str, list[tuple[int, int]]] = {}
    all_matches: list[tuple[dict, SgNode]] = []

    # Execute rules using ast-grep-py
    for file_path in file_result.files:
        try:
            src = file_path.read_text(encoding="utf-8")
        except OSError:
            continue

        sg_root = SgRoot(src, "python")
        node = sg_root.root()
        rel_path = _normalize_match_file(str(file_path), root)

        for rule in rules:
            pattern = rule.pattern
            if not pattern or pattern in {"$FUNC", "$METHOD", "$CLASS"}:
                if rule.kind:
                    matches = node.find_all(kind=rule.kind)
                else:
                    continue
            else:
                matches = node.find_all(pattern=pattern)

            for match in matches:
                range_obj = match.range()
                start_line = range_obj.start.line + 1
                end_line = range_obj.end.line + 1
                spans.setdefault(rel_path, []).append((start_line, end_line))

                # Store match for filtering
                match_data = {
                    "file": rel_path,
                    "range": {
                        "start": {"line": range_obj.start.line, "column": range_obj.start.column},
                        "end": {"line": range_obj.end.line, "column": range_obj.end.column},
                    },
                }
                all_matches.append((match_data, match))

    if not spans:
        return spans

    if not query.metavar_filters:
        return spans

    # Apply metavar filters
    filtered: dict[str, list[tuple[int, int]]] = {}
    for match_data, sg_match in all_matches:
        captures = _parse_sgpy_metavariables(sg_match)
        if not apply_metavar_filters(captures, query.metavar_filters):
            continue
        range_data = match_data.get("range", {})
        start = range_data.get("start", {})
        end = range_data.get("end", {})
        file_path = match_data.get("file", "")
        start_line = start.get("line", 0) + 1
        end_line = end.get("line", 0) + 1
        filtered.setdefault(file_path, []).append((start_line, end_line))

    return filtered


def _filter_records_by_spans(
    records: list[SgRecord],
    spans: dict[str, list[tuple[int, int]]],
) -> list[SgRecord]:
    """Filter records to those overlapping matched spans."""
    if not spans:
        return records

    filtered: list[SgRecord] = []
    for record in records:
        ranges = spans.get(record.file)
        if not ranges:
            continue
        for start_line, end_line in ranges:
            if start_line <= record.start_line <= end_line:
                filtered.append(record)
                break
    return filtered


def _record_key(record: SgRecord) -> tuple[str, int, int, int, int]:
    """Return a stable key for a record."""
    return (
        record.file,
        record.start_line,
        record.start_col,
        record.end_line,
        record.end_col,
    )


def _normalize_match_file(file_path: str, root: Path) -> str:
    """Normalize match paths to repo-relative POSIX strings."""
    path = Path(file_path)
    if path.is_absolute():
        try:
            return path.relative_to(root).as_posix()
        except ValueError:
            return file_path
    return path.as_posix()


def _build_def_evidence_map(
    def_records: list[SgRecord],
    root: Path,
) -> dict[tuple[str, int, int, int, int], dict[str, object]]:
    """Build a map of definition records to symtable/bytecode evidence."""
    from tools.cq.query.enrichment import BytecodeInfo, SymtableInfo, enrich_records

    unique_records: dict[tuple[str, int, int, int, int], SgRecord] = {}
    for record in def_records:
        unique_records[_record_key(record)] = record

    if not unique_records:
        return {}

    enrichment = enrich_records(list(unique_records.values()), root)
    evidence_map: dict[tuple[str, int, int, int, int], dict[str, object]] = {}

    for record_key, record in unique_records.items():
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
            evidence_map[record_key] = details

    return evidence_map


def _apply_call_evidence(
    details: dict[str, object],
    evidence: dict[str, object] | None,
    call_target: str,
) -> None:
    """Attach call evidence details to the finding payload."""
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


def _collect_cache_files(plan: ToolPlan, root: Path) -> list[Path]:
    """Collect files relevant for caching."""
    paths = scope_to_paths(plan.scope, root)
    globs = scope_to_globs(plan.scope)
    repo_context = resolve_repo_context(root)
    repo_index = build_repo_file_index(repo_context)
    result = tabulate_files(
        repo_index,
        paths,
        globs,
        extensions=(".py",),
    )
    return result.files


def _build_query_cache_key(
    query: Query,
    plan: ToolPlan,
    root: Path,
    tc: Toolchain,
) -> str:
    """Build a stable cache key for a query execution."""
    hazard_rules_hash: str | None = None
    if "hazards" in query.fields:
        from tools.cq.query.hazards import HazardDetector

        detector = HazardDetector()
        hazard_rules_hash = hashlib.sha256(
            detector.build_inline_rules_yaml().encode("utf-8")
        ).hexdigest()
    plan_signature = {
        "scope": asdict(plan.scope),
        "sg_record_types": sorted(plan.sg_record_types),
        "need_symtable": plan.need_symtable,
        "need_bytecode": plan.need_bytecode,
        "expand_ops": [asdict(exp) for exp in plan.expand_ops],
        "is_pattern_query": plan.is_pattern_query,
        "sg_rules": [rule.to_yaml_dict() for rule in plan.sg_rules],
    }
    payload = {
        "query": asdict(query),
        "plan": plan_signature,
        "root": str(root),
        "toolchain": tc.to_dict(),
        "hazard_rules_hash": hazard_rules_hash,
        "cache_version": QUERY_CACHE_VERSION,
    }
    encoded = json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _process_import_query(
    import_records: list[SgRecord],
    query: Query,
    result: CqResult,
    root: Path,
) -> None:
    """Process an import entity query."""
    matching_imports = _filter_to_matching(import_records, query)

    for import_record in matching_imports:
        finding = _import_to_finding(import_record)
        result.key_findings.append(finding)

    # Apply scope filter if present
    if query.scope_filter and matching_imports:
        enricher = SymtableEnricher(root)
        result.key_findings = filter_by_scope(
            result.key_findings,
            query.scope_filter,
            enricher,
            matching_imports,
        )

    result.summary["total_imports"] = len(import_records)
    result.summary["matches"] = len(result.key_findings)


def _process_def_query(
    ctx: ScanContext,
    query: Query,
    result: CqResult,
    root: Path,
    def_candidates: list[SgRecord] | None = None,
) -> None:
    """Process a definition entity query."""
    candidate_records = def_candidates if def_candidates is not None else ctx.def_records
    matching_defs = _filter_to_matching(candidate_records, query)
    matching_records = list(matching_defs)  # Keep copy for scope filtering

    for def_record in matching_defs:
        finding = _def_to_finding(def_record, ctx.calls_by_def.get(def_record, []))
        result.key_findings.append(finding)

    # Apply scope filter if present
    if query.scope_filter:
        enricher = SymtableEnricher(root)
        result.key_findings = filter_by_scope(
            result.key_findings,
            query.scope_filter,
            enricher,
            matching_records,
        )

    # Add sections based on query fields
    if "callers" in query.fields:
        callers_section = _build_callers_section(
            matching_defs,
            ctx.call_records,
            ctx.file_index,
            root,
        )
        if callers_section.findings:
            result.sections.append(callers_section)

    if "callees" in query.fields:
        callees_section = _build_callees_section(matching_defs, ctx.calls_by_def, root)
        if callees_section.findings:
            result.sections.append(callees_section)

    if "imports" in query.fields:
        imports_section = _build_imports_section(matching_defs, ctx.all_records)
        if imports_section.findings:
            result.sections.append(imports_section)

    if "hazards" in query.fields:
        hazards_section = _build_hazards_section(
            matching_defs,
            root,
            scope_to_globs(query.scope),
        )
        if hazards_section.findings:
            result.sections.append(hazards_section)

    _append_expander_sections(result, matching_defs, ctx, root, query)

    result.summary["total_defs"] = len(ctx.def_records)
    result.summary["total_calls"] = len(ctx.call_records)
    result.summary["matches"] = len(result.key_findings)


def _process_decorator_query(
    ctx: ScanContext,
    query: Query,
    result: CqResult,
    root: Path,
    def_candidates: list[SgRecord] | None = None,
) -> None:
    """Process a decorator entity query."""
    from tools.cq.query.enrichment import enrich_with_decorators

    # Look for decorated definitions
    matching_defs: list[SgRecord] = []

    candidate_records = def_candidates if def_candidates is not None else ctx.def_records
    for def_record in candidate_records:
        # Skip non-function/class definitions
        if def_record.kind not in (
            "function",
            "async_function",
            "function_typeparams",
            "class",
            "class_bases",
        ):
            continue

        # Check if matches name pattern
        if query.name and not _matches_name(def_record, query.name):
            continue

        # Read source to check for decorators
        file_path = root / def_record.file
        try:
            source = file_path.read_text(encoding="utf-8")
        except OSError:
            continue

        decorator_info = enrich_with_decorators(
            Finding(
                category="definition",
                message="",
                anchor=Anchor(file=def_record.file, line=def_record.start_line),
            ),
            source,
        )

        # Apply decorator filter if present
        if query.decorator_filter:
            decorators = decorator_info.get("decorators", [])
            count = len(decorators)

            # Filter by decorated_by
            if query.decorator_filter.decorated_by:
                if query.decorator_filter.decorated_by not in decorators:
                    continue

            # Filter by count
            if query.decorator_filter.decorator_count_min is not None:
                if count < query.decorator_filter.decorator_count_min:
                    continue
            if query.decorator_filter.decorator_count_max is not None:
                if count > query.decorator_filter.decorator_count_max:
                    continue

        # Only include if has decorators (for entity=decorator queries)
        if decorator_info.get("decorator_count", 0) > 0:
            matching_defs.append(def_record)

            finding = _def_to_finding(def_record, ctx.calls_by_def.get(def_record, []))
            finding.details["decorators"] = decorator_info.get("decorators", [])
            finding.details["decorator_count"] = decorator_info.get("decorator_count", 0)
            result.key_findings.append(finding)

    result.summary["total_defs"] = len(ctx.def_records)
    result.summary["matches"] = len(result.key_findings)


def _process_call_query(
    ctx: ScanContext,
    query: Query,
    result: CqResult,
    root: Path,
) -> None:
    """Process a callsite entity query."""
    matching_calls = _filter_to_matching(ctx.call_records, query)
    call_contexts: list[tuple[SgRecord, SgRecord | None]] = []
    for call_record in matching_calls:
        containing = ctx.file_index.find_containing(call_record)
        call_contexts.append((call_record, containing))

    containing_defs = [containing for _, containing in call_contexts if containing is not None]
    evidence_map = _build_def_evidence_map(containing_defs, root)

    for call_record, containing in call_contexts:
        details: dict[str, object] = {}
        call_target = _extract_call_target(call_record)
        if containing is not None:
            caller_name = _extract_def_name(containing) or "<module>"
            details["caller"] = caller_name
            evidence = evidence_map.get(_record_key(containing))
            _apply_call_evidence(details, evidence, call_target)
        finding = _call_to_finding(call_record, extra_details=details)
        result.key_findings.append(finding)

    result.summary["total_calls"] = len(ctx.call_records)
    result.summary["matches"] = len(result.key_findings)


def _append_expander_sections(
    result: CqResult,
    target_defs: list[SgRecord],
    ctx: ScanContext,
    root: Path,
    query: Query,
) -> None:
    """Append sections for requested expanders."""
    if not query.expand:
        return

    expand_kinds = {expander.kind for expander in query.expand}
    field_kinds = set(query.fields)
    if "callers" in expand_kinds and "callers" not in field_kinds:
        callers_section = _build_callers_section(
            target_defs,
            ctx.call_records,
            ctx.file_index,
            root,
        )
        if callers_section.findings:
            result.sections.append(callers_section)

    if "callees" in expand_kinds and "callees" not in field_kinds:
        callees_section = _build_callees_section(
            target_defs,
            ctx.calls_by_def,
            root,
        )
        if callees_section.findings:
            result.sections.append(callees_section)

    if "imports" in expand_kinds and "imports" not in field_kinds:
        imports_section = _build_imports_section(
            target_defs,
            ctx.all_records,
        )
        if imports_section.findings:
            result.sections.append(imports_section)

    if "raises" in expand_kinds:
        raises_section = _build_raises_section(
            target_defs,
            ctx.all_records,
            ctx.file_index,
        )
        if raises_section.findings:
            result.sections.append(raises_section)

    if "scope" in expand_kinds:
        scope_section = _build_scope_section(target_defs, root, ctx.calls_by_def)
        if scope_section.findings:
            result.sections.append(scope_section)

    if "bytecode_surface" in expand_kinds:
        bytecode_section = _build_bytecode_surface_section(target_defs, root)
        if bytecode_section.findings:
            result.sections.append(bytecode_section)


def rg_files_with_matches(
    root: Path,
    pattern: str,
    scope: Scope,
    *,
    limits: SearchLimits | None = None,
) -> list[Path]:
    """Use rpygrep to find files matching pattern.

    Parameters
    ----------
    root
        Repository root
    pattern
        Regex pattern to search
    scope
        Scope constraints
    limits
        Optional search safety limits. Uses scope.max_depth if not provided.

    Returns
    -------
    list[Path]
        Files containing matches
    """
    # Determine search root from scope
    search_root = root / scope.in_dir if scope.in_dir else root

    # Build limits from scope or defaults
    effective_limits = limits or SearchLimits(
        max_depth=scope.max_depth or 50,
    )

    return find_files_with_pattern(
        search_root,
        pattern,
        include_globs=list(scope.globs) if scope.globs else None,
        exclude_globs=list(scope.exclude) if scope.exclude else None,
        limits=effective_limits,
    )


def assign_calls_to_defs(
    index: IntervalIndex,
    calls: list[SgRecord],
) -> dict[SgRecord, list[SgRecord]]:
    """Assign call records to their containing definitions.

    Parameters
    ----------
    index
        Interval index of definitions
    calls
        Call records to assign

    Returns
    -------
    dict[SgRecord, list[SgRecord]]
        Mapping from definition to calls within it
    """
    result: dict[SgRecord, list[SgRecord]] = {}

    # Group calls by file
    calls_by_file = group_records_by_file(calls)

    # Group defs by file
    defs_by_file: dict[str, list[SgRecord]] = {}
    for _start, _end, record in index.intervals:
        if record.file not in defs_by_file:
            defs_by_file[record.file] = []
        defs_by_file[record.file].append(record)

    # For each file, build local index and assign
    for file_path, file_calls in calls_by_file.items():
        file_defs = defs_by_file.get(file_path, [])
        if not file_defs:
            continue

        # Build local index
        local_index = IntervalIndex.from_records(file_defs)

        # Assign each call
        for call in file_calls:
            containing_def = local_index.find_containing(call.start_line)
            if containing_def:
                if containing_def not in result:
                    result[containing_def] = []
                result[containing_def].append(call)

    return result


def _filter_to_matching(
    def_records: list[SgRecord],
    query: Query,
) -> list[SgRecord]:
    """Filter definitions to those matching the query."""
    matching: list[SgRecord] = []

    for record in def_records:
        # Filter by entity type
        if not _matches_entity(record, query.entity):
            continue

        # Filter by name pattern
        if query.name and not _matches_name(record, query.name):
            continue

        matching.append(record)

    return matching


def _matches_entity(record: SgRecord, entity: str | None) -> bool:
    """Check if record matches entity type."""
    if entity is None:
        return False

    function_kinds = {"function", "async_function", "function_typeparams"}
    class_kinds = {"class", "class_bases", "class_typeparams", "class_typeparams_bases"}
    import_kinds = {
        "import",
        "import_as",
        "from_import",
        "from_import_as",
        "from_import_multi",
        "from_import_paren",
    }
    decorator_kinds = function_kinds | {"class", "class_bases"}

    if entity == "function":
        return record.kind in function_kinds
    if entity == "class":
        return record.kind in class_kinds
    if entity == "method":
        # Methods are functions inside classes - would need context analysis
        return record.kind in {"function", "async_function"}
    if entity == "module":
        return False  # Module-level would need different handling
    if entity == "callsite":
        return record.record == "call"
    if entity == "import":
        return record.kind in import_kinds
    if entity == "decorator":
        # Decorators are applied to functions/classes - check for decorated definitions
        return record.kind in decorator_kinds
    return False


def _matches_name(record: SgRecord, name: str) -> bool:
    """Check if record matches name pattern."""
    # Extract name based on record type
    if record.record == "import":
        extracted_name = _extract_import_name(record)
    elif record.record == "call":
        extracted_name = _extract_call_name(record)
    else:
        extracted_name = _extract_def_name(record)

    if not extracted_name:
        return False

    # Regex match if pattern starts with ~
    if name.startswith("~"):
        pattern = name[1:]
        return bool(re.search(pattern, extracted_name))

    # Exact match otherwise
    return extracted_name == name


def _extract_def_name(record: SgRecord) -> str | None:
    """Extract the name from a definition record."""
    text = record.text

    # Match def name(...) or class name
    if record.record == "def":
        match = re.match(r"(?:async\s+)?(?:def|class)\s+(\w+)", text)
        if match:
            return match.group(1)

    return None


def _extract_import_name(record: SgRecord) -> str | None:
    """Extract the imported name from an import record.

    For single imports, returns the imported name or alias.
    For multi-imports (comma-separated or parenthesized), returns the module name.
    """
    text = record.text.strip()
    kind = record.kind

    # Dispatch to specific extractors
    if kind == "import":
        return _extract_simple_import(text)
    if kind == "import_as":
        return _extract_import_alias(text)
    if kind == "from_import":
        return _extract_from_import(text)
    if kind == "from_import_as":
        return _extract_from_import_alias(text)
    if kind in ("from_import_multi", "from_import_paren"):
        return _extract_from_module(text)
    return None


def _extract_simple_import(text: str) -> str | None:
    """Extract name from 'import foo' or 'import foo.bar'."""
    match = re.match(r"import\s+([\w.]+)", text)
    return match.group(1) if match else None


def _extract_import_alias(text: str) -> str | None:
    """Extract alias from 'import foo as bar'."""
    match = re.match(r"import\s+[\w.]+\s+as\s+(\w+)", text)
    return match.group(1) if match else None


def _extract_from_import(text: str) -> str | None:
    """Extract name from 'from x import y' (single import only)."""
    if "," not in text:
        match = re.search(r"import\s+(\w+)\s*$", text)
        if match:
            return match.group(1)
    # Fall back to module name for multi-imports
    return _extract_from_module(text)


def _extract_from_import_alias(text: str) -> str | None:
    """Extract alias from 'from x import y as z'."""
    match = re.search(r"as\s+(\w+)\s*$", text)
    return match.group(1) if match else None


def _extract_from_module(text: str) -> str | None:
    """Extract module name from 'from x import ...'."""
    match = re.match(r"from\s+([\w.]+)", text)
    return match.group(1) if match else None


def _def_to_finding(
    def_record: SgRecord,
    calls_within: list[SgRecord],
) -> Finding:
    """Convert a definition record to a Finding."""
    def_name = _extract_def_name(def_record) or "unknown"

    # Build anchor
    anchor = Anchor(
        file=def_record.file,
        line=def_record.start_line,
        col=def_record.start_col,
        end_line=def_record.end_line,
        end_col=def_record.end_col,
    )

    # Calculate scores
    impact_signals = ImpactSignals(
        sites=len(calls_within),
        files=1,
        depth=1,
    )
    conf_signals = ConfidenceSignals(evidence_kind="resolved_ast")

    impact = impact_score(impact_signals)
    confidence = confidence_score(conf_signals)

    return Finding(
        category="definition",
        message=f"{def_record.kind}: {def_name}",
        anchor=anchor,
        severity="info",
        details={
            "kind": def_record.kind,
            "name": def_name,
            "calls_within": len(calls_within),
            "impact_score": impact,
            "impact_bucket": bucket(impact),
            "confidence_score": confidence,
            "confidence_bucket": bucket(confidence),
            "evidence_kind": "resolved_ast",
        },
    )


def _import_to_finding(import_record: SgRecord) -> Finding:
    """Convert an import record to a Finding."""
    import_name = _extract_import_name(import_record) or "unknown"

    anchor = Anchor(
        file=import_record.file,
        line=import_record.start_line,
        col=import_record.start_col,
        end_line=import_record.end_line,
        end_col=import_record.end_col,
    )

    # Determine category based on import kind
    if import_record.kind in (
        "from_import",
        "from_import_as",
        "from_import_multi",
        "from_import_paren",
    ):
        category = "from_import"
    else:
        category = "import"

    return Finding(
        category=category,
        message=f"{category}: {import_name}",
        anchor=anchor,
        severity="info",
        details={
            "kind": import_record.kind,
            "name": import_name,
            "text": import_record.text.strip(),
        },
    )


def _build_callers_section(
    target_defs: list[SgRecord],
    all_calls: list[SgRecord],
    index: FileIntervalIndex,
    root: Path,
) -> Section:
    """Build section showing callers of target definitions."""
    findings: list[Finding] = []

    # Get names of target definitions
    target_names: set[str] = set()
    method_targets: set[str] = set()
    function_targets: set[str] = set()
    class_methods: dict[str, set[str]] = {}
    for def_record in target_defs:
        def_name = _extract_def_name(def_record)
        if not def_name:
            continue
        target_names.add(def_name)
        enclosing_class = _find_enclosing_class(def_record, index)
        if enclosing_class is None:
            function_targets.add(def_name)
            continue
        method_targets.add(def_name)
        class_name = _extract_def_name(enclosing_class)
        if not class_name:
            continue
        class_methods.setdefault(class_name, set()).add(def_name)

    # Find calls to target names
    call_contexts: list[tuple[SgRecord, str, SgRecord | None]] = []
    for call in all_calls:
        call_target = _extract_call_target(call)
        if call_target not in target_names:
            continue
        receiver = _extract_call_receiver(call)
        containing = index.find_containing(call)
        if receiver in {"self", "cls"} and containing is not None:
            caller_class = _find_enclosing_class(containing, index)
            if caller_class is not None:
                caller_class_name = _extract_def_name(caller_class)
                if caller_class_name:
                    methods = class_methods.get(caller_class_name, set())
                    if call_target not in methods:
                        continue
        if receiver is None and call_target in method_targets:
            if call_target not in function_targets:
                continue
        call_contexts.append((call, call_target, containing))

    containing_defs = [containing for _, _, containing in call_contexts if containing is not None]
    evidence_map = _build_def_evidence_map(containing_defs, root)

    for call, call_target, containing in call_contexts:
        caller_name = _extract_def_name(containing) if containing else "<module>"

        anchor = Anchor(
            file=call.file,
            line=call.start_line,
            col=call.start_col,
        )

        details: dict[str, object] = {
            "caller": caller_name,
            "callee": call_target,
        }
        if containing is not None:
            evidence = evidence_map.get(_record_key(containing))
            _apply_call_evidence(details, evidence, call_target)

        findings.append(
            Finding(
                category="caller",
                message=f"caller: {caller_name} calls {call_target}",
                anchor=anchor,
                severity="info",
                details=details,
            )
        )

    return Section(
        title="Callers",
        findings=findings,
    )


def _build_callees_section(
    target_defs: list[SgRecord],
    calls_by_def: dict[SgRecord, list[SgRecord]],
    root: Path,
) -> Section:
    """Build section showing callees for target definitions."""
    findings: list[Finding] = []
    evidence_map = _build_def_evidence_map(target_defs, root)

    for def_record in target_defs:
        def_name = _extract_def_name(def_record) or "<unknown>"
        evidence = evidence_map.get(_record_key(def_record))
        for call in calls_by_def.get(def_record, []):
            call_target = _extract_call_target(call)
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
            _apply_call_evidence(details, evidence, call_target)
            findings.append(
                Finding(
                    category="callee",
                    message=f"callee: {def_name} calls {call_target}",
                    anchor=anchor,
                    severity="info",
                    details=details,
                )
            )

    return Section(
        title="Callees",
        findings=findings,
    )


def _build_imports_section(
    target_defs: list[SgRecord],
    all_records: list[SgRecord],
) -> Section:
    """Build section showing imports within target files."""
    target_files = {record.file for record in target_defs}
    findings: list[Finding] = []

    for record in all_records:
        if record.record != "import":
            continue
        if record.file not in target_files:
            continue
        findings.append(_import_to_finding(record))

    return Section(
        title="Imports",
        findings=findings,
    )


def _build_raises_section(
    target_defs: list[SgRecord],
    all_records: list[SgRecord],
    index: FileIntervalIndex,
) -> Section:
    """Build section showing raises/excepts within target definitions."""
    findings: list[Finding] = []
    target_def_keys = {_record_key(record) for record in target_defs}

    for record in all_records:
        if record.record not in {"raise", "except"}:
            continue
        containing = index.find_containing(record)
        if containing is None or _record_key(containing) not in target_def_keys:
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
                details={
                    "context_def": _extract_def_name(containing) or "<module>",
                },
            )
        )

    return Section(
        title="Raises",
        findings=findings,
    )


def _build_scope_section(
    target_defs: list[SgRecord],
    root: Path,
    calls_by_def: dict[SgRecord, list[SgRecord]],
) -> Section:
    """Build section showing scope details for target definitions."""
    from tools.cq.query.enrichment import SymtableEnricher

    findings: list[Finding] = []
    enricher = SymtableEnricher(root)

    for def_record in target_defs:
        base_finding = _def_to_finding(def_record, calls_by_def.get(def_record, []))
        scope_info = enricher.enrich_function_finding(base_finding, def_record)
        if not scope_info:
            continue
        def_name = _extract_def_name(def_record) or "<unknown>"
        free_vars = scope_info.get("free_vars", [])
        cell_vars = scope_info.get("cell_vars", [])
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
                details=scope_info,
            )
        )

    return Section(
        title="Scope",
        findings=findings,
    )


def _build_bytecode_surface_section(
    target_defs: list[SgRecord],
    root: Path,
) -> Section:
    """Build section showing bytecode surface info for target definitions."""
    from tools.cq.query.enrichment import enrich_records

    findings: list[Finding] = []
    enrichment = enrich_records(target_defs, root)

    for record in target_defs:
        location = f"{record.file}:{record.start_line}:{record.start_col}"
        info = enrichment.get(location, {})
        bytecode_info = info.get("bytecode_info")
        if bytecode_info is None:
            continue
        def_name = _extract_def_name(record) or "<unknown>"
        details = {
            "globals": list(bytecode_info.load_globals),
            "attrs": list(bytecode_info.load_attrs),
            "calls": list(bytecode_info.call_functions),
        }
        anchor = Anchor(
            file=record.file,
            line=record.start_line,
            col=record.start_col,
        )
        message = (
            f"bytecode: {def_name} globals={len(details['globals'])} "
            f"attrs={len(details['attrs'])} calls={len(details['calls'])}"
        )
        findings.append(
            Finding(
                category="bytecode_surface",
                message=message,
                anchor=anchor,
                severity="info",
                details=details,
            )
        )

    return Section(
        title="Bytecode Surface",
        findings=findings,
    )


def _build_hazards_section(
    target_defs: list[SgRecord],
    root: Path,
    globs: list[str] | None,
) -> Section:
    """Build section showing potential hazards in target definitions using ast-grep-py."""
    from tools.cq.query.hazards import HazardDetector

    findings: list[Finding] = []
    if not target_defs:
        return Section(title="Hazards", findings=findings)

    detector = HazardDetector()
    scan_paths = [root / record.file for record in target_defs]

    # Execute hazard detection using ast-grep-py
    hazard_matches: list[dict] = []
    for file_path in scan_paths:
        try:
            src = file_path.read_text(encoding="utf-8")
        except OSError:
            continue

        sg_root = SgRoot(src, "python")
        node = sg_root.root()
        rel_path = _normalize_match_file(str(file_path), root)

        for spec in detector.specs:
            pattern = spec.pattern
            if not pattern:
                continue

            matches = node.find_all(pattern=pattern)
            for match in matches:
                range_obj = match.range()
                hazard_matches.append(
                    {
                        "ruleId": f"hazard_{spec.id}",
                        "file": rel_path,
                        "text": match.text(),
                        "range": {
                            "start": {
                                "line": range_obj.start.line,
                                "column": range_obj.start.column,
                            },
                            "end": {"line": range_obj.end.line, "column": range_obj.end.column},
                        },
                    }
                )

    target_index = FileIntervalIndex.from_records(target_defs)
    target_def_keys = {_record_key(record) for record in target_defs}

    for data in hazard_matches:
        rule_id = data.get("ruleId", "")
        if not rule_id.startswith("hazard_"):
            continue
        hazard_id = rule_id.removeprefix("hazard_")
        spec = detector.get_spec(hazard_id)
        if spec is None:
            continue
        range_data = data.get("range", {})
        start = range_data.get("start", {})
        end = range_data.get("end", {})
        file_path = _normalize_match_file(str(data.get("file", "")), root)
        record = SgRecord(
            record="call",
            kind="hazard",
            file=file_path,
            start_line=start.get("line", 0) + 1,
            start_col=start.get("column", 0),
            end_line=end.get("line", 0) + 1,
            end_col=end.get("column", 0),
            text=data.get("text", ""),
            rule_id=rule_id,
        )
        if spec.id == "bare_except":
            if not re.match(r"except\\s*:\\s", record.text.lstrip()):
                continue
        if spec.id == "broad_except":
            if not re.match(r"except\\s+Exception\\b", record.text.lstrip()):
                continue
        if spec.id == "except_pass":
            if not re.search(r"except[^\\n]*:\\n\\s+pass\\b", record.text):
                continue
        containing = target_index.find_containing(record)
        if containing is None or _record_key(containing) not in target_def_keys:
            continue
        anchor = Anchor(
            file=file_path,
            line=record.start_line,
            col=record.start_col,
            end_line=record.end_line,
            end_col=record.end_col,
        )
        findings.append(
            Finding(
                category="hazard",
                message=f"hazard: {spec.id} - {spec.message}",
                anchor=anchor,
                severity=spec.severity.value,
                details={
                    "kind": spec.id,
                    "category": spec.category.value,
                    "confidence_penalty": spec.confidence_penalty,
                },
            )
        )

    return Section(
        title="Hazards",
        findings=findings,
    )


def _call_to_finding(
    record: SgRecord,
    *,
    extra_details: dict[str, object] | None = None,
) -> Finding:
    """Convert a call record to a Finding."""
    call_target = _extract_call_target(record) or "<unknown>"
    anchor = Anchor(
        file=record.file,
        line=record.start_line,
        col=record.start_col,
        end_line=record.end_line,
        end_col=record.end_col,
    )
    details = {"text": record.text.strip()}
    if extra_details:
        details.update(extra_details)
    return Finding(
        category="callsite",
        message=f"call: {call_target}",
        anchor=anchor,
        severity="info",
        details=details,
    )


def _extract_call_target(call: SgRecord) -> str:
    """Extract the target name from a call record."""
    text = call.text

    # For attribute calls (obj.method()), extract the method name
    if call.kind == "attr_call" or call.kind == "attr":
        match = re.search(r"\.(\w+)\s*\(", text)
        if match:
            return match.group(1)

    # For name calls (func()), extract the function name
    match = re.match(r"(\w+)\s*\(", text)
    if match:
        return match.group(1)

    return ""


def _extract_call_receiver(call: SgRecord) -> str | None:
    """Extract the receiver name for attribute calls."""
    if call.kind not in {"attr_call", "attr"}:
        return None
    match = re.match(r"\s*(\w+)\s*\.", call.text)
    if match:
        return match.group(1)
    return None


def _find_enclosing_class(
    record: SgRecord,
    index: FileIntervalIndex,
) -> SgRecord | None:
    """Find the innermost class containing a record."""
    class_kinds = {
        "class",
        "class_bases",
        "class_typeparams",
        "class_typeparams_bases",
    }
    file_index = index.by_file.get(record.file)
    if file_index is None:
        return None
    candidates: list[SgRecord] = []
    for start, end, candidate in file_index.intervals:
        if candidate.kind not in class_kinds:
            continue
        if _record_key(candidate) == _record_key(record):
            continue
        if start <= record.start_line <= end:
            candidates.append(candidate)
    if not candidates:
        return None
    return min(candidates, key=lambda candidate: candidate.end_line - candidate.start_line)


def _extract_call_name(call: SgRecord) -> str | None:
    """Extract the name for callsite matching."""
    target = _extract_call_target(call)
    return target or None
