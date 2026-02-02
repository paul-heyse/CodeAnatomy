"""Query executor for cq queries.

Executes ToolPlans and returns CqResult objects.
"""

from __future__ import annotations

import json
import re
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from tools.cq.core.schema import Anchor, CqResult, Finding, Section, mk_result, mk_runmeta, ms
from tools.cq.core.scoring import (
    ConfidenceSignals,
    ImpactSignals,
    bucket,
    confidence_score,
    impact_score,
)
from tools.cq.query.enrichment import SymtableEnricher, filter_by_scope
from tools.cq.query.planner import AstGrepRule, ToolPlan, scope_to_paths
from tools.cq.query.sg_parser import (
    SgRecord,
    filter_records_by_kind,
    group_records_by_file,
    sg_scan,
)

if TYPE_CHECKING:
    from tools.cq.core.toolchain import Toolchain

from tools.cq.query.ir import Query, Scope


@dataclass
class ScanContext:
    """Bundled context from ast-grep scan for query processing."""

    def_records: list[SgRecord]
    call_records: list[SgRecord]
    interval_index: IntervalIndex
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


def execute_plan(
    plan: ToolPlan,
    query: Query,
    tc: Toolchain,
    root: Path,
    argv: list[str] | None = None,
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

    # Dispatch to pattern query executor if this is a pattern query
    if plan.is_pattern_query:
        return _execute_pattern_query(plan, query, tc, root, argv, started_ms)

    return _execute_entity_query(plan, query, tc, root, argv, started_ms)


def _execute_entity_query(
    plan: ToolPlan,
    query: Query,
    tc: Toolchain,
    root: Path,
    argv: list[str] | None,
    started_ms: float,
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

    # Phase 1: File narrowing with ripgrep (optional)
    if plan.rg_pattern and tc.rg_path:
        matched_files = rg_files_with_matches(root, plan.rg_pattern, plan.scope)
        if not matched_files:
            run = mk_runmeta(
                macro="q",
                argv=argv or [],
                root=str(root),
                started_ms=started_ms,
                toolchain=tc.to_dict(),
            )
            result = mk_result(run)
            result.summary["matches"] = 0
            result.summary["pattern"] = plan.rg_pattern
            return result
        paths = matched_files

    # Phase 2: ast-grep scan
    if not tc.has_sg:
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
    )

    # Phase 3: Build scan context
    def_records = filter_records_by_kind(records, "def")
    interval_index = IntervalIndex.from_records(def_records)
    call_records = filter_records_by_kind(records, "call")
    calls_by_def = assign_calls_to_defs(interval_index, call_records)

    ctx = ScanContext(
        def_records=def_records,
        call_records=call_records,
        interval_index=interval_index,
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
    if query.entity == "import":
        _process_import_query(records, query, result, root)
    elif query.entity == "decorator":
        _process_decorator_query(ctx, query, result, root)
    else:
        _process_def_query(ctx, query, result, root)

    result.summary["files_scanned"] = len({r.file for r in records})

    if plan.explain:
        result.summary["plan"] = {
            "rg_pattern": plan.rg_pattern,
            "sg_record_types": list(plan.sg_record_types),
            "need_symtable": plan.need_symtable,
            "need_bytecode": plan.need_bytecode,
            "is_pattern_query": plan.is_pattern_query,
        }

    return result


def _execute_pattern_query(
    plan: ToolPlan,
    query: Query,
    tc: Toolchain,
    root: Path,
    argv: list[str] | None,
    started_ms: float,
) -> CqResult:
    """Execute a pattern-based query using inline ast-grep rules."""
    if not tc.has_sg:
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

    # Execute ast-grep rules
    findings: list[Finding] = []
    records: list[SgRecord] = []
    raw_matches: list[dict] = []

    for rule in plan.sg_rules:
        rule_findings, rule_records, rule_matches = _execute_ast_grep_rule(
            rule, paths, root, query
        )
        findings.extend(rule_findings)
        records.extend(rule_records)
        raw_matches.extend(rule_matches)

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

    return result


def _execute_ast_grep_rule(
    rule: AstGrepRule,
    paths: list[Path],
    root: Path,
    query: Query | None = None,
) -> tuple[list[Finding], list[SgRecord], list[dict]]:
    """Execute a single ast-grep rule and return findings.

    Parameters
    ----------
    rule
        The ast-grep rule to execute
    paths
        Paths to scan
    root
        Repository root
    query
        Optional query for metavar filtering

    Returns
    -------
    tuple[list[Finding], list[SgRecord], list[dict]]
        Findings, underlying records, and raw match data.
    """
    from tools.cq.query.metavar import apply_metavar_filters, parse_metavariables

    findings: list[Finding] = []
    records: list[SgRecord] = []
    raw_matches: list[dict] = []

    # Build inline rule YAML
    rule_dict = _build_inline_rule(rule)

    # Write to temp file and run ast-grep
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        import yaml

        yaml.dump(rule_dict, f, default_flow_style=False)
        rule_file = Path(f.name)

    try:
        cmd = [
            "ast-grep",
            "scan",
            "-r",
            str(rule_file),
            "--json=stream",
        ]
        cmd.extend(str(p) for p in paths)

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=root,
        )

        if result.returncode != 0:
            return findings, records, raw_matches

        # Parse JSON stream output
        for line in result.stdout.strip().split("\n"):
            if not line:
                continue
            try:
                data = json.loads(line)
                raw_matches.append(data)

                # Apply metavar filtering if filters are present
                if query and query.metavar_filters:
                    captures = parse_metavariables(data)
                    if not apply_metavar_filters(captures, query.metavar_filters):
                        continue  # Skip this match

                finding, record = _match_to_finding(data)
                if finding:
                    # Add metavar captures to finding details
                    if query and query.metavar_filters:
                        captures = parse_metavariables(data)
                        finding.details["metavar_captures"] = {
                            name: cap.text for name, cap in captures.items()
                        }
                    findings.append(finding)
                if record:
                    records.append(record)
            except json.JSONDecodeError:
                continue

    finally:
        rule_file.unlink(missing_ok=True)

    return findings, records, raw_matches


def _build_inline_rule(rule: AstGrepRule) -> dict:
    """Build ast-grep inline rule from AstGrepRule."""
    rule_config = rule.to_yaml_dict()
    return {
        "id": "pattern_query",
        "language": "python",
        "rule": rule_config,
        "message": "Pattern match",
    }


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


def _process_import_query(
    records: list[SgRecord],
    query: Query,
    result: CqResult,
    root: Path,
) -> None:
    """Process an import entity query."""
    import_records = filter_records_by_kind(records, "import")
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
) -> None:
    """Process a definition entity query."""
    matching_defs = _filter_to_matching(ctx.def_records, query)
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
            matching_defs, ctx.call_records, ctx.interval_index
        )
        if callers_section.findings:
            result.sections.append(callers_section)

    if "hazards" in query.fields:
        hazards_section = _build_hazards_section(matching_defs, ctx.all_records)
        if hazards_section.findings:
            result.sections.append(hazards_section)

    result.summary["total_defs"] = len(ctx.def_records)
    result.summary["total_calls"] = len(ctx.call_records)
    result.summary["matches"] = len(result.key_findings)


def _process_decorator_query(
    ctx: ScanContext,
    query: Query,
    result: CqResult,
    root: Path,
) -> None:
    """Process a decorator entity query."""
    from tools.cq.query.enrichment import enrich_with_decorators

    # Look for decorated definitions
    matching_defs: list[SgRecord] = []

    for def_record in ctx.def_records:
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


def rg_files_with_matches(
    root: Path,
    pattern: str,
    scope: Scope,
) -> list[Path]:
    """Use ripgrep to find files matching pattern.

    Parameters
    ----------
    root
        Repository root
    pattern
        Regex pattern to search
    scope
        Scope constraints

    Returns
    -------
    list[Path]
        Files containing matches
    """
    cmd = ["rg", "--files-with-matches", "-e", pattern, "--type", "py"]

    # Add scope constraints
    if scope.in_dir:
        cmd.append(scope.in_dir)
    else:
        cmd.append(".")

    for exclude in scope.exclude:
        cmd.extend(["-g", f"!{exclude}"])

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        cwd=root,
    )

    if result.returncode not in (0, 1):  # 1 means no matches
        return []

    files: list[Path] = []
    for line in result.stdout.strip().split("\n"):
        if line:
            files.append(root / line)

    return files


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
    index: IntervalIndex,
) -> Section:
    """Build section showing callers of target definitions."""
    findings: list[Finding] = []

    # Get names of target definitions
    target_names = {_extract_def_name(d) for d in target_defs if _extract_def_name(d)}

    # Find calls to target names
    for call in all_calls:
        call_target = _extract_call_target(call)
        if call_target not in target_names:
            continue

        # Find containing definition (the caller)
        containing = index.find_containing(call.start_line)
        caller_name = _extract_def_name(containing) if containing else "<module>"

        anchor = Anchor(
            file=call.file,
            line=call.start_line,
            col=call.start_col,
        )

        findings.append(
            Finding(
                category="caller",
                message=f"caller: {caller_name} calls {call_target}",
                anchor=anchor,
                severity="info",
                details={
                    "caller": caller_name,
                    "callee": call_target,
                },
            )
        )

    return Section(
        title="Callers",
        findings=findings,
    )


def _build_hazards_section(
    target_defs: list[SgRecord],
    all_records: list[SgRecord],
) -> Section:
    """Build section showing potential hazards in target definitions."""
    findings: list[Finding] = []

    # Build index for target defs
    index = IntervalIndex.from_records(target_defs)

    # Check for hazardous patterns in call records
    for record in all_records:
        if record.record != "call":
            continue

        containing = index.find_containing(record.start_line)
        if not containing:
            continue

        hazard = _detect_hazard(record)
        if hazard:
            anchor = Anchor(
                file=record.file,
                line=record.start_line,
                col=record.start_col,
            )

            findings.append(
                Finding(
                    category="hazard",
                    message=f"hazard: {hazard['kind']} - {hazard['reason']}",
                    anchor=anchor,
                    severity="warning",
                    details=hazard,
                )
            )

    return Section(
        title="Hazards",
        findings=findings,
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


def _detect_hazard(call: SgRecord) -> dict | None:
    """Detect potential hazards in a call.

    Returns hazard dict or None if no hazard detected.
    """
    text = call.text

    # Dynamic dispatch hazard: getattr, __getattr__
    if "getattr" in text.lower():
        return {
            "kind": "dynamic_dispatch",
            "reason": "getattr usage may resolve dynamically",
            "confidence": 0.50,
        }

    # Forwarding hazard: *args, **kwargs in call
    if "*" in text and "(" in text:
        match = re.search(r"\(\s*\*", text)
        if match:
            return {
                "kind": "forwarding",
                "reason": "argument forwarding may obscure call target",
                "confidence": 0.70,
            }

    return None
