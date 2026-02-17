"""AST-grep execution logic extracted from executor.py.

This module contains ast-grep match execution logic using ast-grep-py.
"""

from __future__ import annotations

import hashlib
import logging
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, cast

import msgspec
from ast_grep_py import Config, Rule, SgRoot

from tools.cq.astgrep.sgpy_scanner import (
    SgRecord,
    group_records_by_file,
    is_variadic_separator,
    node_payload,
)
from tools.cq.core.cache.fragment_codecs import decode_fragment_payload
from tools.cq.core.cache.fragment_contracts import (
    FragmentEntryV1,
    FragmentHitV1,
    FragmentMissV1,
    FragmentWriteV1,
)
from tools.cq.core.contracts import contract_to_builtins
from tools.cq.core.locations import SourceSpan
from tools.cq.core.pathing import normalize_repo_relative_path
from tools.cq.core.schema import Anchor, Finding
from tools.cq.core.scoring import build_detail_payload
from tools.cq.core.types import QueryLanguage
from tools.cq.query.cache_converters import (
    cache_record_to_record,
    record_to_cache_record,
)
from tools.cq.query.cache_converters import (
    finding_sort_key_lightweight as finding_sort_key,
)
from tools.cq.query.cache_converters import (
    record_sort_key_lightweight as record_sort_key,
)
from tools.cq.query.language import DEFAULT_QUERY_LANGUAGE, is_rust_language
from tools.cq.query.match_contracts import MatchData, MatchRange, MatchRangePoint
from tools.cq.query.metavar import (
    apply_metavar_filters,
    extract_rule_metavars,
    extract_rule_variadic_metavars,
    partition_metavar_filters,
)
from tools.cq.query.planner import AstGrepRule
from tools.cq.query.query_cache import (
    QueryFragmentCacheContext,
    QueryFragmentContextBuildRequest,
    build_query_fragment_cache_context,
    build_query_fragment_entries,
    run_query_fragment_scan,
)
from tools.cq.search.cache.contracts import PatternFragmentCacheV1
from tools.cq.search.rg.prefilter import extract_literal_fragments, rg_prefilter_files

if TYPE_CHECKING:
    from ast_grep_py import SgNode

    from tools.cq.query.ir import MetaVarCapture, MetaVarFilter, Query

from tools.cq.index.files import build_repo_file_index, tabulate_files
from tools.cq.index.repo import resolve_repo_context
from tools.cq.query.language import file_extensions_for_scope

_ENTITY_FRAGMENT_PAYLOAD_LEN = 3
_MIN_PREFILTER_LITERAL_LEN = 3
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PatternFragmentContext:
    """Context for pattern fragment caching."""

    cache_ctx: QueryFragmentCacheContext
    rules_digest: str
    query_filters_digest: str


@dataclass(frozen=True)
class AstGrepExecutionContext:
    """Inputs for executing inline ast-grep rules."""

    rules: tuple[AstGrepRule, ...]
    paths: list[Path]
    root: Path
    query: Query | None
    lang: QueryLanguage


@dataclass
class AstGrepExecutionState:
    """Mutable state for ast-grep rule execution."""

    findings: list[Finding]
    records: list[SgRecord]
    raw_matches: list[MatchData]


@dataclass(frozen=True)
class AstGrepRuleContext:
    """Per-rule execution context for ast-grep-py scanning."""

    node: SgNode
    rule: AstGrepRule
    rel_path: str
    rule_id: str


@dataclass(frozen=True)
class AstGrepMatchSpan:
    """Captured match span for relational filtering."""

    span: SourceSpan
    match: SgNode

    @property
    def file(self) -> str:
        """Return file path from span."""
        return self.span.file

    @property
    def start_line(self) -> int:
        """Return start line from span."""
        return self.span.start_line

    @property
    def end_line(self) -> int:
        """Return end line from span."""
        return self.span.end_line if self.span.end_line is not None else self.span.start_line


def execute_ast_grep_rules(
    rules: tuple[AstGrepRule, ...],
    paths: list[Path],
    root: Path,
    query: Query | None = None,
    globs: list[str] | None = None,
    run_id: str | None = None,
) -> tuple[list[Finding], list[SgRecord], list[MatchData]]:
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
    run_id
        Optional run ID for cache tagging

    Returns:
    -------
    tuple[list[Finding], list[SgRecord], list[MatchData]]
        Findings, underlying records, and raw match data.
    """
    if not rules or not paths:
        return [], [], []
    logger.debug("Executing ast-grep rules count=%d files=%d", len(rules), len(paths))

    fragment_ctx = build_pattern_fragment_context(
        root=root,
        paths=paths,
        query=query,
        rules=rules,
        globs=globs,
        run_id=run_id,
    )
    entries = pattern_fragment_entries(fragment_ctx)
    scan_result = run_query_fragment_scan(
        context=fragment_ctx.cache_ctx,
        entries=entries,
        run_id=run_id,
        decode=decode_pattern_fragment_payload,
        scan_misses=lambda misses: _scan_pattern_fragment_misses(
            rules=rules,
            query=query,
            fragment_ctx=fragment_ctx,
            misses=misses,
        ),
    )
    findings_by_rel, records_by_rel, raw_by_rel = pattern_data_from_hits(scan_result.hits)
    if scan_result.miss_payload is not None:
        findings_by_rel.update(scan_result.miss_payload[0])
        records_by_rel.update(scan_result.miss_payload[1])
        raw_by_rel.update(scan_result.miss_payload[2])
    return assemble_pattern_output(
        paths=fragment_ctx.cache_ctx.files,
        root=fragment_ctx.cache_ctx.root,
        findings_by_rel=findings_by_rel,
        records_by_rel=records_by_rel,
        raw_by_rel=raw_by_rel,
    )


def _scan_pattern_fragment_misses(
    *,
    rules: tuple[AstGrepRule, ...],
    query: Query | None,
    fragment_ctx: PatternFragmentContext,
    misses: list[FragmentMissV1],
) -> tuple[
    tuple[
        dict[str, list[Finding]],
        dict[str, list[SgRecord]],
        dict[str, list[MatchData]],
    ],
    list[FragmentWriteV1],
]:
    miss_data = compute_pattern_miss_data(
        rules=rules,
        query=query,
        fragment_ctx=fragment_ctx,
        misses=tuple(misses),
    )
    writes: list[FragmentWriteV1] = []
    normalized_findings: dict[str, list[Finding]] = {}
    normalized_records: dict[str, list[SgRecord]] = {}
    normalized_raw: dict[str, list[MatchData]] = {}
    for miss in misses:
        rel_path = miss.entry.file
        findings = sorted(miss_data[0].get(rel_path, []), key=finding_sort_key)
        records = sorted(miss_data[1].get(rel_path, []), key=record_sort_key)
        raw_matches = sorted(miss_data[2].get(rel_path, []), key=raw_match_sort_key)
        normalized_findings[rel_path] = findings
        normalized_records[rel_path] = records
        normalized_raw[rel_path] = raw_matches
        writes.append(
            FragmentWriteV1(
                entry=miss.entry,
                payload=PatternFragmentCacheV1(
                    findings=cast("list[dict[str, object]]", contract_to_builtins(findings)),
                    records=[record_to_cache_record(item) for item in records],
                    raw_matches=cast("list[dict[str, object]]", contract_to_builtins(raw_matches)),
                ),
            )
        )
    return (normalized_findings, normalized_records, normalized_raw), writes


def build_pattern_fragment_context(
    *,
    root: Path,
    paths: list[Path],
    query: Query | None,
    rules: tuple[AstGrepRule, ...],
    globs: list[str] | None,
    run_id: str | None,
) -> PatternFragmentContext:
    """Build context for pattern fragment caching.

    Returns:
        PatternFragmentContext: Cache/runtime context for pattern fragment operations.
    """
    lang = query.primary_language if query is not None else DEFAULT_QUERY_LANGUAGE
    rules_digest = hashlib.sha256(
        msgspec.json.encode(contract_to_builtins(list(rules)))
    ).hexdigest()
    query_filters_digest = hashlib.sha256(
        msgspec.json.encode(
            contract_to_builtins(list(query.metavar_filters if query is not None else []))
        )
    ).hexdigest()
    return PatternFragmentContext(
        cache_ctx=build_query_fragment_cache_context(
            QueryFragmentContextBuildRequest(
                root=root,
                files=paths,
                scope_roots=paths,
                scope_globs=globs,
                namespace="pattern_fragment",
                language=lang,
                scope_hash_extras={"rules_digest": rules_digest},
                run_id=run_id,
            )
        ),
        rules_digest=rules_digest,
        query_filters_digest=query_filters_digest,
    )


def pattern_fragment_entries(fragment_ctx: PatternFragmentContext) -> list[FragmentEntryV1]:
    """Build fragment entries for pattern caching.

    Returns:
        list[FragmentEntryV1]: Cache entry descriptors for every scoped file path.
    """
    return build_query_fragment_entries(
        fragment_ctx.cache_ctx,
        extras_builder=lambda _path, _rel: {
            "rules_digest": fragment_ctx.rules_digest,
            "query_filters_digest": fragment_ctx.query_filters_digest,
        },
    )


def decode_pattern_fragment_payload(payload: object) -> object | None:
    """Decode pattern fragment cache payload.

    Returns:
        object | None: Decoded ``(findings, records, raw_matches)`` tuple, or ``None``.
    """
    decoded = decode_fragment_payload(payload, type_=PatternFragmentCacheV1)
    if decoded is None:
        return None
    findings = [msgspec.convert(item, type=Finding) for item in decoded.findings]
    records = [cache_record_to_record(item) for item in decoded.records]
    raw_matches = [msgspec.convert(item, type=MatchData) for item in decoded.raw_matches]
    return (findings, records, raw_matches)


def pattern_data_from_hits(
    hits: tuple[FragmentHitV1, ...],
) -> tuple[dict[str, list[Finding]], dict[str, list[SgRecord]], dict[str, list[MatchData]]]:
    """Extract pattern data from cache hits.

    Returns:
        tuple[dict[str, list[Finding]], dict[str, list[SgRecord]], dict[str, list[MatchData]]]:
            File-bucketed findings, records, and raw-match payloads.
    """
    findings_by_rel: dict[str, list[Finding]] = {}
    records_by_rel: dict[str, list[SgRecord]] = {}
    raw_by_rel: dict[str, list[MatchData]] = {}
    for hit in hits:
        payload = hit.payload
        if not (
            isinstance(payload, tuple)
            and len(payload) == _ENTITY_FRAGMENT_PAYLOAD_LEN
            and isinstance(payload[0], list)
            and isinstance(payload[1], list)
            and isinstance(payload[2], list)
        ):
            continue
        findings_by_rel[hit.entry.file] = cast("list[Finding]", payload[0])
        records_by_rel[hit.entry.file] = cast("list[SgRecord]", payload[1])
        raw_by_rel[hit.entry.file] = cast("list[MatchData]", payload[2])
    return findings_by_rel, records_by_rel, raw_by_rel


def compute_pattern_miss_data(
    *,
    rules: tuple[AstGrepRule, ...],
    query: Query | None,
    fragment_ctx: PatternFragmentContext,
    misses: tuple[FragmentMissV1, ...],
) -> tuple[dict[str, list[Finding]], dict[str, list[SgRecord]], dict[str, list[MatchData]]]:
    """Compute pattern data for cache misses.

    Returns:
        tuple[dict[str, list[Finding]], dict[str, list[SgRecord]], dict[str, list[MatchData]]]:
            File-bucketed findings, records, and raw-match payloads for missed entries.
    """
    miss_paths = [fragment_ctx.cache_ctx.root / miss.entry.file for miss in misses]
    state = AstGrepExecutionState(findings=[], records=[], raw_matches=[])
    run_ast_grep(
        AstGrepExecutionContext(
            rules=rules,
            paths=miss_paths,
            root=fragment_ctx.cache_ctx.root,
            query=query,
            lang=fragment_ctx.cache_ctx.language,
        ),
        state,
    )
    miss_records = group_records_by_file(state.records)
    miss_findings: dict[str, list[Finding]] = {}
    for finding in state.findings:
        rel_path = finding.anchor.file if finding.anchor is not None else ""
        miss_findings.setdefault(rel_path, []).append(finding)
    miss_raw: dict[str, list[MatchData]] = {}
    for row in state.raw_matches:
        miss_raw.setdefault(row.file, []).append(row)
    return miss_findings, miss_records, miss_raw


def assemble_pattern_output(
    *,
    paths: list[Path],
    root: Path,
    findings_by_rel: dict[str, list[Finding]],
    records_by_rel: dict[str, list[SgRecord]],
    raw_by_rel: dict[str, list[MatchData]],
) -> tuple[list[Finding], list[SgRecord], list[MatchData]]:
    """Assemble pattern output from file-bucketed data.

    Returns:
        tuple[list[Finding], list[SgRecord], list[MatchData]]:
            Deterministically ordered findings, records, and raw matches.
    """
    findings: list[Finding] = []
    records: list[SgRecord] = []
    raw_matches: list[MatchData] = []
    for file_path in paths:
        rel_path = normalize_repo_relative_path(str(file_path), root=root)
        findings.extend(findings_by_rel.get(rel_path, []))
        records.extend(records_by_rel.get(rel_path, []))
        raw_matches.extend(raw_by_rel.get(rel_path, []))
    findings.sort(key=finding_sort_key)
    records.sort(key=record_sort_key)
    raw_matches.sort(key=raw_match_sort_key)
    return findings, records, raw_matches


def collect_rule_prefilter_literals(rules: tuple[AstGrepRule, ...]) -> tuple[str, ...]:
    """Collect literal fragments from rules for prefiltering.

    Returns:
        tuple[str, ...]: Unique literal fragments sorted by descending specificity.
    """
    fragments: list[str] = []
    for rule in rules:
        for text in (
            rule.pattern,
            rule.context,
            rule.inside,
            rule.has,
            rule.precedes,
            rule.follows,
        ):
            if isinstance(text, str) and text:
                fragments.extend(extract_literal_fragments(text)[:2])
        if rule.composite is not None:
            for pattern in rule.composite.patterns:
                fragments.extend(extract_literal_fragments(pattern)[:2])
    unique = sorted(
        {fragment for fragment in fragments if len(fragment) >= _MIN_PREFILTER_LITERAL_LEN},
        key=len,
        reverse=True,
    )
    return tuple(unique[:8])


def maybe_prefilter_pattern_paths(
    *,
    root: Path,
    files: list[Path],
    rules: tuple[AstGrepRule, ...],
    lang: QueryLanguage,
) -> list[Path]:
    """Prefilter paths using ripgrep before ast-grep execution.

    Returns:
        list[Path]: Candidate files likely to match at least one pattern literal.
    """
    if len(files) <= 1 or not rules:
        return files
    literals = collect_rule_prefilter_literals(rules)
    if not literals:
        return files
    return rg_prefilter_files(
        root,
        files=files,
        literals=literals,
        lang_scope="rust" if is_rust_language(lang) else "python",
    )


def run_ast_grep(ctx: AstGrepExecutionContext, state: AstGrepExecutionState) -> None:
    """Execute ast-grep rules and accumulate results in state."""
    candidate_paths = maybe_prefilter_pattern_paths(
        root=ctx.root,
        files=ctx.paths,
        rules=ctx.rules,
        lang=ctx.lang,
    )
    for file_path in candidate_paths:
        process_ast_grep_file(ctx, state, file_path)


def process_ast_grep_file(
    ctx: AstGrepExecutionContext,
    state: AstGrepExecutionState,
    file_path: Path,
) -> None:
    """Process a single file with ast-grep rules."""
    try:
        src = file_path.read_text(encoding="utf-8")
    except OSError:
        logger.warning("Skipping unreadable file for ast-grep: %s", file_path)
        return

    sg_root = SgRoot(src, ctx.lang)
    node = sg_root.root()
    rel_path = normalize_repo_relative_path(str(file_path), root=ctx.root)

    for idx, rule in enumerate(ctx.rules):
        rule_ctx = AstGrepRuleContext(
            node=node,
            rule=rule,
            rel_path=rel_path,
            rule_id=f"pattern_{idx}",
        )
        process_ast_grep_rule(ctx, state, rule_ctx)


def process_ast_grep_rule(
    ctx: AstGrepExecutionContext,
    state: AstGrepExecutionState,
    rule_ctx: AstGrepRuleContext,
) -> None:
    """Process a single ast-grep rule on a parsed node."""
    metavar_names = resolve_rule_metavar_names(rule_ctx.rule, ctx.query)
    variadic_names = resolve_rule_variadic_metavars(rule_ctx.rule)
    constraints, residual_filters = partition_query_metavar_filters(
        ctx.query,
        allowed_names=frozenset(metavar_names),
    )

    for match in execute_rule_matches(
        rule_ctx.node,
        rule_ctx.rule,
        constraints=constraints or None,
    ):
        match_data = build_match_data(
            match,
            rule_id=rule_ctx.rule_id,
            rel_path=rule_ctx.rel_path,
            metavar_names=metavar_names,
            variadic_names=variadic_names,
        )
        state.raw_matches.append(match_data)
        if not match_passes_filters(
            match,
            filters=residual_filters,
            metavar_names=metavar_names,
            variadic_names=variadic_names,
        ):
            continue
        finding, record = match_to_finding(match_data)
        if finding:
            apply_metavar_details(
                match,
                finding,
                metavar_names=metavar_names,
                variadic_names=variadic_names,
            )
            state.findings.append(finding)
        if record:
            state.records.append(record)


def resolve_rule_metavar_names(rule: AstGrepRule, query: Query | None) -> tuple[str, ...]:
    """Resolve metavar names from rule and query.

    Returns:
        tuple[str, ...]: Sorted metavariable names used by the rule/filter pipeline.
    """
    names = set(extract_rule_metavars(rule))
    if query is not None:
        names.update(filter_spec.name for filter_spec in query.metavar_filters)
    return tuple(sorted(names))


def resolve_rule_variadic_metavars(rule: AstGrepRule) -> frozenset[str]:
    """Resolve variadic metavar names from rule.

    Returns:
        frozenset[str]: Variadic metavariable names (for example, ``$$$ARGS``).
    """
    return extract_rule_variadic_metavars(rule)


def partition_query_metavar_filters(
    query: Query | None,
    *,
    allowed_names: frozenset[str] | None = None,
) -> tuple[dict[str, dict[str, str]], tuple[MetaVarFilter, ...]]:
    """Partition metavar filters into constraints and residual filters.

    Returns:
        tuple[dict[str, dict[str, str]], tuple[MetaVarFilter, ...]]:
            Constraint payload and residual filters to apply post-match.
    """
    if query is None or not query.metavar_filters:
        return {}, ()
    if allowed_names is None:
        return partition_metavar_filters(query.metavar_filters)
    relevant = tuple(
        filter_spec for filter_spec in query.metavar_filters if filter_spec.name in allowed_names
    )
    constraints, residual = partition_metavar_filters(relevant)
    residual_all = [
        *residual,
        *[
            filter_spec
            for filter_spec in query.metavar_filters
            if filter_spec.name not in allowed_names
        ],
    ]
    return constraints, tuple(residual_all)


def execute_rule_matches(
    node: SgNode,
    rule: AstGrepRule,
    *,
    constraints: dict[str, dict[str, str]] | None = None,
) -> list[SgNode]:
    """Execute ast-grep rule and return matched nodes.

    Returns:
        list[SgNode]: Nodes matched by either inline-rule or pattern execution.
    """
    if rule.requires_inline_rule() or constraints:
        rule_payload = strip_unsupported_sgpy_rule_keys(rule.to_yaml_dict())
        config: Config = {"rule": cast("Rule", rule_payload)}
        if constraints:
            constraint_payload = cast("dict[str, Mapping[object, object]]", constraints)
            config["constraints"] = constraint_payload
        return list(node.find_all(config=config))

    pattern = rule.pattern
    if not pattern or pattern in {"$FUNC", "$METHOD", "$CLASS"}:
        if rule.kind:
            return list(node.find_all(kind=rule.kind))
        return []
    return list(node.find_all(pattern=pattern))


def strip_unsupported_sgpy_rule_keys(value: object) -> object:
    """Remove rule keys not supported by ast-grep-py bindings.

    Returns:
    -------
    object
        Sanitized rule payload compatible with ast-grep-py runtime config.
    """
    if isinstance(value, dict):
        filtered: dict[str, object] = {}
        for key, child in value.items():
            if key == "strictness":
                continue
            filtered[key] = strip_unsupported_sgpy_rule_keys(child)
        return filtered
    if isinstance(value, list):
        return [strip_unsupported_sgpy_rule_keys(child) for child in value]
    return value


def build_match_data(
    match: SgNode,
    *,
    rule_id: str,
    rel_path: str,
    metavar_names: tuple[str, ...],
    variadic_names: frozenset[str],
) -> MatchData:
    """Build match data dictionary from ast-grep match.

    Returns:
        MatchData: Serializable match payload used by downstream conversion.
    """
    range_obj = match.range()
    return MatchData(
        file=rel_path,
        pattern=rule_id,
        text=match.text(),
        range=MatchRange(
            start=MatchRangePoint(line=range_obj.start.line, column=range_obj.start.column),
            end=MatchRangePoint(line=range_obj.end.line, column=range_obj.end.column),
        ),
        message="Pattern match",
        metavars=extract_match_metavars(
            match,
            metavar_names=metavar_names,
            variadic_names=variadic_names,
            include_multi=True,
        ),
    )


def match_passes_filters(
    match: SgNode,
    *,
    filters: tuple[MetaVarFilter, ...],
    metavar_names: tuple[str, ...],
    variadic_names: frozenset[str],
) -> bool:
    """Check if match passes metavar filters.

    Returns:
        bool: ``True`` when the match satisfies all residual metavariable filters.
    """
    if filters:
        captures = parse_sgpy_metavariables(
            match,
            metavar_names=metavar_names,
            variadic_names=variadic_names,
        )
        return apply_metavar_filters(captures, filters)
    return True


def apply_metavar_details(
    match: SgNode,
    finding: Finding,
    *,
    metavar_names: tuple[str, ...],
    variadic_names: frozenset[str],
) -> None:
    """Apply metavar capture details to finding."""
    captures = extract_match_metavars(
        match,
        metavar_names=metavar_names,
        variadic_names=variadic_names,
        include_multi=True,
    )
    if captures:
        finding.details["metavar_captures"] = captures


def extract_match_metavars(
    match: SgNode,
    *,
    metavar_names: tuple[str, ...],
    variadic_names: frozenset[str],
    include_multi: bool = False,
) -> dict[str, object]:
    """Extract metavariable captures from an ast-grep-py match.

    Parameters
    ----------
    match
        ast-grep-py SgNode match.
    metavar_names
        Names of metavariables to extract.
    variadic_names
        Names of variadic metavariables.
    include_multi
        Whether to include multi-node captures.

    Returns:
    -------
    dict[str, object]
        Dictionary of metavariable names to capture payloads.
    """
    metavars: dict[str, object] = {}
    for bare_name in metavar_names:
        captured = match.get_match(bare_name)
        if captured is None:
            pass
        else:
            text = captured.text()
            # Keep both bare and `$`-prefixed keys for output compatibility.
            metavars[bare_name] = text
            metavars[f"${bare_name}"] = text

        if include_multi and bare_name in variadic_names:
            captured_multi = match.get_multiple_matches(bare_name)
            all_nodes: list[SgNode] = list(captured_multi) if captured_multi is not None else []
            captured_nodes = [node for node in all_nodes if not is_variadic_separator(node)]
            if captured_nodes:
                text = ", ".join(node.text() for node in captured_nodes)
                metavars[f"$$${bare_name}"] = {
                    "kind": "multi",
                    "text": text,
                    "nodes": [node_payload(node) for node in captured_nodes],
                }
    return metavars


def parse_sgpy_metavariables(
    match: SgNode,
    *,
    metavar_names: tuple[str, ...],
    variadic_names: frozenset[str],
) -> dict[str, MetaVarCapture]:
    """Parse metavariables from ast-grep-py match for filter application.

    Parameters
    ----------
    match
        ast-grep-py SgNode match.
    metavar_names
        Names of metavariables to extract.
    variadic_names
        Names of variadic metavariables.

    Returns:
    -------
    dict[str, MetaVarCapture]
        Dictionary of metavariable info for filtering.
    """
    from tools.cq.query.ir import MetaVarCapture

    result: dict[str, MetaVarCapture] = {}
    for bare_name in metavar_names:
        captured = match.get_match(bare_name)
        if captured is not None:
            result[bare_name] = MetaVarCapture(name=bare_name, kind="single", text=captured.text())
            continue
        if bare_name not in variadic_names:
            continue
        captured_multi = match.get_multiple_matches(bare_name)
        all_nodes: list[SgNode] = list(captured_multi) if captured_multi is not None else []
        captured_nodes = [node for node in all_nodes if not is_variadic_separator(node)]
        if not captured_nodes:
            continue
        texts = [node.text() for node in captured_nodes]
        result[bare_name] = MetaVarCapture(
            name=bare_name,
            kind="multi",
            text=", ".join(texts),
            nodes=texts,
        )
    return result


def coerce_int(value: object) -> int:
    """Coerce a value to ``int`` with a zero fallback.

    Returns:
        int: ``value`` when already an ``int``; otherwise ``0``.
    """
    if isinstance(value, int):
        return value
    return 0


def match_to_finding(data: MatchData) -> tuple[Finding | None, SgRecord | None]:
    """Convert ast-grep match to Finding and SgRecord.

    Returns:
    -------
    tuple[Finding | None, SgRecord | None]
        Finding and record for the match when available.
    """
    file_name = data.file
    start = data.range.start
    end = data.range.end

    anchor = Anchor(
        file=file_name,
        line=coerce_int(start.line) + 1,  # Convert to 1-indexed
        col=coerce_int(start.column),
        end_line=coerce_int(end.line) + 1,
        end_col=coerce_int(end.column),
    )

    finding = Finding(
        category="pattern_match",
        message=data.message or "Pattern match",
        anchor=anchor,
        severity="info",
        details=build_detail_payload(
            data={
                "text": data.text,
                "rule_id": data.pattern,
            }
        ),
    )

    record = SgRecord(
        record="def",  # Default, may not be accurate for all patterns
        kind="pattern_match",
        file=file_name,
        start_line=coerce_int(start.line) + 1,
        start_col=coerce_int(start.column),
        end_line=coerce_int(end.line) + 1,
        end_col=coerce_int(end.column),
        text=data.text,
        rule_id=data.pattern,
    )

    return finding, record


def collect_ast_grep_match_spans(
    files: list[Path],
    rules: tuple[AstGrepRule, ...],
    root: Path,
    lang: QueryLanguage,
    *,
    query: Query,
) -> list[AstGrepMatchSpan]:
    """Collect match spans from ast-grep execution.

    Returns:
    -------
    list[AstGrepMatchSpan]
        List of matched spans.
    """
    matches: list[AstGrepMatchSpan] = []
    candidate_files = maybe_prefilter_pattern_paths(
        root=root,
        files=files,
        rules=rules,
        lang=lang,
    )
    for file_path in candidate_files:
        try:
            src = file_path.read_text(encoding="utf-8")
        except OSError:
            logger.warning("Skipping unreadable file for ast-grep span collection: %s", file_path)
            continue
        sg_root = SgRoot(src, lang)
        node = sg_root.root()
        rel_path = normalize_repo_relative_path(str(file_path), root=root)
        for rule in rules:
            metavar_names = resolve_rule_metavar_names(rule, query)
            variadic_names = resolve_rule_variadic_metavars(rule)
            constraints, residual_filters = partition_query_metavar_filters(
                query,
                allowed_names=frozenset(metavar_names),
            )
            for match in execute_rule_matches(node, rule, constraints=constraints or None):
                if not match_passes_filters(
                    match,
                    filters=residual_filters,
                    metavar_names=metavar_names,
                    variadic_names=variadic_names,
                ):
                    continue
                range_obj = match.range()
                matches.append(
                    AstGrepMatchSpan(
                        span=SourceSpan(
                            file=rel_path,
                            start_line=range_obj.start.line + 1,
                            start_col=range_obj.start.column,
                            end_line=range_obj.end.line + 1,
                            end_col=range_obj.end.column,
                        ),
                        match=match,
                    )
                )
    return matches


def raw_match_sort_key(match: MatchData) -> tuple[str, int, int]:
    """Return sort key for raw match data."""
    return (match.file, match.range.start.line, match.range.start.column)


def collect_match_spans(
    rules: tuple[AstGrepRule, ...],
    paths: list[Path],
    root: Path,
    query: Query,
    globs: list[str] | None,
) -> dict[str, list[tuple[int, int]]]:
    """Collect matched spans for relational constraints using ast-grep-py.

    Returns:
    -------
    dict[str, list[tuple[int, int]]]
        Mapping from file to matched (start_line, end_line) spans.
    """
    repo_context = resolve_repo_context(root)
    repo_index = build_repo_file_index(repo_context)
    file_result = tabulate_files(
        repo_index,
        paths,
        globs,
        extensions=file_extensions_for_scope(query.lang_scope),
    )
    matches = collect_ast_grep_match_spans(
        file_result.files,
        rules,
        root,
        query.primary_language,
        query=query,
    )
    if not matches:
        return {}
    return group_match_spans(matches)


def group_match_spans(
    matches: list[AstGrepMatchSpan],
) -> dict[str, list[tuple[int, int]]]:
    """Group match spans by file.

    Returns:
    -------
    dict[str, list[tuple[int, int]]]
        Mapping from file to matched (start_line, end_line) spans.
    """
    spans: dict[str, list[tuple[int, int]]] = {}
    for match in matches:
        spans.setdefault(match.file, []).append((match.start_line, match.end_line))
    return spans


def filter_records_by_spans(
    records: list[SgRecord],
    spans: dict[str, list[tuple[int, int]]],
) -> list[SgRecord]:
    """Filter records to those overlapping matched spans.

    Returns:
    -------
    list[SgRecord]
        Records that overlap the provided spans.
    """
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


__all__ = [
    "AstGrepExecutionContext",
    "AstGrepExecutionState",
    "AstGrepMatchSpan",
    "AstGrepRuleContext",
    "PatternFragmentContext",
    "apply_metavar_details",
    "assemble_pattern_output",
    "build_match_data",
    "build_pattern_fragment_context",
    "cache_record_to_record",
    "coerce_int",
    "collect_ast_grep_match_spans",
    "collect_match_spans",
    "collect_rule_prefilter_literals",
    "compute_pattern_miss_data",
    "decode_pattern_fragment_payload",
    "execute_ast_grep_rules",
    "execute_rule_matches",
    "extract_match_metavars",
    "filter_records_by_spans",
    "finding_sort_key",
    "group_match_spans",
    "is_variadic_separator",
    "match_passes_filters",
    "match_to_finding",
    "maybe_prefilter_pattern_paths",
    "node_payload",
    "parse_sgpy_metavariables",
    "partition_query_metavar_filters",
    "pattern_data_from_hits",
    "pattern_fragment_entries",
    "process_ast_grep_file",
    "process_ast_grep_rule",
    "raw_match_sort_key",
    "record_sort_key",
    "record_to_cache_record",
    "resolve_rule_metavar_names",
    "resolve_rule_variadic_metavars",
    "run_ast_grep",
    "strip_unsupported_sgpy_rule_keys",
]
