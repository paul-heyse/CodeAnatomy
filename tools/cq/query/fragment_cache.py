"""Shared fragment-cache helpers for query execution."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, cast

import msgspec

from tools.cq.astgrep.sgpy_scanner import SgRecord, group_records_by_file
from tools.cq.core.cache.fragment_codecs import decode_fragment_payload
from tools.cq.core.cache.fragment_contracts import (
    FragmentEntryV1,
    FragmentHitV1,
    FragmentMissV1,
    FragmentWriteV1,
)
from tools.cq.core.contracts import contract_to_builtins
from tools.cq.core.pathing import normalize_repo_relative_path
from tools.cq.core.schema import Finding
from tools.cq.query.cache_converters import (
    cache_record_to_record,
    finding_sort_key_lightweight,
    record_sort_key_detailed,
    record_sort_key_lightweight,
    record_to_cache_record,
)
from tools.cq.query.match_contracts import MatchData
from tools.cq.query.query_cache import (
    QueryFragmentCacheContext,
    QueryFragmentContextBuildRequest,
    build_query_fragment_cache_context,
    build_query_fragment_entries,
)
from tools.cq.query.sg_parser import list_scan_files, sg_scan
from tools.cq.search.cache.contracts import PatternFragmentCacheV1, QueryEntityScanCacheV1

if TYPE_CHECKING:
    from tools.cq.query.execution_context import QueryExecutionContext
    from tools.cq.query.ir import Query
    from tools.cq.query.planner import AstGrepRule

_ENTITY_FRAGMENT_PAYLOAD_LEN = 3

__all__ = [
    "EntityFragmentContext",
    "PatternFragmentContext",
    "assemble_entity_records",
    "assemble_pattern_output",
    "build_entity_fragment_context",
    "build_pattern_fragment_context",
    "decode_entity_fragment_payload",
    "decode_pattern_fragment_payload",
    "entity_fragment_entries",
    "entity_records_from_hits",
    "pattern_data_from_hits",
    "pattern_fragment_entries",
    "raw_match_sort_key",
    "scan_entity_fragment_misses",
    "scan_pattern_fragment_misses",
]


@dataclass(frozen=True)
class EntityFragmentContext:
    """Context for entity-record fragment caching."""

    cache_ctx: QueryFragmentCacheContext
    record_types: tuple[str, ...]


@dataclass(frozen=True)
class PatternFragmentContext:
    """Context for pattern fragment caching."""

    cache_ctx: QueryFragmentCacheContext
    rules_digest: str
    query_filters_digest: str


def build_entity_fragment_context(
    ctx: QueryExecutionContext,
    *,
    paths: list[Path],
    scope_globs: list[str] | None,
) -> EntityFragmentContext:
    """Build context for entity fragment caching.

    Returns:
        EntityFragmentContext: Fragment cache context and record-type metadata.
    """
    files = list_scan_files(
        paths=paths,
        root=ctx.root.resolve(),
        globs=scope_globs,
        lang=ctx.plan.lang,
    )
    record_types = tuple(sorted(ctx.plan.sg_record_types))
    return EntityFragmentContext(
        cache_ctx=build_query_fragment_cache_context(
            QueryFragmentContextBuildRequest(
                root=ctx.root,
                files=files,
                scope_roots=paths,
                scope_globs=scope_globs,
                namespace="query_entity_fragment",
                language=ctx.plan.lang,
                scope_hash_extras={"record_types": record_types},
                run_id=ctx.run_id,
            )
        ),
        record_types=record_types,
    )


def entity_fragment_entries(fragment_ctx: EntityFragmentContext) -> list[FragmentEntryV1]:
    """Build entity fragment entries with record-type extras.

    Returns:
        list[FragmentEntryV1]: Cache fragment entries for entity scanning.
    """
    return build_query_fragment_entries(
        fragment_ctx.cache_ctx,
        extras_builder=lambda _path, _rel: {"record_types": fragment_ctx.record_types},
    )


def decode_entity_fragment_payload(payload: object) -> list[SgRecord] | None:
    """Decode entity scan cache payload.

    Returns:
        list[SgRecord] | None: Decoded entity records or None for invalid payloads.
    """
    decoded = decode_fragment_payload(payload, type_=QueryEntityScanCacheV1)
    if decoded is None:
        return None
    return [cache_record_to_record(item) for item in decoded.records]


def entity_records_from_hits(hits: tuple[FragmentHitV1, ...]) -> dict[str, list[SgRecord]]:
    """Extract entity records from cache hits.

    Returns:
        dict[str, list[SgRecord]]: Entity records grouped by repo-relative path.
    """
    records_by_rel: dict[str, list[SgRecord]] = {}
    for hit in hits:
        if isinstance(hit.payload, list):
            records_by_rel[hit.entry.file] = cast("list[SgRecord]", hit.payload)
    return records_by_rel


def scan_entity_fragment_misses(
    *,
    ctx: QueryExecutionContext,
    fragment_ctx: EntityFragmentContext,
    misses: list[FragmentMissV1],
) -> tuple[dict[str, list[SgRecord]], list[FragmentWriteV1]]:
    """Scan miss paths and prepare entity fragment cache writes.

    Returns:
        tuple[dict[str, list[SgRecord]], list[FragmentWriteV1]]: Miss records and cache writes.
    """
    miss_paths = [fragment_ctx.cache_ctx.root / miss.entry.file for miss in misses]
    scanned = sg_scan(
        paths=miss_paths,
        record_types=ctx.plan.sg_record_types,
        root=fragment_ctx.cache_ctx.root,
        globs=None,
        lang=ctx.plan.lang,
    )
    grouped = group_records_by_file(scanned)
    records_by_rel: dict[str, list[SgRecord]] = {}
    writes: list[FragmentWriteV1] = []
    for miss in misses:
        rel_path = miss.entry.file
        fragment_records = sorted(grouped.get(rel_path, []), key=record_sort_key_detailed)
        records_by_rel[rel_path] = fragment_records
        writes.append(
            FragmentWriteV1(
                entry=miss.entry,
                payload=QueryEntityScanCacheV1(
                    records=[record_to_cache_record(item) for item in fragment_records]
                ),
            )
        )
    return records_by_rel, writes


def assemble_entity_records(
    files: list[Path],
    root: Path,
    records_by_rel: dict[str, list[SgRecord]],
) -> list[SgRecord]:
    """Assemble deterministic record ordering from file-bucketed payloads.

    Returns:
        list[SgRecord]: Deterministically ordered entity records.
    """
    ordered_records: list[SgRecord] = []
    for file_path in files:
        rel_path = normalize_repo_relative_path(str(file_path), root=root)
        ordered_records.extend(records_by_rel.get(rel_path, []))
    ordered_records.sort(key=record_sort_key_detailed)
    return ordered_records


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
        PatternFragmentContext: Fragment context and digest metadata for pattern scans.
    """
    from tools.cq.query.language import DEFAULT_QUERY_LANGUAGE

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
        list[FragmentEntryV1]: Cache fragment entries for pattern scanning.
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
        object | None: Decoded pattern payload tuple or None for invalid payloads.
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
    """Extract pattern findings/records/raw-match payloads from cache hits.

    Returns:
        tuple[dict[str, list[Finding]], dict[str, list[SgRecord]], dict[str, list[MatchData]]]:
            Pattern findings, records, and raw matches grouped by path.
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


def _compute_pattern_miss_data(
    *,
    rules: tuple[AstGrepRule, ...],
    query: Query | None,
    fragment_ctx: PatternFragmentContext,
    misses: tuple[FragmentMissV1, ...],
) -> tuple[dict[str, list[Finding]], dict[str, list[SgRecord]], dict[str, list[MatchData]]]:
    """Compute pattern data for cache misses.

    Returns:
        tuple[dict[str, list[Finding]], dict[str, list[SgRecord]], dict[str, list[MatchData]]]:
            Pattern findings, records, and raw matches grouped by miss path.
    """
    from tools.cq.query.executor_ast_grep import (
        AstGrepExecutionContext,
        AstGrepExecutionState,
        run_ast_grep,
    )

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


def scan_pattern_fragment_misses(
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
    """Scan miss paths and prepare pattern fragment cache writes.

    Returns:
        tuple[
            tuple[
                dict[str, list[Finding]],
                dict[str, list[SgRecord]],
                dict[str, list[MatchData]],
            ],
            list[FragmentWriteV1],
        ]: Normalized miss data and fragment cache writes.
    """
    miss_data = _compute_pattern_miss_data(
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
        findings = sorted(miss_data[0].get(rel_path, []), key=finding_sort_key_lightweight)
        records = sorted(miss_data[1].get(rel_path, []), key=record_sort_key_lightweight)
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


def assemble_pattern_output(
    *,
    paths: list[Path],
    root: Path,
    findings_by_rel: dict[str, list[Finding]],
    records_by_rel: dict[str, list[SgRecord]],
    raw_by_rel: dict[str, list[MatchData]],
) -> tuple[list[Finding], list[SgRecord], list[MatchData]]:
    """Assemble deterministic output from file-bucketed pattern data.

    Returns:
        tuple[list[Finding], list[SgRecord], list[MatchData]]: Deterministically ordered payloads.
    """
    findings: list[Finding] = []
    records: list[SgRecord] = []
    raw_matches: list[MatchData] = []
    for file_path in paths:
        rel_path = normalize_repo_relative_path(str(file_path), root=root)
        findings.extend(findings_by_rel.get(rel_path, []))
        records.extend(records_by_rel.get(rel_path, []))
        raw_matches.extend(raw_by_rel.get(rel_path, []))
    findings.sort(key=finding_sort_key_lightweight)
    records.sort(key=record_sort_key_lightweight)
    raw_matches.sort(key=raw_match_sort_key)
    return findings, records, raw_matches


def raw_match_sort_key(match: MatchData) -> tuple[str, int, int]:
    """Deterministic ordering key for raw match payloads.

    Returns:
        tuple[str, int, int]: Sort key `(file, line, col)`.
    """
    return (
        match.file,
        int(match.range.start.line),
        int(match.range.start.column),
    )
