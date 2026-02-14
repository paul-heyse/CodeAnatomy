"""Shared cache-backed pipeline for neighborhood structural scans."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import msgspec

from tools.cq.astgrep.sgpy_scanner import SgRecord
from tools.cq.core.cache import (
    CacheWriteTagRequestV1,
    CqCacheBackend,
    CqCachePolicyV1,
    FragmentEntryV1,
    FragmentMissV1,
    FragmentPersistRuntimeV1,
    FragmentProbeRuntimeV1,
    FragmentRequestV1,
    FragmentWriteV1,
    build_cache_key,
    build_scope_hash,
    build_scope_snapshot_fingerprint,
    default_cache_policy,
    file_content_hash,
    get_cq_cache_backend,
    is_namespace_cache_enabled,
    partition_fragment_entries,
    persist_fragment_writes,
    record_cache_decode_failure,
    record_cache_get,
    record_cache_set,
    resolve_namespace_ttl_seconds,
    resolve_write_cache_tag,
)
from tools.cq.core.cache.contracts import QueryEntityScanCacheV1, SgRecordCacheV1
from tools.cq.core.cache.fragment_codecs import decode_fragment_payload, encode_fragment_payload
from tools.cq.core.contracts import contract_to_builtins
from tools.cq.query.language import QueryLanguage
from tools.cq.query.sg_parser import list_scan_files, sg_scan


class NeighborhoodScanRequestV1(msgspec.Struct, frozen=True):
    """Serializable request for neighborhood repository scanning."""

    root: str
    language: str = "python"
    run_id: str | None = None


@dataclass(frozen=True, slots=True)
class _RuntimeContext:
    root: Path
    language: QueryLanguage
    files: list[Path]
    scope_hash: str | None
    snapshot_digest: str
    cache: CqCacheBackend
    policy: CqCachePolicyV1


def build_neighborhood_records(request: NeighborhoodScanRequestV1) -> list[SgRecord]:
    """Build structural records from cache fragments with aggregate fallback."""
    context = _runtime_context(request)
    if not context.files:
        return []
    aggregate_records = _read_aggregate_records(context)
    if aggregate_records is not None:
        return aggregate_records
    records = _read_or_compute_fragment_records(context, run_id=request.run_id)
    _write_aggregate_records(context, records=records, run_id=request.run_id)
    return records


def _runtime_context(request: NeighborhoodScanRequestV1) -> _RuntimeContext:
    root = Path(request.root).resolve()
    language: QueryLanguage = "rust" if request.language == "rust" else "python"
    files = list_scan_files(paths=[root], root=root, globs=None, lang=language)
    scope_hash = build_scope_hash(
        {
            "scope_roots": (str(root),),
            "lang": language,
            "record_types": ("call", "def"),
        }
    )
    snapshot = build_scope_snapshot_fingerprint(
        root=root,
        files=files,
        language=language,
        scope_globs=[],
        scope_roots=[root],
    )
    return _RuntimeContext(
        root=root,
        language=language,
        files=files,
        scope_hash=scope_hash,
        snapshot_digest=snapshot.digest,
        cache=get_cq_cache_backend(root=root),
        policy=default_cache_policy(root=root),
    )


def _read_aggregate_records(context: _RuntimeContext) -> list[SgRecord] | None:
    namespace = "neighborhood_snapshot"
    if not is_namespace_cache_enabled(policy=context.policy, namespace=namespace):
        return None
    key = build_cache_key(
        namespace,
        version="v1",
        workspace=str(context.root),
        language=context.language,
        target=context.snapshot_digest,
        extras={
            "scope_hash": context.scope_hash,
            "record_types": ("call", "def"),
        },
    )
    cached = context.cache.get(key)
    record_cache_get(namespace=namespace, hit=isinstance(cached, dict), key=key)
    payload = decode_fragment_payload(cached, type_=QueryEntityScanCacheV1)
    if payload is None:
        if isinstance(cached, dict):
            record_cache_decode_failure(namespace=namespace)
        return None
    return [_cache_record_to_record(item) for item in payload.records]


def _read_or_compute_fragment_records(
    context: _RuntimeContext,
    *,
    run_id: str | None,
) -> list[SgRecord]:
    namespace = "neighborhood_fragment"
    cache_enabled = is_namespace_cache_enabled(policy=context.policy, namespace=namespace)
    fragment_request = _fragment_request(
        context=context,
        namespace=namespace,
        run_id=run_id,
    )
    entries = _fragment_entries(context)
    partition = partition_fragment_entries(
        fragment_request,
        entries,
        FragmentProbeRuntimeV1(
            cache_get=context.cache.get,
            decode=lambda payload: decode_fragment_payload(payload, type_=QueryEntityScanCacheV1),
            cache_enabled=cache_enabled,
            record_get=record_cache_get,
            record_decode_failure=record_cache_decode_failure,
        ),
    )
    records_by_rel: dict[str, list[SgRecord]] = {
        hit.entry.file: [_cache_record_to_record(item) for item in hit.payload.records]
        for hit in partition.hits
    }
    writes = _compute_fragment_writes(
        context, misses=list(partition.misses), records_by_rel=records_by_rel
    )
    persist_fragment_writes(
        fragment_request,
        writes,
        FragmentPersistRuntimeV1(
            cache_set=context.cache.set,
            encode=encode_fragment_payload,
            cache_enabled=cache_enabled,
            transact=context.cache.transact,
            record_set=record_cache_set,
        ),
    )
    return _assemble_records(context.files, context.root, records_by_rel)


def _fragment_request(
    *,
    context: _RuntimeContext,
    namespace: str,
    run_id: str | None,
) -> FragmentRequestV1:
    ttl_seconds = resolve_namespace_ttl_seconds(policy=context.policy, namespace=namespace)
    tag = resolve_write_cache_tag(
        CacheWriteTagRequestV1(
            policy=context.policy,
            workspace=str(context.root),
            language=context.language,
            namespace=namespace,
            scope_hash=context.scope_hash,
            snapshot=context.snapshot_digest,
            run_id=run_id,
        )
    )
    return FragmentRequestV1(
        namespace=namespace,
        workspace=str(context.root),
        language=context.language,
        scope_hash=context.scope_hash,
        snapshot_digest=context.snapshot_digest,
        ttl_seconds=ttl_seconds,
        tag=tag,
        run_id=run_id,
    )


def _fragment_entries(context: _RuntimeContext) -> list[FragmentEntryV1]:
    entries: list[FragmentEntryV1] = []
    for file_path in context.files:
        rel_path = _normalize_file(file_path, context.root)
        file_hash = file_content_hash(file_path).digest
        key = build_cache_key(
            "neighborhood_fragment",
            version="v1",
            workspace=str(context.root),
            language=context.language,
            target=rel_path,
            extras={
                "file_content_hash": file_hash,
                "record_types": ("call", "def"),
            },
        )
        entries.append(
            FragmentEntryV1(
                file=rel_path,
                cache_key=key,
                content_hash=file_hash,
            )
        )
    return entries


def _compute_fragment_writes(
    context: _RuntimeContext,
    *,
    misses: list[FragmentMissV1],
    records_by_rel: dict[str, list[SgRecord]],
) -> list[FragmentWriteV1]:
    if not misses:
        return []
    miss_entries = [miss.entry for miss in misses]
    miss_paths = [context.root / entry.file for entry in miss_entries]
    scanned = sg_scan(
        paths=miss_paths,
        record_types={"def", "call"},
        root=context.root,
        lang=context.language,
    )
    grouped = _group_by_rel(scanned)
    writes: list[FragmentWriteV1] = []
    for entry in miss_entries:
        records = sorted(grouped.get(entry.file, []), key=_record_sort_key)
        records_by_rel[entry.file] = records
        writes.append(
            FragmentWriteV1(
                entry=entry,
                payload=QueryEntityScanCacheV1(
                    records=[_record_to_cache_record(item) for item in records]
                ),
            )
        )
    return writes


def _write_aggregate_records(
    context: _RuntimeContext,
    *,
    records: list[SgRecord],
    run_id: str | None,
) -> None:
    namespace = "neighborhood_snapshot"
    if not is_namespace_cache_enabled(policy=context.policy, namespace=namespace):
        return
    key = build_cache_key(
        namespace,
        version="v1",
        workspace=str(context.root),
        language=context.language,
        target=context.snapshot_digest,
        extras={
            "scope_hash": context.scope_hash,
            "record_types": ("call", "def"),
        },
    )
    ttl_seconds = resolve_namespace_ttl_seconds(policy=context.policy, namespace=namespace)
    tag = resolve_write_cache_tag(
        CacheWriteTagRequestV1(
            policy=context.policy,
            workspace=str(context.root),
            language=context.language,
            namespace=namespace,
            scope_hash=context.scope_hash,
            snapshot=context.snapshot_digest,
            run_id=run_id,
        )
    )
    payload = QueryEntityScanCacheV1(records=[_record_to_cache_record(item) for item in records])
    ok = context.cache.set(key, contract_to_builtins(payload), expire=ttl_seconds, tag=tag)
    record_cache_set(namespace=namespace, ok=ok, key=key)


def _assemble_records(
    files: list[Path],
    root: Path,
    records_by_rel: dict[str, list[SgRecord]],
) -> list[SgRecord]:
    records: list[SgRecord] = []
    for file_path in sorted(files, key=lambda item: item.as_posix()):
        rel_path = _normalize_file(file_path, root)
        records.extend(records_by_rel.get(rel_path, []))
    records.sort(key=_record_sort_key)
    return records


def _normalize_file(path: Path, root: Path) -> str:
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        return path.resolve().as_posix()


def _group_by_rel(records: list[SgRecord]) -> dict[str, list[SgRecord]]:
    grouped: dict[str, list[SgRecord]] = {}
    for record in records:
        grouped.setdefault(record.file, []).append(record)
    return grouped


def _record_sort_key(record: SgRecord) -> tuple[str, int, int, str, str, str, str]:
    return (
        record.file,
        int(record.start_line),
        int(record.start_col),
        record.record,
        record.kind,
        record.rule_id,
        record.text,
    )


def _record_to_cache_record(record: SgRecord) -> SgRecordCacheV1:
    return SgRecordCacheV1(
        record=record.record,
        kind=record.kind,
        file=record.file,
        start_line=record.start_line,
        start_col=record.start_col,
        end_line=record.end_line,
        end_col=record.end_col,
        text=record.text,
        rule_id=record.rule_id,
    )


def _cache_record_to_record(payload: SgRecordCacheV1) -> SgRecord:
    return SgRecord(
        record=payload.record,
        kind=payload.kind,
        file=payload.file,
        start_line=payload.start_line,
        start_col=payload.start_col,
        end_line=payload.end_line,
        end_col=payload.end_col,
        text=payload.text,
        rule_id=payload.rule_id,
    )


__all__ = [
    "NeighborhoodScanRequestV1",
    "build_neighborhood_records",
]
