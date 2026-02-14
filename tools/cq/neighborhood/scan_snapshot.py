"""Public scan snapshot adapter for structural neighborhood collection.

Decouples structural collector from private query/executor internals.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tools.cq.query.executor import ScanContext

import msgspec

from tools.cq.astgrep.sgpy_scanner import SgRecord
from tools.cq.core.cache import (
    build_cache_key,
    build_scope_hash,
    build_scope_snapshot_fingerprint,
    default_cache_policy,
    file_content_hash,
    get_cq_cache_backend,
    is_namespace_cache_enabled,
    record_cache_decode_failure,
    record_cache_get,
    record_cache_set,
    resolve_namespace_ttl_seconds,
    resolve_write_cache_tag,
)
from tools.cq.core.cache.contracts import QueryEntityScanCacheV1, SgRecordCacheV1
from tools.cq.core.contracts import contract_to_builtins
from tools.cq.core.structs import CqStruct
from tools.cq.query.sg_parser import list_scan_files, sg_scan


class ScanSnapshot(CqStruct, frozen=True):
    """Public adapter exposing only what neighborhood needs from ScanContext.

    Decouples structural collector from private query/executor internals.

    Parameters
    ----------
    def_records : tuple[SgRecord, ...]
        Definition records (functions, classes, etc.).
    call_records : tuple[SgRecord, ...]
        Call records.
    interval_index : object
        Interval index for containment queries (duck-typed).
    file_index : object
        Per-file interval index (duck-typed).
    calls_by_def : dict[str, tuple[SgRecord, ...]]
        Mapping from definition location to calls within that definition.
    """

    def_records: tuple[SgRecord, ...] = ()
    call_records: tuple[SgRecord, ...] = ()
    interval_index: object = None
    file_index: object = None
    calls_by_def: dict[str, tuple[SgRecord, ...]] = msgspec.field(default_factory=dict)

    @classmethod
    def from_scan_context(cls, ctx: ScanContext) -> ScanSnapshot:
        """Build snapshot from a ScanContext."""
        calls_by_def_keyed: dict[str, tuple[SgRecord, ...]] = {
            _record_key(def_rec): tuple(calls) for def_rec, calls in ctx.calls_by_def.items()
        }
        return cls(
            def_records=tuple(ctx.def_records),
            call_records=tuple(ctx.call_records),
            interval_index=ctx.interval_index,
            file_index=ctx.file_index,
            calls_by_def=calls_by_def_keyed,
        )

    @classmethod
    def from_records(cls, records: list[SgRecord]) -> ScanSnapshot:
        """Build snapshot from raw SgRecords."""
        from tools.cq.query.executor import build_scan_context

        ctx = build_scan_context(records)
        return cls.from_scan_context(ctx)

    @classmethod
    def build_from_repo(
        cls,
        root: Path,
        lang: str = "auto",
        run_id: str | None = None,
    ) -> ScanSnapshot:
        """Build snapshot from repository scan using cached fragments/snapshots."""
        from tools.cq.query.language import QueryLanguage

        resolved_root = root.resolve()
        query_lang: QueryLanguage = lang if lang in {"python", "rust"} else "python"  # type: ignore[assignment]

        files = list_scan_files(
            paths=[resolved_root], root=resolved_root, globs=None, lang=query_lang
        )
        if not files:
            return cls.from_records([])

        scope_hash = build_scope_hash(
            {
                "scope_roots": (str(resolved_root),),
                "lang": query_lang,
                "record_types": ("call", "def"),
            }
        )
        snapshot = build_scope_snapshot_fingerprint(
            root=resolved_root,
            files=files,
            language=query_lang,
            scope_globs=[],
            scope_roots=[resolved_root],
        )

        policy = default_cache_policy(root=resolved_root)
        cache = get_cq_cache_backend(root=resolved_root)

        aggregate_namespace = "neighborhood_snapshot"
        aggregate_key = build_cache_key(
            aggregate_namespace,
            version="v1",
            workspace=str(resolved_root),
            language=query_lang,
            target=snapshot.digest,
            extras={
                "scope_hash": scope_hash,
                "record_types": ("call", "def"),
            },
        )
        aggregate_cache_enabled = is_namespace_cache_enabled(
            policy=policy,
            namespace=aggregate_namespace,
        )
        if aggregate_cache_enabled:
            cached = cache.get(aggregate_key)
            record_cache_get(
                namespace=aggregate_namespace,
                hit=isinstance(cached, dict),
                key=aggregate_key,
            )
            if isinstance(cached, dict):
                try:
                    payload = msgspec.convert(cached, type=QueryEntityScanCacheV1)
                    records = [_cache_record_to_record(item) for item in payload.records]
                    return cls.from_records(records)
                except (RuntimeError, TypeError, ValueError):
                    record_cache_decode_failure(namespace=aggregate_namespace)

        fragment_namespace = "neighborhood_fragment"
        fragment_cache_enabled = is_namespace_cache_enabled(
            policy=policy, namespace=fragment_namespace
        )
        fragment_ttl = resolve_namespace_ttl_seconds(policy=policy, namespace=fragment_namespace)
        fragment_tag = resolve_write_cache_tag(
            policy=policy,
            workspace=str(resolved_root),
            language=query_lang,
            namespace=fragment_namespace,
            scope_hash=scope_hash,
            snapshot=snapshot.digest,
            run_id=run_id,
        )

        records_by_rel: dict[str, list[SgRecord]] = {}
        misses: list[tuple[Path, str, str]] = []

        for file_path in files:
            rel_path = _normalize_file(file_path, resolved_root)
            file_hash = file_content_hash(file_path).digest
            fragment_key = build_cache_key(
                fragment_namespace,
                version="v1",
                workspace=str(resolved_root),
                language=query_lang,
                target=rel_path,
                extras={
                    "file_content_hash": file_hash,
                    "record_types": ("call", "def"),
                },
            )
            if fragment_cache_enabled and file_hash:
                cached = cache.get(fragment_key)
                record_cache_get(
                    namespace=fragment_namespace,
                    hit=isinstance(cached, dict),
                    key=fragment_key,
                )
                if isinstance(cached, dict):
                    try:
                        payload = msgspec.convert(cached, type=QueryEntityScanCacheV1)
                        records_by_rel[rel_path] = [
                            _cache_record_to_record(item) for item in payload.records
                        ]
                        continue
                    except (RuntimeError, TypeError, ValueError):
                        record_cache_decode_failure(namespace=fragment_namespace)
            misses.append((file_path, rel_path, fragment_key))

        if misses:
            scanned = sg_scan(
                paths=[item[0] for item in misses],
                record_types={"def", "call"},
                root=resolved_root,
                lang=query_lang,
            )
            grouped = _group_by_rel(scanned)
            with cache.transact():
                for _file_path, rel_path, fragment_key in misses:
                    records = sorted(grouped.get(rel_path, []), key=_record_sort_key)
                    records_by_rel[rel_path] = records
                    if not fragment_cache_enabled:
                        continue
                    payload = QueryEntityScanCacheV1(
                        records=[_record_to_cache_record(item) for item in records]
                    )
                    ok = cache.set(
                        fragment_key,
                        contract_to_builtins(payload),
                        expire=fragment_ttl,
                        tag=fragment_tag,
                    )
                    record_cache_set(namespace=fragment_namespace, ok=ok, key=fragment_key)

        records: list[SgRecord] = []
        for file_path in sorted(files, key=lambda item: item.as_posix()):
            rel_path = _normalize_file(file_path, resolved_root)
            records.extend(records_by_rel.get(rel_path, []))
        records.sort(key=_record_sort_key)

        if aggregate_cache_enabled:
            aggregate_ttl = resolve_namespace_ttl_seconds(
                policy=policy, namespace=aggregate_namespace
            )
            aggregate_tag = resolve_write_cache_tag(
                policy=policy,
                workspace=str(resolved_root),
                language=query_lang,
                namespace=aggregate_namespace,
                scope_hash=scope_hash,
                snapshot=snapshot.digest,
                run_id=run_id,
            )
            payload = QueryEntityScanCacheV1(
                records=[_record_to_cache_record(item) for item in records]
            )
            ok = cache.set(
                aggregate_key,
                contract_to_builtins(payload),
                expire=aggregate_ttl,
                tag=aggregate_tag,
            )
            record_cache_set(namespace=aggregate_namespace, ok=ok, key=aggregate_key)

        return cls.from_records(records)


def _record_key(record: SgRecord) -> str:
    """Build a deterministic key for a record."""
    return f"{record.file}:{record.start_line}:{record.start_col}"


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


__all__ = ["ScanSnapshot"]
