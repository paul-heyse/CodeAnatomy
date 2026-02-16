"""Persistable query-pack/grammar contract snapshots."""

from __future__ import annotations

import hashlib

from tools.cq.core.structs import CqStruct
from tools.cq.search.tree_sitter.query.drift import GrammarDiffV1, diff_schema


class QueryPackSnapshotRowV1(CqStruct, frozen=True):
    """One query-pack snapshot row."""

    pack_name: str
    source_digest: str
    source_length: int


class QueryContractSnapshotV1(CqStruct, frozen=True):
    """Serialized snapshot of grammar + query-pack contracts."""

    language: str
    grammar_digest: str
    query_digest: str
    all_node_kinds: tuple[str, ...] = ()
    field_names: tuple[str, ...] = ()
    packs: tuple[QueryPackSnapshotRowV1, ...] = ()


class _SnapshotIndexV1(CqStruct, frozen=True):
    all_node_kinds: tuple[str, ...] = ()
    field_names: tuple[str, ...] = ()


def _digest(parts: list[str]) -> str:
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()[:16]


def _source_digest(source: str) -> str:
    return hashlib.sha256(source.encode("utf-8")).hexdigest()[:16]


def _query_pack_rows(query_sources: tuple[object, ...]) -> tuple[QueryPackSnapshotRowV1, ...]:
    rows: list[QueryPackSnapshotRowV1] = []
    for row in query_sources:
        pack_name = getattr(row, "pack_name", "")
        source = getattr(row, "source", "")
        if not isinstance(pack_name, str) or not pack_name:
            continue
        source_text = source if isinstance(source, str) else ""
        rows.append(
            QueryPackSnapshotRowV1(
                pack_name=pack_name,
                source_digest=_source_digest(source_text),
                source_length=len(source_text),
            )
        )
    rows.sort(key=lambda value: value.pack_name)
    return tuple(rows)


def _query_digest(rows: tuple[QueryPackSnapshotRowV1, ...]) -> str:
    return _digest([f"{row.pack_name}:{row.source_digest}:{row.source_length}" for row in rows])


def build_contract_snapshot(
    *,
    language: str,
    schema_index: object,
    query_sources: tuple[object, ...],
    grammar_digest: str,
    query_digest: str | None = None,
) -> QueryContractSnapshotV1:
    """Build stable contract snapshot for one language lane.

    Returns:
        QueryContractSnapshotV1: Function return value.
    """
    node_kinds = tuple(
        sorted(
            str(kind)
            for kind in getattr(schema_index, "all_node_kinds", ())
            if isinstance(kind, str)
        )
    )
    field_names = tuple(
        sorted(
            str(name) for name in getattr(schema_index, "field_names", ()) if isinstance(name, str)
        )
    )
    pack_rows = _query_pack_rows(query_sources)
    digest = (
        query_digest if isinstance(query_digest, str) and query_digest else _query_digest(pack_rows)
    )
    return QueryContractSnapshotV1(
        language=language,
        grammar_digest=grammar_digest,
        query_digest=digest,
        all_node_kinds=node_kinds,
        field_names=field_names,
        packs=pack_rows,
    )


def diff_snapshots(
    previous: QueryContractSnapshotV1,
    current: QueryContractSnapshotV1,
) -> GrammarDiffV1:
    """Diff two snapshots by node-kind and field-name sets.

    Returns:
        GrammarDiffV1: Function return value.
    """
    return diff_schema(
        _SnapshotIndexV1(
            all_node_kinds=previous.all_node_kinds,
            field_names=previous.field_names,
        ),
        _SnapshotIndexV1(
            all_node_kinds=current.all_node_kinds,
            field_names=current.field_names,
        ),
    )


__all__ = [
    "QueryContractSnapshotV1",
    "QueryPackSnapshotRowV1",
    "build_contract_snapshot",
    "diff_snapshots",
]
