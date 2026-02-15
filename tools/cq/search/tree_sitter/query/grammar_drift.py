"""Grammar drift and query-pack compatibility checks."""

from __future__ import annotations

import hashlib

import msgspec

from tools.cq.search.tree_sitter.contracts.query_models import GrammarDriftReportV1
from tools.cq.search.tree_sitter.query.contract_snapshot import (
    QueryContractSnapshotV1,
    build_contract_snapshot,
    diff_snapshots,
)
from tools.cq.search.tree_sitter.query.drift_diff import GrammarDiffV1, has_breaking_changes
from tools.cq.search.tree_sitter.schema.node_schema import build_schema_index, load_grammar_schema

_LAST_CONTRACT_SNAPSHOTS: dict[str, QueryContractSnapshotV1] = {}


def _digest(parts: list[str]) -> str:
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()[:16]


def _grammar_digest(language: str) -> str | None:
    schema = load_grammar_schema(language)
    if schema is None:
        return None
    grammar_parts = [
        f"{row.type}:{int(row.named)}:{','.join(row.fields)}" for row in schema.node_types
    ]
    return _digest(grammar_parts)


def build_grammar_drift_report(
    *,
    language: str,
    query_sources: tuple[object, ...],
) -> GrammarDriftReportV1:
    """Build compatibility report between grammar schema and query-pack set."""
    schema = load_grammar_schema(language)
    query_digest = _digest(
        [
            f"{getattr(row, 'pack_name', '')!s}:{len(str(getattr(row, 'source', '')))}"
            for row in query_sources
        ]
    )
    if schema is None:
        missing_errors = ["grammar_schema_unavailable"]
        if not query_sources:
            missing_errors.append("query_pack_sources_empty")
        return GrammarDriftReportV1(
            language=language,
            grammar_digest="missing",
            query_digest=query_digest,
            compatible=False,
            errors=tuple(missing_errors),
            schema_diff=msgspec.to_builtins(GrammarDiffV1()),
        )

    schema_index = build_schema_index(schema)
    grammar_digest = _grammar_digest(language) or "missing"
    snapshot = build_contract_snapshot(
        language=language,
        schema_index=schema_index,
        query_sources=query_sources,
        grammar_digest=grammar_digest,
        query_digest=query_digest,
    )

    previous = _LAST_CONTRACT_SNAPSHOTS.get(language)
    schema_diff = diff_snapshots(previous, snapshot) if previous is not None else GrammarDiffV1()

    errors: list[str] = []
    if not query_sources:
        errors.append("query_pack_sources_empty")
    if has_breaking_changes(schema_diff):
        errors.extend(f"removed_node_kind:{name}" for name in schema_diff.removed_node_kinds[:32])
        errors.extend(f"removed_field:{name}" for name in schema_diff.removed_fields[:32])

    _LAST_CONTRACT_SNAPSHOTS[language] = snapshot
    return GrammarDriftReportV1(
        language=language,
        grammar_digest=grammar_digest,
        query_digest=query_digest,
        compatible=not errors,
        errors=tuple(errors),
        schema_diff=msgspec.to_builtins(schema_diff),
    )


def get_last_contract_snapshot(language: str) -> QueryContractSnapshotV1 | None:
    """Return last snapshot captured by ``build_grammar_drift_report``."""
    return _LAST_CONTRACT_SNAPSHOTS.get(language)


__all__ = ["build_grammar_drift_report", "get_last_contract_snapshot"]
