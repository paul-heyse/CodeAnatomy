"""Tests for query contract snapshot helpers."""

from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace

from tools.cq.search.tree_sitter.query.contract_snapshot import (
    build_contract_snapshot,
    diff_snapshots,
)


@dataclass(frozen=True)
class _Source:
    pack_name: str
    source: str


def test_build_contract_snapshot_normalizes_schema_and_pack_rows() -> None:
    schema_index = SimpleNamespace(
        all_node_kinds={"function_definition", "class_definition"},
        field_names={"name", "body"},
    )
    snapshot = build_contract_snapshot(
        language="python",
        schema_index=schema_index,
        query_sources=(_Source("00_defs.scm", "(function_definition) @name"),),
        grammar_digest="abc123",
    )

    assert snapshot.language == "python"
    assert snapshot.grammar_digest == "abc123"
    assert snapshot.all_node_kinds == ("class_definition", "function_definition")
    assert snapshot.field_names == ("body", "name")
    assert snapshot.packs
    assert snapshot.packs[0].pack_name == "00_defs.scm"


def test_diff_snapshots_reports_schema_removals() -> None:
    before = build_contract_snapshot(
        language="rust",
        schema_index=SimpleNamespace(
            all_node_kinds={"mod_item", "trait_item"}, field_names={"name"}
        ),
        query_sources=(_Source("50_modules_imports.scm", "(mod_item) @name"),),
        grammar_digest="old",
    )
    after = build_contract_snapshot(
        language="rust",
        schema_index=SimpleNamespace(all_node_kinds={"mod_item"}, field_names={"name", "path"}),
        query_sources=(_Source("50_modules_imports.scm", "(mod_item) @name"),),
        grammar_digest="new",
    )

    diff = diff_snapshots(before, after)

    assert diff.removed_node_kinds == ("trait_item",)
    assert diff.added_fields == ("path",)
