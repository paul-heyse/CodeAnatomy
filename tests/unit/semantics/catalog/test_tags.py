"""Tests for semantic tag fallback inference."""

from __future__ import annotations

from semantics.catalog.dataset_rows import SemanticDatasetRow
from semantics.catalog.tags import _infer_entity_grain_from_metadata


def _row(*, join_keys: tuple[str, ...], fields: tuple[str, ...] = ()) -> SemanticDatasetRow:
    return SemanticDatasetRow(
        name="test_dataset_v1",
        version=1,
        bundles=(),
        fields=fields,
        category="semantic",
        join_keys=join_keys,
    )


def test_infer_entity_grain_relation_from_compatibility_groups() -> None:
    inferred = _infer_entity_grain_from_metadata(_row(join_keys=("entity_id", "symbol")))
    assert inferred == ("edge", "per_edge")


def test_infer_entity_grain_file_from_file_identity() -> None:
    inferred = _infer_entity_grain_from_metadata(_row(join_keys=("file_id",)))
    assert inferred == ("file", "per_file")


def test_infer_entity_grain_symbol_from_symbol_identity() -> None:
    inferred = _infer_entity_grain_from_metadata(_row(join_keys=("qname",)))
    assert inferred == ("symbol", "per_symbol")


def test_infer_entity_grain_entity_from_identity_group() -> None:
    inferred = _infer_entity_grain_from_metadata(_row(join_keys=("node_id",)))
    assert inferred == ("entity", "per_entity")
