"""Tests for semantics.catalog.dataset_rows module."""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from functools import cache

import pytest

from semantics.catalog.dataset_rows import (
    SEMANTIC_SCHEMA_VERSION,
    DatasetCategory,
    SemanticDatasetRow,
    dataset_names,
    dataset_names_by_category,
    dataset_row,
    dataset_rows,
    get_all_dataset_rows,
    get_analysis_dataset_rows,
    get_diagnostic_dataset_rows,
    get_semantic_dataset_rows,
)
from semantics.ir_pipeline import build_semantic_ir

FULL_ROW_SCHEMA_VERSION = 2
MIN_ROWS_FOR_MULTI_SELECT = 2


@cache
def _ir_rows() -> tuple[SemanticDatasetRow, ...]:
    return build_semantic_ir().dataset_rows


def _ir_rows_by_category(category: DatasetCategory) -> tuple[SemanticDatasetRow, ...]:
    return tuple(row for row in _ir_rows() if row.category == category)


class TestSemanticDatasetRow:
    """Tests for SemanticDatasetRow dataclass."""

    @pytest.mark.smoke
    def test_create_minimal_row(self) -> None:
        """SemanticDatasetRow can be created with minimal fields."""
        row = SemanticDatasetRow(
            name="test_dataset_v1",
            version=1,
            bundles=("file_identity",),
            fields=("id", "name"),
            category="semantic",
        )
        assert row.name == "test_dataset_v1"
        assert row.version == 1
        assert row.bundles == ("file_identity",)
        assert row.fields == ("id", "name")
        assert row.category == "semantic"

    def test_create_row_with_all_fields(self) -> None:
        """SemanticDatasetRow can be created with all fields."""
        row = SemanticDatasetRow(
            name="full_dataset_v1",
            version=2,
            bundles=("file_identity", "span"),
            fields=("id", "value", "extra"),
            category="analysis",
            supports_cdf=False,
            partition_cols=("path",),
            merge_keys=("id", "path"),
            join_keys=("id",),
            template="test_template",
            view_builder="my_builder",
            metadata_extra={b"key": b"value"},
            register_view=False,
            source_dataset="base_dataset",
        )
        assert row.name == "full_dataset_v1"
        assert row.version == FULL_ROW_SCHEMA_VERSION
        assert row.category == "analysis"
        assert row.supports_cdf is False
        assert row.partition_cols == ("path",)
        assert row.merge_keys == ("id", "path")
        assert row.join_keys == ("id",)
        assert row.template == "test_template"
        assert row.view_builder == "my_builder"
        assert row.metadata_extra == {b"key": b"value"}
        assert row.register_view is False
        assert row.source_dataset == "base_dataset"

    def test_row_defaults(self) -> None:
        """SemanticDatasetRow has correct defaults."""
        row = SemanticDatasetRow(
            name="default_test_v1",
            version=1,
            bundles=(),
            fields=(),
            category="diagnostic",
        )
        assert row.supports_cdf is True
        assert row.partition_cols == ()
        assert row.merge_keys is None
        assert row.join_keys == ()
        assert row.template is None
        assert row.view_builder is None
        assert row.metadata_extra == {}
        assert row.register_view is True
        assert row.source_dataset is None

    def test_row_is_frozen(self) -> None:
        """SemanticDatasetRow is immutable."""
        row = SemanticDatasetRow(
            name="frozen_test_v1",
            version=1,
            bundles=(),
            fields=(),
            category="semantic",
        )
        attr_name = "name"
        with pytest.raises(FrozenInstanceError):
            setattr(row, attr_name, "modified")


class TestDatasetRowLookup:
    """Tests for dataset_row lookup function."""

    @pytest.mark.smoke
    def test_dataset_row_found(self) -> None:
        """dataset_row returns row when name exists."""
        all_rows = _ir_rows()
        if all_rows:
            first_row = all_rows[0]
            result = dataset_row(first_row.name)
            assert result is not None
            assert result.name == first_row.name

    def test_dataset_row_not_found_nonstrict(self) -> None:
        """dataset_row returns None for missing name in non-strict mode."""
        result = dataset_row("nonexistent_dataset_xyz_v1")
        assert result is None

    def test_dataset_row_not_found_strict(self) -> None:
        """dataset_row raises KeyError for missing name in strict mode."""
        with pytest.raises(KeyError, match="Dataset not found"):
            dataset_row("nonexistent_dataset_xyz_v1", strict=True)

    def test_dataset_row_strict_returns_valid(self) -> None:
        """dataset_row with strict=True returns row when found."""
        all_rows = _ir_rows()
        if all_rows:
            first_row = all_rows[0]
            result = dataset_row(first_row.name, strict=True)
            assert result.name == first_row.name


class TestDatasetRows:
    """Tests for dataset_rows bulk lookup function."""

    def test_dataset_rows_returns_matching(self) -> None:
        """dataset_rows returns matching rows."""
        all_rows = _ir_rows()
        if len(all_rows) >= MIN_ROWS_FOR_MULTI_SELECT:
            names = [all_rows[0].name, all_rows[1].name]
            result = dataset_rows(names)
            assert len(result) == MIN_ROWS_FOR_MULTI_SELECT
            assert result[0].name == names[0]
            assert result[1].name == names[1]

    def test_dataset_rows_skips_missing(self) -> None:
        """dataset_rows skips missing names."""
        all_rows = _ir_rows()
        if all_rows:
            names = [all_rows[0].name, "nonexistent_xyz_v1"]
            result = dataset_rows(names)
            assert len(result) == 1
            assert result[0].name == all_rows[0].name

    def test_dataset_rows_empty_input(self) -> None:
        """dataset_rows returns empty tuple for empty input."""
        result = dataset_rows([])
        assert result == ()


class TestGetAllDatasetRows:
    """Tests for get_all_dataset_rows function."""

    @pytest.mark.smoke
    def test_returns_tuple(self) -> None:
        """get_all_dataset_rows returns a tuple."""
        result = get_all_dataset_rows()
        assert isinstance(result, tuple)

    def test_returns_nonempty(self) -> None:
        """get_all_dataset_rows returns non-empty result."""
        result = get_all_dataset_rows()
        assert result == _ir_rows()
        assert len(result) > 0

    def test_all_items_are_semantic_dataset_rows(self) -> None:
        """All items in get_all_dataset_rows are SemanticDatasetRow."""
        result = get_all_dataset_rows()
        assert result == _ir_rows()
        for row in result:
            assert isinstance(row, SemanticDatasetRow)

    def test_caching_returns_same_instance(self) -> None:
        """get_all_dataset_rows returns cached result."""
        result1 = get_all_dataset_rows()
        result2 = get_all_dataset_rows()
        assert result1 is result2


class TestCategoryFilters:
    """Tests for category-based filtering functions."""

    def test_get_semantic_dataset_rows(self) -> None:
        """get_semantic_dataset_rows returns only semantic category."""
        result = get_semantic_dataset_rows()
        assert result == _ir_rows_by_category("semantic")
        assert isinstance(result, tuple)
        for row in result:
            assert row.category == "semantic"

    def test_get_analysis_dataset_rows(self) -> None:
        """get_analysis_dataset_rows returns only analysis category."""
        result = get_analysis_dataset_rows()
        assert result == _ir_rows_by_category("analysis")
        assert isinstance(result, tuple)
        for row in result:
            assert row.category == "analysis"

    def test_get_diagnostic_dataset_rows(self) -> None:
        """get_diagnostic_dataset_rows returns only diagnostic category."""
        result = get_diagnostic_dataset_rows()
        assert result == _ir_rows_by_category("diagnostic")
        assert isinstance(result, tuple)
        for row in result:
            assert row.category == "diagnostic"

    def test_categories_are_exhaustive(self) -> None:
        """All categories combined equal total rows."""
        all_rows = _ir_rows()
        semantic = _ir_rows_by_category("semantic")
        analysis = _ir_rows_by_category("analysis")
        diagnostic = _ir_rows_by_category("diagnostic")
        assert len(all_rows) == len(semantic) + len(analysis) + len(diagnostic)


class TestDatasetNames:
    """Tests for dataset_names function."""

    @pytest.mark.smoke
    def test_returns_tuple(self) -> None:
        """dataset_names returns a tuple."""
        result = dataset_names()
        assert isinstance(result, tuple)

    def test_returns_strings(self) -> None:
        """dataset_names returns strings."""
        result = dataset_names()
        for name in result:
            assert isinstance(name, str)

    def test_matches_all_dataset_rows(self) -> None:
        """dataset_names matches get_all_dataset_rows."""
        names = dataset_names()
        rows = _ir_rows()
        assert len(names) == len(rows)
        for name, row in zip(names, rows, strict=True):
            assert name == row.name


class TestDatasetNamesByCategory:
    """Tests for dataset_names_by_category function."""

    def test_semantic_category(self) -> None:
        """dataset_names_by_category returns semantic names."""
        names = dataset_names_by_category("semantic")
        rows = _ir_rows_by_category("semantic")
        assert len(names) == len(rows)
        for name, row in zip(names, rows, strict=True):
            assert name == row.name

    def test_analysis_category(self) -> None:
        """dataset_names_by_category returns analysis names."""
        names = dataset_names_by_category("analysis")
        rows = _ir_rows_by_category("analysis")
        assert len(names) == len(rows)

    def test_diagnostic_category(self) -> None:
        """dataset_names_by_category returns diagnostic names."""
        names = dataset_names_by_category("diagnostic")
        rows = _ir_rows_by_category("diagnostic")
        assert len(names) == len(rows)


class TestSchemaVersion:
    """Tests for schema version constant."""

    def test_semantic_schema_version_is_positive(self) -> None:
        """SEMANTIC_SCHEMA_VERSION is a positive integer."""
        assert isinstance(SEMANTIC_SCHEMA_VERSION, int)
        assert SEMANTIC_SCHEMA_VERSION > 0


class TestDatasetCategory:
    """Tests for DatasetCategory type."""

    def test_valid_categories(self) -> None:
        """Valid category values can be used."""
        categories: list[DatasetCategory] = ["semantic", "analysis", "diagnostic"]
        for cat in categories:
            row = SemanticDatasetRow(
                name=f"test_{cat}_v1",
                version=1,
                bundles=(),
                fields=(),
                category=cat,
            )
            assert row.category == cat
