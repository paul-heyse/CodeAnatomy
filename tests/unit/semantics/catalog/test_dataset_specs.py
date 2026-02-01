"""Tests for semantics.catalog.dataset_specs module."""

from __future__ import annotations

import pytest

from semantics.catalog.dataset_rows import get_all_dataset_rows
from semantics.catalog.dataset_specs import (
    dataset_alias,
    dataset_name_from_alias,
    dataset_names,
    dataset_spec,
    dataset_specs,
    supports_incremental,
)


class TestDatasetSpec:
    """Tests for dataset_spec function."""

    @pytest.mark.smoke
    def test_returns_valid_spec(self) -> None:
        """dataset_spec returns a DatasetSpec for valid name."""
        all_rows = get_all_dataset_rows()
        if all_rows:
            first_row = all_rows[0]
            spec = dataset_spec(first_row.name)
            # Verify it has expected attributes
            assert hasattr(spec, "name")
            assert hasattr(spec, "schema")

    def test_raises_keyerror_for_missing(self) -> None:
        """dataset_spec raises KeyError for unknown name."""
        with pytest.raises(KeyError, match="Unknown semantic dataset"):
            dataset_spec("nonexistent_dataset_xyz_v1")

    def test_spec_has_matching_name(self) -> None:
        """dataset_spec returns spec with matching name attribute."""
        all_rows = get_all_dataset_rows()
        if all_rows:
            first_row = all_rows[0]
            spec = dataset_spec(first_row.name)
            assert spec.name == first_row.name


class TestDatasetSpecs:
    """Tests for dataset_specs function."""

    @pytest.mark.smoke
    def test_returns_iterable(self) -> None:
        """dataset_specs returns an iterable."""
        result = dataset_specs()
        # Verify it's iterable by consuming it
        specs_list = list(result)
        assert isinstance(specs_list, list)

    def test_specs_count_matches_rows(self) -> None:
        """dataset_specs count matches get_all_dataset_rows."""
        specs_list = list(dataset_specs())
        rows = get_all_dataset_rows()
        assert len(specs_list) == len(rows)


class TestDatasetNameFromAlias:
    """Tests for dataset_name_from_alias function."""

    @pytest.mark.smoke
    def test_alias_resolves_to_name(self) -> None:
        """dataset_name_from_alias resolves alias to versioned name."""
        all_rows = get_all_dataset_rows()
        if all_rows:
            first_row = all_rows[0]
            # Strip _v1 suffix to get alias
            name = first_row.name
            if "_v" in name:
                alias = name.rsplit("_v", 1)[0]
                result = dataset_name_from_alias(alias)
                assert result == name

    def test_name_returns_itself(self) -> None:
        """dataset_name_from_alias returns name if given name instead of alias."""
        all_rows = get_all_dataset_rows()
        if all_rows:
            first_row = all_rows[0]
            result = dataset_name_from_alias(first_row.name)
            assert result == first_row.name

    def test_unknown_alias_raises_keyerror(self) -> None:
        """dataset_name_from_alias raises KeyError for unknown alias."""
        with pytest.raises(KeyError, match="Unknown semantic dataset alias"):
            dataset_name_from_alias("totally_fake_alias_xyz")


class TestDatasetAlias:
    """Tests for dataset_alias function."""

    @pytest.mark.smoke
    def test_returns_alias_for_valid_name(self) -> None:
        """dataset_alias returns alias for valid dataset name."""
        all_rows = get_all_dataset_rows()
        if all_rows:
            first_row = all_rows[0]
            alias = dataset_alias(first_row.name)
            assert isinstance(alias, str)
            # Alias should not include version suffix
            assert alias == first_row.name or not alias.endswith("_v1")

    def test_alias_returns_itself(self) -> None:
        """dataset_alias returns alias if given alias instead of name."""
        all_rows = get_all_dataset_rows()
        if all_rows:
            first_row = all_rows[0]
            name = first_row.name
            if "_v" in name:
                alias = name.rsplit("_v", 1)[0]
                result = dataset_alias(alias)
                assert result == alias

    def test_unknown_name_raises_keyerror(self) -> None:
        """dataset_alias raises KeyError for unknown name."""
        with pytest.raises(KeyError, match="Unknown semantic dataset"):
            dataset_alias("nonexistent_dataset_xyz_v1")


class TestSupportsIncremental:
    """Tests for supports_incremental function."""

    @pytest.mark.smoke
    def test_returns_bool(self) -> None:
        """supports_incremental returns a boolean."""
        all_rows = get_all_dataset_rows()
        if all_rows:
            first_row = all_rows[0]
            result = supports_incremental(first_row.name)
            assert isinstance(result, bool)

    def test_reflects_cdf_and_merge_keys(self) -> None:
        """supports_incremental reflects supports_cdf and merge_keys."""
        all_rows = get_all_dataset_rows()
        for row in all_rows:
            result = supports_incremental(row.name)
            expected = row.supports_cdf and row.merge_keys is not None
            assert result == expected, f"Mismatch for {row.name}"

    def test_unknown_name_raises_keyerror(self) -> None:
        """supports_incremental raises KeyError for unknown name."""
        with pytest.raises(KeyError, match="Unknown semantic dataset"):
            supports_incremental("nonexistent_dataset_xyz_v1")


class TestDatasetNames:
    """Tests for dataset_names function in dataset_specs module."""

    @pytest.mark.smoke
    def test_returns_tuple(self) -> None:
        """dataset_names returns a tuple of strings."""
        result = dataset_names()
        assert isinstance(result, tuple)
        for name in result:
            assert isinstance(name, str)

    def test_matches_all_rows_order(self) -> None:
        """dataset_names order matches get_all_dataset_rows."""
        names = dataset_names()
        rows = get_all_dataset_rows()
        assert len(names) == len(rows)
        for name, row in zip(names, rows, strict=True):
            assert name == row.name


class TestAliasConsistency:
    """Tests for alias consistency across functions."""

    def test_roundtrip_name_to_alias_to_name(self) -> None:
        """Roundtrip from name to alias back to name works."""
        all_rows = get_all_dataset_rows()
        for row in all_rows:
            alias = dataset_alias(row.name)
            name = dataset_name_from_alias(alias)
            assert name == row.name, f"Roundtrip failed for {row.name}"

    def test_all_names_have_aliases(self) -> None:
        """All dataset names have corresponding aliases."""
        all_rows = get_all_dataset_rows()
        for row in all_rows:
            alias = dataset_alias(row.name)
            assert alias is not None
            assert isinstance(alias, str)
