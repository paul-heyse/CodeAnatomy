"""Tests for semantics.incremental.cdf_joins merge strategies."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

import pyarrow as pa
import pytest
from datafusion import SessionContext
from datafusion.dataframe import DataFrame

from semantics.incremental.cdf_joins import (
    DEFAULT_CDF_COLUMN,
    CDFJoinSpec,
    CDFMergeStrategy,
    apply_cdf_merge,
    is_cdf_enabled,
    merge_incremental_results,
)

STRATEGY_COUNT = 4
APPEND_ROW_COUNT = 4
UPSERT_ROW_COUNT = 3
UPSERT_UPDATED_VALUE = 20
DELETE_INSERT_ROW_COUNT = 2
MERGED_ROW_COUNT = 3
UPDATED_VALUE_DEFAULT = 10


class TestCDFMergeStrategy:
    """Tests for CDFMergeStrategy enum."""

    @pytest.mark.smoke
    def test_strategy_values(self) -> None:
        """CDFMergeStrategy has expected values."""
        assert set(CDFMergeStrategy) == {
            CDFMergeStrategy.APPEND,
            CDFMergeStrategy.UPSERT,
            CDFMergeStrategy.REPLACE,
            CDFMergeStrategy.DELETE_INSERT,
        }

    def test_strategy_is_str_enum(self) -> None:
        """CDFMergeStrategy is a string enum."""
        for strategy in CDFMergeStrategy:
            assert isinstance(strategy, str)
            assert isinstance(strategy.value, str)

    def test_all_strategies_enumerated(self) -> None:
        """All expected strategies are enumerated."""
        strategies = list(CDFMergeStrategy)
        assert len(strategies) == STRATEGY_COUNT
        names = {s.name for s in strategies}
        assert names == {"APPEND", "UPSERT", "REPLACE", "DELETE_INSERT"}


class TestCDFJoinSpec:
    """Tests for CDFJoinSpec dataclass."""

    @pytest.mark.smoke
    def test_create_minimal_spec(self) -> None:
        """CDFJoinSpec can be created with minimal fields."""
        spec = CDFJoinSpec(
            left_table="left",
            right_table="right",
            output_name="output",
            key_columns=("id",),
        )
        assert spec.left_table == "left"
        assert spec.right_table == "right"
        assert spec.output_name == "output"
        assert spec.key_columns == ("id",)

    def test_default_merge_strategy(self) -> None:
        """CDFJoinSpec defaults to UPSERT strategy."""
        spec = CDFJoinSpec(
            left_table="left",
            right_table="right",
            output_name="output",
            key_columns=("id",),
        )
        assert spec.merge_strategy == CDFMergeStrategy.UPSERT

    def test_default_cdf_column(self) -> None:
        """CDFJoinSpec defaults to _change_type cdf column."""
        spec = CDFJoinSpec(
            left_table="left",
            right_table="right",
            output_name="output",
            key_columns=("id",),
        )
        assert spec.cdf_column == DEFAULT_CDF_COLUMN
        assert spec.cdf_column == "_change_type"

    def test_effective_filter_policy_default(self) -> None:
        """effective_filter_policy returns default when not specified."""
        spec = CDFJoinSpec(
            left_table="left",
            right_table="right",
            output_name="output",
            key_columns=("id",),
        )
        policy = spec.effective_filter_policy()
        assert policy is not None
        assert policy.include_insert is True
        assert policy.include_update_postimage is True
        assert policy.include_delete is False

    def test_custom_filter_policy(self) -> None:
        """CDFJoinSpec respects custom filter policy."""
        from semantics.incremental.cdf_types import CdfFilterPolicy

        custom_policy = CdfFilterPolicy.include_all()
        spec = CDFJoinSpec(
            left_table="left",
            right_table="right",
            output_name="output",
            key_columns=("id",),
            filter_policy=custom_policy,
        )
        policy = spec.effective_filter_policy()
        assert policy.include_delete is True


class TestIsCdfEnabled:
    """Tests for is_cdf_enabled function."""

    @pytest.mark.smoke
    def test_detects_cdf_column(self) -> None:
        """is_cdf_enabled returns True when CDF column present."""
        ctx = SessionContext()
        schema = pa.schema(
            [
                pa.field("id", pa.string()),
                pa.field("_change_type", pa.string()),
            ]
        )
        data = pa.table({"id": ["1"], "_change_type": ["insert"]}, schema=schema)
        ctx.register_record_batches("test", [data.to_batches()])
        df = ctx.table("test")

        assert is_cdf_enabled(df) is True

    def test_returns_false_without_cdf_column(self) -> None:
        """is_cdf_enabled returns False when CDF column absent."""
        ctx = SessionContext()
        schema = pa.schema([pa.field("id", pa.string())])
        data = pa.table({"id": ["1"]}, schema=schema)
        ctx.register_record_batches("test", [data.to_batches()])
        df = ctx.table("test")

        assert is_cdf_enabled(df) is False

    def test_custom_cdf_column_name(self) -> None:
        """is_cdf_enabled respects custom column name."""
        ctx = SessionContext()
        schema = pa.schema(
            [
                pa.field("id", pa.string()),
                pa.field("custom_change", pa.string()),
            ]
        )
        data = pa.table({"id": ["1"], "custom_change": ["insert"]}, schema=schema)
        ctx.register_record_batches("test", [data.to_batches()])
        df = ctx.table("test")

        assert is_cdf_enabled(df, cdf_column="custom_change") is True
        assert is_cdf_enabled(df, cdf_column="_change_type") is False


class TestApplyCdfMerge:
    """Tests for apply_cdf_merge function."""

    def _create_df(
        self,
        ctx: SessionContext,
        name: str,
        data: Mapping[str, Sequence[object]],
    ) -> DataFrame:
        table = pa.table(data)
        ctx.register_record_batches(name, [table.to_batches()])
        return ctx.table(name)

    def _collect_all_rows(self, df: DataFrame) -> dict[str, list[object]]:
        collected = df.collect()
        if not collected:
            return {}
        # Concatenate all batches into a single table
        table = pa.Table.from_batches(collected)
        return table.to_pydict()

    @pytest.mark.smoke
    def test_append_strategy(self) -> None:
        """APPEND strategy unions data without deduplication."""
        ctx = SessionContext()
        existing = self._create_df(ctx, "existing", {"id": ["a", "b"], "value": [1, 2]})
        new_data = self._create_df(ctx, "new_data", {"id": ["c", "d"], "value": [3, 4]})

        result = apply_cdf_merge(
            existing,
            new_data,
            key_columns=("id",),
            strategy=CDFMergeStrategy.APPEND,
        )

        rows = self._collect_all_rows(result)
        all_ids = rows.get("id", [])
        assert len(all_ids) == APPEND_ROW_COUNT

    def test_upsert_strategy(self) -> None:
        """UPSERT strategy updates existing and inserts new."""
        ctx = SessionContext()
        existing = self._create_df(ctx, "existing", {"id": ["a", "b"], "value": [1, 2]})
        new_data = self._create_df(ctx, "new_data", {"id": ["b", "c"], "value": [20, 3]})

        result = apply_cdf_merge(
            existing,
            new_data,
            key_columns=("id",),
            strategy=CDFMergeStrategy.UPSERT,
        )

        rows = self._collect_all_rows(result)
        # Should have a (original), b (updated to 20), c (new)
        assert len(rows["id"]) == UPSERT_ROW_COUNT

        # b should have value 20 (from new_data)
        b_idx = rows["id"].index("b")
        assert rows["value"][b_idx] == UPSERT_UPDATED_VALUE

    def test_delete_insert_strategy(self) -> None:
        """DELETE_INSERT strategy works like upsert."""
        ctx = SessionContext()
        existing = self._create_df(ctx, "existing", {"id": ["a", "b"], "value": [1, 2]})
        new_data = self._create_df(ctx, "new_data", {"id": ["b"], "value": [20]})

        result = apply_cdf_merge(
            existing,
            new_data,
            key_columns=("id",),
            strategy=CDFMergeStrategy.DELETE_INSERT,
        )

        rows = self._collect_all_rows(result)
        # Should have a (original) and b (replaced with 20)
        assert len(rows["id"]) == DELETE_INSERT_ROW_COUNT

    def test_replace_strategy_requires_partition_column(self) -> None:
        """REPLACE strategy requires partition_column."""
        ctx = SessionContext()
        existing = self._create_df(ctx, "existing", {"id": ["a"], "value": [1]})
        new_data = self._create_df(ctx, "new_data", {"id": ["b"], "value": [2]})

        with pytest.raises(ValueError, match="partition_column"):
            apply_cdf_merge(
                existing,
                new_data,
                key_columns=("id",),
                strategy=CDFMergeStrategy.REPLACE,
            )

    def test_replace_strategy_with_partition(self) -> None:
        """REPLACE strategy replaces all data in affected partitions."""
        ctx = SessionContext()
        existing = self._create_df(
            ctx,
            "existing",
            {"file_id": ["f1", "f1", "f2"], "id": ["a", "b", "c"], "value": [1, 2, 3]},
        )
        new_data = self._create_df(
            ctx,
            "new_data",
            {"file_id": ["f1"], "id": ["x"], "value": [10]},
        )

        result = apply_cdf_merge(
            existing,
            new_data,
            key_columns=("id",),
            strategy=CDFMergeStrategy.REPLACE,
            partition_column="file_id",
        )

        rows = self._collect_all_rows(result)
        # f1 partition replaced (a, b removed, x added)
        # f2 partition unchanged (c remains)
        assert len(rows["id"]) == DELETE_INSERT_ROW_COUNT
        assert "c" in rows["id"]
        assert "x" in rows["id"]
        assert "a" not in rows["id"]
        assert "b" not in rows["id"]


class TestMergeIncrementalResults:
    """Tests for merge_incremental_results function."""

    def _create_df(
        self,
        ctx: SessionContext,
        name: str,
        data: Mapping[str, Sequence[object]],
    ) -> DataFrame:
        table = pa.table(data)
        ctx.register_record_batches(name, [table.to_batches()])
        return ctx.table(name)

    def _collect_all_rows(self, df: DataFrame) -> dict[str, list[object]]:
        collected = df.collect()
        if not collected:
            return {}
        # Concatenate all batches into a single table
        table = pa.Table.from_batches(collected)
        return table.to_pydict()

    @pytest.mark.smoke
    def test_returns_incremental_when_base_missing(self) -> None:
        """Returns incremental data when base table does not exist."""
        ctx = SessionContext()
        incremental = self._create_df(
            ctx,
            "incremental",
            {"id": ["a"], "value": [1]},
        )

        result = merge_incremental_results(
            ctx,
            incremental_df=incremental,
            base_table="nonexistent_base",
            key_columns=("id",),
        )

        rows = self._collect_all_rows(result)
        assert len(rows.get("id", [])) == 1

    def test_merges_with_existing_base(self) -> None:
        """Merges incremental with existing base table."""
        ctx = SessionContext()

        # Create base table
        base_data = pa.table({"id": ["a", "b"], "value": [1, 2]})
        ctx.register_record_batches("base_table", [base_data.to_batches()])

        incremental = self._create_df(
            ctx,
            "incremental",
            {"id": ["b", "c"], "value": [20, 3]},
        )

        result = merge_incremental_results(
            ctx,
            incremental_df=incremental,
            base_table="base_table",
            key_columns=("id",),
        )

        rows = self._collect_all_rows(result)
        # Should have a, b (updated), c
        assert len(rows["id"]) == MERGED_ROW_COUNT

    def test_default_strategy_is_upsert(self) -> None:
        """Default merge strategy is UPSERT."""
        ctx = SessionContext()

        base_data = pa.table({"id": ["a"], "value": [1]})
        ctx.register_record_batches("base_table", [base_data.to_batches()])

        incremental = self._create_df(
            ctx,
            "incremental",
            {"id": ["a"], "value": [10]},
        )

        # Default strategy should be UPSERT
        result = merge_incremental_results(
            ctx,
            incremental_df=incremental,
            base_table="base_table",
            key_columns=("id",),
        )

        collected = result.collect()
        rows = collected[0].to_pydict()
        # a should be updated to 10
        assert rows["value"][0] == UPDATED_VALUE_DEFAULT


class TestDefaultCdfColumn:
    """Tests for DEFAULT_CDF_COLUMN constant."""

    def test_default_cdf_column_value(self) -> None:
        """DEFAULT_CDF_COLUMN has expected value."""
        assert DEFAULT_CDF_COLUMN == "_change_type"
