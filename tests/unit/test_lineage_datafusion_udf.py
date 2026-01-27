"""Test DataFusion-native UDF extraction from logical plans."""

from __future__ import annotations

from datafusion_engine.lineage_datafusion import (
    LineageReport,
    extract_lineage_from_display,
)


def test_extract_lineage_with_scalar_udf() -> None:
    """Test UDF extraction from plan display with ScalarUDF."""
    plan_display = """
    Projection: col1, my_custom_udf(col2)
      ScalarUDF { name: "my_custom_udf", args: [col2], return_type: Int64 }
      TableScan: test_table projection=[col1, col2]
    """

    lineage = extract_lineage_from_display(plan_display)

    assert isinstance(lineage, LineageReport)
    assert "my_custom_udf" in lineage.referenced_udfs
    assert len(lineage.referenced_udfs) == 1


def test_extract_lineage_with_aggregate_udf() -> None:
    """Test UDF extraction from plan display with AggregateUDF."""
    plan_display = """
    Aggregate: groupBy=[col1], aggr=[custom_agg(col2)]
      AggregateUDF { name: "custom_agg", args: [col2], return_type: Float64 }
      TableScan: test_table projection=[col1, col2]
    """

    lineage = extract_lineage_from_display(plan_display)

    assert isinstance(lineage, LineageReport)
    assert "custom_agg" in lineage.referenced_udfs
    assert len(lineage.referenced_udfs) == 1


def test_extract_lineage_with_multiple_udfs() -> None:
    """Test UDF extraction with multiple different UDFs."""
    plan_display = """
    Projection: udf_a(col1), udf_b(col2), udf_c(col3)
      ScalarUDF { name: "udf_a", args: [col1] }
      ScalarUDF { name: "udf_b", args: [col2] }
      AggregateUDF { name: "udf_c", args: [col3] }
      TableScan: test_table projection=[col1, col2, col3]
    """

    lineage = extract_lineage_from_display(plan_display)

    assert isinstance(lineage, LineageReport)
    # Should be sorted and unique
    assert lineage.referenced_udfs == ("udf_a", "udf_b", "udf_c")


def test_extract_lineage_with_single_quotes() -> None:
    """Test UDF extraction handles single-quoted names."""
    plan_display = """
    Projection: my_func(col1)
      ScalarUDF { name: 'my_func', args: [col1] }
      TableScan: test_table projection=[col1]
    """

    lineage = extract_lineage_from_display(plan_display)

    assert isinstance(lineage, LineageReport)
    assert "my_func" in lineage.referenced_udfs


def test_extract_lineage_no_udfs() -> None:
    """Test UDF extraction when no UDFs are present."""
    plan_display = """
    Projection: col1, col2 + col3
      Filter: col1 > 10
        TableScan: test_table projection=[col1, col2, col3]
    """

    lineage = extract_lineage_from_display(plan_display)

    assert isinstance(lineage, LineageReport)
    assert lineage.referenced_udfs == ()


def test_extract_lineage_duplicate_udfs() -> None:
    """Test UDF extraction deduplicates repeated UDF calls."""
    plan_display = """
    Projection: my_udf(col1), my_udf(col2), my_udf(col3)
      ScalarUDF { name: "my_udf", args: [col1] }
      ScalarUDF { name: "my_udf", args: [col2] }
      ScalarUDF { name: "my_udf", args: [col3] }
      TableScan: test_table projection=[col1, col2, col3]
    """

    lineage = extract_lineage_from_display(plan_display)

    assert isinstance(lineage, LineageReport)
    # Should deduplicate to single entry
    assert lineage.referenced_udfs == ("my_udf",)


def test_extract_lineage_preserves_other_fields() -> None:
    """Test that UDF extraction doesn't break other lineage fields."""
    plan_display = """
    Projection: col1, my_udf(col2)
      ScalarUDF { name: "my_udf", args: [col2] }
      Filter: col1 > 10
        TableScan: test_table projection=[col1, col2]
    """

    lineage = extract_lineage_from_display(plan_display)

    assert isinstance(lineage, LineageReport)
    # Check UDF extraction works
    assert "my_udf" in lineage.referenced_udfs
    # Check other fields still populated
    assert len(lineage.scans) > 0
    assert lineage.scans[0].dataset_name == "test_table"
    assert len(lineage.filters) > 0
