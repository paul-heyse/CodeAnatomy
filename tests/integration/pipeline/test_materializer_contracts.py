"""Integration tests for Hamilton materializer contracts.

Tests the boundary where materializers return metadata dicts conforming to
expected payload structures for Delta Lake output validation.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import Mock

import pyarrow as pa
import pytest

from hamilton_pipeline.io_contracts import (
    OutputPlanContext,
    OutputRuntimeContext,
    delta_output_specs,
    validate_delta_output_payload,
)
from hamilton_pipeline.materializers import (
    ParamTableSummarySaver,
    TableSummarySaver,
    build_hamilton_materializers,
)
from hamilton_pipeline.types import CacheRuntimeContext, OutputConfig


@pytest.fixture
def mock_output_runtime_context(tmp_path: Path) -> OutputRuntimeContext:
    """Create a mock OutputRuntimeContext for testing.

    Parameters
    ----------
    tmp_path
        Pytest temporary directory fixture.

    Returns:
    -------
    OutputRuntimeContext
        Mock runtime context with minimal configuration.
    """
    from engine.runtime_profile import RuntimeProfileSpec

    runtime_profile = Mock(spec=RuntimeProfileSpec)
    runtime_profile.name = "test_profile"

    output_config = OutputConfig(
        output_dir=str(tmp_path / "output"),
        work_dir=str(tmp_path / "work"),
        overwrite_intermediate_datasets=False,
    )

    cache_context = CacheRuntimeContext(
        cache_path=None,
        cache_log_dir=None,
        cache_log_glob=None,
        cache_policy_profile=None,
        cache_log_enabled=False,
    )

    return OutputRuntimeContext(
        runtime_profile_spec=runtime_profile,
        output_config=output_config,
        cache_context=cache_context,
    )


@pytest.fixture
def mock_output_plan_context() -> OutputPlanContext:
    """Create a mock OutputPlanContext for testing.

    Returns:
    -------
    OutputPlanContext
        Mock plan context with minimal metadata.
    """
    return OutputPlanContext(
        plan_signature="test_plan_sig",
        plan_fingerprints={"test_dataset": "fp_abc123"},
        plan_bundles_by_task={},
        run_id="test_run_id",
        artifact_ids={},
        materialized_outputs=None,
    )


@pytest.fixture
def sample_arrow_table() -> pa.Table:
    """Create a sample Arrow table for testing.

    Returns:
    -------
    pa.Table
        Sample table with 100 rows and 3 columns.
    """
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
            pa.field("value", pa.float64()),
        ]
    )
    data = {
        "id": list(range(100)),
        "name": [f"name_{i}" for i in range(100)],
        "value": [float(i * 1.5) for i in range(100)],
    }
    return pa.Table.from_pydict(data, schema=schema)


@pytest.mark.integration
def test_table_summary_saver_metadata_payload(
    mock_output_runtime_context: OutputRuntimeContext,
    mock_output_plan_context: OutputPlanContext,
    sample_arrow_table: pa.Table,
) -> None:
    """Call save_data() with Arrow table and verify metadata dict has all required keys.

    The metadata payload must include: dataset_name, materialization,
    materialized_name, path, rows, columns, column_count, plan_signature, run_id.
    """
    saver = TableSummarySaver(
        output_runtime_context=mock_output_runtime_context,
        output_plan_context=mock_output_plan_context,
        dataset_name="test_dataset",
        materialized_name="test.table_v1",
        materialization="delta",
    )

    metadata = saver.save_data(sample_arrow_table)

    assert isinstance(metadata, dict), "save_data should return a dict"

    required_keys = {
        "dataset_name",
        "materialization",
        "materialized_name",
        "path",
        "rows",
        "columns",
        "column_count",
        "plan_signature",
        "run_id",
    }
    assert required_keys.issubset(metadata.keys()), (
        f"Metadata missing required keys: {required_keys - metadata.keys()}"
    )

    assert metadata["dataset_name"] == "test_dataset"
    assert metadata["materialization"] == "delta"
    assert metadata["materialized_name"] == "test.table_v1"
    assert metadata["plan_signature"] == "test_plan_sig"
    assert metadata["run_id"] == "test_run_id"
    assert isinstance(metadata["rows"], int)
    assert isinstance(metadata["columns"], list)
    assert isinstance(metadata["column_count"], int)


@pytest.mark.integration
def test_table_summary_saver_row_count_accuracy(
    mock_output_runtime_context: OutputRuntimeContext,
    mock_output_plan_context: OutputPlanContext,
    sample_arrow_table: pa.Table,
) -> None:
    """Pass 100-row table and verify rows field equals 100."""
    saver = TableSummarySaver(
        output_runtime_context=mock_output_runtime_context,
        output_plan_context=mock_output_plan_context,
        dataset_name="test_dataset",
        materialized_name="test.table_v1",
        materialization="delta",
    )

    metadata = saver.save_data(sample_arrow_table)

    assert metadata["rows"] == 100, "Row count should match table size"


@pytest.mark.integration
def test_param_table_summary_saver_mapping_required(
    mock_output_runtime_context: OutputRuntimeContext,
) -> None:
    """Pass non-mapping to ParamTableSummarySaver.save_data() and verify TypeError."""
    saver = ParamTableSummarySaver(
        output_runtime_context=mock_output_runtime_context,
        materialization="delta",
    )

    with pytest.raises(TypeError, match="expected a mapping"):
        saver.save_data("not_a_mapping")


@pytest.mark.integration
def test_materializer_factory_count() -> None:
    """Verify build_hamilton_materializers() returns exactly 7 factories.

    6 TableSummarySaver factories (one per delta output spec) + 1 ParamTableSummarySaver.
    """
    factories = build_hamilton_materializers()

    assert len(factories) == 7, (
        f"Expected 7 materializer factories (6 table + 1 param), got {len(factories)}"
    )


@pytest.mark.integration
def test_materializer_factory_wiring_against_delta_specs() -> None:
    """Verify each TableSummarySaver factory references a valid delta_output_specs() entry.

    All 6 factory dataset_name values should match a spec in delta_output_specs().
    """
    _ = build_hamilton_materializers()
    specs = delta_output_specs()
    spec_dataset_names = {spec.dataset_name for spec in specs}

    assert len(specs) == 6, f"Expected 6 delta output specs, got {len(specs)}"

    expected_datasets = {
        "cpg_nodes",
        "cpg_edges",
        "cpg_props",
        "cpg_props_map",
        "cpg_edges_by_src",
        "cpg_edges_by_dst",
    }
    assert spec_dataset_names == expected_datasets, (
        f"Delta output spec dataset names mismatch: {spec_dataset_names}"
    )


@pytest.mark.integration
def test_validate_delta_output_payload_accepts_valid() -> None:
    """Valid payload passes validate_delta_output_payload() without exception."""
    valid_payload = {
        "dataset_name": "cpg_nodes",
        "materialization": "delta",
        "materialized_name": "semantic.cpg_nodes_v1",
        "path": "/tmp/test/cpg_nodes",
        "rows": 100,
        "delta_version": 0,
    }

    validate_delta_output_payload(valid_payload, dataset_name="cpg_nodes")


@pytest.mark.integration
def test_validate_delta_output_payload_rejects_missing_key() -> None:
    """Payload missing required key raises ValueError naming the missing key."""
    invalid_payload = {
        "dataset_name": "cpg_nodes",
        "materialization": "delta",
        "materialized_name": "semantic.cpg_nodes_v1",
        "path": "/tmp/test/cpg_nodes",
        "delta_version": 0,
    }

    with pytest.raises(ValueError, match=r"missing required keys.*rows"):
        validate_delta_output_payload(invalid_payload, dataset_name="cpg_nodes")
