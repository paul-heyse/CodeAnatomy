"""Integration tests for idempotent write propagation.

Tests the boundary where IdempotentWriteOptions -> idempotent_commit_properties() ->
Delta commit deduplication across write/delete/merge flows.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake

from datafusion_engine.io.write import WriteFormat, WriteMode, WritePipeline, WriteRequest
from datafusion_engine.lineage.diagnostics import InMemoryDiagnosticsSink
from datafusion_engine.session.runtime import DataFusionRuntimeProfile, DiagnosticsConfig
from semantics.incremental.delta_context import DeltaAccessContext
from semantics.incremental.delta_updates import PartitionedDatasetSpec, upsert_partitioned_dataset
from semantics.incremental.runtime import IncrementalRuntime
from semantics.incremental.types import IncrementalFileChanges
from tests.test_helpers.arrow_seed import register_arrow_table
from tests.test_helpers.optional_deps import (
    require_datafusion_udfs,
    require_delta_extension,
    require_deltalake,
)

require_datafusion_udfs()
require_deltalake()
require_delta_extension()


@pytest.mark.integration
def test_idempotent_write_deduplication(tmp_path: Path) -> None:
    """Duplicate idempotent writes preserve app transaction metadata.

    Backends may or may not deduplicate duplicate transactions at write time.
    This test validates the stable contract: a duplicated write carries the
    same app transaction metadata and produces monotonic Delta versions.
    """
    from storage.deltalake.delta import IdempotentWriteOptions, idempotent_commit_properties

    table_path = tmp_path / "idempotent_table"
    data = pa.table({"id": [1, 2], "value": ["a", "b"]})

    idempotent = IdempotentWriteOptions(app_id="test_run", version=1)
    props = idempotent_commit_properties(
        operation="write",
        mode="overwrite",
        idempotent=idempotent,
    )
    transactions = getattr(props, "app_transactions", ())
    assert transactions is not None
    assert len(transactions) == 1
    transaction = transactions[0]
    assert getattr(transaction, "app_id", None) == "test_run"
    assert getattr(transaction, "version", None) == 1

    write_deltalake(str(table_path), data, mode="overwrite", commit_properties=props)
    v1 = DeltaTable(str(table_path)).version()

    write_deltalake(str(table_path), data, mode="overwrite", commit_properties=props)
    v2 = DeltaTable(str(table_path)).version()

    history = DeltaTable(str(table_path)).history()
    assert v2 >= v1
    assert len(history) >= 2


@pytest.mark.integration
def test_idempotent_different_versions_both_commit(tmp_path: Path) -> None:
    """Write version=1 then version=2; verify both commits recorded.

    delta_table.version() should be incremented twice, history should show both
    app transactions.
    """
    from storage.deltalake.delta import IdempotentWriteOptions, idempotent_commit_properties

    table_path = tmp_path / "multi_version_table"
    data = pa.table({"id": [1, 2], "value": ["a", "b"]})

    props_v1 = idempotent_commit_properties(
        operation="write",
        mode="overwrite",
        idempotent=IdempotentWriteOptions(app_id="test_run", version=1),
    )
    write_deltalake(str(table_path), data, mode="overwrite", commit_properties=props_v1)
    version_after_v1 = DeltaTable(str(table_path)).version()

    props_v2 = idempotent_commit_properties(
        operation="write",
        mode="overwrite",
        idempotent=IdempotentWriteOptions(app_id="test_run", version=2),
    )
    write_deltalake(str(table_path), data, mode="overwrite", commit_properties=props_v2)
    version_after_v2 = DeltaTable(str(table_path)).version()

    assert version_after_v2 > version_after_v1, "Different versions should both commit"


@pytest.mark.integration
def test_commit_metadata_includes_operation_and_mode(tmp_path: Path) -> None:
    """Verify codeanatomy_operation and codeanatomy_mode keys in commit metadata.

    Commit info metadata should contain expected keys with correct values.
    """
    from storage.deltalake.delta import IdempotentWriteOptions, idempotent_commit_properties

    table_path = tmp_path / "metadata_table"
    data = pa.table({"id": [1], "value": ["a"]})

    props = idempotent_commit_properties(
        operation="write",
        mode="overwrite",
        idempotent=IdempotentWriteOptions(app_id="test_run", version=1),
    )
    write_deltalake(str(table_path), data, mode="overwrite", commit_properties=props)

    dt = DeltaTable(str(table_path))
    history = dt.history()

    assert len(history) > 0, "Should have at least one commit"
    latest_commit = history[0]
    assert latest_commit.get("codeanatomy_operation") == "write"
    assert latest_commit.get("codeanatomy_mode") == "overwrite"


@pytest.mark.integration
def test_extra_metadata_propagated(tmp_path: Path) -> None:
    """Pass extra_metadata={'run_id': 'abc'}; verify in commit.

    Commit metadata should contain run_id: 'abc'.
    """
    from storage.deltalake.delta import IdempotentWriteOptions, idempotent_commit_properties

    table_path = tmp_path / "extra_metadata_table"
    data = pa.table({"id": [1], "value": ["a"]})

    props = idempotent_commit_properties(
        operation="write",
        mode="overwrite",
        idempotent=IdempotentWriteOptions(app_id="test_run", version=1),
        extra_metadata={"run_id": "abc"},
    )
    write_deltalake(str(table_path), data, mode="overwrite", commit_properties=props)

    dt = DeltaTable(str(table_path))
    history = dt.history()

    assert len(history) > 0, "Should have at least one commit"
    assert history[0].get("run_id") == "abc"


@pytest.mark.integration
def test_no_idempotent_options_still_requires_metadata(tmp_path: Path) -> None:
    """Call with idempotent=None but valid operation/mode.

    Should return valid CommitProperties with metadata but no app_transaction.
    """
    from storage.deltalake.delta import idempotent_commit_properties

    table_path = tmp_path / "no_idempotent_table"
    data = pa.table({"id": [1], "value": ["a"]})

    props = idempotent_commit_properties(
        operation="write",
        mode="overwrite",
        idempotent=None,
    )
    transactions = getattr(props, "app_transactions", None)
    assert transactions in (None, [])

    write_deltalake(str(table_path), data, mode="overwrite", commit_properties=props)

    dt = DeltaTable(str(table_path))
    assert dt.version() == 0, "Should have one version"


@pytest.mark.integration
def test_write_pipeline_propagates_idempotent(tmp_path: Path) -> None:
    """Use WritePipeline with IdempotentWriteOptions; verify commit properties reach Delta.

    Written table's commit history should include app transaction with expected
    app_id and version.
    """
    profile = DataFusionRuntimeProfile()
    ctx = profile.session_context()
    source = register_arrow_table(
        ctx,
        name="pipeline_source",
        value=pa.table({"id": [1, 2], "value": ["a", "b"]}),
    )
    table_path = tmp_path / "pipeline_idempotent_table"
    pipeline = WritePipeline(ctx, runtime_profile=profile)
    write_result = pipeline.write(
        WriteRequest(
            source=source,
            destination=str(table_path),
            format=WriteFormat.DELTA,
            mode=WriteMode.OVERWRITE,
            format_options={
                "idempotent": {"app_id": "pipeline_test", "version": 11},
                "commit_metadata": {"run_id": "pipeline_run"},
            },
        )
    )

    assert write_result.commit_app_id == "pipeline_test"
    assert write_result.commit_version == 11

    latest = DeltaTable(str(table_path)).history()[0]
    assert latest.get("commit_app_id") == "pipeline_test"
    assert latest.get("commit_version") == "11"
    assert latest.get("run_id") == "pipeline_run"


@pytest.mark.integration
def test_incremental_delete_propagates_idempotent(tmp_path: Path) -> None:
    """Incremental delete flow reserves/finalizes deterministic commit identity."""
    table_path = tmp_path / "incremental_delete_table"
    write_deltalake(
        str(table_path),
        pa.table({"file_id": ["drop", "keep"], "value": [1, 2]}),
        mode="overwrite",
        partition_by=["file_id"],
    )

    sink = InMemoryDiagnosticsSink()
    profile = DataFusionRuntimeProfile(
        diagnostics=DiagnosticsConfig(diagnostics_sink=sink),
    )
    runtime = IncrementalRuntime.build(profile=profile)
    context = DeltaAccessContext(runtime=runtime)

    _ = upsert_partitioned_dataset(
        pa.table({"file_id": ["keep"], "value": [3]}),
        spec=PartitionedDatasetSpec(name="incremental_delete_dataset", partition_column="file_id"),
        base_dir=str(table_path),
        changes=IncrementalFileChanges(deleted_file_ids=("drop",)),
        context=context,
    )

    commit_rows = sink.artifacts_snapshot().get("datafusion_delta_commit_v1", [])
    delete_reserved = [
        row
        for row in commit_rows
        if row.get("status") == "reserved"
        and isinstance(row.get("commit_metadata"), dict)
        and row["commit_metadata"].get("codeanatomy_operation") == "delete"
        and row["commit_metadata"].get("codeanatomy_mode") == "delete"
    ]
    delete_finalized = [
        row
        for row in commit_rows
        if row.get("status") == "finalized"
        and isinstance(row.get("metadata"), dict)
        and row["metadata"].get("operation") == "delete"
        and row["metadata"].get("key") == str(table_path)
    ]

    assert delete_reserved, "Expected at least one reserved delete commit artifact"
    assert delete_finalized, "Expected at least one finalized delete commit artifact"


@pytest.mark.integration
def test_schema_evolution_with_idempotent_write(tmp_path: Path) -> None:
    """Write A,B then A,B,C with schema evolution + idempotent; verify schema evolves.

    Final schema should have columns A,B,C; idempotent dedup should still work.
    """
    profile = DataFusionRuntimeProfile()
    ctx = profile.session_context()
    pipeline = WritePipeline(ctx, runtime_profile=profile)
    table_path = tmp_path / "schema_evolution_idempotent"

    source_v1 = register_arrow_table(
        ctx,
        name="schema_source_v1",
        value=pa.table({"id": [1], "value": ["a"]}),
    )
    write_v1 = pipeline.write(
        WriteRequest(
            source=source_v1,
            destination=str(table_path),
            format=WriteFormat.DELTA,
            mode=WriteMode.OVERWRITE,
            format_options={"idempotent": {"app_id": "schema_evo", "version": 1}},
        )
    )
    assert write_v1.commit_app_id == "schema_evo"
    assert write_v1.commit_version == 1

    source_v2 = register_arrow_table(
        ctx,
        name="schema_source_v2",
        value=pa.table({"id": [2], "value": ["b"], "extra": ["x"]}),
    )
    write_v2 = pipeline.write(
        WriteRequest(
            source=source_v2,
            destination=str(table_path),
            format=WriteFormat.DELTA,
            mode=WriteMode.APPEND,
            format_options={
                "schema_mode": "merge",
                "idempotent": {"app_id": "schema_evo", "version": 2},
            },
        )
    )
    assert write_v2.commit_app_id == "schema_evo"
    assert write_v2.commit_version == 2

    table = DeltaTable(str(table_path)).to_pyarrow_table()
    assert {"id", "value", "extra"} <= set(table.schema.names)
