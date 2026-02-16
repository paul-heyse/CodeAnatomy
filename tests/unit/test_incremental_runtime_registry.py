"""Unit tests for incremental runtime table registry."""

from __future__ import annotations

import inspect

import pyarrow as pa
import pytest

from datafusion_engine.session.runtime import DataFusionRuntimeProfile
from semantics.compile_context import build_semantic_execution_context
from semantics.incremental.runtime import (
    IncrementalRuntime,
    IncrementalRuntimeBuildRequest,
    TempTableRegistry,
)

EXPECTED_ROW_COUNT = 2


def test_temp_table_registry_registers_and_cleans() -> None:
    """Register temp tables and ensure they are cleaned up."""
    runtime = _runtime_or_skip()
    ctx = runtime.session_context()
    table = pa.table({"file_id": ["a", "b"], "value": [1, 2]})

    with TempTableRegistry(runtime) as registry:
        name = registry.register_table(table, prefix="temp")
        result = ctx.sql(f"SELECT COUNT(*) AS cnt FROM {name}").to_arrow_table()
        assert result["cnt"][0].as_py() == EXPECTED_ROW_COUNT

    with pytest.raises(Exception, match="not found"):
        ctx.sql(f"SELECT * FROM {name}").to_arrow_table()


def _runtime_or_skip() -> IncrementalRuntime:
    profile = DataFusionRuntimeProfile()
    runtime = IncrementalRuntime.build(
        IncrementalRuntimeBuildRequest(
            profile=profile,
            dataset_resolver=build_semantic_execution_context(
                runtime_profile=profile
            ).dataset_resolver,
        )
    )
    _ = runtime.session_context()
    return runtime


def test_incremental_runtime_build_requires_dataset_resolver() -> None:
    """Test incremental runtime build requires dataset resolver."""
    signature = inspect.signature(IncrementalRuntimeBuildRequest)
    dataset_resolver = signature.parameters["dataset_resolver"]
    assert dataset_resolver.default is inspect.Parameter.empty
