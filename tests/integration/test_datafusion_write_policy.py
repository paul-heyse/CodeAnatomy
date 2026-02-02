"""Integration tests for DataFusion write policy handling."""

from __future__ import annotations

import pytest

from datafusion_engine.plan.execution import datafusion_write_options
from schema_spec.policies import DataFusionWritePolicy
from tests.test_helpers.optional_deps import require_datafusion_udfs

datafusion = require_datafusion_udfs()


@pytest.mark.integration
def test_datafusion_write_policy_defaults() -> None:
    """Construct DataFusion write options from defaults."""
    policy = DataFusionWritePolicy()
    write_options = datafusion_write_options(policy)
    assert isinstance(write_options, datafusion.DataFrameWriteOptions)
    assert policy.payload()["partition_by"] == []
