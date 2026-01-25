"""Unit tests for DataFusion SQL policy presets."""

from __future__ import annotations

import pytest

from datafusion_engine.compile_options import DataFusionCompileOptions, resolve_sql_policy
from datafusion_engine.execution_facade import DataFusionExecutionFacade
from datafusion_engine.runtime import DataFusionRuntimeProfile

pytest.importorskip("datafusion")


def test_sql_policy_presets() -> None:
    """Expose the expected SQL policy preset matrix."""
    read_only = resolve_sql_policy("read_only")
    service = resolve_sql_policy("service")
    admin = resolve_sql_policy("admin")
    assert not read_only.allow_ddl
    assert not read_only.allow_dml
    assert not service.allow_ddl
    assert not service.allow_dml
    assert admin.allow_ddl
    assert admin.allow_dml


def test_dml_policy_enforces_read_only() -> None:
    """Reject DML statements when read-only policy is applied."""
    ctx = DataFusionRuntimeProfile().session_context()
    facade = DataFusionExecutionFacade(ctx=ctx, runtime_profile=None)
    options = DataFusionCompileOptions(sql_policy_name="read_only")
    plan = facade.compile("INSERT INTO missing_table VALUES (1)", options=options)
    with pytest.raises(ValueError, match="policy violations"):
        facade.execute(plan)
