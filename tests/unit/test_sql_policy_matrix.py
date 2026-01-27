"""Unit tests for DataFusion SQL policy presets."""

from __future__ import annotations

import pytest

from datafusion_engine.compile_options import resolve_sql_policy
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
    policy = resolve_sql_policy("read_only")
    options = policy.to_sql_options()
    with pytest.raises(RuntimeError, match=r".*"):
        ctx.sql_with_options("INSERT INTO missing_table VALUES (1)", options).collect()
