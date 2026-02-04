"""Unit tests for DataFusion SQL policy presets."""

from __future__ import annotations

from dataclasses import replace

import pytest

from datafusion_engine.compile.options import resolve_sql_policy
from datafusion_engine.sql.guard import safe_sql
from tests.test_helpers.datafusion_runtime import df_profile
from tests.test_helpers.optional_deps import require_datafusion

require_datafusion()


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
    base = df_profile()
    profile = replace(base, policies=replace(base.policies, sql_policy_name="read_only"))
    ctx = profile.session_context()
    with pytest.raises(PermissionError, match=r"DML is blocked by SQL policy"):
        safe_sql(ctx, "INSERT INTO missing_table VALUES (1)", runtime_profile=profile)
