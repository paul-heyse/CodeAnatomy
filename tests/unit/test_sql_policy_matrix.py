"""Unit tests for DataFusion SQL policy presets."""

from __future__ import annotations

import msgspec
import pytest

from datafusion_engine.compile.options import resolve_sql_policy
from datafusion_engine.sql.guard import safe_sql
from datafusion_engine.sql.options import planning_sql_options, statement_sql_options_for_profile
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
    profile = msgspec.structs.replace(
        base,
        policies=msgspec.structs.replace(base.policies, sql_policy_name="read_only"),
    )
    ctx = profile.session_context()
    with pytest.raises(PermissionError, match=r"DML is blocked by SQL policy"):
        safe_sql(ctx, "INSERT INTO missing_table VALUES (1)", runtime_profile=profile)


def test_statement_sql_options_respect_runtime_service_policy() -> None:
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile, PolicyBundleConfig

    profile = DataFusionRuntimeProfile(
        policies=PolicyBundleConfig(sql_policy_name="service"),
    )
    ctx = profile.session_context()
    admin_options = resolve_sql_policy("admin").to_sql_options()
    ctx.sql_with_options("CREATE TABLE policy_gate(a INT)", admin_options).to_arrow_table()

    with pytest.raises(Exception, match="DML not supported"):
        ctx.sql_with_options(
            "INSERT INTO policy_gate VALUES (1)",
            statement_sql_options_for_profile(profile),
        ).to_arrow_table()
    with pytest.raises(Exception, match="DDL not supported"):
        ctx.sql_with_options(
            "CREATE TABLE policy_gate_blocked(a INT)",
            planning_sql_options(profile),
        ).to_arrow_table()
