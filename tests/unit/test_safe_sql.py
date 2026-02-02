"""Tests for safe SQL planning guardrails."""

from __future__ import annotations

import pytest

from datafusion_engine.sql.guard import safe_sql
from tests.test_helpers.datafusion_runtime import df_ctx
from tests.test_helpers.optional_deps import require_datafusion

require_datafusion()


def test_safe_sql_blocks_ddl() -> None:
    """Reject DDL statements when using safe SQL planning."""
    ctx = df_ctx()
    with pytest.raises(ValueError, match=r"SQL execution failed under safe options"):
        safe_sql(ctx, "CREATE TABLE blocked AS SELECT 1")


def test_safe_sql_blocks_dml() -> None:
    """Reject DML statements when using safe SQL planning."""
    ctx = df_ctx()
    with pytest.raises(ValueError, match=r"SQL execution failed under safe options"):
        safe_sql(ctx, "INSERT INTO blocked VALUES (1)")
