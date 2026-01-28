"""Tests for safe SQL planning guardrails."""

from __future__ import annotations

import pytest

from datafusion_engine.runtime import DataFusionRuntimeProfile
from datafusion_engine.sql_guard import safe_sql

pytest.importorskip("datafusion")


def test_safe_sql_blocks_ddl() -> None:
    """Reject DDL statements when using safe SQL planning."""
    ctx = DataFusionRuntimeProfile().session_context()
    with pytest.raises(ValueError, match=r"SQL execution failed under safe options\."):
        safe_sql(ctx, "CREATE TABLE blocked AS SELECT 1")


def test_safe_sql_blocks_dml() -> None:
    """Reject DML statements when using safe SQL planning."""
    ctx = DataFusionRuntimeProfile().session_context()
    with pytest.raises(ValueError, match=r"SQL execution failed under safe options\."):
        safe_sql(ctx, "INSERT INTO blocked VALUES (1)")
