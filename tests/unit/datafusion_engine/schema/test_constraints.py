"""Tests for schema constraint helper exports."""

from __future__ import annotations

from datafusion_engine.schema import constraints


def test_constraints_exports() -> None:
    """Constraint helper module should expose expected symbols."""
    assert hasattr(constraints, "ConstraintSpec")
    assert callable(constraints.table_constraint_definitions)
