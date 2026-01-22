"""Tests for SQLGlot planner DAG snapshots."""

from __future__ import annotations

from sqlglot.optimizer.canonicalize import canonicalize

from sqlglot_tools.compat import parse_one
from sqlglot_tools.optimizer import planner_dag_snapshot, register_datafusion_dialect


def test_planner_dag_snapshot_stable_under_canonicalization() -> None:
    """Keep planner DAG hashes stable after canonicalization."""
    register_datafusion_dialect("datafusion_ext")
    expr = parse_one("SELECT a FROM t", dialect="datafusion_ext")
    snapshot = planner_dag_snapshot(expr, dialect="datafusion_ext")
    canonical = canonicalize(expr, dialect="datafusion_ext")
    canonical_snapshot = planner_dag_snapshot(canonical, dialect="datafusion_ext")
    assert snapshot.dag_hash == canonical_snapshot.dag_hash
    assert snapshot.steps
