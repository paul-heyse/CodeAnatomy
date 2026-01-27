"""Tests for SQLGlot plan artifact collection."""

from __future__ import annotations

import ibis

from sqlglot_tools.bridge import collect_sqlglot_plan_artifacts
from sqlglot_tools.optimizer import (
    canonical_ast_fingerprint,
    resolve_sqlglot_policy,
    sqlglot_policy_snapshot_for,
)


def test_collect_sqlglot_plan_artifacts_uses_datafusion_compile_policy() -> None:
    """Ensure plan artifacts align with datafusion_compile policy defaults."""
    backend = ibis.datafusion.connect()
    table = ibis.memtable({"a": [1, 2]})
    expr = table.filter(table.a.notnull())
    artifacts = collect_sqlglot_plan_artifacts(expr, backend=backend)
    policy = resolve_sqlglot_policy(name="datafusion_compile")
    snapshot = sqlglot_policy_snapshot_for(policy)
    assert artifacts.policy_hash == snapshot.policy_hash
    assert artifacts.policy_rules_hash == snapshot.rules_hash
    expected_fingerprint = canonical_ast_fingerprint(artifacts.diagnostics.optimized)
    assert artifacts.ast_fingerprint == expected_fingerprint
