"""Tests for relspec registry snapshots."""

from relspec.registry.snapshot import build_relspec_snapshot
from relspec.rules.cache import (
    rule_diagnostics_table_cached,
    rule_plan_signatures_cached,
    rule_registry_cached,
    rule_table_cached,
    template_diagnostics_table_cached,
    template_table_cached,
)


def test_relspec_snapshot_matches_cached_tables() -> None:
    """Ensure snapshot tables match cached registry tables."""
    snapshot = build_relspec_snapshot(rule_registry_cached())
    assert snapshot.rule_table.equals(rule_table_cached())
    assert snapshot.template_table.equals(template_table_cached())
    assert snapshot.rule_diagnostics.equals(rule_diagnostics_table_cached())
    assert snapshot.template_diagnostics.equals(template_diagnostics_table_cached())
    assert snapshot.plan_signatures == rule_plan_signatures_cached()
