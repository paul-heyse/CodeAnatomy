"""Unit tests for Delta write policy helpers."""

from __future__ import annotations

from storage.deltalake.config import DeltaWritePolicy, StatsColumnsInputs, resolve_stats_columns


def test_resolve_stats_columns_override_respects_schema() -> None:
    """Override should win and filter to schema columns."""
    policy = DeltaWritePolicy(stats_policy="auto")
    inputs = StatsColumnsInputs(
        policy=policy,
        override=("id", "missing", "id"),
        schema_columns=("id", "name"),
    )
    assert resolve_stats_columns(inputs) == ("id",)


def test_resolve_stats_columns_explicit_policy() -> None:
    """Explicit policy should use configured stats columns."""
    policy = DeltaWritePolicy(stats_policy="explicit", stats_columns=("a", "b", "a"))
    inputs = StatsColumnsInputs(policy=policy, schema_columns=("a", "c"))
    assert resolve_stats_columns(inputs) == ("a",)


def test_resolve_stats_columns_auto_caps_and_orders() -> None:
    """Auto policy should merge candidates and cap to max columns."""
    policy = DeltaWritePolicy(stats_policy="auto", stats_max_columns=2)
    inputs = StatsColumnsInputs(
        policy=policy,
        partition_by=("p1",),
        zorder_by=("z1", "z2"),
        extra_candidates=("e1",),
        schema_columns=("p1", "z1", "z2", "e1"),
    )
    assert resolve_stats_columns(inputs) == ("p1", "z1")


def test_resolve_stats_columns_off_returns_none() -> None:
    """Off policy should disable stats columns."""
    policy = DeltaWritePolicy(stats_policy="off")
    inputs = StatsColumnsInputs(policy=policy)
    assert resolve_stats_columns(inputs) is None
