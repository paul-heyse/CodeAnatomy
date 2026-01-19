"""Unit tests for SQLGlot transform lanes."""

from __future__ import annotations

from sqlglot.transforms import (
    eliminate_full_outer_join,
    eliminate_semi_and_anti_joins,
    explode_projection_to_unnest,
)

from sqlglot_tools.optimizer import default_sqlglot_policy


def test_transform_lane_includes_join_and_explode_rewrites() -> None:
    """Ensure default policy includes rewrite transforms."""
    policy = default_sqlglot_policy()
    transforms = policy.transforms
    assert eliminate_full_outer_join in transforms
    assert eliminate_semi_and_anti_joins in transforms
    assert explode_projection_to_unnest in transforms
