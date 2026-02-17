"""Tests for LDMD collapse policy inversion helpers."""

from __future__ import annotations

from tools.cq.ldmd.collapse_policy import LdmdCollapsePolicyV1


def test_default_policy_keeps_core_sections_expanded() -> None:
    """Default policy should keep critical sections uncollapsed."""
    policy = LdmdCollapsePolicyV1.default()
    assert policy.is_collapsed("target_tldr") is False
    assert policy.is_collapsed("neighborhood_summary") is False


def test_dynamic_threshold_sections_collapse_only_above_threshold() -> None:
    """Dynamic thresholds should gate collapse behavior by total row count."""
    policy = LdmdCollapsePolicyV1.default()
    assert policy.is_collapsed("parents", total=2) is False
    assert policy.is_collapsed("parents", total=4) is True
