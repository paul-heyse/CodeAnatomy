"""Tests for test_tree_sitter_cache_key_temporal_ordering."""

from __future__ import annotations

from tools.cq.utils.uuid_temporal_contracts import resolve_run_identity_contract

UUID7_VERSION = 7


def test_run_identity_temporal_ordering_is_non_decreasing() -> None:
    """Ensure run identity contracts expose non-decreasing creation timestamps."""
    rows = [resolve_run_identity_contract() for _ in range(32)]
    created = [row.run_created_ms for row in rows]
    assert all(value >= 0 for value in created)
    assert all(created[idx] <= created[idx + 1] for idx in range(len(created) - 1))
    assert all(row.run_uuid_version == UUID7_VERSION for row in rows)
