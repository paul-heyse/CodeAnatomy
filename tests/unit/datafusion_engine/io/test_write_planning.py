"""Tests for write planning partition normalization helper."""

from __future__ import annotations

from datafusion_engine.io.write_planning import resolved_partition_by


def test_resolved_partition_by_normalizes_sequence() -> None:
    """Partition-by options are normalized to canonical tuple form."""
    assert resolved_partition_by(["a", "b"]) == ("a", "b")
    assert resolved_partition_by(None) == ()
