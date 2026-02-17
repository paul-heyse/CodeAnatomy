"""Tests for Delta read module surface."""

from __future__ import annotations

from storage.deltalake import delta_read


def test_delta_data_checker_not_exported() -> None:
    """Legacy delta_read helper symbols remain removed from public API."""
    assert "delta_data_checker" not in delta_read.__all__
    assert "build_commit_properties" not in delta_read.__all__
    assert "idempotent_commit_properties" not in delta_read.__all__
    assert "canonical_table_uri" not in delta_read.__all__
    assert "delta_table_schema" not in delta_read.__all__
    assert "snapshot_key_for_table" not in delta_read.__all__
    assert "vacuum_delta" not in delta_read.__all__
    assert "create_delta_checkpoint" not in delta_read.__all__
    assert "cleanup_delta_log" not in delta_read.__all__
