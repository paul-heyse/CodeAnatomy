"""Tests for metadata replace-based update patterns."""

from __future__ import annotations

from pathlib import Path


def test_metadata_with_methods_removed() -> None:
    """Legacy TableProviderMetadata with_* helpers remain removed."""
    source = Path("src/datafusion_engine/tables/metadata.py").read_text(encoding="utf-8")
    assert "def with_ddl" not in source
    assert "def with_constraints" not in source
    assert "def with_schema_identity_hash" not in source
    assert "def with_schema_adapter" not in source
