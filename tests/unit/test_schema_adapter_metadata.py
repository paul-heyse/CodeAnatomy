"""Tests for schema adapter metadata tracking in TableProviderMetadata."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import FrozenInstanceError
from typing import cast

import pytest

from datafusion_engine.table_provider_metadata import TableProviderMetadata


def test_table_provider_metadata_default_schema_adapter_disabled() -> None:
    """Schema adapter should be disabled by default."""
    metadata = TableProviderMetadata(table_name="test_table")
    assert metadata.schema_adapter_enabled is False


def test_table_provider_metadata_with_schema_adapter_enabled() -> None:
    """Schema adapter can be enabled explicitly."""
    metadata = TableProviderMetadata(
        table_name="test_table",
        schema_adapter_enabled=True,
    )
    assert metadata.schema_adapter_enabled is True


def test_with_schema_adapter_method() -> None:
    """with_schema_adapter method should update the flag."""
    metadata = TableProviderMetadata(table_name="test_table")
    assert metadata.schema_adapter_enabled is False

    enabled_metadata = metadata.with_schema_adapter(enabled=True)
    assert enabled_metadata.schema_adapter_enabled is True
    assert not metadata.schema_adapter_enabled  # Original unchanged

    disabled_metadata = enabled_metadata.with_schema_adapter(enabled=False)
    assert disabled_metadata.schema_adapter_enabled is False


def test_with_schema_adapter_default_enabled() -> None:
    """with_schema_adapter should default to enabled=True."""
    metadata = TableProviderMetadata(table_name="test_table")
    enabled_metadata = metadata.with_schema_adapter()
    assert enabled_metadata.schema_adapter_enabled is True


def test_with_ddl_preserves_schema_adapter_flag() -> None:
    """with_ddl should preserve schema_adapter_enabled."""
    metadata = TableProviderMetadata(
        table_name="test_table",
        schema_adapter_enabled=True,
    )
    updated = metadata.with_ddl("CREATE EXTERNAL TABLE test_table ...")
    assert updated.schema_adapter_enabled is True


def test_with_constraints_preserves_schema_adapter_flag() -> None:
    """with_constraints should preserve schema_adapter_enabled."""
    metadata = TableProviderMetadata(
        table_name="test_table",
        schema_adapter_enabled=True,
    )
    updated = metadata.with_constraints(("constraint1",))
    assert updated.schema_adapter_enabled is True


def test_with_schema_identity_hash_preserves_schema_adapter_flag() -> None:
    """with_schema_identity_hash should preserve schema_adapter_enabled."""
    metadata = TableProviderMetadata(
        table_name="test_table",
        schema_adapter_enabled=True,
    )
    updated = metadata.with_schema_identity_hash("fingerprint123")
    assert updated.schema_adapter_enabled is True


def test_schema_adapter_metadata_immutability() -> None:
    """TableProviderMetadata should be immutable."""
    metadata = TableProviderMetadata(
        table_name="test_table",
        schema_adapter_enabled=False,
    )
    setattr_fn = cast("Callable[[str, object], None]", metadata.__setattr__)
    new_value = True
    with pytest.raises(FrozenInstanceError):
        setattr_fn("schema_adapter_enabled", new_value)
