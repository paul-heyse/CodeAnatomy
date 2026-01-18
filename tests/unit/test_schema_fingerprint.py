"""Schema fingerprint tests."""

from __future__ import annotations

import pyarrow as pa

from arrowdsl.schema.serialization import schema_fingerprint


def test_schema_fingerprint_changes_on_metadata() -> None:
    """Change schema metadata and verify fingerprint changes."""
    base = pa.schema([pa.field("a", pa.int64())], metadata={b"source": b"v1"})
    updated = base.with_metadata({b"source": b"v2"})
    assert schema_fingerprint(base) != schema_fingerprint(updated)


def test_schema_fingerprint_changes_on_field_metadata() -> None:
    """Change field metadata and verify fingerprint changes."""
    base_field = pa.field("a", pa.int64(), metadata={b"hint": b"one"})
    updated_field = pa.field("a", pa.int64(), metadata={b"hint": b"two"})
    base = pa.schema([base_field])
    updated = pa.schema([updated_field])
    assert schema_fingerprint(base) != schema_fingerprint(updated)
