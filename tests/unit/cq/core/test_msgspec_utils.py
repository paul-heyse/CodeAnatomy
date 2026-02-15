"""Tests for tools.cq.core.msgspec_utils."""

from __future__ import annotations

import msgspec
import pytest
from tools.cq.core.msgspec_utils import (
    decode_raw_json_blob,
    struct_field_names,
    union_schema_summary,
)


class SampleStruct(msgspec.Struct, frozen=True):
    """Test fixture struct with multiple fields."""

    name: str
    value: int = 0
    tags: list[str] = msgspec.field(default_factory=list)


class OtherStruct(msgspec.Struct, frozen=True):
    """Test fixture struct with single field."""

    label: str


class TestStructFieldNames:
    """Test struct_field_names function."""

    def test_returns_field_names(self) -> None:
        """Verify field names are returned in declaration order."""
        names = struct_field_names(SampleStruct)
        assert names == ("name", "value", "tags")

    def test_empty_struct(self) -> None:
        """Verify empty struct returns empty tuple."""

        class Empty(msgspec.Struct, frozen=True):
            pass

        names = struct_field_names(Empty)
        assert names == ()


class TestUnionSchemaSummary:
    """Test union_schema_summary function."""

    def test_summary_variant_count(self) -> None:
        """Verify variant count matches number of types."""
        summary = union_schema_summary((SampleStruct, OtherStruct))
        assert summary["variant_count"] == 2

    def test_summary_has_variants_key(self) -> None:
        """Verify summary includes variants list."""
        summary = union_schema_summary((SampleStruct,))
        assert "variants" in summary
        assert isinstance(summary["variants"], list)


class TestDecodeRawJsonBlob:
    """Test decode_raw_json_blob function."""

    def test_decode_returns_raw(self) -> None:
        """Verify valid JSON decodes to Raw type."""
        data = b'{"key": "value"}'
        result = decode_raw_json_blob(data)
        assert isinstance(result, msgspec.Raw)

    def test_decode_invalid_json_raises(self) -> None:
        """Verify invalid JSON raises DecodeError."""
        with pytest.raises(msgspec.DecodeError):
            decode_raw_json_blob(b"not json{{{")
