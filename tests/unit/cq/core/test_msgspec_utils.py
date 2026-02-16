"""Tests for tools.cq.core.msgspec_utils."""

from __future__ import annotations

import msgspec
import pytest
from tools.cq.core.msgspec_utils import (
    decode_raw_json_blob,
    struct_field_names,
    union_schema_summary,
)

UNION_VARIANT_COUNT = 2


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

    @staticmethod
    def test_returns_field_names() -> None:
        """Verify field names are returned in declaration order."""
        names = struct_field_names(SampleStruct)
        assert names == ("name", "value", "tags")

    @staticmethod
    def test_empty_struct() -> None:
        """Verify empty struct returns empty tuple."""

        class Empty(msgspec.Struct, frozen=True):
            pass

        names = struct_field_names(Empty)
        assert names == ()


class TestUnionSchemaSummary:
    """Test union_schema_summary function."""

    @staticmethod
    def test_summary_variant_count() -> None:
        """Verify variant count matches number of types."""
        summary = union_schema_summary((SampleStruct, OtherStruct))
        assert summary["variant_count"] == UNION_VARIANT_COUNT

    @staticmethod
    def test_summary_has_variants_key() -> None:
        """Verify summary includes variants list."""
        summary = union_schema_summary((SampleStruct,))
        assert "variants" in summary
        assert isinstance(summary["variants"], list)


class TestDecodeRawJsonBlob:
    """Test decode_raw_json_blob function."""

    @staticmethod
    def test_decode_returns_raw() -> None:
        """Verify valid JSON decodes to Raw type."""
        data = b'{"key": "value"}'
        result = decode_raw_json_blob(data)
        assert isinstance(result, msgspec.Raw)

    @staticmethod
    def test_decode_invalid_json_raises() -> None:
        """Verify invalid JSON raises DecodeError."""
        with pytest.raises(msgspec.DecodeError):
            decode_raw_json_blob(b"not json{{{")
