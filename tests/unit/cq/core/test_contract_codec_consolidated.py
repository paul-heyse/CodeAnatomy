"""Tests for consolidated contract_codec exports (absorbed from codec.py + public_serialization.py)."""

from __future__ import annotations

from typing import cast

import msgspec
from tools.cq.core.contract_codec import (
    decode_json,
    decode_json_result,
    encode_json,
    to_public_dict,
    to_public_list,
)
from tools.cq.core.schema import CqResult, RunMeta
from tools.cq.core.structs import CqStruct, JsonScalar, JsonValue

SAMPLE_STRUCT_COUNT = 5
PUBLIC_LIST_LENGTH = 2
JSON_SCALAR_COUNT = 5


class _SampleStruct(CqStruct, frozen=True):
    """Sample struct used by public-serialization tests."""

    name: str = "test"
    count: int = 0


class TestEncodeJson:
    """Tests for JSON encode/decode helpers."""

    @staticmethod
    def test_roundtrip_dict() -> None:
        """Round-trip dictionary payloads through JSON codec."""
        val = {"key": "value", "num": 42}
        encoded = encode_json(val)
        decoded = decode_json(encoded)
        assert decoded == val

    @staticmethod
    def test_with_indent() -> None:
        """Emit multi-line JSON when indent is requested."""
        val = {"a": 1}
        encoded = encode_json(val, indent=2)
        assert "\n" in encoded

    @staticmethod
    def test_roundtrip_list() -> None:
        """Round-trip list payloads through JSON codec."""
        val = [1, "two", 3.0, None, True]
        encoded = encode_json(val)
        decoded = decode_json(encoded)
        assert decoded == val


class TestDecodeJsonResult:
    """Tests for decoding CQ result payloads."""

    @staticmethod
    def test_decode_minimal_result() -> None:
        """Decode a minimal serialized CqResult payload."""
        result = CqResult(
            run=RunMeta(
                macro="test",
                argv=[],
                root=".",
                started_ms=0.0,
                elapsed_ms=0.0,
            )
        )
        encoded = encode_json(result)
        decoded = decode_json_result(encoded)
        assert isinstance(decoded, CqResult)


class TestToPublicDict:
    """Tests for public dictionary projection helpers."""

    @staticmethod
    def test_struct_to_dict() -> None:
        """Convert a struct into a public dictionary payload."""
        s = _SampleStruct(name="hello", count=SAMPLE_STRUCT_COUNT)
        d = to_public_dict(s)
        assert d["name"] == "hello"
        assert d["count"] == SAMPLE_STRUCT_COUNT

    @staticmethod
    def test_non_dict_raises() -> None:
        """Raise when public-dict helper receives non-dict payload."""
        import pytest

        with pytest.raises(TypeError, match="Expected dict payload"):
            to_public_dict(cast("msgspec.Struct", [1, 2, 3]))


class TestToPublicList:
    """Tests for public list projection helpers."""

    @staticmethod
    def test_structs_to_list() -> None:
        """Convert a sequence of structs into list payloads."""
        items = [_SampleStruct(name="a"), _SampleStruct(name="b")]
        result = to_public_list(items)
        assert len(result) == PUBLIC_LIST_LENGTH
        assert result[0]["name"] == "a"
        assert result[1]["name"] == "b"


class TestJsonTypeAliases:
    """Tests for exported JSON type aliases."""

    @staticmethod
    def test_scalar_types() -> None:
        """Accept all supported JsonScalar variants."""
        scalars: list[JsonScalar] = ["str", 1, 1.0, True, None]
        assert len(scalars) == JSON_SCALAR_COUNT

    @staticmethod
    def test_nested_value() -> None:
        """Support nested JsonValue shapes."""
        val: JsonValue = {"key": [1, "two", {"nested": None}]}
        assert isinstance(val, dict)
