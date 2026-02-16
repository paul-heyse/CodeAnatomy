"""Tests for consolidated contract_codec exports (absorbed from codec.py + public_serialization.py)."""
# ruff: noqa: D101, D102

from __future__ import annotations

import msgspec
from tools.cq.core.contract_codec import (
    dumps_json_value,
    loads_json_result,
    loads_json_value,
    to_public_dict,
    to_public_list,
)
from tools.cq.core.schema import CqResult, RunMeta
from tools.cq.core.structs import CqStruct, JsonScalar, JsonValue

SAMPLE_STRUCT_COUNT = 5
PUBLIC_LIST_LENGTH = 2
JSON_SCALAR_COUNT = 5


class _SampleStruct(CqStruct, frozen=True):
    name: str = "test"
    count: int = 0


class TestDumpsJsonValue:
    def test_roundtrip_dict(self) -> None:
        val = {"key": "value", "num": 42}
        encoded = dumps_json_value(val)
        decoded = loads_json_value(encoded)
        assert decoded == val

    def test_with_indent(self) -> None:
        val = {"a": 1}
        encoded = dumps_json_value(val, indent=2)
        assert "\n" in encoded

    def test_roundtrip_list(self) -> None:
        val = [1, "two", 3.0, None, True]
        encoded = dumps_json_value(val)
        decoded = loads_json_value(encoded)
        assert decoded == val


class TestLoadsJsonResult:
    def test_decode_minimal_result(self) -> None:
        result = CqResult(
            run=RunMeta(
                macro="test",
                argv=[],
                root=".",
                started_ms=0.0,
                elapsed_ms=0.0,
            )
        )
        encoded = msgspec.json.encode(result)
        decoded = loads_json_result(encoded)
        assert isinstance(decoded, CqResult)


class TestToPublicDict:
    def test_struct_to_dict(self) -> None:
        s = _SampleStruct(name="hello", count=SAMPLE_STRUCT_COUNT)
        d = to_public_dict(s)
        assert d["name"] == "hello"
        assert d["count"] == SAMPLE_STRUCT_COUNT

    def test_non_dict_raises(self) -> None:
        import pytest

        with pytest.raises(TypeError, match="Expected dict payload"):
            to_public_dict([1, 2, 3])  # type: ignore[arg-type]


class TestToPublicList:
    def test_structs_to_list(self) -> None:
        items = [_SampleStruct(name="a"), _SampleStruct(name="b")]
        result = to_public_list(items)
        assert len(result) == PUBLIC_LIST_LENGTH
        assert result[0]["name"] == "a"
        assert result[1]["name"] == "b"


class TestJsonTypeAliases:
    def test_scalar_types(self) -> None:
        scalars: list[JsonScalar] = ["str", 1, 1.0, True, None]
        assert len(scalars) == JSON_SCALAR_COUNT

    def test_nested_value(self) -> None:
        val: JsonValue = {"key": [1, "two", {"nested": None}]}
        assert isinstance(val, dict)
