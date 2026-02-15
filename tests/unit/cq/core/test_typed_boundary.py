"""Tests for shared typed boundary conversion helpers."""

from __future__ import annotations

import pytest
from tools.cq.core.structs import CqStruct
from tools.cq.core.typed_boundary import (
    BoundaryDecodeError,
    convert_lax,
    convert_strict,
    decode_json_strict,
    decode_toml_strict,
    decode_yaml_strict,
)


class _Contract(CqStruct, frozen=True):
    value: int


def test_convert_strict_raises_boundary_error() -> None:
    with pytest.raises(BoundaryDecodeError):
        convert_strict({"value": "nope"}, type_=_Contract)


def test_convert_lax_converts_numeric_string() -> None:
    converted = convert_lax({"value": "3"}, type_=_Contract)
    assert converted.value == 3


def test_decode_json_toml_yaml_strict() -> None:
    assert decode_json_strict(b'{"value": 4}', type_=_Contract).value == 4
    assert decode_toml_strict(b"value = 5", type_=_Contract).value == 5
    assert decode_yaml_strict(b"value: 6\n", type_=_Contract).value == 6
