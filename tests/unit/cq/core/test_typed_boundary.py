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

CONVERTED_VALUE = 3
JSON_VALUE = 4
TOML_VALUE = 5
YAML_VALUE = 6


class _Contract(CqStruct, frozen=True):
    value: int


def test_convert_strict_raises_boundary_error() -> None:
    """Test convert strict raises boundary error."""
    with pytest.raises(BoundaryDecodeError):
        convert_strict({"value": "nope"}, type_=_Contract)


def test_convert_lax_converts_numeric_string() -> None:
    """Test convert lax converts numeric string."""
    converted = convert_lax({"value": str(CONVERTED_VALUE)}, type_=_Contract)
    assert converted.value == CONVERTED_VALUE


def test_decode_json_toml_yaml_strict() -> None:
    """Test decode json toml yaml strict."""
    assert decode_json_strict(b'{"value": 4}', type_=_Contract).value == JSON_VALUE
    assert decode_toml_strict(b"value = 5", type_=_Contract).value == TOML_VALUE
    assert decode_yaml_strict(b"value: 6\n", type_=_Contract).value == YAML_VALUE
