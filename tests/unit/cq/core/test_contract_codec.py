"""Tests for canonical CQ contract codec helpers."""

from __future__ import annotations

import pytest
from tools.cq.core.contract_codec import (
    encode_json,
    require_mapping,
    to_contract_builtins,
)
from tools.cq.core.structs import CqStruct


class _Payload(CqStruct, frozen=True):
    b: int
    a: int


def test_encode_json_is_deterministic() -> None:
    encoded = encode_json(_Payload(a=1, b=2), indent=None)
    assert encoded == '{"a":1,"b":2}'


def test_to_contract_builtins_normalizes_struct() -> None:
    payload = to_contract_builtins(_Payload(a=1, b=2))
    assert payload == {"a": 1, "b": 2}


def test_require_mapping_raises_for_non_mapping() -> None:
    with pytest.raises(TypeError, match="Expected mapping payload"):
        require_mapping(["x", "y"])
