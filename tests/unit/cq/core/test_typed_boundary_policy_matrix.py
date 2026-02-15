"""Policy matrix tests for strict vs lax boundary conversion behavior."""

from __future__ import annotations

import pytest
from tools.cq.core.structs import CqStruct
from tools.cq.core.typed_boundary import BoundaryDecodeError, convert_lax, convert_strict


class _PolicyContract(CqStruct, frozen=True):
    count: int


@pytest.mark.parametrize(
    ("payload", "strict_ok", "lax_ok"),
    [
        ({"count": 1}, True, True),
        ({"count": "2"}, False, True),
        ({"count": "bad"}, False, False),
    ],
)
def test_policy_matrix(payload: dict[str, object], strict_ok: object, lax_ok: object) -> None:
    strict_expected = bool(strict_ok)
    lax_expected = bool(lax_ok)
    if strict_expected:
        assert convert_strict(payload, type_=_PolicyContract).count >= 0
    else:
        with pytest.raises(BoundaryDecodeError):
            convert_strict(payload, type_=_PolicyContract)

    if lax_expected:
        assert convert_lax(payload, type_=_PolicyContract).count >= 0
    else:
        with pytest.raises(BoundaryDecodeError):
            convert_lax(payload, type_=_PolicyContract)
