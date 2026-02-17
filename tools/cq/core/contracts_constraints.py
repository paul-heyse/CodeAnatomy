"""Reusable constraints for public CQ contract payloads."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Annotated

import msgspec

from tools.cq.core.structs import CqStruct

PositiveInt = Annotated[int, msgspec.Meta(ge=1)]
NonNegativeInt = Annotated[int, msgspec.Meta(ge=0)]
NonNegativeFloat = Annotated[float, msgspec.Meta(ge=0.0)]
PositiveFloat = Annotated[float, msgspec.Meta(gt=0.0)]
NonEmptyStr = Annotated[str, msgspec.Meta(min_length=1)]
BoundedRatio = Annotated[float, msgspec.Meta(ge=0.0, le=1.0)]


class ContractConstraintPolicyV1(CqStruct, frozen=True):
    """Global constraints applied to mapping-like output contracts."""

    max_key_count: int = 10_000
    max_key_length: int = 128
    max_string_length: int = 100_000


_DEFAULT_CONTRACT_POLICY = ContractConstraintPolicyV1()


def enforce_mapping_constraints(
    payload: Mapping[str, object],
    *,
    policy: ContractConstraintPolicyV1 | None = None,
) -> None:
    """Validate mapping payload against shared contract constraints.

    Raises:
        ValueError: Raised when a contract payload field violates limits.
    """
    effective_policy = policy or _DEFAULT_CONTRACT_POLICY
    if len(payload) > effective_policy.max_key_count:
        msg = f"mapping exceeds max_key_count={effective_policy.max_key_count}"
        raise ValueError(msg)
    for key, value in payload.items():
        if len(key) > effective_policy.max_key_length:
            msg = f"key exceeds max_key_length={effective_policy.max_key_length}: {key!r}"
            raise ValueError(msg)
        if isinstance(value, str) and len(value) > effective_policy.max_string_length:
            msg = f"value exceeds max_string_length={effective_policy.max_string_length}: {key!r}"
            raise ValueError(msg)


__all__ = [
    "BoundedRatio",
    "ContractConstraintPolicyV1",
    "NonEmptyStr",
    "NonNegativeFloat",
    "NonNegativeInt",
    "PositiveFloat",
    "PositiveInt",
    "enforce_mapping_constraints",
]
