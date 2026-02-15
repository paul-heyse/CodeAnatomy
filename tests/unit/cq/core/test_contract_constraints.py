"""Tests for contract constraint enforcement helpers."""

from __future__ import annotations

import pytest
from tools.cq.core.contracts_constraints import (
    ContractConstraintPolicyV1,
    enforce_mapping_constraints,
)


def test_enforce_mapping_constraints_allows_valid_payload() -> None:
    enforce_mapping_constraints({"ok": "value"})


def test_enforce_mapping_constraints_rejects_long_keys() -> None:
    policy = ContractConstraintPolicyV1(max_key_length=4)
    with pytest.raises(ValueError, match="max_key_length"):
        enforce_mapping_constraints({"toolong": "x"}, policy=policy)
