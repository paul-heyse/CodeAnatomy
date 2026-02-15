"""Tests for output contract strictness checks."""

from __future__ import annotations

import pytest
from tools.cq.core.contract_codec import require_mapping
from tools.cq.core.contracts_constraints import enforce_mapping_constraints


def test_require_mapping_enforces_mapping_shape() -> None:
    with pytest.raises(TypeError):
        require_mapping(["not", "a", "mapping"])


def test_enforce_mapping_constraints_rejects_oversized_payload() -> None:
    with pytest.raises(ValueError, match="max_key_count"):
        enforce_mapping_constraints({f"k{i}": i for i in range(20_000)})
