"""Unit tests for ordering enforcement on outputs."""

from __future__ import annotations

import pyarrow as pa
import pytest

from arrowdsl.core.context import DeterminismTier, execution_context_factory
from relspec.compiler import finalize_output_tables
from relspec.registry import ContractCatalog


def test_canonical_outputs_require_ordering() -> None:
    """Require explicit ordering metadata for canonical outputs."""
    table = pa.table({"id": [1, 2]})
    ctx = execution_context_factory("default").with_determinism(DeterminismTier.CANONICAL)
    with pytest.raises(ValueError, match="requires explicit ordering"):
        _ = finalize_output_tables(
            output_dataset="output",
            contract_name=None,
            table_parts=[table],
            ctx=ctx,
            contracts=ContractCatalog(),
        )
