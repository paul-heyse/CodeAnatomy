"""Delta mutation service contract alignment tests."""

from __future__ import annotations

import pyarrow as pa
import pytest

from datafusion_engine.delta.service import DeltaMutationRequest
from storage.deltalake.delta_read import DeltaDeleteWhereRequest, DeltaMergeArrowRequest


def test_delta_mutation_request_requires_one_operation() -> None:
    """Validation should fail when neither merge nor delete payload is provided."""
    with pytest.raises(ValueError, match="requires merge or delete"):
        DeltaMutationRequest().validate()


def test_delta_mutation_request_rejects_merge_and_delete_together() -> None:
    """Validation should fail when both merge and delete payloads are set."""
    merge = DeltaMergeArrowRequest(
        path="/tmp/table",
        source=pa.table({"id": [1]}),
        predicate="target.id = source.id",
    )
    delete = DeltaDeleteWhereRequest(
        path="/tmp/table",
        predicate="id > 0",
    )
    request = DeltaMutationRequest(merge=merge, delete=delete)
    with pytest.raises(ValueError, match="must not include both"):
        request.validate()
