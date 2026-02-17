"""Schema smoke tests for CQ msgspec contracts."""

from __future__ import annotations

import msgspec
from tools.cq.core.summary_contracts import SummaryOutputEnvelopeV1


def test_summary_envelope_schema_smoke() -> None:
    """Test summary envelope schema smoke."""
    schema = msgspec.json.schema(SummaryOutputEnvelopeV1)
    assert isinstance(schema, dict)
    if schema.get("type") is not None:
        assert schema.get("type") == "object"
    else:
        assert "$ref" in schema
