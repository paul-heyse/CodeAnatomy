"""Unit tests for schema contract builder helpers."""

from __future__ import annotations

import pyarrow as pa

from datafusion_engine.schema.contract_builders import (
    decode_field_metadata,
    field_spec_from_arrow_field,
)


def test_decode_field_metadata_decodes_utf8_pairs() -> None:
    """Decode byte metadata pairs into UTF-8 strings."""
    payload = {b"encoding": b"dictionary", b"default_value": b"0"}
    assert decode_field_metadata(payload) == {"encoding": "dictionary", "default_value": "0"}


def test_field_spec_from_arrow_field_populates_encoding() -> None:
    """Populate encoding and default metadata on field-spec conversion."""
    field = pa.field(
        "value",
        pa.string(),
        metadata={b"encoding": b"dictionary", b"default_value": b"na"},
    )
    spec = field_spec_from_arrow_field(field)

    assert spec.name == "value"
    assert spec.encoding == "dictionary"
    assert spec.default_value == "na"
