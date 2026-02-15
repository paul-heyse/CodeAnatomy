"""Schema checks for tree-sitter lane payload contracts."""

from __future__ import annotations

import msgspec
from tools.cq.search.tree_sitter.contracts.lane_payloads import (
    PythonTreeSitterPayloadV1,
    RustTreeSitterPayloadV1,
)


def test_tree_sitter_lane_payload_schemas_exist() -> None:
    py_schema = msgspec.json.schema(PythonTreeSitterPayloadV1)
    rust_schema = msgspec.json.schema(RustTreeSitterPayloadV1)
    if py_schema.get("type") is not None:
        assert py_schema.get("type") == "object"
    else:
        assert "$ref" in py_schema

    if rust_schema.get("type") is not None:
        assert rust_schema.get("type") == "object"
    else:
        assert "$ref" in rust_schema
