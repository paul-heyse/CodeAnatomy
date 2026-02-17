"""Compatibility re-export surface for shared search modules."""

from __future__ import annotations

from tools.cq.search._shared.helpers import (
    RuntimeBoundarySummary,
    assert_no_runtime_only_keys,
    convert_from_attributes,
    decode_mapping,
    encode_mapping,
    has_runtime_only_keys,
    line_col_to_byte_offset,
    node_text,
    sg_node_text,
    source_hash,
    to_mapping_payload,
    truncate,
)
from tools.cq.search._shared.requests import (
    CandidateCollectionRequest,
    PythonByteRangeEnrichmentRequest,
    PythonByteRangeEnrichmentSettingsV1,
    PythonByteRangeRuntimeV1,
    PythonNodeEnrichmentRequest,
    PythonNodeEnrichmentSettingsV1,
    PythonNodeRuntimeV1,
    RgRunRequest,
    RustEnrichmentRequest,
)
from tools.cq.search._shared.timeouts import (
    search_async_with_timeout,
    search_sync_with_timeout,
)

__all__ = [
    "CandidateCollectionRequest",
    "PythonByteRangeEnrichmentRequest",
    "PythonByteRangeEnrichmentSettingsV1",
    "PythonByteRangeRuntimeV1",
    "PythonNodeEnrichmentRequest",
    "PythonNodeEnrichmentSettingsV1",
    "PythonNodeRuntimeV1",
    "RgRunRequest",
    "RuntimeBoundarySummary",
    "RustEnrichmentRequest",
    "assert_no_runtime_only_keys",
    "convert_from_attributes",
    "decode_mapping",
    "encode_mapping",
    "has_runtime_only_keys",
    "line_col_to_byte_offset",
    "node_text",
    "search_async_with_timeout",
    "search_sync_with_timeout",
    "sg_node_text",
    "source_hash",
    "to_mapping_payload",
    "truncate",
]
