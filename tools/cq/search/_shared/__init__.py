"""Shared helpers and contracts for CQ search modules."""

from __future__ import annotations

from tools.cq.search._shared.core import (
    PythonByteRangeEnrichmentSettingsV1,
    PythonByteRangeRuntimeV1,
    PythonNodeEnrichmentSettingsV1,
    PythonNodeRuntimeV1,
    line_col_to_byte_offset,
    node_text,
    sg_node_text,
    source_hash,
    truncate,
)

__all__ = [
    "PythonByteRangeEnrichmentSettingsV1",
    "PythonByteRangeRuntimeV1",
    "PythonNodeEnrichmentSettingsV1",
    "PythonNodeRuntimeV1",
    "line_col_to_byte_offset",
    "node_text",
    "sg_node_text",
    "source_hash",
    "truncate",
]
