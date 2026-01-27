"""Arrow type helpers for normalize diagnostics fields."""

from __future__ import annotations

import pyarrow as pa

from arrow_utils.schema.build import list_view_type, struct_type

DIAG_TAGS_TYPE = list_view_type(pa.string(), large=True)
DIAG_DETAIL_STRUCT = struct_type(
    {
        "detail_kind": pa.string(),
        "error_type": pa.string(),
        "source": pa.string(),
        "tags": DIAG_TAGS_TYPE,
    }
)
DIAG_DETAILS_TYPE = list_view_type(DIAG_DETAIL_STRUCT, large=True)

__all__ = ["DIAG_DETAILS_TYPE", "DIAG_DETAIL_STRUCT", "DIAG_TAGS_TYPE"]
