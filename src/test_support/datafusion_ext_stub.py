"""Install a minimal ``datafusion_ext`` stub for test environments."""

from __future__ import annotations

import sys
from types import ModuleType


def _expr_stub(*_args: object, **_kwargs: object) -> object:
    return object()


def _snapshot_stub(*_args: object, **_kwargs: object) -> dict[str, object]:
    return {}


def _noop(*_args: object, **_kwargs: object) -> None:
    return None


def _install_stub() -> None:
    if "datafusion_ext" in sys.modules:
        return
    stub = ModuleType("datafusion_ext")
    expr_names = (
        "arrow_metadata",
        "cdf_change_rank",
        "cdf_is_delete",
        "cdf_is_upsert",
        "col_to_byte",
        "count_distinct_agg",
        "first_value_agg",
        "lag_window",
        "last_value_agg",
        "lead_window",
        "list_compact",
        "list_extract",
        "list_unique",
        "list_unique_sorted",
        "map_entries",
        "map_extract",
        "map_get_default",
        "map_keys",
        "map_normalize",
        "map_values",
        "prefixed_hash64",
        "prefixed_hash_parts64",
        "qname_normalize",
        "row_number_window",
        "span_contains",
        "span_id",
        "span_len",
        "span_make",
        "span_overlaps",
        "stable_hash128",
        "stable_hash64",
        "stable_hash_any",
        "stable_id",
        "stable_id_parts",
        "string_agg",
        "struct_pick",
        "union_extract",
        "union_tag",
        "utf8_normalize",
        "utf8_null_if_blank",
    )
    for name in expr_names:
        stub.__dict__[name] = _expr_stub
    stub.__dict__["register_udfs"] = _noop
    stub.__dict__["registry_snapshot"] = _snapshot_stub
    stub.__dict__["udf_docs_snapshot"] = _snapshot_stub
    sys.modules[stub.__name__] = stub


_install_stub()

__all__ = ["_install_stub"]
