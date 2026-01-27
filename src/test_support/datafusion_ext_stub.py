"""Install a minimal ``datafusion_ext`` stub for test environments."""

from __future__ import annotations

import sys
from types import ModuleType


def _expr_stub(*_args: object, **_kwargs: object) -> object:
    try:
        from datafusion import lit
    except ImportError:
        return object()
    return lit(None)


def _snapshot_stub(*_args: object, **_kwargs: object) -> dict[str, object]:
    # Provide a minimal but structurally valid snapshot for strict validation
    # paths that require signature and return metadata when functions exist.
    scalar = [
        "col_to_byte",
        "prefixed_hash64",
        "prefixed_hash_parts64",
        "stable_hash64",
        "stable_hash128",
        "stable_hash_any",
        "stable_id",
        "stable_id_parts",
    ]
    signature_inputs: dict[str, list[list[str]]] = {
        "col_to_byte": [["string", "int64", "string"]],
        "prefixed_hash64": [["string", "string"]],
        "prefixed_hash_parts64": [["string", "string"]],
        "stable_hash64": [["string"]],
        "stable_hash128": [["string"]],
        "stable_hash_any": [["string"]],
        "stable_id": [["string", "string"]],
        "stable_id_parts": [["string", "string"]],
    }
    return_types: dict[str, list[str]] = {
        "col_to_byte": ["int64"],
        "prefixed_hash64": ["uint64"],
        "prefixed_hash_parts64": ["uint64"],
        "stable_hash64": ["uint64"],
        "stable_hash128": ["string"],
        "stable_hash_any": ["uint64"],
        "stable_id": ["string"],
        "stable_id_parts": ["string"],
    }
    parameter_names: dict[str, list[str]] = {
        "col_to_byte": ["line_text", "col_index", "col_unit"],
        "prefixed_hash64": ["prefix", "value"],
        "prefixed_hash_parts64": ["prefix", "value"],
        "stable_hash64": ["value"],
        "stable_hash128": ["value"],
        "stable_hash_any": ["value"],
        "stable_id": ["prefix", "value"],
        "stable_id_parts": ["prefix", "part1"],
    }
    volatility = dict.fromkeys(scalar, "immutable")
    return {
        "scalar": scalar,
        "aggregate": [],
        "window": [],
        "table": [],
        "aliases": {},
        "parameter_names": parameter_names,
        "signature_inputs": signature_inputs,
        "return_types": return_types,
        "volatility": volatility,
        "rewrite_tags": {},
        # Maintain the key expected by diagnostics tests without requiring
        # native pycapsule payloads.
        "pycapsule_udfs": [],
        # Include scalar functions as custom UDFs so catalog specs can be built.
        "custom_udfs": scalar,
    }


def _noop(*_args: object, **_kwargs: object) -> None:
    return None


def _install_stub() -> None:
    stub = sys.modules.get("datafusion_ext")
    if stub is None:
        stub = ModuleType("datafusion_ext")
        sys.modules[stub.__name__] = stub
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
        stub.__dict__.setdefault(name, _expr_stub)
    stub.__dict__.setdefault("register_udfs", _noop)
    stub.__dict__.setdefault("registry_snapshot", _snapshot_stub)
    stub.__dict__.setdefault("udf_docs_snapshot", _snapshot_stub)


_install_stub()

__all__ = ["_install_stub"]
