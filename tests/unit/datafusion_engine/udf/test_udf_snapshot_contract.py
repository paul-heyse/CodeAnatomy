"""Unit tests for typed Rust UDF snapshot contracts."""

from __future__ import annotations

from collections.abc import Mapping

from datafusion_engine.udf.extension_runtime import validate_rust_udf_snapshot
from datafusion_engine.udf.runtime_snapshot_types import (
    RustUdfSnapshot,
    coerce_rust_udf_snapshot,
    rust_udf_snapshot_mapping,
)


def _payload() -> dict[str, object]:
    return {
        "version": 1,
        "scalar": ["stable_id"],
        "aggregate": [],
        "window": [],
        "table": [],
        "aliases": {},
        "parameter_names": {"stable_id": []},
        "volatility": {"stable_id": "stable"},
        "rewrite_tags": {},
        "signature_inputs": {"stable_id": [["Utf8"]]},
        "return_types": {"stable_id": ["Utf8"]},
        "simplify": {"stable_id": True},
        "coerce_types": {"stable_id": False},
        "short_circuits": {"stable_id": False},
        "config_defaults": {},
        "custom_udfs": ["stable_id"],
    }


def test_coerce_rust_udf_snapshot_contract() -> None:
    """Typed coercion should accept canonical snapshot payloads."""
    typed = coerce_rust_udf_snapshot(_payload())
    mapping = rust_udf_snapshot_mapping(typed)

    assert isinstance(typed, RustUdfSnapshot)
    assert typed.scalar == ("stable_id",)
    assert isinstance(mapping, Mapping)
    assert mapping["parameter_names"] == {"stable_id": ()}


def test_validate_rust_udf_snapshot_uses_typed_contract() -> None:
    """Validation should accept payloads once typed coercion succeeds."""
    validate_rust_udf_snapshot(_payload())
