"""Tests for extension runtime snapshot validation helpers."""

from __future__ import annotations

from datafusion_engine.udf.extension_runtime import (
    snapshot_alias_mapping,
    snapshot_parameter_names,
    snapshot_return_types,
    udf_names_from_snapshot,
    validate_required_udfs,
    validate_rust_udf_snapshot,
)


def _payload() -> dict[str, object]:
    return {
        "version": 1,
        "scalar": ["stable_id"],
        "aggregate": [],
        "window": [],
        "table": [],
        "aliases": {"stable_id_alias": ["stable_id"]},
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


def test_validate_rust_udf_snapshot_accepts_typed_contract_payload() -> None:
    """Canonical snapshots should pass structural validation."""
    validate_rust_udf_snapshot(_payload())


def test_validate_required_udfs_accepts_alias_resolution() -> None:
    """Alias metadata should satisfy required UDF checks."""
    validate_required_udfs(_payload(), required=("stable_id_alias",))


def test_snapshot_helpers_return_normalized_mappings() -> None:
    """Snapshot helper accessors should expose typed, deterministic mappings."""
    payload = _payload()
    assert snapshot_parameter_names(payload) == {"stable_id": ()}
    assert snapshot_return_types(payload) == {"stable_id": ("Utf8",)}
    assert snapshot_alias_mapping(payload) == {"stable_id_alias": "stable_id"}
    assert udf_names_from_snapshot(payload) == frozenset({"stable_id"})
