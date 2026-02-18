"""Tests for runtime snapshot type normalization helpers."""

from __future__ import annotations

import msgspec

from datafusion_engine.udf.runtime_snapshot_types import (
    RuntimeInstallSnapshot,
    RustUdfSnapshot,
    decode_rust_udf_snapshot_msgpack,
    normalize_runtime_install_snapshot,
)

EXPECTED_CONTRACT_VERSION = 3


def test_normalize_runtime_install_snapshot_defaults() -> None:
    """Normalization should populate default runtime snapshot values."""
    snapshot = normalize_runtime_install_snapshot({"snapshot": {"scalar": []}})
    assert isinstance(snapshot, RuntimeInstallSnapshot)
    assert snapshot.contract_version == EXPECTED_CONTRACT_VERSION
    assert snapshot.runtime_install_mode == "unified"
    assert snapshot.snapshot == {"scalar": []}


def test_decode_rust_udf_snapshot_msgpack_contract() -> None:
    """Decode typed Rust UDF snapshot payload from msgpack bytes."""
    payload = msgspec.msgpack.encode(
        {
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
            "pycapsule_udfs": [],
        }
    )

    snapshot = decode_rust_udf_snapshot_msgpack(payload)

    assert isinstance(snapshot, RustUdfSnapshot)
    assert snapshot.version == 1
    assert snapshot.scalar == ("stable_id",)
    assert snapshot.return_types["stable_id"] == ("Utf8",)
