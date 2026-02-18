"""Contract tests for Rust UDF snapshot msgpack payloads."""

from __future__ import annotations

import msgspec

from datafusion_engine.udf.runtime_snapshot_types import (
    RustUdfSnapshot,
    decode_rust_udf_snapshot_msgpack,
    rust_udf_snapshot_mapping,
)


def _snapshot_payload() -> dict[str, object]:
    return {
        "version": 1,
        "scalar": ["stable_id"],
        "aggregate": [],
        "window": [],
        "table": [],
        "aliases": {"sid": ["stable_id"]},
        "parameter_names": {"stable_id": []},
        "volatility": {"stable_id": "stable"},
        "rewrite_tags": {"stable_id": ["idempotent"]},
        "signature_inputs": {"stable_id": [["Utf8"]]},
        "return_types": {"stable_id": ["Utf8"]},
        "simplify": {"stable_id": True},
        "coerce_types": {"stable_id": False},
        "short_circuits": {"stable_id": False},
        "config_defaults": {},
        "custom_udfs": ["stable_id"],
        "pycapsule_udfs": [],
    }


def test_rust_udf_snapshot_msgpack_roundtrip() -> None:
    """Decode and normalize Rust UDF snapshot msgpack contract."""
    encoded = msgspec.msgpack.encode(_snapshot_payload())
    decoded = decode_rust_udf_snapshot_msgpack(encoded)
    normalized = rust_udf_snapshot_mapping(decoded)

    assert isinstance(decoded, RustUdfSnapshot)
    assert decoded.scalar == ("stable_id",)
    assert normalized["volatility"] == {"stable_id": "stable"}
    assert normalized["return_types"] == {"stable_id": ("Utf8",)}
