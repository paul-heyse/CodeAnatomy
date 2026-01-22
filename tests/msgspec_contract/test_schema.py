"""Contract tests for msgspec schemas."""

from __future__ import annotations

import os
from pathlib import Path

import msgspec

from datafusion_engine.runtime import DataFusionRuntimeProfile
from incremental.cdf_cursors import CdfCursor, CdfCursorStore
from obs.diagnostics import PreparedStatementSpec
from serde_msgspec import dumps_json, dumps_msgpack
from sqlglot_tools.optimizer import AstArtifact

_SCHEMA_DIR = Path("tests/fixtures/msgspec_schema")
_SCHEMA_SNAPSHOTS: dict[str, type[msgspec.Struct]] = {
    "cdf_cursor": CdfCursor,
    "cdf_cursor_store": CdfCursorStore,
    "prepared_statement_spec": PreparedStatementSpec,
    "sqlglot_ast_artifact": AstArtifact,
}


def _schema_hook(value_type: type) -> dict[str, object] | None:
    if value_type is Path:
        return {"type": "string"}
    return None


def test_msgspec_schema_snapshots() -> None:
    update_goldens = os.environ.get("UPDATE_GOLDENS") == "1"
    for name, schema_type in _SCHEMA_SNAPSHOTS.items():
        schema = msgspec.json.schema(schema_type, schema_hook=_schema_hook)
        payload = dumps_json(schema, pretty=True)
        path = _SCHEMA_DIR / f"{name}.json"
        if update_goldens:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(payload)
            continue
        if not path.exists():
            msg = f"Missing schema snapshot: {path}"
            raise AssertionError(msg)
        assert path.read_bytes() == payload


def test_runtime_telemetry_msgpack_snapshot() -> None:
    update_goldens = os.environ.get("UPDATE_GOLDENS") == "1"
    payload = DataFusionRuntimeProfile().telemetry_payload_v1()
    encoded = dumps_msgpack(payload)
    path = _SCHEMA_DIR / "datafusion_runtime_telemetry.msgpack"
    if update_goldens:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(encoded)
        return
    if not path.exists():
        msg = f"Missing MessagePack snapshot: {path}"
        raise AssertionError(msg)
    assert path.read_bytes() == encoded
