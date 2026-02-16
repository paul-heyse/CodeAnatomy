"""Tests for admin command output contracts."""

from __future__ import annotations

import json
from pathlib import Path

from tools.cq.cli_app.commands import admin
from tools.cq.cli_app.context import CliContext, CliResult, CliTextResult
from tools.cq.cli_app.types import OutputFormat, SchemaKind


def _json_ctx(tmp_path: Path) -> CliContext:
    return CliContext.build(
        argv=["cq", "admin"],
        root=tmp_path,
        output_format=OutputFormat.json,
    )


def _payload(result: CliResult) -> dict[str, object]:
    assert isinstance(result.result, CliTextResult)
    return json.loads(result.result.text)


def test_admin_index_json_output(tmp_path: Path) -> None:
    """Test admin index json output."""
    ctx = _json_ctx(tmp_path)
    result = admin.index(ctx=ctx)
    assert isinstance(result, CliResult)
    payload = _payload(result)
    assert payload == {
        "deprecated": True,
        "message": "Index management has been removed. Caching is no longer used.",
    }


def test_admin_cache_json_output(tmp_path: Path) -> None:
    """Test admin cache json output."""
    ctx = _json_ctx(tmp_path)
    result = admin.cache(ctx=ctx)
    assert isinstance(result, CliResult)
    payload = _payload(result)
    assert payload == {
        "deprecated": True,
        "message": "Cache management has been removed. Caching is no longer used.",
    }


def test_admin_schema_result_json_output(tmp_path: Path) -> None:
    """Test admin schema result json output."""
    ctx = _json_ctx(tmp_path)
    result = admin.schema(kind=SchemaKind.result, ctx=ctx)
    assert isinstance(result, CliResult)
    payload = _payload(result)
    assert isinstance(payload, dict)
    assert "$ref" in payload
    assert isinstance(payload.get("$defs"), dict)


def test_admin_schema_components_json_output(tmp_path: Path) -> None:
    """Test admin schema components json output."""
    ctx = _json_ctx(tmp_path)
    result = admin.schema(kind=SchemaKind.components, ctx=ctx)
    assert isinstance(result, CliResult)
    payload = _payload(result)
    assert isinstance(payload, dict)
    schema_payload = payload.get("schema")
    assert isinstance(schema_payload, (list, dict, tuple))
    assert isinstance(payload.get("components"), dict)
