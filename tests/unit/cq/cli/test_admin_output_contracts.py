"""Tests for admin command output contracts."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from tools.cq.cli_app.commands import admin
from tools.cq.cli_app.context import CliContext
from tools.cq.cli_app.types import OutputFormat


def _json_ctx(tmp_path: Path) -> CliContext:
    return CliContext.build(
        argv=["cq", "admin"],
        root=tmp_path,
        output_format=OutputFormat.json,
    )


def test_admin_index_json_output(capsys: pytest.CaptureFixture[str], tmp_path: Path) -> None:
    ctx = _json_ctx(tmp_path)
    code = admin.index(ctx=ctx)
    assert code == 0
    out = capsys.readouterr().out
    payload = json.loads(out)
    assert payload == {
        "deprecated": True,
        "message": "Index management has been removed. Caching is no longer used.",
    }


def test_admin_cache_json_output(capsys: pytest.CaptureFixture[str], tmp_path: Path) -> None:
    ctx = _json_ctx(tmp_path)
    code = admin.cache(ctx=ctx)
    assert code == 0
    out = capsys.readouterr().out
    payload = json.loads(out)
    assert payload == {
        "deprecated": True,
        "message": "Cache management has been removed. Caching is no longer used.",
    }


def test_admin_schema_result_json_output(
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    ctx = _json_ctx(tmp_path)
    code = admin.schema(kind="result", ctx=ctx)
    assert code == 0
    out = capsys.readouterr().out
    payload = json.loads(out)
    assert isinstance(payload, dict)
    assert "$ref" in payload
    assert isinstance(payload.get("$defs"), dict)


def test_admin_schema_components_json_output(
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    ctx = _json_ctx(tmp_path)
    code = admin.schema(kind="components", ctx=ctx)
    assert code == 0
    out = capsys.readouterr().out
    payload = json.loads(out)
    assert isinstance(payload, dict)
    schema_payload = payload.get("schema")
    assert isinstance(schema_payload, (list, dict, tuple))
    assert isinstance(payload.get("components"), dict)
