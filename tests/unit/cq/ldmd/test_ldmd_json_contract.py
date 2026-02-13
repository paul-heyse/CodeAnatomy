"""LDMD command JSON output contract tests."""

from __future__ import annotations

import json
from pathlib import Path

from tools.cq.cli_app.commands.ldmd import get as ldmd_get
from tools.cq.cli_app.commands.ldmd import neighbors as ldmd_neighbors
from tools.cq.cli_app.context import CliContext, CliTextResult
from tools.cq.cli_app.types import OutputFormat


def _write_ldmd(path: Path) -> None:
    path.write_bytes(
        b"""<!--LDMD:BEGIN id="summary"-->
summary
<!--LDMD:END id="summary"-->
<!--LDMD:BEGIN id="details"-->
details
<!--LDMD:END id="details"-->
"""
    )


def test_ldmd_get_json_returns_structured_payload(tmp_path: Path) -> None:
    ldmd_path = tmp_path / "result.ldmd"
    _write_ldmd(ldmd_path)
    ctx = CliContext.build(
        argv=["cq", "ldmd", "get"],
        root=tmp_path,
        output_format=OutputFormat.json,
    )

    cli_result = ldmd_get(
        str(ldmd_path),
        section_id="root",
        mode="preview",
        depth=1,
        ctx=ctx,
    )
    assert isinstance(cli_result.result, CliTextResult)
    payload = json.loads(cli_result.result.text)
    assert payload["section_id"] == "summary"
    assert payload["mode"] == "preview"
    assert payload["depth"] == 1
    assert "content" in payload


def test_ldmd_neighbors_json_returns_envelope(tmp_path: Path) -> None:
    ldmd_path = tmp_path / "result.ldmd"
    _write_ldmd(ldmd_path)
    ctx = CliContext.build(
        argv=["cq", "ldmd", "neighbors"],
        root=tmp_path,
        output_format=OutputFormat.json,
    )

    cli_result = ldmd_neighbors(
        str(ldmd_path),
        section_id="root",
        ctx=ctx,
    )
    assert isinstance(cli_result.result, CliTextResult)
    payload = json.loads(cli_result.result.text)
    assert payload["section_id"] == "summary"
    assert isinstance(payload["neighbors"], dict)
    assert payload["neighbors"]["prev"] is None
    assert payload["neighbors"]["next"] == "details"
