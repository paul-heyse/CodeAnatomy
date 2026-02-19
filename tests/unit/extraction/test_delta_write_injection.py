"""Tests for injected write-context behavior in Delta materialization."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, cast

import pyarrow as pa
import pytest

from extraction import orchestrator as orchestrator_mod

if TYPE_CHECKING:
    from datafusion import SessionContext
    from pyarrow import Table


def test_write_delta_uses_injected_write_ctx(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """_write_delta should use caller-injected SessionContext when provided."""
    captured: dict[str, object] = {}

    def _build_delta_write_request(
        *,
        table_uri: str,
        table: Table,
        options: object,
    ) -> dict[str, str]:
        captured["table_uri"] = table_uri
        captured["table"] = table
        captured["options"] = options
        return {"request": "ok"}

    def _write_transaction(ctx: object, *, request: object) -> None:
        captured["ctx"] = ctx
        captured["request"] = request

    monkeypatch.setattr(
        "datafusion_engine.delta.write_ipc_payload.build_delta_write_request",
        _build_delta_write_request,
    )
    monkeypatch.setattr(
        "datafusion_engine.delta.transactions.write_transaction",
        _write_transaction,
    )
    monkeypatch.setattr(
        orchestrator_mod,
        "_get_delta_write_ctx",
        lambda: (_ for _ in ()).throw(RuntimeError("should not be called")),
    )

    table = pa.table({"x": [1]})
    injected_ctx = cast("SessionContext", object())
    location = tmp_path / "delta_table"

    result = orchestrator_mod._write_delta(  # noqa: SLF001
        table,
        location,
        "delta_table",
        write_ctx=injected_ctx,
    )

    assert result == str(location)
    assert captured["ctx"] is injected_ctx
