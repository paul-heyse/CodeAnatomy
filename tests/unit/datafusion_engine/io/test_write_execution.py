# ruff: noqa: D100, D103, INP001
from __future__ import annotations

from datafusion_engine.io.write_execution import execute_write


def test_execute_write_calls_callable() -> None:
    assert execute_write(lambda req: {"request": req}, "payload") == {"request": "payload"}
