"""Tests for write execution adapter helper."""

from __future__ import annotations

from datafusion_engine.io.write_execution import execute_write


def test_execute_write_calls_callable() -> None:
    """execute_write delegates payload into callable function."""
    assert execute_write(lambda req: {"request": req}, "payload") == {"request": "payload"}
