"""Tests for Ibis portability operation gates."""

from __future__ import annotations

from typing import cast

import ibis
import ibis.expr.operations as ops

from ibis_engine.expr_compiler import OperationSupportBackend, unsupported_operations


class _Backend:
    def __init__(self, *, supported: set[type[ops.Node]]) -> None:
        self._supported = supported

    def has_operation(self, operation: type[ops.Node], /) -> bool:
        return operation in self._supported


class _NoOpBackend:
    has_operation: None = None


def test_unsupported_operations_flags_table_unnest() -> None:
    """Detect unsupported table unnest operations."""
    table = ibis.memtable({"items": [[1, 2], [3]]})
    expr = table.unnest("items")
    backend = _Backend(supported=set())
    missing = unsupported_operations(expr, backend=backend)
    assert "TableUnnest" in missing


def test_unsupported_operations_flags_window_functions() -> None:
    """Detect unsupported window functions."""
    table = ibis.memtable({"a": [1, 2]})
    expr = table.mutate(rn=ibis.row_number().over(ibis.window()))
    backend = _Backend(supported={ops.TableUnnest})
    missing = unsupported_operations(expr, backend=backend)
    assert "WindowFunction" in missing


def test_unsupported_operations_requires_capabilities() -> None:
    """Require explicit capability checks for portability ops."""
    table = ibis.memtable({"items": [[1, 2], [3]]})
    expr = table.unnest("items")
    backend = cast("OperationSupportBackend", _NoOpBackend())
    missing = unsupported_operations(expr, backend=backend)
    assert missing == ("TableUnnest",)
