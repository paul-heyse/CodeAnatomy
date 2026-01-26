"""Unit tests for incremental runtime table registry."""

from __future__ import annotations

import pyarrow as pa
import pytest

from incremental.runtime import IncrementalRuntime, TempTableRegistry

EXPECTED_ROW_COUNT = 2


def test_temp_table_registry_registers_and_cleans() -> None:
    """Register temp tables and ensure they are cleaned up."""
    runtime = _runtime_or_skip()
    ctx = runtime.session_context()
    table = pa.table({"file_id": ["a", "b"], "value": [1, 2]})

    with TempTableRegistry(runtime) as registry:
        name = registry.register_table(table, prefix="temp")
        result = ctx.sql(f"SELECT COUNT(*) AS cnt FROM {name}").to_arrow_table()
        assert result["cnt"][0].as_py() == EXPECTED_ROW_COUNT

    with pytest.raises(RuntimeError, match="not found"):
        ctx.sql(f"SELECT * FROM {name}").to_arrow_table()


def _runtime_or_skip() -> IncrementalRuntime:
    try:
        runtime = IncrementalRuntime.build()
        _ = runtime.session_context()
    except ImportError as exc:
        pytest.skip(str(exc))
    else:
        return runtime
    msg = "Incremental runtime unavailable."
    raise RuntimeError(msg)
