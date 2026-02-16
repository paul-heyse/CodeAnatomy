# ruff: noqa: D100, D103, INP001
from __future__ import annotations

from datafusion_engine.io.write_planning import resolved_partition_by


def test_resolved_partition_by_normalizes_sequence() -> None:
    assert resolved_partition_by(["a", "b"]) == ("a", "b")
    assert resolved_partition_by(None) == ()
