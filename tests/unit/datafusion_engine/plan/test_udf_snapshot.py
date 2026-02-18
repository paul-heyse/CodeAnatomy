"""Tests for UDF snapshot helper bridge."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

import pytest

from datafusion_engine.plan import udf_snapshot

if TYPE_CHECKING:
    from datafusion import SessionContext


class _Artifacts:
    def __init__(self) -> None:
        self.snapshot = {"name": "value"}
        self.snapshot_hash = "abc"


def test_collect_udf_snapshot_artifacts_delegates(monkeypatch: pytest.MonkeyPatch) -> None:
    """UDF snapshot helper should delegate and normalize result payload."""
    monkeypatch.setattr(
        udf_snapshot,
        "collect_udf_artifacts",
        lambda *_args, **_kwargs: udf_snapshot.UdfArtifacts(
            snapshot={"name": "value"},
            snapshot_hash="abc",
            rewrite_tags=(),
            domain_planner_names=(),
        ),
    )
    collected = udf_snapshot.collect_udf_snapshot_artifacts(
        cast("SessionContext", object()),
        session_runtime=None,
    )
    assert collected.snapshot == {"name": "value"}
    assert collected.snapshot_hash == "abc"
