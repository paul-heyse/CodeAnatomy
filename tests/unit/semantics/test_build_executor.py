"""Tests for semantic build-executor helper."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

import pytest

import semantics.build_executor

if TYPE_CHECKING:
    from datafusion import SessionContext

    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from semantics.compile_context import SemanticExecutionContext
    from semantics.pipeline_build import CpgBuildOptions


def test_execute_semantic_build_delegates(monkeypatch: pytest.MonkeyPatch) -> None:
    """Build executor should delegate to canonical semantic build entrypoint."""
    monkeypatch.setattr(semantics.build_executor, "build_cpg", lambda *_args, **_kwargs: "ok")
    assert (
        semantics.build_executor.execute_semantic_build(
            cast("SessionContext", object()),
            runtime_profile=cast("DataFusionRuntimeProfile", object()),
            execution_context=cast("SemanticExecutionContext", object()),
            options=cast("CpgBuildOptions", object()),
        )
        == "ok"
    )
