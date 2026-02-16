"""Tests for registry specs runtime config threading."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, cast

import pytest

from datafusion_engine.views import registry_specs
from semantics.ir_pipeline import build_semantic_ir
from tests.test_helpers.datafusion_runtime import df_profile

if TYPE_CHECKING:
    from datafusion import SessionContext

    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from semantics.ir import SemanticIR
    from semantics.program_manifest import SemanticProgramManifest


def test_view_graph_nodes_threads_runtime_inputs(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test view graph nodes threads runtime inputs."""
    profile = df_profile()
    semantic_ir = build_semantic_ir()

    def _noop_snapshot(_: Mapping[str, object]) -> None:
        return None

    captured: dict[str, object | None] = {
        "runtime_profile": None,
        "semantic_ir": None,
    }

    def _capture(
        *_args: object,
        **kwargs: object,
    ) -> list[object]:
        captured["runtime_profile"] = cast(
            "DataFusionRuntimeProfile | None",
            kwargs.get("runtime_profile"),
        )
        captured["semantic_ir"] = cast(
            "SemanticIR | None",
            kwargs.get("semantic_ir"),
        )
        return []

    monkeypatch.setattr(registry_specs, "validate_rust_udf_snapshot", _noop_snapshot)
    monkeypatch.setattr(registry_specs, "_semantics_view_nodes", _capture)

    ctx = cast("SessionContext", object())
    nodes = registry_specs.view_graph_nodes(
        ctx,
        snapshot={},
        runtime_profile=profile,
        semantic_ir=semantic_ir,
        manifest=cast("SemanticProgramManifest", object()),
    )

    assert nodes == ()
    assert captured["runtime_profile"] is profile
    assert captured["semantic_ir"] is semantic_ir
