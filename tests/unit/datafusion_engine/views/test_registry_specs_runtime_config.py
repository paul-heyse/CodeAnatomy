"""Tests for registry specs runtime config threading."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, cast

import pytest

from datafusion_engine.views import registry_specs
from semantics.runtime import SemanticRuntimeConfig
from tests.test_helpers.datafusion_runtime import df_profile

if TYPE_CHECKING:
    from datafusion import SessionContext


def test_view_graph_nodes_threads_runtime_config(monkeypatch: pytest.MonkeyPatch) -> None:
    profile = df_profile()
    runtime_config = SemanticRuntimeConfig(output_locations={"example_v1": "/tmp/example"})

    def _runtime_from_profile(_: object) -> SemanticRuntimeConfig:
        return runtime_config

    def _noop_snapshot(_: Mapping[str, object]) -> None:
        return None

    def _empty_nodes(*_args: object, **_kwargs: object) -> list[object]:
        return []

    captured: dict[str, SemanticRuntimeConfig | None] = {"runtime_config": None}

    def _capture(
        *_args: object,
        **kwargs: object,
    ) -> list[object]:
        runtime_config_value = cast(
            "SemanticRuntimeConfig | None",
            kwargs.get("runtime_config"),
        )
        captured["runtime_config"] = runtime_config_value
        return []

    monkeypatch.setattr(registry_specs, "semantic_runtime_from_profile", _runtime_from_profile)
    monkeypatch.setattr(registry_specs, "validate_rust_udf_snapshot", _noop_snapshot)
    monkeypatch.setattr(registry_specs, "_alias_nodes", _empty_nodes)
    monkeypatch.setattr(registry_specs, "_semantics_view_nodes", _capture)

    ctx = cast("SessionContext", object())
    nodes = registry_specs.view_graph_nodes(
        ctx,
        snapshot={},
        runtime_profile=profile,
    )

    assert nodes == ()
    assert captured["runtime_config"] is runtime_config
