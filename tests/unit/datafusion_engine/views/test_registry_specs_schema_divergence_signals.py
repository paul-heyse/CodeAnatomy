"""Tests for schema divergence extraction via plan signals."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

import pyarrow as pa
import pytest

from datafusion_engine.plan.signals import PlanSignals
from datafusion_engine.views import registry_specs
from serde_artifact_specs import SCHEMA_DIVERGENCE_SPEC

if TYPE_CHECKING:
    from datafusion import SessionContext

    from datafusion_engine.io.adapter import DataFusionIOAdapter
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from semantics.program_manifest import SemanticProgramManifest


@dataclass
class _Adapter:
    def register_view(self, *_args: object, **_kwargs: object) -> None:
        return None


@dataclass
class _RuntimeProfile:
    data_sources: object


def _context() -> registry_specs.SemanticViewNodeContext:
    runtime_profile = _RuntimeProfile(
        data_sources=type(
            "_DataSources",
            (),
            {
                "semantic_output": type(
                    "_SemanticOutput",
                    (),
                    {"cache_overrides": {}},
                )()
            },
        )()
    )
    return registry_specs.SemanticViewNodeContext(
        ctx=cast("SessionContext", object()),
        snapshot={},
        runtime_profile=cast("DataFusionRuntimeProfile", runtime_profile),
        semantic_manifest=cast("SemanticProgramManifest", object()),
        adapter=cast("DataFusionIOAdapter", _Adapter()),
        dataset_specs={},
    )


def test_schema_divergence_uses_plan_signals_schema(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Schema divergence path should read plan schema from PlanSignals."""
    expected_schema = pa.schema([("id", pa.int64())])
    plan_schema = pa.schema([("id", pa.int64()), ("new_col", pa.string())])
    bundle = type("_Bundle", (), {"df": object()})()
    signals_calls: list[object] = []
    artifact_calls: list[tuple[object, str, dict[str, object]]] = []

    monkeypatch.setattr(
        registry_specs,
        "_bundle_deps_and_udfs",
        lambda *_args, **_kwargs: (bundle, (), ()),
    )
    monkeypatch.setattr(
        registry_specs,
        "_dataset_contract_for",
        lambda *_args, **_kwargs: (expected_schema, False),
    )

    def _fake_extract_plan_signals(*_args: object, **_kwargs: object) -> PlanSignals:
        signals_calls.append(object())
        return PlanSignals(schema=plan_schema)

    monkeypatch.setattr(
        "datafusion_engine.plan.signals.extract_plan_signals",
        _fake_extract_plan_signals,
    )
    monkeypatch.setattr(
        "datafusion_engine.schema.contracts.compute_schema_divergence",
        lambda *_args, **_kwargs: type(
            "_Divergence",
            (),
            {
                "has_divergence": True,
                "added_columns": ("new_col",),
                "removed_columns": (),
                "type_mismatches": (),
            },
        )(),
    )
    monkeypatch.setattr(
        "datafusion_engine.lineage.diagnostics.record_artifact",
        lambda profile, name, payload: artifact_calls.append((profile, name, payload)),
    )

    build_semantic_view_node = registry_specs.__dict__["_build_semantic_view_node"]
    build_semantic_view_node(
        context=_context(),
        name="__missing_semantic_dataset__",
        builder=lambda _ctx: object(),
    )

    assert signals_calls
    assert len(artifact_calls) == 1
    _, artifact_name, payload = artifact_calls[0]
    assert artifact_name is SCHEMA_DIVERGENCE_SPEC
    assert payload["plan_columns"] == ["id", "new_col"]


def test_schema_divergence_skips_artifact_when_plan_signals_schema_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No divergence artifact should be recorded when signals.schema is unavailable."""
    expected_schema = pa.schema([("id", pa.int64())])
    bundle = type("_Bundle", (), {"df": object()})()
    artifact_calls: list[tuple[object, str, dict[str, object]]] = []

    monkeypatch.setattr(
        registry_specs,
        "_bundle_deps_and_udfs",
        lambda *_args, **_kwargs: (bundle, (), ()),
    )
    monkeypatch.setattr(
        registry_specs,
        "_dataset_contract_for",
        lambda *_args, **_kwargs: (expected_schema, False),
    )
    monkeypatch.setattr(
        "datafusion_engine.plan.signals.extract_plan_signals",
        lambda *_args, **_kwargs: PlanSignals(schema=None),
    )
    monkeypatch.setattr(
        "datafusion_engine.schema.contracts.compute_schema_divergence",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("should not be called")),
    )
    monkeypatch.setattr(
        "datafusion_engine.lineage.diagnostics.record_artifact",
        lambda profile, name, payload: artifact_calls.append((profile, name, payload)),
    )

    build_semantic_view_node = registry_specs.__dict__["_build_semantic_view_node"]
    build_semantic_view_node(
        context=_context(),
        name="__missing_semantic_dataset__",
        builder=lambda _ctx: object(),
    )

    assert artifact_calls == []
