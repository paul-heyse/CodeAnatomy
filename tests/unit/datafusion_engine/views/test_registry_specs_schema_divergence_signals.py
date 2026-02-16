"""Tests for schema divergence extraction via plan signals."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

import msgspec
import pyarrow as pa
import pytest

from datafusion_engine.plan.signals import PlanSignals
from datafusion_engine.views import registry_specs
from schema_spec.dataset_spec import dataset_spec_from_schema
from serde_artifact_specs import SCHEMA_DIVERGENCE_SPEC

if TYPE_CHECKING:
    from datafusion import SessionContext

    from datafusion_engine.io.adapter import DataFusionIOAdapter
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from schema_spec.dataset_spec import DatasetSpec
    from semantics.program_manifest import SemanticProgramManifest

_VIEW_NAME = "__missing_semantic_dataset__"


@dataclass
class _Adapter:
    @staticmethod
    def register_view(*_args: object, **_kwargs: object) -> None:
        return None


@dataclass
class _RuntimeProfile:
    data_sources: object


def _context(
    *,
    dataset_specs: dict[str, DatasetSpec] | None = None,
) -> registry_specs.SemanticViewNodeContext:
    resolved_dataset_specs: dict[str, DatasetSpec] = (
        dataset_specs if dataset_specs is not None else {}
    )
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
        dataset_specs=resolved_dataset_specs,
    )


def _dataset_spec_with_strict_schema_validation(*, strict: bool | None) -> DatasetSpec:
    spec = dataset_spec_from_schema(_VIEW_NAME, pa.schema([("id", pa.int64())]))
    policies = msgspec.structs.replace(spec.policies, strict_schema_validation=strict)
    return msgspec.structs.replace(spec, policies=policies)


def test_schema_divergence_uses_plan_signals_schema(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Schema divergence path should read plan schema from PlanSignals."""
    monkeypatch.setenv("CODEANATOMY_SCHEMA_DIVERGENCE_STRICT", "0")
    monkeypatch.delenv("CI", raising=False)

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
        name=_VIEW_NAME,
        builder=lambda _ctx: object(),
    )

    assert signals_calls
    assert len(artifact_calls) == 1
    _, artifact_name, payload = artifact_calls[0]
    assert artifact_name is SCHEMA_DIVERGENCE_SPEC
    assert payload["strict"] is False
    assert payload["severity"] == "warning"
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
        name=_VIEW_NAME,
        builder=lambda _ctx: object(),
    )

    assert artifact_calls == []


def test_schema_divergence_strict_env_raises_after_recording_artifact(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Strict env mode should raise while still emitting divergence artifact."""
    monkeypatch.setenv("CODEANATOMY_SCHEMA_DIVERGENCE_STRICT", "1")
    monkeypatch.delenv("CI", raising=False)

    expected_schema = pa.schema([("id", pa.int64())])
    plan_schema = pa.schema([("id", pa.int64()), ("new_col", pa.string())])
    bundle = type("_Bundle", (), {"df": object()})()
    artifact_calls: list[tuple[object, object, dict[str, object]]] = []

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
        lambda *_args, **_kwargs: PlanSignals(schema=plan_schema),
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
    with pytest.raises(ValueError, match="Schema divergence detected"):
        build_semantic_view_node(
            context=_context(),
            name=_VIEW_NAME,
            builder=lambda _ctx: object(),
        )

    assert len(artifact_calls) == 1
    _, artifact_name, payload = artifact_calls[0]
    assert artifact_name is SCHEMA_DIVERGENCE_SPEC
    assert payload["strict"] is True
    assert payload["severity"] == "error"


def test_schema_divergence_dataset_policy_overrides_strict_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Per-dataset strict policy should override env strict setting."""
    monkeypatch.setenv("CODEANATOMY_SCHEMA_DIVERGENCE_STRICT", "1")
    monkeypatch.delenv("CI", raising=False)

    expected_schema = pa.schema([("id", pa.int64())])
    plan_schema = pa.schema([("id", pa.int64()), ("new_col", pa.string())])
    bundle = type("_Bundle", (), {"df": object()})()
    artifact_calls: list[tuple[object, object, dict[str, object]]] = []

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
        lambda *_args, **_kwargs: PlanSignals(schema=plan_schema),
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

    dataset_specs = {_VIEW_NAME: _dataset_spec_with_strict_schema_validation(strict=False)}
    build_semantic_view_node = registry_specs.__dict__["_build_semantic_view_node"]
    build_semantic_view_node(
        context=_context(dataset_specs=dataset_specs),
        name=_VIEW_NAME,
        builder=lambda _ctx: object(),
    )

    assert len(artifact_calls) == 1
    _, artifact_name, payload = artifact_calls[0]
    assert artifact_name is SCHEMA_DIVERGENCE_SPEC
    assert payload["strict"] is False
    assert payload["severity"] == "warning"


def test_schema_divergence_dataset_policy_can_force_strict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Per-dataset strict policy should force strict behavior even when env is non-strict."""
    monkeypatch.setenv("CODEANATOMY_SCHEMA_DIVERGENCE_STRICT", "0")
    monkeypatch.delenv("CI", raising=False)

    expected_schema = pa.schema([("id", pa.int64())])
    plan_schema = pa.schema([("id", pa.int64()), ("new_col", pa.string())])
    bundle = type("_Bundle", (), {"df": object()})()
    artifact_calls: list[tuple[object, object, dict[str, object]]] = []

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
        lambda *_args, **_kwargs: PlanSignals(schema=plan_schema),
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

    dataset_specs = {_VIEW_NAME: _dataset_spec_with_strict_schema_validation(strict=True)}
    build_semantic_view_node = registry_specs.__dict__["_build_semantic_view_node"]
    with pytest.raises(ValueError, match="Schema divergence detected"):
        build_semantic_view_node(
            context=_context(dataset_specs=dataset_specs),
            name=_VIEW_NAME,
            builder=lambda _ctx: object(),
        )

    assert len(artifact_calls) == 1
    _, artifact_name, payload = artifact_calls[0]
    assert artifact_name is SCHEMA_DIVERGENCE_SPEC
    assert payload["strict"] is True
    assert payload["severity"] == "error"
