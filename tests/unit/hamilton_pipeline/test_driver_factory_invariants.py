"""Tests for compile/resolver invariant wiring in driver factory."""

from __future__ import annotations

from types import SimpleNamespace
from typing import TYPE_CHECKING, cast

import pytest

from hamilton_pipeline import driver_factory

if TYPE_CHECKING:
    from collections.abc import Mapping

    from core_types import JsonValue
    from semantics.compile_context import SemanticExecutionContext


def _stub_view_graph_context_runtime() -> tuple[SimpleNamespace, SimpleNamespace]:
    runtime_ctx = object()
    profile = SimpleNamespace(
        artifact_calls=[],
        session_runtime=lambda: SimpleNamespace(ctx=runtime_ctx),
        record_artifact=lambda name, payload: profile.artifact_calls.append((name, dict(payload))),
    )
    spec = SimpleNamespace(
        datafusion=profile,
        determinism_tier="deterministic",
    )
    return profile, spec


def _semantic_execution_context_with_locations(
    locations: dict[str, object],
) -> SimpleNamespace:
    manifest = SimpleNamespace(
        semantic_ir=object(),
        dataset_bindings=SimpleNamespace(locations=locations),
    )
    return SimpleNamespace(manifest=manifest, dataset_resolver=object())


def test_build_view_graph_context_records_invariant_artifact(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Driver factory should emit compile/resolver invariant artifact."""
    profile, spec = _stub_view_graph_context_runtime()
    semantic_context = _semantic_execution_context_with_locations({"events": object()})

    class _Facade:
        def __init__(self, **_kwargs: object) -> None:
            return None

        def ensure_view_graph(self, **_kwargs: object) -> dict[str, object]:
            return {"snapshot": "ok"}

    monkeypatch.setattr(driver_factory, "resolve_runtime_profile", lambda *_args, **_kwargs: spec)
    monkeypatch.setattr(
        driver_factory,
        "_apply_datafusion_cache_config",
        lambda datafusion, **_kwargs: datafusion,
    )
    monkeypatch.setattr("datafusion_engine.session.facade.DataFusionExecutionFacade", _Facade)
    monkeypatch.setattr(
        "datafusion_engine.session.runtime.refresh_session_runtime",
        lambda _profile, *, ctx: SimpleNamespace(ctx=ctx),
    )
    monkeypatch.setattr(
        "datafusion_engine.views.registry_specs.view_graph_nodes",
        lambda *_args, **_kwargs: (),
    )
    monkeypatch.setattr(
        "cpg.kind_catalog.validate_edge_kind_requirements",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        driver_factory, "_relation_output_schema", lambda *_args, **_kwargs: object()
    )
    monkeypatch.setattr(
        "datafusion_engine.session.runtime.compile_resolver_invariants_strict_mode",
        lambda: False,
    )
    builder_calls: list[tuple[object, object]] = []
    monkeypatch.setattr(
        "datafusion_engine.session.runtime.datasource_config_from_manifest",
        lambda manifest, *, runtime_profile: (
            builder_calls.append((manifest, runtime_profile)) or {"templates": "ok"}
        ),
    )
    monkeypatch.setattr(
        "datafusion_engine.session.runtime.datasource_config_from_profile",
        lambda _runtime_profile: (_ for _ in ()).throw(
            AssertionError("profile datasource builder should not be used for bound manifests")
        ),
    )

    result = driver_factory.build_view_graph_context(
        cast("Mapping[str, JsonValue]", {}),
        execution_context=cast("SemanticExecutionContext", semantic_context),
    )

    assert result.snapshot == {"snapshot": "ok"}
    assert len(profile.artifact_calls) == 1
    spec_obj, payload = profile.artifact_calls[0]
    assert getattr(spec_obj, "canonical_name", None) == "compile_resolver_invariants_v1"
    assert payload["label"] == "build_view_graph_context"
    assert payload["compile_count"] == 0
    assert payload["max_compiles"] == 0
    assert payload["strict"] is False
    assert payload["violations"] == ()
    assert len(builder_calls) == 1
    assert builder_calls[0][0] is semantic_context.manifest
    assert builder_calls[0][1] is profile


def test_build_view_graph_context_strict_mode_raises_on_resolver_drift(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Strict mode should fail when multiple resolver identities are observed."""
    profile, spec = _stub_view_graph_context_runtime()
    semantic_context = _semantic_execution_context_with_locations({"events": object()})

    class _FacadeDrift:
        resolver_a = object()
        resolver_b = object()

        def __init__(self, **_kwargs: object) -> None:
            return None

        def ensure_view_graph(self, **_kwargs: object) -> dict[str, object]:
            from semantics.resolver_identity import record_resolver_if_tracking

            record_resolver_if_tracking(self.resolver_a, label="view_registration_a")
            record_resolver_if_tracking(self.resolver_b, label="view_registration_b")
            return {"snapshot": "ok"}

    monkeypatch.setattr(driver_factory, "resolve_runtime_profile", lambda *_args, **_kwargs: spec)
    monkeypatch.setattr(
        driver_factory,
        "_apply_datafusion_cache_config",
        lambda datafusion, **_kwargs: datafusion,
    )
    monkeypatch.setattr("datafusion_engine.session.facade.DataFusionExecutionFacade", _FacadeDrift)
    monkeypatch.setattr(
        "datafusion_engine.session.runtime.refresh_session_runtime",
        lambda _profile, *, ctx: SimpleNamespace(ctx=ctx),
    )
    monkeypatch.setattr(
        "datafusion_engine.views.registry_specs.view_graph_nodes",
        lambda *_args, **_kwargs: (),
    )
    monkeypatch.setattr(
        "cpg.kind_catalog.validate_edge_kind_requirements",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        driver_factory, "_relation_output_schema", lambda *_args, **_kwargs: object()
    )
    monkeypatch.setattr(
        "datafusion_engine.session.runtime.compile_resolver_invariants_strict_mode",
        lambda: True,
    )
    monkeypatch.setattr(
        "datafusion_engine.session.runtime.datasource_config_from_manifest",
        lambda _manifest, *, runtime_profile: {"templates": runtime_profile},
    )
    monkeypatch.setattr(
        "datafusion_engine.session.runtime.datasource_config_from_profile",
        lambda _runtime_profile: (_ for _ in ()).throw(
            AssertionError("profile datasource builder should not be used for bound manifests")
        ),
    )

    with pytest.raises(RuntimeError, match="Resolver identity violation"):
        driver_factory.build_view_graph_context(
            cast("Mapping[str, JsonValue]", {}),
            execution_context=cast("SemanticExecutionContext", semantic_context),
        )

    assert len(profile.artifact_calls) == 1
    _, payload = profile.artifact_calls[0]
    assert payload["strict"] is True
    assert payload["distinct_resolver_count"] == 2
    assert any("Resolver identity violation" in item for item in payload["violations"])


def test_build_view_graph_context_uses_profile_builder_when_manifest_bindings_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Empty manifest bindings should select bootstrap/profile datasource builder path."""
    profile, spec = _stub_view_graph_context_runtime()
    semantic_context = _semantic_execution_context_with_locations({})

    class _Facade:
        def __init__(self, **_kwargs: object) -> None:
            return None

        def ensure_view_graph(self, **_kwargs: object) -> dict[str, object]:
            return {"snapshot": "ok"}

    monkeypatch.setattr(driver_factory, "resolve_runtime_profile", lambda *_args, **_kwargs: spec)
    monkeypatch.setattr(
        driver_factory,
        "_apply_datafusion_cache_config",
        lambda datafusion, **_kwargs: datafusion,
    )
    monkeypatch.setattr("datafusion_engine.session.facade.DataFusionExecutionFacade", _Facade)
    monkeypatch.setattr(
        "datafusion_engine.session.runtime.refresh_session_runtime",
        lambda _profile, *, ctx: SimpleNamespace(ctx=ctx),
    )
    monkeypatch.setattr(
        "datafusion_engine.views.registry_specs.view_graph_nodes",
        lambda *_args, **_kwargs: (),
    )
    monkeypatch.setattr(
        "cpg.kind_catalog.validate_edge_kind_requirements",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        driver_factory, "_relation_output_schema", lambda *_args, **_kwargs: object()
    )
    monkeypatch.setattr(
        "datafusion_engine.session.runtime.compile_resolver_invariants_strict_mode",
        lambda: False,
    )
    profile_builder_calls: list[object] = []
    monkeypatch.setattr(
        "datafusion_engine.session.runtime.datasource_config_from_profile",
        lambda runtime_profile: (
            profile_builder_calls.append(runtime_profile) or {"templates": "profile"}
        ),
    )
    monkeypatch.setattr(
        "datafusion_engine.session.runtime.datasource_config_from_manifest",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("manifest datasource builder should not be used when bindings are empty")
        ),
    )

    result = driver_factory.build_view_graph_context(
        cast("Mapping[str, JsonValue]", {}),
        execution_context=cast("SemanticExecutionContext", semantic_context),
    )

    assert result.snapshot == {"snapshot": "ok"}
    assert len(profile_builder_calls) == 1
    assert profile_builder_calls[0] is profile
