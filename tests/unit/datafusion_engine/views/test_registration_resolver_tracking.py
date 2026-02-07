"""Tests for resolver tracking wiring in view registration."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

import pytest

from datafusion_engine.views import registration as registration_module

if TYPE_CHECKING:
    from datafusion import SessionContext

    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from semantics.program_manifest import ManifestDatasetResolver, SemanticProgramManifest


def test_ensure_view_graph_records_resolver_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """View registration boundary should record resolver identity when provided."""
    calls: list[tuple[object, str]] = []
    resolver = object()

    monkeypatch.setattr(
        "semantics.resolver_identity.record_resolver_if_tracking",
        lambda tracked_resolver, *, label="unknown": calls.append((tracked_resolver, label)),
    )
    monkeypatch.setattr(
        "datafusion_engine.views.registration.RegistrationPhaseOrchestrator.run",
        lambda _self, _phases: None,
    )

    with pytest.raises(RuntimeError, match="UDF snapshot is required"):
        registration_module.ensure_view_graph(
            cast("SessionContext", object()),
            runtime_profile=cast("DataFusionRuntimeProfile", object()),
            semantic_manifest=cast("SemanticProgramManifest", object()),
            dataset_resolver=cast("ManifestDatasetResolver", resolver),
        )

    assert calls == [(resolver, "view_registration")]
