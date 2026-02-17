"""Tests for typed delta compatibility payload contracts."""

from __future__ import annotations

from datafusion_engine.delta.capabilities import DeltaExtensionCompatibility
from datafusion_engine.delta.control_plane_core import _compatibility_message
from datafusion_engine.delta.provider_artifacts import (
    DeltaProviderBuildRequest,
    build_delta_provider_build_result,
)


def test_provider_artifact_uses_typed_compatibility_fields() -> None:
    """Provider artifact payload reflects typed compatibility fields."""
    compatibility = DeltaExtensionCompatibility(
        available=True,
        compatible=True,
        error=None,
        entrypoint="delta_table_provider_from_session",
        module="datafusion_engine.extensions.datafusion_ext",
        ctx_kind="outer",
        probe_result="ok",
    )
    request = DeltaProviderBuildRequest(
        table_uri="memory://delta",
        dataset_format="delta",
        provider_kind="delta",
        compatibility=compatibility,
    )

    result = build_delta_provider_build_result(request)
    payload = result.as_payload()

    assert payload["module"] == compatibility.module
    assert payload["entrypoint"] == compatibility.entrypoint
    assert payload["ctx_kind"] == compatibility.ctx_kind
    assert payload["probe_result"] == compatibility.probe_result


def test_control_plane_compatibility_message_uses_typed_contract() -> None:
    """Compatibility message includes typed compatibility metadata."""
    compatibility = DeltaExtensionCompatibility(
        available=True,
        compatible=False,
        error="incompatible",
        entrypoint="delta_scan_config_from_session",
        module="datafusion_engine.extensions.datafusion_ext",
        ctx_kind="outer",
        probe_result="error",
    )

    message = _compatibility_message(
        "Delta extension incompatible.",
        compatibility=compatibility,
        entrypoint=compatibility.entrypoint,
    )

    assert "entrypoint=delta_scan_config_from_session" in message
    assert "module=datafusion_engine.extensions.datafusion_ext" in message
    assert "ctx_kind=outer" in message
    assert "probe_result=error" in message
    assert "error=incompatible" in message
