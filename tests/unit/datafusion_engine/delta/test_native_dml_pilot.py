"""Pilot guardrails for provider-native Delta DML entrypoints."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

import pytest

from datafusion_engine.delta import control_plane_mutation as mutation
from datafusion_engine.delta.capabilities import DeltaExtensionCompatibility
from datafusion_engine.delta.control_plane_core import DeltaDeleteRequest

if TYPE_CHECKING:
    from datafusion import SessionContext


def test_native_delete_requires_non_empty_predicate() -> None:
    """Delete guard should reject an empty predicate."""
    with pytest.raises(ValueError, match="requires a non-empty predicate"):
        mutation.delta_delete(
            cast("SessionContext", object()),
            request=DeltaDeleteRequest(
                table_uri="/tmp/table",
                storage_options=None,
                version=None,
                timestamp=None,
                predicate="",
                extra_constraints=None,
            ),
        )


def test_native_delete_routes_through_internal_entrypoint(monkeypatch: pytest.MonkeyPatch) -> None:
    """Delete path should route payloads through the internal entrypoint hook."""
    captured: dict[str, Any] = {}

    def _entrypoint(_ctx: object, payload: bytes) -> dict[str, object]:
        captured["payload"] = payload
        return {"operation": "delete", "status": "ok"}

    monkeypatch.setattr(mutation, "_require_internal_entrypoint", lambda _name: _entrypoint)
    monkeypatch.setattr(mutation, "_internal_ctx", lambda _ctx, *, entrypoint: (entrypoint, "ctx"))
    monkeypatch.setattr(
        mutation,
        "provider_supports_native_dml",
        lambda _ctx, *, operation, require_non_fallback=True: (
            operation == "delete" and require_non_fallback
        ),
    )

    result = mutation.delta_delete(
        cast("SessionContext", object()),
        request=DeltaDeleteRequest(
            table_uri="/tmp/table",
            storage_options=None,
            version=None,
            timestamp=None,
            predicate="id > 0",
            extra_constraints=None,
        ),
    )

    assert result["operation"] == "delete"
    assert isinstance(captured.get("payload"), bytes)


def test_native_delete_does_not_allow_python_fallback_paths(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Delete path should fail closed when native DML is unavailable."""
    monkeypatch.setattr(
        mutation,
        "provider_supports_native_dml",
        lambda *_args, **_kwargs: False,
    )
    monkeypatch.setattr(
        mutation,
        "native_dml_compatibility",
        lambda *_args, **_kwargs: DeltaExtensionCompatibility(
            available=True,
            compatible=False,
            error="fallback_disallowed",
            entrypoint="delta_delete_request",
            module="datafusion_engine.extensions.datafusion_ext",
            ctx_kind="fallback",
            probe_result="fallback_disallowed",
        ),
    )

    with pytest.raises(
        Exception,
        match="Degraded Python fallback mutation paths have been removed",
    ):
        mutation.delta_delete(
            cast("SessionContext", object()),
            request=DeltaDeleteRequest(
                table_uri="/tmp/table",
                storage_options=None,
                version=None,
                timestamp=None,
                predicate="id > 0",
                extra_constraints=None,
            ),
        )


def test_native_delete_raises_without_fallback_when_native_dml_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Delete path should raise a plugin error when native DML is unavailable."""
    monkeypatch.setattr(
        mutation,
        "provider_supports_native_dml",
        lambda *_args, **_kwargs: False,
    )
    monkeypatch.setattr(
        mutation,
        "native_dml_compatibility",
        lambda *_args, **_kwargs: DeltaExtensionCompatibility(
            available=True,
            compatible=False,
            error="fallback_disallowed",
            entrypoint="delta_delete_request",
            module="datafusion_engine.extensions.datafusion_ext",
            ctx_kind="fallback",
            probe_result="fallback_disallowed",
        ),
    )

    with pytest.raises(
        Exception,
        match="Degraded Python fallback mutation paths have been removed",
    ):
        mutation.delta_delete(
            cast("SessionContext", object()),
            request=DeltaDeleteRequest(
                table_uri="/tmp/table",
                storage_options=None,
                version=None,
                timestamp=None,
                predicate="id > 0",
                extra_constraints=None,
            ),
        )
