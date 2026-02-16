"""Unit tests for compile/resolver invariant runtime helpers."""

from __future__ import annotations

from types import SimpleNamespace
from typing import TYPE_CHECKING, cast

import pytest

from datafusion_engine.session.runtime_compile import (
    compile_resolver_invariants_strict_mode,
    record_compile_resolver_invariants,
)
from datafusion_engine.session.runtime_dataset_io import record_dataset_readiness

DISTINCT_RESOLVER_COUNT = 2

if TYPE_CHECKING:
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from semantics.program_manifest import ManifestDatasetResolver


def test_compile_resolver_invariants_strict_mode_defaults_non_strict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Runtime default should be non-strict when no env flag is set."""
    monkeypatch.delenv("CODEANATOMY_COMPILE_RESOLVER_INVARIANTS_STRICT", raising=False)
    monkeypatch.delenv("CI", raising=False)
    assert compile_resolver_invariants_strict_mode() is False


def test_compile_resolver_invariants_strict_mode_defaults_to_ci(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CI should enable strict mode by default when no explicit override is set."""
    monkeypatch.delenv("CODEANATOMY_COMPILE_RESOLVER_INVARIANTS_STRICT", raising=False)
    monkeypatch.setenv("CI", "true")
    assert compile_resolver_invariants_strict_mode() is True


def test_compile_resolver_invariants_strict_mode_explicit_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Explicit strict env should override CI fallback behavior."""
    monkeypatch.setenv("CI", "true")
    monkeypatch.setenv("CODEANATOMY_COMPILE_RESOLVER_INVARIANTS_STRICT", "false")
    assert compile_resolver_invariants_strict_mode() is False


def test_record_compile_resolver_invariants_strict_raises_after_recording() -> None:
    """Strict mode should raise after recording artifact when violations exist."""
    calls: list[tuple[object, dict[str, object]]] = []

    profile = SimpleNamespace(
        record_artifact=lambda name, payload: calls.append((name, dict(payload)))
    )

    with pytest.raises(RuntimeError, match="compile mismatch"):
        record_compile_resolver_invariants(
            cast("DataFusionRuntimeProfile", profile),
            label="unit-test",
            compile_count=0,
            max_compiles=1,
            distinct_resolver_count=2,
            strict=True,
            violations=("compile mismatch",),
        )

    assert len(calls) == 1
    spec, payload = calls[0]
    assert getattr(spec, "canonical_name", None) == "compile_resolver_invariants_v1"
    assert payload["label"] == "unit-test"
    assert payload["compile_count"] == 0
    assert payload["max_compiles"] == 1
    assert payload["distinct_resolver_count"] == DISTINCT_RESOLVER_COUNT
    assert payload["strict"] is True
    assert payload["violations"] == ("compile mismatch",)


def test_record_dataset_readiness_records_resolver_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Readiness boundary should record resolver identity before early-return paths."""
    calls: list[tuple[object, str]] = []
    resolver = object()
    monkeypatch.setattr(
        "semantics.resolver_identity.record_resolver_if_tracking",
        lambda tracked_resolver, *, label="unknown": calls.append((tracked_resolver, label)),
    )
    profile = SimpleNamespace(
        diagnostics=SimpleNamespace(diagnostics_sink=None),
    )

    record_dataset_readiness(
        cast("DataFusionRuntimeProfile", profile),
        dataset_names=(),
        dataset_resolver=cast("ManifestDatasetResolver", resolver),
    )

    assert calls == [(resolver, "dataset_readiness")]
