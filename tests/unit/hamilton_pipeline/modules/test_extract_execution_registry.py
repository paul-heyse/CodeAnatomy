"""Unit tests for extract execution registry dispatch contracts."""

from __future__ import annotations

import warnings
from collections.abc import Mapping
from typing import Any

import pytest

from datafusion_engine.extract.adapter_registry import (
    adapter_executor_key,
    extract_template_adapters,
)
from hamilton_pipeline.modules import extract_execution_registry, task_execution


def _dummy_executor(
    _inputs: Any, _extract_session: Any, _profile_name: str
) -> Mapping[str, object]:
    """Return an empty mapping for testing."""
    return {}


def test_get_extract_executor_raises_for_unknown_adapter() -> None:
    """Verify that get_extract_executor raises for missing adapters."""
    with pytest.raises(ValueError, match="Unknown extract adapter"):
        extract_execution_registry.get_extract_executor("__definitely_missing__")


def test_register_and_get_extract_executor_roundtrip(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify register/get roundtrip emits deprecation warning."""
    monkeypatch.setattr(extract_execution_registry, "_EXTRACT_ADAPTER_EXECUTORS", {})
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        extract_execution_registry.register_extract_executor(
            "__unit_test_adapter__",
            _dummy_executor,
        )

    assert (
        extract_execution_registry.get_extract_executor("__unit_test_adapter__") is _dummy_executor
    )
    assert "__unit_test_adapter__" in extract_execution_registry.registered_adapter_names()


def test_register_extract_executor_emits_deprecation_warning(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify register_extract_executor emits a DeprecationWarning."""
    monkeypatch.setattr(extract_execution_registry, "_EXTRACT_ADAPTER_EXECUTORS", {})
    with pytest.warns(DeprecationWarning, match="register_extract_executor.*deprecated"):
        extract_execution_registry.register_extract_executor(
            "__unit_test_adapter__",
            _dummy_executor,
        )


def test_ensure_extract_executors_registered_emits_deprecation_warning(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify ensure_extract_executors_registered emits a DeprecationWarning."""
    monkeypatch.setattr(extract_execution_registry, "_EXTRACT_ADAPTER_EXECUTORS", {})
    with pytest.warns(DeprecationWarning, match="ensure_extract_executors_registered.*deprecated"):
        task_execution.ensure_extract_executors_registered(force=True)


def test_registry_matches_adapter_names_after_task_execution_initializer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify legacy registry contains all adapters after initialization."""
    monkeypatch.setattr(extract_execution_registry, "_EXTRACT_ADAPTER_EXECUTORS", {})

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        task_execution.ensure_extract_executors_registered(force=True)

    expected = {adapter_executor_key(adapter.name) for adapter in extract_template_adapters()}
    assert extract_execution_registry.registered_adapter_names() == expected
