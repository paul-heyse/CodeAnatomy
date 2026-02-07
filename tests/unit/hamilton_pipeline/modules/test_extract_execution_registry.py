"""Unit tests for extract execution registry dispatch contracts."""

from __future__ import annotations

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
    return {}


def test_get_extract_executor_raises_for_unknown_adapter() -> None:
    with pytest.raises(ValueError, match="Unknown extract adapter"):
        extract_execution_registry.get_extract_executor("__definitely_missing__")


def test_register_and_get_extract_executor_roundtrip(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(extract_execution_registry, "_EXTRACT_ADAPTER_EXECUTORS", {})
    extract_execution_registry.register_extract_executor(
        "__unit_test_adapter__",
        _dummy_executor,
    )

    assert (
        extract_execution_registry.get_extract_executor("__unit_test_adapter__") is _dummy_executor
    )
    assert "__unit_test_adapter__" in extract_execution_registry.registered_adapter_names()


def test_registry_matches_adapter_names_after_task_execution_initializer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(extract_execution_registry, "_EXTRACT_ADAPTER_EXECUTORS", {})
    monkeypatch.setattr(task_execution, "_EXTRACT_EXECUTORS_REGISTERED", False)

    task_execution._ensure_extract_executors_registered()

    expected = {adapter_executor_key(adapter.name) for adapter in extract_template_adapters()}
    assert extract_execution_registry.registered_adapter_names() == expected
