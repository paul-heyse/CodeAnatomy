"""Unit tests for authority-backed extract task dispatch."""

from __future__ import annotations

from types import SimpleNamespace
from typing import cast

import pytest

from hamilton_pipeline.modules import task_execution
from hamilton_pipeline.modules.task_execution import TaskExecutionInputs, TaskExecutionSpec


def test_require_execution_authority_rejects_missing() -> None:
    """Missing authority should fail fast with a deterministic error."""
    inputs = cast(
        "TaskExecutionInputs",
        SimpleNamespace(execution_authority_context=None),
    )
    with pytest.raises(ValueError, match="ExecutionAuthorityContext is required"):
        task_execution._require_execution_authority(inputs)


def test_execute_extract_task_dispatches_via_authority_executor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Extract dispatch should resolve handlers through authority executor lookup."""
    called: dict[str, object] = {}

    class _Authority:
        def executor_for_adapter(self, adapter_name: str):
            called["adapter_name"] = adapter_name

            def _handler(
                inputs: object,
                extract_session: object,
                profile_name: str,
            ) -> dict[str, object]:
                called["inputs"] = inputs
                called["extract_session"] = extract_session
                called["profile_name"] = profile_name
                return {"extract_output": "ok"}

            return _handler

    class _ExtractSession:
        def __init__(self, *, engine_session: object) -> None:
            self.engine_session = engine_session

    monkeypatch.setattr(
        task_execution,
        "_require_execution_authority",
        lambda _inputs: _Authority(),
    )
    monkeypatch.setattr(
        "relspec.extract_plan.extract_output_task_map",
        lambda: {"extract_output": SimpleNamespace(extractor="repo_scan")},
    )
    monkeypatch.setattr(
        "datafusion_engine.extract.adapter_registry.extract_template_adapter",
        lambda _extractor: SimpleNamespace(name="repo_scan"),
    )
    monkeypatch.setattr("extract.session.ExtractSession", _ExtractSession)
    monkeypatch.setattr(task_execution, "_normalize_extract_outputs", lambda outputs: dict(outputs))
    monkeypatch.setattr(
        task_execution,
        "_ensure_extract_output",
        lambda **_kwargs: None,
    )

    inputs = cast(
        "TaskExecutionInputs",
        SimpleNamespace(
            runtime=SimpleNamespace(execution=object()),
            engine_session=SimpleNamespace(
                datafusion_profile=SimpleNamespace(
                    policies=SimpleNamespace(config_policy_name="test_profile"),
                )
            ),
            execution_authority_context=object(),
            plan_signature="plan:test",
        ),
    )
    spec = TaskExecutionSpec(
        task_name="extract_output",
        task_output="extract_output",
        plan_fingerprint="fp",
        plan_task_signature="sig",
        task_kind="extract",
    )

    output = task_execution._execute_extract_task(inputs, spec=spec)

    assert called["adapter_name"] == "repo_scan"
    assert called["profile_name"] == "test_profile"
    assert output == {"extract_output": "ok"}
