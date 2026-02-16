# ruff: noqa: D103
"""Tests for execution-policy request contract typing."""

from __future__ import annotations

from relspec.contracts import CompileExecutionPolicyRequestV1


def test_compile_execution_policy_request_annotations_are_typed() -> None:
    annotations = CompileExecutionPolicyRequestV1.__annotations__
    assert "task_graph" in annotations
    assert "runtime_profile" in annotations
