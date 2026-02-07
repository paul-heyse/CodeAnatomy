"""Unit tests for extract executor-map construction."""

from __future__ import annotations

from datafusion_engine.extract.adapter_registry import (
    adapter_executor_key,
    extract_template_adapters,
)
from extract.coordination.evidence_plan import EvidencePlan
from hamilton_pipeline.modules.task_execution import build_extract_executor_map


def test_build_extract_executor_map_includes_all_template_adapters() -> None:
    """Executor-map keys should align with registered extract template adapters."""
    executors = build_extract_executor_map()
    expected = {adapter_executor_key(adapter.name) for adapter in extract_template_adapters()}
    assert set(executors) == expected


def test_build_extract_executor_map_respects_evidence_plan_required_keys() -> None:
    """Executor-map should be filtered when evidence plan declares required keys."""
    required = {
        adapter_executor_key("repo_scan"),
        adapter_executor_key("scip"),
    }
    evidence_plan = EvidencePlan(sources=("repo_files_v1", "scip_index_v1"))
    executors = build_extract_executor_map(evidence_plan=evidence_plan)
    assert set(executors) == required
