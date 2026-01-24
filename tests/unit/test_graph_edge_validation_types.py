"""Tests for edge validation with required types and metadata."""

from __future__ import annotations

from relspec.evidence import EvidenceCatalog
from relspec.graph_edge_validation import (
    validate_edge_requirements,
    validate_edge_requirements_detailed,
)
from relspec.inferred_deps import InferredDeps
from relspec.rustworkx_graph import TaskGraph, build_task_graph_from_inferred_deps


def _build_graph() -> tuple[TaskGraph, int]:
    deps = InferredDeps(
        task_name="task.alpha",
        output="out_alpha",
        inputs=("input_table",),
        required_columns={"input_table": ("col_a",)},
        required_types={"input_table": (("col_a", "string"),)},
        required_metadata={"input_table": ((b"owner", b"normalize"),)},
        plan_fingerprint="fp-alpha",
    )
    graph = build_task_graph_from_inferred_deps((deps,))
    task_idx = graph.task_idx["task.alpha"]
    return graph, task_idx


def test_validate_edge_requirements_types_metadata() -> None:
    """Validate required types/metadata against evidence."""
    graph, task_idx = _build_graph()
    evidence = EvidenceCatalog(sources={"input_table"})
    evidence.columns_by_dataset["input_table"] = {"col_a"}
    evidence.types_by_dataset["input_table"] = {"col_a": "string"}
    evidence.metadata_by_dataset["input_table"] = {b"owner": b"normalize"}
    assert validate_edge_requirements(graph, task_idx, catalog=evidence) is True


def test_validate_edge_requirements_type_mismatch() -> None:
    """Detect a missing type requirement."""
    graph, task_idx = _build_graph()
    evidence = EvidenceCatalog(sources={"input_table"})
    evidence.columns_by_dataset["input_table"] = {"col_a"}
    evidence.types_by_dataset["input_table"] = {"col_a": "int64"}
    evidence.metadata_by_dataset["input_table"] = {b"owner": b"normalize"}
    result = validate_edge_requirements_detailed(graph, task_idx, catalog=evidence)
    assert result.is_valid is False
    assert result.edge_results[0].missing_types == (("col_a", "string"),)


def test_validate_edge_requirements_metadata_mismatch() -> None:
    """Detect a missing metadata requirement."""
    graph, task_idx = _build_graph()
    evidence = EvidenceCatalog(sources={"input_table"})
    evidence.columns_by_dataset["input_table"] = {"col_a"}
    evidence.types_by_dataset["input_table"] = {"col_a": "string"}
    evidence.metadata_by_dataset["input_table"] = {b"owner": b"other"}
    result = validate_edge_requirements_detailed(graph, task_idx, catalog=evidence)
    assert result.is_valid is False
    assert result.edge_results[0].missing_metadata == ((b"owner", b"normalize"),)
