"""Immutability contract smoke tests for frozen dataclasses and structs.

Scope: Smoke assertions validating mutation prevention on key frozen
types used across subsystem boundaries. Individual type behavior is
unit-level; these verify the immutability invariant holds for types
that affect cross-component contracts.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import FrozenInstanceError, dataclass

import pytest

from tests.test_helpers.immutability import assert_immutable_assignment


@dataclass(frozen=True)
class _MutationCase:
    factory: Callable[[], object]
    attribute: str
    attempted_value: object
    expected_exception: type[BaseException]
    expected_value: object


def _cdf_cursor() -> object:
    from semantics.incremental.cdf_cursors import CdfCursor

    return CdfCursor(dataset_name="test", last_version=1, last_timestamp=None)


def _cdf_cursor_create() -> object:
    from semantics.incremental.cdf_cursors import CdfCursor

    return CdfCursor.create("test_ds", version=5)


def _semantic_runtime_config_default() -> object:
    from semantics.runtime import SemanticRuntimeConfig

    return SemanticRuntimeConfig(output_locations={})


def _semantic_runtime_config_custom() -> object:
    from semantics.runtime import SemanticRuntimeConfig

    return SemanticRuntimeConfig(
        output_locations={"view_a": "/tmp/a"},
        schema_evolution_enabled=True,
    )


def _inferred_deps_default() -> object:
    from relspec.inferred_deps import InferredDeps

    return InferredDeps(task_name="t", output="o", inputs=())


def _inferred_deps_custom() -> object:
    from relspec.inferred_deps import InferredDeps

    return InferredDeps(task_name="task_a", output="out_a", inputs=("ev1",))


def _file_context() -> object:
    from extract.coordination.context import FileContext

    return FileContext(file_id="f", path="p", abs_path=None, file_sha256=None)


def _evidence_plan() -> object:
    from extract.coordination.evidence_plan import EvidencePlan

    return EvidencePlan(sources=("cst_refs",))


def _evidence_requirement() -> object:
    from extract.coordination.evidence_plan import EvidenceRequirement

    return EvidenceRequirement(name="test_ds", required_columns=("col_a",))


def _graph_node() -> object:
    from relspec.rustworkx_graph import EvidenceNode, GraphNode

    return GraphNode(kind="evidence", payload=EvidenceNode(name="ev1"))


def _task_node() -> object:
    from relspec.rustworkx_graph import TaskNode

    return TaskNode(
        name="t1",
        output="out1",
        inputs=("ev1",),
        sources=("src1",),
        priority=0,
        task_kind="extract",
    )


def _extract_execution_options() -> object:
    from extract.coordination.spec_helpers import ExtractExecutionOptions

    return ExtractExecutionOptions(module_allowlist=(), feature_flags={})


_CASES: tuple[tuple[str, _MutationCase], ...] = (
    (
        "cdf_cursor.last_version",
        _MutationCase(
            factory=_cdf_cursor,
            attribute="last_version",
            attempted_value=2,
            expected_exception=FrozenInstanceError,
            expected_value=1,
        ),
    ),
    (
        "cdf_cursor.dataset_name",
        _MutationCase(
            factory=_cdf_cursor_create,
            attribute="dataset_name",
            attempted_value="other",
            expected_exception=FrozenInstanceError,
            expected_value="test_ds",
        ),
    ),
    (
        "semantic_runtime_config.cdf_enabled",
        _MutationCase(
            factory=_semantic_runtime_config_default,
            attribute="cdf_enabled",
            attempted_value=True,
            expected_exception=FrozenInstanceError,
            expected_value=False,
        ),
    ),
    (
        "semantic_runtime_config.schema_evolution_enabled",
        _MutationCase(
            factory=_semantic_runtime_config_custom,
            attribute="schema_evolution_enabled",
            attempted_value=False,
            expected_exception=FrozenInstanceError,
            expected_value=True,
        ),
    ),
    (
        "inferred_deps.inputs",
        _MutationCase(
            factory=_inferred_deps_default,
            attribute="inputs",
            attempted_value=("new",),
            expected_exception=AttributeError,
            expected_value=(),
        ),
    ),
    (
        "inferred_deps.task_name",
        _MutationCase(
            factory=_inferred_deps_custom,
            attribute="task_name",
            attempted_value="task_b",
            expected_exception=AttributeError,
            expected_value="task_a",
        ),
    ),
    (
        "file_context.path",
        _MutationCase(
            factory=_file_context,
            attribute="path",
            attempted_value="new_path",
            expected_exception=FrozenInstanceError,
            expected_value="p",
        ),
    ),
    (
        "evidence_plan.sources",
        _MutationCase(
            factory=_evidence_plan,
            attribute="sources",
            attempted_value=("other",),
            expected_exception=FrozenInstanceError,
            expected_value=("cst_refs",),
        ),
    ),
    (
        "evidence_requirement.name",
        _MutationCase(
            factory=_evidence_requirement,
            attribute="name",
            attempted_value="other",
            expected_exception=FrozenInstanceError,
            expected_value="test_ds",
        ),
    ),
    (
        "graph_node.kind",
        _MutationCase(
            factory=_graph_node,
            attribute="kind",
            attempted_value="task",
            expected_exception=AttributeError,
            expected_value="evidence",
        ),
    ),
    (
        "task_node.priority",
        _MutationCase(
            factory=_task_node,
            attribute="priority",
            attempted_value=10,
            expected_exception=AttributeError,
            expected_value=0,
        ),
    ),
    (
        "extract_execution_options.module_allowlist",
        _MutationCase(
            factory=_extract_execution_options,
            attribute="module_allowlist",
            attempted_value=("mod",),
            expected_exception=FrozenInstanceError,
            expected_value=(),
        ),
    ),
)


@pytest.mark.integration
class TestImmutabilityContracts:
    """Tests for immutability enforcement on frozen dataclasses and structs."""

    @pytest.mark.parametrize(
        ("_case_id", "case"),
        _CASES,
    )
    def test_immutable_assignments_raise_and_preserve_values(
        self,
        _case_id: str,
        case: _MutationCase,
    ) -> None:
        """Verify immutable contracts reject assignment and preserve values."""
        _ = _case_id
        assert_immutable_assignment(
            factory=case.factory,
            attribute=case.attribute,
            attempted_value=case.attempted_value,
            expected_exception=case.expected_exception,
            expected_value=case.expected_value,
        )
