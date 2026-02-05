"""Immutability contract smoke tests for frozen dataclasses and structs.

Scope: Smoke assertions validating mutation prevention on key frozen
types used across subsystem boundaries. Individual type behavior is
unit-level; these verify the immutability invariant holds for types
that affect cross-component contracts.
"""

from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest


@pytest.mark.integration
class TestImmutabilityContracts:
    """Tests for immutability enforcement on frozen dataclasses and structs."""

    def test_cdf_cursor_mutation_raises_frozen_error(self) -> None:
        """Verify CdfCursor raises FrozenInstanceError on mutation.

        CdfCursor uses a custom __setattr__ that explicitly raises
        FrozenInstanceError (from dataclasses), even though the underlying
        msgspec.Struct is frozen=False.
        """
        from semantics.incremental.cdf_cursors import CdfCursor

        cursor = CdfCursor(dataset_name="test", last_version=1, last_timestamp=None)
        with pytest.raises(FrozenInstanceError):
            cursor.last_version = 2  # type: ignore[misc]

    def test_cdf_cursor_dataset_name_immutable(self) -> None:
        """Verify CdfCursor.dataset_name is also immutable."""
        from semantics.incremental.cdf_cursors import CdfCursor

        cursor = CdfCursor.create("test_ds", version=5)
        with pytest.raises(FrozenInstanceError):
            cursor.dataset_name = "other"  # type: ignore[misc]

    def test_semantic_runtime_config_is_frozen(self) -> None:
        """Verify SemanticRuntimeConfig is immutable.

        Uses @dataclass(frozen=True), so raises FrozenInstanceError.
        """
        from semantics.runtime import SemanticRuntimeConfig

        config = SemanticRuntimeConfig(output_locations={})
        with pytest.raises(FrozenInstanceError):
            config.cdf_enabled = True  # type: ignore[misc]

    def test_semantic_runtime_config_output_locations_immutable(self) -> None:
        """Verify SemanticRuntimeConfig field assignment blocked."""
        from semantics.runtime import SemanticRuntimeConfig

        config = SemanticRuntimeConfig(
            output_locations={"view_a": "/tmp/a"},
            schema_evolution_enabled=True,
        )
        with pytest.raises(FrozenInstanceError):
            config.schema_evolution_enabled = False  # type: ignore[misc]

    def test_inferred_deps_is_frozen(self) -> None:
        """Verify InferredDeps is immutable.

        Uses StructBaseStrict with frozen=True (msgspec), raises AttributeError.
        """
        from relspec.inferred_deps import InferredDeps

        deps = InferredDeps(task_name="t", output="o", inputs=())
        with pytest.raises(AttributeError):
            deps.inputs = ("new",)  # type: ignore[misc]

    def test_inferred_deps_task_name_immutable(self) -> None:
        """Verify InferredDeps.task_name cannot be reassigned."""
        from relspec.inferred_deps import InferredDeps

        deps = InferredDeps(task_name="task_a", output="out_a", inputs=("ev1",))
        with pytest.raises(AttributeError):
            deps.task_name = "task_b"  # type: ignore[misc]

    def test_file_context_is_frozen(self) -> None:
        """Verify FileContext is immutable.

        Uses @dataclass(frozen=True), so raises FrozenInstanceError.
        """
        from extract.coordination.context import FileContext

        ctx = FileContext(file_id="f", path="p", abs_path=None, file_sha256=None)
        with pytest.raises(FrozenInstanceError):
            ctx.path = "new_path"  # type: ignore[misc]

    def test_evidence_plan_is_frozen(self) -> None:
        """Verify EvidencePlan is immutable.

        Uses @dataclass(frozen=True).
        """
        from extract.coordination.evidence_plan import EvidencePlan

        plan = EvidencePlan(sources=("cst_refs",))
        with pytest.raises(FrozenInstanceError):
            plan.sources = ("other",)  # type: ignore[misc]

    def test_evidence_requirement_is_frozen(self) -> None:
        """Verify EvidenceRequirement is immutable."""
        from extract.coordination.evidence_plan import EvidenceRequirement

        req = EvidenceRequirement(name="test_ds", required_columns=("col_a",))
        with pytest.raises(FrozenInstanceError):
            req.name = "other"  # type: ignore[misc]

    def test_graph_node_is_frozen(self) -> None:
        """Verify GraphNode is immutable (StructBaseStrict, frozen=True)."""
        from relspec.rustworkx_graph import EvidenceNode, GraphNode

        node = GraphNode(kind="evidence", payload=EvidenceNode(name="ev1"))
        with pytest.raises(AttributeError):
            node.kind = "task"  # type: ignore[misc]

    def test_task_node_is_frozen(self) -> None:
        """Verify TaskNode is immutable (StructBaseStrict, frozen=True)."""
        from relspec.rustworkx_graph import TaskNode

        task = TaskNode(
            name="t1",
            output="out1",
            inputs=("ev1",),
            sources=("src1",),
            priority=0,
            task_kind="extract",
        )
        with pytest.raises(AttributeError):
            task.priority = 10  # type: ignore[misc]

    def test_extract_execution_options_is_frozen(self) -> None:
        """Verify ExtractExecutionOptions is immutable."""
        from extract.coordination.spec_helpers import ExtractExecutionOptions

        opts = ExtractExecutionOptions(module_allowlist=(), feature_flags={})
        with pytest.raises(FrozenInstanceError):
            opts.module_allowlist = ("mod",)  # type: ignore[misc]
