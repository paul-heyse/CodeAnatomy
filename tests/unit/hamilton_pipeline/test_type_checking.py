"""Unit tests for Hamilton runtime type checking helpers."""

from __future__ import annotations

from collections.abc import Mapping

import pytest
from hamilton import htypes
from src.hamilton_pipeline.type_checking import CodeAnatomyTypeChecker


def test_run_after_node_execution_accepts_mapping_union() -> None:
    checker = CodeAnatomyTypeChecker()
    checker.run_after_node_execution(
        node_name="param_table_delta_paths",
        node_return_type=Mapping[str, str] | None,
        result={},
    )


def test_run_after_node_execution_rejects_invalid_mapping_values() -> None:
    checker = CodeAnatomyTypeChecker()
    with pytest.raises(TypeError, match="param_table_delta_paths"):
        checker.run_after_node_execution(
            node_name="param_table_delta_paths",
            node_return_type=Mapping[str, str] | None,
            result={"events": 1},
        )


def test_run_before_node_execution_accepts_mapping_union() -> None:
    checker = CodeAnatomyTypeChecker()
    checker.run_before_node_execution(
        node_name="session_runtime_context",
        node_kwargs={"param_table_delta_paths": {}},
        node_input_types={"param_table_delta_paths": Mapping[str, str] | None},
    )


def test_run_before_node_execution_rejects_invalid_mapping_values() -> None:
    checker = CodeAnatomyTypeChecker()
    with pytest.raises(TypeError, match="param_table_delta_paths"):
        checker.run_before_node_execution(
            node_name="session_runtime_context",
            node_kwargs={"param_table_delta_paths": {"events": 1}},
            node_input_types={"param_table_delta_paths": Mapping[str, str] | None},
        )


def test_run_after_node_execution_accepts_parallelizable_output() -> None:
    checker = CodeAnatomyTypeChecker()
    checker.run_after_node_execution(
        node_name="scan_unit_stream",
        node_return_type=htypes.Parallelizable[str],
        result=["scan::a", "scan::b"],
    )


def test_run_after_node_execution_rejects_parallelizable_element_type() -> None:
    checker = CodeAnatomyTypeChecker()
    with pytest.raises(TypeError, match="scan_unit_stream"):
        checker.run_after_node_execution(
            node_name="scan_unit_stream",
            node_return_type=htypes.Parallelizable[str],
            result=[1, 2],
        )
