"""Tests for task and plan catalog helpers."""

from __future__ import annotations

import ibis
import pytest

from arrowdsl.core.execution_context import execution_context_factory
from ibis_engine.execution_factory import ibis_backend_from_ctx
from ibis_engine.plan import IbisPlan
from relspec.plan_catalog import compile_task_catalog, compile_task_plan
from relspec.task_catalog import TaskCatalog, TaskSpec

pytest.importorskip("datafusion")


def _memtable_plan() -> IbisPlan:
    expr = ibis.memtable({"x": [1]})
    return IbisPlan(expr=expr)


def _build_memtable(_: object) -> IbisPlan:
    return _memtable_plan()


def test_task_catalog_indexing() -> None:
    """Index task catalog by name and output."""
    task_one = TaskSpec(name="task.one", output="out_one", build=_build_memtable)
    task_two = TaskSpec(name="task.two", output="out_two", build=_build_memtable)
    catalog = TaskCatalog(tasks=(task_one, task_two))
    assert catalog.by_name()["task.one"] is task_one
    assert catalog.by_output()["out_two"] is task_two


def test_compile_task_plan_infers_inputs() -> None:
    """Compile a plan and infer dependencies from Ibis expressions."""
    ctx = execution_context_factory("default")
    backend = ibis_backend_from_ctx(ctx)

    def _build(_: object) -> IbisPlan:
        table = ibis.table({"col_a": "string", "col_b": "int64"}, name="source_table")
        expr = table.select("col_a")
        return IbisPlan(expr=expr)

    task = TaskSpec(name="task.infer", output="out_table", build=_build)
    artifact = compile_task_plan(task, backend=backend, ctx=ctx)
    assert "source_table" in artifact.deps.inputs
    required_cols = {
        col for cols in artifact.deps.required_columns.values() for col in cols
    }
    assert "col_a" in required_cols
    assert artifact.plan_fingerprint


def test_compile_task_catalog_builds_artifacts() -> None:
    """Compile a catalog into plan artifacts."""
    ctx = execution_context_factory("default")
    backend = ibis_backend_from_ctx(ctx)
    task_one = TaskSpec(name="task.one", output="out_one", build=_build_memtable)
    task_two = TaskSpec(name="task.two", output="out_two", build=_build_memtable)
    catalog = TaskCatalog(tasks=(task_one, task_two))
    plan_catalog = compile_task_catalog(catalog, backend=backend, ctx=ctx)
    by_task = plan_catalog.by_task()
    assert by_task["task.one"].task.output == "out_one"
    assert by_task["task.two"].plan_fingerprint
