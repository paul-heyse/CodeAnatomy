"""Hamilton module registry for pipeline stages."""

from __future__ import annotations

from types import ModuleType

from hamilton_pipeline.modules import (
    incremental_plan,
    inputs,
    outputs,
    params,
    plan_catalog,
    task_catalog,
    task_execution,
    task_graph,
)

ALL_MODULES: list[ModuleType] = [
    inputs,
    params,
    task_catalog,
    plan_catalog,
    task_graph,
    incremental_plan,
    task_execution,
    outputs,
]

__all__ = [
    "ALL_MODULES",
    "incremental_plan",
    "inputs",
    "outputs",
    "params",
    "plan_catalog",
    "task_catalog",
    "task_execution",
    "task_graph",
]
