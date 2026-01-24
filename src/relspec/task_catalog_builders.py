"""Default task catalog builders for the inference pipeline."""

from __future__ import annotations

from cpg.task_catalog import cpg_task_catalog
from normalize.task_catalog import normalize_task_catalog
from relspec.relationship_task_catalog import relationship_task_catalog
from relspec.task_catalog import TaskCatalog


def default_task_catalog() -> TaskCatalog:
    """Return the default task catalog for the pipeline.

    Returns
    -------
    TaskCatalog
        Combined task catalog for normalization + CPG outputs.
    """
    normalize_catalog = normalize_task_catalog()
    rel_catalog = relationship_task_catalog()
    cpg_catalog = cpg_task_catalog()
    tasks = (*normalize_catalog.tasks, *rel_catalog.tasks, *cpg_catalog.tasks)
    return TaskCatalog(tasks=tasks)


def build_task_catalog() -> TaskCatalog:
    """Return the default task catalog (compatibility alias).

    Returns
    -------
    TaskCatalog
        Combined task catalog for normalization + CPG outputs.
    """
    return default_task_catalog()


__all__ = ["build_task_catalog", "default_task_catalog"]
