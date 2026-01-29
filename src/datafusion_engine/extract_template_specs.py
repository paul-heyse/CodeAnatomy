"""Template specs for extract dataset metadata expansion."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field


@dataclass(frozen=True)
class DatasetTemplateSpec:
    """Minimal template spec for extract dataset metadata expansion."""

    name: str
    template: str
    params: Mapping[str, object] = field(default_factory=dict)


_TEMPLATE_NAMES: tuple[str, ...] = (
    "repo_scan",
    "ast",
    "cst",
    "bytecode",
    "symtable",
    "scip",
    "tree_sitter",
    "python_imports",
    "python_external",
)


def dataset_template_specs() -> tuple[DatasetTemplateSpec, ...]:
    """Return extract dataset template specs in registry order.

    Returns
    -------
    tuple[DatasetTemplateSpec, ...]
        Dataset template specs for metadata expansion.
    """
    return tuple(DatasetTemplateSpec(name=name, template=name) for name in _TEMPLATE_NAMES)


__all__ = ["DatasetTemplateSpec", "dataset_template_specs"]
