"""Template specifications for extract dataset families."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from functools import cache


@dataclass(frozen=True)
class DatasetTemplateSpec:
    """Specification for expanding dataset-family templates."""

    name: str
    template: str
    params: Mapping[str, object] = field(default_factory=dict)


@cache
def dataset_template_specs() -> tuple[DatasetTemplateSpec, ...]:
    """Return extract dataset template specs.

    Returns
    -------
    tuple[DatasetTemplateSpec, ...]
        Dataset template specs for extract registries.
    """
    return (
        DatasetTemplateSpec(name="tree_sitter_core", template="tree_sitter"),
        DatasetTemplateSpec(name="runtime_inspect_core", template="runtime_inspect"),
        DatasetTemplateSpec(name="repo_scan_core", template="repo_scan"),
        DatasetTemplateSpec(name="ast_core", template="ast"),
        DatasetTemplateSpec(name="cst_core", template="cst"),
        DatasetTemplateSpec(name="bytecode_core", template="bytecode"),
        DatasetTemplateSpec(name="symtable_core", template="symtable"),
        DatasetTemplateSpec(name="scip_core", template="scip"),
    )


__all__ = ["DatasetTemplateSpec", "dataset_template_specs"]
