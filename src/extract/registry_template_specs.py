"""Template specifications for extract dataset families."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field


@dataclass(frozen=True)
class DatasetTemplateSpec:
    """Specification for expanding dataset-family templates."""

    name: str
    template: str
    params: Mapping[str, object] = field(default_factory=dict)


DATASET_TEMPLATE_SPECS: tuple[DatasetTemplateSpec, ...] = (
    DatasetTemplateSpec(name="tree_sitter_core", template="tree_sitter"),
    DatasetTemplateSpec(name="runtime_inspect_core", template="runtime_inspect"),
)


__all__ = ["DATASET_TEMPLATE_SPECS", "DatasetTemplateSpec"]
