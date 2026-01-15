"""Extractor registry describing capabilities and outputs."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass

from extract.registry_rows import DATASET_ROWS, DatasetRow

_REQUIRED_INPUTS: dict[str, tuple[str, ...]] = {
    "ast": ("repo_files",),
    "cst": ("repo_files",),
    "tree_sitter": ("repo_files",),
    "bytecode": ("repo_files",),
    "symtable": ("repo_files",),
    "runtime_inspect": ("repo_root",),
    "scip": ("scip_index_path", "repo_root"),
    "repo_scan": ("repo_root",),
}

_SUPPORTS_PLAN: dict[str, bool] = {
    "ast": True,
    "cst": True,
    "tree_sitter": True,
    "bytecode": True,
    "symtable": True,
    "runtime_inspect": True,
    "scip": True,
    "repo_scan": False,
}
_SUPPORTS_IBIS: dict[str, bool] = {
    "ast": True,
    "cst": True,
    "tree_sitter": True,
    "bytecode": True,
    "symtable": True,
    "runtime_inspect": True,
    "scip": True,
    "repo_scan": False,
}


@dataclass(frozen=True)
class ExtractorSpec:
    """Programmatic extractor capability spec."""

    name: str
    template: str
    outputs: tuple[str, ...]
    optional_outputs: tuple[str, ...] = ()
    required_inputs: tuple[str, ...] = ()
    supports_plan: bool = False
    supports_ibis: bool = False


def _rows_by_template() -> Mapping[str, tuple[DatasetRow, ...]]:
    grouped: dict[str, list[DatasetRow]] = {}
    for row in DATASET_ROWS:
        if row.template is None:
            continue
        grouped.setdefault(row.template, []).append(row)
    return {key: tuple(rows) for key, rows in grouped.items()}


def _outputs_for_rows(rows: Sequence[DatasetRow]) -> tuple[str, ...]:
    return tuple(row.output_name() for row in rows)


def _optional_outputs(rows: Sequence[DatasetRow]) -> tuple[str, ...]:
    return tuple(row.output_name() for row in rows if row.enabled_when is not None)


_EXTRACTOR_SPECS: dict[str, ExtractorSpec] = {}
for _template, _rows in _rows_by_template().items():
    _outputs = _outputs_for_rows(_rows)
    _EXTRACTOR_SPECS[_template] = ExtractorSpec(
        name=_template,
        template=_template,
        outputs=_outputs,
        optional_outputs=_optional_outputs(_rows),
        required_inputs=_REQUIRED_INPUTS.get(_template, ()),
        supports_plan=_SUPPORTS_PLAN.get(_template, False),
        supports_ibis=_SUPPORTS_IBIS.get(_template, False),
    )

_OUTPUT_TO_TEMPLATE: dict[str, str] = {
    row.output_name(): row.template for row in DATASET_ROWS if row.template is not None
}


def extractor_spec(name: str) -> ExtractorSpec:
    """Return an extractor spec by name.

    Returns
    -------
    ExtractorSpec
        Extractor spec for the name.
    """
    return _EXTRACTOR_SPECS[name]


def extractor_specs() -> tuple[ExtractorSpec, ...]:
    """Return all extractor specs in registry order.

    Returns
    -------
    tuple[ExtractorSpec, ...]
        Extractor specs.
    """
    return tuple(_EXTRACTOR_SPECS[name] for name in sorted(_EXTRACTOR_SPECS))


def extractor_for_output(output: str) -> ExtractorSpec | None:
    """Return the extractor spec responsible for an output.

    Returns
    -------
    ExtractorSpec | None
        Extractor spec, or None when unknown.
    """
    template = _OUTPUT_TO_TEMPLATE.get(output)
    if template is None:
        return None
    return _EXTRACTOR_SPECS.get(template)


def outputs_for_template(template: str) -> tuple[str, ...]:
    """Return outputs for a template.

    Returns
    -------
    tuple[str, ...]
        Output aliases for the template.
    """
    return extractor_spec(template).outputs


def select_extractors_for_outputs(outputs: Iterable[str]) -> tuple[ExtractorSpec, ...]:
    """Return extractor specs required for the outputs.

    Returns
    -------
    tuple[ExtractorSpec, ...]
        Extractor specs required for the outputs.
    """
    required: dict[str, ExtractorSpec] = {}
    for output in outputs:
        spec = extractor_for_output(output)
        if spec is None:
            continue
        required[spec.name] = spec
    return tuple(required[name] for name in sorted(required))


__all__ = [
    "ExtractorSpec",
    "extractor_for_output",
    "extractor_spec",
    "extractor_specs",
    "outputs_for_template",
    "select_extractors_for_outputs",
]
