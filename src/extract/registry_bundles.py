"""Bundle catalog for extract dataset schemas."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field

from extract.registry_rows import DATASET_ROWS, DatasetRow
from schema_spec.specs import FieldBundle, call_span_bundle, file_identity_bundle, scip_range_bundle

_BUNDLE_CATALOG: Mapping[str, FieldBundle] = {
    "file_identity": file_identity_bundle(),
    "file_identity_no_sha": file_identity_bundle(include_sha256=False),
    "call_span": call_span_bundle(),
    "scip_range": scip_range_bundle(),
    "scip_range_len": scip_range_bundle(include_len=True),
    "scip_range_enc_len": scip_range_bundle(prefix="enc_", include_len=True),
}


@dataclass(frozen=True)
class OutputBundleSpec:
    """Specification for extractor output bundles."""

    name: str
    outputs: tuple[str, ...]
    template: str | None = None
    ordering: tuple[str, ...] = ()
    dataset_map: Mapping[str, str | None] = field(default_factory=dict)

    def ordered_outputs(self) -> tuple[str, ...]:
        """Return outputs in registry ordering.

        Returns
        -------
        tuple[str, ...]
            Output aliases in order.
        """
        return self.ordering or self.outputs

    def dataset_name_for_output(self, output: str) -> str | None:
        """Return the dataset name for an output alias.

        Returns
        -------
        str | None
            Dataset name, or ``None`` when the output is derived-only.
        """
        if output in self.dataset_map:
            return self.dataset_map[output]
        return _OUTPUT_TO_DATASET.get(output)


_OUTPUT_ALIAS_OVERRIDES: dict[str, str] = {}
_DERIVED_OUTPUTS_BY_BUNDLE: dict[str, tuple[str, ...]] = {}
_BUNDLE_ORDER_OVERRIDES: dict[str, tuple[str, ...]] = {
    "ast_bundle": ("ast_files",),
    "bytecode_bundle": ("bytecode_files",),
    "cst_bundle": ("libcst_files",),
    "scip_bundle": ("scip_index",),
    "tree_sitter_bundle": ("tree_sitter_files",),
    "runtime_inspect_bundle": ("rt_objects", "rt_signatures", "rt_signature_params", "rt_members"),
}


def _output_alias(row: DatasetRow) -> str:
    override = _OUTPUT_ALIAS_OVERRIDES.get(row.name)
    if override is not None:
        return override
    return row.output_name()


def _bundle_templates(rows: tuple[DatasetRow, ...]) -> dict[str, str | None]:
    templates: dict[str, set[str]] = {}
    for row in rows:
        if row.template is None:
            continue
        bundles = set(row.bundles)
        bundles.add(f"{row.template}_bundle")
        for bundle in bundles:
            templates.setdefault(bundle, set()).add(row.template)
    resolved: dict[str, str | None] = {}
    for bundle, values in templates.items():
        if len(values) == 1:
            resolved[bundle] = next(iter(values))
        else:
            resolved[bundle] = None
    return resolved


def _bundle_specs_from_rows(rows: tuple[DatasetRow, ...]) -> tuple[OutputBundleSpec, ...]:
    bundle_outputs: dict[str, list[str]] = {}
    dataset_map: dict[str, str | None] = {}
    for row in rows:
        output = _output_alias(row)
        dataset_map[output] = row.name
        bundles = set(row.bundles)
        if row.template:
            bundles.add(f"{row.template}_bundle")
        for bundle in bundles:
            bundle_outputs.setdefault(bundle, []).append(output)
    for bundle, outputs in _DERIVED_OUTPUTS_BY_BUNDLE.items():
        bundle_outputs.setdefault(bundle, []).extend(outputs)
        for output in outputs:
            dataset_map.setdefault(output, None)
    templates = _bundle_templates(rows)
    specs: list[OutputBundleSpec] = []
    for bundle, outputs in bundle_outputs.items():
        ordering = _BUNDLE_ORDER_OVERRIDES.get(bundle, tuple(outputs))
        mapped = {output: dataset_map.get(output) for output in outputs}
        specs.append(
            OutputBundleSpec(
                name=bundle,
                outputs=tuple(outputs),
                template=templates.get(bundle),
                ordering=ordering,
                dataset_map=mapped,
            )
        )
    return tuple(sorted(specs, key=lambda spec: spec.name))


_OUTPUT_BUNDLES: Mapping[str, OutputBundleSpec] = {
    spec.name: spec for spec in _bundle_specs_from_rows(DATASET_ROWS)
}

_OUTPUT_TO_DATASET: dict[str, str] = {}
_OUTPUT_SKIP: set[str] = set()
_DATASET_NAMES: set[str] = {row.name for row in DATASET_ROWS}
for _bundle in _OUTPUT_BUNDLES.values():
    for _output, _dataset in _bundle.dataset_map.items():
        if _dataset is None:
            _OUTPUT_SKIP.add(_output)
            continue
        existing = _OUTPUT_TO_DATASET.get(_output)
        if existing is None:
            _OUTPUT_TO_DATASET[_output] = _dataset
        elif existing != _dataset:
            msg = f"Extract output {_output!r} mapped to multiple datasets: {existing!r}, {_dataset!r}."
            raise ValueError(msg)


def bundle(name: str) -> FieldBundle:
    """Return a bundle by name.

    Returns
    -------
    FieldBundle
        Bundle definition for the name.
    """
    return _BUNDLE_CATALOG[name]


def output_bundle(name: str) -> OutputBundleSpec:
    """Return an output bundle spec by name.

    Returns
    -------
    OutputBundleSpec
        Output bundle definition for the name.
    """
    return _OUTPUT_BUNDLES[name]


def output_bundles() -> tuple[OutputBundleSpec, ...]:
    """Return all output bundle specs.

    Returns
    -------
    tuple[OutputBundleSpec, ...]
        Output bundle specs in registry order.
    """
    return tuple(_OUTPUT_BUNDLES[name] for name in sorted(_OUTPUT_BUNDLES))


def output_bundle_outputs(name: str) -> tuple[str, ...]:
    """Return the outputs for an output bundle.

    Returns
    -------
    tuple[str, ...]
        Output names in bundle order.
    """
    return output_bundle(name).ordered_outputs()


def dataset_name_for_output(output: str) -> str | None:
    """Return dataset name for an extract output alias.

    Returns
    -------
    str | None
        Dataset name, or ``None`` when the output is derived-only.

    Raises
    ------
    KeyError
        Raised when the output alias is unknown.
    """
    if output in _OUTPUT_SKIP:
        return None
    dataset = _OUTPUT_TO_DATASET.get(output)
    if dataset is not None:
        return dataset
    if output in _DATASET_NAMES:
        return output
    msg = f"Unknown extract output: {output!r}."
    raise KeyError(msg)


__all__ = [
    "OutputBundleSpec",
    "bundle",
    "dataset_name_for_output",
    "output_bundle",
    "output_bundle_outputs",
    "output_bundles",
]
