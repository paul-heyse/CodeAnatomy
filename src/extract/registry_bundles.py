"""Bundle catalog for extract dataset schemas."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field

from extract.registry_rows import DATASET_ROWS
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


_OUTPUT_BUNDLES: Mapping[str, OutputBundleSpec] = {
    "ast_bundle": OutputBundleSpec(
        name="ast_bundle",
        outputs=("ast_nodes", "ast_edges", "ast_defs"),
        template="ast",
        ordering=("ast_nodes", "ast_edges", "ast_defs"),
        dataset_map={"ast_defs": None},
    ),
    "cst_bundle": OutputBundleSpec(
        name="cst_bundle",
        outputs=(
            "cst_parse_manifest",
            "cst_parse_errors",
            "cst_name_refs",
            "cst_imports",
            "cst_callsites",
            "cst_defs",
            "cst_type_exprs",
        ),
        template="cst",
        ordering=(
            "cst_parse_manifest",
            "cst_parse_errors",
            "cst_name_refs",
            "cst_imports",
            "cst_callsites",
            "cst_defs",
            "cst_type_exprs",
        ),
    ),
    "scip_bundle": OutputBundleSpec(
        name="scip_bundle",
        outputs=(
            "scip_metadata",
            "scip_documents",
            "scip_occurrences",
            "scip_symbol_information",
            "scip_symbol_relationships",
            "scip_external_symbol_information",
            "scip_diagnostics",
        ),
        template="scip",
        ordering=(
            "scip_metadata",
            "scip_documents",
            "scip_occurrences",
            "scip_symbol_information",
            "scip_symbol_relationships",
            "scip_external_symbol_information",
            "scip_diagnostics",
        ),
        dataset_map={
            "scip_symbol_information": "scip_symbol_info_v1",
            "scip_external_symbol_information": "scip_external_symbol_info_v1",
        },
    ),
    "bytecode_bundle": OutputBundleSpec(
        name="bytecode_bundle",
        outputs=(
            "py_bc_code_units",
            "py_bc_instructions",
            "py_bc_exception_table",
            "py_bc_blocks",
            "py_bc_cfg_edges",
            "py_bc_errors",
        ),
        template="bytecode",
        ordering=(
            "py_bc_code_units",
            "py_bc_instructions",
            "py_bc_exception_table",
            "py_bc_blocks",
            "py_bc_cfg_edges",
            "py_bc_errors",
        ),
    ),
    "tree_sitter_bundle": OutputBundleSpec(
        name="tree_sitter_bundle",
        outputs=("ts_nodes", "ts_errors", "ts_missing"),
        template="tree_sitter",
        ordering=("ts_nodes", "ts_errors", "ts_missing"),
    ),
    "runtime_inspect_bundle": OutputBundleSpec(
        name="runtime_inspect_bundle",
        outputs=("rt_objects", "rt_signatures", "rt_signature_params", "rt_members"),
        template="runtime_inspect",
        ordering=("rt_objects", "rt_signatures", "rt_signature_params", "rt_members"),
    ),
}

_OUTPUT_TO_DATASET: dict[str, str] = {row.output_name(): row.name for row in DATASET_ROWS}
_OUTPUT_TO_DATASET_OVERRIDES: dict[str, str | None] = {}
for _bundle in _OUTPUT_BUNDLES.values():
    _OUTPUT_TO_DATASET_OVERRIDES.update(_bundle.dataset_map)
_OUTPUT_SKIP: set[str] = {
    output for output, dataset in _OUTPUT_TO_DATASET_OVERRIDES.items() if dataset is None
}
for _output, _dataset in _OUTPUT_TO_DATASET_OVERRIDES.items():
    if _dataset is None:
        _OUTPUT_TO_DATASET.pop(_output, None)
    else:
        _OUTPUT_TO_DATASET[_output] = _dataset


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
    if dataset is None:
        msg = f"Unknown extract output: {output!r}."
        raise KeyError(msg)
    return dataset


__all__ = [
    "OutputBundleSpec",
    "bundle",
    "dataset_name_for_output",
    "output_bundle",
    "output_bundle_outputs",
    "output_bundles",
]
