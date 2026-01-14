"""Registry for extract pipeline specifications."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass

from arrowdsl.core.interop import TableLike
from extract.registry_ids import (
    add_scip_diagnostic_ids,
    add_scip_document_ids,
    add_scip_occurrence_ids,
    add_scip_relationship_ids,
    add_scip_symbol_ids,
)
from extract.registry_rows import DATASET_ROWS, DatasetRow

KernelFn = Callable[[TableLike], TableLike]


@dataclass(frozen=True)
class ExtractPipelineSpec:
    """Pipeline specification for a dataset's plan and kernel lanes."""

    name: str
    query_ops: tuple[Mapping[str, object], ...] = ()
    post_kernels: tuple[KernelFn, ...] = ()


_POST_KERNELS: Mapping[str, KernelFn] = {
    "scip_documents": lambda table: add_scip_document_ids(table, path_col="path"),
    "scip_occurrences": lambda table: add_scip_occurrence_ids(
        add_scip_document_ids(table, path_col="path")
    ),
    "scip_diagnostics": lambda table: add_scip_diagnostic_ids(
        add_scip_document_ids(table, path_col="path")
    ),
    "scip_symbol_info": lambda table: add_scip_symbol_ids(table, prefix="scip_sym"),
    "scip_external_symbol_info": lambda table: add_scip_symbol_ids(table, prefix="scip_ext_sym"),
    "scip_symbol_relationships": add_scip_relationship_ids,
}


def _post_kernels_for_row(row: DatasetRow) -> tuple[KernelFn, ...]:
    return post_kernels_for_postprocess(row.postprocess)


def post_kernels_for_postprocess(name: str | None) -> tuple[KernelFn, ...]:
    """Return postprocess kernels for the given postprocess key.

    Returns
    -------
    tuple[KernelFn, ...]
        Kernel functions for the postprocess key.

    Raises
    ------
    KeyError
        Raised when the postprocess key is unknown.
    """
    if name is None:
        return ()
    kernel = _POST_KERNELS.get(name)
    if kernel is None:
        msg = f"Unknown postprocess kernel: {name!r}."
        raise KeyError(msg)
    return (kernel,)


def _pipeline_spec_for_row(row: DatasetRow) -> ExtractPipelineSpec:
    return ExtractPipelineSpec(
        name=row.name,
        post_kernels=_post_kernels_for_row(row),
    )


_PIPELINES: Mapping[str, ExtractPipelineSpec] = {
    row.name: _pipeline_spec_for_row(row) for row in DATASET_ROWS
}


def pipeline_spec(name: str) -> ExtractPipelineSpec:
    """Return the pipeline spec for a dataset name.

    Returns
    -------
    ExtractPipelineSpec
        Pipeline spec for the dataset.

    Raises
    ------
    KeyError
        Raised when the dataset name is unknown.
    """
    spec = _PIPELINES.get(name)
    if spec is None:
        msg = f"Unknown extract pipeline: {name!r}."
        raise KeyError(msg)
    return spec


def pipeline_specs() -> tuple[ExtractPipelineSpec, ...]:
    """Return all pipeline specs in registry order.

    Returns
    -------
    tuple[ExtractPipelineSpec, ...]
        Pipeline specs for all datasets.
    """
    return tuple(_PIPELINES[name] for name in sorted(_PIPELINES))


__all__ = [
    "ExtractPipelineSpec",
    "pipeline_spec",
    "pipeline_specs",
    "post_kernels_for_postprocess",
]
