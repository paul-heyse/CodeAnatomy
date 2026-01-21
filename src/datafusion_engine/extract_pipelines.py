"""Pipeline registry for extract postprocess kernels."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass

from arrowdsl.core.interop import TableLike

KernelFn = Callable[[TableLike], TableLike]


@dataclass(frozen=True)
class ExtractPipelineSpec:
    """Pipeline specification for a dataset's plan and kernel lanes."""

    name: str
    query_ops: tuple[Mapping[str, object], ...] = ()
    post_kernels: tuple[KernelFn, ...] = ()


_POST_KERNELS: Mapping[str, KernelFn] = {}


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


def pipeline_spec(name: str) -> ExtractPipelineSpec:
    """Return the pipeline spec for a dataset name.

    Returns
    -------
    ExtractPipelineSpec
        Pipeline spec for the dataset.
    """
    return ExtractPipelineSpec(name=name)


__all__ = [
    "ExtractPipelineSpec",
    "pipeline_spec",
    "post_kernels_for_postprocess",
]
