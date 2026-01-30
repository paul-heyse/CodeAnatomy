"""Parameterized output nodes for CPG tables."""

from __future__ import annotations

import logging

from hamilton.function_modifiers import cache, pipe_input, step

from hamilton_pipeline.validators import NonEmptyTableValidator, SchemaContractValidator
from relspec.runtime_artifacts import TableLike

LOGGER = logging.getLogger(__name__)


def _stage_identity(table: TableLike) -> TableLike:
    return table


def _stage_ready(table: TableLike) -> TableLike:
    return table


def _validate_output(table: TableLike, *, dataset_name: str) -> None:
    validators = (
        SchemaContractValidator(dataset_name=dataset_name, importance="fail"),
        NonEmptyTableValidator(),
    )
    for validator in validators:
        result = validator.validate(table)
        if result.passes:
            continue
        if validator.importance == "fail":
            msg = f"Output validation failed for {dataset_name}: {result.message}"
            raise ValueError(msg)
        LOGGER.warning(
            "Output validation warning for %s: %s",
            dataset_name,
            result.message,
        )


@pipe_input(
    step(_stage_identity),
    step(_stage_ready),
    on_input="table",
    namespace="cpg_output",
)
@cache(format="delta", behavior="default")
def cpg_output(
    table: TableLike,
    *,
    dataset_name: str,
) -> TableLike:
    """Return a validated output table for the given dataset.

    Returns
    -------
    TableLike
        Validated output table.
    """
    _validate_output(table, dataset_name=dataset_name)
    return table


__all__ = ["cpg_output"]
