"""Runtime-dependent schema-finalization operations."""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING, Protocol, cast

if TYPE_CHECKING:
    from datafusion_engine.arrow.interop import TableLike
    from datafusion_engine.schema.policy import SchemaPolicy
    from datafusion_engine.schema.validation import ArrowValidationOptions
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from schema_spec.specs import TableSchemaSpec


class _ValidateArrowTable(Protocol):
    def __call__(
        self,
        table: TableLike,
        *,
        spec: TableSchemaSpec,
        options: ArrowValidationOptions,
        runtime_profile: DataFusionRuntimeProfile | None,
    ) -> TableLike: ...


class FinalizeContractLike(Protocol):
    """Structural contract interface for runtime finalize validation."""

    @property
    def schema_spec(self) -> TableSchemaSpec | None:
        """Return schema spec used for Arrow validation."""
        ...

    @property
    def validation(self) -> ArrowValidationOptions | None:
        """Return explicit Arrow validation options, when configured."""
        ...


def _validate_arrow_table(
    table: TableLike,
    *,
    spec: TableSchemaSpec,
    options: ArrowValidationOptions,
    runtime_profile: DataFusionRuntimeProfile | None,
) -> TableLike:
    module = importlib.import_module("schema_spec.dataset_spec")
    validate_fn = cast("_ValidateArrowTable", module.validate_arrow_table)
    return validate_fn(table, spec=spec, options=options, runtime_profile=runtime_profile)


def validate_with_arrow(
    table: TableLike,
    *,
    contract: FinalizeContractLike,
    schema_policy: SchemaPolicy | None = None,
    schema_validation: ArrowValidationOptions | None = None,
    runtime_profile: DataFusionRuntimeProfile | None = None,
) -> TableLike:
    """Validate aligned output through Arrow validation when configured.

    Returns:
        TableLike: Validated table payload.
    """
    if contract.schema_spec is None:
        return table
    options = None
    if schema_policy is not None and schema_policy.validation is not None:
        options = schema_policy.validation
    if options is None:
        options = contract.validation
    if options is None:
        options = schema_validation
    if options is None:
        return table
    return _validate_arrow_table(
        table,
        spec=contract.schema_spec,
        options=options,
        runtime_profile=runtime_profile,
    )


__all__ = ["FinalizeContractLike", "validate_with_arrow"]
