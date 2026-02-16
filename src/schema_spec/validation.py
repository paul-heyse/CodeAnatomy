"""Schema validation helpers."""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from datafusion_engine.arrow.interop import TableLike
from datafusion_engine.schema.validation import ArrowValidationOptions, validate_table
from schema_spec.dataset_spec import ValidationPolicySpec
from schema_spec.specs import TableSchemaSpec

if TYPE_CHECKING:
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile


def validation_policy_to_arrow_options(
    policy: ValidationPolicySpec | None,
) -> ArrowValidationOptions | None:
    """Convert dataframe validation policy to Arrow validation options.

    Returns:
        ArrowValidationOptions | None: Arrow-native validation options when policy is enabled.
    """
    if policy is None or not policy.enabled:
        return None
    strict: bool | Literal["filter"] = "filter"
    if policy.strict is not None:
        strict = policy.strict
    coerce = bool(policy.coerce) if policy.coerce is not None else False
    return ArrowValidationOptions(
        strict=strict,
        coerce=coerce,
    )


def validate_arrow_table(
    table: TableLike,
    *,
    spec: TableSchemaSpec,
    options: ArrowValidationOptions | None = None,
    runtime_profile: DataFusionRuntimeProfile | None = None,
) -> TableLike:
    """Validate Arrow table content against dataset schema spec.

    Returns:
        TableLike: Validated (and potentially coerced) table.

    Raises:
        ValueError: If strict validation is enabled and validation fails.
    """
    options = options or ArrowValidationOptions()
    report = validate_table(
        table,
        spec=spec,
        options=options,
        runtime_profile=runtime_profile,
    )
    if options.strict is True and not report.valid:
        msg = f"Arrow validation failed for {spec.name!r}."
        raise ValueError(msg)
    return report.validated


__all__ = [
    "ArrowValidationOptions",
    "validate_arrow_table",
    "validation_policy_to_arrow_options",
]
