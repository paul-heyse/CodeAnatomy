"""Pandera schema bridge for msgspec table specifications."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import pyarrow as pa

from schema_spec.arrow_types import ArrowTypeBase, arrow_type_to_pyarrow
from schema_spec.field_spec import FieldSpec
from schema_spec.specs import TableSchemaSpec
from schema_spec.system import ValidationPolicySpec


def _arrow_to_pandas_dtype(dtype: pa.DataType) -> object:
    to_pandas = getattr(dtype, "to_pandas_dtype", None)
    if callable(to_pandas):
        try:
            return to_pandas()
        except (TypeError, ValueError):
            return object
    return object


def field_to_pandera_dtype(field: FieldSpec) -> object:
    """Return a pandas dtype suitable for pandera from a field spec.

    Parameters
    ----------
    field
        Field specification describing the column type.

    Returns
    -------
    object
        Pandas dtype object usable with pandera.
    """
    dtype = field.dtype
    if isinstance(dtype, ArrowTypeBase):
        arrow_dtype = arrow_type_to_pyarrow(dtype)
    elif isinstance(dtype, pa.DataType):
        arrow_dtype = dtype
    else:
        arrow_dtype = None
    if arrow_dtype is None:
        return object
    return _arrow_to_pandas_dtype(arrow_dtype)


def to_pandera_schema(
    spec: TableSchemaSpec,
    *,
    policy: ValidationPolicySpec | None = None,
) -> Any:
    """Build a pandera DataFrameSchema from a table schema spec.

    Parameters
    ----------
    spec
        Table schema specification to convert.
    policy
        Optional validation policy (unused beyond configuration).

    Returns
    -------
    Any
        Pandera DataFrameSchema instance.
    """
    import pandera.pandas as pa_schema

    columns = {
        field.name: pa_schema.Column(
            dtype=field_to_pandera_dtype(field),
            nullable=field.nullable,
            required=True,
        )
        for field in spec.fields
    }
    return pa_schema.DataFrameSchema(
        columns,
        strict=True,
        ordered=True,
    )


def _maybe_to_pandas(df: object) -> tuple[object, object]:
    import pandas as pd

    if isinstance(df, pd.DataFrame):
        return df, df
    if isinstance(df, pa.Table):
        return df, df.to_pandas()
    to_pandas = getattr(df, "to_pandas", None)
    if callable(to_pandas):
        resolved = to_pandas()
        if isinstance(resolved, pd.DataFrame):
            return df, resolved
    to_arrow = getattr(df, "to_arrow_table", None)
    if callable(to_arrow):
        resolved = to_arrow()
        if isinstance(resolved, pa.Table):
            return df, resolved.to_pandas()
    msg = f"Unsupported dataframe type for pandera validation: {type(df).__name__}"
    raise TypeError(msg)


def validate_dataframe(
    df: object,
    *,
    schema_spec: TableSchemaSpec,
    policy: ValidationPolicySpec | None,
) -> object:
    """Validate a dataframe-like object with pandera when enabled.

    Parameters
    ----------
    df
        DataFrame-like object to validate.
    schema_spec
        Table schema specification to enforce.
    policy
        Validation policy controlling whether to validate.

    Returns
    -------
    object
        Original dataframe-like object after validation.
    """
    if policy is None or not policy.enabled:
        return df
    schema = to_pandera_schema(schema_spec, policy=policy)
    original, pandas_df = _maybe_to_pandas(df)
    validate_kwargs: dict[str, object] = {"lazy": policy.lazy}
    if policy.sample is not None:
        validate_kwargs["sample"] = policy.sample
    if policy.head is not None:
        validate_kwargs["head"] = policy.head
    if policy.tail is not None:
        validate_kwargs["tail"] = policy.tail
    schema.validate(pandas_df, **validate_kwargs)
    return original


def validation_policy_payload(policy: ValidationPolicySpec | None) -> Mapping[str, object] | None:
    """Return a JSON-serializable payload for the validation policy.

    Parameters
    ----------
    policy
        Validation policy to serialize.

    Returns
    -------
    Mapping[str, object] | None
        Serializable policy payload when defined.
    """
    if policy is None:
        return None
    return {
        "enabled": policy.enabled,
        "lazy": policy.lazy,
        "sample": policy.sample,
        "head": policy.head,
        "tail": policy.tail,
    }


__all__ = [
    "field_to_pandera_dtype",
    "to_pandera_schema",
    "validate_dataframe",
    "validation_policy_payload",
]
