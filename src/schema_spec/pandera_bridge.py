"""Pandera schema bridge for msgspec table specifications."""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING, Any, cast

import polars as pl
import pyarrow as pa

from schema_spec.arrow_types import ArrowTypeBase, arrow_type_to_pyarrow
from schema_spec.field_spec import FieldSpec
from schema_spec.specs import TableSchemaSpec
from schema_spec.system import ValidationPolicySpec
from serde_msgspec import StructBaseStrict

if TYPE_CHECKING:
    from datafusion_engine.lineage.diagnostics import DiagnosticsSink


class DataframeValidationRequest[TDF](StructBaseStrict, frozen=True):
    """Validation request for Pandera dataframe checks."""

    df: TDF
    schema_spec: TableSchemaSpec
    policy: ValidationPolicySpec | None
    constraints: Iterable[str] | None = None
    diagnostics: DiagnosticsSink | None = None
    name: str = "unknown"


def _arrow_to_polars_dtype(dtype: pa.DataType) -> object:
    try:
        table = pa.table({"_": pa.array([], type=dtype)})
        df = cast("pl.DataFrame", pl.from_arrow(table, rechunk=False))
        return df.schema["_"]
    except (TypeError, ValueError, pa.ArrowInvalid):
        return object


def _model_name(spec: TableSchemaSpec) -> str:
    import re

    safe = re.sub(r"\W+", "_", spec.name).strip("_")
    if not safe:
        safe = "SchemaSpec"
    return f"{safe}Model"


_CONSTRAINT_NOT_NULL_RE = re.compile(
    r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s+IS\s+NOT\s+NULL\s*$",
    re.IGNORECASE,
)
_CONSTRAINT_NULL_RE = re.compile(
    r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s+IS\s+NULL\s*$",
    re.IGNORECASE,
)


def _constraints_required_non_null(constraints: Iterable[str] | None) -> set[str]:
    if not constraints:
        return set()
    required: set[str] = set()
    for constraint in constraints:
        match = _CONSTRAINT_NOT_NULL_RE.match(constraint)
        if match is not None:
            required.add(match.group(1))
    return required


def _constraints_force_null(constraints: Iterable[str] | None) -> set[str]:
    if not constraints:
        return set()
    forced: set[str] = set()
    for constraint in constraints:
        match = _CONSTRAINT_NULL_RE.match(constraint)
        if match is not None:
            forced.add(match.group(1))
    return forced


def _constraint_sets(
    spec: TableSchemaSpec,
    constraints: Iterable[str] | None,
) -> tuple[set[str], set[str]]:
    required = set(spec.required_non_null)
    required |= _constraints_required_non_null(constraints)
    force_null = _constraints_force_null(constraints)
    required -= force_null
    return required, force_null


def dataframe_model_for_spec(
    spec: TableSchemaSpec,
    *,
    strict: bool | str = True,
    coerce: bool = False,
    constraints: Iterable[str] | None = None,
) -> type[Any] | None:
    """Return a pandera DataFrameModel for the schema spec when possible.

    Returns:
    -------
    type[Any] | None
        Generated DataFrameModel class when column names are valid identifiers.
    """
    import pandera.polars as pa_schema

    model_base = getattr(pa_schema, "DataFrameModel", None)
    field_cls = getattr(pa_schema, "Field", None)
    check_cls = getattr(pa_schema, "Check", None)
    if model_base is None or field_cls is None or check_cls is None:
        return None
    try:
        import pandera.typing.polars as pa_typing
    except (ImportError, ModuleNotFoundError):
        return None

    if any(not field.name.isidentifier() for field in spec.fields):
        return None
    required_non_null, force_null = _constraint_sets(spec, constraints)
    annotations: dict[str, object] = {}
    attrs: dict[str, object] = {}
    for field in spec.fields:
        dtype = field_to_pandera_dtype(field)
        annotations[field.name] = (
            pa_typing.Series[object] if dtype is object else pa_typing.Series[dtype]
        )
        if field.name in force_null:
            nullable = True
        else:
            nullable = field.nullable and field.name not in required_non_null
        field_kwargs: dict[str, object] = {"nullable": nullable}
        if field.name in force_null:
            field_kwargs["eq"] = None
        attrs[field.name] = field_cls(**field_kwargs)
    attrs["__annotations__"] = annotations
    attrs["Config"] = type(
        "Config",
        (),
        {"strict": strict, "coerce": coerce, "ordered": True},
    )
    return type(_model_name(spec), (model_base,), attrs)


def field_to_pandera_dtype(field: FieldSpec) -> Any:
    """Return a polars dtype suitable for pandera from a field spec.

    Parameters
    ----------
    field
        Field specification describing the column type.

    Returns:
    -------
    Any
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
    return _arrow_to_polars_dtype(arrow_dtype)


def to_pandera_schema(
    spec: TableSchemaSpec,
    *,
    policy: ValidationPolicySpec | None = None,
    constraints: Iterable[str] | None = None,
) -> Any:
    """Build a pandera DataFrameSchema from a table schema spec.

    Args:
        spec: Table schema spec to convert.
        policy: Optional validation policy override.
        constraints: Optional constraint expressions.

    Returns:
        Any: Result.

    Raises:
        TypeError: If field types cannot be mapped to pandera dtypes.
    """
    import pandera.polars as pa_schema

    column_cls = getattr(pa_schema, "Column", None)
    check_cls = getattr(pa_schema, "Check", None)
    if column_cls is None or check_cls is None:
        msg = "Pandera polars backend is unavailable; install pandera[polars]."
        raise TypeError(msg)

    required_non_null, force_null = _constraint_sets(spec, constraints)
    columns = {}
    for field in spec.fields:
        if field.name in force_null:
            nullable = True
        else:
            nullable = field.nullable and field.name not in required_non_null
        checks = None
        if field.name in force_null:
            checks = [check_cls(lambda s: s.is_null().all(), element_wise=False)]
        columns[field.name] = column_cls(
            dtype=field_to_pandera_dtype(field),
            nullable=nullable,
            required=True,
            checks=checks,
        )
    strict = True
    coerce = False
    if policy is not None:
        if policy.strict is not None:
            strict = policy.strict
        if policy.coerce is not None:
            coerce = policy.coerce
    unique = list(spec.key_fields) if spec.key_fields else None
    if unique:
        try:
            return pa_schema.DataFrameSchema(
                columns=columns,
                strict=strict,
                ordered=True,
                coerce=coerce,
                unique=unique,
            )
        except TypeError:
            return pa_schema.DataFrameSchema(
                columns=columns,
                strict=strict,
                ordered=True,
                coerce=coerce,
            )
    return pa_schema.DataFrameSchema(
        columns=columns,
        strict=strict,
        ordered=True,
        coerce=coerce,
    )


def _maybe_to_polars[TDF](df: TDF) -> tuple[TDF, pl.DataFrame]:
    def _reader_to_table(reader: pa.RecordBatchReader) -> pa.Table:
        batches = list(reader)
        return pa.Table.from_batches(batches, schema=reader.schema)

    if isinstance(df, pl.DataFrame):
        return df, df
    if isinstance(df, pa.Table):
        table = cast("pa.Table", df)
        return df, cast("pl.DataFrame", pl.from_arrow(table, rechunk=False))
    if isinstance(df, pa.RecordBatchReader):
        reader = cast("pa.RecordBatchReader", df)
        table = _reader_to_table(reader)
        replay_reader = pa.RecordBatchReader.from_batches(reader.schema, table.to_batches())
        return cast("TDF", replay_reader), cast("pl.DataFrame", pl.from_arrow(table, rechunk=False))
    if hasattr(df, "__arrow_c_stream__"):
        reader = pa.RecordBatchReader.from_stream(df)
        table = _reader_to_table(reader)
        return df, cast("pl.DataFrame", pl.from_arrow(table, rechunk=False))
    to_arrow = getattr(df, "to_arrow_table", None)
    if callable(to_arrow):
        resolved = to_arrow()
        if isinstance(resolved, pa.Table):
            table = cast("pa.Table", resolved)
            return df, cast("pl.DataFrame", pl.from_arrow(table, rechunk=False))
        if isinstance(resolved, pa.RecordBatchReader):
            reader = cast("pa.RecordBatchReader", resolved)
            table = _reader_to_table(reader)
            return df, cast("pl.DataFrame", pl.from_arrow(table, rechunk=False))
    msg = f"Unsupported dataframe type for pandera validation: {type(df).__name__}"
    raise TypeError(msg)


def _bounded_validation_rows(
    requested: int | None,
    *,
    row_count: int,
) -> int | None:
    """Clamp validation row-window inputs to available rows.

    Pandera forwards these values into Polars sampling/head/tail calls, which
    raise when asked for more rows than present.

    Returns:
    -------
    int | None
        Clamped row count when applicable, otherwise ``None``.
    """
    if requested is None:
        return None
    if requested <= 0 or row_count <= 0:
        return None
    return min(requested, row_count)


def validate_dataframe[TDF](
    df: TDF,
    *,
    schema_spec: TableSchemaSpec,
    policy: ValidationPolicySpec | None,
    constraints: Iterable[str] | None = None,
) -> TDF:
    """Validate a dataframe-like object with pandera when enabled.

    Parameters
    ----------
    df
        DataFrame-like object to validate.
    schema_spec
        Table schema specification to enforce.
    policy
        Validation policy controlling whether to validate.
    constraints
        Optional constraint expressions for minimal checks.

    Returns:
    -------
    TDF
        Original dataframe-like object after validation.
    """
    if policy is None or not policy.enabled:
        return df
    original, polars_df = _maybe_to_polars(df)
    row_count = polars_df.height
    sample = _bounded_validation_rows(policy.sample, row_count=row_count)
    head = _bounded_validation_rows(policy.head, row_count=row_count)
    tail = _bounded_validation_rows(policy.tail, row_count=row_count)
    validate_kwargs: dict[str, object] = {"lazy": policy.lazy}
    if sample is not None:
        validate_kwargs["sample"] = sample
    if head is not None:
        validate_kwargs["head"] = head
    if tail is not None:
        validate_kwargs["tail"] = tail
    strict = True
    coerce = False
    if policy is not None:
        if policy.strict is not None:
            strict = policy.strict
        if policy.coerce is not None:
            coerce = policy.coerce
    model = dataframe_model_for_spec(
        schema_spec,
        strict=strict,
        coerce=coerce,
        constraints=constraints,
    )
    if model is not None:
        model.validate(
            polars_df,
            lazy=policy.lazy,
            sample=sample,
            head=head,
            tail=tail,
        )
    schema = to_pandera_schema(schema_spec, policy=policy, constraints=constraints)
    schema.validate(polars_df, **validate_kwargs)
    return original


def validate_with_policy[TDF](request: DataframeValidationRequest[TDF]) -> TDF:
    """Validate a dataframe-like object and emit diagnostics on failure.

    Returns:
    -------
    TDF
        Original dataframe-like object after validation.
    """
    if request.policy is None or not request.policy.enabled:
        return request.df
    try:
        return validate_dataframe(
            request.df,
            schema_spec=request.schema_spec,
            policy=request.policy,
            constraints=request.constraints,
        )
    except Exception as exc:
        if request.diagnostics is not None:
            from obs.diagnostics import record_dataframe_validation_error

            record_dataframe_validation_error(
                request.diagnostics,
                name=request.name,
                error=exc,
                policy=request.policy,
            )
        raise


def validation_policy_payload(policy: ValidationPolicySpec | None) -> Mapping[str, object] | None:
    """Return a JSON-serializable payload for the validation policy.

    Parameters
    ----------
    policy
        Validation policy to serialize.

    Returns:
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
        "strict": policy.strict,
        "coerce": policy.coerce,
    }


__all__ = [
    "DataframeValidationRequest",
    "dataframe_model_for_spec",
    "field_to_pandera_dtype",
    "to_pandera_schema",
    "validate_dataframe",
    "validate_with_policy",
    "validation_policy_payload",
]
