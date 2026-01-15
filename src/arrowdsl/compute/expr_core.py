"""Expression core helpers for ArrowDSL."""

from __future__ import annotations

import importlib
from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, Protocol

import pyarrow.compute as pc

from arrowdsl.compute.expr_ops import and_expr, and_exprs, or_expr, or_exprs
from arrowdsl.compute.ids import (
    HashSpec,
    hash64_from_arrays,
    hash_column_values,
    hash_expression,
    hash_expression_from_parts,
    masked_hash_array,
    masked_hash_expr,
    prefixed_hash_id,
)
from arrowdsl.compute.position_encoding import (
    DEFAULT_POSITION_ENCODING,
    ENC_UTF8,
    ENC_UTF16,
    ENC_UTF32,
    VALID_POSITION_ENCODINGS,
    normalize_position_encoding,
)
from arrowdsl.core.interop import (
    ArrayLike,
    ComputeExpression,
    DataTypeLike,
    ScalarLike,
    TableLike,
    ensure_expression,
)

if TYPE_CHECKING:
    from arrowdsl.compute.macros import (
        CoalesceStringExprSpec,
        ComputeExprSpec,
        DefUseKindExprSpec,
        TrimExprSpec,
        coalesce_string_expr,
        trimmed_non_empty_expr,
    )

type ScalarValue = bool | int | float | str | bytes | ScalarLike | None
type PredicateKind = Literal["in_set", "is_null", "not"]


class ExprSpec(Protocol):
    """Protocol for expressions usable in plan or kernel lanes."""

    def to_expression(self) -> ComputeExpression:
        """Return the compute expression for plan-lane projection or filtering.

        Returns
        -------
        ComputeExpression
            Plan-lane compute expression.
        """
        ...

    def materialize(self, table: TableLike) -> ArrayLike:
        """Materialize the expression against a table in kernel lane.

        Returns
        -------
        ArrayLike
            Kernel-lane array result.
        """
        ...

    def is_scalar(self) -> bool:
        """Return whether this expression is scalar-safe for scan projection.

        Returns
        -------
        bool
            ``True`` when the expression is scalar-safe.
        """
        ...


@dataclass(frozen=True)
class CastOptionsConfig:
    """Configuration for Arrow cast options."""

    allow_int_overflow: bool | None = None
    allow_time_truncate: bool | None = None
    allow_time_overflow: bool | None = None
    allow_decimal_truncate: bool | None = None
    allow_float_truncate: bool | None = None
    allow_invalid_utf8: bool | None = None


def cast_options_factory(
    config: CastOptionsConfig | None = None,
    *,
    safe: bool = True,
    target_type: DataTypeLike | None = None,
) -> pc.CastOptions:
    """Return a configured CastOptions instance.

    Returns
    -------
    pc.CastOptions
        Cast options instance.
    """
    config = config or CastOptionsConfig()
    if safe:
        allow_int_overflow = config.allow_int_overflow
        allow_time_truncate = config.allow_time_truncate
        allow_time_overflow = config.allow_time_overflow
        allow_decimal_truncate = config.allow_decimal_truncate
        allow_float_truncate = config.allow_float_truncate
        allow_invalid_utf8 = config.allow_invalid_utf8
    else:
        allow_int_overflow = (
            True if config.allow_int_overflow is None else config.allow_int_overflow
        )
        allow_time_truncate = (
            True if config.allow_time_truncate is None else config.allow_time_truncate
        )
        allow_time_overflow = (
            True if config.allow_time_overflow is None else config.allow_time_overflow
        )
        allow_decimal_truncate = (
            True if config.allow_decimal_truncate is None else config.allow_decimal_truncate
        )
        allow_float_truncate = (
            True if config.allow_float_truncate is None else config.allow_float_truncate
        )
        allow_invalid_utf8 = (
            True if config.allow_invalid_utf8 is None else config.allow_invalid_utf8
        )
    return pc.CastOptions(
        target_type=target_type,
        allow_int_overflow=allow_int_overflow,
        allow_time_truncate=allow_time_truncate,
        allow_time_overflow=allow_time_overflow,
        allow_decimal_truncate=allow_decimal_truncate,
        allow_float_truncate=allow_float_truncate,
        allow_invalid_utf8=allow_invalid_utf8,
    )


def cast_expr(
    expr: ComputeExpression,
    dtype: DataTypeLike,
    *,
    safe: bool = True,
    config: CastOptionsConfig | None = None,
) -> ComputeExpression:
    """Return a cast expression using shared options.

    Returns
    -------
    ComputeExpression
        Cast expression for the provided dtype.
    """
    options = cast_options_factory(config, safe=safe, target_type=dtype)
    return ensure_expression(expr.cast(options=options))


@dataclass(frozen=True)
class ScalarAggregateOptionsConfig:
    """Configuration for scalar aggregate options."""

    skip_nulls: bool = True
    min_count: int = 1


def scalar_aggregate_options_factory(
    config: ScalarAggregateOptionsConfig | None = None,
) -> pc.ScalarAggregateOptions:
    """Return a configured ScalarAggregateOptions instance.

    Returns
    -------
    pc.ScalarAggregateOptions
        Scalar aggregate options instance.
    """
    config = config or ScalarAggregateOptionsConfig()
    return pc.ScalarAggregateOptions(skip_nulls=config.skip_nulls, min_count=config.min_count)


@dataclass(frozen=True)
class HashExprSpec:
    """ExprSpec for hash-based identifiers from table columns."""

    spec: HashSpec

    def to_expression(self) -> ComputeExpression:
        """Return the plan-lane hash expression.

        Returns
        -------
        ComputeExpression
            Hash expression for the spec.
        """
        return hash_expression(self.spec)

    def materialize(self, table: TableLike) -> ArrayLike:
        """Materialize the hash expression against a table.

        Returns
        -------
        ArrayLike
            Hash array for the spec.
        """
        return hash_column_values(table, spec=self.spec)

    def is_scalar(self) -> bool:
        """Return whether this expression is scalar-safe.

        Returns
        -------
        bool
            ``True`` for scalar-safe expressions.
        """
        return self is not None


@dataclass(frozen=True)
class MaskedHashExprSpec:
    """ExprSpec for hash-based identifiers with required column masking."""

    spec: HashSpec
    required: Sequence[str]

    def to_expression(self) -> ComputeExpression:
        """Return the masked hash expression for plan-lane use.

        Returns
        -------
        ComputeExpression
            Masked hash expression.
        """
        return masked_hash_expr(self.spec, required=tuple(self.required))

    def materialize(self, table: TableLike) -> ArrayLike:
        """Materialize the masked hash expression against a table.

        Returns
        -------
        ArrayLike
            Masked hash array for the spec.
        """
        return masked_hash_array(table, spec=self.spec, required=self.required)

    def is_scalar(self) -> bool:
        """Return whether this expression is scalar-safe.

        Returns
        -------
        bool
            ``True`` for scalar-safe expressions.
        """
        return self is not None


@dataclass(frozen=True)
class HashFromExprsSpec:
    """ExprSpec for hash IDs built from expression parts."""

    prefix: str
    parts: Sequence[ExprSpec]
    as_string: bool = True
    null_sentinel: str = "None"

    def to_expression(self) -> ComputeExpression:
        """Return the plan-lane hash expression from parts.

        Returns
        -------
        ComputeExpression
            Hash expression for the parts.
        """
        exprs = [part.to_expression() for part in self.parts]
        return hash_expression_from_parts(
            exprs,
            prefix=self.prefix,
            null_sentinel=self.null_sentinel,
            as_string=self.as_string,
        )

    def materialize(self, table: TableLike) -> ArrayLike:
        """Materialize the hash expression against a table.

        Returns
        -------
        ArrayLike
            Hash array for the parts.
        """
        arrays = [part.materialize(table) for part in self.parts]
        if self.as_string:
            return prefixed_hash_id(arrays, prefix=self.prefix, null_sentinel=self.null_sentinel)
        return hash64_from_arrays(arrays, prefix=self.prefix, null_sentinel=self.null_sentinel)

    def is_scalar(self) -> bool:
        """Return whether this expression is scalar-safe.

        Returns
        -------
        bool
            ``True`` for scalar-safe expressions.
        """
        return self is not None


_MACRO_EXPORTS: frozenset[str] = frozenset(
    (
        "CoalesceStringExprSpec",
        "ComputeExprSpec",
        "DefUseKindExprSpec",
        "TrimExprSpec",
        "coalesce_string_expr",
        "trimmed_non_empty_expr",
    )
)


def __getattr__(name: str) -> object:
    if name in _MACRO_EXPORTS:
        macros = importlib.import_module("arrowdsl.compute.macros")
        return getattr(macros, name)
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)


def __dir__() -> list[str]:
    return sorted(__all__)


__all__ = [
    "DEFAULT_POSITION_ENCODING",
    "ENC_UTF8",
    "ENC_UTF16",
    "ENC_UTF32",
    "VALID_POSITION_ENCODINGS",
    "CastOptionsConfig",
    "CoalesceStringExprSpec",
    "ComputeExprSpec",
    "DefUseKindExprSpec",
    "ExprSpec",
    "HashExprSpec",
    "HashFromExprsSpec",
    "MaskedHashExprSpec",
    "PredicateKind",
    "ScalarAggregateOptionsConfig",
    "ScalarValue",
    "TrimExprSpec",
    "and_expr",
    "and_exprs",
    "cast_expr",
    "cast_options_factory",
    "coalesce_string_expr",
    "normalize_position_encoding",
    "or_expr",
    "or_exprs",
    "scalar_aggregate_options_factory",
    "trimmed_non_empty_expr",
]
