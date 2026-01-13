"""Compute options factories for ArrowDSL."""

from __future__ import annotations

from dataclasses import dataclass

import pyarrow.compute as pc

from arrowdsl.core.interop import ComputeExpression, DataTypeLike, ensure_expression


@dataclass(frozen=True)
class CastOptionsConfig:
    """Configuration for Arrow cast options."""

    allow_int_overflow: bool | None = None
    allow_time_truncate: bool | None = None
    allow_time_overflow: bool | None = None
    allow_decimal_truncate: bool | None = None
    allow_float_truncate: bool | None = None
    allow_invalid_utf8: bool | None = None


def cast_options_factory(config: CastOptionsConfig | None = None) -> pc.CastOptions:
    """Return a configured CastOptions instance.

    Returns
    -------
    pc.CastOptions
        Cast options instance.
    """
    config = config or CastOptionsConfig()
    return pc.CastOptions(
        allow_int_overflow=config.allow_int_overflow,
        allow_time_truncate=config.allow_time_truncate,
        allow_time_overflow=config.allow_time_overflow,
        allow_decimal_truncate=config.allow_decimal_truncate,
        allow_float_truncate=config.allow_float_truncate,
        allow_invalid_utf8=config.allow_invalid_utf8,
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
    options = cast_options_factory(config)
    return ensure_expression(pc.cast(expr, dtype, safe=safe, options=options))


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


__all__ = [
    "CastOptionsConfig",
    "ScalarAggregateOptionsConfig",
    "cast_expr",
    "cast_options_factory",
    "scalar_aggregate_options_factory",
]
