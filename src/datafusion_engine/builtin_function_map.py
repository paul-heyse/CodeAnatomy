"""DataFusion builtin function mapping for UDF performance tiers.

This module provides comprehensive mappings of common operations to DataFusion's
builtin functions, allowing the UDF tier system to prioritize native implementations
over custom Python/PyArrow UDFs.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import pyarrow as pa

BuiltinCategory = Literal[
    "math", "string", "aggregate", "window", "comparison", "logical", "datetime"
]


@dataclass(frozen=True)
class BuiltinFunctionSpec:
    """Specification for a DataFusion builtin function mapping."""

    func_id: str
    datafusion_name: str
    category: BuiltinCategory
    input_types: tuple[pa.DataType, ...]
    return_type: pa.DataType
    volatility: str = "immutable"
    description: str | None = None


# Math functions
MATH_BUILTINS: tuple[BuiltinFunctionSpec, ...] = (
    BuiltinFunctionSpec(
        func_id="abs",
        datafusion_name="abs",
        category="math",
        input_types=(pa.float64(),),
        return_type=pa.float64(),
        volatility="immutable",
        description="Absolute value",
    ),
    BuiltinFunctionSpec(
        func_id="ceil",
        datafusion_name="ceil",
        category="math",
        input_types=(pa.float64(),),
        return_type=pa.float64(),
        volatility="immutable",
        description="Ceiling function",
    ),
    BuiltinFunctionSpec(
        func_id="floor",
        datafusion_name="floor",
        category="math",
        input_types=(pa.float64(),),
        return_type=pa.float64(),
        volatility="immutable",
        description="Floor function",
    ),
    BuiltinFunctionSpec(
        func_id="round",
        datafusion_name="round",
        category="math",
        input_types=(pa.float64(),),
        return_type=pa.float64(),
        volatility="immutable",
        description="Round to nearest integer",
    ),
    BuiltinFunctionSpec(
        func_id="sqrt",
        datafusion_name="sqrt",
        category="math",
        input_types=(pa.float64(),),
        return_type=pa.float64(),
        volatility="immutable",
        description="Square root",
    ),
    BuiltinFunctionSpec(
        func_id="ln",
        datafusion_name="ln",
        category="math",
        input_types=(pa.float64(),),
        return_type=pa.float64(),
        volatility="immutable",
        description="Natural logarithm",
    ),
    BuiltinFunctionSpec(
        func_id="log10",
        datafusion_name="log10",
        category="math",
        input_types=(pa.float64(),),
        return_type=pa.float64(),
        volatility="immutable",
        description="Base-10 logarithm",
    ),
    BuiltinFunctionSpec(
        func_id="exp",
        datafusion_name="exp",
        category="math",
        input_types=(pa.float64(),),
        return_type=pa.float64(),
        volatility="immutable",
        description="Exponential function",
    ),
    BuiltinFunctionSpec(
        func_id="power",
        datafusion_name="power",
        category="math",
        input_types=(pa.float64(), pa.float64()),
        return_type=pa.float64(),
        volatility="immutable",
        description="Power function (base^exponent)",
    ),
    BuiltinFunctionSpec(
        func_id="sin",
        datafusion_name="sin",
        category="math",
        input_types=(pa.float64(),),
        return_type=pa.float64(),
        volatility="immutable",
        description="Sine function",
    ),
    BuiltinFunctionSpec(
        func_id="cos",
        datafusion_name="cos",
        category="math",
        input_types=(pa.float64(),),
        return_type=pa.float64(),
        volatility="immutable",
        description="Cosine function",
    ),
    BuiltinFunctionSpec(
        func_id="tan",
        datafusion_name="tan",
        category="math",
        input_types=(pa.float64(),),
        return_type=pa.float64(),
        volatility="immutable",
        description="Tangent function",
    ),
)

# String functions
STRING_BUILTINS: tuple[BuiltinFunctionSpec, ...] = (
    BuiltinFunctionSpec(
        func_id="length",
        datafusion_name="length",
        category="string",
        input_types=(pa.string(),),
        return_type=pa.int64(),
        volatility="immutable",
        description="String length in characters",
    ),
    BuiltinFunctionSpec(
        func_id="lower",
        datafusion_name="lower",
        category="string",
        input_types=(pa.string(),),
        return_type=pa.string(),
        volatility="immutable",
        description="Convert to lowercase",
    ),
    BuiltinFunctionSpec(
        func_id="upper",
        datafusion_name="upper",
        category="string",
        input_types=(pa.string(),),
        return_type=pa.string(),
        volatility="immutable",
        description="Convert to uppercase",
    ),
    BuiltinFunctionSpec(
        func_id="trim",
        datafusion_name="trim",
        category="string",
        input_types=(pa.string(),),
        return_type=pa.string(),
        volatility="immutable",
        description="Trim whitespace from both ends",
    ),
    BuiltinFunctionSpec(
        func_id="ltrim",
        datafusion_name="ltrim",
        category="string",
        input_types=(pa.string(),),
        return_type=pa.string(),
        volatility="immutable",
        description="Trim whitespace from left",
    ),
    BuiltinFunctionSpec(
        func_id="rtrim",
        datafusion_name="rtrim",
        category="string",
        input_types=(pa.string(),),
        return_type=pa.string(),
        volatility="immutable",
        description="Trim whitespace from right",
    ),
    BuiltinFunctionSpec(
        func_id="substr",
        datafusion_name="substr",
        category="string",
        input_types=(pa.string(), pa.int64(), pa.int64()),
        return_type=pa.string(),
        volatility="immutable",
        description="Extract substring",
    ),
    BuiltinFunctionSpec(
        func_id="concat",
        datafusion_name="concat",
        category="string",
        input_types=(pa.string(), pa.string()),
        return_type=pa.string(),
        volatility="immutable",
        description="Concatenate strings",
    ),
    BuiltinFunctionSpec(
        func_id="starts_with",
        datafusion_name="starts_with",
        category="string",
        input_types=(pa.string(), pa.string()),
        return_type=pa.bool_(),
        volatility="immutable",
        description="Check if string starts with prefix",
    ),
    BuiltinFunctionSpec(
        func_id="ends_with",
        datafusion_name="ends_with",
        category="string",
        input_types=(pa.string(), pa.string()),
        return_type=pa.bool_(),
        volatility="immutable",
        description="Check if string ends with suffix",
    ),
    BuiltinFunctionSpec(
        func_id="replace",
        datafusion_name="replace",
        category="string",
        input_types=(pa.string(), pa.string(), pa.string()),
        return_type=pa.string(),
        volatility="immutable",
        description="Replace substring occurrences",
    ),
)

# Aggregate functions
AGGREGATE_BUILTINS: tuple[BuiltinFunctionSpec, ...] = (
    BuiltinFunctionSpec(
        func_id="sum",
        datafusion_name="sum",
        category="aggregate",
        input_types=(pa.float64(),),
        return_type=pa.float64(),
        volatility="immutable",
        description="Sum aggregate",
    ),
    BuiltinFunctionSpec(
        func_id="avg",
        datafusion_name="avg",
        category="aggregate",
        input_types=(pa.float64(),),
        return_type=pa.float64(),
        volatility="immutable",
        description="Average aggregate",
    ),
    BuiltinFunctionSpec(
        func_id="count",
        datafusion_name="count",
        category="aggregate",
        input_types=(pa.null(),),
        return_type=pa.int64(),
        volatility="immutable",
        description="Count aggregate",
    ),
    BuiltinFunctionSpec(
        func_id="min",
        datafusion_name="min",
        category="aggregate",
        input_types=(pa.null(),),
        return_type=pa.null(),
        volatility="immutable",
        description="Minimum aggregate",
    ),
    BuiltinFunctionSpec(
        func_id="max",
        datafusion_name="max",
        category="aggregate",
        input_types=(pa.null(),),
        return_type=pa.null(),
        volatility="immutable",
        description="Maximum aggregate",
    ),
    BuiltinFunctionSpec(
        func_id="first_value",
        datafusion_name="first_value",
        category="aggregate",
        input_types=(pa.null(),),
        return_type=pa.null(),
        volatility="immutable",
        description="First value in group",
    ),
    BuiltinFunctionSpec(
        func_id="last_value",
        datafusion_name="last_value",
        category="aggregate",
        input_types=(pa.null(),),
        return_type=pa.null(),
        volatility="immutable",
        description="Last value in group",
    ),
    BuiltinFunctionSpec(
        func_id="stddev",
        datafusion_name="stddev",
        category="aggregate",
        input_types=(pa.float64(),),
        return_type=pa.float64(),
        volatility="immutable",
        description="Standard deviation aggregate",
    ),
    BuiltinFunctionSpec(
        func_id="variance",
        datafusion_name="variance",
        category="aggregate",
        input_types=(pa.float64(),),
        return_type=pa.float64(),
        volatility="immutable",
        description="Variance aggregate",
    ),
)

# Window functions
WINDOW_BUILTINS: tuple[BuiltinFunctionSpec, ...] = (
    BuiltinFunctionSpec(
        func_id="row_number",
        datafusion_name="row_number",
        category="window",
        input_types=(),
        return_type=pa.int64(),
        volatility="immutable",
        description="Row number within partition",
    ),
    BuiltinFunctionSpec(
        func_id="rank",
        datafusion_name="rank",
        category="window",
        input_types=(),
        return_type=pa.int64(),
        volatility="immutable",
        description="Rank within partition",
    ),
    BuiltinFunctionSpec(
        func_id="dense_rank",
        datafusion_name="dense_rank",
        category="window",
        input_types=(),
        return_type=pa.int64(),
        volatility="immutable",
        description="Dense rank within partition",
    ),
    BuiltinFunctionSpec(
        func_id="ntile",
        datafusion_name="ntile",
        category="window",
        input_types=(pa.int64(),),
        return_type=pa.int64(),
        volatility="immutable",
        description="Divide rows into buckets",
    ),
    BuiltinFunctionSpec(
        func_id="lag",
        datafusion_name="lag",
        category="window",
        input_types=(pa.null(),),
        return_type=pa.null(),
        volatility="immutable",
        description="Access previous row value",
    ),
    BuiltinFunctionSpec(
        func_id="lead",
        datafusion_name="lead",
        category="window",
        input_types=(pa.null(),),
        return_type=pa.null(),
        volatility="immutable",
        description="Access next row value",
    ),
)

# Comparison functions
COMPARISON_BUILTINS: tuple[BuiltinFunctionSpec, ...] = (
    BuiltinFunctionSpec(
        func_id="coalesce",
        datafusion_name="coalesce",
        category="comparison",
        input_types=(pa.null(), pa.null()),
        return_type=pa.null(),
        volatility="immutable",
        description="Return first non-null value",
    ),
    BuiltinFunctionSpec(
        func_id="nullif",
        datafusion_name="nullif",
        category="comparison",
        input_types=(pa.null(), pa.null()),
        return_type=pa.null(),
        volatility="immutable",
        description="Return null if values are equal",
    ),
)

# Logical functions
LOGICAL_BUILTINS: tuple[BuiltinFunctionSpec, ...] = (
    BuiltinFunctionSpec(
        func_id="not",
        datafusion_name="not",
        category="logical",
        input_types=(pa.bool_(),),
        return_type=pa.bool_(),
        volatility="immutable",
        description="Logical NOT",
    ),
)

# DateTime functions
DATETIME_BUILTINS: tuple[BuiltinFunctionSpec, ...] = (
    BuiltinFunctionSpec(
        func_id="date_trunc",
        datafusion_name="date_trunc",
        category="datetime",
        input_types=(pa.string(), pa.timestamp("us")),
        return_type=pa.timestamp("us"),
        volatility="immutable",
        description="Truncate timestamp to specified precision",
    ),
    BuiltinFunctionSpec(
        func_id="extract",
        datafusion_name="extract",
        category="datetime",
        input_types=(pa.string(), pa.timestamp("us")),
        return_type=pa.int64(),
        volatility="immutable",
        description="Extract date/time component",
    ),
    BuiltinFunctionSpec(
        func_id="now",
        datafusion_name="now",
        category="datetime",
        input_types=(),
        return_type=pa.timestamp("us"),
        volatility="stable",
        description="Current timestamp",
    ),
)

# Combined registry of all builtin functions
BUILTIN_FUNCTION_MAP: dict[str, BuiltinFunctionSpec] = {
    spec.func_id: spec
    for spec in (
        *MATH_BUILTINS,
        *STRING_BUILTINS,
        *AGGREGATE_BUILTINS,
        *WINDOW_BUILTINS,
        *COMPARISON_BUILTINS,
        *LOGICAL_BUILTINS,
        *DATETIME_BUILTINS,
    )
}


def get_builtin_function(func_id: str) -> BuiltinFunctionSpec | None:
    """Look up a DataFusion builtin function by its function ID.

    Parameters
    ----------
    func_id:
        Function identifier to look up.

    Returns
    -------
    BuiltinFunctionSpec | None
        Builtin function spec if found, None otherwise.
    """
    return BUILTIN_FUNCTION_MAP.get(func_id)


def get_builtins_by_category(category: BuiltinCategory) -> tuple[BuiltinFunctionSpec, ...]:
    """Get all builtin functions in a specific category.

    Parameters
    ----------
    category:
        Category to filter by.

    Returns
    -------
    tuple[BuiltinFunctionSpec, ...]
        All builtin functions in the specified category.
    """
    return tuple(spec for spec in BUILTIN_FUNCTION_MAP.values() if spec.category == category)


def is_builtin_function(func_id: str) -> bool:
    """Check if a function ID corresponds to a DataFusion builtin.

    Parameters
    ----------
    func_id:
        Function identifier to check.

    Returns
    -------
    bool
        True if the function is a DataFusion builtin, False otherwise.
    """
    return func_id in BUILTIN_FUNCTION_MAP


__all__ = [
    "AGGREGATE_BUILTINS",
    "BUILTIN_FUNCTION_MAP",
    "COMPARISON_BUILTINS",
    "DATETIME_BUILTINS",
    "LOGICAL_BUILTINS",
    "MATH_BUILTINS",
    "STRING_BUILTINS",
    "WINDOW_BUILTINS",
    "BuiltinCategory",
    "BuiltinFunctionSpec",
    "get_builtin_function",
    "get_builtins_by_category",
    "is_builtin_function",
]
