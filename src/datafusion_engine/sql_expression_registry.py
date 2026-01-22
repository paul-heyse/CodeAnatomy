"""DataFusion SQL expression registry helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import pyarrow as pa

BuiltinKind = Literal["scalar", "aggregate", "window", "table"]


@dataclass(frozen=True)
class DataFusionSqlExpressionSpec:
    """Specification for DataFusion SQL expression operators."""

    func_id: str
    sql_expression: str
    kind: BuiltinKind
    input_types: tuple[pa.DataType, ...]
    return_type: pa.DataType
    volatility: str = "stable"
    arg_names: tuple[str, ...] | None = None


DATAFUSION_SQL_EXPRESSION_SPECS: tuple[DataFusionSqlExpressionSpec, ...] = (
    DataFusionSqlExpressionSpec(
        func_id="bit_wise_and",
        sql_expression="AND",
        kind="scalar",
        input_types=(pa.bool_(), pa.bool_()),
        return_type=pa.bool_(),
        arg_names=("left", "right"),
    ),
    DataFusionSqlExpressionSpec(
        func_id="equal",
        sql_expression="=",
        kind="scalar",
        input_types=(pa.null(), pa.null()),
        return_type=pa.bool_(),
        arg_names=("left", "right"),
    ),
    DataFusionSqlExpressionSpec(
        func_id="fill_null",
        sql_expression="COALESCE(value, fill_value)",
        kind="scalar",
        input_types=(pa.null(), pa.null()),
        return_type=pa.null(),
        arg_names=("value", "fill_value"),
    ),
    DataFusionSqlExpressionSpec(
        func_id="if_else",
        sql_expression="CASE WHEN ... THEN ... ELSE ... END",
        kind="scalar",
        input_types=(pa.bool_(), pa.null(), pa.null()),
        return_type=pa.null(),
        arg_names=("cond", "true_value", "false_value"),
    ),
    DataFusionSqlExpressionSpec(
        func_id="invert",
        sql_expression="NOT",
        kind="scalar",
        input_types=(pa.bool_(),),
        return_type=pa.bool_(),
        arg_names=("value",),
    ),
    DataFusionSqlExpressionSpec(
        func_id="not_equal",
        sql_expression="!=",
        kind="scalar",
        input_types=(pa.null(), pa.null()),
        return_type=pa.bool_(),
        arg_names=("left", "right"),
    ),
    DataFusionSqlExpressionSpec(
        func_id="is_null",
        sql_expression="IS NULL",
        kind="scalar",
        input_types=(pa.null(),),
        return_type=pa.bool_(),
        arg_names=("value",),
    ),
    DataFusionSqlExpressionSpec(
        func_id="stringify",
        sql_expression="CAST(... AS STRING)",
        kind="scalar",
        input_types=(pa.null(),),
        return_type=pa.string(),
        arg_names=("value",),
    ),
)


def datafusion_sql_expression_specs() -> tuple[DataFusionSqlExpressionSpec, ...]:
    """Return the DataFusion SQL expression specs."""
    return DATAFUSION_SQL_EXPRESSION_SPECS


__all__ = [
    "DATAFUSION_SQL_EXPRESSION_SPECS",
    "DataFusionSqlExpressionSpec",
    "datafusion_sql_expression_specs",
]
