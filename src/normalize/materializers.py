"""Materializers for normalize rule outputs."""

from __future__ import annotations

from collections.abc import Mapping

import pyarrow as pa

from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import RecordBatchReaderLike, TableLike
from arrowdsl.plan.plan import Plan
from arrowdsl.plan.runner import run_plan
from arrowdsl.schema.build import const_array, set_or_append_column
from normalize.rule_model import NormalizeRule


def materialize_rule_outputs(
    plans: Mapping[str, Plan],
    *,
    ctx: ExecutionContext,
    rules: Mapping[str, NormalizeRule] | None = None,
    attach_rule_meta: bool = False,
) -> dict[str, TableLike]:
    """Materialize rule plans into tables with optional rule metadata columns.

    Returns
    -------
    dict[str, TableLike]
        Materialized outputs keyed by dataset name.
    """
    outputs: dict[str, TableLike] = {}
    for name, plan in plans.items():
        result = run_plan(
            plan,
            ctx=ctx,
            prefer_reader=True,
            attach_ordering_metadata=True,
        )
        table = _materialize_table(result.value)
        if attach_rule_meta and rules is not None:
            rule = rules.get(name)
            if rule is not None:
                table = _with_rule_meta(table, rule)
        outputs[name] = table
    return outputs


def _with_rule_meta(table: TableLike, rule: NormalizeRule) -> TableLike:
    count = table.num_rows
    if "rule_name" not in table.column_names:
        table = set_or_append_column(
            table,
            "rule_name",
            const_array(count, rule.name, dtype=pa.string()),
        )
    if "rule_priority" not in table.column_names:
        table = set_or_append_column(
            table,
            "rule_priority",
            const_array(count, int(rule.priority), dtype=pa.int32()),
        )
    return table


def _materialize_table(value: TableLike | RecordBatchReaderLike) -> TableLike:
    if isinstance(value, RecordBatchReaderLike):
        return value.read_all()
    return value


__all__ = ["materialize_rule_outputs"]
