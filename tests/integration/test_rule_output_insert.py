"""Integration tests for rule output materialization."""

from __future__ import annotations

from pathlib import Path

import ibis
import pyarrow as pa
import pytest

from arrowdsl.core.execution_context import ExecutionContext, execution_context_factory
from arrowdsl.core.ordering import OrderingLevel
from arrowdsl.schema.metadata import ordering_metadata_spec
from ibis_engine.plan import IbisPlan
from relspec.compiler import (
    CompiledOutput,
    CompiledOutputExecutionOptions,
    CompiledRule,
    InMemoryPlanResolver,
    PlanExecutor,
    PlanResolver,
)
from relspec.model import DatasetRef, RelationshipRule, RuleKind
from relspec.registry import ContractCatalog

pytest.importorskip("duckdb")

EXPECTED_ROW_COUNT = 2


def _ordered_table() -> pa.Table:
    table = pa.table({"id": [1, 2], "value": ["a", "b"]})
    spec = ordering_metadata_spec(OrderingLevel.EXPLICIT, keys=(("id", "ascending"),))
    schema = spec.apply(table.schema)
    return table.cast(schema)


@pytest.mark.integration
def test_compiled_output_materializes_to_backend(tmp_path: Path) -> None:
    """Insert rule outputs into backend tables."""
    backend = ibis.duckdb.connect(str(tmp_path / "outputs.duckdb"))
    ctx = execution_context_factory("default")
    rule = RelationshipRule(
        name="rule_output_insert",
        kind=RuleKind.FILTER_PROJECT,
        output_dataset="rule_output_table",
        inputs=(DatasetRef(name="input"),),
    )

    def _execute_fn(
        _ctx: ExecutionContext,
        _resolver: PlanResolver[IbisPlan],
        _executor: PlanExecutor,
    ) -> pa.Table:
        return _ordered_table()

    compiled_rule = CompiledRule(rule=rule, rel_plan=None, execute_fn=_execute_fn)
    compiled = CompiledOutput(
        output_dataset="rule_output_table",
        contract_name=None,
        contributors=(compiled_rule,),
    )
    options = CompiledOutputExecutionOptions(
        contracts=ContractCatalog(),
        ibis_backend=backend,
    )
    resolver = InMemoryPlanResolver({}, backend=backend)
    _ = compiled.execute(ctx=ctx, resolver=resolver, options=options)
    assert "rule_output_table" in backend.list_tables()
    assert backend.table("rule_output_table").count().execute() == EXPECTED_ROW_COUNT


@pytest.mark.integration
def test_compiled_output_requires_ordering_for_insert(tmp_path: Path) -> None:
    """Require explicit ordering metadata for inserted outputs."""
    backend = ibis.duckdb.connect(str(tmp_path / "outputs.duckdb"))
    ctx = execution_context_factory("default")
    rule = RelationshipRule(
        name="rule_output_ordering",
        kind=RuleKind.FILTER_PROJECT,
        output_dataset="rule_output_table",
        inputs=(DatasetRef(name="input"),),
    )

    def _execute_fn(
        _ctx: ExecutionContext,
        _resolver: PlanResolver[IbisPlan],
        _executor: PlanExecutor,
    ) -> pa.Table:
        return pa.table({"id": [1]})

    compiled_rule = CompiledRule(rule=rule, rel_plan=None, execute_fn=_execute_fn)
    compiled = CompiledOutput(
        output_dataset="rule_output_table",
        contract_name=None,
        contributors=(compiled_rule,),
    )
    options = CompiledOutputExecutionOptions(
        contracts=ContractCatalog(),
        ibis_backend=backend,
    )
    resolver = InMemoryPlanResolver({}, backend=backend)
    with pytest.raises(ValueError, match="requires explicit ordering"):
        _ = compiled.execute(ctx=ctx, resolver=resolver, options=options)
