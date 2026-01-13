"""Relationship rule graph compilation and evidence gating."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field

from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import SchemaLike
from arrowdsl.plan.plan import Plan, union_all_plans
from arrowdsl.plan.scan_io import DatasetSource
from arrowdsl.schema.build import ConstExpr, FieldExpr
from cpg.catalog import PlanCatalog
from relspec.compiler import RelationshipRuleCompiler
from relspec.contracts import RELATION_OUTPUT_NAME, relation_output_schema
from relspec.model import EvidenceSpec, RelationshipRule
from schema_spec.system import GLOBAL_SCHEMA_REGISTRY


@dataclass
class EvidenceCatalog:
    """Track available evidence sources and columns."""

    sources: set[str] = field(default_factory=set)
    columns_by_dataset: dict[str, set[str]] = field(default_factory=dict)
    types_by_dataset: dict[str, dict[str, str]] = field(default_factory=dict)

    @classmethod
    def from_plan_catalog(cls, catalog: PlanCatalog, *, ctx: ExecutionContext) -> EvidenceCatalog:
        """Build an evidence catalog from a plan catalog.

        Returns
        -------
        EvidenceCatalog
            Evidence catalog populated from catalog sources.
        """
        evidence = cls(sources=set(catalog.tables))
        for name, source in catalog.tables.items():
            schema = _schema_from_source(source, ctx=ctx)
            if schema is not None:
                evidence.columns_by_dataset[name] = set(schema.names)
                evidence.types_by_dataset[name] = _schema_types(schema)
        return evidence

    def register(self, name: str, schema: SchemaLike) -> None:
        """Register an evidence dataset and its schema."""
        self.sources.add(name)
        self.columns_by_dataset[name] = set(schema.names)
        self.types_by_dataset[name] = _schema_types(schema)

    def clone(self) -> EvidenceCatalog:
        """Return a shallow copy for staged updates.

        Returns
        -------
        EvidenceCatalog
            Copy of the catalog for staged updates.
        """
        return EvidenceCatalog(
            sources=set(self.sources),
            columns_by_dataset={key: set(cols) for key, cols in self.columns_by_dataset.items()},
            types_by_dataset={key: dict(types) for key, types in self.types_by_dataset.items()},
        )

    def satisfies(self, spec: EvidenceSpec | None, *, inputs: Sequence[str]) -> bool:
        """Return whether the evidence spec requirements are met.

        Parameters
        ----------
        spec:
            Evidence requirements to evaluate.
        inputs:
            Input dataset names for the rule.

        Returns
        -------
        bool
            ``True`` when the spec requirements are satisfied.
        """
        resolved = spec or EvidenceSpec(sources=tuple(inputs))
        sources = resolved.sources or tuple(inputs)
        has_sources = self._sources_available(sources)
        has_columns = not resolved.required_columns or self._columns_available(
            sources, resolved.required_columns
        )
        has_types = not resolved.required_types or self._types_available(
            sources, resolved.required_types
        )
        return has_sources and has_columns and has_types

    def _sources_available(self, sources: Sequence[str]) -> bool:
        return set(sources).issubset(self.sources)

    def _columns_available(self, sources: Sequence[str], required_columns: Sequence[str]) -> bool:
        required = set(required_columns)
        for source in sources:
            columns = self.columns_by_dataset.get(source)
            if columns is None or not required.issubset(columns):
                return False
        return True

    def _types_available(self, sources: Sequence[str], required_types: Mapping[str, str]) -> bool:
        for source in sources:
            types = self.types_by_dataset.get(source)
            if types is None:
                return False
            for col, dtype in required_types.items():
                if types.get(col) != dtype:
                    return False
        return True


@dataclass(frozen=True)
class RuleNode:
    """Node in the relationship rule graph."""

    name: str
    rule: RelationshipRule
    requires: tuple[str, ...] = ()


@dataclass(frozen=True)
class GraphPlan:
    """Compiled plan graph for relationship rules."""

    plan: Plan
    outputs: dict[str, Plan]


def compile_graph_plan(
    rules: Sequence[RelationshipRule],
    *,
    ctx: ExecutionContext,
    compiler: RelationshipRuleCompiler,
    evidence: EvidenceCatalog,
) -> GraphPlan:
    """Compile relationship rules into a graph-level plan.

    Parameters
    ----------
    rules:
        Relationship rules to compile.
    ctx:
        Execution context for plan compilation.
    compiler:
        Rule compiler with plan resolver.
    evidence:
        Evidence catalog used to order rules.

    Returns
    -------
    GraphPlan
        Graph-level plan and per-output subplans.
    """
    work = evidence.clone()
    outputs: dict[str, Plan] = {}
    for rule in order_rules(rules, evidence=work):
        compiled = compiler.compile_rule(rule, ctx=ctx)
        if compiled.plan is not None and not compiled.post_kernels:
            plan = _apply_rule_meta(compiled.plan, rule=rule, ctx=ctx)
        else:
            table = compiled.execute(ctx=ctx, resolver=compiler.resolver)
            plan = Plan.table_source(table, label=rule.name)
        outputs[rule.output_dataset] = plan
        work.register(rule.output_dataset, plan.schema(ctx=ctx))
    union = union_all_plans(list(outputs.values()), label="relspec_graph")
    return GraphPlan(plan=union, outputs=outputs)


def _apply_rule_meta(plan: Plan, *, rule: RelationshipRule, ctx: ExecutionContext) -> Plan:
    if not rule.emit_rule_meta:
        return plan
    schema = plan.schema(ctx=ctx)
    names = list(schema.names)
    exprs = [FieldExpr(name=name).to_expression() for name in names]
    if rule.rule_name_col not in names:
        exprs.append(ConstExpr(value=rule.name).to_expression())
        names.append(rule.rule_name_col)
    if rule.rule_priority_col not in names:
        exprs.append(ConstExpr(value=int(rule.priority)).to_expression())
        names.append(rule.rule_priority_col)
    return plan.project(exprs, names, label=plan.label or rule.name)


def order_rules(
    rules: Sequence[RelationshipRule],
    *,
    evidence: EvidenceCatalog,
) -> list[RelationshipRule]:
    """Return rules ordered by dependency and priority.

    Returns
    -------
    list[RelationshipRule]
        Ordered, evidence-eligible rules.

    Raises
    ------
    ValueError
        Raised when the rule dependency graph contains cycles.
    """
    work = evidence.clone()
    pending = list(rules)
    resolved: list[RelationshipRule] = []
    while pending:
        ready = _ready_rules(pending, work)
        if not ready:
            missing = sorted({rule.name for rule in pending})
            msg = f"Relationship rule graph cannot resolve evidence for: {missing}"
            raise ValueError(msg)

        ready_sorted = sorted(ready, key=lambda rule: (rule.priority, rule.name))
        for rule in ready_sorted:
            resolved.append(rule)
            pending.remove(rule)
            _register_rule_output(work, rule)
    return resolved


def _ready_rules(
    pending: Sequence[RelationshipRule], evidence: EvidenceCatalog
) -> list[RelationshipRule]:
    ready: list[RelationshipRule] = []
    for rule in pending:
        inputs = tuple(ref.name for ref in rule.inputs)
        if evidence.satisfies(rule.evidence, inputs=inputs):
            ready.append(rule)
    return ready


def _register_rule_output(evidence: EvidenceCatalog, rule: RelationshipRule) -> None:
    output_schema = _virtual_output_schema(rule)
    if output_schema is not None:
        evidence.register(rule.output_dataset, output_schema)
    else:
        evidence.sources.add(rule.output_dataset)


def _schema_from_source(source: object, *, ctx: ExecutionContext) -> SchemaLike | None:
    if isinstance(source, Plan):
        return source.schema(ctx=ctx)
    if isinstance(source, DatasetSource):
        return source.dataset.schema
    schema = getattr(source, "schema", None)
    if schema is not None and hasattr(schema, "names"):
        return schema
    return None


def _schema_types(schema: SchemaLike) -> dict[str, str]:
    return {field.name: str(field.type) for field in schema}


def _virtual_output_schema(rule: RelationshipRule) -> SchemaLike | None:
    if rule.contract_name:
        dataset_spec = GLOBAL_SCHEMA_REGISTRY.dataset_specs.get(rule.contract_name)
        if dataset_spec is not None:
            return dataset_spec.schema()
        if rule.contract_name == RELATION_OUTPUT_NAME:
            return relation_output_schema()
        return None
    return relation_output_schema()


__all__ = ["EvidenceCatalog", "GraphPlan", "RuleNode", "compile_graph_plan", "order_rules"]
