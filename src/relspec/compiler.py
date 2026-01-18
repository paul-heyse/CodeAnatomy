"""Compile relationship rules into relational plans and kernel operations."""

from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from typing import cast

import ibis
import pyarrow as pa
from ibis.backends import BaseBackend
from ibis.expr.types import Table as IbisTable
from ibis.expr.types import Value as IbisValue

from arrowdsl.compute.filters import FilterSpec
from arrowdsl.compute.kernels import canonical_sort, resolve_kernel
from arrowdsl.core.context import DeterminismTier, ExecutionContext, Ordering
from arrowdsl.core.interop import RecordBatchReaderLike, SchemaLike, TableLike
from arrowdsl.finalize.finalize import Contract, FinalizeResult
from arrowdsl.json_factory import json_default
from arrowdsl.plan.ops import DedupeSpec, IntervalAlignOptions, SortKey
from arrowdsl.plan.query import ScanTelemetry
from arrowdsl.schema.build import const_array, set_or_append_column
from arrowdsl.schema.schema import SchemaEvolutionSpec, SchemaMetadataSpec
from datafusion_engine.runtime import AdapterExecutionPolicy, ExecutionLabel
from engine.materialize import build_plan_product
from engine.plan_policy import ExecutionSurfacePolicy
from ibis_engine.compiler_checkpoint import try_plan_hash
from ibis_engine.execution import IbisExecutionContext
from ibis_engine.lineage import required_columns_by_table
from ibis_engine.plan import IbisPlan
from ibis_engine.query_compiler import IbisQuerySpec, apply_query_spec
from ibis_engine.registry import IbisDatasetRegistry
from ibis_engine.schema_utils import validate_expr_schema
from ibis_engine.sources import SourceToIbisOptions, table_to_ibis
from relspec.edge_contract_validator import (
    EdgeContractValidationConfig,
    validate_relationship_output_contracts_for_edge_kinds,
)
from relspec.engine import IbisRelPlanCompiler, PlanResolver, RelPlanCompiler, output_plan_hash
from relspec.model import (
    AddLiteralSpec,
    CanonicalSortKernelSpec,
    DatasetRef,
    DedupeKernelSpec,
    DropColumnsSpec,
    ExplodeListSpec,
    FilterKernelSpec,
    KernelSpecT,
    ProjectConfig,
    RelationshipRule,
    RenameColumnsSpec,
    RuleKind,
)
from relspec.plan import (
    RelJoin,
    RelNode,
    RelPlan,
    RelProject,
    RelSource,
    RelUnion,
    rel_plan_signature,
)
from relspec.policies import (
    ambiguity_policy_from_schema,
    confidence_policy_from_schema,
    default_tie_breakers,
    evidence_spec_from_schema,
)
from relspec.registry import ContractCatalog, DatasetCatalog
from relspec.rules.exec_events import RuleExecutionObserver, rule_execution_event_from_table
from relspec.rules.policies import PolicyRegistry
from schema_spec.system import GLOBAL_SCHEMA_REGISTRY, dataset_spec_from_contract
from sqlglot_tools.bridge import IbisCompilerBackend

KernelFn = Callable[[TableLike, ExecutionContext], TableLike]
PlanExecutor = Callable[
    [IbisPlan, ExecutionContext, Mapping[IbisValue, object] | None, ExecutionLabel | None],
    TableLike,
]


def _scan_ordering_for_ctx(ctx: ExecutionContext) -> Ordering:
    """Return ordering metadata for scan output based on context.

    Parameters
    ----------
    ctx:
        Execution context.

    Returns
    -------
    Ordering
        Ordering marker for scan output.
    """
    if ctx.runtime.scan.implicit_ordering or ctx.runtime.scan.require_sequenced_output:
        return Ordering.implicit()
    return Ordering.unordered()


def runtime_telemetry_for_ctx(ctx: ExecutionContext) -> Mapping[str, object]:
    """Return runtime telemetry payload for the execution context.

    Parameters
    ----------
    ctx
        Execution context with runtime profiles.

    Returns
    -------
    Mapping[str, object]
        Telemetry payload for the runtime backend.
    """
    profile = ctx.runtime.datafusion
    if profile is None:
        return {}
    return {"datafusion": profile.telemetry_payload_v1()}


def _rule_signature_for_output(rule: RelationshipRule) -> str:
    """Return a stable signature hash for a relationship rule.

    Parameters
    ----------
    rule
        Rule definition to hash.

    Returns
    -------
    str
        Deterministic hash for the rule configuration payload.
    """
    payload = json_default(rule)
    encoded = json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _compiled_rule_signature(compiled: CompiledRule) -> str:
    """Return a stable signature hash for a compiled rule.

    Parameters
    ----------
    compiled
        Compiled rule instance.

    Returns
    -------
    str
        Deterministic signature hash for the rule plan.
    """
    if compiled.rel_plan is not None:
        return rel_plan_signature(compiled.rel_plan)
    return _rule_signature_for_output(compiled.rule)


def _input_dataset_names(rules: Sequence[RelationshipRule]) -> tuple[str, ...]:
    """Return sorted input dataset names for a rule set.

    Parameters
    ----------
    rules
        Relationship rules contributing to an output.

    Returns
    -------
    tuple[str, ...]
        Sorted input dataset names.
    """
    names = {ref.name for rule in rules for ref in rule.inputs}
    return tuple(sorted(names))


def _adapter_plan_executor(
    *,
    execution_policy: AdapterExecutionPolicy | None,
    ibis_backend: BaseBackend | None,
    surface_policy: ExecutionSurfacePolicy | None,
) -> PlanExecutor:
    def _resolved_policy(exec_ctx: ExecutionContext) -> ExecutionSurfacePolicy:
        if surface_policy is not None:
            return surface_policy
        return ExecutionSurfacePolicy(determinism_tier=exec_ctx.determinism)

    def _plan_id(label: ExecutionLabel | None) -> str | None:
        if label is None:
            return None
        return f"{label.rule_name}:{label.output_dataset}"

    def _executor(
        plan: IbisPlan,
        exec_ctx: ExecutionContext,
        exec_params: Mapping[IbisValue, object] | None,
        execution_label: ExecutionLabel | None = None,
    ) -> TableLike:
        execution = IbisExecutionContext(
            ctx=exec_ctx,
            execution_policy=execution_policy,
            execution_label=execution_label,
            ibis_backend=ibis_backend,
            params=exec_params,
        )
        product = build_plan_product(
            plan,
            execution=execution,
            policy=_resolved_policy(exec_ctx),
            plan_id=_plan_id(execution_label),
        )
        return product.materialize_table()

    return _executor


@dataclass(frozen=True)
class RuleExecutionOptions:
    """Execution options for compiled rules."""

    params: Mapping[IbisValue, object] | None = None
    plan_executor: PlanExecutor | None = None
    execution_policy: AdapterExecutionPolicy | None = None
    ibis_backend: BaseBackend | None = None
    surface_policy: ExecutionSurfacePolicy | None = None


class FilesystemPlanResolver(PlanResolver[IbisPlan]):
    """Resolve dataset names to filesystem-backed Ibis tables."""

    def __init__(
        self,
        catalog: DatasetCatalog,
        *,
        registry: IbisDatasetRegistry,
    ) -> None:
        self.catalog = catalog
        if registry.catalog is not catalog:
            for name in catalog.names():
                registry.catalog.register(name, catalog.get(name))
        self.registry = registry
        self._backend: BaseBackend | None = registry.backend

    @property
    def backend(self) -> BaseBackend | None:
        """Return the backend associated with this resolver."""
        return self._backend

    def resolve(self, ref: DatasetRef, *, ctx: ExecutionContext) -> IbisPlan:
        """Resolve a dataset reference into a filesystem-backed plan.

        Parameters
        ----------
        ref:
            Dataset reference to resolve.
        ctx:
            Execution context.

        Returns
        -------
        IbisPlan
            Executable plan for the dataset reference.
        """
        table = self.registry.table(ref.name)
        if ref.query is not None:
            table = apply_query_spec(table, spec=ref.query)
        return IbisPlan(expr=table, ordering=_scan_ordering_for_ctx(ctx))

    @staticmethod
    def telemetry(ref: DatasetRef, *, ctx: ExecutionContext) -> ScanTelemetry | None:
        """Return scan telemetry for filesystem-backed datasets (unavailable).

        Returns
        -------
        ScanTelemetry | None
            ``None`` because Ibis scans do not expose telemetry.
        """
        _ = ref
        _ = ctx
        return None


@dataclass(frozen=True)
class RequiredColumnResolver:
    """Plan resolver wrapper that applies required column projections."""

    base: PlanResolver[IbisPlan]
    required_columns: Mapping[str, Sequence[str]]

    @property
    def backend(self) -> BaseBackend | None:
        """Return the backend associated with this resolver.

        Returns
        -------
        ibis.backends.BaseBackend | None
            Backend used by the underlying resolver.
        """
        return self.base.backend

    def resolve(self, ref: DatasetRef, *, ctx: ExecutionContext) -> IbisPlan:
        """Resolve a dataset reference with required column projection.

        Returns
        -------
        IbisPlan
            Plan with required column projection applied when available.
        """
        plan = self.base.resolve(ref, ctx=ctx)
        required = self.required_columns.get(ref.name)
        if not required and ref.label:
            required = self.required_columns.get(ref.label)
        if not required:
            return plan
        projected = apply_query_spec(plan.expr, spec=IbisQuerySpec.simple(*required))
        return IbisPlan(expr=projected, ordering=plan.ordering)

    def telemetry(self, ref: DatasetRef, *, ctx: ExecutionContext) -> ScanTelemetry | None:
        """Return scan telemetry for the dataset reference.

        Returns
        -------
        ScanTelemetry | None
            Telemetry data when available.
        """
        return self.base.telemetry(ref, ctx=ctx)


class InMemoryPlanResolver(PlanResolver[IbisPlan]):
    """Resolve dataset names to in-memory tables or plans."""

    def __init__(
        self,
        mapping: Mapping[str, TableLike | IbisPlan | IbisTable],
        *,
        backend: BaseBackend | None = None,
    ) -> None:
        self.mapping = dict(mapping)
        self._backend: BaseBackend | None = backend

    @property
    def backend(self) -> BaseBackend | None:
        """Return the backend associated with this resolver."""
        return self._backend

    def resolve(self, ref: DatasetRef, *, ctx: ExecutionContext) -> IbisPlan:
        """Resolve a dataset reference into an in-memory plan.

        Parameters
        ----------
        ref:
            Dataset reference to resolve.
        ctx:
            Execution context.

        Returns
        -------
        IbisPlan
            Executable plan for the dataset reference.

        Raises
        ------
        KeyError
            Raised when the dataset reference is unknown.
        """
        obj = self.mapping.get(ref.name)
        if obj is None:
            msg = f"InMemoryPlanResolver: unknown dataset {ref.name!r}."
            raise KeyError(msg)
        if isinstance(obj, IbisPlan):
            plan = obj
        elif isinstance(obj, IbisTable):
            plan = IbisPlan(expr=obj, ordering=_scan_ordering_for_ctx(ctx))
        else:
            plan = _ibis_plan_from_table_like(
                obj,
                ordering=_scan_ordering_for_ctx(ctx),
                backend=self.backend,
                name=ref.name,
            )
        if ref.query is not None:
            expr = apply_query_spec(plan.expr, spec=ref.query)
            plan = IbisPlan(expr=expr, ordering=plan.ordering)
        return plan

    @staticmethod
    def telemetry(ref: DatasetRef, *, ctx: ExecutionContext) -> ScanTelemetry | None:
        """Return scan telemetry for in-memory datasets (unavailable).

        Returns
        -------
        ScanTelemetry | None
            ``None`` because in-memory datasets do not have scan telemetry.
        """
        _ = ref
        _ = ctx
        return None


def _ibis_plan_from_table_like(
    value: TableLike | RecordBatchReaderLike,
    *,
    ordering: Ordering,
    backend: BaseBackend | None = None,
    name: str | None = None,
) -> IbisPlan:
    """Build an Ibis plan from Arrow table-like inputs.

    Parameters
    ----------
    value
        Table-like input or record batch reader.
    ordering
        Ordering metadata for the resulting plan.
    backend
        Optional Ibis backend to register the table with.
    name
        Optional table name when using a backend.

    Returns
    -------
    IbisPlan
        Plan wrapping the table-like input.
    """
    table = _ensure_table(value)
    if backend is not None:
        return table_to_ibis(
            table,
            options=SourceToIbisOptions(
                backend=backend,
                name=name,
                ordering=ordering,
            ),
        )
    expr = ibis.memtable(table)
    return IbisPlan(expr=expr, ordering=ordering)


def _ensure_table(value: TableLike | RecordBatchReaderLike) -> pa.Table:
    """Return a concrete Arrow table from table-like inputs.

    Parameters
    ----------
    value
        Table-like input or record batch reader.

    Returns
    -------
    pyarrow.Table
        Materialized Arrow table.
    """
    if isinstance(value, RecordBatchReaderLike):
        return value.read_all()
    if isinstance(value, pa.Table):
        return value
    return cast("pa.Table", value)


def _build_rule_meta_kernel(rule: RelationshipRule) -> KernelFn:
    """Build a kernel that injects rule metadata columns.

    Parameters
    ----------
    rule
        Relationship rule providing metadata values.

    Returns
    -------
    KernelFn
        Kernel that adds rule name and priority columns.
    """

    def _add_rule_meta(table: TableLike, _ctx: ExecutionContext) -> TableLike:
        """Append rule metadata columns when missing.

        Returns
        -------
        TableLike
            Table with rule metadata columns applied.
        """
        count = table.num_rows
        if rule.rule_name_col not in table.column_names:
            table = set_or_append_column(
                table,
                rule.rule_name_col,
                const_array(count, rule.name, dtype=pa.string()),
            )
        if rule.rule_priority_col not in table.column_names:
            table = set_or_append_column(
                table,
                rule.rule_priority_col,
                const_array(count, int(rule.priority), dtype=pa.int32()),
            )
        return table

    return _add_rule_meta


def _rule_metadata_spec(rule: RelationshipRule) -> SchemaMetadataSpec:
    """Build schema metadata spec containing rule metadata.

    Parameters
    ----------
    rule
        Relationship rule with metadata values.

    Returns
    -------
    SchemaMetadataSpec
        Schema metadata spec for rule name and priority.
    """
    return SchemaMetadataSpec(
        schema_metadata={
            b"rule_name": rule.name.encode("utf-8"),
            b"rule_priority": str(rule.priority).encode("utf-8"),
        }
    )


def _apply_rule_metadata(table: TableLike, *, rule: RelationshipRule) -> TableLike:
    """Apply rule metadata to a table schema.

    Parameters
    ----------
    table
        Table to update.
    rule
        Relationship rule providing metadata values.

    Returns
    -------
    TableLike
        Table with updated schema metadata.
    """
    schema = _rule_metadata_spec(rule).apply(table.schema)
    return table.cast(schema)


def _build_add_literal_kernel(spec: AddLiteralSpec) -> KernelFn:
    """Build a kernel that adds a literal column when missing.

    Parameters
    ----------
    spec
        Literal column specification.

    Returns
    -------
    KernelFn
        Kernel that adds the literal column.
    """

    def _fn(table: TableLike, _ctx: ExecutionContext) -> TableLike:
        """Add the literal column if it does not exist.

        Returns
        -------
        TableLike
            Table with the literal column added when missing.
        """
        if spec.name in table.column_names:
            return table
        return set_or_append_column(table, spec.name, const_array(table.num_rows, spec.value))

    return _fn


def _build_drop_columns_kernel(spec: DropColumnsSpec) -> KernelFn:
    """Build a kernel that drops specified columns.

    Parameters
    ----------
    spec
        Drop-columns specification.

    Returns
    -------
    KernelFn
        Kernel that drops columns from the table.
    """

    def _fn(table: TableLike, _ctx: ExecutionContext) -> TableLike:
        """Drop requested columns when present.

        Returns
        -------
        TableLike
            Table with requested columns removed when present.
        """
        cols = [col for col in spec.columns if col in table.column_names]
        return table.drop(cols) if cols else table

    return _fn


def _build_filter_kernel(spec: FilterKernelSpec) -> KernelFn:
    """Build a kernel that filters rows using a predicate.

    Parameters
    ----------
    spec
        Filter specification.

    Returns
    -------
    KernelFn
        Kernel that filters a table.
    """

    def _fn(table: TableLike, _ctx: ExecutionContext) -> TableLike:
        """Filter rows using the configured predicate.

        Returns
        -------
        TableLike
            Filtered table.
        """
        predicate = FilterSpec(spec.predicate.to_expr_spec()).mask(table)
        return table.filter(predicate)

    return _fn


def _build_rename_columns_kernel(spec: RenameColumnsSpec) -> KernelFn:
    """Build a kernel that renames columns per mapping.

    Parameters
    ----------
    spec
        Rename-columns specification.

    Returns
    -------
    KernelFn
        Kernel that renames columns.
    """

    def _fn(table: TableLike, _ctx: ExecutionContext) -> TableLike:
        """Rename columns when a mapping is provided.

        Returns
        -------
        TableLike
            Table with renamed columns when a mapping is provided.
        """
        if not spec.mapping:
            return table
        names = list(table.column_names)
        new_names = [spec.mapping.get(name, name) for name in names]
        return table.rename_columns(new_names)

    return _fn


def _build_explode_list_kernel(spec: ExplodeListSpec) -> KernelFn:
    """Build a kernel that explodes list columns into parent/value pairs.

    Parameters
    ----------
    spec
        Explode-list kernel specification.

    Returns
    -------
    KernelFn
        Kernel that explodes list columns.
    """

    def _fn(table: TableLike, ctx: ExecutionContext) -> TableLike:
        """Explode list values using the configured kernel.

        Returns
        -------
        TableLike
            Table with exploded list columns.
        """
        kernel = resolve_kernel("explode_list", ctx=ctx)
        return kernel(
            table,
            parent_id_col=spec.parent_id_col,
            list_col=spec.list_col,
            out_parent_col=spec.out_parent_col,
            out_value_col=spec.out_value_col,
        )

    return _fn


def _build_dedupe_kernel(spec: DedupeKernelSpec) -> KernelFn:
    """Build a kernel that deduplicates rows using default tie breakers.

    Parameters
    ----------
    spec
        Dedupe kernel specification.

    Returns
    -------
    KernelFn
        Kernel that deduplicates a table.
    """

    def _fn(table: TableLike, ctx: ExecutionContext) -> TableLike:
        """Apply dedupe kernel with schema-derived defaults.

        Returns
        -------
        TableLike
            Table with deduplication applied.
        """
        resolved = _dedupe_spec_with_defaults(spec.spec, schema=table.schema)
        kernel = resolve_kernel("dedupe", ctx=ctx)
        return kernel(table, spec=resolved)

    return _fn


def _build_canonical_sort_kernel(spec: CanonicalSortKernelSpec) -> KernelFn:
    def _fn(table: TableLike, ctx: ExecutionContext) -> TableLike:
        _ = ctx
        if not spec.sort_keys:
            return table
        return canonical_sort(table, sort_keys=spec.sort_keys)

    return _fn


_KERNEL_BUILDERS: tuple[tuple[type[object], Callable[[object], KernelFn]], ...] = (
    (AddLiteralSpec, cast("Callable[[object], KernelFn]", _build_add_literal_kernel)),
    (DropColumnsSpec, cast("Callable[[object], KernelFn]", _build_drop_columns_kernel)),
    (FilterKernelSpec, cast("Callable[[object], KernelFn]", _build_filter_kernel)),
    (RenameColumnsSpec, cast("Callable[[object], KernelFn]", _build_rename_columns_kernel)),
    (ExplodeListSpec, cast("Callable[[object], KernelFn]", _build_explode_list_kernel)),
    (DedupeKernelSpec, cast("Callable[[object], KernelFn]", _build_dedupe_kernel)),
    (CanonicalSortKernelSpec, cast("Callable[[object], KernelFn]", _build_canonical_sort_kernel)),
)


def _kernel_from_spec(spec: object) -> KernelFn:
    """Resolve a kernel function from a kernel spec.

    Parameters
    ----------
    spec
        Kernel specification to resolve.

    Returns
    -------
    KernelFn
        Resolved kernel function.

    Raises
    ------
    ValueError
        Raised when the kernel spec type is unknown.
    """
    handler: Callable[[object], KernelFn] | None = None
    for spec_type, builder in _KERNEL_BUILDERS:
        if isinstance(spec, spec_type):
            handler = builder
            break
    if handler is None:
        msg = f"Unknown KernelSpec type: {type(spec).__name__}."
        raise ValueError(msg)
    return handler(spec)


def _compile_post_kernels(specs: Sequence[KernelSpecT]) -> tuple[KernelFn, ...]:
    """Compile kernel specs into executable kernel functions.

    Parameters
    ----------
    specs
        Kernel specifications to compile.

    Returns
    -------
    tuple[KernelFn, ...]
        Kernel functions in spec order.
    """
    return tuple(_kernel_from_spec(spec) for spec in specs)


def _dedupe_spec_with_defaults(spec: DedupeSpec, *, schema: SchemaLike) -> DedupeSpec:
    """Apply default tie breakers for score-based deduplication.

    Parameters
    ----------
    spec
        Deduplication specification.
    schema
        Schema used to derive default tie breakers.

    Returns
    -------
    DedupeSpec
        Updated dedupe spec with defaults applied when needed.
    """
    if spec.strategy != "KEEP_BEST_BY_SCORE":
        return spec
    defaults = default_tie_breakers(schema)
    filtered_defaults = tuple(sk for sk in defaults if sk.column != "score")
    if spec.tie_breakers:
        if not filtered_defaults:
            return spec
        existing = {sk.column for sk in spec.tie_breakers}
        extra = tuple(sk for sk in filtered_defaults if sk.column not in existing)
        if not extra:
            return spec
        return DedupeSpec(
            keys=spec.keys,
            strategy=spec.strategy,
            tie_breakers=tuple(spec.tie_breakers) + extra,
        )
    if "score" not in schema.names:
        return spec
    return DedupeSpec(
        keys=spec.keys,
        strategy=spec.strategy,
        tie_breakers=(SortKey("score", "descending"), *filtered_defaults),
    )


def _schema_for_contract(contract: Contract) -> SchemaLike:
    """Resolve schema for a contract via the global registry.

    Parameters
    ----------
    contract
        Contract to resolve.

    Returns
    -------
    SchemaLike
        Schema for the contract.
    """
    dataset_spec = GLOBAL_SCHEMA_REGISTRY.dataset_specs.get(contract.name)
    if dataset_spec is not None:
        return dataset_spec.schema()
    return contract.schema


def _schema_for_rule(
    rule: RelationshipRule,
    *,
    contracts: ContractCatalog | None,
) -> SchemaLike | None:
    """Resolve schema for a rule's contract when available.

    Parameters
    ----------
    rule
        Relationship rule with optional contract reference.
    contracts
        Optional contract catalog.

    Returns
    -------
    SchemaLike | None
        Schema for the rule contract when available.
    """
    if rule.contract_name is None:
        return None
    if contracts is not None:
        return _schema_for_contract(contracts.get(rule.contract_name))
    dataset_spec = GLOBAL_SCHEMA_REGISTRY.dataset_specs.get(rule.contract_name)
    if dataset_spec is not None:
        return dataset_spec.schema()
    return None


def apply_policy_defaults(
    rule: RelationshipRule,
    schema: SchemaLike,
    *,
    registry: PolicyRegistry,
) -> RelationshipRule:
    """Apply schema-derived default policies to a rule.

    Parameters
    ----------
    rule:
        Rule to update.
    schema:
        Schema providing policy metadata.
    registry:
        Policy registry for resolving named policies.

    Returns
    -------
    RelationshipRule
        Rule with policies populated when missing.
    """
    confidence = rule.confidence_policy or confidence_policy_from_schema(
        schema,
        registry=registry,
    )
    ambiguity = rule.ambiguity_policy or ambiguity_policy_from_schema(
        schema,
        registry=registry,
    )
    if confidence is rule.confidence_policy and ambiguity is rule.ambiguity_policy:
        return rule
    return replace(
        rule,
        confidence_policy=confidence,
        ambiguity_policy=ambiguity,
    )


def validate_policy_requirements(rule: RelationshipRule, schema: SchemaLike) -> None:
    """Validate policy-specific schema requirements.

    Parameters
    ----------
    rule:
        Rule to validate.
    schema:
        Schema providing available columns.

    Raises
    ------
    ValueError
        Raised when required columns are missing.
    """
    names = set(schema.names)
    if rule.confidence_policy is not None:
        required = {"confidence", "score"}
        missing = required - names
        if missing:
            msg = f"Rule {rule.name!r} missing confidence columns: {sorted(missing)}."
            raise ValueError(msg)
    if rule.ambiguity_policy is not None and "ambiguity_group_id" not in names:
        msg = f"Rule {rule.name!r} requires ambiguity_group_id column."
        raise ValueError(msg)


def _source_node(ref: DatasetRef) -> RelSource:
    """Build a source node from a dataset reference.

    Parameters
    ----------
    ref
        Dataset reference.

    Returns
    -------
    RelSource
        Source node for relational plans.
    """
    return RelSource(ref=ref, query=ref.query, label=ref.label or ref.name)


def _project_node(node: RelNode, project: ProjectConfig | None) -> RelNode:
    """Apply a projection config to a relational plan node.

    Parameters
    ----------
    node
        Input relational plan node.
    project
        Optional projection configuration.

    Returns
    -------
    RelNode
        Projected node when configuration is provided.
    """
    if project is None or (not project.select and not project.exprs):
        return node
    return RelProject(source=node, columns=project.select, derived=dict(project.exprs))


def rel_plan_for_rule(rule: RelationshipRule) -> RelPlan | None:
    """Return a relational plan for rules that can compile to plan lane.

    Returns
    -------
    RelPlan | None
        Compiled plan for plan-lane rules, otherwise ``None``.

    Raises
    ------
    ValueError
        Raised when a rule is missing required configuration.
    """
    if rule.kind == RuleKind.FILTER_PROJECT:
        node = _project_node(_source_node(rule.inputs[0]), rule.project)
        return RelPlan(root=node, ordering=Ordering.unordered(), label=rule.name)
    if rule.kind == RuleKind.HASH_JOIN:
        if rule.hash_join is None:
            msg = "HASH_JOIN rules require hash_join config."
            raise ValueError(msg)
        left, right = rule.inputs
        join_node = RelJoin(
            left=_source_node(left),
            right=_source_node(right),
            join=rule.hash_join,
            label=rule.name,
        )
        node = _project_node(join_node, rule.project)
        return RelPlan(root=node, ordering=Ordering.unordered(), label=rule.name)
    if rule.kind == RuleKind.EXPLODE_LIST:
        return RelPlan(
            root=_source_node(rule.inputs[0]),
            ordering=Ordering.unordered(),
            label=rule.name,
        )
    if rule.kind == RuleKind.UNION_ALL:
        union_node = RelUnion(
            inputs=tuple(_source_node(ref) for ref in rule.inputs),
            distinct=False,
            label=rule.name,
        )
        node = _project_node(union_node, rule.project)
        return RelPlan(root=node, ordering=Ordering.unordered(), label=rule.name)
    return None


@dataclass(frozen=True)
class CompiledRule:
    """Executable compiled form of a rule."""

    rule: RelationshipRule
    rel_plan: RelPlan | None
    execute_fn: Callable[[ExecutionContext, PlanResolver[IbisPlan], PlanExecutor], TableLike] | None
    post_kernels: tuple[KernelFn, ...] = ()
    emit_rule_meta: bool = True
    required_columns: Mapping[str, tuple[str, ...]] = field(default_factory=dict)

    def execute(
        self,
        *,
        ctx: ExecutionContext,
        resolver: PlanResolver[IbisPlan],
        compiler: RelPlanCompiler[IbisPlan],
        options: RuleExecutionOptions | None = None,
    ) -> TableLike:
        """Execute the compiled rule into a table.

        Parameters
        ----------
        ctx:
            Execution context.
        resolver:
            Plan resolver for dataset references.
        compiler:
            Compiler for relationship plans.
        options:
            Optional execution options for plan materialization.

        Returns
        -------
        pyarrow.Table
            Rule output table.

        Raises
        ------
        RuntimeError
            Raised when no execution path is available.
        """
        options = options or RuleExecutionOptions()
        label = ExecutionLabel(
            rule_name=self.rule.name,
            output_dataset=self.rule.output_dataset,
        )
        if options.plan_executor is None:
            resolved_backend = options.ibis_backend or resolver.backend
            resolved_executor = _adapter_plan_executor(
                execution_policy=options.execution_policy,
                ibis_backend=resolved_backend,
                surface_policy=options.surface_policy,
            )
        else:
            resolved_executor = options.plan_executor

        def _execute_plan(
            plan: IbisPlan,
            exec_ctx: ExecutionContext,
            exec_params: Mapping[IbisValue, object] | None,
            execution_label: ExecutionLabel | None = None,
        ) -> TableLike:
            resolved_params = exec_params if exec_params is not None else options.params
            effective_label = execution_label or label
            return resolved_executor(plan, exec_ctx, resolved_params, effective_label)

        effective_resolver: PlanResolver[IbisPlan] = resolver
        if self.required_columns:
            effective_resolver = RequiredColumnResolver(
                base=resolver,
                required_columns=self.required_columns,
            )
        if self.execute_fn is not None:
            table = self.execute_fn(ctx, effective_resolver, _execute_plan)
        elif self.rel_plan is not None:
            plan = compiler.compile(self.rel_plan, ctx=ctx, resolver=effective_resolver)
            table = resolved_executor(plan, ctx, options.params, label)
        else:
            msg = "CompiledRule has neither rel_plan nor execute_fn."
            raise RuntimeError(msg)
        if self.emit_rule_meta:
            table = _build_rule_meta_kernel(self.rule)(table, ctx)
        for fn in self.post_kernels:
            table = fn(table, ctx)
        return _apply_rule_metadata(table, rule=self.rule)


@dataclass(frozen=True)
class CompiledOutputExecutionOptions:
    """Execution options for compiled outputs."""

    contracts: ContractCatalog
    compiler: RelPlanCompiler[IbisPlan] | None = None
    params: Mapping[IbisValue, object] | None = None
    plan_executor: PlanExecutor | None = None
    execution_policy: AdapterExecutionPolicy | None = None
    ibis_backend: BaseBackend | None = None
    rule_exec_observer: RuleExecutionObserver | None = None
    surface_policy: ExecutionSurfacePolicy | None = None


@dataclass(frozen=True)
class CompiledOutput:
    """Compiled output with contributing rules."""

    output_dataset: str
    contract_name: str | None
    plan_hash: str | None = None
    input_datasets: tuple[str, ...] = ()
    contributors: tuple[CompiledRule, ...] = ()
    telemetry: Mapping[str, ScanTelemetry] = field(default_factory=dict)
    runtime_telemetry: Mapping[str, object] = field(default_factory=dict)

    def execute(
        self,
        *,
        ctx: ExecutionContext,
        resolver: PlanResolver[IbisPlan],
        options: CompiledOutputExecutionOptions,
    ) -> FinalizeResult:
        """Execute contributing rules and finalize against the output contract.

        Parameters
        ----------
        ctx:
            Execution context.
        resolver:
            Plan resolver for dataset references.
        options:
            Execution options including compiler, contracts, and parameter bindings.

        Returns
        -------
        FinalizeResult
            Finalized tables for the output dataset.

        Raises
        ------
        ValueError
            Raised when the output has no contributors.
        """
        ctx_exec = ctx
        if ctx.determinism == DeterminismTier.CANONICAL and not ctx.provenance:
            ctx_exec = ctx.with_provenance(provenance=True)
        if not self.contributors:
            msg = f"CompiledOutput {self.output_dataset!r} has no contributors."
            raise ValueError(msg)
        plan_compiler = options.compiler or IbisRelPlanCompiler()
        expected_schema = None
        if self.contract_name is not None:
            expected_schema = options.contracts.get(self.contract_name).schema
        rule_options = RuleExecutionOptions(
            params=options.params,
            plan_executor=options.plan_executor,
            execution_policy=options.execution_policy,
            ibis_backend=options.ibis_backend,
            surface_policy=options.surface_policy,
        )
        if expected_schema is not None:
            for compiled in self.contributors:
                _validate_compiled_rule_schema(
                    compiled,
                    expected_schema=expected_schema,
                    compiler=plan_compiler,
                    ctx=ctx_exec,
                    resolver=resolver,
                )
        table_parts: list[TableLike] = []
        observer = options.rule_exec_observer
        if observer is None:
            table_parts.extend(
                compiled.execute(
                    ctx=ctx_exec,
                    resolver=resolver,
                    compiler=plan_compiler,
                    options=rule_options,
                )
                for compiled in self.contributors
            )
        else:
            for compiled in self.contributors:
                started = time.perf_counter()
                table = compiled.execute(
                    ctx=ctx_exec,
                    resolver=resolver,
                    compiler=plan_compiler,
                    options=rule_options,
                )
                plan_hash = None
                if compiled.rel_plan is not None:
                    try:
                        plan = plan_compiler.compile(
                            compiled.rel_plan,
                            ctx=ctx_exec,
                            resolver=resolver,
                        )
                        plan_hash = try_plan_hash(
                            plan.expr,
                            backend=rule_options.ibis_backend or resolver.backend,
                        )
                    except (TypeError, ValueError):
                        plan_hash = None
                duration_ms = (time.perf_counter() - started) * 1000.0
                observer.record(
                    rule_execution_event_from_table(
                        rule_name=compiled.rule.name,
                        output_dataset=compiled.rule.output_dataset,
                        table=table,
                        duration_ms=duration_ms,
                        plan_hash=plan_hash,
                    )
                )
                table_parts.append(table)
        return _finalize_output_tables(
            output_dataset=self.output_dataset,
            contract_name=self.contract_name,
            table_parts=table_parts,
            ctx=ctx_exec,
            contracts=options.contracts,
        )


def _validate_compiled_rule_schema(
    compiled: CompiledRule,
    *,
    expected_schema: pa.Schema | None,
    compiler: RelPlanCompiler[IbisPlan],
    ctx: ExecutionContext,
    resolver: PlanResolver[IbisPlan],
) -> None:
    if expected_schema is None or compiled.rel_plan is None:
        return
    plan = compiler.compile(compiled.rel_plan, ctx=ctx, resolver=resolver)
    validate_expr_schema(plan.expr, expected=expected_schema)


class RelationshipRuleCompiler:
    """Compile ``RelationshipRule`` instances into executable units."""

    def __init__(
        self,
        *,
        resolver: PlanResolver[IbisPlan],
        plan_compiler: RelPlanCompiler[IbisPlan] | None = None,
    ) -> None:
        self.resolver = resolver
        self.plan_compiler = plan_compiler or IbisRelPlanCompiler()

    @staticmethod
    def _compile_filter_project(
        rule: RelationshipRule,
        *,
        post_kernels: tuple[KernelSpecT, ...],
    ) -> CompiledRule:
        """Compile a filter/project rule into a plan-based compiled rule.

        Parameters
        ----------
        rule
            Relationship rule to compile.
        post_kernels
            Post-kernel specs to apply after execution.

        Returns
        -------
        CompiledRule
            Compiled rule instance.
        """
        src = _source_node(rule.inputs[0])
        node = _project_node(src, rule.project)
        plan = RelPlan(root=node, ordering=Ordering.unordered(), label=rule.name)
        return CompiledRule(
            rule=rule,
            rel_plan=plan,
            execute_fn=None,
            post_kernels=_compile_post_kernels(post_kernels),
            emit_rule_meta=rule.emit_rule_meta,
        )

    @staticmethod
    def _compile_hash_join(
        rule: RelationshipRule,
        *,
        post_kernels: tuple[KernelSpecT, ...],
    ) -> CompiledRule:
        """Compile a hash-join rule into a plan-based compiled rule.

        Parameters
        ----------
        rule
            Relationship rule to compile.
        post_kernels
            Post-kernel specs to apply after execution.

        Returns
        -------
        CompiledRule
            Compiled rule instance.

        Raises
        ------
        ValueError
            Raised when required hash-join config is missing.
        """
        if rule.hash_join is None:
            msg = "HASH_JOIN rules require hash_join config."
            raise ValueError(msg)
        left_ref, right_ref = rule.inputs
        join_cfg = rule.hash_join
        left = _source_node(left_ref)
        right = _source_node(right_ref)
        join_node = RelJoin(left=left, right=right, join=join_cfg, label=rule.name)
        node = _project_node(join_node, rule.project)
        plan = RelPlan(root=node, ordering=Ordering.unordered(), label=rule.name)
        return CompiledRule(
            rule=rule,
            rel_plan=plan,
            execute_fn=None,
            post_kernels=_compile_post_kernels(post_kernels),
            emit_rule_meta=rule.emit_rule_meta,
        )

    @staticmethod
    def _compile_passthrough(
        rule: RelationshipRule,
        *,
        post_kernels: tuple[KernelSpecT, ...],
    ) -> CompiledRule:
        """Compile a passthrough rule into a plan-based compiled rule.

        Parameters
        ----------
        rule
            Relationship rule to compile.
        post_kernels
            Post-kernel specs to apply after execution.

        Returns
        -------
        CompiledRule
            Compiled rule instance.
        """
        src = _source_node(rule.inputs[0])
        plan = RelPlan(root=src, ordering=Ordering.unordered(), label=rule.name)
        return CompiledRule(
            rule=rule,
            rel_plan=plan,
            execute_fn=None,
            post_kernels=_compile_post_kernels(post_kernels),
            emit_rule_meta=rule.emit_rule_meta,
        )

    @staticmethod
    def _compile_union_all(
        rule: RelationshipRule,
        *,
        post_kernels: tuple[KernelSpecT, ...],
    ) -> CompiledRule:
        """Compile a union-all rule into a plan-based compiled rule.

        Parameters
        ----------
        rule
            Relationship rule to compile.
        post_kernels
            Post-kernel specs to apply after execution.

        Returns
        -------
        CompiledRule
            Compiled rule instance.
        """
        inputs = tuple(_source_node(inp) for inp in rule.inputs)
        union_node = RelUnion(inputs=inputs, distinct=False, label=rule.name)
        node = _project_node(union_node, rule.project)
        plan = RelPlan(root=node, ordering=Ordering.unordered(), label=rule.name)
        return CompiledRule(
            rule=rule,
            rel_plan=plan,
            execute_fn=None,
            post_kernels=_compile_post_kernels(post_kernels),
            emit_rule_meta=rule.emit_rule_meta,
        )

    @staticmethod
    def _compile_interval_align(
        rule: RelationshipRule,
        *,
        post_kernels: tuple[KernelSpecT, ...],
    ) -> CompiledRule:
        """Compile an interval-align rule into an execution function.

        Parameters
        ----------
        rule
            Relationship rule to compile.
        post_kernels
            Post-kernel specs to apply after execution.

        Returns
        -------
        CompiledRule
            Compiled rule instance.

        Raises
        ------
        ValueError
            Raised when required interval-align config is missing.
        """
        cfg = rule.interval_align
        if cfg is None:
            msg = "INTERVAL_ALIGN rules require interval_align config."
            raise ValueError(msg)
        interval_cfg = IntervalAlignOptions(
            mode=cfg.mode,
            how=cfg.how,
            left_path_col=cfg.left_path_col,
            left_start_col=cfg.left_start_col,
            left_end_col=cfg.left_end_col,
            right_path_col=cfg.right_path_col,
            right_start_col=cfg.right_start_col,
            right_end_col=cfg.right_end_col,
            select_left=cfg.select_left,
            select_right=cfg.select_right,
            tie_breakers=cfg.tie_breakers,
            emit_match_meta=cfg.emit_match_meta,
            match_kind_col=cfg.match_kind_col,
            match_score_col=cfg.match_score_col,
        )

        def _exec(
            ctx2: ExecutionContext,
            resolver: PlanResolver[IbisPlan],
            plan_executor: PlanExecutor,
        ) -> TableLike:
            """Execute interval alignment using resolved input tables.

            Returns
            -------
            TableLike
                Interval-aligned output table.
            """
            left_ref, right_ref = rule.inputs
            left_plan = resolver.resolve(left_ref, ctx=ctx2)
            right_plan = resolver.resolve(right_ref, ctx=ctx2)
            label = ExecutionLabel(
                rule_name=rule.name,
                output_dataset=rule.output_dataset,
            )
            lt = plan_executor(left_plan, ctx2, None, label)
            rt = plan_executor(right_plan, ctx2, None, label)
            kernel = resolve_kernel("interval_align", ctx=ctx2)
            return kernel(lt, rt, cfg=interval_cfg)

        return CompiledRule(
            rule=rule,
            rel_plan=None,
            execute_fn=_exec,
            post_kernels=_compile_post_kernels(post_kernels),
            emit_rule_meta=rule.emit_rule_meta,
        )

    @staticmethod
    def _compile_winner_select(
        rule: RelationshipRule,
        *,
        post_kernels: tuple[KernelSpecT, ...],
    ) -> CompiledRule:
        """Compile a winner-select rule into an execution function.

        Parameters
        ----------
        rule
            Relationship rule to compile.
        post_kernels
            Post-kernel specs to apply after execution.

        Returns
        -------
        CompiledRule
            Compiled rule instance.

        Raises
        ------
        ValueError
            Raised when required winner-select config is missing.
        """
        cfg = rule.winner_select
        if cfg is None:
            msg = "WINNER_SELECT rules require winner_select config."
            raise ValueError(msg)
        winner_cfg = cfg

        def _exec(
            ctx2: ExecutionContext,
            resolver: PlanResolver[IbisPlan],
            plan_executor: PlanExecutor,
        ) -> TableLike:
            """Execute winner selection using the resolved input table.

            Returns
            -------
            TableLike
                Winner-selected output table.
            """
            src_exec = rule.inputs[0]
            plan_exec = resolver.resolve(src_exec, ctx=ctx2)
            label = ExecutionLabel(
                rule_name=rule.name,
                output_dataset=rule.output_dataset,
            )
            table = plan_executor(plan_exec, ctx2, None, label)
            kernel = resolve_kernel("winner_select", ctx=ctx2)
            return kernel(
                table,
                keys=winner_cfg.keys,
                score_col=winner_cfg.score_col,
                score_order=winner_cfg.score_order,
                tie_breakers=winner_cfg.tie_breakers,
            )

        return CompiledRule(
            rule=rule,
            rel_plan=None,
            execute_fn=_exec,
            post_kernels=_compile_post_kernels(post_kernels),
            emit_rule_meta=rule.emit_rule_meta,
        )

    def compile_rule(self, rule: RelationshipRule, *, ctx: ExecutionContext) -> CompiledRule:
        """Compile a single relationship rule into an executable unit.

        Parameters
        ----------
        rule:
            Rule to compile.
        ctx:
            Execution context.

        Returns
        -------
        CompiledRule
            Compiled representation of the rule.

        Raises
        ------
        ValueError
            Raised when the rule kind is unknown.
        """
        _ = ctx
        handlers: dict[RuleKind, Callable[..., CompiledRule]] = {
            RuleKind.FILTER_PROJECT: self._compile_filter_project,
            RuleKind.HASH_JOIN: self._compile_hash_join,
            RuleKind.EXPLODE_LIST: self._compile_passthrough,
            RuleKind.WINNER_SELECT: self._compile_winner_select,
            RuleKind.UNION_ALL: self._compile_union_all,
            RuleKind.INTERVAL_ALIGN: self._compile_interval_align,
        }
        handler = handlers.get(rule.kind)
        if handler is None:
            msg = f"Unknown rule kind: {rule.kind}."
            raise ValueError(msg)
        compiled = handler(rule, post_kernels=rule.post_kernels)
        self._enforce_execution_mode(rule, compiled)
        return compiled

    @staticmethod
    def _enforce_execution_mode(rule: RelationshipRule, compiled: CompiledRule) -> None:
        """Enforce plan-only execution mode when configured.

        Parameters
        ----------
        rule
            Relationship rule being compiled.
        compiled
            Compiled rule instance to validate.

        Raises
        ------
        ValueError
            Raised when plan-only execution constraints are violated.
        """
        if rule.execution_mode != "plan":
            return
        if compiled.rel_plan is None or compiled.post_kernels:
            msg = f"Rule {rule.name!r} requires plan execution."
            raise ValueError(msg)

    def compile_registry(
        self,
        registry_rules: Sequence[RelationshipRule],
        *,
        ctx: ExecutionContext,
        contract_catalog: ContractCatalog | None = None,
        edge_validation: EdgeContractValidationConfig | None = None,
        policy_registry: PolicyRegistry | None = None,
    ) -> dict[str, CompiledOutput]:
        """Compile relationship rules into executable outputs.

        Parameters
        ----------
        registry_rules:
            Rules to compile.
        ctx:
            Execution context.
        contract_catalog:
            Optional contract catalog for validation and finalization.
        edge_validation:
            Optional edge contract validation config.
        policy_registry:
            Optional policy registry for resolving named policies.

        Returns
        -------
        dict[str, CompiledOutput]
            Compiled outputs keyed by output dataset name.

        """
        rules = list(registry_rules)
        resolved_policy_registry = policy_registry or PolicyRegistry()
        if contract_catalog is not None:
            validate_relationship_output_contracts_for_edge_kinds(
                rules=rules,
                contract_catalog=contract_catalog,
                config=edge_validation or EdgeContractValidationConfig(),
            )

        resolved_rules: list[RelationshipRule] = []
        for rule in rules:
            schema = _schema_for_rule(rule, contracts=contract_catalog)
            resolved_rule = rule
            if schema is not None:
                resolved_rule = apply_policy_defaults(
                    rule,
                    schema,
                    registry=resolved_policy_registry,
                )
                if resolved_rule.evidence is None:
                    inferred = evidence_spec_from_schema(schema)
                    if inferred is not None:
                        resolved_rule = replace(resolved_rule, evidence=inferred)
                validate_policy_requirements(resolved_rule, schema)
            resolved_rules.append(resolved_rule)

        by_out: dict[str, list[RelationshipRule]] = {}
        for rule in resolved_rules:
            by_out.setdefault(rule.output_dataset, []).append(rule)

        compiled: dict[str, CompiledOutput] = {}
        for out_name in sorted(by_out):
            compiled[out_name] = self._compile_output(out_name, by_out[out_name], ctx=ctx)
        return compiled

    def _compile_output(
        self,
        out_name: str,
        rules: Sequence[RelationshipRule],
        *,
        ctx: ExecutionContext,
    ) -> CompiledOutput:
        rules_sorted = sorted(rules, key=lambda rr: (rr.priority, rr.name))
        contract_names = {rr.contract_name for rr in rules_sorted}
        if len(contract_names) > 1:
            msg = (
                f"Output {out_name!r} has inconsistent contract_name across rules: "
                f"{contract_names}."
            )
            raise ValueError(msg)

        contributors = tuple(
            replace(
                compiled_rule,
                required_columns=self._required_columns_for_rule(
                    compiled_rule,
                    ctx=ctx,
                ),
            )
            for compiled_rule in (self.compile_rule(rule, ctx=ctx) for rule in rules_sorted)
        )
        input_datasets = _input_dataset_names(rules_sorted)
        rule_signatures = tuple(_compiled_rule_signature(rule) for rule in contributors)
        plan_hash = output_plan_hash(
            output_dataset=out_name,
            rule_signatures=rule_signatures,
            input_datasets=input_datasets,
        )
        telemetry = self.collect_scan_telemetry(rules_sorted, ctx=ctx)
        runtime_telemetry = runtime_telemetry_for_ctx(ctx)
        return CompiledOutput(
            output_dataset=out_name,
            contract_name=rules_sorted[0].contract_name,
            plan_hash=plan_hash,
            input_datasets=input_datasets,
            contributors=contributors,
            telemetry=telemetry,
            runtime_telemetry=runtime_telemetry,
        )

    def _required_columns_for_rule(
        self,
        compiled: CompiledRule,
        *,
        ctx: ExecutionContext,
    ) -> Mapping[str, tuple[str, ...]]:
        if compiled.rel_plan is None:
            return {}
        backend = self.resolver.backend
        if backend is None or not hasattr(backend, "compiler"):
            return {}
        try:
            plan = (self.plan_compiler or IbisRelPlanCompiler()).compile(
                compiled.rel_plan,
                ctx=ctx,
                resolver=self.resolver,
            )
        except (TypeError, ValueError):
            return {}
        return required_columns_by_table(
            plan.expr,
            backend=cast("IbisCompilerBackend", backend),
        )

    def collect_scan_telemetry(
        self,
        rules: Sequence[RelationshipRule],
        *,
        ctx: ExecutionContext,
    ) -> dict[str, ScanTelemetry]:
        """Collect scan telemetry for rule inputs.

        Returns
        -------
        dict[str, ScanTelemetry]
            Telemetry keyed by dataset label or name.
        """
        telemetry: dict[str, ScanTelemetry] = {}
        for rule in rules:
            for ref in rule.inputs:
                key = ref.label or ref.name
                if key in telemetry:
                    continue
                info = self.resolver.telemetry(ref, ctx=ctx)
                if info is not None:
                    telemetry[key] = info
        return telemetry


def _finalize_output_tables(
    *,
    output_dataset: str,
    contract_name: str | None,
    table_parts: Sequence[TableLike],
    ctx: ExecutionContext,
    contracts: ContractCatalog,
) -> FinalizeResult:
    """Finalize output tables against the resolved contract.

    Parameters
    ----------
    output_dataset
        Output dataset name.
    contract_name
        Contract name to finalize against.
    table_parts
        Table parts to merge and finalize.
    ctx
        Execution context for finalization.
    contracts
        Contract catalog to resolve contract definitions.

    Returns
    -------
    FinalizeResult
        Finalized output tables and artifacts.
    """
    if contract_name is None:
        unioned = SchemaEvolutionSpec().unify_and_cast(
            table_parts,
            safe_cast=ctx.safe_cast,
            on_error="unsafe" if ctx.safe_cast else "raise",
            keep_extra_columns=ctx.provenance,
        )
        dummy = Contract(name=f"{output_dataset}_NO_CONTRACT", schema=unioned.schema)
        finalize_ctx = dataset_spec_from_contract(dummy).finalize_context(ctx)
        return finalize_ctx.run(unioned, ctx=ctx)

    contract = contracts.get(contract_name)
    dataset_spec = GLOBAL_SCHEMA_REGISTRY.dataset_specs.get(contract.name)
    if dataset_spec is None:
        dataset_spec = dataset_spec_from_contract(contract)
    unioned = dataset_spec.unify_tables(table_parts, ctx=ctx)
    return dataset_spec.finalize_context(ctx).run(unioned, ctx=ctx)


__all__ = [
    "CompiledOutput",
    "CompiledOutputExecutionOptions",
    "CompiledRule",
    "FilesystemPlanResolver",
    "InMemoryPlanResolver",
    "PlanResolver",
    "RelationshipRuleCompiler",
    "RuleExecutionOptions",
    "apply_policy_defaults",
    "validate_policy_requirements",
]
