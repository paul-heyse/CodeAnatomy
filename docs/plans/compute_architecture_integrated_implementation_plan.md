# Compute Architecture Integrated Implementation Plan

## Purpose
This plan merges `docs/plans/compute_architecture_improvement.md` and
`docs/plans/library_features_to_enhance_architecture_improvement.md` into a single
implementation roadmap. It incorporates the approved recommendations and the advanced
stack guidance in `docs/plans/compute_stack_advanced.md`, and it breaks the work into
granular scope items with code patterns, target files, and checklists.

## Objectives
- Make rule execution the single compile and execute surface (relspec-first).
- Default to Ibis and DataFusion execution with streaming materialization.
- Enforce determinism tiers as explicit policy, not incidental behavior.
- Remove Python loops from core primitives and replace with native kernels or UDFs.
- Provide deterministic plan identity, schema contracts, and incremental invalidation.
- Emit rich diagnostics tables and reproducibility artifacts for every run.

## Non-goals
- No short-term promise to preserve every legacy API unchanged.
- No optimization for non-DataFusion backends beyond basic fallbacks.

## System invariants
- Streaming-first: the default output surface is a `RecordBatchReader` or batch iterator.
  Materialize only for canonical ordering, incompatible downstream APIs, or debugging.
- Determinism tiers are a policy contract: Tier 0 (fast), Tier 1 (stable), Tier 2 (canonical).
- Clear ownership boundaries:
  - DataFusion owns execution, optimization, and optional engine-native writers.
  - Ibis owns relational IR, backend compilation, and ergonomics.
  - SQLGlot owns AST policy, qualification, lineage, and semantic diffs.
  - PyArrow owns physical batches, kernels, dataset scanning, and dataset writing.

## Runtime profile presets
- `dev_debug`: deep explain metrics, lower parallelism, `fuse_selects=False`.
- `prod_fast`: streaming-first, higher partitions, spill enabled, `fuse_selects=True`.
- `memory_tight`: smaller batch sizes, lower partitions, spill enforced.

## Failure-mode playbooks
- Dynamic filter pushdown issues: feature-gate it off per run and record the gate state.
- OOM during sorts/joins: lower batch size and partitions, require spill runtime.
- Deterministic output: Tier 1 ordering (implicit ordering) or Tier 2 canonical sort.

## Phases at a glance
- Phase 0: Execution substrate, invariants, profiles, and feature gates
- Phase 1: Streaming materialization and writer backends
- Phase 2: Ibis-first CPG nodes, props, edges
- Phase 3: Primitive acceleration and UDF ladder
- Phase 4: Compiler analysis and plan identity
- Phase 5: Rule unification and product entrypoints
- Phase 6: Caching and replay

## Scope items

### Phase 0 - Execution substrate and policy

#### Scope 0.0: System invariants and determinism tiers
Goal: encode streaming-first defaults and determinism tiers as explicit, testable policy.

Representative code patterns:
```python
from dataclasses import dataclass
from enum import Enum


class DeterminismTier(str, Enum):
    FAST = "fast"
    STABLE = "stable"
    CANONICAL = "canonical"


@dataclass(frozen=True)
class ExecutionSurfacePolicy:
    prefer_streaming: bool = True
    determinism_tier: DeterminismTier = DeterminismTier.STABLE
```

Target files:
- (new) `src/engine/plan_policy.py`
- (update) `src/arrowdsl/core/context.py`
- (update) `src/arrowdsl/schema/metadata.py`
- (update) `src/arrowdsl/io/parquet.py`

Implementation checklist:
- [ ] Define determinism tiers and make them a first-class policy input.
- [ ] Default all materialization paths to streaming unless policy requires a table.
- [ ] Attach determinism tier to ordering metadata and write options.
- [ ] Add tests that validate Tier 1 vs Tier 2 ordering behavior.

#### Scope 0.1: EngineSession substrate
Goal: unify execution context, runtime profile, DataFusion runtime, Ibis backend,
dataset registry, and diagnostics into a single session object.

Representative code patterns:
```python
from dataclasses import dataclass

from datafusion import SessionContext
from ibis.backends import BaseBackend

from arrowdsl.core.context import ExecutionContext
from datafusion_engine.runtime import DataFusionRuntimeProfile
from engine.plan_policy import ExecutionSurfacePolicy
from engine.runtime_profile import RuntimeProfile
from ibis_engine.registry import IbisDatasetRegistry
from obs.diagnostics import DiagnosticsCollector


@dataclass(frozen=True)
class EngineSession:
    ctx: ExecutionContext
    runtime_profile: RuntimeProfile
    df_profile: DataFusionRuntimeProfile | None
    ibis_backend: BaseBackend
    datasets: IbisDatasetRegistry
    diagnostics: DiagnosticsCollector | None = None
    surface_policy: ExecutionSurfacePolicy = ExecutionSurfacePolicy()
    settings_hash: str | None = None

    def df_ctx(self) -> SessionContext | None:
        return None if self.df_profile is None else self.df_profile.session_context()
```

Target files:
- (new) `src/engine/session.py`
- (new) `src/engine/session_factory.py`
- (new) `src/engine/runtime_profile.py`
- (update) `src/hamilton_pipeline/modules/cpg_build.py`
- (update) `src/hamilton_pipeline/modules/params.py`
- (update) `src/ibis_engine/backend.py`

Implementation checklist:
- [ ] Define `EngineSession` with runtime profile and policy fields.
- [ ] Replace ad-hoc backend or registry creation with session usage.
- [ ] Thread the session through relspec and CPG entrypoints.
- [ ] Add a smoke test that constructs a session and runs a trivial plan.

#### Scope 0.2: DataFusion settings contract and feature gates
Goal: treat runtime settings as a contract, apply them consistently, and record
feature gate state in manifests.

Representative code patterns:
```python
from dataclasses import dataclass
from collections.abc import Mapping

import pyarrow as pa
from datafusion import SessionConfig


@dataclass(frozen=True)
class DataFusionFeatureGates:
    enable_dynamic_filter_pushdown: bool = True
    enable_join_dynamic_filter_pushdown: bool = True
    enable_aggregate_dynamic_filter_pushdown: bool = True
    enable_topk_dynamic_filter_pushdown: bool = True

    def to_settings(self) -> dict[str, str]:
        return {
            "datafusion.optimizer.enable_dynamic_filter_pushdown": str(
                self.enable_dynamic_filter_pushdown
            ).lower(),
            "datafusion.optimizer.enable_join_dynamic_filter_pushdown": str(
                self.enable_join_dynamic_filter_pushdown
            ).lower(),
            "datafusion.optimizer.enable_aggregate_dynamic_filter_pushdown": str(
                self.enable_aggregate_dynamic_filter_pushdown
            ).lower(),
            "datafusion.optimizer.enable_topk_dynamic_filter_pushdown": str(
                self.enable_topk_dynamic_filter_pushdown
            ).lower(),
        }


@dataclass(frozen=True)
class DataFusionSettingsContract:
    settings: Mapping[str, str]
    feature_gates: DataFusionFeatureGates

    def apply(self, config: SessionConfig) -> SessionConfig:
        for key, value in {**self.settings, **self.feature_gates.to_settings()}.items():
            config = config.set(key, value)
        return config


def apply_concurrency(cpu_threads: int | None, io_threads: int | None) -> None:
    if cpu_threads is not None:
        pa.set_cpu_count(cpu_threads)
    if io_threads is not None:
        pa.set_io_thread_count(io_threads)
```

Target files:
- (update) `src/datafusion_engine/runtime.py`
- (update) `src/arrowdsl/core/context.py`
- (update) `src/obs/manifest.py`
- (update) `src/engine/runtime_profile.py`

Implementation checklist:
- [ ] Extend `DataFusionConfigPolicy` to expose a stable settings snapshot and hash.
- [ ] Apply settings and feature gates in `DataFusionRuntimeProfile.session_config`.
- [ ] Set Arrow thread pools from the runtime profile.
- [ ] Record settings, hashes, and gate state in `obs/manifest.py`.

#### Scope 0.3: Diagnostics sink baseline
Goal: make diagnostics table capture a first-class service and route all rule and
runtime events through it.

Representative code patterns:
```python
from collections.abc import Mapping, Sequence
from dataclasses import dataclass


@dataclass
class DiagnosticsCollector:
    def record_events(self, name: str, rows: Sequence[Mapping[str, object]]) -> None:
        ...

    def record_artifact(self, name: str, payload: Mapping[str, object]) -> None:
        ...
```

Target files:
- (new) `src/obs/diagnostics.py`
- (update) `src/obs/diagnostics_tables.py`
- (update) `src/datafusion_engine/runtime.py`
- (update) `src/relspec/rules/exec_events.py`

Implementation checklist:
- [ ] Define a minimal collector interface with table and artifact hooks.
- [ ] Hook DataFusion explain and fallback collectors into the sink.
- [ ] Route rule execution events through the same sink.
- [ ] Persist diagnostics tables into the run bundle.

#### Scope 0.4: Runtime profile presets and builder
Goal: standardize runtime profiles and provide a builder that configures DataFusion,
Arrow thread pools, and spill runtime consistently.

Representative code patterns:
```python
from dataclasses import dataclass
from collections.abc import Mapping

import pyarrow as pa
from datafusion import RuntimeEnvBuilder, SessionConfig, SessionContext

from engine.plan_policy import DeterminismTier


@dataclass(frozen=True)
class RuntimeProfile:
    name: str
    target_partitions: int
    arrow_cpu_threads: int
    arrow_io_threads: int
    spill_pool_bytes: int
    datafusion_settings: Mapping[str, str]
    determinism_tier: DeterminismTier
    ibis_fuse_selects: bool


def build_datafusion_ctx(profile: RuntimeProfile) -> SessionContext:
    pa.set_cpu_count(profile.arrow_cpu_threads)
    pa.set_io_thread_count(profile.arrow_io_threads)
    runtime = (
        RuntimeEnvBuilder()
        .with_disk_manager_os()
        .with_fair_spill_pool(profile.spill_pool_bytes)
    )
    config = SessionConfig().with_target_partitions(profile.target_partitions)
    for key, value in profile.datafusion_settings.items():
        config = config.set(key, value)
    return SessionContext(config, runtime)
```

Target files:
- (new) `src/engine/runtime_profile.py`
- (update) `src/datafusion_engine/runtime.py`
- (update) `src/ibis_engine/backend.py`
- (update) `src/obs/manifest.py`

Implementation checklist:
- [ ] Define `dev_debug`, `prod_fast`, and `memory_tight` presets.
- [ ] Ensure spill runtime is configured for non-debug profiles.
- [ ] Record profile name and settings hash in manifests and run bundles.
- [ ] Expose profile selection through the graph product entrypoint.

#### Scope 0.5: Failure-mode playbooks and feature state capture
Goal: provide explicit, tested fallback paths and record feature gate state in
diagnostics for postmortems.

Representative code patterns:
```python
from dataclasses import dataclass

from engine.plan_policy import DeterminismTier


@dataclass(frozen=True)
class FeatureStateSnapshot:
    profile_name: str
    determinism_tier: DeterminismTier
    dynamic_filters_enabled: bool
    spill_enabled: bool


def disable_join_dynamic_filters(config: SessionConfig) -> SessionConfig:
    return config.set("datafusion.optimizer.enable_join_dynamic_filter_pushdown", "false")
```

Target files:
- (update) `src/datafusion_engine/runtime.py`
- (update) `src/obs/diagnostics_tables.py`
- (update) `src/obs/manifest.py`
- (update) `src/engine/runtime_profile.py`

Implementation checklist:
- [ ] Implement explicit toggles for dynamic filter pushdown features.
- [ ] Record feature state snapshots in diagnostics tables.
- [ ] Add run-time overrides to force Tier 2 ordering when requested.
- [ ] Add regression tests that exercise the fallback paths.

### Phase 1 - Streaming materialization and writer backends

#### Scope 1.1: PlanProduct with streaming surfaces
Goal: standardize plan outputs as stream or table with explicit schema, plan id,
and determinism metadata.

Representative code patterns:
```python
from dataclasses import dataclass
from typing import Literal

import pyarrow as pa

from arrowdsl.core.interop import RecordBatchReaderLike, TableLike
from engine.plan_policy import DeterminismTier

WriterStrategy = Literal["arrow", "datafusion"]


@dataclass(frozen=True)
class PlanProduct:
    plan_id: str
    schema: pa.Schema
    determinism_tier: DeterminismTier
    writer_strategy: WriterStrategy
    stream: RecordBatchReaderLike | None = None
    table: TableLike | None = None
```

Target files:
- (new) `src/engine/plan_product.py`
- (update) `src/ibis_engine/plan.py`
- (update) `src/arrowdsl/plan/runner.py`
- (update) `src/relspec/compiler.py`

Implementation checklist:
- [ ] Introduce `PlanProduct` and wire it into compiler outputs.
- [ ] Add a path that returns `IbisPlan.to_reader` when streaming is requested.
- [ ] Preserve ordering metadata on `RecordBatchReader` and tables.
- [ ] Update materializers to accept `PlanProduct`.

#### Scope 1.2: Streaming surface policy (DataFusion + Ibis)
Goal: make streaming the default execution surface for both DataFusion and Ibis,
with explicit guardrails on unsupported projection semantics.

Representative code patterns:
```python
import pyarrow as pa

reader = pa.RecordBatchReader.from_stream(df)
reader = ibis_plan.expr.to_pyarrow_batches(chunk_size=1_000_000)
```

Target files:
- (update) `src/datafusion_engine/bridge.py`
- (update) `src/ibis_engine/plan.py`
- (update) `src/ibis_engine/io_bridge.py`
- (update) `src/arrowdsl/io/parquet.py`

Implementation checklist:
- [ ] Default DataFusion materialization to `RecordBatchReader.from_stream`.
- [ ] Default Ibis materialization to `to_pyarrow_batches`.
- [ ] Document and enforce the `requested_schema` projection-only restriction.
- [ ] Add tests that stream from both DataFusion and Ibis into dataset writes.

#### Scope 1.3: Writer backend selection and determinism integration
Goal: select DataFusion or PyArrow writers per dataset, and align writer options
with determinism tiers.

Representative code patterns:
```python
import pyarrow.dataset as ds
from datafusion import DataFrameWriteOptions, ParquetWriterOptions

write_options = DataFrameWriteOptions(
    partition_by=["repo_id"],
    sort_by=["stable_id"],
    single_file_output=False,
)
parquet_options = ParquetWriterOptions(compression="zstd")
df.write_parquet_with_options(str(output_dir), parquet_options, write_options)

written = []

def visitor(written_file: object) -> None:
    written.append(written_file)

ds.write_dataset(
    reader,
    base_dir=str(output_dir),
    format="parquet",
    preserve_order=determinism_tier != DeterminismTier.FAST,
    file_visitor=visitor,
)
```

Target files:
- (update) `src/arrowdsl/io/parquet.py`
- (update) `src/ibis_engine/io_bridge.py`
- (update) `src/storage/io.py`
- (update) `src/obs/manifest.py`

Implementation checklist:
- [ ] Add a writer strategy switch (DataFusion native vs PyArrow dataset writer).
- [ ] Use `sort_by` for Tier 2 determinism in DataFusion writers.
- [ ] Capture `file_visitor` metadata for PyArrow writes.
- [ ] Record writer strategy and options in the run manifest.

#### Scope 1.4: Ordering policy and Acero scan knobs
Goal: avoid global sorts when implicit ordering is sufficient and encode ordering in scan
policy.

Representative code patterns:
```python
from pyarrow import acero as ac
from pyarrow import dataset as ds

scan = ac.ScanNodeOptions(
    dataset=ds.dataset(str(base_dir), format="parquet"),
    require_sequenced_output=True,
    implicit_ordering=True,
)
```

Target files:
- (update) `src/arrowdsl/core/context.py`
- (update) `src/arrowdsl/compile/plan_compiler.py`
- (update) `src/arrowdsl/schema/metadata.py`

Implementation checklist:
- [ ] Promote `implicit_ordering` into scan policy defaults for Tier 1.
- [ ] Propagate ordering metadata through plan execution and IO.
- [ ] Add a determinism tier decision that chooses sort vs implicit ordering.
- [ ] Validate ordering in a small end-to-end scan pipeline test.

#### Scope 1.5: Partitioned streaming and parallel writers
Goal: support partitioned streaming outputs for large outputs while preserving
Tier 0 semantics.

Representative code patterns:
```python
for partition_reader in df.execute_stream_partitioned():
    ds.write_dataset(partition_reader, base_dir=str(output_dir), format="parquet")
```

Target files:
- (update) `src/datafusion_engine/bridge.py`
- (update) `src/arrowdsl/io/parquet.py`
- (update) `src/ibis_engine/io_bridge.py`

Implementation checklist:
- [ ] Add an optional partitioned streaming path for large outputs.
- [ ] Restrict partitioned streaming to Tier 0 determinism by default.
- [ ] Add row-count and schema validation after parallel writes.

### Phase 2 - Ibis-first CPG nodes, props, edges

#### Scope 2.1: Ibis nodes emission and symbol collection
Goal: move node emission into Ibis and replace Python set logic with plan-native
distinct and union operations.

Representative code patterns:
```python
symbols = (
    scip_symbol_information.select("symbol")
    .union(scip_occurrences.select("symbol"))
    .distinct()
)


def emit_nodes_ibis(rel: IbisPlan | Table, *, spec: NodeEmitSpec) -> IbisPlan:
    t = rel.expr if isinstance(rel, IbisPlan) else rel
    node_id = coalesce_columns(t, spec.id_cols, default=ibis_null_literal(pa.string()))
    return IbisPlan(
        expr=t.select(node_id=node_id, node_kind=ibis.literal(spec.node_kind.value)),
        ordering=Ordering.unordered(),
    )
```

Target files:
- (new) `src/relspec/cpg/emit_nodes_ibis.py`
- (update) `src/cpg/build_nodes.py`
- (update) `src/cpg/specs.py`

Implementation checklist:
- [ ] Implement `emit_nodes_ibis` with schema alignment.
- [ ] Replace `_collect_symbols()` Python set path with Ibis `distinct`.
- [ ] Make Ibis nodes the default when DataFusion is enabled.
- [ ] Keep a fallback to legacy plan-lane nodes for no-DF mode.

#### Scope 2.2: Ibis props fast lane and heavy JSON lane
Goal: keep most props in the fast lane and isolate JSON-heavy props as an optional dataset.

Representative code patterns:
```python
def emit_props_fast(rel: IbisPlan, *, spec: PropEmitSpec) -> IbisPlan:
    t = rel.expr
    return IbisPlan(
        expr=t.select(
            prop_name=ibis.literal(spec.name),
            value_str=coalesce_columns(t, spec.value_cols),
            value_json=ibis.literal(None).cast("string"),
        )
    )


def emit_props_json(rel: IbisPlan, *, spec: PropEmitSpec) -> IbisPlan:
    t = rel.expr
    value_json = ibis.udf.scalar.builtin("to_json", input_type=["json"], output_type="string")(
        t[spec.value_col]
    )
    return IbisPlan(expr=t.select(prop_name=ibis.literal(spec.name), value_json=value_json))
```

Target files:
- (new) `src/relspec/cpg/emit_props_ibis.py`
- (update) `src/cpg/build_props.py`
- (update) `src/cpg/specs.py`
- (update) `src/hamilton_pipeline/modules/outputs.py`

Implementation checklist:
- [ ] Add fast lane props emission with no JSON serialization.
- [ ] Add optional JSON props dataset and make it opt-in.
- [ ] Update output assembly to union or view fast plus JSON on demand.
- [ ] Ensure the manifest reports the optional JSON dataset separately.

#### Scope 2.3: Ibis edges as the default execution lane
Goal: route edges through Ibis and only use plan-lane edges as a fallback.

Representative code patterns:
```python
if ctx.runtime.datafusion is not None:
    rel_plans = compile_relation_plans_ibis(...)
    return emit_edges_ibis(rel_plans, ...)
return emit_edges_plan_lane(...)
```

Target files:
- (update) `src/cpg/build_edges.py`
- (update) `src/cpg/emit_edges_ibis.py`
- (update) `src/relspec/compiler.py`

Implementation checklist:
- [ ] Make Ibis edges the default when a DF session is configured.
- [ ] Remove materialize-at-compile behavior from the default path.
- [ ] Keep plan-lane edges only for no-DF mode and debugging.

#### Scope 2.4: Ibis compilation policy and SQLGlot AST execution
Goal: make SQLGlot AST the policy boundary and execute ASTs directly when possible.

Representative code patterns:
```python
import ibis
import sqlglot.expressions as sge

with ibis.options.sql.fuse_selects.set(profile.ibis_fuse_selects):
    sqlglot_expr = sge.select("node_id").from_("cpg_nodes")
    table = backend.raw_sql(sqlglot_expr)
```

Target files:
- (update) `src/ibis_engine/backend.py`
- (update) `src/ibis_engine/compiler_checkpoint.py`
- (update) `src/relspec/compiler.py`
- (update) `src/engine/runtime_profile.py`

Implementation checklist:
- [ ] Tie `ibis.options.sql.fuse_selects` to runtime profiles.
- [ ] Add a SQLGlot AST execution path via `raw_sql`.
- [ ] Use AST execution for policy-controlled compilation outputs.
- [ ] Add a fallback path for AST incompatibilities.

### Phase 3 - Primitive acceleration and UDF ladder

#### Scope 3.1: UDF performance ladder (builtin to pyarrow to python)
Goal: standardize UDF fallbacks so vectorized Arrow UDFs replace Python loops.

Representative code patterns:
```python
import ibis
import pyarrow as pa
import pyarrow.compute as pc

stable_hash64 = ibis.udf.scalar.builtin(
    name="stable_hash64",
    input_type=["string"],
    output_type="int64",
)


@ibis.udf.scalar.pyarrow
def stable_hash64_fallback(
    values: pa.Array | pa.ChunkedArray | pa.Scalar,
) -> pa.Array | pa.Scalar:
    return pc.hash(values).cast(pa.int64())


@ibis.udf.scalar.python
def stable_hash64_last_resort(value: str | None) -> int | None:
    if value is None:
        return None
    return int.from_bytes(value.encode("utf-8"), byteorder="little", signed=False) & 0xFFFFFFFF
```

Target files:
- (update) `src/ibis_engine/builtin_udfs.py`
- (update) `src/datafusion_engine/udf_registry.py`
- (update) `src/arrowdsl/core/ids.py`

Implementation checklist:
- [ ] Promote builtin UDFs as the default path for core primitives.
- [ ] Implement pyarrow vectorized fallbacks for stable hash and col_to_byte.
- [ ] Keep scalar Python fallbacks as last resort only.
- [ ] Add conformance tests that compare builtin and fallback outputs.

#### Scope 3.2: DataFusion FunctionFactory extension (Rust-backed)
Goal: implement core primitives in a native extension and register them with named
argument support.

Representative code patterns:
```python
from datafusion_engine.function_factory import (
    default_function_factory_policy,
    install_function_factory,
)

policy = default_function_factory_policy()
install_function_factory(session.df_ctx(), policy=policy)
```
```rust
#[datafusion_ext::scalar_udf(name = "stable_hash64", volatility = "stable")]
fn stable_hash64(args: &[ArrayRef]) -> Result<ArrayRef> {
    // Rust implementation, no Python loops.
}
```

Target files:
- (update) `src/datafusion_engine/function_factory.py`
- (new) `rust/datafusion_ext/src/lib.rs`
- (update) `rust/datafusion_ext/Cargo.toml`

Implementation checklist:
- [ ] Implement stable_hash64, stable_hash128, col_to_byte, position_encoding_norm.
- [ ] Expose parameter names for named arguments (DataFusion 51).
- [ ] Register primitives via FunctionFactory policy JSON.
- [ ] Replace default Python UDF registration with the native extension.

#### Scope 3.3: Arrow kernel fallback for list explode
Goal: provide a non-Python fallback for explode operations using Arrow kernels.

Representative code patterns:
```python
import pyarrow.compute as pc

parent_idx = pc.list_parent_indices(list_col)
values = pc.list_flatten(list_col)
parents = pc.take(parent_ids, parent_idx)
```

Target files:
- (update) `src/arrowdsl/compute/filters.py`
- (update) `src/arrowdsl/compute/explode.py`
- (update) `src/cpg/build_edges.py`

Implementation checklist:
- [ ] Add a kernel-only explode helper using list_parent_indices and list_flatten.
- [ ] Replace Python loops in explode-related code paths.
- [ ] Add tests that validate ordering and parent alignment.

### Phase 4 - Compiler analysis and plan identity

#### Scope 4.1: SQLGlot checkpoint, policy pipeline, and plan hash
Goal: make SQLGlot the analysis IR, apply policy transforms, and generate stable plan
identity hashes.

Representative code patterns:
```python
import hashlib

from sqlglot.optimizer.normalize import normalize, normalization_distance
from sqlglot.optimizer.normalize_identifiers import normalize_identifiers
from sqlglot.optimizer.qualify import qualify

sqlglot_expr = backend.compiler.to_sqlglot(ibis_expr)
qualified = qualify(sqlglot_expr, schema=schema_map, dialect="datafusion")
normalized = normalize_identifiers(qualified, dialect="datafusion")
if normalization_distance(normalized, max_=1000) <= 1000:
    normalized = normalize(normalized)
plan_hash = hashlib.sha256(
    normalized.sql(dialect="datafusion").encode("utf-8")
).hexdigest()
```

Target files:
- (new) `src/ibis_engine/compiler_checkpoint.py`
- (update) `src/relspec/compiler.py`
- (update) `src/obs/manifest.py`

Implementation checklist:
- [ ] Add a compiler checkpoint that returns SQLGlot AST and plan hash.
- [ ] Require qualification and identifier normalization in the pipeline.
- [ ] Gate CNF/DNF normalization with `normalization_distance`.
- [ ] Record plan hash in rule diagnostics and run manifests.

#### Scope 4.2: Column lineage extraction and projection pushdown
Goal: use SQLGlot lineage to minimize input columns and improve performance.

Representative code patterns:
```python
from sqlglot.lineage import lineage

graph = lineage("node_id", normalized, schema=schema_map, dialect="datafusion")
required_columns = sorted(graph.sources)
```

Target files:
- (new) `src/ibis_engine/lineage.py`
- (update) `src/relspec/compiler.py`
- (update) `src/arrowdsl/plan/query.py`

Implementation checklist:
- [ ] Extract lineage graphs per rule output.
- [ ] Push required columns into scan or select nodes.
- [ ] Persist required column sets in diagnostics tables.

#### Scope 4.3: Semantic diff for incremental invalidation
Goal: use semantic SQL diffs to decide whether to invalidate caches.

Representative code patterns:
```python
from sqlglot.diff import diff

changes = diff(old_expr, new_expr)
meaningful = [change for change in changes if change.meta.get("semantic", True)]
invalidate = bool(meaningful)
```

Target files:
- (new) `src/ibis_engine/plan_diff.py`
- (update) `src/incremental/impact_update.py`
- (update) `src/relspec/engine.py`

Implementation checklist:
- [ ] Compute semantic diffs for rule plan revisions.
- [ ] Tie invalidation to semantic changes instead of source edits alone.
- [ ] Emit diff artifacts into run bundles.

#### Scope 4.4: Schema contract validation via Ibis Schema and SQLGlot
Goal: validate compiled outputs against explicit schema contracts.

Representative code patterns:
```python
import ibis

expected = ibis.schema({"node_id": "string", "node_kind": "string"})
actual = ibis_expr.schema()
if actual != expected:
    raise ValueError("Schema mismatch for node output")

column_defs = expected.to_sqlglot_column_defs(dialect="datafusion")
```

Target files:
- (update) `src/schema_spec/*`
- (update) `src/ibis_engine/schema_utils.py`
- (update) `src/relspec/compiler.py`

Implementation checklist:
- [ ] Generate expected schema from contracts.
- [ ] Validate compiled schema before execution.
- [ ] Persist expected schema AST in repro bundles.

### Phase 5 - Rule unification and product entrypoints

#### Scope 5.1: Relspec CPG rule handlers and wrappers
Goal: make relspec the canonical engine for nodes, edges, and props.

Representative code patterns:
```python
class NodeEmitRuleHandler(RuleHandler):
    def compile(self, spec: NodeEmitSpec, *, session: EngineSession) -> IbisPlan:
        return emit_nodes_ibis(spec.source, spec=spec)


def build_cpg_nodes(**kwargs):
    warnings.warn(
        "cpg.build_nodes.build_cpg_nodes is deprecated; use relspec.cpg.build_nodes",
        DeprecationWarning,
        stacklevel=2,
    )
    return relspec.cpg.build_nodes.build_cpg_nodes(**kwargs)
```

Target files:
- (new) `src/relspec/cpg/build_nodes.py`
- (new) `src/relspec/cpg/build_edges.py`
- (new) `src/relspec/cpg/build_props.py`
- (update) `src/cpg/build_nodes.py`
- (update) `src/cpg/build_edges.py`
- (update) `src/cpg/build_props.py`

Implementation checklist:
- [ ] Add relspec rule handlers for nodes, props, and edges.
- [ ] Convert `cpg/build_*` into compatibility wrappers with warnings.
- [ ] Update Hamilton modules to import from `relspec.cpg`.

#### Scope 5.2: Graph product build entrypoint
Goal: provide a single entrypoint for building graph products with typed outputs,
runtime profile selection, and determinism tier controls.

Representative code patterns:
```python
from engine.plan_policy import DeterminismTier
from graph import GraphProductBuildRequest, build_graph_product

result = build_graph_product(
    GraphProductBuildRequest(
        repo_root=repo_root,
        output_dir=output_dir,
        runtime_profile="prod_fast",
        determinism_tier=DeterminismTier.CANONICAL,
        writer_strategy="datafusion",
    )
)
```

Target files:
- (new) `src/graph/product_build.py`
- (new) `src/graph/__init__.py`
- (update) `scripts/run_full_pipeline.py`

Implementation checklist:
- [ ] Add a typed request and response API for graph product builds.
- [ ] Accept runtime profile and determinism tier in the request.
- [ ] Choose pipeline outputs automatically based on flags.
- [ ] Update CLI entrypoints to call the new API.

#### Scope 5.3: Observability tables and metrics capture
Goal: capture plan metrics, fallbacks, feature state, and rule execution artifacts
as tables.

Representative code patterns:
```python
explain = df_profile.explain_collector.snapshot()
fallbacks = df_profile.fallback_collector.snapshot()

tables = {
    "datafusion_explains_v1": datafusion_explains_table(explain),
    "datafusion_fallbacks_v1": datafusion_fallbacks_table(fallbacks),
}
```

Target files:
- (update) `src/obs/diagnostics_tables.py`
- (update) `src/obs/diagnostics_schemas.py`
- (update) `src/obs/repro.py`
- (update) `src/datafusion_engine/runtime.py`

Implementation checklist:
- [ ] Capture EXPLAIN ANALYZE metrics at a configurable verbosity level.
- [ ] Persist fallback events and explain summaries for every ruleset run.
- [ ] Add a stable diagnostics schema version and record it in the manifest.
- [ ] Store feature state snapshots alongside diagnostics outputs.

### Phase 6 - Caching and replay

#### Scope 6.1: Substrait caching and replay
Goal: cache compiled plans and replay them without re-compiling, keyed by
plan hash and runtime profile hash.

Representative code patterns:
```python
from datafusion_engine.bridge import replay_substrait_bytes

df = replay_substrait_bytes(session.df_ctx(), plan_bytes)
reader = pa.RecordBatchReader.from_stream(df)
```

Target files:
- (update) `src/datafusion_engine/bridge.py`
- (update) `src/obs/repro.py`
- (new) `src/engine/plan_cache.py`

Implementation checklist:
- [ ] Store Substrait bytes alongside plan hashes and profile hashes.
- [ ] Add a replay path that bypasses Ibis or SQL compilation.
- [ ] Fall back to normal compilation when Substrait is unavailable.

#### Scope 6.2: Dataset fingerprints and plan identity in manifests
Goal: tie output datasets to plan hashes, runtime profile hashes, writer strategy,
and input fingerprints.

Representative code patterns:
```python
import hashlib

fingerprint = hashlib.sha256(
    (plan_hash + schema_fingerprint + settings_hash + writer_strategy).encode("utf-8")
).hexdigest()
```

Target files:
- (update) `src/obs/manifest.py`
- (update) `src/arrowdsl/schema/serialization.py`
- (update) `src/relspec/engine.py`

Implementation checklist:
- [ ] Extend manifest records to include plan hash, profile hash, and writer strategy.
- [ ] Use fingerprints to skip materialization when unchanged.
- [ ] Emit fingerprint changes into incremental impact reports.
