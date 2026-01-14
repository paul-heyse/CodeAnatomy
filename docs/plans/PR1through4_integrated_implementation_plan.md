Below is a **comprehensive integrated implementation plan** that merges the
original PR-01..PR-04 scope with the additional scope items we agreed on. It
follows the same **PR-by-PR structure** as existing plans and adds:

* explicit **scope items** per PR,
* **code patterns** per scope item,
* **target file lists** per scope item,
* and **implementation checklists** per scope item.

This plan is intentionally **incremental and non-architectural**: it deepens
use of DataFusion, SQLGlot, Ibis, PyArrow, and Acero without changing the
target architecture defined in `docs/plans/Target_Architecture.md`.

---

# PR Stack Overview (integrated)

**PR-01 — DataFusion Runtime & Session Contract (expanded)**

> Centralize a single SessionContext factory with explicit catalog/schema,
> info schema toggle, and a policy pack of configuration keys (cache, stats,
> planning). Align kernel lane sessions with the same runtime contract.

**PR-02 — Dataset Registry → DataFusion Table Providers (scan + dataset I/O)**

> Make registry scan options fully wired (including file sort order) and
> add a provider-grade path for `pyarrow.dataset.Dataset` registration with
> optional `_metadata` sidecars and schema hints.

**PR-03 — Relspec Compile/Diagnostics Hardening (canonical + strict + Substrait)**

> Add canonical plan signatures, strict SQLGlot diagnostics, semantic diffs,
> optional Substrait artifacts, and contract validation at compile-time.

**PR-04 — Hybrid Kernel Bridge (capability-driven execution lanes)**

> Normalize kernel selection across DataFusion built-ins, UDFs, and Arrow
> fallbacks, with lane diagnostics and explicit ordering/volatility semantics.

---

# PR-01: DataFusion Runtime & Session Contract (expanded)

### Description (what changes)

This PR formalizes a **single, reproducible DataFusion runtime profile** and
ensures **all execution lanes** (DataFusion, Ibis-compiled plans, and kernel
helpers) **share the same session contract**. It adds a **policy pack** of
configuration keys that materially change plans and cache behavior, and it
establishes the **diagnostics payload** required for plan reproducibility.

---

## Scope item 1.1 — Runtime profile: catalog/schema + info schema

**Code pattern**

```python
@dataclass(frozen=True)
class DataFusionRuntimeProfile:
    default_catalog: str = "codeintel"
    default_schema: str = "public"
    enable_information_schema: bool = True

    def session_config(self) -> SessionConfig:
        config = SessionConfig()
        config = config.with_default_catalog_and_schema(
            self.default_catalog,
            self.default_schema,
        )
        config = config.with_create_default_catalog_and_schema(True)
        config = config.with_information_schema(self.enable_information_schema)
        return config
```

**Target files**

- `src/datafusion_engine/runtime.py`
- `src/arrowdsl/core/context.py`

**Implementation checklist**

- [ ] Add `default_catalog`, `default_schema`, `enable_information_schema`.
- [ ] Thread catalog/schema defaults into `SessionConfig`.
- [ ] Ensure every SessionContext is created via the profile factory.

---

## Scope item 1.2 — Policy pack: cache, stats, and planning keys

**Code pattern**

```python
@dataclass(frozen=True)
class DataFusionConfigPolicy:
    settings: Mapping[str, str]

    def apply(self, config: SessionConfig) -> SessionConfig:
        for key, value in self.settings.items():
            config = config.set(key, value)
        return config

DEFAULT_DF_POLICY = DataFusionConfigPolicy(
    settings={
        "datafusion.execution.collect_statistics": "true",
        "datafusion.execution.meta_fetch_concurrency": "32",
        "datafusion.execution.planning_concurrency": "0",
        "datafusion.execution.parquet.pushdown_filters": "true",
        "datafusion.execution.parquet.max_predicate_cache_size": "64M",
        "datafusion.runtime.list_files_cache_limit": "16M",
        "datafusion.runtime.list_files_cache_ttl": "2m",
        "datafusion.runtime.metadata_cache_limit": "128M",
        "datafusion.runtime.memory_limit": "8G",
        "datafusion.runtime.temp_directory": "/tmp/datafusion",
        "datafusion.runtime.max_temp_directory_size": "100G",
        "datafusion.execution.parquet.enable_page_index": "true",
        "datafusion.execution.parquet.metadata_size_hint": "524288",
    }
)
```

**Target files**

- `src/datafusion_engine/runtime.py`
- `src/ibis_engine/backend.py`
- `src/obs/manifest.py`

**Implementation checklist**

- [ ] Define a policy pack for critical config keys.
- [ ] Apply policy pack in the session factory.
- [ ] Include policy values in telemetry payloads.
- [ ] Include predicate cache + pushdown filters + runtime guardrails.

---

## Scope item 1.3 — Settings snapshot via information_schema

**Code pattern**

```python
def settings_snapshot(ctx: SessionContext) -> pa.Table:
    return ctx.sql(
        "SELECT name, value FROM information_schema.df_settings"
    ).to_arrow_table()
```

**Target files**

- `src/obs/manifest.py`
- `src/relspec/rules/diagnostics.py`
- `src/datafusion_engine/runtime.py`

**Implementation checklist**

- [ ] Enable information_schema in the runtime profile.
- [ ] Capture df_settings into run bundle artifacts.
- [ ] Record settings snapshots alongside plan diagnostics.

---

## Scope item 1.4 — Plan diagnostics snapshot

**Code pattern**

```python
def snapshot_plans(df: DataFrame) -> dict[str, object]:
    return {
        "logical": df.logical_plan(),
        "optimized": df.optimized_logical_plan(),
        "physical": df.execution_plan(),
    }
```

**Target files**

- `src/datafusion_engine/runtime.py`
- `src/relspec/rules/diagnostics.py`

**Implementation checklist**

- [ ] Add a plan snapshot helper to diagnostics.
- [ ] Attach runtime profile payload to diagnostics artifacts.

---

## Scope item 1.5 — DataFrame cache policy for hot inputs

**Code pattern**

```python
def maybe_cache(df: DataFrame, *, enabled: bool) -> DataFrame:
    if enabled:
        return df.cache()
    return df
```

**Target files**

- `src/datafusion_engine/registry_bridge.py`
- `src/datafusion_engine/bridge.py`
- `src/ibis_engine/runner.py`

**Implementation checklist**

- [ ] Add a cache policy hook for hot normalized inputs.
- [ ] Avoid caching wide tables before projection + filter.
- [ ] Keep caching opt-in and profile-driven.

---

## Scope item 1.6 — Kernel lane sessions must share profile

**Code pattern**

```python
def _session_context(
    ctx: ExecutionContext | None,
    *,
    default_ctx: SessionContext,
) -> SessionContext:
    if ctx is None or ctx.runtime.datafusion is None:
        return default_ctx
    return ctx.runtime.datafusion.session_context()
```

**Target files**

- `src/datafusion_engine/kernels.py`

**Implementation checklist**

- [ ] Ensure kernel helpers never create a bare SessionContext by default.
- [ ] Register UDFs on the profile-derived session only.

---

## Scope item 1.7 — Optional Rust extension: RuntimeEnv CacheManager

**Code pattern**

```rust
// Rust (optional extension hook)
let runtime = RuntimeEnvBuilder::new()
    .with_cache_manager(Arc::new(MyCacheManager::new()))
    .build()?;
```

**Target files**

- `src/datafusion_engine/runtime.py`
- `rust/datafusion_ext/lib.rs`

**Implementation checklist**

- [ ] Define a cache manager contract for embedded deployments.
- [ ] Keep this optional and gated by a feature flag.
- [ ] Surface cache policy in runtime diagnostics.

---

## Scope item 1.8 — Optional distributed runtime (Ballista/Ray)

**Code pattern**

```python
# Optional distributed execution (Ballista)
ctx = BallistaContext("scheduler:50050")
df = ctx.sql("SELECT * FROM cst_callsites")
```

**Target files**

- `src/datafusion_engine/runtime.py`
- `src/ibis_engine/runner.py`

**Implementation checklist**

- [ ] Keep distributed execution optional and off by default.
- [ ] Provide a runtime profile switch for distributed contexts.
- [ ] Treat Ballista/Ray as alternatives, not requirements.

---

### Watchouts

* Do not allow ad-hoc SessionContext creation once the profile is introduced.
* Config keys must be set **before** table registration for stats/cache to apply.

---

# PR-02: Dataset Registry → DataFusion Table Providers (scan + dataset I/O)

### Description (what changes)

This PR completes **registry-owned scan behavior** and adds a **provider-grade**
path for registering `pyarrow.dataset.Dataset` objects. It wires in file sort
order hints, optional schema hints, and metadata sidecars to ensure stable,
high-performance scans in DataFusion.

---

## Scope item 2.1 — Scan options: file sort order + pruning + metadata

**Code pattern**

```python
def _register_parquet(
    ctx: SessionContext,
    *,
    name: str,
    location: DatasetLocation,
    options: DataFusionRegistryOptions,
) -> DataFrame:
    scan = options.scan
    table_partition_cols = (
        [(col, str(dtype)) for col, dtype in scan.partition_cols]
        if scan and scan.partition_cols
        else None
    )
    kwargs: dict[str, object] = {
        "schema": options.schema,
        "file_extension": scan.file_extension if scan else ".parquet",
        "table_partition_cols": table_partition_cols,
    }
    if scan is not None:
        kwargs["file_sort_order"] = scan.file_sort_order or None
        kwargs["parquet_pruning"] = scan.parquet_pruning
        kwargs["skip_metadata"] = scan.skip_metadata
    if table_partition_cols:
        _call_register(ctx.register_listing_table, name, location.path, kwargs)
    else:
        _call_register(ctx.register_parquet, name, location.path, kwargs)
    return ctx.table(name)
```

**Target files**

- `src/datafusion_engine/registry_bridge.py`
- `src/schema_spec/system.py`
- `src/ibis_engine/registry.py`

**Implementation checklist**

- [ ] Wire `file_sort_order` into registry registration.
- [ ] Preserve `skip_metadata` for schema conflict control.
- [ ] Keep scan options centralized in `DatasetSpec`.

---

## Scope item 2.2 — Register `pyarrow.dataset.Dataset` providers

**Code pattern**

```python
import pyarrow.dataset as ds

dataset = ds.dataset(
    location.path,
    format="parquet",
    partitioning=location.partitioning or "hive",
)
ctx.register_dataset(name, dataset)
```

**Target files**

- `src/datafusion_engine/registry_bridge.py`
- `src/ibis_engine/registry.py`

**Implementation checklist**

- [ ] Add DatasetLocation option for provider registration.
- [ ] Support dataset-based registration path in the registry bridge.

---

## Scope item 2.3 — LocalFileSystem object store registration

**Code pattern**

```python
from datafusion.object_store import LocalFileSystem

store = LocalFileSystem(prefix=str(root))
ctx.register_object_store("file://", store, None)
```

**Target files**

- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/registry_bridge.py`
- `src/storage/io.py`

**Implementation checklist**

- [ ] Add local filesystem object store registration when a root is configured.
- [ ] Keep the registration optional and scoped to local paths only.
- [ ] Document the registered scheme for local paths.

---

## Scope item 2.4 — Metadata sidecars for faster discovery

**Code pattern**

```python
import pyarrow.parquet as pq

def write_metadata_sidecars(schema: pa.Schema, *, root: Path) -> None:
    pq.write_metadata(schema, root / "_common_metadata")
```

**Target files**

- `src/arrowdsl/io/parquet.py`
- `src/storage/io.py`

**Implementation checklist**

- [ ] Generate `_common_metadata` on materialization paths.
- [ ] Favor `skip_metadata=False` when sidecars exist.

---

## Scope item 2.5 — URL table mode for dev workflows (optional)

**Code pattern**

```python
ctx = SessionContext().enable_url_table()
df = ctx.table("/tmp/datasets/events.parquet")
```

**Target files**

- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/registry_bridge.py`

**Implementation checklist**

- [ ] Gate url-table mode behind a dev-only flag.
- [ ] Keep production registry paths explicit and catalog-backed.

---

## Scope item 2.6 — Registry introspection artifact

**Code pattern**

```python
def registry_snapshot(registry: DatasetCatalog) -> list[dict[str, object]]:
    return [
        {"name": name, "location": registry.get(name).path}
        for name in registry.names()
    ]
```

**Target files**

- `src/ibis_engine/registry.py`
- `src/obs/manifest.py`

**Implementation checklist**

- [ ] Emit dataset name → location → scan options snapshot.
- [ ] Include schema hints for validation tooling.

---

## Scope item 2.7 — Ibis memtable naming and view materialization

**Code pattern**

```python
mt = ibis.memtable(table)
backend.create_view("staging_rules", mt, overwrite=True)
expr = backend.table("staging_rules")
```

**Target files**

- `src/ibis_engine/plan_bridge.py`
- `src/relspec/compiler.py`
- `src/relspec/compiler_graph.py`
- `src/cpg/relationship_plans.py`

**Implementation checklist**

- [ ] Keep memtables anonymous unless a stable name is required.
- [ ] Prefer create_view/create_table over Table.alias side effects.
- [ ] Ensure view naming is centralized and deterministic.

---

## Scope item 2.8 — Optional streaming inputs (Rust StreamingTable)

**Code pattern**

```rust
// Rust (optional streaming table provider)
let table = StreamingTable::try_new(schema, stream)?;
ctx.register_table("repo_events", table)?;
```

**Target files**

- `rust/datafusion_ext/lib.rs`
- `src/hamilton_pipeline/modules/inputs.py`

**Implementation checklist**

- [ ] Keep streaming providers optional and behind a feature flag.
- [ ] Use for incremental pipelines, not batch runs.

---

### Watchouts

* Partition columns must match actual directory layout or pruning will fail.
* Use dataset providers for stable schemas across heterogeneous shards.

---

# PR-03: Relspec Compile/Diagnostics Hardening

### Description (what changes)

This PR turns the relspec compiler into a **real compiler toolchain** with
canonical plan signatures, strict SQLGlot diagnostics, semantic diffs, optional
Substrait artifacts, and compile-time contract validation.

---

## Scope item 3.1 — Canonical plan signature

**Code pattern**

```python
def plan_signature(payload: Mapping[str, object]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()
```

**Target files**

- `src/relspec/rules/compiler.py`
- `src/relspec/rules/diagnostics.py`

**Implementation checklist**

- [ ] Canonicalize payloads before hashing.
- [ ] Store signatures in diagnostics artifacts.

---

## Scope item 3.2 — SQLGlot normalize + strict mode

**Code pattern**

```python
from sqlglot import ErrorLevel, parse_one

expr = parse_one(sql, error_level=ErrorLevel.RAISE)
optimized = normalize_expr(expr, schema=schema_map)
```

**Target files**

- `src/sqlglot_tools/optimizer.py`
- `src/relspec/sqlglot_diagnostics.py`

**Implementation checklist**

- [ ] Fail fast on unsupported constructs in diagnostics mode.
- [ ] Normalize ASTs before lineage extraction.

---

## Scope item 3.3 — Semantic diffs (SQLGlot)

**Code pattern**

```python
import sqlglot

diff = sqlglot.diff(old_expr, new_expr)
```

**Target files**

- `src/sqlglot_tools/bridge.py`
- `src/relspec/rules/diagnostics.py`

**Implementation checklist**

- [ ] Attach diff summaries to diagnostic metadata.
- [ ] Keep AST diffs alongside raw SQL snapshots.

---

## Scope item 3.4 — Substrait plan capture

**Code pattern**

```python
from datafusion.substrait import Serde

plan_bytes = Serde.serialize_bytes(sql, ctx)
```

**Target files**

- `src/relspec/rules/diagnostics.py`
- `src/datafusion_engine/bridge.py`

**Implementation checklist**

- [ ] Make Substrait capture optional by config flag.
- [ ] Store bytes in run bundle artifacts.

---

## Scope item 3.5 — Contract validation hook

**Code pattern**

```python
missing = required_cols - set(produced_schema.names)
if missing:
    raise ContractError(f"Missing required columns: {sorted(missing)}")
```

**Target files**

- `src/relspec/edge_contract_validator.py`
- `src/relspec/rules/validation.py`

**Implementation checklist**

- [ ] Validate produced schema against contract at compile time.
- [ ] Fail before materialization to preserve local context.

---

## Scope item 3.6 — Ibis to SQLGlot as compiler IR + lineage metadata

**Code pattern**

```python
sg_expr = backend.compiler.to_sqlglot(expr)
tables = {tbl.name for tbl in sg_expr.find_all(exp.Table)}
columns = {col.name for col in sg_expr.find_all(exp.Column)}
ast_repr = repr(sg_expr)
```

**Target files**

- `src/sqlglot_tools/bridge.py`
- `src/sqlglot_tools/lineage.py`
- `src/relspec/sqlglot_diagnostics.py`

**Implementation checklist**

- [ ] Treat SQLGlot AST as the canonical diagnostics IR.
- [ ] Extract tables/columns/identifiers from the AST.
- [ ] Store AST repr for debug-grade artifacts.

---

## Scope item 3.7 — SQLGlot AST transforms for canonicalization

**Code pattern**

```python
def canonicalize(expr: Expression) -> Expression:
    def _rewrite(node: Expression) -> Expression:
        if isinstance(node, exp.Column) and node.name == "repo":
            return exp.column("repo_id")
        return node
    return expr.transform(_rewrite)
```

**Target files**

- `src/sqlglot_tools/optimizer.py`
- `src/relspec/sqlglot_diagnostics.py`

**Implementation checklist**

- [ ] Normalize column/function names before hashing.
- [ ] Keep rewrite rules deterministic and explicit.

---

## Scope item 3.8 — Optional SQLGlot custom dialect for DataFusion output

**Code pattern**

```python
from sqlglot import Dialect, exp

class DataFusionDialect(Dialect):
    class Generator(Dialect.Generator):
        TRANSFORMS = {
            **Dialect.Generator.TRANSFORMS,
            exp.Array: lambda self, e: "ARRAY[" + self.expressions(e) + "]",
        }
```

**Target files**

- `src/sqlglot_tools/optimizer.py`
- `src/sqlglot_tools/bridge.py`

**Implementation checklist**

- [ ] Only enable when DataFusion SQL rendering needs overrides.
- [ ] Keep dialect changes scoped to SQL emission, not parsing.

---

## Scope item 3.9 — Ibis schema DDL round-trip for contract artifacts

**Code pattern**

```python
schema = ibis.schema({"repo_id": "int64", "name": "string"})
column_defs = schema.to_sqlglot_column_defs(dialect="ansi")
```

**Target files**

- `src/schema_spec/specs.py`
- `src/relspec/contracts.py`
- `src/relspec/rules/validation.py`

**Implementation checklist**

- [ ] Emit contract DDL artifacts from schemas.
- [ ] Use DDL artifacts in diagnostics bundles for audits.

---

## Scope item 3.10 — Use analyzable SQL nodes, avoid raw_sql

**Code pattern**

```python
expr = backend.sql("SELECT * FROM cst_callsites")
# Avoid backend.raw_sql(...) to preserve AST visibility.
```

**Target files**

- `src/ibis_engine/query_compiler.py`
- `src/ibis_engine/query_bridge.py`
- `src/relspec/compiler.py`

**Implementation checklist**

- [ ] Prefer Table.sql/Backend.sql to keep SQLGlot visibility.
- [ ] Use raw_sql only for truly opaque operations.

---

## Scope item 3.11 — Parameterized compilation (no SQL string interpolation)

**Code pattern**

```python
sql = expr.to_sql(params={"repo_id": 1}, pretty=False)
compiled = backend.compile(expr, params={"repo_id": 1})
```

**Target files**

- `src/ibis_engine/query_compiler.py`
- `src/ibis_engine/params_bridge.py`
- `src/relspec/compiler.py`

**Implementation checklist**

- [ ] Standardize scalar params via Ibis compile params.
- [ ] Avoid ad-hoc SQL string interpolation.

---

## Scope item 3.12 — SQLGlot executor for lightweight CI checks (optional)

**Code pattern**

```python
from sqlglot import executor

result = executor.execute(
    "SELECT x + 1 AS y FROM t",
    tables={"t": [{"x": 1}, {"x": 2}]},
)
```

**Target files**

- `tests/test_sqlglot_executor_smoke.py`
- `tests/normalize/test_sqlglot_executor_smoke.py`

**Implementation checklist**

- [ ] Add tiny executor-based tests for compiler regressions.
- [ ] Keep datasets small and deterministic.

---

## Scope item 3.13 — Optional Rust extension: tracing + metrics

**Code pattern**

```rust
// Rust (optional extension hook)
let span = tracing::info_span!("rule_exec", rule = rule_name);
let _enter = span.enter();
let metrics = plan.metrics().unwrap_or_default();
```

**Target files**

- `rust/datafusion_ext/lib.rs`
- `src/obs/manifest.py`

**Implementation checklist**

- [ ] Gate tracing behind an opt-in flag.
- [ ] Expose operator metrics in run bundle artifacts.

---

### Watchouts

* SQL text is not stable across SQLGlot versions; prefer AST + Substrait.
* Substrait capture must remain diagnostics-only until stabilized.

---

# PR-04: Hybrid Kernel Bridge (capability-driven lanes)

### Description (what changes)

This PR formalizes **kernel selection** across DataFusion built-ins, UDFs, and
Arrow fallbacks. It preserves determinism by encoding ordering and volatility
in kernel specs and records lane selection for diagnostics.

---

## Scope item 4.1 — Capability registry (built-in → UDF → Arrow fallback)

**Code pattern**

```python
class KernelLane(Enum):
    BUILTIN = "builtin"
    DF_UDF = "df_udf"
    ARROW_FALLBACK = "arrow_fallback"

@dataclass(frozen=True)
class KernelSpec:
    name: str
    lane: KernelLane
    volatility: str
```

**Target files**

- `src/datafusion_engine/kernels.py`
- `src/arrowdsl/compute/kernels.py`

**Implementation checklist**

- [ ] Map kernel names to a preferred lane.
- [ ] Include volatility and ordering requirements in the spec.

---

## Scope item 4.2 — Arrow-native UDF utilities

**Code pattern**

```python
def normalize_span(values: pa.Array) -> pa.Array:
    text = pc.utf8_trim(pc.cast(values, pa.string(), safe=False))
    return pc.cast(text, pa.int64(), safe=False)

udf_fn = udf(normalize_span, [pa.string()], pa.int64(), "stable", name="normalize_span")
ctx.register_udf(udf_fn)
```

**Target files**

- `src/datafusion_engine/kernels.py`

**Implementation checklist**

- [ ] Keep UDFs Arrow-native (no `as_py()` loops).
- [ ] Register UDFs once per SessionContext.

---

## Scope item 4.3 — Arrow fallback kernels (compute + Acero)

**Code pattern**

```python
indices = pc.sort_indices(table, sort_keys=sort_keys)
result = pc.take(table, indices)
```

**Target files**

- `src/arrowdsl/compute/kernels.py`
- `src/arrowdsl/plan/ops.py`

**Implementation checklist**

- [ ] Use compute options objects for stable behavior.
- [ ] Honor ordering metadata when falling back.

---

## Scope item 4.4 — Lane diagnostics + ordering requirements

**Code pattern**

```python
diagnostics = {
    "kernel": spec.name,
    "lane": spec.lane.value,
    "volatility": spec.volatility,
    "ordering_required": spec.requires_ordering,
}
```

**Target files**

- `src/relspec/rules/diagnostics.py`
- `src/arrowdsl/core/context.py`

**Implementation checklist**

- [ ] Emit lane selection in diagnostics artifacts.
- [ ] Enforce explicit ordering before dedupe kernels.

---

## Scope item 4.5 — Prefer DataFusion unnest/UDTF for list explode

**Code pattern**

```python
exploded = df.unnest_columns("dst_ids")
selected = exploded.select(
    col("src_id"),
    col("dst_ids").alias("dst_id"),
)
```

**Target files**

- `src/datafusion_engine/kernels.py`
- `src/arrowdsl/compute/kernels.py`

**Implementation checklist**

- [ ] Use DataFusion unnest/table functions before Arrow fallback.
- [ ] Keep explode semantics centralized in the kernel registry.

---

## Scope item 4.6 — Ibis builtin UDFs for backend-native functions

**Code pattern**

```python
import ibis.expr.datatypes as dt

@ibis.udf.scalar.builtin
def cpg_score(value: dt.float64) -> dt.float64:
    ...
```

**Target files**

- `src/relspec/rules/rel_ops.py`
- `src/ibis_engine/expr_compiler.py`
- `src/cpg/relationship_plans.py`

**Implementation checklist**

- [ ] Use builtin UDFs when Ibis lacks a function wrapper.
- [ ] Keep signatures stable and document volatility.

---

## Scope item 4.7 — Ibis expression modernization (cases/as_/order_by/unpack)

**Code pattern**

```python
expr = ibis.cases((cond, value), else_=default)
expr = table.order_by(ibis.desc("score"))
expr = table.unpack("payload")
ts = table.col.as_timestamp("us")
```

**Target files**

- `src/relspec/rules/rel_ops.py`
- `src/normalize/plan_builders.py`
- `src/cpg/relationship_plans.py`
- `src/normalize/rule_specs.py`

**Implementation checklist**

- [ ] Replace deprecated Ibis APIs with v11 equivalents.
- [ ] Keep expression builders aligned with SQLGlot compilation.

---

## Scope item 4.8 — Optional FunctionFactory + advanced UDF planning (Rust)

**Code pattern**

```sql
-- Requires Rust extension to enable FunctionFactory
CREATE FUNCTION cpg_score(x DOUBLE) RETURNS DOUBLE
RETURN x;
```

**Target files**

- `rust/datafusion_ext/lib.rs`
- `src/datafusion_engine/runtime.py`

**Implementation checklist**

- [ ] Gate FunctionFactory behind a feature flag.
- [ ] Use it to standardize global rule primitives.
- [ ] Consider async scalar UDFs only for IO-bound primitives.
- [ ] Use custom expression planning hooks for domain operators.
- [ ] Prefer named arguments when the SQL surface supports them.

---

### Watchouts

* Dedupe and winner selection require explicit ordering.
* Volatility must be part of the kernel contract for planner correctness.

---

# Global Implementation Notes

* Keep edits **strictly scoped** to PR-01..PR-04; do not change architecture.
* Ensure all additions are **DataFusion-first** and **Arrow-native** where needed.
* Treat diagnostics artifacts as **run-bundle critical** for reproducibility.
