## Advanced Integrations Implementation Plan

This plan captures integration upgrades identified after reviewing:
- `docs/python_library_reference/advanced_integrations.md`
- `docs/python_library_reference/datafusion_advanced_integration.md`
- `docs/python_library_reference/ibis_advanced_integration.md`
- `docs/python_library_reference/sqlglot_advanced_integration.md`
- `docs/python_library_reference/pyarrow_advanced_integration.md`

Each scope item includes the target pattern, file touch list, and implementation checklist.

### Scope 1: Canonical SQLGlot AST identity + lineage-driven scan columns

**Objective:** Move query identity and dependency extraction to canonical
SQLGlot ASTs (serde dump) and compute required input columns via lineage.

**Pattern snippet**
```python
from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable

from sqlglot import Expression, exp
from sqlglot.lineage import lineage
from sqlglot.optimizer import scope
from sqlglot.serde import dump


def canonical_ast_fingerprint(expr: Expression) -> str:
    payload = dump(expr)
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def required_columns(expr: Expression, *, output_columns: Iterable[str]) -> dict[str, set[str]]:
    scope_root = scope.build_scope(expr)
    deps: dict[str, set[str]] = {}
    for col in output_columns:
        tree = lineage(col, expr, scope=scope_root, trim_selects=True, copy=False)
        deps[col] = {node.name for node in tree.find_all(exp.Column)}
    return deps
```

**Target files**
- `src/sqlglot_tools/optimizer.py`
- `src/sqlglot_tools/lineage.py`
- `src/sqlglot_tools/bridge.py`
- `src/datafusion_engine/bridge.py`
- `src/ibis_engine/compiler_checkpoint.py`
- `src/obs/repro.py`
- `src/obs/manifest.py`

**Implementation checklist**
- [ ] Add serde-based fingerprint helper and swap plan hash usage where appropriate.
- [ ] Add lineage extraction helper that returns required base columns.
- [ ] Fingerprint the policy-engine canonical AST (qualify + normalize identifiers + type-aware canonicalization).
- [ ] Emit canonical AST payloads into repro/manifest artifacts.
- [ ] Use required column sets to drive scan projections for dataset reads.
- [ ] Cache scope objects for repeated lineage extraction across output columns.
- [ ] Persist generator/dialect settings alongside AST fingerprints.
- [ ] Add tests that prove AST fingerprint stability across formatting changes.

---

### Scope 2: Execute SQLGlot ASTs directly in the Ibis raw SQL lane

**Objective:** Avoid string SQL when backend supports `raw_sql(Expression)`, while
enforcing cursor lifecycle safety and schema-bearing SQL escape hatches.

**Pattern snippet**
```python
from __future__ import annotations

from contextlib import closing

from sqlglot import Expression


def execute_raw_sql(backend: object, expr: Expression) -> object:
    raw = getattr(backend, "raw_sql", None)
    if not callable(raw):
        msg = "Backend missing raw_sql."
        raise TypeError(msg)
    try:
        return raw(expr)
    except TypeError:
        sql = expr.sql(dialect="datafusion_ext", unsupported_level="raise")
        return raw(sql)


def fetch_all(cursor: object) -> list[tuple[object, ...]]:
    with closing(cursor) as handle:
        return list(handle.fetchall())


def sql_table(backend: object, query: str, *, schema) -> object:
    return backend.sql(query, schema=schema)
```

**Target files**
- `src/ibis_engine/runner.py`
- `src/ibis_engine/query_compiler.py`
- `src/sqlglot_tools/optimizer.py`
- `src/datafusion_engine/bridge.py`

**Implementation checklist**
- [ ] Add `raw_sql(Expression)` capability detection and fallback to strings.
- [ ] Enforce cursor lifecycle safety via `contextlib.closing` on `raw_sql`.
- [ ] Require explicit schemas for `Backend.sql` / `Table.sql` escape hatches.
- [ ] Ensure diagnostics capture the canonical AST regardless of execution lane.
- [ ] Add tests for AST execution on the DataFusion backend.

---

### Scope 3: Unified runtime profile v1 (DataFusion + Arrow + SQLGlot + Ibis)

**Objective:** Expand the runtime profile into a versioned contract that includes
DataFusion configuration keys, join strategy knobs, parquet read/write policies,
Arrow thread pools, SQLGlot policy, and Ibis options (including `fuse_selects`,
`default_limit`, `default_dialect`, and `interactive`).

**Pattern snippet**
```python
from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from typing import Mapping


@dataclass(frozen=True)
class DataFusionRuntimeProfileV1:
    versions: Mapping[str, str]
    session_config: Mapping[str, str]
    feature_gates: Mapping[str, bool]
    join_strategy: Mapping[str, bool]
    parquet_read: Mapping[str, object]
    output_writes: Mapping[str, object]
    spill: Mapping[str, object]
    sql_surfaces: Mapping[str, Mapping[str, bool]]
    arrow_threads: Mapping[str, int]
    sqlglot_policy: Mapping[str, object]
    ibis_options: Mapping[str, object]


def runtime_profile_hash(profile: DataFusionRuntimeProfileV1) -> str:
    payload = asdict(profile)
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
```

**Target files**
- `src/datafusion_engine/runtime.py`
- `src/engine/runtime_profile.py`
- `src/arrowdsl/core/context.py`
- `src/sqlglot_tools/optimizer.py`
- `src/ibis_engine/backend.py`
- `src/obs/manifest.py`
- `src/obs/repro.py`

**Implementation checklist**
- [ ] Add a versioned DataFusionRuntimeProfile v1 payload in runtime artifacts.
- [ ] Include join strategy, listing-table, parquet read, output writes, spill, ANSI mode.
- [ ] Add Arrow thread pools, SQLGlot policy, and Ibis options into the same hash.
- [ ] Record `ibis.options.sql.fuse_selects`, `default_limit`, `default_dialect`, and `interactive`.
- [ ] Include SQLGlot read/write dialects, rule list hash, generator options, and tokenizer mode.
- [ ] Capture PyArrow version, CPU/IO thread pools, memory pool backend, and peak memory.
- [ ] Replace `datafusion_settings_hash` usage in manifests with unified profile hash.
- [ ] Add tests covering profile hash changes when any policy changes.

---

### Scope 4: Dataset discovery with inspection + URI normalization + `_metadata` fast path

**Objective:** Add a factory-based dataset discovery pipeline that supports
`FileSystem.from_uri`, `FileSystemDatasetFactory.inspect`, and metadata sidecars.

**Pattern snippet**
```python
from __future__ import annotations

from pathlib import Path

import pyarrow.dataset as ds
from pyarrow import fs


def build_dataset_from_uri(uri: str, *, schema=None, strict: bool = True) -> ds.Dataset:
    filesystem, path = fs.FileSystem.from_uri(uri)
    if isinstance(path, str):
        metadata_path = Path(path) / "_metadata"
        if metadata_path.exists():
            return ds.parquet_dataset(str(metadata_path))
    selector = fs.FileSelector(path, recursive=True)
    opts = ds.FileSystemFactoryOptions(
        partition_base_dir=path,
        exclude_invalid_files=True,
        selector_ignore_prefixes=[".", "_"],
    )
    factory = ds.FileSystemDatasetFactory(filesystem, selector, ds.ParquetFileFormat(), opts)
    promote = "default" if strict else "permissive"
    inspected = factory.inspect(promote_options=promote)
    return factory.finish(schema=inspected)
```

**Target files**
- `src/arrowdsl/plan/source_normalize.py`
- `src/arrowdsl/plan/scan_builder.py`
- `src/arrowdsl/io/parquet.py`

**Implementation checklist**
- [ ] Add `from_uri` normalization and return `(filesystem, path)` consistently.
- [ ] Add factory-based discovery with strict/permissive schema inspection.
- [ ] Add `_metadata` fast path when metadata sidecars exist.
- [ ] Persist `FileSystemDatasetFactory` inspect/finish settings and promote policy.
- [ ] Emit discovery policy and inspected schema as artifacts.

---

### Scope 5: Unified writer contract across Arrow and DataFusion lanes

**Objective:** Centralize dataset layout policy (threading, ordering, partitioning,
row group sizing, file visitor) and apply it consistently across writer strategies,
including Ibis `to_parquet_dir` / `to_pyarrow_batches` sinks.

**Pattern snippet**
```python
from __future__ import annotations

from dataclasses import dataclass

from datafusion import DataFrameWriteOptions, ParquetWriterOptions, col
from datafusion.functions import order_by


@dataclass(frozen=True)
class WriteSpec:
    basename_template: str
    use_threads: bool
    preserve_order: bool
    max_rows_per_file: int | None
    min_rows_per_group: int | None
    max_rows_per_group: int | None
    partition_cols: tuple[str, ...] = ()
    partitioning_flavor: str | None = "hive"
    sort_by: tuple[str, ...] = ()
    single_file: bool = False


def df_write_options(spec: WriteSpec) -> DataFrameWriteOptions:
    sort_exprs = [order_by(col(name)) for name in spec.sort_by]
    return DataFrameWriteOptions(
        partition_by=list(spec.partition_cols),
        sort_by=sort_exprs if sort_exprs else None,
        single_file_output=spec.single_file,
    )


def parquet_write_options() -> ParquetWriterOptions:
    return ParquetWriterOptions()


def ibis_write_dataset(expr, spec: WriteSpec, out_dir: str) -> None:
    if spec.sort_by:
        expr = expr.order_by(list(spec.sort_by))
    expr.to_parquet_dir(out_dir, partitioning=list(spec.partition_cols))
```

**Target files**
- `src/arrowdsl/io/parquet.py`
- `src/ibis_engine/io_bridge.py`
- `src/engine/materialize.py`
- `src/datafusion_engine/runtime.py`

**Implementation checklist**
- [ ] Add a `WriteSpec` type that fully captures Arrow `write_dataset` knobs.
- [ ] Add DataFusion `DataFrameWriteOptions` + `ParquetWriterOptions` mapping.
- [ ] Standardize file visitor + metadata sidecars for Arrow and DataFusion writers.
- [ ] Align output sizing knobs with DataFusion runtime config keys.
- [ ] Use `to_parquet_dir` or Arrow dataset writes for partitioned outputs on DataFusion.
- [ ] Encode `preserve_order`, `max_open_files`, and row-group sizing limits.
- [ ] Gate `_metadata` emission based on size constraints and workload policy.
- [ ] Add tests validating ordering, partition layout, and file sizing controls.

---

### Scope 6: Schema fingerprint fidelity (metadata + extension types + DDL hash)

**Objective:** Include schema/field metadata and extension type identity in the
storage fingerprint; add a separate DDL fingerprint for execution identity.

**Pattern snippet**
```python
from __future__ import annotations

import hashlib
import json


def schema_fingerprint_payload(schema) -> dict[str, object]:
    return {
        "fields": [
            {
                "name": f.name,
                "type": str(f.type),
                "nullable": bool(f.nullable),
                "metadata": dict(f.metadata or {}),
                "extension": getattr(f.type, "extension_name", None),
            }
            for f in schema
        ],
        "metadata": dict(schema.metadata or {}),
    }


def schema_fingerprint(schema) -> str:
    payload = schema_fingerprint_payload(schema)
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
```

**Target files**
- `src/arrowdsl/schema/serialization.py`
- `src/schema_spec/system.py`
- `src/obs/manifest.py`
- `src/obs/repro.py`

**Implementation checklist**
- [ ] Extend schema serialization to include metadata + extension types.
- [ ] Add DDL fingerprint derived from SQLGlot column defs.
- [ ] Update manifest and repro outputs to store both fingerprints.
- [ ] Add tests ensuring metadata changes affect schema fingerprint.

---

### Scope 7: Unified nested data contract (ExplodeSpec + idx + keep_empty)

**Objective:** Implement a single nested-data contract with explicit offsets,
parent alignment, deterministic ordering, and nested constructors across Ibis,
DataFusion, and Arrow kernels.

**Pattern snippet**
```python
from __future__ import annotations

from dataclasses import dataclass

import ibis


@dataclass(frozen=True)
class ExplodeSpec:
    parent_keys: tuple[str, ...]
    list_col: str
    value_col: str
    idx_col: str | None = "idx"
    keep_empty: bool = True


def explode_with_idx(table, spec: ExplodeSpec):
    # Ibis: .unnest(list_col, offset=idx_col, keep_empty=keep_empty)
    # DataFusion: df.unnest_columns + idx synthesis via range/cardinality
    # Arrow: list_parent_indices + list_flatten + idx computation
    ...


def stable_unnest(expr, spec: ExplodeSpec):
    indexed = expr.mutate(_row=ibis.row_number())
    exploded = indexed.unnest(
        spec.list_col,
        offset=spec.idx_col,
        keep_empty=spec.keep_empty,
    )
    return exploded.order_by((*spec.parent_keys, "_row", spec.idx_col))
```

**Target files**
- `src/cpg/relationship_plans.py`
- `src/datafusion_engine/kernels.py`
- `src/arrowdsl/compute/kernels.py`
- `src/arrowdsl/compute/expr_core.py`

**Implementation checklist**
- [ ] Define `ExplodeSpec` and a dispatch helper for Ibis/DF/Arrow lanes.
- [ ] Ensure idx is 0-based and ordering is explicit (`parent_key, idx`).
- [ ] Implement `keep_empty` behavior across all lanes.
- [ ] Standardize nested constructors (`ibis.array`, `ibis.map`, `ibis.struct`).
- [ ] Use Arrow kernel-based explode (`list_parent_indices` + `list_flatten` + `take`).
- [ ] Add golden tests for empty/null lists and list<struct> unpack.

---

### Scope 8: Unified FunctionSpec registry (cross-lane identity + UDAF/UDWF/UDTF)

**Objective:** Centralize function definitions (signature, volatility, naming,
lane availability, catalog/database placement, and lane precedence) and emit a
registry fingerprint into build identity.

**Pattern snippet**
```python
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import pyarrow as pa


@dataclass(frozen=True)
class FunctionSpec:
    func_id: str
    engine_name: str
    catalog: str | None = None
    database: str | None = None
    kind: Literal["scalar", "aggregate", "window", "table"]
    input_types: tuple[pa.DataType, ...]
    return_type: pa.DataType
    state_type: pa.DataType | None = None
    volatility: str = "stable"
    arg_names: tuple[str, ...] | None = None
    lanes: tuple[str, ...] = (
        "ibis_builtin",
        "ibis_pyarrow",
        "ibis_pandas",
        "ibis_python",
        "df_udf",
        "df_rust",
    )
    lane_precedence: tuple[str, ...] = (
        "ibis_builtin",
        "ibis_pyarrow",
        "ibis_pandas",
        "ibis_python",
    )
    rewrite_tags: tuple[str, ...] = ()
```

**Target files**
- `src/datafusion_engine/function_factory.py`
- `src/datafusion_engine/udf_registry.py`
- `src/ibis_engine/builtin_udfs.py`
- `src/engine/runtime_profile.py`
- `src/obs/manifest.py`
- `src/obs/repro.py`

**Implementation checklist**
- [ ] Create a unified registry that describes all functions in one place.
- [ ] Add installers for scalar/aggregate/window/table UDF families.
- [ ] Encode catalog/database placement and Ibis UDF lane precedence.
- [ ] Add rewrite tags/hooks for pushdown-friendly UDF transformations.
- [ ] Add registry fingerprint to build identity and manifest.
- [ ] Snapshot installed UDFs in repro bundles for diagnostics.

---

### Scope 9: DataFusion plan artifacts + plan rehydration

**Objective:** Persist logical/optimized/physical plans (and Graphviz) and
support rehydrating DataFrames from stored logical plans.

**Pattern snippet**
```python
from __future__ import annotations

def capture_plans(ctx, df):
    logical = df.logical_plan()
    optimized = df.optimized_logical_plan()
    physical = df.execution_plan()
    graphviz = optimized.display_graphviz()
    return {
        "logical": logical.display_indent_schema(),
        "optimized_dot": graphviz,
        "physical": physical.display_indent(),
        "partition_count": physical.partition_count,
    }


def rehydrate_plan(ctx, logical_plan):
    return ctx.create_dataframe_from_logical_plan(logical_plan)
```

**Target files**
- `src/datafusion_engine/bridge.py`
- `src/datafusion_engine/runtime.py`
- `src/obs/repro.py`

**Implementation checklist**
- [ ] Capture logical/optimized/physical plan artifacts per product.
- [ ] Emit Graphviz DOT for optimized plans.
- [ ] Persist plan partition counts for downstream writer planning.
- [ ] Implement rehydration path for cached logical plans.
- [ ] Add unparser SQL capture for review artifacts.

---

### Scope 10: Join algorithm controls as profile fields

**Objective:** Expose join planner knobs and persist them as part of the runtime
profile hash and diagnostics.

**Pattern snippet**
```python
from datafusion import SessionConfig

config = (
    SessionConfig()
    .set("datafusion.optimizer.prefer_hash_join", "true")
    .set("datafusion.optimizer.enable_piecewise_merge_join", "false")
    .set("datafusion.optimizer.allow_symmetric_joins_without_pruning", "true")
)
```

**Target files**
- `src/datafusion_engine/runtime.py`
- `src/engine/runtime_profile.py`
- `src/obs/repro.py`

**Implementation checklist**
- [ ] Add join strategy fields to the runtime profile contract.
- [ ] Persist join strategy settings in repro/manifest artifacts.
- [ ] Add plan evidence capture to verify join operator selection.

---

### Scope 11: Listing table policy + file sort order declarations

**Objective:** Standardize `register_listing_table` usage and declare known
file ordering to enable sort elimination.

**Pattern snippet**
```python
from datafusion import SessionContext, col
from datafusion.functions import order_by

ctx = SessionContext()
ctx.register_listing_table(
    "edges",
    "s3://bucket/edges/",
    table_partition_cols=[("repo", "string")],
    file_extension=".parquet",
    file_sort_order=[order_by(col("stable_id"))],
)
```

**Target files**
- `src/datafusion_engine/registry_bridge.py`
- `src/arrowdsl/io/parquet.py`
- `src/obs/repro.py`

**Implementation checklist**
- [ ] Expose listing-table inference + ignore-subdir knobs in the profile.
- [ ] Emit `file_sort_order` metadata when outputs are canonically sorted.
- [ ] Add artifact snapshots for listing-table registration settings.

---

### Scope 12: DML sink lane (`COPY` / `INSERT`) with SQLOptions gating

**Objective:** Support DataFusion SQL writes where the statement-level format
options are beneficial, gated by explicit SQLOptions.

**Pattern snippet**
```python
from datafusion import SessionContext, SQLOptions

ctx = SessionContext()
opts = (
    SQLOptions()
    .with_allow_dml(True)
    .with_allow_ddl(False)
    .with_allow_statements(False)
)
ctx.sql_with_options(
    "COPY my_table TO 'out' STORED AS PARQUET",
    opts,
)
```

**Target files**
- `src/datafusion_engine/bridge.py`
- `src/datafusion_engine/compile_options.py`
- `src/ibis_engine/io_bridge.py`
- `src/obs/repro.py`

**Implementation checklist**
- [ ] Add a gated DML execution surface for COPY/INSERT.
- [ ] Encode format options layering (session/table/statement).
- [ ] Persist executed DML statements into repro artifacts.

---

### Scope 13: Input plugins for dataset handle resolution

**Objective:** Resolve internal dataset handles directly in DataFusion via the
input plugin system.

**Pattern snippet**
```python
from datafusion.input.base import BaseInputSource


class ArtifactInputSource(BaseInputSource):
    def is_correct_input(self, input_item, table_name, **kwargs):
        return str(input_item).startswith("artifact://")

    def build_table(self, input_item, table_name, **kwargs):
        # Resolve to path + schema + file_sort_order and return SqlTable
        ...
```

**Target files**
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/registry_bridge.py`
- `src/obs/repro.py`

**Implementation checklist**
- [ ] Implement an input source for artifact/dataset handles.
- [ ] Wire registration into session initialization.
- [ ] Persist input plugin registry and config in artifacts.

---

### Scope 14: MemTable shaping + cache policy

**Objective:** Ensure in-memory tables have batch shapes aligned with
`batch_size` and add explicit cache policy when reuse is high.

**Pattern snippet**
```python
import pyarrow as pa
from datafusion import SessionContext

ctx = SessionContext()
table = pa.table({"a": range(100_000)})
batches = table.to_batches(max_chunksize=8192)
ctx.register_record_batches("t", [batches])
df = ctx.table("t").cache()
```

**Target files**
- `src/datafusion_engine/bridge.py`
- `src/datafusion_engine/kernels.py`

**Implementation checklist**
- [ ] Add batch slicing helper for MemTable registrations.
- [ ] Add cache policy hooks where subplans are reused.
- [ ] Document cache behavior in repro artifacts.

---

### Scope 15: Prepared statements for repeated diagnostics queries

**Objective:** Use PREPARE/EXECUTE for repeated SQL diagnostics with parameters.

**Pattern snippet**
```python
from datafusion import SessionContext, SQLOptions

ctx = SessionContext()
opts = SQLOptions().with_allow_statements(True).with_allow_ddl(False).with_allow_dml(False)
ctx.sql_with_options("PREPARE diag(INT) AS SELECT * FROM runs WHERE run_id = $1", opts)
ctx.sql_with_options("EXECUTE diag(42)", opts)
```

**Target files**
- `src/obs/diagnostics.py`
- `src/obs/repro.py`
- `src/datafusion_engine/bridge.py`

**Implementation checklist**
- [ ] Add a prepared-statement surface for diagnostics queries.
- [ ] Persist prepared statements and parameter types in artifacts.
- [ ] Add tests that prepared paths match unprepared output.

---

### Scope 16: Catalog snapshots + information_schema artifacts

**Objective:** Persist catalog/schema/table state and information_schema views
as part of each run bundle for reproducibility.

**Pattern snippet**
```python
def catalog_snapshot(ctx):
    catalogs = sorted(ctx.catalog_names())
    tables = ctx.sql("SELECT * FROM information_schema.tables").to_arrow_table()
    return {"catalogs": catalogs, "tables": tables}
```

**Target files**
- `src/datafusion_engine/runtime.py`
- `src/obs/repro.py`

**Implementation checklist**
- [ ] Capture catalog tree (catalogs/schemas/tables) per run.
- [ ] Persist `information_schema.tables` in repro bundles.
- [ ] Add gating to ensure info_schema is enabled in profile.

---

### Scope 17: Determinism contract + winner selection templates

**Objective:** Enforce explicit ordering for persisted outputs and provide
window-based winner selection helpers with deterministic tie-breakers.

**Pattern snippet**
```python
from __future__ import annotations

import ibis


def enforce_order(expr, sort_keys: tuple[str, ...]):
    if not sort_keys:
        msg = "Persisted outputs require explicit ordering."
        raise ValueError(msg)
    return expr.order_by(list(sort_keys))


def winner_rows(expr, *, partition_keys: tuple[str, ...], order_keys: tuple[str, ...]):
    window = ibis.window(group_by=list(partition_keys), order_by=list(order_keys))
    ranked = expr.mutate(row_id=ibis.row_number().over(window))
    return ranked.filter(ranked.row_id == 0).order_by(list(partition_keys) + list(order_keys))
```

**Target files**
- `src/engine/plan_policy.py`
- `src/ibis_engine/plan.py`
- `src/relspec/rules/graph.py`
- `src/obs/manifest.py`

**Implementation checklist**
- [ ] Require explicit ordering for persisted products (or fail fast).
- [ ] Add canonical tie-break order fields per product type.
- [ ] Capture ordering keys in manifest/repro artifacts.
- [ ] Add tests for deterministic winner selection across reruns.

---

### Scope 18: Ibis macro DSL for rulepacks (Deferred + selectors + bind + pipe)

**Objective:** Standardize a macro layer that composes rules via deferred
expressions, selectors, and bind helpers for wide-table transformations.

**Pattern snippet**
```python
from __future__ import annotations

from ibis import _, selectors as s


def center_numeric(table):
    return table.mutate(
        s.across(s.numeric(), dict(centered=_ - _.mean()), names="{fn}_{col}")
    )


def bind_column(table, column):
    (bound,) = table.bind(column)
    return table.select(bound)


def apply_rules(table):
    return table.pipe(center_numeric).filter(_.centered > 0)
```

**Target files**
- `src/ibis_engine/expr_compiler.py`
- `src/ibis_engine/plan.py`
- `src/relspec/rules/spec_tables.py`
- `src/relspec/rules/graph.py`

**Implementation checklist**
- [ ] Add a macro module for reusable deferred/selector-based transforms.
- [ ] Accept column names, selectors, and deferred expressions via `Table.bind`.
- [ ] Standardize `.pipe()` / `.substitute()` rewrite hooks for macro expansion.
- [ ] Add tests that validate macro behavior across wide schemas.

---

### Scope 19: Parameterized rulepacks + param tables

**Objective:** Treat parameters as first-class rulepack inputs with explicit
specs, bindings, and persisted parameter artifacts.

**Pattern snippet**
```python
from __future__ import annotations

import ibis

from ibis_engine.params_bridge import IbisParamRegistry, ScalarParamSpec


def parameterized_edges(edges):
    registry = IbisParamRegistry()
    kind = registry.register(ScalarParamSpec(name="kind", dtype="string"))
    expr = edges.filter(edges.kind == kind)
    bindings = registry.bindings({"kind": "CALL"})
    return expr.to_pyarrow_batches(params=bindings)
```

**Target files**
- `src/ibis_engine/params_bridge.py`
- `src/ibis_engine/param_tables.py`
- `src/ibis_engine/runner.py`
- `src/obs/manifest.py`
- `src/obs/repro.py`

**Implementation checklist**
- [ ] Formalize parameter spec extraction for all rulepack entry points.
- [ ] Pass bindings through compile/execute/export surfaces.
- [ ] Persist param specs and param tables in run bundles.
- [ ] Add tests for missing/optional parameter behavior.

---

### Scope 20: SQL ingestion + IR round-trip artifacts (parse_sql + to_sql + decompile)

**Objective:** Support SQL → Ibis IR ingestion and persist IR round-trip artifacts
for reproducibility and agent review.

**Pattern snippet**
```python
from __future__ import annotations

import ibis


def parse_rule_sql(sql: str, *, schema: ibis.Schema):
    return ibis.parse_sql(sql, schema=schema)


def capture_ir_artifacts(expr, con):
    return {
        "sqlglot": con.compiler.to_sqlglot(expr),
        "sql": ibis.to_sql(expr, dialect="datafusion"),
        "decompiled": ibis.decompile(expr),
    }
```

**Target files**
- `src/ibis_engine/expr_compiler.py`
- `src/ibis_engine/query_compiler.py`
- `src/sqlglot_tools/optimizer.py`
- `src/obs/manifest.py`
- `src/obs/repro.py`

**Implementation checklist**
- [ ] Add a gated `parse_sql` entry path with schema requirements.
- [ ] Persist SQLGlot AST, rendered SQL, and decompiled IR artifacts.
- [ ] Add golden tests for supported SQL forms and round-trip fidelity.
- [ ] Document known limitations for parse_sql-supported subsets.

---

### Scope 21: Ibis caching policy for reusable intermediates

**Objective:** Introduce explicit caching rules for heavy intermediate tables
that are reused across multiple downstream products.

**Pattern snippet**
```python
from __future__ import annotations


def cached_pipeline(expr):
    with expr.cache() as cached:
        return cached.group_by("key").aggregate(count=cached.count())
```

**Target files**
- `src/ibis_engine/plan.py`
- `src/ibis_engine/runner.py`
- `src/engine/materialize.py`
- `src/obs/repro.py`

**Implementation checklist**
- [ ] Add a policy for when caching is allowed vs streaming-only outputs.
- [ ] Ensure cached tables are released deterministically.
- [ ] Record caching decisions in runtime artifacts.
- [ ] Add tests for cache reuse and release behavior.

---

### Scope 22: Backend namespace + materialization surface + join collision policy

**Objective:** Standardize `create_table`/`create_view`/`insert` usage for
named objects, avoid reliance on `.alias`, and enforce join collision policy.

**Pattern snippet**
```python
from __future__ import annotations


def register_view(con, name, expr, database: str):
    con.create_view(name, expr, database=database, overwrite=True)
    return con.table(name, database=database)


def insert_output(con, name, expr, database: str):
    con.create_table(name, schema=expr.schema(), database=database, overwrite=True)
    con.insert(name, expr, database=database, overwrite=True)


def stable_join(left, right):
    right_view = right.view()
    return left.join(right_view, [left.id == right_view.id], rname="{name}_r")
```

**Target files**
- `src/ibis_engine/sources.py`
- `src/ibis_engine/registry.py`
- `src/ibis_engine/runner.py`
- `src/obs/repro.py`

**Implementation checklist**
- [ ] Route naming through `create_table`/`create_view`/`insert` only.
- [ ] Ban `.alias` usage for correctness-critical paths.
- [ ] Standardize join collision policy (`lname`/`rname`) and self-join `view()`.
- [ ] Persist namespace/materialization actions in repro artifacts.

---

### Scope 23: Portability lane (unbind + has_operation gating)

**Objective:** Enable compilation to unbound IR and gate features via backend
operation support probes with fallback lanes.

**Pattern snippet**
```python
from __future__ import annotations

from ibis.expr.operations import Unnest


def prepare_portable(expr, backend):
    if not backend.has_operation(Unnest):
        msg = "Backend missing Unnest support."
        raise ValueError(msg)
    return expr.unbind()
```

**Target files**
- `src/ibis_engine/expr_compiler.py`
- `src/ibis_engine/backend.py`
- `src/engine/plan_policy.py`
- `src/obs/repro.py`

**Implementation checklist**
- [ ] Add `has_operation` gates for non-universal operators (unnest, windows).
- [ ] Provide fallback lanes (Arrow kernels or DataFusion-only) when unsupported.
- [ ] Persist support matrix decisions in run artifacts.

---

### Scope 24: Set operation assembly (union/intersect/difference)

**Objective:** Standardize multi-rule output assembly with explicit multiset vs
distinct semantics and schema alignment checks.

**Pattern snippet**
```python
from __future__ import annotations


def assemble_outputs(tables, *, distinct: bool = False):
    head, *rest = tables
    return head.union(*rest, distinct=distinct)
```

**Target files**
- `src/relspec/rules/graph.py`
- `src/relspec/rules/spec_tables.py`
- `src/ibis_engine/expr_compiler.py`

**Implementation checklist**
- [ ] Define union semantics per rulepack stage (all vs distinct).
- [ ] Add schema alignment and ordering checks before set operations.
- [ ] Add tests for union/intersect/difference consistency.

---

### Scope 25: SQLGlot policy engine v1 (pinned rules + dialect/generator policy)

**Objective:** Treat SQLGlot’s optimizer rule list, dialect mapping, generator
options, and normalization budgets as a versioned compiler contract.

**Pattern snippet**
```python
from __future__ import annotations

from dataclasses import dataclass

from sqlglot.optimizer import RULES, optimize


@dataclass(frozen=True)
class SqlGlotPolicy:
    read_dialect: str
    write_dialect: str
    rules: tuple[object, ...]
    generator: dict[str, object]
    normalization_distance: int


def apply_sqlglot_policy(expr, *, schema, policy: SqlGlotPolicy):
    optimized = optimize(expr, schema=schema, dialect=policy.read_dialect, rules=policy.rules)
    sql = optimized.sql(dialect=policy.write_dialect, **policy.generator)
    return optimized, sql
```

**Target files**
- `src/sqlglot_tools/optimizer.py`
- `src/sqlglot_tools/bridge.py`
- `src/engine/runtime_profile.py`
- `src/obs/manifest.py`
- `src/obs/repro.py`

**Implementation checklist**
- [ ] Pin `optimize(..., rules=...)` with a serialized rule list/hash.
- [ ] Maintain explicit `read_dialect` / `write_dialect` mappings in policy.
- [ ] Persist generator options (`pretty`, `identify`, `comments`, `unsupported_level`).
- [ ] Capture normalization budgets and error-level strictness in artifacts.
- [ ] Include policy hash in plan fingerprints and runtime profile.

---

### Scope 26: DataFusion SQL compatibility transforms lane

**Objective:** Apply a consistent SQLGlot transforms chain to normalize upstream
SQL and emit DataFusion-compatible SQL shapes.

**Pattern snippet**
```python
from __future__ import annotations

from sqlglot.transforms import (
    eliminate_qualify,
    ensure_bools,
    move_ctes_to_top_level,
    unnest_to_explode,
)


def datafusion_transforms(expr):
    expr = eliminate_qualify(expr)
    expr = move_ctes_to_top_level(expr)
    expr = unnest_to_explode(expr, unnest_using_arrays_zip=True)
    return ensure_bools(expr)
```

**Target files**
- `src/sqlglot_tools/optimizer.py`
- `src/sqlglot_tools/bridge.py`
- `src/datafusion_engine/bridge.py`

**Implementation checklist**
- [ ] Define a pinned transform list for DataFusion generation.
- [ ] Apply transforms at generation time (or as a dedicated rewrite stage).
- [ ] Add tests that compare transformed SQL to DataFusion parser acceptance.
- [ ] Persist transform list + version hash in repro artifacts.

---

### Scope 27: Qualification hardening + output alias enforcement

**Objective:** Make qualification failures deterministic and enforce aliased
outputs for stable lineage and diffs.

**Pattern snippet**
```python
from __future__ import annotations

from sqlglot.optimizer.qualify_columns import validate_qualify_columns
from sqlglot.optimizer.qualify_outputs import qualify_outputs
from sqlglot.optimizer.quote_identifiers import quote_identifiers


def qualify_strict(expr, *, sql: str | None = None, dialect: str):
    validate_qualify_columns(expr, sql=sql)
    qualify_outputs(expr)
    return quote_identifiers(expr, dialect=dialect, identify=True)
```

**Target files**
- `src/sqlglot_tools/optimizer.py`
- `src/relspec/rules/validation.py`
- `src/obs/repro.py`

**Implementation checklist**
- [ ] Require `expand_stars=True` and `validate_qualify_columns=True`.
- [ ] Enforce `qualify_outputs` so all output columns are aliased.
- [ ] Pass original SQL to validators for highlighted error messages.
- [ ] Store qualification failure payloads in diagnostics artifacts.

---

### Scope 28: Predicate normalization + projection/predicate pushdown policy

**Objective:** Apply predicate CNF normalization only when cheap enough, then
stabilize structure with pushdown passes.

**Pattern snippet**
```python
from __future__ import annotations

from sqlglot.optimizer.normalize import normalization_distance, normalize
from sqlglot.optimizer.pushdown_predicates import pushdown_predicates
from sqlglot.optimizer.pushdown_projections import pushdown_projections


def normalize_predicates(expr, *, max_distance: int):
    if normalization_distance(expr, max_=max_distance) <= max_distance:
        expr = normalize(expr)
    expr = pushdown_projections(expr)
    return pushdown_predicates(expr)
```

**Target files**
- `src/sqlglot_tools/optimizer.py`
- `src/ibis_engine/compiler_checkpoint.py`
- `src/relspec/rules/diagnostics.py`

**Implementation checklist**
- [ ] Compute normalization distance and skip normalization when too costly.
- [ ] Record normalization distance + applied/skipped flags in artifacts.
- [ ] Apply projection/predicate pushdowns after canonicalization.
- [ ] Add golden tests for predicate placement stability.

---

### Scope 29: Semantic diff + incremental invalidation policy

**Objective:** Use SQLGlot diff on canonical ASTs to classify breaking vs
non-breaking changes and drive incremental rebuild decisions.

**Pattern snippet**
```python
from __future__ import annotations

from sqlglot.diff import Insert, Keep, diff


def diff_is_breaking(left, right) -> bool:
    for op in diff(left, right):
        if isinstance(op, (Insert, Keep)):
            continue
        return True
    return False
```

**Target files**
- `src/ibis_engine/plan_diff.py`
- `src/sqlglot_tools/bridge.py`
- `src/relspec/rules/diagnostics.py`
- `src/obs/repro.py`

**Implementation checklist**
- [ ] Diff after canonicalization and normalization passes.
- [ ] Add special-case checks for windows and join-graph changes.
- [ ] Persist diff edit scripts and breaking/non-breaking decisions.
- [ ] Add tests covering incremental invalidation classification.

---

### Scope 30: Scope caching + lineage scaling

**Objective:** Cache SQLGlot scopes and reuse them for per-column lineage to
stabilize required-column extraction and reduce cost.

**Pattern snippet**
```python
from __future__ import annotations

from sqlglot.lineage import lineage
from sqlglot.optimizer import scope


def lineage_by_output(expr, outputs):
    scoped = scope.build_scope(expr)
    return {
        name: lineage(name, expr, scope=scoped, trim_selects=True, copy=False)
        for name in outputs
    }
```

**Target files**
- `src/ibis_engine/lineage.py`
- `src/sqlglot_tools/lineage.py`
- `src/sqlglot_tools/optimizer.py`

**Implementation checklist**
- [ ] Build scope once per query and reuse for lineage calls.
- [ ] Union base columns from lineage graphs for scan projections.
- [ ] Record lineage graph payloads per output column.

---

### Scope 31: Planner DAG + subplan dependency artifacts

**Objective:** Capture SQLGlot planner DAGs for CTE/subquery dependency analysis
and incremental rebuild decisions.

**Pattern snippet**
```python
from __future__ import annotations

from sqlglot.planner import Step


def plan_steps(expr):
    return Step.from_expression(expr)
```

**Target files**
- `src/sqlglot_tools/optimizer.py`
- `src/obs/manifest.py`
- `src/obs/repro.py`

**Implementation checklist**
- [ ] Persist planner DAG steps and subplan edges in run bundles.
- [ ] Track changed subplans for incremental invalidation heuristics.
- [ ] Add tests that DAG extraction is stable under canonicalization.

---

### Scope 32: Parsing strictness + tokenizer/dialect control plane

**Objective:** Make parse/generate strictness deterministic and resilient to
templated SQL and tokenizer variability.

**Pattern snippet**
```python
from __future__ import annotations

import sqlglot
from sqlglot import ErrorLevel


def parse_strict(sql: str, *, dialect: str):
    return sqlglot.parse_one(sql, dialect=dialect, error_level=ErrorLevel.IMMEDIATE)
```

**Target files**
- `src/sqlglot_tools/optimizer.py`
- `src/relspec/rules/validation.py`
- `src/engine/runtime_profile.py`

**Implementation checklist**
- [ ] Enforce strict error levels for compiler pipelines.
- [ ] Add templated SQL sanitization before parsing.
- [ ] Record tokenizer mode (`sqlglot[rs]` vs python) in profiles.
- [ ] Maintain engine → dialect mapping and persist chosen dialects.

---

### Scope 33: DataFusion dialect shim (when transforms aren’t enough)

**Objective:** Provide a SQLGlot dialect shim with pinned transforms and
generator policies when DataFusion requires custom syntax handling.

**Pattern snippet**
```python
from __future__ import annotations

from sqlglot.dialects.duckdb import DuckDB
from sqlglot.transforms import preprocess


class DataFusionDialect(DuckDB):
    TRANSFORMS = preprocess([])
```

**Target files**
- `src/sqlglot_tools/optimizer.py`
- `src/datafusion_engine/registry_bridge.py`

**Implementation checklist**
- [ ] Decide transforms lane vs dialect shim per surface.
- [ ] Register the shim dialect and use it for DataFusion SQL emission.
- [ ] Add tests for shim stability on critical rule outputs.

---

### Scope 34: Semantic rewrite harness (non-gating diagnostics)

**Objective:** Add a lightweight SQLGlot executor harness to compare original
and rewritten SQL for semantic equivalence on small fixtures.

**Pattern snippet**
```python
from __future__ import annotations

from sqlglot.executor import execute


def assert_equivalent(original_sql: str, rewritten_sql: str, *, tables):
    left = execute(original_sql, tables=tables)
    right = execute(rewritten_sql, tables=tables)
    if left != right:
        msg = "Rewrite altered results."
        raise AssertionError(msg)
```

**Target files**
- `scripts/sqlglot_semantic_harness.py`
- `docs/plans/advanced_integrations_implementation_plan.md`

**Implementation checklist**
- [ ] Keep harness non-gating and explicitly diagnostic-only.
- [ ] Use small deterministic fixtures for equivalence checks.
- [ ] Record harness results in debug artifacts when enabled.

---

### Scope 35: Arrow resource policy + telemetry (thread pools + memory pools)

**Objective:** Make Arrow CPU/IO pools and memory pool telemetry explicit in the
runtime profile and repro artifacts.

**Pattern snippet**
```python
from __future__ import annotations

import pyarrow as pa


def snapshot_arrow_resources() -> dict[str, object]:
    pool = pa.default_memory_pool()
    return {
        "pyarrow_version": pa.__version__,
        "cpu_threads": pa.cpu_count(),
        "io_threads": pa.io_thread_count(),
        "memory_pool": pool.backend_name,
        "bytes_allocated": pool.bytes_allocated(),
        "max_memory": pool.max_memory(),
    }
```

**Target files**
- `src/arrowdsl/core/context.py`
- `src/engine/runtime_profile.py`
- `src/obs/manifest.py`
- `src/obs/repro.py`

**Implementation checklist**
- [ ] Capture Arrow CPU/IO thread pool settings in runtime profiles.
- [ ] Snapshot memory pool backend + bytes/peak per run.
- [ ] Expose debug toggle for allocator logging in repro mode.

---

### Scope 36: Scanner provenance + TaggedRecordBatch policy

**Objective:** Standardize batch provenance capture and schema snapshots via
`scan_batches()` and `TaggedRecordBatch`.

**Pattern snippet**
```python
from __future__ import annotations

import pyarrow.dataset as ds


def scan_with_provenance(dataset: ds.Dataset):
    scanner = dataset.scanner()
    for tagged in scanner.scan_batches():
        yield {
            "batch": tagged.record_batch,
            "fragment": tagged.fragment,
        }
```

**Target files**
- `src/arrowdsl/plan/scan_telemetry.py`
- `src/arrowdsl/plan/scan_builder.py`
- `src/obs/repro.py`

**Implementation checklist**
- [ ] Capture dataset_schema/projected_schema in scan artifacts.
- [ ] Record fragment paths, partition expressions, and row group counts.
- [ ] Use special fields only in projections, not filter expressions.

---

### Scope 37: Parquet fragment pruning + scan options profile

**Objective:** Expose Parquet fragment metadata caching, row-group pruning, and
read/scan options as runtime policy.

**Pattern snippet**
```python
from __future__ import annotations

import pyarrow.dataset as ds


def fragment_subset(fragment: ds.ParquetFileFragment, predicate):
    fragment.ensure_complete_metadata()
    return fragment.subset(predicate)
```

**Target files**
- `src/arrowdsl/plan/scan_builder.py`
- `src/arrowdsl/plan/scan_telemetry.py`
- `src/arrowdsl/io/parquet.py`
- `src/engine/runtime_profile.py`

**Implementation checklist**
- [ ] Add ParquetReadOptions + ParquetFragmentScanOptions to runtime profile.
- [ ] Cache fragment metadata for repeated scans (`cache_metadata=True`).
- [ ] Support row-group pruning via `subset()` where predicates allow.
- [ ] Persist scan options and row-group stats in repro bundles.

---

### Scope 38: Writer determinism + metadata sidecars

**Objective:** Encode deterministic ordering and metadata sidecar policies for
dataset writes, including `_common_metadata` / `_metadata`.

**Pattern snippet**
```python
from __future__ import annotations

import pyarrow.dataset as ds


def write_with_metadata(reader, base_dir: str, file_visitor):
    ds.write_dataset(
        reader,
        base_dir=base_dir,
        format="parquet",
        preserve_order=False,
        file_visitor=file_visitor,
    )
```

**Target files**
- `src/arrowdsl/io/parquet.py`
- `src/arrowdsl/plan/ordering_policy.py`
- `src/obs/repro.py`

**Implementation checklist**
- [ ] Require explicit sorting when order is semantically meaningful.
- [ ] Gate `_metadata` emission for scale; prefer `_common_metadata` when large.
- [ ] Persist writer ordering policy and sidecar decisions in artifacts.

---

### Scope 39: Arrow kernel registry + scalar UDF lane

**Objective:** Formalize a compute-kernel registry and support Arrow scalar UDFs
as a middle lane between kernels and DataFusion UDFs.

**Pattern snippet**
```python
from __future__ import annotations

import pyarrow.compute as pc


def list_kernels() -> tuple[str, ...]:
    return tuple(sorted(pc.list_functions()))
```

**Target files**
- `src/arrowdsl/compute/registry.py`
- `src/arrowdsl/compute/kernel_utils.py`
- `src/obs/repro.py`

**Implementation checklist**
- [ ] Snapshot available kernels and registered scalar UDFs in repro bundles.
- [ ] Add a registration surface for scalar Arrow UDFs.
- [ ] Prefer kernel lanes before Python UDFs for performance.

---

### Scope 40: Arrow interchange contracts (PyCapsule + dataframe protocol)

**Objective:** Accept Arrow-like objects via PyCapsule and dataframe
interchange protocols as ingestion boundaries.

**Pattern snippet**
```python
from __future__ import annotations

import pyarrow as pa


def to_arrow_table(obj) -> pa.Table:
    return pa.table(obj)
```

**Target files**
- `src/arrowdsl/core/interop.py`
- `src/arrowdsl/plan/source_normalize.py`
- `src/ibis_engine/io_bridge.py`

**Implementation checklist**
- [ ] Accept `__arrow_c_stream__` / `__arrow_c_array__` providers in ingestion.
- [ ] Support `__dataframe__` interchange for table-like inputs.
- [ ] Document single-consumption/lifetime rules for PyCapsule inputs.

---

### Scope 41: Acero streaming outputs + order-by boundary policy

**Objective:** Prefer `Declaration.to_reader()` for large outputs and treat
order-by nodes as explicit materialization boundaries.

**Pattern snippet**
```python
from __future__ import annotations

import pyarrow.acero as acero


def stream_acero(plan: acero.Declaration):
    return plan.to_reader(use_threads=True)
```

**Target files**
- `src/arrowdsl/compile/plan_compiler.py`
- `src/arrowdsl/exec/runtime.py`
- `src/obs/repro.py`

**Implementation checklist**
- [ ] Standardize `to_reader()` for large Acero outputs.
- [ ] Capture `implicit_ordering` / `require_sequenced_output` in artifacts.
- [ ] Mark order-by nodes as pipeline breakers in plan telemetry.

---

### Scope 42: IPC artifacts for repro bundles

**Objective:** Persist IPC streams for scanned/produced batches to enable
byte-for-byte repro without engine dependencies.

**Pattern snippet**
```python
from __future__ import annotations

from arrowdsl.io.ipc import write_table_ipc_stream


def write_ipc(reader, path: str) -> str:
    return write_table_ipc_stream(reader, path)
```

**Target files**
- `src/arrowdsl/io/ipc.py`
- `src/obs/repro.py`
- `src/obs/manifest.py`

**Implementation checklist**
- [ ] Add optional IPC dump for scan/output readers.
- [ ] Record IPC options and schema metadata alongside dumps.
- [ ] Add replay helpers using `ipc.open_stream`.

---

### Scope 43: Extension types + schema metadata contracts

**Objective:** Encode domain semantics via Arrow extension types and metadata,
and include them in schema fingerprints.

**Pattern snippet**
```python
from __future__ import annotations

import pyarrow as pa


def register_ext(ext_type: pa.ExtensionType) -> None:
    pa.register_extension_type(ext_type)
```

**Target files**
- `src/arrowdsl/schema/schema.py`
- `src/arrowdsl/schema/serialization.py`
- `src/obs/manifest.py`

**Implementation checklist**
- [ ] Register extension types used in schemas at startup.
- [ ] Persist extension metadata and field metadata in fingerprints.
- [ ] Provide deterministic flattening with `Field.flatten()` where needed.

---

### Scope 44: Dataset/table relational fallback lanes

**Objective:** Provide Arrow-native joins/sorts for small intermediate products
or fallback lanes when DataFusion is unavailable.

**Pattern snippet**
```python
from __future__ import annotations

import pyarrow.dataset as ds


def join_small(left: ds.Dataset, right: ds.Dataset):
    return left.join(right, keys="id", right_suffix="_r")
```

**Target files**
- `src/arrowdsl/compute/kernels.py`
- `src/arrowdsl/exec/runtime.py`
- `src/arrowdsl/plan/planner.py`

**Implementation checklist**
- [ ] Add dataset/table join/asof fallback paths for small outputs.
- [ ] Enforce explicit ordering or `use_threads=False` on canonical outputs.
- [ ] Record fallback usage in artifacts.

---

### Scope 45: Filesystem plugin bridge + IO primitives

**Objective:** Support filesystem shims and IO primitives for artifact packs
with zero-copy mmap and compression pipelines.

**Pattern snippet**
```python
from __future__ import annotations

import pyarrow as pa


def mmap_file(path: str):
    return pa.memory_map(path, "r")
```

**Target files**
- `src/arrowdsl/plan/source_normalize.py`
- `src/arrowdsl/io/parquet.py`
- `src/storage/io.py`

**Implementation checklist**
- [ ] Add `SubTreeFileSystem` / `PyFileSystem` / `FSSpecHandler` bridges.
- [ ] Expose memory-mapped IO for large local artifacts.
- [ ] Add compressed stream helpers for large artifact packs.

---

### Deliverables Summary

- Canonical SQLGlot AST identity + lineage for required columns.
- SQLGlot AST execution via `raw_sql`, with cursor safety + schema-bearing SQL.
- Unified runtime profile v1 (DataFusion + Arrow + SQLGlot + Ibis) and hash.
- Dataset discovery with factory inspection + `_metadata` fast path.
- Unified writer contract spanning Arrow + DataFusion + Ibis write surfaces.
- Schema fingerprint fidelity with metadata + DDL hash.
- Unified nested data contract with deterministic offsets and constructors.
- Cross-lane FunctionSpec registry including UDAF/UDWF/UDTF + UDF ladder.
- DataFusion plan artifacts + rehydration.
- Join algorithm controls + listing table policy artifacts.
- Gated DML sink lane (COPY/INSERT) and input plugin resolution.
- MemTable shaping, prepared statements, and catalog snapshots.
- Deterministic ordering contract + winner selection templates.
- Ibis macro DSL (deferred/selectors/bind/pipe) for rulepacks.
- Parameterized rulepacks + param tables.
- SQL ingestion + IR round-trip artifacts (`parse_sql`, `to_sql`, `decompile`).
- Ibis caching policy for reusable intermediates.
- Backend namespace/materialization rules + join collision policy.
- Portability lane with `unbind` + `has_operation` gating.
- Set operation assembly (union/intersect/difference) with schema checks.
- SQLGlot policy engine v1 with pinned rules and generator policy.
- DataFusion SQL transforms lane + optional dialect shim.
- Qualification hardening + output alias enforcement + error payloads.
- Predicate normalization + pushdown policy with distance diagnostics.
- Semantic diff-driven invalidation policy for incremental rebuilds.
- Scope caching + lineage scaling and required-column extraction.
- Planner DAG artifacts for subplan dependency tracking.
- Parsing strictness + tokenizer/dialect mapping + template sanitization.
- Semantic rewrite harness for non-gating equivalence checks.
- Arrow resource policy + memory telemetry + thread pool control.
- Scanner provenance + TaggedRecordBatch artifacts.
- Parquet fragment pruning + scan option profiles.
- Writer determinism + metadata sidecar policy.
- Arrow compute kernel registry + scalar UDF lane.
- Arrow interchange contracts (PyCapsule + dataframe protocol).
- Acero streaming outputs + order-by boundary policy.
- IPC repro artifacts for batch-level replay.
- Extension types + schema metadata contracts.
- Dataset/table relational fallback lanes.
- Filesystem plugin bridge + IO primitives (mmap + compression).
