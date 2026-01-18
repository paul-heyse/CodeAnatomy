Below is a comprehensive implementation plan for the **high‑impact** integrations
identified from `docs/python_library_reference/additional_advanced_integrations.md`,
plus additional DataFusion deployment enhancements sourced from the DataFusion
reference docs (SQL policy, stats, caching, catalogs, function surfaces, and
system‑builder extension hooks).

Each scope item includes representative code snippets, target files, and a
granular implementation checklist.

---

## Scope A1: Parquet ordering handshake (write metadata + verify)

**Objective:** Persist sort order in Parquet row group metadata and validate it
in artifacts so DataFusion ordering analysis can trust it end‑to‑end.

**Pattern snippet**
```python
from __future__ import annotations

import pyarrow.parquet as pq

from arrowdsl.plan.ordering_policy import ordering_keys_for_schema


def sorting_columns_for_schema(schema) -> list[pq.SortingColumn]:
    keys = ordering_keys_for_schema(schema)
    return [
        pq.SortingColumn(
            column_index=i,
            descending=order == "desc",
            nulls_first=False,
        )
        for i, (name, order) in enumerate(keys)
    ]
```

**Target files**
- `src/arrowdsl/io/parquet.py`
- `src/arrowdsl/plan/orderering_policy.py`
- `src/obs/repro.py`
- `src/obs/manifest.py`
- `tests/unit/test_parquet_sorting_metadata.py`

**Implementation checklist**
- [x] Derive `sorting_columns` from explicit ordering keys at write time.
- [ ] Emit sorting metadata only when order is canonical or explicitly provided.
- [x] Capture row group `sorting_columns()` in write artifacts.
- [x] Add tests validating that metadata appears when keys are present.
- [ ] Add tests that no metadata is written when keys are absent.

---

## Scope A2: Contract‑driven external table DDL + format options precedence

**Objective:** Generate DataFusion DDL from contracts and enforce a clear
precedence model (session defaults < table options < statement overrides).

**Pattern snippet**
```python
from __future__ import annotations

from schema_spec.specs import ExternalTableConfig, TableSchemaSpec


def external_table_sql(spec: TableSchemaSpec, *, location: str) -> str:
    config = ExternalTableConfig(
        location=location,
        file_format="parquet",
        table_name=spec.name,
        dialect="datafusion_ext",
        options={"compression": "zstd"},
        partitioned_by=tuple(spec.partition_cols),
        file_sort_order=tuple(spec.key_fields),
    )
    return spec.to_create_external_table_sql(config)
```

**Target files**
- `src/schema_spec/specs.py`
- `src/datafusion_engine/registry_bridge.py`
- `src/datafusion_engine/bridge.py`
- `src/datafusion_engine/runtime.py`
- `tests/unit/test_contract_external_table_ddl.py`

**Implementation checklist**
- [x] Emit `OPTIONS(...)`, `PARTITIONED BY`, and `WITH ORDER` from contracts.
- [x] Encode “session defaults” in runtime profile settings.
- [x] Encode “table defaults” via DDL `OPTIONS`.
- [x] Encode “statement overrides” via `COPY ... OPTIONS(...)`.
- [ ] Add tests that DDL contains expected clauses.
- [ ] Add tests that options precedence is deterministic.

---

## Scope A3: Manifest‑first ingestion (row group stats + pruning diagnostics)

**Objective:** Persist compact per‑file metadata and row‑group stats to debug
pruning and validate `_metadata` sidecar creation at scale.

**Pattern snippet**
```python
from __future__ import annotations

import pyarrow.parquet as pq


def row_group_stats(path: str) -> list[dict[str, object]]:
    pf = pq.ParquetFile(path)
    stats: list[dict[str, object]] = []
    for i in range(pf.num_row_groups):
        rg = pf.metadata.row_group(i)
        stats.append(rg.to_dict())
    return stats
```

**Target files**
- `src/arrowdsl/io/parquet.py`
- `src/arrowdsl/plan/metrics.py`
- `src/obs/repro.py`
- `tests/unit/test_manifest_row_group_stats.py`

**Implementation checklist**
- [x] Extend `file_visitor` to collect per‑file stats (bounded).
- [x] Persist row‑group metadata for key columns only.
- [x] Record `_metadata`/`_common_metadata` emission in artifacts.
- [ ] Add tests for stats capture and scale gating.

---

## Scope A4: Async streaming surfaces (DataFusion `__aiter__`)

**Objective:** Provide async batch streaming for DF results without forcing
materialization, enabling backpressure‑aware sinks.

**Pattern snippet**
```python
from __future__ import annotations

async def iter_batches(df):
    async for batch in df:
        yield batch.to_pyarrow()
```

**Target files**
- `src/datafusion_engine/bridge.py`
- `src/ibis_engine/runner.py`
- `tests/integration/test_async_streaming.py`

**Implementation checklist**
- [x] Add async generator helper for DataFusion DataFrame.
- [ ] Wire async batch streaming into writer surfaces (optional).
- [ ] Add tests that async iteration yields batches.

---

## Scope A5: Substrait cross‑engine validation lane

**Objective:** Execute DataFusion Substrait plans in PyArrow and compare results
as a non‑gating correctness canary.

**Pattern snippet**
```python
from __future__ import annotations

import pyarrow.substrait as substrait


def run_substrait(plan_bytes: bytes):
    return substrait.run_query(plan_bytes, use_threads=True)
```

**Target files**
- `src/datafusion_engine/bridge.py`
- `src/obs/repro.py`
- `scripts/substrait_validation_harness.py`
- `tests/integration/test_substrait_cross_validation.py`

**Implementation checklist**
- [ ] Emit Substrait bytes for logical plans.
- [ ] Run PyArrow Substrait engine on fixtures (non‑gating).
- [ ] Compare output tables (row count + hash).
- [ ] Record failures to repro artifacts.

---

## Scope A6: Identifier case + quoting policy

**Objective:** Align SQLGlot quoting with DataFusion identifier behavior to
avoid case‑sensitive “column not found” errors.

**Pattern snippet**
```python
from __future__ import annotations

from sqlglot import Expression
from sqlglot.optimizer.normalize_identifiers import normalize_identifiers
from sqlglot.optimizer.qualify_columns import quote_identifiers


def normalize_case(
    expr: Expression,
    *,
    dialect: str,
    store_original: bool,
) -> Expression:
    normalized = normalize_identifiers(
        expr,
        dialect=dialect,
        store_original_column_identifiers=store_original,
    )
    return quote_identifiers(normalized, dialect=dialect, identify=True)
```

**Target files**
- `src/sqlglot_tools/optimizer.py`
- `src/engine/runtime_profile.py`
- `src/relspec/rules/validation.py`
- `tests/unit/test_identifier_quoting.py`

**Implementation checklist**
- [ ] Detect mixed‑case schemas and force quoting.
- [ ] Preserve original identifiers for diagnostics artifacts.
- [ ] Persist quoting policy in runtime profile artifacts.
- [ ] Add CI test for capitalized field lookup.

---

## Scope A7: UnionDataset + one‑shot dataset handling

**Objective:** Support dataset composition and enforce single‑scan semantics
for in‑memory datasets created from RecordBatchReader.

**Pattern snippet**
```python
from __future__ import annotations

import pyarrow.dataset as ds


def union_dataset(datasets: list[ds.Dataset]) -> ds.Dataset:
    return ds.dataset(datasets)
```

**Target files**
- `src/arrowdsl/plan/source_normalize.py`
- `src/arrowdsl/plan/scan_io.py`
- `tests/unit/test_union_dataset.py`

**Implementation checklist**
- [x] Add helper to create UnionDataset from multiple sources.
- [x] Detect one‑shot InMemoryDataset sources and guard re‑scan.
- [ ] Add tests for union schema alignment and one‑shot behavior.

---

## Scope A8: FilenamePartitioning for basename templates

**Objective:** Parse run/build metadata from filenames and expose them as
partition columns without a directory partition layout.

**Pattern snippet**
```python
from __future__ import annotations

import pyarrow.dataset as ds


def filename_partitioning(schema):
    return ds.FilenamePartitioning(schema)
```

**Target files**
- `src/arrowdsl/io/parquet.py`
- `src/arrowdsl/plan/source_normalize.py`
- `tests/integration/test_filename_partitioning.py`

**Implementation checklist**
- [x] Add filename partitioning support to read paths.
- [x] Align basename template tokens with schema fields.
- [ ] Add test verifying extracted partition keys.

---

## Scope A9: DataFusion universal Arrow import standardization

**Objective:** Centralize DataFusion in‑memory ingestion surfaces and record
partition layout as an artifact.

**Pattern snippet**
```python
from __future__ import annotations

from datafusion import SessionContext


def from_arrow_any(ctx: SessionContext, obj, *, name: str):
    return ctx.from_arrow(obj, name=name)
```

**Target files**
- `src/datafusion_engine/bridge.py`
- `src/datafusion_engine/runtime.py`
- `src/obs/repro.py`
- `tests/unit/test_from_arrow_ingest.py`

**Implementation checklist**
- [ ] Add a single ingestion helper for `from_arrow`/`from_pydict`/`from_pylist`.
- [ ] Record partitioning strategy for record batch ingestion.
- [ ] Add tests for each ingestion method.

---

## Scope A10: View registry snapshot

**Objective:** Persist view names and defining SQL/plan payloads to make
cross‑module plan reuse observable and reproducible.

**Pattern snippet**
```python
from __future__ import annotations

def record_view(registry: dict[str, str], name: str, sql: str) -> None:
    registry[name] = sql
```

**Target files**
- `src/datafusion_engine/bridge.py`
- `src/datafusion_engine/runtime.py`
- `src/obs/repro.py`
- `tests/unit/test_view_registry_snapshot.py`

**Implementation checklist**
- [ ] Capture view names and definition SQL.
- [ ] Persist registry snapshot in run bundles.
- [ ] Add test asserting snapshot stability for repeated registration.

---

## Scope A11: PyCapsule UDF/provider registry

**Objective:** Track PyCapsule‑backed UDFs and TableProviders in registry
fingerprints, including capability hints for pushdown behavior.

**Pattern snippet**
```python
from __future__ import annotations

from typing import Protocol

from datafusion.user_defined import ScalarUDF


class TableProviderCapsule(Protocol):
    def __datafusion_table_provider__(self) -> object: ...


def load_udf_from_capsule(capsule: object) -> ScalarUDF:
    return ScalarUDF.from_pycapsule(capsule)


def provider_capsule_id(provider: TableProviderCapsule) -> str:
    return repr(provider.__datafusion_table_provider__())
```

**Target files**
- `src/datafusion_engine/udf_registry.py`
- `src/datafusion_engine/registry_bridge.py`
- `src/engine/function_registry.py`
- `src/obs/repro.py`
- `src/obs/manifest.py`
- `tests/integration/test_pycapsule_udf_registry.py`
- `tests/integration/test_pycapsule_provider_registry.py`

**Implementation checklist**
- [ ] Add registry hooks for PyCapsule UDFs and TableProviders.
- [ ] Capture provider capability hints (projection/predicate/limit pushdown).
- [ ] Include capsule identifiers in function registry fingerprints.
- [ ] Persist snapshots in run bundles and manifest notes.
- [ ] Add minimal query tests for UDFs and providers.

---

## Scope A12: Unified streaming adapter (anything → RecordBatchReader)

**Objective:** Standardize a single adapter for DataFusion/Ibis/PyArrow inputs,
making streaming the default and enforcing one‑shot semantics.

**Pattern snippet**
```python
from __future__ import annotations

import pyarrow as pa
import pyarrow.dataset as ds


def to_reader(obj: object, *, schema: pa.Schema | None = None) -> pa.RecordBatchReader:
    if isinstance(obj, pa.RecordBatchReader):
        return obj
    if hasattr(obj, "__arrow_c_stream__"):
        return pa.RecordBatchReader.from_stream(obj, schema=schema)
    to_batches = getattr(obj, "to_pyarrow_batches", None)
    if callable(to_batches):
        return to_batches()
    if isinstance(obj, ds.Scanner):
        return obj.to_reader()
    msg = f"Unsupported streaming source: {type(obj)}."
    raise TypeError(msg)
```

**Target files**
- `src/arrowdsl/core/streaming.py`
- `src/datafusion_engine/bridge.py`
- `src/ibis_engine/io_bridge.py`
- `src/arrowdsl/plan/source_normalize.py`
- `tests/unit/test_streaming_adapter.py`

**Implementation checklist**
- [ ] Introduce a single `to_reader` adapter with explicit one‑shot semantics.
- [ ] Update DataFusion/Ibis/Arrow ingestion surfaces to call `to_reader`.
- [ ] Enforce schema negotiation only when supported (no reshaping).
- [ ] Add tests for DataFusion DF, Ibis expr, Scanner, and capsule sources.

---

## Scope A13: One‑shot stream scanning via `Scanner.from_batches`

**Objective:** Apply scan policy (projection/filter/readahead) to one‑shot
streams before writing or execution.

**Pattern snippet**
```python
from __future__ import annotations

import pyarrow as pa
import pyarrow.dataset as ds


def scanner_from_reader(
    reader: pa.RecordBatchReader,
    *,
    schema: pa.Schema,
    batch_size: int | None,
) -> ds.Scanner:
    return ds.Scanner.from_batches(reader, schema=schema, batch_size=batch_size)
```

**Target files**
- `src/arrowdsl/plan/source_normalize.py`
- `src/arrowdsl/plan/scan_builder.py`
- `src/arrowdsl/plan/query.py`
- `tests/unit/test_scan_from_batches.py`

**Implementation checklist**
- [ ] Wrap one‑shot streams in `Scanner.from_batches` when scan policy is needed.
- [ ] Prevent re‑scans of one‑shot sources (explicit guard/error).
- [ ] Add tests that scan policy applies to streaming sources without materialization.

---

## Scope A14: Lineage‑driven scan projection + pushdown evidence

**Objective:** Use SQLGlot lineage to minimize scan columns and record
required‑vs‑scanned evidence in scan telemetry artifacts.

**Pattern snippet**
```python
from __future__ import annotations

from ibis_engine.lineage import required_columns_by_table


def required_scan_columns(expr, *, backend, dataset: str) -> tuple[str, ...]:
    required = required_columns_by_table(expr, backend=backend)
    return tuple(required.get(dataset, ()))
```

**Target files**
- `src/arrowdsl/plan/scan_builder.py`
- `src/arrowdsl/plan/query.py`
- `src/relspec/compiler.py`
- `src/arrowdsl/plan/scan_telemetry.py`
- `tests/unit/test_required_columns_scan.py`

**Implementation checklist**
- [ ] Thread `required_columns` into `QuerySpec.scan_columns`.
- [ ] Ensure filter‑only columns are included in scan projections.
- [ ] Persist required vs scanned columns in telemetry artifacts.
- [ ] Add tests verifying projection shrinkage and evidence payloads.

---

## Scope A15: DataFusion Unparser artifacts (plan → SQL)

**Objective:** Persist minimal SQL repros from optimized logical plans using the
DataFusion Unparser.

**Pattern snippet**
```python
from __future__ import annotations

from datafusion.unparser import Unparser


def plan_to_sql(plan) -> str:
    return Unparser().plan_to_sql(plan)
```

**Target files**
- `src/datafusion_engine/bridge.py`
- `src/obs/repro.py`
- `tests/unit/test_datafusion_unparser_artifacts.py`

**Implementation checklist**
- [ ] Capture unparsed SQL when plan artifacts are collected.
- [ ] Store unparser output (or error) in run bundle artifacts.
- [ ] Add tests for deterministic unparser payloads.

---

## Scope A16: Cross‑lane function registry completion

**Objective:** Extend the unified registry to include PyArrow compute functions
and PyCapsule identifiers, making the registry fingerprint fully semantic.

**Pattern snippet**
```python
from __future__ import annotations

import pyarrow.compute as pc


def function_registry_snapshot() -> dict[str, object]:
    return {"pyarrow_compute": sorted(pc.list_functions())}
```

**Target files**
- `src/engine/function_registry.py`
- `src/datafusion_engine/udf_registry.py`
- `src/arrowdsl/compute/registry.py`
- `src/obs/repro.py`
- `tests/unit/test_function_registry_snapshot.py`

**Implementation checklist**
- [ ] Extend registry payload with PyArrow compute + PyCapsule identifiers.
- [ ] Incorporate new fields into registry fingerprint.
- [ ] Persist registry snapshot in run bundles.
- [ ] Add tests for sorted, stable snapshots.

---

## Scope A17: Nested data dispatcher across lanes (Ibis/DF/kernel)

**Objective:** Provide a single explode/unnest dispatcher that preserves
`idx`/`keep_empty` semantics across Ibis, DataFusion, and kernel lanes.

**Pattern snippet**
```python
from __future__ import annotations

import ibis

from arrowdsl.compute.expr_core import ExplodeSpec


def explode_ibis(table: ibis.Table, *, spec: ExplodeSpec) -> ibis.Table:
    return table.unnest(spec.list_col, offset=spec.idx_col, keep_empty=spec.keep_empty)
```

**Target files**
- `src/arrowdsl/compute/explode_dispatch.py`
- `src/arrowdsl/compute/kernels.py`
- `src/relspec/compiler.py`
- `src/ibis_engine/registry.py`
- `tests/unit/test_explode_dispatch.py`

**Implementation checklist**
- [ ] Add a dispatcher that chooses Ibis, DataFusion, or kernel lane per spec.
- [ ] Enforce deterministic ordering (`parent_key`, `idx`) after explode.
- [ ] Expose array/map/struct constructors for nested payload normalization.
- [ ] Add tests for empty/null lists, list<struct> + unpack, and map entries.

---

## Scope A18: SQL surface policy matrix + parameter safety

**Objective:** Standardize SQL policy presets and enforce safe parameter binding
for DataFusion SQL execution.

**Pattern snippet**
```python
from __future__ import annotations

from collections.abc import Mapping

from datafusion_engine.compile_options import DataFusionSqlPolicy
from ibis_engine.params_bridge import datafusion_param_bindings

SQL_POLICIES = {
    "read_only": DataFusionSqlPolicy(),
    "service": DataFusionSqlPolicy(
        allow_ddl=False,
        allow_dml=False,
        allow_statements=False,
    ),
    "admin": DataFusionSqlPolicy(
        allow_ddl=True,
        allow_dml=True,
        allow_statements=True,
    ),
}


def sanitize_identifiers(identifiers: Mapping[str, str]) -> dict[str, str]:
    return {key: value for key, value in identifiers.items() if value.isidentifier()}


def execute_safe_sql(ctx, sql: str, *, policy_name: str, params: dict[str, object]):
    policy = SQL_POLICIES[policy_name]
    bindings = datafusion_param_bindings(params)
    return ctx.sql_with_options(sql, policy.to_sql_options(), param_values=bindings)
```

**Target files**
- `src/datafusion_engine/compile_options.py`
- `src/datafusion_engine/bridge.py`
- `src/datafusion_engine/runtime.py`
- `src/obs/repro.py`
- `tests/unit/test_sql_policy_matrix.py`
- `tests/unit/test_sql_param_binding.py`

**Implementation checklist**
- [x] Add read‑only/service/admin SQL policy presets.
- [x] Enforce `param_values` for user‑provided scalar inputs.
- [ ] Restrict named‑param identifier substitution to allowlisted identifiers.
- [ ] Record policy name and parameter mode in artifacts.
- [ ] Add tests for policy enforcement and parameter safety.

---

## Scope A19: DataFusion function catalog snapshot

**Objective:** Persist the full DataFusion function surface (built‑ins + UDFs) as
run artifacts for reproducibility and registry alignment.

**Pattern snippet**
```python
from __future__ import annotations

from datafusion import SessionContext


def function_catalog_snapshot(ctx: SessionContext) -> list[dict[str, object]]:
    table = ctx.sql("SHOW FUNCTIONS").to_arrow_table()
    return table.to_pylist()
```

**Target files**
- `src/datafusion_engine/runtime.py`
- `src/hamilton_pipeline/modules/outputs.py`
- `src/obs/repro.py`
- `src/obs/manifest.py`
- `tests/unit/test_function_catalog_snapshot.py`

**Implementation checklist**
- [ ] Capture `SHOW FUNCTIONS` output as a stable snapshot.
- [ ] Optionally enrich with `information_schema.routines` when enabled.
- [ ] Persist snapshot and hash in run bundle artifacts.
- [ ] Add tests for snapshot stability and ordering.

---

## Scope A20: Per‑table statistics policy + registration latency controls

**Objective:** Control stats collection and metadata fetch concurrency per
dataset and record the policy in scan artifacts.

**Pattern snippet**
```python
from __future__ import annotations


def apply_stats_policy(
    ctx,
    *,
    collect_statistics: bool | None,
    meta_fetch_concurrency: int | None,
) -> None:
    if collect_statistics is not None:
        value = str(collect_statistics).lower()
        ctx.sql(f"SET datafusion.execution.collect_statistics = '{value}'").collect()
    if meta_fetch_concurrency is not None:
        ctx.sql(
            "SET datafusion.execution.meta_fetch_concurrency = "
            f"'{meta_fetch_concurrency}'"
        ).collect()
```

**Target files**
- `src/schema_spec/system.py`
- `src/datafusion_engine/registry_bridge.py`
- `src/datafusion_engine/runtime.py`
- `src/obs/repro.py`
- `tests/unit/test_datafusion_stats_policy.py`

**Implementation checklist**
- [x] Add `collect_statistics` and `meta_fetch_concurrency` to scan options.
- [x] Apply stats settings before external table registration.
- [x] Record stats policy in registry/scan telemetry artifacts.
- [ ] Add tests for per‑dataset stats policy application.

---

## Scope A21: Listing cache lifecycle rules (mutable prefixes)

**Objective:** Prevent stale ListingTable scans by encoding mutability
policies and refreshing cached listings when required.

**Pattern snippet**
```python
from __future__ import annotations

from collections.abc import Callable


def refresh_listing_table(ctx, *, name: str, register: Callable[[], None]) -> None:
    ctx.deregister_table(name)
    register()
```

**Target files**
- `src/schema_spec/system.py`
- `src/datafusion_engine/registry_bridge.py`
- `src/datafusion_engine/runtime.py`
- `src/obs/repro.py`
- `tests/integration/test_listing_cache_lifecycle.py`

**Implementation checklist**
- [x] Add dataset mutability flag (immutable vs mutable prefixes).
- [x] Configure `list_files_cache_ttl` via profile for mutable datasets.
- [x] Force provider refresh when mutable datasets are re‑registered.
- [ ] Record listing cache policy + refresh events in artifacts.
- [ ] Add integration tests covering refresh behavior.

---

## Scope A22: DataFusion write policy lane (DataFrame + Parquet options)

**Objective:** Standardize DataFusion write options and persist write‑policy
metadata for reproducible outputs.

**Pattern snippet**
```python
from __future__ import annotations

from datafusion import DataFrameWriteOptions, ParquetWriterOptions


def default_write_policy():
    return (
        DataFrameWriteOptions(
            partition_by=["repo"],
            single_file_output=False,
        ),
        ParquetWriterOptions(
            compression="zstd(5)",
            statistics_enabled="page",
        ),
    )
```

**Target files**
- `src/datafusion_engine/bridge.py`
- `src/datafusion_engine/runtime.py`
- `src/schema_spec/specs.py`
- `src/obs/repro.py`
- `tests/integration/test_datafusion_write_policy.py`

**Implementation checklist**
- [x] Add a write‑policy model for DataFusion outputs.
- [x] Map write policy to `DataFrameWriteOptions` and `ParquetWriterOptions`.
- [x] Record output layout + writer settings in run bundle artifacts.
- [ ] Add tests for write policy serialization and defaults.

---

## Scope A23: SQL `CREATE FUNCTION` lane via FunctionFactory (system builder)

**Objective:** Enable DataFusion `CREATE FUNCTION` by installing a FunctionFactory
hook and record capability/diagnostics in artifacts.

**Pattern snippet**
```python
from __future__ import annotations

from datafusion_engine.function_factory import install_function_factory
from datafusion_engine.runtime import DataFusionRuntimeProfile


def install_factory(ctx) -> None:
    install_function_factory(ctx)


profile = DataFusionRuntimeProfile(
    enable_function_factory=True,
    function_factory_hook=install_factory,
)
```

**Target files**
- `src/datafusion_engine/function_factory.py`
- `src/datafusion_engine/runtime.py`
- `src/obs/repro.py`
- `tests/integration/test_create_function_factory.py`

**Implementation checklist**
- [ ] Ensure a FunctionFactory hook is installed when enabled.
- [ ] Record FunctionFactory availability and failures in artifacts.
- [ ] Add tests for `CREATE FUNCTION` success or structured failure.

---

## Scope A24: Named args + ExprPlanner hooks (system builder)

**Objective:** Support named‑argument UDFs and custom expression planning via
an extension hook and record capability gates.

**Pattern snippet**
```python
from __future__ import annotations

from datafusion_engine.expr_planner import install_expr_planners
from datafusion_engine.runtime import DataFusionRuntimeProfile


def install_planners(ctx) -> None:
    install_expr_planners(ctx, planner_names=("codeintel_ops",))


profile = DataFusionRuntimeProfile(session_context_hook=install_planners)
```

**Target files**
- `src/datafusion_engine/expr_planner.py`
- `src/datafusion_engine/runtime.py`
- `src/obs/repro.py`
- `tests/integration/test_expr_planner_hooks.py`

**Implementation checklist**
- [ ] Provide an extension hook to register ExprPlanner instances.
- [ ] Record named‑argument capability in feature gates artifacts.
- [ ] Add tests for named‑argument UDF calls and planner hooks.

---

## Scope A25: Streaming/unbounded table providers

**Objective:** Enable unbounded table registration and record streaming source
policies for incremental pipelines.

**Pattern snippet**
```python
from __future__ import annotations


def create_unbounded_table(ctx, *, name: str, location: str) -> None:
    ctx.sql(
        "CREATE UNBOUNDED EXTERNAL TABLE "
        f"{name} STORED AS PARQUET LOCATION '{location}'"
    )
```

**Target files**
- `src/schema_spec/system.py`
- `src/datafusion_engine/registry_bridge.py`
- `src/datafusion_engine/runtime.py`
- `src/obs/repro.py`
- `tests/integration/test_unbounded_table.py`

**Implementation checklist**
- [ ] Add a dataset flag for unbounded/streaming sources.
- [ ] Register unbounded tables via DDL or provider abstraction.
- [ ] Record unbounded source policies in artifacts.
- [ ] Add integration tests covering basic unbounded reads.

---

## Scope A26: Distributed runtime integration (Ballista)

**Objective:** Support distributed DataFusion execution using Ballista contexts
with explicit runtime policy and artifact tracking.

**Pattern snippet**
```python
from __future__ import annotations

from ballista import BallistaBuilder

from datafusion_engine.runtime import DataFusionRuntimeProfile


def ballista_context():
    return BallistaBuilder().standalone()


profile = DataFusionRuntimeProfile(
    distributed=True,
    distributed_context_factory=ballista_context,
)
```

**Target files**
- `src/datafusion_engine/runtime.py`
- `src/engine/session_factory.py`
- `src/obs/repro.py`
- `tests/integration/test_ballista_smoke.py`

**Implementation checklist**
- [ ] Wire `distributed_context_factory` into session creation.
- [ ] Record distributed runtime mode in runtime artifacts.
- [ ] Add a smoke test for Ballista context creation (gated by marker).

---

## Scope A27: Rule macro DSL (Deferred + selectors + bind)

**Objective:** Standardize a macro DSL for rulepacks using Ibis deferred
expressions, selectors, and `Table.bind`, so rule fragments are composable and
type‑checked.

**Pattern snippet**
```python
from __future__ import annotations

from ibis import _, selectors as s
from ibis.expr.types import Table

from ibis_engine.macros import bind_columns


def apply_rule_macro(table: Table) -> Table:
    exprs = bind_columns(table, s.numeric(), _.status)
    return table.mutate(
        s.across(s.numeric(), dict(z=_ - _.mean()), names="{fn}_{col}"),
        status_norm=exprs[-1].lower(),
    )
```

**Target files**
- `src/ibis_engine/macros.py`
- `src/relspec/compiler.py`
- `src/relspec/model.py`
- `tests/unit/test_ibis_macros.py`

**Implementation checklist**
- [ ] Add deferred/selector helpers for rule macros.
- [ ] Extend macro binding to accept selectors and deferred exprs.
- [ ] Add macro library entrypoints for rulepacks.
- [ ] Add tests for selector‑based macro expansion.

---

## Scope A28: Param‑driven execution + cache policy gating

**Objective:** Treat rule parameters as first‑class inputs and gate caching on
stable parameter sets to avoid incorrect reuse.

**Pattern snippet**
```python
from __future__ import annotations

from ibis_engine.params_bridge import IbisParamRegistry
from ibis_engine.runner import IbisPlanExecutionOptions


def execution_with_params(params: dict[str, object]) -> IbisPlanExecutionOptions:
    registry = IbisParamRegistry()
    bindings = registry.bindings(params)
    return IbisPlanExecutionOptions(params=bindings)
```

**Target files**
- `src/ibis_engine/params_bridge.py`
- `src/ibis_engine/runner.py`
- `src/engine/materialize.py`
- `src/obs/repro.py`
- `tests/unit/test_param_cache_policy.py`

**Implementation checklist**
- [ ] Enforce parameter bindings for plan execution and SQL emission.
- [ ] Record parameter mode (named/typed) in artifacts.
- [ ] Disable caching when params are non‑deterministic or missing defaults.
- [ ] Add tests for param bindings + cache policy behavior.

---

## Scope A29: SQL ingestion guardrails (parse_sql + golden tests)

**Objective:** Make SQL→Ibis IR ingestion explicit, deterministic, and validated
against schema contracts.

**Pattern snippet**
```python
from __future__ import annotations

from ibis_engine.sql_bridge import SqlIngestSpec, parse_sql_table


def ingest_sql(sql: str, *, backend, schema) -> None:
    spec = SqlIngestSpec(sql=sql, catalog=backend, schema=schema, dialect="datafusion")
    _ = parse_sql_table(spec)
```

**Target files**
- `src/ibis_engine/sql_bridge.py`
- `src/relspec/rules/validation.py`
- `src/obs/repro.py`
- `tests/unit/test_sql_ingest_guardrails.py`

**Implementation checklist**
- [ ] Require explicit schemas for SQL ingestion.
- [ ] Preserve SQLGlot AST checkpoints for ingested SQL.
- [ ] Add golden tests for supported SQL shapes.
- [ ] Fail fast on parse/qualification errors with repro artifacts.

---

## Scope A30: Rule IR artifacts (decompile + SQL + SQLGlot AST)

**Objective:** Persist Ibis decompile output and rendered SQL alongside SQLGlot
ASTs for all rule outputs, not just raw SQL ingestion.

**Pattern snippet**
```python
from __future__ import annotations

import ibis

from ibis_engine.compiler_checkpoint import compile_checkpoint


def rule_ir_artifacts(expr, *, backend) -> dict[str, object]:
    checkpoint = compile_checkpoint(expr, backend=backend)
    return {
        "decompile": ibis.decompile(expr),
        "sql": ibis.to_sql(expr),
        "sqlglot_plan_hash": checkpoint.plan_hash,
    }
```

**Target files**
- `src/ibis_engine/compiler_checkpoint.py`
- `src/ibis_engine/sql_bridge.py`
- `src/obs/repro.py`
- `src/obs/manifest.py`
- `tests/unit/test_rule_ir_artifacts.py`

**Implementation checklist**
- [ ] Emit decompiled Ibis expressions for rule outputs.
- [ ] Persist rendered SQL (pretty + canonical) alongside SQLGlot AST.
- [ ] Add plan hash + policy hash to artifacts for reproducibility.
- [ ] Add tests for artifact stability.

---

## Scope A31: Materialization surfaces (create_table/create_view/insert)

**Objective:** Standardize backend‑native materialization APIs for durable
intermediate products and rule outputs.

**Pattern snippet**
```python
from __future__ import annotations

from ibis.expr.types import Table


def materialize_output(backend, *, name: str, expr: Table) -> None:
    backend.create_table(name, schema=expr.schema(), overwrite=True)
    backend.insert(name, expr, overwrite=True)
```

**Target files**
- `src/ibis_engine/sources.py`
- `src/ibis_engine/registry.py`
- `src/ibis_engine/io_bridge.py`
- `src/obs/repro.py`
- `tests/integration/test_materialization_surfaces.py`

**Implementation checklist**
- [ ] Use `create_table` for named, durable intermediates.
- [ ] Use `create_view` for reusable IR‑level views.
- [ ] Use `insert` for rule outputs into cataloged tables.
- [ ] Record namespace actions in artifacts.

---

## Scope A32: Determinism contract (order_by enforcement)

**Objective:** Enforce explicit ordering on all persisted outputs and normalize
ordering for nested explode/unnest flows.

**Pattern snippet**
```python
from __future__ import annotations

import ibis


def ensure_canonical_order(expr: ibis.expr.types.Table, *, keys: list[str]) -> ibis.Table:
    return expr.order_by(keys)
```

**Target files**
- `src/relspec/compiler.py`
- `src/ibis_engine/runner.py`
- `src/arrowdsl/plan/ordering_policy.py`
- `tests/unit/test_ordering_contract.py`

**Implementation checklist**
- [ ] Require explicit `order_by` for persisted rule outputs.
- [ ] For explode/unnest, enforce `row_number` + `order_by` canonical patterns.
- [ ] Record ordering keys in artifacts and manifests.
- [ ] Add tests for ordering enforcement across outputs.

---

## Scope A33: SQLGlot transforms lane expansion + executor tests

**Objective:** Expand the dialect‑compatibility transform lane and add semantic
equivalence tests using the SQLGlot executor harness.

**Pattern snippet**
```python
from __future__ import annotations

from sqlglot.executor import execute
from sqlglot.transforms import (
    eliminate_full_outer_join,
    eliminate_semi_and_anti_joins,
    explode_projection_to_unnest,
)


def rewrite_and_check(sql: str, *, tables: dict[str, object]) -> None:
    original = execute(sql, tables=tables)
    rewritten = execute(explode_projection_to_unnest()(sql), tables=tables)
    assert original == rewritten
```

**Target files**
- `src/sqlglot_tools/optimizer.py`
- `src/sqlglot_tools/bridge.py`
- `tests/unit/test_sqlglot_executor_rewrites.py`
- `tests/unit/test_sqlglot_transforms_lane.py`

**Implementation checklist**
- [ ] Add EXPLODE/UNNEST and join‑rewrite transforms to policy lane.
- [ ] Add semantic equivalence tests using SQLGlot executor.
- [ ] Record rewrite lane + transform list in artifacts.
- [ ] Gate DataFusion SQL emission on strict unsupported‑level settings.

---

## Scope A34: Ibis options control plane (SQL shape policy)

**Objective:** Pin Ibis SQL options (fuse selects, default limit, dialect) to a
runtime profile and record for reproducible SQL shapes.

**Pattern snippet**
```python
from __future__ import annotations

import ibis


def apply_sql_options() -> None:
    ibis.options.sql.fuse_selects = True
    ibis.options.sql.default_limit = None
    ibis.options.sql.default_dialect = "datafusion"
```

**Target files**
- `src/engine/runtime_profile.py`
- `src/ibis_engine/runner.py`
- `src/obs/repro.py`
- `tests/unit/test_ibis_sql_options.py`

**Implementation checklist**
- [ ] Apply SQL options via runtime profile hooks.
- [ ] Record SQL option settings in runtime artifacts.
- [ ] Add tests for deterministic SQL output with options pinned.

---

## Scope A35: UDF ladder enforcement + registry tagging

**Objective:** Enforce the builtin → pyarrow → pandas → python UDF ladder and
record the UDF tier in function registry snapshots.

**Pattern snippet**
```python
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class UdfTier:
    name: str


UDF_TIER_PRIORITY = ("builtin", "pyarrow", "pandas", "python")
```

**Target files**
- `src/datafusion_engine/udf_registry.py`
- `src/engine/function_registry.py`
- `src/arrowdsl/compute/registry.py`
- `tests/unit/test_udf_ladder_policy.py`

**Implementation checklist**
- [ ] Tag UDFs by tier when registered.
- [ ] Enforce tier priority for new UDF definitions.
- [ ] Persist tier metadata in registry fingerprints.
- [ ] Add tests for tier ordering and registry payloads.

---

## Deliverables Summary

- Parquet sort metadata handshake and verification artifacts.
- Contract‑driven DDL with format options precedence.
- Manifest‑first row group stats and pruning diagnostics.
- Async streaming entrypoints for DataFusion pipelines.
- Substrait cross‑engine validation harness.
- Identifier case/quoting policy with CI guard.
- UnionDataset and one‑shot dataset support with tests.
- FilenamePartitioning support for basename templates.
- Standardized DataFusion Arrow ingestion helpers.
- View registry snapshots in run bundles.
- PyCapsule UDF/provider registry fingerprinting and tests.
- Unified streaming adapter for all producer types.
- One‑shot stream scanning with scanner policy enforcement.
- Lineage‑driven scan projections and pushdown evidence artifacts.
- DataFusion Unparser SQL repro artifacts.
- Cross‑lane function registry completion and fingerprints.
- Nested data dispatcher with deterministic explode semantics.
- SQL policy presets with parameter safety enforcement.
- Function catalog snapshots (built‑ins + UDFs).
- Per‑table stats policy controls and registration latency tuning.
- Listing cache lifecycle policy for mutable prefixes.
- DataFusion write policy lane with reproducible outputs.
- FunctionFactory‑enabled `CREATE FUNCTION` lane.
- ExprPlanner/named‑argument extension hooks.
- Unbounded/streaming table provider support.
- Ballista distributed runtime integration.
- Ibis deferred/selector macro DSL for rulepacks.
- Param‑driven execution with cache gating and artifacts.
- SQL ingestion guardrails with parse_sql golden tests.
- Rule IR artifacts (decompile + SQL + SQLGlot AST).
- Backend materialization via create_table/create_view/insert.
- Deterministic ordering contract across rule outputs.
- Expanded SQLGlot transform lane + executor equivalence tests.
- Ibis SQL options control plane.
- UDF ladder enforcement and registry tagging.
