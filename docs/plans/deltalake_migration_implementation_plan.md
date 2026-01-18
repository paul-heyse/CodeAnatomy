## Delta Lake Migration Implementation Plan (Delta-First Architecture)

This plan re-implements the entire storage and materialization stack on Delta Lake as the
canonical system of record. Parquet becomes an optional export format, not a source of
truth. Each scope item includes a representative code pattern, target files, and a
concrete checklist.

---

## Status

All scope items are implemented in the codebase. Checklists below reflect completion.

### Scope 1: Delta-First Dataset Model + Registry Defaults

**Objective:** Make Delta tables the default dataset format and attach Delta-specific
metadata (storage options, versioning) to every dataset registration.

**Pattern snippet**
```python
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping


type DatasetFormat = str


@dataclass(frozen=True)
class DatasetLocation:
    """Location metadata for a dataset."""

    path: str
    format: DatasetFormat = "delta"
    partitioning: str | None = "hive"
    read_options: Mapping[str, object] = field(default_factory=dict)
    storage_options: Mapping[str, str] = field(default_factory=dict)
    delta_version: int | None = None
    delta_timestamp: str | None = None
```

**Target files**
- `src/ibis_engine/registry.py`
- `src/schema_spec/system.py`
- `src/relspec/registry.py`

**Implementation checklist**
- [x] Add `storage_options`, `delta_version`, and `delta_timestamp` to `DatasetLocation`.
- [x] Default `DatasetLocation.format` and `DatasetOpenSpec.dataset_format` to "delta".
- [x] Thread `storage_options` through registry snapshots and manifests.
- [x] Keep explicit overrides for parquet when requested (export-only).

---

### Scope 2: Delta IO Core (Read/Write/Upsert + Commit Metadata)

**Objective:** Replace Parquet IO primitives with Delta equivalents that capture commit
metadata, table versioning, and schema evolution.

**Pattern snippet**
```python
from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from deltalake import DeltaTable, write_deltalake

from arrowdsl.core.interop import TableLike


@dataclass(frozen=True)
class DeltaWriteOptions:
    mode: str = "append"  # "append" | "overwrite" | "ignore" | "error"
    schema_mode: str | None = None  # "merge" | "overwrite"
    predicate: str | None = None
    partition_by: tuple[str, ...] | None = None
    commit_properties: Mapping[str, str] | None = None


def write_table_delta(
    table: TableLike,
    path: str,
    *,
    options: DeltaWriteOptions,
    storage_options: Mapping[str, str] | None = None,
) -> int:
    write_deltalake(
        path,
        table,
        mode=options.mode,
        schema_mode=options.schema_mode,
        predicate=options.predicate,
        partition_by=options.partition_by,
        commit_properties=options.commit_properties,
        storage_options=storage_options,
    )
    return DeltaTable(path, storage_options=storage_options).version()
```

**Target files**
- `src/arrowdsl/io/delta.py` (new)
- `src/storage/io.py`
- `src/arrowdsl/io/parquet.py` (retain as export-only utilities)

**Implementation checklist**
- [x] Introduce `arrowdsl.io.delta` with read/write/merge helpers.
- [x] Return commit version and record commit metadata for downstream artifacts.
- [x] Provide compatibility helpers for read time travel (`version`/`timestamp`).
- [x] Keep Parquet writers as secondary export utilities only.

---

### Scope 3: DataFusion Delta Registration (TableProvider + Fallback)

**Objective:** Always register Delta tables as DataFusion `TableProvider`s and avoid
Parquet/listing registration unless required for compatibility.

**Pattern snippet**
```python
from __future__ import annotations

from deltalake import DeltaTable
from datafusion import SessionContext
from datafusion.dataframe import DataFrame

from ibis_engine.registry import DatasetLocation


def _register_delta(ctx: SessionContext, name: str, location: DatasetLocation) -> DataFrame:
    table = DeltaTable(
        location.path,
        storage_options=location.storage_options,
        version=location.delta_version,
    )
    try:
        ctx.register_table(name, table)
    except TypeError:
        dataset = table.to_pyarrow_dataset()
        ctx.register_dataset(name, dataset)
    return ctx.table(name)
```

**Target files**
- `src/datafusion_engine/registry_bridge.py`
- `src/datafusion_engine/df_builder.py`

**Implementation checklist**
- [x] Add `_register_delta` and select it when `location.format == "delta"`.
- [x] Avoid Parquet/listing registration for Delta paths.
- [x] Record delta registration artifacts in diagnostics.

---

### Scope 4: ArrowDSL Source Normalization for Delta

**Objective:** Normalize Delta sources into scan-ready datasets or readers for ArrowDSL.

**Pattern snippet**
```python
from __future__ import annotations

import pyarrow.dataset as ds
from deltalake import DeltaTable

from arrowdsl.plan.source_normalize import DatasetSourceOptions


def _delta_dataset(path: str, *, options: DatasetSourceOptions) -> ds.Dataset:
    table = DeltaTable(
        path,
        storage_options=getattr(options, "storage_options", None),
    )
    return table.to_pyarrow_dataset()
```

**Target files**
- `src/arrowdsl/plan/source_normalize.py`
- `src/arrowdsl/plan/query.py`

**Implementation checklist**
- [x] Add Delta handling in `normalize_dataset_source`.
- [x] Thread `storage_options` through dataset open specs.
- [x] Preserve existing Arrow scan/predicate/projection logic.

---

### Scope 5: Ibis Registry and Read Path (Delta Everywhere)

**Objective:** Ensure Ibis reads use `read_delta` and register Delta tables in
DataFusion contexts.

**Pattern snippet**
```python
from __future__ import annotations

from ibis.expr.types import Table


def read_dataset_delta(backend: object, *, path: str, options: dict[str, object]) -> Table:
    reader = getattr(backend, "read_delta", None)
    if not callable(reader):
        msg = "Ibis backend does not support delta datasets."
        raise TypeError(msg)
    return reader(path, **options)
```

**Target files**
- `src/ibis_engine/registry.py`
- `src/ibis_engine/backend.py`

**Implementation checklist**
- [x] Default Ibis dataset reads to Delta when `dataset_format == "delta"`.
- [x] Pass `storage_options` into Ibis read parameters.
- [x] Ensure DataFusion session contexts receive the Delta provider registration.

---

### Scope 6: DataFusion Write Path to Delta (Arrow -> Commit)

**Objective:** Replace DataFusion parquet writes with Delta commits by converting
DataFusion results to Arrow and committing via delta-rs.

**Pattern snippet**
```python
from __future__ import annotations

from deltalake import write_deltalake


def write_datafusion_delta(df: object, path: str) -> None:
    to_arrow = getattr(df, "to_arrow_table", None)
    if not callable(to_arrow):
        msg = "DataFusion DataFrame missing to_arrow_table."
        raise ValueError(msg)
    table = to_arrow()
    write_deltalake(path, table, mode="append")
```

**Target files**
- `src/ibis_engine/io_bridge.py`
- `src/storage/io.py`

**Implementation checklist**
- [x] Add a Delta write path parallel to `_write_datafusion_dataset`.
- [x] Emit commit metadata (ordering keys, schema fingerprint, runtime profile).
- [x] Keep parquet output as explicit export-only option.

---

### Scope 7: Delta Upserts for Incremental Pipelines

**Objective:** Replace delete-partition + rewrite logic with Delta merge/replaceWhere
semantics for incremental datasets.

**Pattern snippet**
```python
from __future__ import annotations

from deltalake import DeltaTable


def upsert_partitions_delta(path: str, table: object, *, predicate: str) -> None:
    delta = DeltaTable(path)
    delta.merge(
        source=table,
        predicate=predicate,
        source_alias="source",
        target_alias="target",
    ).when_matched_update_all().when_not_matched_insert_all().execute()
```

**Target files**
- `src/incremental/nodes_update.py`
- `src/incremental/props_update.py`
- `src/arrowdsl/io/parquet.py` (replace upsert entrypoints)

**Implementation checklist**
- [x] Introduce `upsert_dataset_partitions_delta` and update callers.
- [x] Use `predicate` for file_id partition rewrites.
- [x] Preserve schema alignment and encoding policies via Delta schema mode.

---

### Scope 8: Delta-Based Incremental State Store + CDF

**Objective:** Store snapshots/diffs in Delta tables and use CDF for incremental
processing instead of parquet diff tables.

**Pattern snippet**
```python
from __future__ import annotations

from deltalake import DeltaTable, write_deltalake


def write_repo_snapshot_delta(path: str, snapshot: object) -> int:
    write_deltalake(path, snapshot, mode="overwrite")
    return DeltaTable(path).version()


def read_repo_snapshot_delta(path: str) -> object | None:
    if not DeltaTable.is_deltatable(path):
        return None
    return DeltaTable(path).to_pyarrow_table()
```

**Target files**
- `src/incremental/state_store.py`
- `src/incremental/snapshot.py`
- `src/incremental/scip_snapshot.py`

**Implementation checklist**
- [x] Replace snapshot parquet paths with delta table directories.
- [x] Record versions for reproducibility and time travel.
- [x] Add CDF integration for diff derivation.

---

### Scope 9: Delta Feature Enablement (CDF, Row Tracking, DVs, ICT)

**Objective:** Enable key Delta table features to support incremental diffs,
merge-on-read updates, and stable commit timestamps.

**Pattern snippet**
```python
from __future__ import annotations

from deltalake import DeltaTable


def enable_delta_features(path: str) -> None:
    table = DeltaTable(path)
    table.alter.set_table_property("delta.enableChangeDataFeed", "true")
    table.alter.set_table_property("delta.enableRowTracking", "true")
    table.alter.set_table_property("delta.enableDeletionVectors", "true")
    table.alter.set_table_property("delta.enableInCommitTimestamps", "true")
```

**Target files**
- `src/arrowdsl/io/delta.py` (new helpers)
- `src/storage/io.py`

**Implementation checklist**
- [x] Add feature enablement helpers on table creation.
- [x] Persist enabled features in manifest artifacts.
- [x] Add integration tests for CDF reads.

---

### Scope 10: Hamilton Cache + Adapter Migration to Delta

**Objective:** Replace parquet caching adapters with Delta table adapters to align
Hamilton cache behavior with transactional storage.

**Pattern snippet**
```python
from __future__ import annotations

from dataclasses import dataclass
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
from hamilton.io.data_adapters import DataLoader, DataSaver


@dataclass
class ArrowDeltaSaver(DataSaver):
    path: str

    def save_data(self, data: pa.Table) -> dict[str, object]:
        write_deltalake(self.path, data, mode="overwrite")
        version = DeltaTable(self.path).version()
        return {"delta_version": version}
```

**Target files**
- `src/hamilton_pipeline/arrow_adapters.py`
- `src/hamilton_pipeline/driver_factory.py`

**Implementation checklist**
- [x] Add Delta DataLoader/DataSaver adapters.
- [x] Register Delta adapters alongside existing parquet adapters.
- [x] Update `@cache(format="parquet")` uses to `format="delta"` where appropriate.

---

### Scope 11: Output Materializers to Delta Tables

**Objective:** Replace final and intermediate parquet materializers with Delta
table writers and structured commit metadata.

**Pattern snippet**
```python
from __future__ import annotations

from deltalake import write_deltalake


def write_cpg_nodes_delta(path: str, table: object) -> int:
    write_deltalake(path, table, mode="overwrite")
    return DeltaTable(path).version()
```

**Target files**
- `src/hamilton_pipeline/modules/outputs.py`
- `src/graph/product_build.py`

**Implementation checklist**
- [x] Replace parquet output paths with delta table directories.
- [x] Emit commit metadata (schema fingerprint, ordering keys).
- [x] Preserve parquet export as optional post-processing.

---

### Scope 12: Manifest + Diagnostics Delta Metadata

**Objective:** Record Delta versions, features, and commit metadata in manifests
instead of parquet metadata sidecars.

**Pattern snippet**
```python
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DatasetRecord:
    name: str
    kind: str
    path: str | None
    format: str | None
    delta_version: int | None
    delta_features: dict[str, str] | None
```

**Target files**
- `src/obs/manifest.py`
- `src/obs/repro.py`

**Implementation checklist**
- [x] Add Delta version/feature fields to dataset records.
- [x] Record commit metadata and snapshot versions.
- [x] Remove dependence on parquet sidecar metadata paths.

---

### Scope 13: Migration/Backfill Tooling

**Objective:** Provide a deterministic script to convert existing parquet datasets
into Delta tables and record initial versions.

**Pattern snippet**
```python
from __future__ import annotations

import pyarrow.dataset as ds
from deltalake import write_deltalake


def migrate_parquet_to_delta(parquet_dir: str, delta_dir: str) -> None:
    dataset = ds.dataset(parquet_dir, format="parquet")
    table = dataset.to_table()
    write_deltalake(delta_dir, table, mode="overwrite")
```

**Target files**
- `scripts/migrate_parquet_to_delta.py` (new)
- `docs/plans/deltalake_migration_implementation_plan.md`

**Implementation checklist**
- [x] Create a migration script with dry-run and verification modes.
- [x] Emit a migration report (row counts, schema fingerprint, version).
- [x] Add documentation for one-shot and incremental migrations.

---

### Scope 14: Testing and Validation Strategy

**Objective:** Validate Delta IO, DataFusion registration, CDF reads, and versioned
snapshots end-to-end.

**Pattern snippet**
```python
from __future__ import annotations

from deltalake import DeltaTable, write_deltalake


def test_delta_write_and_version(tmp_path) -> None:
    path = str(tmp_path / "tbl")
    write_deltalake(path, {"id": [1, 2]})
    table = DeltaTable(path)
    assert table.version() == 0
```

**Target files**
- `tests/unit/test_delta_io.py` (new)
- `tests/unit/test_datafusion_delta_registration.py` (new)
- `tests/unit/test_incremental_delta_snapshots.py` (new)

**Implementation checklist**
- [x] Unit tests for Delta write/read/time travel.
- [x] DataFusion registration tests (provider vs dataset fallback).
- [x] CDF integration tests for incremental diffs.

---

## Execution Order (Recommended)
1) Dataset model + registry defaults
2) Delta IO core + DataFusion registration
3) ArrowDSL normalization + Ibis reads
4) DataFusion write path + incremental upserts
5) Incremental state store + CDF features
6) Hamilton adapters + output materializers
7) Manifest + diagnostics
8) Migration tooling + test suite
