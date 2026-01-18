## Delta Lake Advanced Integration Implementation Plan

This plan extends the delta-first architecture with higher-fidelity DataFusion
interop, operational robustness, and richer governance metadata. Each scope
item includes representative code patterns, target files, and a concrete
checklist.

---

### Scope 1: Delta Scan Options + DataFusion Registration Controls

**Objective:** Expose Delta-specific scan knobs (file column, pushdown,
schema shaping) and attach them to dataset registration.

**Pattern snippet**
```python
from __future__ import annotations

from dataclasses import dataclass
import pyarrow as pa


@dataclass(frozen=True)
class DeltaScanOptions:
    file_column_name: str | None = None
    enable_parquet_pushdown: bool = True
    schema_force_view_types: bool = False
    schema: pa.Schema | None = None


def _register_delta(ctx: SessionContext, name: str, location: DatasetLocation) -> DataFrame:
    table = DeltaTable(location.path, storage_options=location.storage_options)
    provider = getattr(table, "__datafusion_table_provider__", None)
    if callable(provider) and location.delta_scan is not None:
        delta_config = _delta_scan_config(location.delta_scan)
        ctx.register_table(name, provider(delta_config))
    else:
        ctx.register_table(name, table)
    return ctx.table(name)
```

**Target files**
- `src/schema_spec/system.py`
- `src/ibis_engine/registry.py`
- `src/datafusion_engine/registry_bridge.py`

**Implementation checklist**
- [ ] Add `DeltaScanOptions` and wire into `DatasetLocation`.
- [ ] Build `_delta_scan_config(...)` adapter for provider registration.
- [ ] Support file lineage column via scan config.
- [ ] Emit diagnostics artifacts with scan config summary.

---

### Scope 2: File-List Constrained Delta Scans (Incremental-Friendly)

**Objective:** Allow explicit file lists to constrain scans for incremental
pipelines and targeted validation.

**Pattern snippet**
```python
from __future__ import annotations

import pyarrow.dataset as ds
from deltalake import DeltaTable


def delta_dataset_from_files(
    path: str,
    *,
    files: tuple[str, ...],
    filesystem: object | None,
) -> ds.Dataset:
    table = DeltaTable(path)
    file_uris = table.file_uris(files)
    return ds.dataset(file_uris, format="parquet", filesystem=filesystem)
```

**Target files**
- `src/arrowdsl/plan/source_normalize.py`
- `src/datafusion_engine/registry_bridge.py`
- `src/incremental/*`

**Implementation checklist**
- [ ] Add `files` to `DatasetLocation` and `DatasetOpenSpec`.
- [ ] Build `delta_dataset_from_files(...)` helper for fallback datasets.
- [ ] Update incremental callers to pass file lists when available.

---

### Scope 3: CDF TableProvider Registration (DataFusion Queryable CDF)

**Objective:** Register Delta CDF as a DataFusion table provider when available,
with a fallback to Arrow-based registration.

**Pattern snippet**
```python
from __future__ import annotations

from deltalake import DeltaTable


def register_delta_cdf(
    ctx: SessionContext,
    *,
    name: str,
    path: str,
    cdf_options: DeltaCdfOptions,
) -> DataFrame:
    table = DeltaTable(path)
    provider = getattr(table, "cdf_table_provider", None)
    if callable(provider):
        ctx.register_table(name, provider(cdf_options))
    else:
        cdf = read_delta_cdf(path, options=cdf_options)
        ctx.register_table(name, cdf.to_pyarrow_dataset())
    return ctx.table(name)
```

**Target files**
- `src/arrowdsl/io/delta.py`
- `src/datafusion_engine/registry_bridge.py`
- `src/ibis_engine/registry.py`

**Implementation checklist**
- [ ] Add CDF registration helper with provider fallback.
- [ ] Expose `read_cdf` registration in Ibis registry.
- [ ] Emit diagnostics artifact capturing CDF registration path.

---

### Scope 4: Storage Plumbing Split (Log Store vs Data Filesystem)

**Objective:** Separate Delta log store configuration from bulk data filesystem
configuration to avoid mismatched credentials and IO paths.

**Pattern snippet**
```python
from __future__ import annotations

import pyarrow.fs as fs
from deltalake import DeltaTable


def delta_filesystem(table_uri: str) -> fs.FileSystem:
    raw_fs, normalized_path = fs.FileSystem.from_uri(table_uri)
    return fs.SubTreeFileSystem(normalized_path, raw_fs)


def delta_dataset(path: str, *, filesystem: fs.FileSystem | None) -> ds.Dataset:
    table = DeltaTable(path)
    return table.to_pyarrow_dataset(filesystem=filesystem)
```

**Target files**
- `src/arrowdsl/plan/source_normalize.py`
- `src/datafusion_engine/registry_bridge.py`
- `src/ibis_engine/registry.py`

**Implementation checklist**
- [ ] Add `filesystem` to Delta dataset opens (explicit when provided).
- [ ] Use SubTree filesystem for Delta fallback dataset scans.
- [ ] Ensure `storage_options` remain log-store-only.

---

### Scope 5: Concurrency Safety + Commit Retries

**Objective:** Make Delta writes robust under concurrent writers (object stores),
including lock configuration and retry-on-conflict.

**Pattern snippet**
```python
from __future__ import annotations

from dataclasses import dataclass
import time


@dataclass(frozen=True)
class DeltaWriteRetryPolicy:
    max_attempts: int = 3
    backoff_seconds: float = 0.5


def write_table_delta_with_retry(
    table: DeltaWriteInput,
    path: str,
    *,
    options: DeltaWriteOptions,
    retry_policy: DeltaWriteRetryPolicy,
) -> DeltaWriteResult:
    for attempt in range(retry_policy.max_attempts):
        try:
            return write_table_delta(table, path, options=options)
        except DeltaError as exc:
            if attempt == retry_policy.max_attempts - 1:
                raise
            if "conflict" not in str(exc).lower():
                raise
            time.sleep(retry_policy.backoff_seconds * (attempt + 1))
```

**Target files**
- `src/arrowdsl/io/delta.py`
- `src/storage/io.py`
- `src/ibis_engine/registry.py`

**Implementation checklist**
- [ ] Add retry policy support to delta writes.
- [ ] Surface log-store locking options in dataset configs.
- [ ] Record retry attempts in commit metadata.

---

### Scope 6: Maintenance Utilities (Vacuum, Checkpoint, Log Cleanup)

**Objective:** Provide first-class maintenance helpers and scripts for keeping
Delta tables healthy and logs bounded.

**Pattern snippet**
```python
from __future__ import annotations

from deltalake import DeltaTable


def vacuum_delta(path: str, *, retention_hours: int, dry_run: bool) -> list[str]:
    table = DeltaTable(path)
    return table.vacuum(
        retention_hours=retention_hours,
        dry_run=dry_run,
    )


def create_checkpoint(path: str) -> None:
    DeltaTable(path).create_checkpoint()
```

**Target files**
- `src/arrowdsl/io/delta.py`
- `scripts/delta_maintenance.py` (new)
- `docs/plans/deltalake_advanced_integration_plan.md`

**Implementation checklist**
- [ ] Add vacuum/checkpoint helpers in `arrowdsl.io.delta`.
- [ ] Create a maintenance script with schedule-friendly CLI.
- [ ] Emit maintenance results into run bundles/diagnostics.

---

### Scope 7: Data Skipping Stats + File Sizing Policy

**Objective:** Make stats collection and file sizing configurable per dataset to
improve pruning and IO efficiency.

**Pattern snippet**
```python
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DeltaWritePolicy:
    target_file_size: int | None = None
    stats_columns: tuple[str, ...] | None = None


def delta_table_configuration(policy: DeltaWritePolicy) -> dict[str, str]:
    config: dict[str, str] = {}
    if policy.target_file_size is not None:
        config["delta.targetFileSize"] = str(policy.target_file_size)
    if policy.stats_columns:
        config["delta.dataSkippingStatsColumns"] = ",".join(policy.stats_columns)
    return config
```

**Target files**
- `src/schema_spec/system.py`
- `src/arrowdsl/io/delta.py`
- `src/ibis_engine/registry.py`

**Implementation checklist**
- [ ] Add `DeltaWritePolicy` to dataset specs/config.
- [ ] Map write policy into `DeltaWriteOptions.configuration`.
- [ ] Persist stats policy in manifests for reproducibility.

---

### Scope 8: Schema Evolution + Column Mapping Policy

**Objective:** Encapsulate schema evolution rules and column mapping mode to
enable safe renames/drops and long-term schema changes.

**Pattern snippet**
```python
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DeltaSchemaPolicy:
    schema_mode: str | None = None  # "merge" | "overwrite"
    column_mapping_mode: str | None = None  # "id" | "name"


def delta_schema_configuration(policy: DeltaSchemaPolicy) -> dict[str, str]:
    config: dict[str, str] = {}
    if policy.column_mapping_mode is not None:
        config["delta.columnMapping.mode"] = policy.column_mapping_mode
    return config
```

**Target files**
- `src/schema_spec/system.py`
- `src/arrowdsl/io/delta.py`
- `src/ibis_engine/registry.py`

**Implementation checklist**
- [ ] Add schema policy to dataset specs/config.
- [ ] Merge schema policy into write options + table properties.
- [ ] Record column mapping mode in manifest metadata.

---

### Scope 9: Constraint / Invariant Enforcement

**Objective:** Validate table invariants and constraints using DataFusion at
commit time, capturing violations in diagnostics.

**Pattern snippet**
```python
from __future__ import annotations

import pyarrow as pa


def validate_table_constraints(
    ctx: SessionContext,
    *,
    name: str,
    table: pa.Table,
    constraints: tuple[str, ...],
) -> list[str]:
    ctx.register_table(name, table)
    violations: list[str] = []
    for constraint in constraints:
        df = ctx.sql(
            f\"SELECT * FROM {name} WHERE NOT ({constraint}) LIMIT 1\"
        )
        if df.to_pyarrow_table().num_rows > 0:
            violations.append(constraint)
    return violations
```

**Target files**
- `src/schema_spec/*`
- `src/hamilton_pipeline/modules/outputs.py`
- `src/datafusion_engine/bridge.py`

**Implementation checklist**
- [ ] Introduce constraint spec wiring for Delta writes.
- [ ] Validate constraints before commit; fail fast with diagnostics.
- [ ] Persist violations into run bundles and manifests.

---

### Scope 10: Manifest + Run Bundle Delta History Metadata

**Objective:** Capture Delta protocol and history details for reproducibility
and auditability.

**Pattern snippet**
```python
from __future__ import annotations

from deltalake import DeltaTable


def delta_history_snapshot(path: str) -> dict[str, object] | None:
    table = DeltaTable(path)
    history = table.history(limit=1)
    return history[0] if history else None
```

**Target files**
- `src/obs/manifest.py`
- `src/obs/repro.py`

**Implementation checklist**
- [ ] Add last-commit history payload to dataset records.
- [ ] Include protocol versions and active features in run bundles.
- [ ] Track history payload in `materialization_reports`.

---

### Scope 11: Restore + Snapshot Export Tooling

**Objective:** Provide CLI tooling for restoring Delta tables to prior versions
and exporting snapshot copies for experiments.

**Pattern snippet**
```python
from __future__ import annotations

from deltalake import DeltaTable


def restore_table(path: str, *, version: int | None, timestamp: str | None) -> None:
    table = DeltaTable(path)
    table.restore(version=version, timestamp=timestamp)


def export_snapshot(path: str, target: str, *, version: int | None) -> None:
    table = DeltaTable(path, version=version)
    table.to_pyarrow_table().to_pandas().to_parquet(target)
```

**Target files**
- `scripts/delta_restore_table.py` (new)
- `scripts/delta_export_snapshot.py` (new)

**Implementation checklist**
- [ ] Add CLI for restore (version or timestamp).
- [ ] Add snapshot export CLI for debugging and experiments.
- [ ] Document supported restore semantics and retention constraints.

---

### Scope 12: Optional Distributed Plan Serialization Support

**Objective:** Prepare a path to distributed execution by supporting Delta plan
serialization hooks in DataFusion (when applicable).

**Pattern snippet**
```python
from __future__ import annotations


def install_delta_plan_codecs(ctx: SessionContext) -> None:
    codec = getattr(ctx, "register_extension_codecs", None)
    if callable(codec):
        codec("delta_physical", "delta_logical")
```

**Target files**
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/df_builder.py`

**Implementation checklist**
- [ ] Add optional runtime flag to install Delta plan codecs.
- [ ] Gate on availability of codec APIs in DataFusion Python.
- [ ] Add diagnostics to report codec installation.

---

## Execution Order (Recommended)
1) Delta scan options + file-list constrained scans
2) CDF TableProvider registration + storage plumbing split
3) Concurrency safety + maintenance utilities
4) Stats + schema evolution policies
5) Constraint enforcement + manifest metadata
6) Restore/snapshot tooling
7) Optional plan serialization support
