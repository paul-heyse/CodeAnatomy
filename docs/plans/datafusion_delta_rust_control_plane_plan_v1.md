# Delta Lake Rust Control Plane + DataFusion — Best-in-Class Implementation Plan (v1)

> Goal: define and implement a **Rust-first Delta Lake control plane** that treats Delta as the authoritative storage/transaction engine and DataFusion as the authoritative planner/executor.
>
> Constraint: this is a wholesale switch to the go-forward architecture. We will not run legacy and target-state Delta architectures in parallel.

---

## Target end state (non-negotiable contracts)

1. **Single physical scan boundary**: all Delta reads flow through Rust `DeltaTableProvider` / `DeltaCdfTableProvider` exposed via `datafusion_ext`.
2. **Single mutation/commit boundary**: all Delta writes and mutations flow through delta-rs operations in Rust.
3. **Protocol-aware execution**: protocol versions and table features are validated and recorded at registration, planning, and execution time.
4. **Pinned snapshots everywhere**: every planned scan resolves to a pinned Delta version or timestamp, and pins participate in fingerprints and scheduling keys.
5. **Delta-native incrementalism**: change data feed (CDF) is a first-class dataset surface, not an afterthought.
6. **Delta as an observable subsystem**: snapshot metadata, protocol/features, scan plans, mutations, and maintenance events are persisted as queryable artifacts.

---

## Scope 1 — Rust Delta Control Plane as the single Delta surface

**Intent**: consolidate all Delta semantics (snapshot loading, protocol gating, providers, mutations, maintenance, and observability) behind a single Rust control-plane module exposed through `datafusion_ext`.

### Representative pattern

```rust
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use deltalake::delta_datafusion::{DeltaScanConfig, DeltaTableProvider};
use deltalake::{ensure_table_uri, DeltaTableBuilder};
use deltalake::errors::DeltaTableError;

#[derive(Debug, Clone)]
pub struct DeltaSnapshotInfo {
    pub table_uri: String,
    pub version: i64,
    pub min_reader_version: i32,
    pub min_writer_version: i32,
    pub reader_features: Vec<String>,
    pub writer_features: Vec<String>,
    pub table_properties: BTreeMap<String, String>,
    pub schema_json: String,
    pub partition_columns: Vec<String>,
}

pub struct DeltaControlPlane;

impl DeltaControlPlane {
    pub async fn snapshot_info(
        table_uri: &str,
        storage_options: Option<HashMap<String, String>>,
        version: Option<i64>,
        timestamp: Option<String>,
    ) -> Result<DeltaSnapshotInfo, DeltaTableError> {
        let table_url = ensure_table_uri(table_uri)?;
        let mut builder = DeltaTableBuilder::from_url(table_url)?;
        if let Some(options) = storage_options {
            builder = builder.with_storage_options(options);
        }
        if let Some(v) = version {
            builder = builder.with_version(v);
        }
        if let Some(ts) = timestamp {
            builder = builder.with_datestring(ts)?;
        }
        let table = builder.load().await?;
        let state = table.snapshot()?.snapshot().clone();
        let protocol = state.protocol();
        let metadata = state.metadata();
        Ok(DeltaSnapshotInfo {
            table_uri: table_uri.to_owned(),
            version: state.version(),
            min_reader_version: protocol.min_reader_version,
            min_writer_version: protocol.min_writer_version,
            reader_features: protocol.reader_features().cloned().unwrap_or_default(),
            writer_features: protocol.writer_features().cloned().unwrap_or_default(),
            table_properties: metadata.configuration.clone(),
            schema_json: metadata.schema_string.clone(),
            partition_columns: metadata.partition_columns.clone(),
        })
    }

    pub fn provider_from_snapshot(
        snapshot: deltalake::kernel::snapshot::EagerSnapshot,
        log_store: Arc<dyn deltalake::logstore::LogStore>,
        scan_config: DeltaScanConfig,
    ) -> Result<DeltaTableProvider, DeltaTableError> {
        DeltaTableProvider::try_new(snapshot, log_store, scan_config)
    }
}
```

### Target files to modify

- `rust/datafusion_ext/src/lib.rs`
- `rust/datafusion_ext/src/delta_control_plane.rs` (new)
- `rust/datafusion_ext/src/delta_protocol.rs` (new)
- `rust/datafusion_ext/src/delta_mutations.rs` (new)
- `rust/datafusion_ext/src/delta_maintenance.rs` (new)
- `rust/datafusion_ext/src/delta_observability.rs` (new)
- `rust/datafusion_ext/Cargo.toml`

### Code and modules to delete

- Ad-hoc Delta helpers in Rust that duplicate control-plane responsibilities after the control plane is installed and adopted.

### Status (2026-01-27)

- Rust control-plane modules now exist and are wired into `datafusion_ext`: `rust/datafusion_ext/src/delta_control_plane.rs`, `rust/datafusion_ext/src/delta_protocol.rs`, `rust/datafusion_ext/src/delta_mutations.rs`, `rust/datafusion_ext/src/delta_maintenance.rs`, and `rust/datafusion_ext/src/delta_observability.rs`.
- The PyO3 surface now exposes control-plane entrypoints including snapshot info, add actions, session-derived providers, mutations, and maintenance in `rust/datafusion_ext/src/lib.rs`.
- Delta providers and scan planning now depend on the control plane in Python via `src/datafusion_engine/delta_control_plane.py`, but legacy `DeltaTableBuilder` usage still exists in `rust/datafusion_ext/src/lib.rs` and Python delta-rs usage still exists in `src/storage/deltalake/delta.py`.

### Implementation checklist

- [ ] Introduce a Rust `DeltaControlPlane` module and make it the only place where `DeltaTableBuilder` is used.
- [ ] Expose control-plane functions through `datafusion_ext` PyO3 functions rather than direct Python delta-rs usage.
- [ ] Ensure all provider construction flows through control-plane surfaces.
- [ ] Ensure all mutation and maintenance flows through control-plane surfaces.

---

## Scope 2 — Protocol and table-feature gating as a hard contract

**Intent**: make protocol versions and table features explicit requirements that are checked and recorded at registration, plan time, and execution time.

### Representative pattern

```python
from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Sequence

from datafusion import SessionContext


@dataclass(frozen=True)
class DeltaFeatureGate:
    """Protocol and feature requirements for a Delta dataset."""

    min_reader_version: int | None = None
    min_writer_version: int | None = None
    required_reader_features: tuple[str, ...] = ()
    required_writer_features: tuple[str, ...] = ()


def enforce_delta_gate(
    ctx: SessionContext,
    *,
    table_uri: str,
    storage_options: Mapping[str, str] | None,
    gate: DeltaFeatureGate,
) -> Mapping[str, object]:
    """Validate protocol/features and return a snapshot payload."""
    from datafusion_engine.delta_control_plane import delta_snapshot_info

    snapshot = delta_snapshot_info(
        ctx,
        table_uri=table_uri,
        storage_options=storage_options,
    )
    _validate_gate(snapshot, gate)
    return snapshot


def _validate_gate(snapshot: Mapping[str, object], gate: DeltaFeatureGate) -> None:
    reader_version = int(snapshot["min_reader_version"])
    writer_version = int(snapshot["min_writer_version"])
    reader_features = set(snapshot.get("reader_features") or ())
    writer_features = set(snapshot.get("writer_features") or ())
    if gate.min_reader_version is not None and reader_version < gate.min_reader_version:
        msg = "Delta reader protocol gate failed."
        raise ValueError(msg)
    if gate.min_writer_version is not None and writer_version < gate.min_writer_version:
        msg = "Delta writer protocol gate failed."
        raise ValueError(msg)
    if not set(gate.required_reader_features).issubset(reader_features):
        msg = "Delta reader feature gate failed."
        raise ValueError(msg)
    if not set(gate.required_writer_features).issubset(writer_features):
        msg = "Delta writer feature gate failed."
        raise ValueError(msg)
```

### Target files to modify

- `src/datafusion_engine/dataset_registry.py`
- `src/schema_spec/system.py`
- `src/schema_spec/dataset_handle.py`
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/registry_bridge.py`
- `src/datafusion_engine/plan_bundle.py`
- `src/datafusion_engine/plan_artifact_store.py`
- `src/datafusion_engine/delta_control_plane.py` (new)

### Code and modules to delete

- Any Delta registration or execution path that ignores protocol or feature compatibility.

### Implementation checklist

- [ ] Add Delta protocol/feature requirements to dataset specs and dataset locations.
- [ ] Validate gates during dataset registration and fail fast.
- [ ] Record protocol versions and active feature flags into plan artifacts.
- [ ] Include protocol/feature gates in plan fingerprints and cache keys.

---

## Scope 3 — Snapshot pinning and Delta inputs as canonical plan state

**Intent**: make pinned Delta versions/timestamps non-optional and fully integrated into scan planning, plan bundles, and scheduling keys.

### Representative pattern

```python
from __future__ import annotations

from dataclasses import replace
from typing import Mapping, Sequence

from datafusion_engine.plan_bundle import DataFusionPlanBundle, DeltaInputPin
from datafusion_engine.scan_planner import ScanUnit


def pins_from_scan_units(
    *,
    scan_units: Sequence[ScanUnit],
    snapshot_by_dataset: Mapping[str, Mapping[str, object]],
) -> tuple[DeltaInputPin, ...]:
    """Derive canonical Delta pins from scan units and snapshot metadata."""
    pins: dict[str, DeltaInputPin] = {}
    for unit in scan_units:
        snapshot = snapshot_by_dataset.get(unit.dataset_name)
        timestamp = None
        if snapshot is not None:
            raw_ts = snapshot.get("snapshot_timestamp")
            if isinstance(raw_ts, str) and raw_ts:
                timestamp = raw_ts
        pins[unit.dataset_name] = DeltaInputPin(
            dataset_name=unit.dataset_name,
            version=unit.delta_version,
            timestamp=timestamp,
        )
    return tuple(sorted(pins.values(), key=lambda pin: (pin.dataset_name, pin.version or -1)))


def attach_delta_pins(
    bundle: DataFusionPlanBundle,
    *,
    pins: Sequence[DeltaInputPin],
) -> DataFusionPlanBundle:
    """Return a new bundle with canonical Delta pins attached."""
    return replace(bundle, delta_inputs=tuple(pins))
```

### Target files to modify

- `src/relspec/execution_plan.py`
- `src/datafusion_engine/scan_planner.py`
- `src/datafusion_engine/scan_overrides.py`
- `src/datafusion_engine/plan_bundle.py`
- `src/engine/plan_cache.py`
- `src/datafusion_engine/plan_artifact_store.py`

### Code and modules to delete

- Any scan planning or plan fingerprinting logic that does not account for pinned Delta versions/timestamps.

### Status (2026-01-27)

- The two-pass planning pipeline exists in `src/datafusion_engine/planning_pipeline.py` and is used by `src/relspec/execution_plan.py` to pin Delta inputs before scheduling.
- Plan bundles now include Delta pins and hash them into the plan fingerprint in `src/datafusion_engine/plan_bundle.py`.
- Plan-cache keys incorporate Delta pins when present in `src/datafusion_engine/execution_helpers.py` and `src/engine/plan_cache.py`, but many ad-hoc planning flows still bypass the pinning pipeline.

### Implementation checklist

- [ ] Resolve pinned snapshots for all Delta datasets used by a plan.
- [ ] Derive `DeltaInputPin` values from scan units and snapshot metadata.
- [ ] Attach Delta pins to all plan bundles used for scheduling and execution.
- [ ] Ensure plan cache keys and scheduling keys incorporate Delta pins.

---

## Scope 4 — Canonical provider registration: session-derived config + restricted files

**Intent**: make session-derived Delta scan config and file-restricted providers the only registration paths.

### Representative pattern

```python
from __future__ import annotations

from collections.abc import Mapping, Sequence

from datafusion import SessionContext


def register_delta_dataset(
    ctx: SessionContext,
    *,
    name: str,
    table_uri: str,
    storage_options: Mapping[str, str] | None,
    version: int | None,
    timestamp: str | None,
    scan_files: Sequence[str],
) -> None:
    """Register a Delta dataset through the Rust control plane."""
    from datafusion_engine.delta_control_plane import (
        delta_provider_from_session,
        delta_provider_with_files,
    )
    from datafusion_engine.io_adapter import DataFusionIOAdapter
    from datafusion_engine.table_provider_capsule import TableProviderCapsule

    provider = (
        delta_provider_with_files(
            ctx,
            table_uri=table_uri,
            storage_options=storage_options,
            version=version,
            timestamp=timestamp,
            files=scan_files,
        )
        if scan_files
        else delta_provider_from_session(
            ctx,
            table_uri=table_uri,
            storage_options=storage_options,
            version=version,
            timestamp=timestamp,
        )
    )
    adapter = DataFusionIOAdapter(ctx=ctx, profile=None)
    adapter.register_delta_table_provider(name, TableProviderCapsule(provider), overwrite=True)
```

### Target files to modify

- `src/datafusion_engine/registry_bridge.py`
- `src/datafusion_engine/scan_overrides.py`
- `src/datafusion_engine/scan_planner.py`
- `src/datafusion_engine/delta_control_plane.py` (new)
- `rust/datafusion_ext/src/lib.rs`

### Code and modules to delete

- `datafusion_ext.delta_table_provider(...)` Python entrypoints after all call sites use `delta_table_provider_from_session(...)` or `..._with_files(...)`.
- Any Arrow Dataset fallback registration paths for Delta tables.

### Status (2026-01-27)

- Delta provider construction now routes through the control-plane adapter in `src/datafusion_engine/delta_control_plane.py`.
- Canonical registration paths now use session-derived providers in `src/datafusion_engine/registry_bridge.py`, file-restricted providers in `src/datafusion_engine/scan_overrides.py`, and session-derived temporary providers in `src/datafusion_engine/write_pipeline.py`.
- Effective scan configuration and scan-file restrictions are recorded via provider artifacts in `src/datafusion_engine/registry_bridge.py` and scan-override artifacts in `src/datafusion_engine/scan_overrides.py`.

### Implementation checklist

- [x] Route all Delta provider construction through session-derived scan config.
- [x] Use file-restricted providers whenever scan units provide file sets.
- [ ] Remove non-session provider entrypoints once call sites are migrated.
- [x] Record effective scan config and file restrictions in artifacts.

---

## Scope 5 — Rust-native mutation APIs as the only commit path

**Intent**: make delta-rs operations in Rust the only supported way to commit data to Delta tables, even when DataFusion performs the compute work.

### Representative pattern

```rust
use std::collections::HashMap;

use datafusion::prelude::SessionContext;
use deltalake::protocol::SaveMode;
use deltalake::DeltaTableBuilder;

pub async fn delta_write_append(
    ctx: &SessionContext,
    table_uri: &str,
    storage_options: HashMap<String, String>,
    input: datafusion::prelude::DataFrame,
) -> Result<i64, deltalake::errors::DeltaTableError> {
    let mut builder = DeltaTableBuilder::from_uri(table_uri)?;
    builder = builder.with_storage_options(storage_options);
    let table = builder.load().await?;
    let batches = input.collect().await?;
    let table = table
        .write(batches)
        .with_save_mode(SaveMode::Append)
        .await?;
    Ok(table.version().unwrap_or_default())
}
```

### Target files to modify

- `rust/datafusion_ext/src/delta_mutations.rs` (new)
- `rust/datafusion_ext/src/lib.rs`
- `src/datafusion_engine/write_pipeline.py`
- `src/storage/deltalake/delta.py`
- `src/datafusion_engine/plan_artifact_store.py`
- `src/datafusion_engine/runtime.py`

### Code and modules to delete

- Python-first Delta mutation paths that bypass Rust and delta-rs operations once Rust mutation APIs are wired in.

### Status (2026-01-27)

- Rust mutation entrypoints now exist in `rust/datafusion_ext/src/delta_mutations.rs` and are exposed through `rust/datafusion_ext/src/lib.rs`.
- The Rust mutation surface accepts commit options including idempotent app-transaction inputs, but Python call sites still use delta-rs directly in `src/storage/deltalake/delta.py` and `src/datafusion_engine/write_pipeline.py`.

### Implementation checklist

- [x] Add Rust mutation entrypoints for append, overwrite, merge, update, and delete.
- [x] Ensure mutations accept explicit idempotent app transaction inputs.
- [ ] Run Delta constraint checking before mutation commits.
- [ ] Record mutation metadata (operation, mode, version, protocol/features) into plan artifacts.

---

## Scope 6 — Delta constraints and invariants enforced at the boundary

**Intent**: treat Delta constraints as a hard contract by running constraint checks in Rust before committing writes or mutations.

### Representative pattern

```python
from __future__ import annotations

from collections.abc import Mapping, Sequence

from datafusion import SessionContext


def enforce_delta_constraints(
    ctx: SessionContext,
    *,
    table_uri: str,
    storage_options: Mapping[str, str] | None,
    version: int | None,
    timestamp: str | None,
    batches_ipc: bytes,
    extra_constraints: Sequence[str],
) -> tuple[str, ...]:
    """Run Rust DeltaDataChecker and return violations."""
    from datafusion_engine.delta_control_plane import delta_data_check

    violations = delta_data_check(
        ctx,
        table_uri=table_uri,
        storage_options=storage_options,
        version=version,
        timestamp=timestamp,
        data_ipc=batches_ipc,
        extra_constraints=tuple(extra_constraints),
    )
    return tuple(sorted(set(violations)))
```

### Target files to modify

- `src/storage/deltalake/delta.py`
- `src/datafusion_engine/write_pipeline.py`
- `src/schema_spec/system.py`
- `rust/datafusion_ext/src/lib.rs`
- `rust/datafusion_ext/src/delta_mutations.rs` (new)

### Code and modules to delete

- Constraint enforcement paths that run only as best-effort diagnostics instead of a hard pre-commit gate.

### Status (2026-01-27)

- A Rust-backed Delta data checker is available via `datafusion_ext.delta_data_checker` and is wrapped in `src/storage/deltalake/delta.py`.
- Constraint enforcement is still SQL-first in `src/datafusion_engine/write_pipeline.py` and is not yet a hard Rust-side pre-commit gate.

### Implementation checklist

- [ ] Require constraint checks before all Delta commits (writes and mutations).
- [ ] Merge dataset-spec constraints with Delta-native constraints during pre-commit.
- [ ] Fail commits on any constraint violations.
- [ ] Record constraint-check results into plan artifacts.

---

## Scope 7 — CDF-first incremental execution model

**Intent**: make Delta CDF the default incremental surface and register it through Rust providers rather than bespoke materialization paths.

### Representative pattern

```python
from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from datafusion import SessionContext


@dataclass(frozen=True)
class DeltaCdfWindow:
    starting_version: int | None = None
    ending_version: int | None = None
    starting_timestamp: str | None = None
    ending_timestamp: str | None = None
    allow_out_of_range: bool = False


def register_delta_cdf(
    ctx: SessionContext,
    *,
    name: str,
    table_uri: str,
    storage_options: Mapping[str, str] | None,
    window: DeltaCdfWindow,
) -> None:
    """Register CDF via the Rust Delta CDF provider."""
    from datafusion_engine.delta_control_plane import delta_cdf_provider
    from datafusion_engine.io_adapter import DataFusionIOAdapter

    provider = delta_cdf_provider(
        table_uri=table_uri,
        storage_options=storage_options,
        starting_version=window.starting_version,
        ending_version=window.ending_version,
        starting_timestamp=window.starting_timestamp,
        ending_timestamp=window.ending_timestamp,
        allow_out_of_range=window.allow_out_of_range,
    )
    adapter = DataFusionIOAdapter(ctx=ctx, profile=None)
    adapter.register_delta_cdf_provider(name, provider)
```

### Target files to modify

- `src/datafusion_engine/registry_bridge.py`
- `src/storage/deltalake/delta.py`
- `src/incremental/changes.py`
- `src/incremental/cdf_filters.py`
- `src/incremental/snapshot.py`
- `src/incremental/delta_updates.py`
- `src/datafusion_engine/expr_spec.py`
- `rust/datafusion_ext/src/lib.rs`

### Code and modules to delete

- Incremental paths that do not use Delta CDF when CDF is available and required by dataset policy.

### Implementation checklist

- [ ] Add dataset policies that declare CDF as the required incremental source when enabled.
- [ ] Register CDF datasets exclusively through `DeltaCdfTableProvider`.
- [ ] Replace ad-hoc change-type logic with CDF UDFs and CDF providers.
- [ ] Record CDF window pins into plan artifacts.

---

## Scope 8 — Delta maintenance as a first-class, Rust-native subsystem

**Intent**: provide Rust-native maintenance operations (optimize/compact, vacuum, restore, feature enablement, and table properties) and make them explicit policy-driven flows.

### Representative pattern

```rust
use std::collections::HashMap;

use deltalake::DeltaTableBuilder;
use deltalake::table::config::TableProperty;

pub async fn delta_optimize_compact(
    table_uri: &str,
    storage_options: HashMap<String, String>,
    target_size: Option<i64>,
) -> Result<deltalake::operations::optimize::OptimizeMetrics, deltalake::errors::DeltaTableError> {
    let mut builder = DeltaTableBuilder::from_uri(table_uri)?;
    builder = builder.with_storage_options(storage_options);
    let table = builder.load().await?;
    let mut optimize = table.optimize();
    if let Some(size) = target_size {
        optimize = optimize.with_target_size(size);
    }
    let (_table, metrics) = optimize.await?;
    Ok(metrics)
}

pub async fn delta_set_properties(
    table_uri: &str,
    storage_options: HashMap<String, String>,
    properties: HashMap<String, String>,
) -> Result<(), deltalake::errors::DeltaTableError> {
    let mut builder = DeltaTableBuilder::from_uri(table_uri)?;
    builder = builder.with_storage_options(storage_options);
    let table = builder.load().await?;
    table
        .set_tbl_properties()
        .with_properties(properties)
        .await?;
    Ok(())
}
```

### Target files to modify

- `rust/datafusion_ext/src/delta_maintenance.rs` (new)
- `rust/datafusion_ext/src/lib.rs`
- `src/storage/deltalake/delta.py`
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/dataset_registry.py`
- `src/schema_spec/system.py`

### Code and modules to delete

- Maintenance flows that exist only as implicit side effects during writes instead of explicit, policy-driven operations.

### Status (2026-01-27)

- Rust maintenance entrypoints now exist in `rust/datafusion_ext/src/delta_maintenance.rs` and are exposed through `rust/datafusion_ext/src/lib.rs`.
- Current maintenance call sites remain Python-first in `src/engine/delta_tools.py` and `src/storage/deltalake/delta.py`.

### Implementation checklist

- [x] Add Rust maintenance entrypoints: optimize/compact, vacuum, restore, feature enablement, and table property updates.
- [ ] Define dataset-level maintenance policies and wire them into runtime hooks or explicit maintenance jobs.
- [ ] Enforce retention guardrails for vacuum and restore.
- [ ] Record maintenance metrics and resulting versions into plan artifacts.

---

## Scope 9 — Delta-aware scan planning with Add actions and stats

**Intent**: upgrade scan planning to reason over Delta Add actions and file-level statistics from the log, not just file paths and partition directory heuristics.

### Representative pattern

```rust
use deltalake::kernel::models::Add;
use deltalake::DeltaTableBuilder;

pub async fn delta_add_actions(
    table_uri: &str,
    storage_options: std::collections::HashMap<String, String>,
    version: Option<i64>,
) -> Result<Vec<Add>, deltalake::errors::DeltaTableError> {
    let mut builder = DeltaTableBuilder::from_uri(table_uri)?;
    builder = builder.with_storage_options(storage_options);
    if let Some(v) = version {
        builder = builder.with_version(v);
    }
    let table = builder.load().await?;
    let snapshot = table.snapshot()?.snapshot().clone();
    let adds = snapshot.file_actions()?;
    Ok(adds.collect())
}
```

```python
from __future__ import annotations

from collections.abc import Mapping, Sequence

from datafusion_engine.scan_planner import ScanUnit


def plan_scan_units_with_delta_stats(
    *,
    dataset_name: str,
    table_uri: str,
    storage_options: Mapping[str, str],
    pinned_version: int | None,
    required_columns: Sequence[str],
    pushed_filters: Sequence[str],
) -> ScanUnit:
    """Plan a scan using Delta Add actions and log stats."""
    from datafusion_engine.delta_control_plane import delta_add_actions
    from datafusion_engine.scan_stats_pruning import prune_add_actions

    adds = delta_add_actions(
        table_uri=table_uri,
        storage_options=storage_options,
        version=pinned_version,
    )
    pruned_files, pruned_version = prune_add_actions(
        adds=adds,
        required_columns=tuple(required_columns),
        pushed_filters=tuple(pushed_filters),
        pinned_version=pinned_version,
    )
    return ScanUnit(
        key=f"scan::{dataset_name}::{pruned_version}",
        dataset_name=dataset_name,
        delta_version=pruned_version,
        candidate_files=pruned_files,
        pushed_filters=tuple(pushed_filters),
        projected_columns=tuple(required_columns),
    )
```

### Target files to modify

- `rust/datafusion_ext/src/delta_control_plane.rs` (new)
- `rust/datafusion_ext/src/lib.rs`
- `src/datafusion_engine/scan_planner.py`
- `src/datafusion_engine/scan_overrides.py`
- `src/storage/deltalake/file_index.py`
- `src/storage/deltalake/file_pruning.py`
- `src/datafusion_engine/scan_stats_pruning.py` (new)

### Code and modules to delete

- Delta scan pruning logic that does not consider log-level Add actions and stats when those are available.

### Status (2026-01-27)

- The Rust control plane now exposes snapshot-aware add actions in `rust/datafusion_ext/src/delta_control_plane.rs` and `rust/datafusion_ext/src/lib.rs`.
- Scan planning now resolves add actions via the control plane in `src/datafusion_engine/scan_planner.py` and builds pruning indexes from those add actions in `src/storage/deltalake/file_index.py`.
- File-restricted provider registration now flows from pruned add actions through `src/datafusion_engine/scan_overrides.py`.
- Stats-aware pruning and scan-planning artifact metrics are not yet implemented.

### Implementation checklist

- [x] Expose Add actions and relevant stats from Rust control plane.
- [ ] Introduce stats-aware pruning that operates over Add actions.
- [x] Use file-restricted Delta providers derived from the pruned Add actions.
- [ ] Record scan planning statistics (candidate vs pruned files) into plan artifacts.

---

## Scope 10 — Delta observability as Delta tables (not just diagnostics blobs)

**Intent**: make Delta metadata and Delta operations themselves queryable via Delta tables, not just emitted as diagnostic blobs.

### Representative pattern

```python
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
import time

import pyarrow as pa

from storage.deltalake.delta import DeltaWriteOptions, idempotent_commit_properties, write_delta_table


DELTA_SNAPSHOT_SCHEMA = pa.schema(
    [
        pa.field("event_time_unix_ms", pa.int64(), nullable=False),
        pa.field("table_uri", pa.string(), nullable=False),
        pa.field("delta_version", pa.int64(), nullable=False),
        pa.field("min_reader_version", pa.int32(), nullable=False),
        pa.field("min_writer_version", pa.int32(), nullable=False),
        pa.field("reader_features_json", pa.string(), nullable=False),
        pa.field("writer_features_json", pa.string(), nullable=False),
        pa.field("table_properties_json", pa.string(), nullable=False),
        pa.field("schema_json", pa.string(), nullable=False),
        pa.field("partition_columns_json", pa.string(), nullable=False),
    ]
)


def record_delta_snapshot_artifact(
    *,
    table_uri: str,
    snapshot: Mapping[str, object],
    artifact_table_uri: str,
) -> int:
    """Persist Delta snapshot metadata as a Delta table row."""
    payload = {
        "event_time_unix_ms": int(time.time() * 1000),
        "table_uri": table_uri,
        "delta_version": int(snapshot["version"]),
        "min_reader_version": int(snapshot["min_reader_version"]),
        "min_writer_version": int(snapshot["min_writer_version"]),
        "reader_features_json": _json_text(snapshot.get("reader_features") or ()),
        "writer_features_json": _json_text(snapshot.get("writer_features") or ()),
        "table_properties_json": _json_text(snapshot.get("table_properties") or {}),
        "schema_json": str(snapshot.get("schema_json") or ""),
        "partition_columns_json": _json_text(snapshot.get("partition_columns") or ()),
    }
    table = pa.Table.from_pylist([payload], schema=DELTA_SNAPSHOT_SCHEMA)
    options = DeltaWriteOptions(
        mode="append",
        schema_mode="merge",
        commit_metadata={"operation": "delta_snapshot_artifact", "table_uri": table_uri},
        commit_properties=idempotent_commit_properties(
            operation="delta_snapshot_artifact",
            mode="append",
            extra_metadata={"table_uri": table_uri},
        ),
    )
    result = write_delta_table(table, artifact_table_uri, options=options)
    version = result.version
    if version is None:
        msg = "Delta snapshot artifact write did not resolve a version."
        raise RuntimeError(msg)
    return version
```

### Target files to modify

- `src/datafusion_engine/plan_artifact_store.py`
- `src/datafusion_engine/schema_registry.py`
- `src/incremental/registry_rows.py`
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/delta_observability.py` (new)
- `rust/datafusion_ext/src/delta_observability.rs` (new)

### Code and modules to delete

- Artifact paths that only emit opaque diagnostics for Delta snapshots, mutations, and maintenance where queryable Delta artifact tables exist.

### Implementation checklist

- [ ] Add Delta snapshot, mutation, scan-plan, and maintenance artifact tables.
- [ ] Record protocol versions and table features in artifact rows.
- [ ] Record scan-pruning metrics and file counts in artifact rows.
- [ ] Make artifact tables automatically registered in the session catalog.

---

## Scope 11 — Delta object-store and log-store policy as part of runtime identity

**Intent**: make storage and log-store configuration part of the runtime identity and plan determinism contract rather than an ad-hoc set of options passed along loosely.

### Representative pattern

```python
from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping


@dataclass(frozen=True)
class DeltaStorePolicy:
    """Runtime-level object store and log store configuration."""

    storage_options: Mapping[str, str]
    log_storage_options: Mapping[str, str]
    require_local_paths: bool = False


def resolve_delta_store_policy(
    *,
    table_uri: str,
    policy: DeltaStorePolicy,
) -> tuple[dict[str, str], dict[str, str]]:
    """Resolve effective storage and log-store options."""
    storage = dict(policy.storage_options)
    log_storage = dict(policy.log_storage_options)
    if policy.require_local_paths and "://" in table_uri:
        msg = "Remote Delta tables are disallowed by policy."
        raise ValueError(msg)
    return storage, log_storage
```

### Target files to modify

- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/dataset_registry.py`
- `src/schema_spec/system.py`
- `src/storage/deltalake/delta.py`
- `src/datafusion_engine/registry_bridge.py`
- `rust/datafusion_ext/src/delta_control_plane.rs` (new)

### Code and modules to delete

- Delta IO paths that allow storage and log-store options to drift silently across registration, planning, and execution.

### Implementation checklist

- [ ] Make storage and log-store options explicit runtime policy objects.
- [ ] Ensure storage policies participate in runtime hashing and plan fingerprints.
- [ ] Ensure the same effective options are used for snapshot info, providers, and mutations.
- [ ] Record effective options in plan artifacts.

---

## Scope 12 — Final decommissioning and deletions (only after all scopes above)

**Intent**: delete transitional and duplicate Delta surfaces only once the Rust control plane and its Python adapters fully replace them.

### Code and modules to delete at the end

- `datafusion_ext.delta_table_provider(...)` and any Python call sites that use it once session-derived provider entrypoints are fully adopted.
- Python mutation and maintenance paths that bypass Rust delta-rs operations.
- Any Delta scan fallback paths that do not go through provider registration (for example, dataset-based Delta scans).
- Any plan or scheduling surfaces that do not include Delta pins, protocol gates, and Delta snapshot metadata.

### Implementation checklist

- [ ] Remove non-session Delta provider entrypoints after all call sites migrate.
- [ ] Remove Python-first mutation and maintenance flows once Rust control-plane operations are adopted.
- [ ] Remove Delta scan fallbacks that bypass providers and pinned snapshots.
- [ ] Verify that all plan artifacts include Delta pins, protocol gates, and Delta snapshot metadata before deleting transitional surfaces.
