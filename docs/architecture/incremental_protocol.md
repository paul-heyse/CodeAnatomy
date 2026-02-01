# Incremental Protocol

This document describes the semantic incremental protocol. The canonical
implementation lives in `src/semantics/incremental/` and supersedes all legacy
incremental modules.

## Goals

- Track **what changed** using Delta Lake Change Data Feed (CDF).
- Avoid reprocessing history with **cursor-based version tracking**.
- Merge incremental outputs using **explicit merge strategies**.
- Treat incremental behavior as a first-class protocol in the semantic pipeline.

## Canonical Modules

- `cdf_cursors.py`
  - `CdfCursor` and `CdfCursorStore` for version checkpoints.
- `cdf_reader.py`
  - `read_cdf_changes` with automatic cursor updates.
- `cdf_types.py`
  - `CdfChangeType` and `CdfFilterPolicy`.
- `cdf_joins.py`
  - `CDFJoinSpec`, `CDFMergeStrategy`, and merge helpers.
- `delta_updates.py`
  - Partitioned and overwrite writes for incremental materialization.
- `runtime.py`
  - `IncrementalRuntime` and `TempTableRegistry`.
- `state_store.py`
  - State directory layout for incremental artifacts.
- `plan_fingerprints.py` / `invalidations.py`
  - Plan fingerprint snapshots and invalidation logic.

## Core Workflow

1. **Resolve inputs** and compute plan bundles for semantic tasks.
2. **Read CDF** for changed datasets via `read_cdf_changes`.
3. **Compute impacted tasks** from evidence and plan fingerprints.
4. **Build incremental results** using semantic join builders.
5. **Merge outputs** with `CDFMergeStrategy` (upsert, replace, delete/insert).
6. **Update cursors** on successful CDF reads.

## Configuration

`IncrementalConfig` (`semantics.incremental.config`) controls:

- `enabled`: enable/disable incremental mode
- `state_dir`: cursor + state storage root
- `cdf_filter_policy`: change-type selection
- `default_merge_strategy`: default merge strategy
- `impact_strategy`: closure strategy for impacted files

## Cursor Tracking

- `CdfCursorStore` persists per-dataset cursors under `<state_dir>/cursors/`.
- Cursors record the last processed Delta version and a timestamp.
- CDF reads start at `last_version + 1` and update cursors on success.

## Merge Strategies

- **UPSERT**: merge incremental results by key columns.
- **REPLACE**: delete partitions then append.
- **APPEND**: add rows without deletes.
- **DELETE_INSERT**: delete by keys then insert.

Merge behavior is explicit and tied to dataset row metadata (`merge_keys`).

## Storage Layout

```
<state_dir>/
  cursors/
  delta/
  snapshots/
  plan_fingerprints/
```

Dataset directories and metadata are managed by `StateStore`.

## Invariants

- CDF cursors are always updated after successful reads.
- Merge keys and partition columns are explicit and validated.
- Incremental outputs are materialized through the semantic catalog.

