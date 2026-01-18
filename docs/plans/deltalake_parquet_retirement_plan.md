Below is a comprehensive implementation plan for fully pivoting pipeline outputs
from Parquet to Delta Lake storage. Each scope item includes representative code
snippets, target files, and a concrete implementation checklist.

---

## Scope 1: Output Format Gate (Delta-only enforcement)

**Objective:** Prevent any runtime pathway from writing Parquet outputs and
make Delta the only sanctioned output storage format.

**Pattern snippet**
```python
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


OutputStorageFormat = Literal["delta"]


@dataclass(frozen=True)
class OutputStoragePolicy:
    format: OutputStorageFormat = "delta"
    allow_parquet_exports: bool = False


def require_delta_output(policy: OutputStoragePolicy, *, sink: str) -> None:
    if policy.format != "delta":
        msg = f"{sink} output requires Delta storage."
        raise ValueError(msg)
```

**Target files**
- `src/hamilton_pipeline/modules/inputs.py`
- `src/hamilton_pipeline/modules/outputs.py`
- `src/ibis_engine/io_bridge.py`
- `src/datafusion_engine/bridge.py`

**Implementation checklist**
- [ ] Add an output storage policy to output config (default: delta only).
- [ ] Block DataFusion `COPY TO ... FORMAT parquet` when policy forbids Parquet.
- [ ] Fail fast if any parquet write path is invoked in production modes.
- [ ] Emit diagnostics when a forbidden parquet sink is requested.

---

## Scope 2: DataFusion write path -> Delta TableProvider or delta-rs fallback

**Objective:** Replace DataFusion `write_parquet` sinks with Delta writes,
preferring TableProvider `insert_into`, falling back to delta-rs commit.

**Pattern snippet**
```python
from __future__ import annotations

from datafusion import DataFrame

from arrowdsl.io.delta import DeltaWriteOptions, write_table_delta


def write_dataframe_delta(
    df: DataFrame,
    *,
    table_name: str,
    base_dir: str,
    options: DeltaWriteOptions,
) -> str:
    writer = getattr(df, "write_table", None)
    if callable(writer):
        writer(table_name)
        return base_dir
    arrow_table = df.to_arrow_table()
    result = write_table_delta(arrow_table, base_dir, options=options)
    return result.path
```

**Target files**
- `src/ibis_engine/io_bridge.py`
- `src/datafusion_engine/registry_bridge.py`
- `src/arrowdsl/io/delta.py`

**Implementation checklist**
- [ ] Register Delta sink tables as DataFusion TableProviders (where supported).
- [ ] Map write modes to Delta table provider `insert_into` semantics.
- [ ] Fall back to delta-rs `write_table_delta` when `write_table` is unavailable.
- [ ] Preserve commit metadata and retry policy on fallback writes.

---

## Scope 3: Retire Parquet writer APIs in core IO surfaces

**Objective:** Remove Parquet writers from runtime storage surfaces and
re-home them as optional export utilities only.

**Pattern snippet**
```python
from __future__ import annotations

from arrowdsl.io.delta import write_dataset_delta


def write_dataset_storage(table, *, base_dir: str, options) -> str:
    result = write_dataset_delta(table, base_dir, options=options)
    return result.path
```

**Target files**
- `src/arrowdsl/io/parquet.py`
- `src/storage/io.py`
- `src/storage/__init__.py`
- `src/ibis_engine/io_bridge.py`

**Implementation checklist**
- [ ] Remove Parquet writer exports from `storage` default surface.
- [ ] Delete or relocate `write_*_parquet` APIs into an explicit export module.
- [ ] Update any import sites to use Delta writers instead.
- [ ] Enforce delta-only in public `write_ibis_*` helpers.

---

## Scope 4: Replace Parquet metadata sidecars with Delta metadata

**Objective:** Replace Parquet `_metadata`/row-group stats with Delta-native
history, protocol, and data-skipping metadata.

**Pattern snippet**
```python
from __future__ import annotations

from arrowdsl.io.delta import delta_history_snapshot, delta_protocol_snapshot


def delta_metadata_payload(path: str) -> dict[str, object]:
    return {
        "protocol": delta_protocol_snapshot(path),
        "history": delta_history_snapshot(path),
    }
```

**Target files**
- `src/obs/manifest.py`
- `src/obs/repro.py`
- `src/ibis_engine/io_bridge.py`

**Implementation checklist**
- [ ] Remove Parquet metadata sidecar writes for output artifacts.
- [ ] Capture Delta history/protocol snapshots in reports and manifests.
- [ ] Record data skipping stats columns in commit metadata.
- [ ] Preserve ordering/constraint metadata as Delta commit properties.

---

## Scope 5: Partitioned outputs -> Delta partition policies

**Objective:** Replace partitioned Parquet writers with Delta partitioned
writes and targeted overwrite semantics.

**Pattern snippet**
```python
from __future__ import annotations

from arrowdsl.io.delta import DeltaWriteOptions, write_table_delta


def write_partitioned_delta(table, *, path: str, partition_by: list[str]) -> str:
    options = DeltaWriteOptions(mode="overwrite", partition_by=partition_by)
    result = write_table_delta(table, path, options=options)
    return result.path
```

**Target files**
- `src/ibis_engine/io_bridge.py`
- `src/arrowdsl/io/delta.py`
- `src/hamilton_pipeline/modules/outputs.py`

**Implementation checklist**
- [ ] Replace `write_partitioned_dataset_parquet` with Delta partitioned writes.
- [ ] Use `predicate` or partition lists for targeted overwrites.
- [ ] Emit per-partition metadata in diagnostics artifacts.

---

## Scope 6: Config surface cleanup (Parquet options -> Delta policies)

**Objective:** Remove Parquet-specific config from output options and replace
them with Delta policies and writer properties.

**Pattern snippet**
```python
from __future__ import annotations

from deltalake import WriterProperties

from arrowdsl.io.delta import DeltaWriteOptions
from arrowdsl.io.delta_config import DeltaWritePolicy


def delta_write_options(policy: DeltaWritePolicy | None) -> DeltaWriteOptions:
    writer_props = WriterProperties(compression="zstd")
    return DeltaWriteOptions(writer_properties=writer_props)
```

**Target files**
- `src/ibis_engine/io_bridge.py`
- `src/schema_spec/specs.py`
- `src/hamilton_pipeline/pipeline_types.py`
- `src/hamilton_pipeline/modules/inputs.py`

**Implementation checklist**
- [ ] Deprecate `ParquetWriteOptions` in dataset write configs.
- [ ] Map compression/row-group settings into Delta writer properties.
- [ ] Align `DataFusionWritePolicy` with Delta policy fields where possible.
- [ ] Ensure delta policy payloads appear in output reports.

---

## Scope 7: Tests and docs migration (Delta-first)

**Objective:** Replace parquet-centric tests and documentation with Delta-first
equivalents.

**Pattern snippet**
```python
from __future__ import annotations

from arrowdsl.io.delta import DeltaWriteOptions, delta_history_snapshot, write_table_delta


def test_delta_history_records_commit(tmp_path):
    table_path = str(tmp_path / "table")
    write_table_delta(table, table_path, options=DeltaWriteOptions(mode="overwrite"))
    assert delta_history_snapshot(table_path) is not None
```

**Target files**
- `tests/unit/test_parquet_sorting_metadata.py`
- `tests/unit/test_manifest_row_group_stats.py`
- `docs/plans/deltalake_migration_implementation_plan.md`
- `docs/plans/additional_advanced_integrations_implementation_plan.md`

**Implementation checklist**
- [ ] Replace parquet writer unit tests with Delta commit/metadata tests.
- [ ] Remove parquet metadata assertions from manifests.
- [ ] Update plan/docs to position Parquet as explicit export-only (or removed).

---

## Scope 8: CLI/tooling updates (Delta-only outputs)

**Objective:** Ensure CLIs do not emit Parquet as a default output format.

**Pattern snippet**
```python
from __future__ import annotations

from arrowdsl.io.delta import DeltaWriteOptions, write_table_delta


def clone_delta_snapshot(source: str, target: str) -> None:
    table = open_delta_table(source)
    write_table_delta(table.to_pyarrow_table(), target, options=DeltaWriteOptions(mode="overwrite"))
```

**Target files**
- `scripts/delta_export_snapshot.py`
- `scripts/migrate_parquet_to_delta.py`
- `scripts/substrait_validation_harness.py`

**Implementation checklist**
- [ ] Replace Delta snapshot export with Delta-to-Delta clone (no Parquet output).
- [ ] Keep parquet migration tooling only for legacy ingestion paths.
- [ ] Adjust validation harness to prefer Delta inputs or in-memory tables.

---

## Execution Order (Recommended)
1) Output format gate + config surface cleanup
2) DataFusion write path -> Delta TableProvider/fallback
3) Replace Parquet writer APIs in IO surfaces
4) Partitioned output migration
5) Metadata/manifest rewiring
6) Tests and docs migration
7) CLI/tooling updates
