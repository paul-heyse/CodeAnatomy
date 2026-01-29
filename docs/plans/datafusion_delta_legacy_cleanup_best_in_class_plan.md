# DataFusion + Delta Legacy Cleanup Best‑in‑Class Implementation Plan

## Purpose
Define a best‑in‑class, breaking‑change plan to remove legacy record‑batch registration paths and ad‑hoc JSON payload columns, replacing them with canonical structured or msgspec‑encoded artifacts. This plan targets an ideal end‑state design (design‑phase, breaking changes acceptable).

## Design Principles (Non‑Negotiable)
- **Single ingestion surface**: one canonical Arrow ingestion path; no hidden fallbacks.
- **Structured artifacts first**: nested data stored as Arrow structs/lists or msgspec bytes, not JSON strings.
- **Determinism & traceability**: stable, schema‑registered payloads with explicit versions.
- **Hard cutover**: remove legacy tables/paths immediately; no dual‑write or backfill.
- **Operational clarity**: explicit cutover checklist and deletion scope.
- **Breaking changes acceptable** when they yield a simpler, safer architecture.

---

## Status (2026-01-29)
- Scopes 1–8 are implemented in the codebase.
- Remaining work is the full verification pass (tests + checks) noted in Scope 9.

---

## Scope 1 — Remove Record‑Batch Registration Path (Single Ingest Surface)

### Goal
Eliminate `register_record_batches` and all record‑batch fallbacks. All ingestion uses one canonical path (DataFusion native or a unified adapter that does not rely on record‑batch registration).

### Representative Code Patterns
```python
# Canonical ingest: no record‑batch fallback
ctx = runtime.session_context()
df = datafusion_from_arrow(ctx, name="events", value=table)
```

```python
# If partitioned ingestion is required, provide a native partitioning interface
# (e.g., via datafusion-native or memtable builder, not register_record_batches)
partitions = build_partitions(table, batch_size=1000)
df = ctx.from_arrow_partitions(partitions, name="events")
```

### Target Files to Modify
- `src/datafusion_engine/ingest.py`
- `src/datafusion_engine/io_adapter.py`
- `src/datafusion_engine/schema_validation.py`
- `src/datafusion_engine/finalize.py`
- `src/datafusion_engine/encoding.py`
- `src/datafusion_engine/nested_tables.py`
- `src/datafusion_engine/runtime.py`
- `src/storage/deltalake/file_pruning.py`
- `src/storage/deltalake/delta.py`
- `src/incremental/runtime.py`
- `tests/unit/test_from_arrow_ingest.py`
- `tests/unit/test_schema_adapter_metadata.py`

### Modules / Code to Delete
- `DataFusionIOAdapter.register_record_batches` and helpers
- `_register_record_batches` and `_record_batches` in `ingest.py`
- Any remaining call sites that register record batches directly

### Implementation Checklist
- [x] Define a single ingestion API surface and required partitioning interface.
- [x] Replace all `register_record_batches` call sites with the canonical ingest method.
- [x] Remove fallback branches that silently register record batches.
- [x] Update ingestion diagnostics to reflect the canonical path only.
- [x] Update tests to assert the canonical ingest pathway and remove record‑batch expectations.
- [x] Confirm no runtime paths require record‑batch registration for correctness/performance.

---

## Scope 2 — Plan Artifacts v8 (Structured/Msgspec Payloads)

### Goal
Replace `datafusion_plan_artifacts_v7` JSON string columns with structured Arrow columns or msgspec bytes, creating a new v8 schema.

### Representative Code Patterns
```python
# Store nested payloads as msgspec bytes
class PlanArtifactsV8(StructBaseStrict, frozen=True):
    lineage: LineagePayload
    scan_units: tuple[ScanUnitPayload, ...]
    plan_details: PlanDetailsPayload

payload = PlanArtifactsV8(...)
row = {
    "lineage_msgpack": dumps_msgpack(payload.lineage),
    "scan_units_msgpack": dumps_msgpack(payload.scan_units),
    "plan_details_msgpack": dumps_msgpack(payload.plan_details),
}
```

```python
# Registry schema with explicit binary fields
pa.field("lineage_msgpack", pa.binary(), nullable=False)
```

### Target Files to Modify
- `src/datafusion_engine/plan_artifact_store.py`
- `src/datafusion_engine/schema_registry.py`
- `src/serde_artifacts.py`
- `src/serde_schema_registry.py`
- `src/incremental/registry_rows.py`
- `tests/plan_golden/*`
- `tests/msgspec_contract/*`

### Modules / Code to Delete
- Legacy v7 schema + JSON columns (removed at cutover)
- Any readers/registrations that only support v7 JSON layout

### Implementation Checklist
- [x] Define v8 schema (structured or msgspec bytes) for plan artifacts.
- [x] Switch writers and readers to v8 only (single table name).
- [x] Remove v7 schema registration, table names, and JSON column handling.
- [x] Delete v7 readers and registration helpers.
- [x] Update plan‑golden and msgspec contract tests to v8 only.

---

## Scope 3 — Write Artifacts v2 (Structured Policies & Metadata)

### Goal
Replace write artifact JSON fields (`*_policy_json`, `commit_metadata_json`) with structured columns or msgspec bytes.

### Representative Code Patterns
```python
class WriteArtifactV2(StructBaseStrict, frozen=True):
    delta_write_policy: DeltaWritePolicy
    delta_schema_policy: DeltaSchemaPolicy
    commit_metadata: dict[str, str]

row = {
    "delta_write_policy_msgpack": dumps_msgpack(artifact.delta_write_policy),
    "delta_schema_policy_msgpack": dumps_msgpack(artifact.delta_schema_policy),
    "commit_metadata": artifact.commit_metadata,
}
```

### Target Files to Modify
- `src/datafusion_engine/plan_artifact_store.py`
- `src/datafusion_engine/schema_registry.py`
- `src/serde_artifacts.py`
- `src/serde_schema_registry.py`
- `tests/msgspec_contract/*`

### Modules / Code to Delete
- JSON‑encoded write policy and commit metadata columns in v1
- v1 write‑artifact reader/registration paths

### Implementation Checklist
- [x] Define `write_artifact_v2` schema with structured fields.
- [x] Switch writers and readers to v2 only (single table name).
- [x] Remove v1 schema registration and JSON column handling.
- [x] Delete v1 reader/registration paths.

---

## Scope 4 — View Artifacts: Structured Schema Describe/Provenance

### Goal
Replace `schema_describe_json` and `schema_provenance_json` with structured columns or msgspec bytes.

### Representative Code Patterns
```python
class ViewSchemaDescribe(StructBaseStrict, frozen=True):
    name: str
    dtype: str
    nullable: bool

row = {
    "schema_describe_msgpack": dumps_msgpack(schema_describe),
    "schema_provenance_msgpack": dumps_msgpack(schema_provenance),
}
```

### Target Files to Modify
- `src/datafusion_engine/view_artifacts.py`
- `src/datafusion_engine/schema_registry.py`
- `src/serde_artifacts.py`

### Modules / Code to Delete
- JSON string columns for schema describe/provenance
- Any view artifact readers expecting JSON strings

### Implementation Checklist
- [x] Define structured or msgspec fields for schema describe/provenance.
- [x] Switch writers/readers to v4 only and update schema registry.
- [x] Remove v3 schema registration and JSON columns.

---

## Scope 5 — Run Manifest v2 (Structured Lists/Maps)

### Goal
Replace `plan_fingerprints_json`, `delta_inputs_json`, `outputs_json`, `artifact_ids_json` with structured columns or msgspec bytes.

### Representative Code Patterns
```python
class RunManifestV2(StructBaseStrict, frozen=True):
    plan_fingerprints: dict[str, str]
    delta_inputs: tuple[DeltaInputPin, ...]
    outputs: tuple[OutputDescriptor, ...]

row = {
    "plan_fingerprints": manifest.plan_fingerprints,
    "delta_inputs_msgpack": dumps_msgpack(manifest.delta_inputs),
    "outputs_msgpack": dumps_msgpack(manifest.outputs),
}
```

### Target Files to Modify
- `src/hamilton_pipeline/modules/outputs.py`
- `src/serde_artifacts.py`
- `src/datafusion_engine/schema_registry.py`
- `tests/msgspec_contract/*`

### Modules / Code to Delete
- JSON string columns for run manifest payloads
- Any run‑manifest consumers relying on JSON column names

### Implementation Checklist
- [x] Define v2 manifest schema with structured fields.
- [x] Switch writers/readers to v2 only (single table name).
- [x] Remove v1 schema registration and JSON column handling.

---

## Scope 6 — Delta Observability v2 (Structured Protocol/Metadata)

### Goal
Move delta observability JSON strings (protocol, table properties, metrics, commit metadata) into structured fields or msgspec bytes.

### Representative Code Patterns
```python
class DeltaProtocolSnapshotPayload(StructBaseStrict, frozen=True):
    reader_features: tuple[str, ...]
    writer_features: tuple[str, ...]

row = {
    "delta_protocol_msgpack": dumps_msgpack(protocol_payload),
    "commit_metadata": commit_metadata,
}
```

### Target Files to Modify
- `src/datafusion_engine/delta_observability.py`
- `src/datafusion_engine/schema_registry.py`
- `src/serde_artifacts.py`

### Modules / Code to Delete
- `*_json` columns for protocol/metrics/commit metadata
- Observability readers expecting JSON strings

### Implementation Checklist
- [x] Define v2 observability schema with structured fields.
- [x] Switch writers/readers to v2 only and update schema registry.
- [x] Remove v1 schema registration and JSON column handling.

---

## Scope 7 — Remove JSON Outputs (Artifact‑Backed)

### Goal
Remove JSON outputs entirely and replace them with Delta‑backed artifacts or structured outputs that flow through the same artifact/manifest pathways.

### Representative Code Patterns
```python
# Structured artifact output recorded alongside other manifests
artifact = NormalizeOutputsArtifact(...)
record_artifact(runtime_profile, "normalize_outputs_v1", to_builtins(artifact, str_keys=True))
_output_table_write(..., table_name="normalize_outputs_v1", ...)
```

### Target Files to Modify
- `src/hamilton_pipeline/modules/outputs.py`
- `src/hamilton_pipeline/cache_lineage.py`
- `src/hamilton_pipeline/execution.py`
- `src/hamilton_pipeline/pipeline_types.py`

### Modules / Code to Delete
- `write_cpg_props_json_delta` output and the `cpg_props_json` output slot.
- Any JSON/JSONL output paths replaced by structured artifacts.

### Implementation Checklist
- [x] Identify all JSON outputs and migrate them to structured artifacts.
- [x] Replace JSON/JSONL output files with Delta‑backed metadata tables.
- [x] Remove redundant JSON output datasets and pipeline wiring.

---

## Scope 8 — Schema Registry + Msgspec Contract Harmonization

### Goal
Ensure all new v2/v8 schemas are registered and fully covered by msgspec contract tests.

### Representative Code Patterns
```python
SCHEMA_TYPES = (
    PlanArtifactsV8,
    WriteArtifactV2,
    RunManifestV2,
    DeltaObservabilityV2,
)
```

### Target Files to Modify
- `src/serde_schema_registry.py`
- `tests/msgspec_contract/*`

### Modules / Code to Delete
- Legacy v1/v7/v3 schema registrations at cutover

### Implementation Checklist
- [x] Add new schema types to registry.
- [x] Regenerate msgspec schema goldens.
- [x] Remove legacy schemas and readers as part of cutover.

---

## Scope 9 — Legacy Removal (Immediate Cutover)

These removals must happen once the new schemas are the only active paths (no backfill/dual‑write).

### Deletions
- [x] Remove legacy `datafusion_plan_artifacts_v7` tables, constants, and JSON columns.
- [x] Remove legacy `write_artifact_v1` tables, constants, and JSON columns.
- [x] Remove legacy run manifest v1 tables/constants and JSON columns.
- [x] Remove delta observability v1 tables/constants and JSON columns.
- [x] Remove remaining ad‑hoc JSON payload emitters where structured replacements exist.

### Checklist
- [x] Verify all consumers read v2/v4/v8 schemas only.
- [x] Remove legacy artifacts and update documentation.
- [ ] All tests (unit/integration/plan golden/msgspec) pass.

---

## Notes on Breaking Changes
- Plan artifact schema cutover (v7 → v8) is a breaking change.
- Write artifact schema cutover (v1 → v2) is a breaking change.
- Run manifest and observability schema cutovers are breaking changes.
- Removal of record‑batch registration is a breaking change for any workflow that depends on that ingestion fallback.
