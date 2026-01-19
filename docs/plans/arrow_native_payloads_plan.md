# Arrow-Native Payloads Plan (No JSON In-Process)

## Purpose
Eliminate JSON usage from in-process payloads, metadata, caches, and diagnostics
by standardizing on Arrow-native representations (RecordBatch/IPC/Delta).
External export of JSON remains allowed at the boundary.

## Objectives
- Remove `json.*`/`orjson` usage from core execution, planning, and metadata paths.
- Replace JSON hashing/fingerprint steps with Arrow IPC hashing.
- Store diagnostics, telemetry, and policy artifacts as Arrow tables or Delta datasets.
- Preserve a minimal JSON boundary for external export and for required formats
  (e.g., Delta log JSON written by deltalake itself).
- Keep stable, deterministic serialization with explicit Arrow schemas.

## Non-goals
- Do not modify Delta log semantics (`_delta_log/*.json`) or third-party formats.
- Do not change external APIs that accept/emit JSON unless explicitly required.
- Do not introduce compatibility shims for removed JSON helpers.

## Universal practices (apply everywhere)
- **Arrow schemas first**: define explicit `pa.Schema` for every payload shape.
- **IPC bytes for hashing**: hash IPC stream bytes of a 1-row table, not JSON.
- **Delta for persistence**: persist diagnostics/caches as Delta tables where durable.
- **Structured columns**: store nested data as `struct`/`list` instead of JSON strings.
- **Boundary-only JSON**: JSON only at export boundaries or external protocols.

## Scope items

### Scope 1: Arrow payload utilities
Goal: create a common Arrow payload helper for IPC encoding and hashing.

Representative code patterns:
```python
import hashlib
import pyarrow as pa
import pyarrow.ipc as ipc


def payload_table(payload: dict[str, object], schema: pa.Schema) -> pa.Table:
    return pa.Table.from_pylist([payload], schema=schema)


def ipc_bytes(table: pa.Table) -> bytes:
    sink = pa.BufferOutputStream()
    with ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


def ipc_hash(table: pa.Table) -> str:
    return hashlib.sha256(ipc_bytes(table)).hexdigest()
```

Target files:
- `src/registry_common/arrow_payloads.py` (new)
- `src/engine/runtime_profile.py`
- `src/engine/function_registry.py`
- `src/datafusion_engine/runtime.py`

Implementation checklist:
- [ ] Add Arrow payload helper module with IPC + hash utilities.
- [ ] Standardize payload schemas for hashing in each caller.
- [ ] Replace JSON-based hashing/fingerprints with IPC hashes.

### Scope 2: Registry and runtime fingerprints
Goal: remove JSON fingerprints for registry/runtime profiles.

Representative code patterns:
```python
schema = pa.schema(
    [
        ("name", pa.string()),
        ("determinism_tier", pa.string()),
        ("scan_profile", pa.struct([("batch_size", pa.int64())])),
    ]
)
table = payload_table(payload, schema)
fingerprint = ipc_hash(table)
```

Target files:
- `src/engine/runtime_profile.py`
- `src/engine/function_registry.py`
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/function_factory.py`
- `src/sqlglot_tools/optimizer.py`
- `src/sqlglot_tools/bridge.py`
- `src/schema_spec/specs.py`
- `src/registry_common/metadata.py`

Implementation checklist:
- [ ] Define per-payload schemas for runtime and registry snapshots.
- [ ] Replace `json.dumps(...).encode(...)` with IPC hashing.
- [ ] Update any callers relying on JSON payload bytes.

### Scope 3: Metadata bytes and schema annotations
Goal: replace JSON metadata blobs with Arrow IPC bytes.

Representative code patterns:
```python
def metadata_bytes(payload: dict[str, object], schema: pa.Schema) -> bytes:
    return ipc_bytes(payload_table(payload, schema))
```

Target files:
- `src/registry_common/metadata.py`
- `src/normalize/registry_rows.py`
- `src/arrowdsl/schema/metadata.py` (remove once ArrowDSL removed)

Implementation checklist:
- [ ] Replace metadata JSON bytes with IPC bytes.
- [ ] Update schema metadata readers to decode IPC.
- [ ] Ensure metadata payload schemas are versioned.

### Scope 4: Diagnostics tables (Arrow-native)
Goal: remove JSON columns and store structured diagnostics as Arrow tables.

Representative code patterns:
```python
rows_payload = pa.Table.from_batches(batches)
row_meta = {
    "artifact_format": "ipc_stream",
    "schema_fingerprint": schema_fingerprint(rows_payload.schema),
}
```

Target files:
- `src/obs/diagnostics_tables.py`
- `src/obs/repro.py`
- `src/hamilton_pipeline/modules/outputs.py`

Implementation checklist:
- [ ] Replace JSON columns (e.g., `explain_rows_json`) with structured fields.
- [ ] Persist row payloads as IPC artifacts or Delta tables.
- [ ] Update diagnostics schemas to include IPC metadata columns.

### Scope 5: Policy and cache payloads
Goal: replace JSON policy blobs and cache entries with Arrow payloads.

Representative code patterns:
```python
def decode_payload(blob: bytes) -> pa.Table:
    reader = ipc.open_stream(blob)
    return reader.read_all()
```

Target files:
- `src/relspec/policies.py`
- `src/normalize/policies.py`
- `src/relspec/rules/cache.py`
- `src/relspec/rules/graph.py`
- `src/relspec/engine.py`

Implementation checklist:
- [ ] Replace JSON payload reads with IPC decode.
- [ ] Store policies/cache entries as IPC bytes or Delta rows.
- [ ] Version payload schemas and maintain compatibility.

### Scope 6: SQLGlot AST and SQL diagnostics
Goal: remove JSON AST dumps and store SQL/textual forms or Arrow-structured AST.

Representative code patterns:
```python
sql_text = expr.sql(dialect="datafusion")
payload = {"sql": sql_text, "dialect": "datafusion"}
```

Target files:
- `src/obs/repro.py`
- `src/hamilton_pipeline/modules/outputs.py`
- `src/sqlglot_tools/optimizer.py`

Implementation checklist:
- [ ] Replace JSON AST payloads with SQL strings + metadata.
- [ ] If AST structure is required, define a structured Arrow schema for nodes.
- [ ] Keep JSON only for external export (optional).

### Scope 7: Incremental state and fingerprints
Goal: replace JSON state files with Delta or IPC datasets.

Representative code patterns:
```python
from deltalake import write_deltalake

write_deltalake(path, table, mode="overwrite", storage_options=storage_options)
```

Target files:
- `src/incremental/invalidations.py`
- `src/incremental/fingerprint_changes.py`
- `src/incremental/diff.py`

Implementation checklist:
- [ ] Define Arrow schemas for incremental state artifacts.
- [ ] Store state in Delta tables instead of JSON files.
- [ ] Update readers to load Arrow/Delta tables.

### Scope 8: Runtime inspection and env payloads
Goal: replace JSON env payloads with non-JSON encodings.

Representative code patterns:
```python
allowlist = os.environ.get("CODEANATOMY_ALLOWLIST", "")
modules = [item.strip() for item in allowlist.split(",") if item.strip()]
```

Target files:
- `src/extract/runtime_inspect_extract.py`

Implementation checklist:
- [ ] Replace JSON allowlist env parsing with CSV/line-based parsing.
- [ ] Ensure outputs remain Arrow-native (no JSON payloads).

### Scope 9: ArrowDSL JSON removal (coordinated with decommission)
Goal: remove ArrowDSL JSON helpers and codec usage.

Target files:
- `src/arrowdsl/json_factory.py`
- `src/arrowdsl/spec/codec.py`
- `src/arrowdsl/schema/serialization.py`
- `src/arrowdsl/spec/expr_ir.py`
- `src/arrowdsl/plan/plan.py`

Implementation checklist:
- [ ] Delete ArrowDSL JSON utilities with ArrowDSL removal.
- [ ] Replace any lingering JSON codecs with Arrow IPC.

### Scope 10: Tests and fixtures
Goal: update tests to validate Arrow payloads instead of JSON strings.

Target files:
- `tests/unit/test_sql_ingest_guardrails.py`
- `tests/unit/test_rule_ir_artifacts.py`
- `tests/integration/test_rule_semantics.py`
- `tests/e2e/test_full_pipeline_repo.py`

Implementation checklist:
- [ ] Replace JSON fixtures with Arrow IPC/Delta fixtures.
- [ ] Update assertions to use Arrow schema + data checks.
- [ ] Add targeted unit tests for IPC hashing stability.

### Scope 11: Guardrails (optional)
Goal: prevent new JSON usage in core code paths.

Representative code patterns:
```bash
rg -n "\\bimport json\\b|\\borjson\\b|\\bujson\\b" src
```

Target files:
- `scripts/validate_no_json.sh` (optional)

Implementation checklist:
- [ ] Add a dev-only script to detect JSON usage in `src`.
- [ ] Define an allowlist for boundary-only modules.

## Acceptance checklist
- [ ] No JSON usage in core `src` code paths (except defined boundaries).
- [ ] All hashes/fingerprints use Arrow IPC bytes.
- [ ] Diagnostics/telemetry stored as Arrow tables or Delta datasets.
- [ ] Policy/caches decode from IPC/Delta, not JSON.
- [ ] External JSON exports remain optional and isolated.
