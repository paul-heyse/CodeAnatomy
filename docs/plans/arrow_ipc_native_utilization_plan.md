 # Arrow IPC Native Utilization Plan
 
 ## Purpose
 Replace remaining JSON-centric assessment and serialization paths with Arrow-native
 IPC payloads and compute-kernel checks, aligned with the Arrow-native payloads
 conversion work.
 
 ## Objectives
 - Eliminate JSON round-trips in in-process payload checks and fingerprints.
 - Standardize IPC schemas for all payloads and assessment artifacts.
 - Prefer Arrow-native compute kernels for validation and diagnostics.
 - Preserve JSON only at explicit export boundaries.
 
 ## Scope Items
 
 ### Scope 1: Registry and signature hashing (IPC-native)
 Replace JSON-based registry signature hashing with IPC payload hashing so the
 digest is derived from structured Arrow rows.
 
 Representative pattern:
 ```python
 import pyarrow as pa
 
 from registry_common.arrow_payloads import payload_hash
 
 _REGISTRY_SIG_ENTRY = pa.struct(
     [
         pa.field("name", pa.string(), nullable=False),
         pa.field("table_ipc_hash", pa.string(), nullable=False),
     ]
 )
 _REGISTRY_SIG_SCHEMA = pa.schema(
     [
         pa.field("version", pa.int32(), nullable=False),
         pa.field("registry", pa.string(), nullable=False),
         pa.field("entries", pa.list_(_REGISTRY_SIG_ENTRY), nullable=False),
     ]
 )
 
 payload = {
     "version": 1,
     "registry": registry_name,
     "entries": [
         {"name": name, "table_ipc_hash": table_hash}
         for name, table_hash in sorted(table_hashes.items())
     ],
 }
 signature = payload_hash(payload, _REGISTRY_SIG_SCHEMA)
 ```
 
 Target files:
 - `src/storage/deltalake/registry_freshness.py`
 - `src/registry_common/arrow_payloads.py`
 
 Implementation checklist:
 - [ ] Define a registry signature IPC schema.
 - [ ] Replace JSON digest with `payload_hash`.
 - [ ] Update any signature source metadata to reflect IPC usage.
 
 ### Scope 2: Diagnostics payloads and artifacts (IPC-native)
 Replace JSON payload fields with structured Arrow tables or IPC bytes. Ensure
 diagnostics rows reference IPC artifacts by schema fingerprint and format.
 
 Representative pattern:
 ```python
 from arrowdsl.io.ipc import write_table_ipc_file
 from arrowdsl.schema.serialization import schema_fingerprint
 
 def diagnostics_rows_payload(
     table: pa.Table,
     *,
     output_path: Path,
 ) -> dict[str, object]:
     return {
         "artifact_path": write_table_ipc_file(table, output_path, overwrite=True),
         "artifact_format": "ipc_file",
         "schema_fingerprint": schema_fingerprint(table.schema),
     }
 ```
 
 Target files:
 - `src/obs/diagnostics_tables.py`
 - `src/obs/repro.py`
 - `src/hamilton_pipeline/modules/outputs.py`
 
 Implementation checklist:
 - [ ] Replace JSON diagnostics payloads with structured Arrow tables.
 - [ ] Ensure schema fingerprints are recorded for all diagnostics artifacts.
 - [ ] Store IPC artifacts under diagnostics paths instead of JSON files.
 
 ### Scope 3: Runtime profile and function registry hashes (IPC-native)
 Use IPC payload hashing for runtime profiles, function catalogs, and registry
 snapshots, removing any JSON round-trip used solely for hashing.
 
 Representative pattern:
 ```python
 import pyarrow as pa
 
 from registry_common.arrow_payloads import payload_hash
 
 _FUNCTION_CATALOG_SCHEMA = pa.schema(
     [
         pa.field("version", pa.int32()),
         pa.field("entries", pa.list_(
             pa.struct(
                 [
                     ("function_name", pa.string()),
                     ("function_type", pa.string()),
                     ("source", pa.string()),
                 ]
             )
         )),
     ]
 )
 
 payload = {"version": 1, "entries": catalog_rows}
 catalog_hash = payload_hash(payload, _FUNCTION_CATALOG_SCHEMA)
 ```
 
 Target files:
 - `src/engine/runtime_profile.py`
 - `src/engine/function_registry.py`
 - `src/datafusion_engine/runtime.py`
 - `src/hamilton_pipeline/modules/outputs.py`
 
 Implementation checklist:
 - [ ] Introduce explicit IPC schemas for catalog and profile payloads.
 - [ ] Replace JSON hashing with `payload_hash`.
 - [ ] Ensure payloads are deterministic by ordering entries before hashing.
 
 ### Scope 4: Policy and cache payloads (IPC-native)
 Replace JSON policy blobs and cache entries with IPC-encoded tables or IPC
 bytes with a schema.
 
 Representative pattern:
 ```python
 from registry_common.arrow_payloads import payload_ipc_bytes, ipc_table
 
 _POLICY_SCHEMA = pa.schema(
     [
         ("version", pa.int32()),
         ("name", pa.string()),
         ("entries", pa.list_(pa.struct([("key", pa.string()), ("value", pa.string())]))),
     ]
 )
 
 policy_bytes = payload_ipc_bytes(payload, _POLICY_SCHEMA)
 policy_table = ipc_table(policy_bytes)
 ```
 
 Target files:
 - `src/relspec/policies.py`
 - `src/relspec/rules/cache.py`
 - `src/relspec/engine.py`
 
 Implementation checklist:
 - [ ] Replace JSON policy encoding with IPC payloads.
 - [ ] Decode policies using `ipc_table` and schema validation.
 - [ ] Version payload schemas to enable forward compatibility.
 
 ### Scope 5: Spec tables and Expr IR (IPC-first)
 Ensure spec tables and IR payloads are written/read in IPC-native formats and
 are not labeled as JSON at the boundaries.
 
 Representative pattern:
 ```python
 from arrowdsl.spec.io import write_spec_table, read_spec_table
 
 write_spec_table(path, spec_table)
 round_trip = read_spec_table(path)
 ```
 
 Target files:
 - `src/arrowdsl/spec/io.py`
 - `src/arrowdsl/spec/expr_ir.py`
 - `src/arrowdsl/spec/codec.py`
 
 Implementation checklist:
 - [ ] Rename IPC payload helpers away from JSON semantics.
 - [ ] Use IPC payloads directly for Expr IR transport.
 - [ ] Standardize IPC options for spec tables (compression, dictionaries).
 
 ### Scope 6: JSON helpers confined to export boundaries
 Restrict JSON helpers to export tooling and external interfaces.
 
 Representative pattern:
 ```python
 from arrowdsl.json_factory import dumps_bytes, JsonPolicy
 
 def export_payload(payload: Mapping[str, object]) -> bytes:
     return dumps_bytes(payload, policy=JsonPolicy(sort_keys=True))
 ```
 
 Target files:
 - `src/arrowdsl/json_factory.py`
 - `src/obs/repro.py`
 - `scripts/e2e_diagnostics_report.py`
 
 Implementation checklist:
 - [ ] Audit JSON usage in core runtime paths and remove in-process uses.
 - [ ] Keep JSON only for explicit export commands and reports.
 - [ ] Document allowed JSON boundaries in the plan acceptance criteria.
 
 ### Scope 7: Arrow-native validation and checks
 Replace JSON-based assessment logic with Arrow compute kernels and schema
 validation checks.
 
 Representative pattern:
 ```python
 import pyarrow.compute as pc
 
 def validate_required_non_null(table: pa.Table, required: Sequence[str]) -> pa.BooleanArray:
     masks = [pc.is_valid(table[name]) for name in required]
     return pc.and_(*masks) if masks else pc.scalar(True)
 ```
 
 Target files:
 - `src/obs/diagnostics_tables.py`
 - `src/arrowdsl/compute/filters.py`
 - `src/arrowdsl/compute/kernels.py`
 
 Implementation checklist:
 - [ ] Use `table.validate(full=True)` for structural checks.
 - [ ] Replace JSON checks with compute masks and kernel evaluations.
 - [ ] Add Arrow-native test assertions on schema and values.
 
 ### Scope 8: Tests and fixtures (IPC-native)
 Migrate JSON fixtures to IPC tables and update tests to read IPC artifacts.
 
 Representative pattern:
 ```python
 import pyarrow as pa
 from arrowdsl.spec.io import read_spec_table
 
 table = read_spec_table("tests/fixtures/rule_signatures.arrow")
 rows = table.to_pylist()
 ```
 
 Target files:
 - `tests/integration/test_rule_semantics.py`
 - `tests/e2e/test_full_pipeline_repo.py`
 - `tests/fixtures/rule_signatures.json` (replace with IPC)
 
 Implementation checklist:
 - [ ] Replace JSON fixtures with IPC tables.
 - [ ] Update tests to read IPC tables and compare Arrow-native payloads.
 - [ ] Add regression tests for IPC hashing stability.
 
 ## Comprehensive Target File List
 - `src/storage/deltalake/registry_freshness.py`
 - `src/registry_common/arrow_payloads.py`
 - `src/engine/runtime_profile.py`
 - `src/engine/function_registry.py`
 - `src/datafusion_engine/runtime.py`
 - `src/datafusion_engine/function_factory.py`
 - `src/hamilton_pipeline/modules/outputs.py`
 - `src/obs/diagnostics_tables.py`
 - `src/obs/repro.py`
 - `src/relspec/policies.py`
 - `src/relspec/rules/cache.py`
 - `src/relspec/engine.py`
 - `src/arrowdsl/spec/io.py`
 - `src/arrowdsl/spec/expr_ir.py`
 - `src/arrowdsl/spec/codec.py`
 - `src/arrowdsl/schema/metadata.py`
 - `src/arrowdsl/json_factory.py`
 - `tests/integration/test_rule_semantics.py`
 - `tests/e2e/test_full_pipeline_repo.py`
 - `tests/fixtures/rule_signatures.json` (replace with IPC fixture)
 
 ## Implementation Checklist (Global)
 - [ ] Define IPC schemas for each payload type and version them explicitly.
 - [ ] Replace JSON hashing with IPC payload hashing (`payload_hash`).
 - [ ] Replace JSON-based diagnostics payloads with Arrow tables and IPC artifacts.
 - [ ] Update test fixtures to IPC tables; add hashing stability tests.
 - [ ] Enforce JSON boundary policy (no in-process JSON usage in core paths).
 - [ ] Validate Arrow payloads with `table.validate(full=True)` and compute kernels.
 - [ ] Document IPC write/read options and reuse `IpcWriteConfig` defaults.
