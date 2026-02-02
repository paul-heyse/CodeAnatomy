# Semantic IR Pytest Alignment Plan v3

Status: Proposed  
Owner: Codex  
Scope: Non-CQ pytest failures; align architecture with Semantic IR guarantees

## Objectives
- Eliminate remaining non-CQ pytest failures while reinforcing Semantic IR invariants.
- Standardize Arrow schema-only registration paths (no union-type gaps).
- Preserve deterministic plan hashing and telemetry contracts.
- Keep one-shot datasets truly one-shot while maintaining correct row counts.

## Scope Item 1 — Centralize Empty-Table Construction for Schema-Only Registration

### Why
Arrow union types (e.g., `sparse_union`) cannot be constructed via `pa.array([], type=...)`. This breaks schema-only table registration across DataFusion runtime, schema registry, cache inventory, etc.

### Design
Introduce a shared helper that builds an empty table **without** constructing empty arrays. Use it everywhere we need schema-only registrations.

### Representative snippet
```python
# src/datafusion_engine/arrow/interop.py (or new helper module)
def empty_table_for_schema(schema: pa.Schema) -> pa.Table:
    """Return an empty table that preserves schema, including union types."""
    return pa.Table.from_batches([], schema=schema)

# src/datafusion_engine/session/runtime.py
def _register_schema_table(ctx: SessionContext, name: str, schema: pa.Schema) -> None:
    table = empty_table_for_schema(schema)
    adapter = DataFusionIOAdapter(ctx=ctx, profile=None)
    adapter.register_arrow_table(name, table)
```

### Target files
- `src/datafusion_engine/session/runtime.py`
- `src/datafusion_engine/arrow/interop.py` (or `src/datafusion_engine/arrow/empty_tables.py`)
- Sweep for schema-only empty arrays:
  - `src/datafusion_engine/cache/inventory.py`
  - `src/datafusion_engine/cache/ledger.py`
  - `src/datafusion_engine/delta/observability.py`
  - `src/datafusion_engine/schema/finalize.py`
  - `src/datafusion_engine/schema/validation.py`
  - `src/storage/deltalake/file_index.py`
  - `src/semantics/diagnostics.py`

### Deprecations / deletions
- None. Replace direct `pa.array([], type=...)` usages in schema-only constructors.

### Implementation checklist
- Add `empty_table_for_schema`.
- Replace schema-only empty array builds with the helper.
- Confirm no `pa.array([], type=field.type)` remain in schema-only paths that may include union types.

---

## Scope Item 2 — One-Shot Dataset Scans: Preserve Reader Semantics

### Why
`OneShotDataset.from_reader` currently consumes the reader during dataset creation, leaving the scanner with 0 rows.

### Design
Do not consume the reader during initialization. Use the reader for **exactly one** scanner call. Only materialize a dataset if `consume()` is called (and treat that as the single use).

### Representative snippet
```python
# src/storage/dataset_sources.py
@dataclass
class OneShotDataset:
    dataset: ds.Dataset | None
    reader: pa.RecordBatchReader | None = None

    @classmethod
    def from_reader(cls, reader: pa.RecordBatchReader) -> OneShotDataset:
        return cls(dataset=None, reader=reader)

    def scanner(self, *args: object, **kwargs: object) -> ds.Scanner:
        if self._scanned:
            raise ValueError("One-shot dataset has already been scanned.")
        self._scanned = True
        if self.reader is None:
            return self.dataset.scanner(*args, **kwargs)  # type: ignore[union-attr]
        return ds.Scanner.from_batches(self.reader, **kwargs)
```

### Target files
- `src/storage/dataset_sources.py`

### Deprecations / deletions
- None.

### Implementation checklist
- Make `dataset` optional in `OneShotDataset`.
- Ensure `scanner()` reads from reader exactly once.
- Ensure `consume()` materializes a dataset only when requested.

---

## Scope Item 3 — Streaming Adapter: Canonical Schema Negotiation Errors

### Why
`to_reader()` uses `__arrow_c_stream__` for `TableLike` objects, yielding a PyArrow error message instead of the canonical “Schema negotiation is not supported”.

### Design
Prefer `TableLike` handling before `__arrow_c_stream__` so schema mismatch raises our canonical error.

### Representative snippet
```python
# src/arrow_utils/core/streaming.py
elif isinstance(obj, TableLike):
    return _reader_from_table(obj, schema=schema)
elif hasattr(obj, "__arrow_c_stream__"):
    reader = pa.RecordBatchReader.from_stream(obj, schema=schema)
    return _ensure_reader_schema(reader, schema=schema)
```

### Target files
- `src/arrow_utils/core/streaming.py`

### Deprecations / deletions
- None.

### Implementation checklist
- Reorder `_reader_from_object` checks.
- Keep error message stable across all table-like inputs.

---

## Scope Item 4 — CLI Config Recursion: Remove Recursive JSON Types from msgspec

### Why
`RootConfig` uses a recursive `JsonValue` type alias; `msgspec.convert` recurses indefinitely.

### Design
Use **non-recursive** mapping types for config option payloads and validate JSON compatibility separately.

### Representative snippet
```python
# src/cli/config_models.py
class GraphAdapterConfig(StructBaseStrict, frozen=True):
    options: dict[str, object] | None = None

# src/cli/config_loader.py
def _validate_json_compat(value: object) -> None:
    # recurse through dict/list, ensure scalars only
    ...
```

### Target files
- `src/cli/config_models.py`
- `src/cli/config_loader.py`
- (Optional) `src/cli/validation.py` for shared validators

### Deprecations / deletions
- Deprecate use of `JsonValue` in config Struct fields.

### Implementation checklist
- Replace `JsonValue` on config `options`/`tags` with `dict[str, object] | None`.
- Add validation to enforce JSON-compatible values.
- Ensure validation errors remain user-friendly.

---

## Scope Item 5 — Deterministic Plan Hashes: Canonicalize Snapshot Order

### Why
The plan golden test diff shows stable data but unstable hash outputs. Hashing must be order-independent for lists of dict rows.

### Design
Canonicalize snapshots before hashing (`information_schema` + `function_registry`).

### Representative snippet
```python
# src/datafusion_engine/plan/bundle.py
def _canonicalize_value(value: object) -> object:
    if isinstance(value, list):
        normalized = [_canonicalize_value(item) for item in value]
        return sorted(normalized, key=lambda item: json.dumps(item, sort_keys=True))
    if isinstance(value, dict):
        return {k: _canonicalize_value(v) for k, v in sorted(value.items())}
    return value

def _information_schema_hash(snapshot: Mapping[str, object]) -> str:
    return hash_msgpack_canonical(_canonicalize_value(dict(snapshot)))
```

### Target files
- `src/datafusion_engine/plan/bundle.py`
- (Optional) update related hash helpers if used elsewhere

### Deprecations / deletions
- None.

### Implementation checklist
- Canonicalize snapshots before hashing.
- Update golden fixture if required.

---

## Scope Item 6 — OTel Gauge Contracts: Ensure Gauge Collection in Tests

### Why
Gauge callbacks only emit on collection; tests can miss them unless the reader is collected.

### Design
Add a harness helper to collect metrics before fetching data; update tests to use it.

### Representative snippet
```python
# tests/obs/_support/otel_harness.py
def collect_metrics(self) -> None:
    self.metric_reader.collect()

# tests/obs/test_otel_metrics_contract.py
harness.collect_metrics()
data = harness.metric_reader.get_metrics_data()
```

### Target files
- `tests/obs/_support/otel_harness.py`
- `tests/obs/test_otel_metrics_contract.py`

### Deprecations / deletions
- None.

### Implementation checklist
- Add `collect_metrics()` on the harness.
- Use it in tests prior to metric assertions.

---

## Optional Scope Item 7 — Delta Extension ABI Alignment (Skip Removal)

### Why
Delta integration tests are skipped due to `SessionContext` ABI mismatch. If we want full coverage, align the extension build to the current DataFusion bindings.

### Design
Use the repo’s Delta integration docs to rebuild/realign the extension.

### Representative snippet
```bash
# Rebuild native artifacts when ABI changes
bash scripts/rebuild_rust_artifacts.sh
```

### Target files
- `docs/python_library_reference/deltalake_datafusion_integration.md`
- `rust/` extension crates

### Deprecations / deletions
- None.

### Implementation checklist
- Verify binding versions.
- Rebuild Rust artifacts.
- Re-enable Delta integration tests.

---

## Cross-Cutting Acceptance Gates
- **Zero non-CQ pytest failures** after these changes.
- **Stable plan hashes** across runs.
- **No union-type Arrow construction errors** in schema-only registrations.

## Notes
- CQ-related failures are explicitly out of scope per user request.
- If new snapshot diffs appear after deterministic hashing changes, update goldens once and re-run.
