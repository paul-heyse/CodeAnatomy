## ArrowDSL Advanced PyArrow Methodologies Plan

### Goals
- Expand ArrowDSL to cover advanced scan telemetry, provenance, and scan configuration features.
- Make scan-time projections and metadata capture the canonical path for all pipelines.
- Reduce JSON-shaped payloads by using native Arrow union/dictionary encodings where appropriate.
- Keep plan lane (Acero) and kernel lane behaviors consistent and deterministic.

### Constraints
- Preserve ordering metadata and pipeline-breaker semantics.
- Avoid ad hoc scan configuration at call sites; centralize in the scan control plane.
- Keep all modules fully typed and pyright/pyrefly clean.
- Prefer Arrow-native encodings over Python fallbacks where feasible.

---

### Scope 1: Scan Preflight Telemetry (count_rows + filtered fragments)
**Description**
Extend scan telemetry to include `dataset.count_rows(filter=...)` estimates and fragment counts
using pushdown predicates. This provides consistent preflight size metrics across pipelines.

**Code patterns**
```python
# src/arrowdsl/plan/query.py
rows = dataset.count_rows(filter=predicate)
fragments = list(dataset.get_fragments(filter=predicate))
```

**Target files**
- Update: `src/arrowdsl/plan/query.py`
- Update: `src/arrowdsl/plan/stats.py`
- Update: `src/hamilton_pipeline/modules/outputs.py`

**Implementation checklist**
- [ ] Add `estimated_rows` and `count_rows` fields to `ScanTelemetry`.
- [ ] Capture `dataset.count_rows(filter=...)` when available.
- [ ] Propagate new fields to telemetry tables and output artifacts.

**Status**
Planned.

---

### Scope 2: Scan Provenance Columns + TaggedRecordBatch Debug Path
**Description**
Support special scan columns (`__filename`, `__fragment_index`, `__batch_index`,
`__last_in_fragment`) and add a `scan_batches()` path for fragment-aware debugging.

**Code patterns**
```python
# src/arrowdsl/plan/query.py
columns = {
    **spec.scan_columns(provenance=ctx.provenance),
    "__filename": pc.field("__filename"),
}
scanner = dataset.scanner(columns=columns, filter=predicate)
for tagged in scanner.scan_batches():
    ...
```

**Target files**
- Update: `src/arrowdsl/plan/query.py`
- Update: `src/arrowdsl/plan/scan_specs.py`
- Update: `src/arrowdsl/plan/source.py`

**Implementation checklist**
- [ ] Add optional provenance column injection to QuerySpec scans.
- [ ] Expose `ScanContext.scan_batches()` for TaggedRecordBatch iteration.
- [ ] Add opt-in debug projection for fragment/batch indices.

**Status**
Planned.

---

### Scope 3: Fragment Scan Options in the Scan Control Plane
**Description**
Surface `fragment_scan_options` and other scan-time options via `ScanProfile`, and pass
through to both `ds.Scanner` and `acero.ScanNodeOptions` to avoid ad hoc tuning.

**Code patterns**
```python
# src/arrowdsl/core/context.py
@dataclass(frozen=True)
class ScanProfile:
    fragment_scan_options: object | None = None

# src/arrowdsl/plan/query.py
scanner = dataset.scanner(
    columns=columns,
    filter=predicate,
    fragment_scan_options=ctx.runtime.scan.fragment_scan_options,
)
```

**Target files**
- Update: `src/arrowdsl/core/context.py`
- Update: `src/arrowdsl/plan/query.py`
- Update: `src/arrowdsl/plan/ops.py`

**Implementation checklist**
- [ ] Add `fragment_scan_options` to ScanProfile.
- [ ] Pass scan options through `ds.Scanner` and Acero `ScanNodeOptions`.
- [ ] Ensure options are set only via runtime profiles.

**Status**
Planned.

---

### Scope 4: Computed Scan Projections (Derived Fields)
**Description**
Use dict-based projections at scan time to compute derived fields early when safe,
reducing downstream projection nodes and data movement.

**Code patterns**
```python
# src/arrowdsl/plan/query.py
if self.projection.derived:
    return {name: expr.to_expression() for name, expr in self.projection.derived.items()}
```

**Target files**
- Update: `src/arrowdsl/plan/query.py`
- Update: `src/schema_spec/system.py`
- Update: `src/arrowdsl/plan/source.py`

**Implementation checklist**
- [ ] Extend QuerySpec to emit dict projections when derived fields exist.
- [ ] Ensure DatasetSpec derived fields participate in scan columns.
- [ ] Skip redundant post-scan projection nodes where safe.

**Status**
Planned.

---

### Scope 5: UnionArray Encodings for Heterogeneous Payloads
**Description**
Replace JSON-encoded heterogenous payloads with UnionArray encodings
(sparse or dense) when payload schemas are finite and known.

**Code patterns**
```python
# src/arrowdsl/schema/nested_builders.py
union = pa.UnionArray.from_dense(type_ids, offsets, children)
```

**Target files**
- Update: `src/arrowdsl/schema/nested_builders.py`
- Update: `src/arrowdsl/spec/codec.py`
- Update: `src/arrowdsl/spec/structs.py`

**Implementation checklist**
- [ ] Add dense/sparse union array factory helpers.
- [ ] Define union field specs for known heterogeneous payloads.
- [ ] Replace JSON blobs with union encodings where feasible.

**Status**
Planned.

---

### Scope 6: DictionaryArray From Indices + Dictionary
**Description**
Provide a factory for dictionary arrays built from precomputed indices + dictionary
values to ensure stable categorical encodings across batches.

**Code patterns**
```python
# src/arrowdsl/schema/encoding.py
arr = pa.DictionaryArray.from_arrays(indices, dictionary)
```

**Target files**
- Update: `src/arrowdsl/schema/encoding.py`
- Update: `src/arrowdsl/schema/nested_builders.py`
- Update: `src/arrowdsl/spec/codec.py`

**Implementation checklist**
- [ ] Add dictionary array factory for index+dictionary inputs.
- [ ] Integrate into spec codecs for categorical fields.
- [ ] Ensure dictionary unification is applied post-assembly.

**Status**
Planned.
