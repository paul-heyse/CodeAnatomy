## ArrowDSL Relspec Centralization Intensification Plan

### Goals
- Eliminate legacy or duplicate ArrowDSL surfaces that conflict with the centralized relspec rules system.
- Expand ArrowDSL to cover advanced PyArrow/Acero capabilities that materially improve relspec performance and integration.
- Keep ArrowDSL focused on shared compute/plan infrastructure, while domain specs remain in their owning modules.

### Constraints
- Preserve output schemas, column names, and metadata semantics.
- Keep strict typing and Ruff compliance (no suppressions).
- Avoid relative imports; keep modules fully typed and pyright clean.
- Breaking changes are acceptable when they improve cohesion and correctness.

---

### Scope 1: Retire Duplicate Relspec Spec Tables in ArrowDSL
**Description**
Remove or relocate the legacy relspec spec-table definitions in `arrowdsl.spec.tables.relspec`
that now overlap the canonical `relspec.rules.spec_tables` implementation. Keep a single
source of truth for rule schemas and table codecs.

**Code patterns**
```python
# src/relspec/rules/spec_tables.py
RULE_DEFINITION_SCHEMA = pa.schema([...], metadata={b"spec_kind": b"relspec_rule_definitions"})

def rule_definition_table(definitions: Sequence[RuleDefinition]) -> pa.Table:
    rows = [_rule_definition_row(defn) for defn in definitions]
    return table_from_rows(RULE_DEFINITION_SCHEMA, rows)
```

**Target files**
- Remove/relocate: `src/arrowdsl/spec/tables/relspec.py`
- Update: `src/relspec/rules/spec_tables.py`
- Update: `src/relspec/__init__.py`
- Update: `src/arrowdsl/spec/__init__.py`

**Implementation checklist**
- [ ] Remove duplicate rule schemas and codec helpers from ArrowDSL.
- [ ] Ensure all relspec schema/table imports point to `relspec.rules.spec_tables`.
- [ ] Keep a minimal, stable re-export surface in `src/relspec/__init__.py`.
- [ ] Confirm no remaining ArrowDSL references to relspec schemas in `src/arrowdsl/spec`.

**Status**
Completed.

---

### Scope 2: Move Domain Spec Tables Out of ArrowDSL
**Description**
Shift domain-specific spec tables (CPG, extract, normalize) out of ArrowDSL so the DSL
remains purely infrastructure. Domain packages own their table schemas and codecs.

**Code patterns**
```python
# src/cpg/spec_tables.py
NODE_PLAN_SCHEMA = pa.schema([...], metadata={b"spec_kind": b"cpg_node_specs"})

def node_plan_table(specs: Sequence[NodePlanSpec]) -> pa.Table:
    rows = [_node_plan_row(spec) for spec in specs]
    return pa.Table.from_pylist(rows, schema=NODE_PLAN_SCHEMA)
```

**Target files**
- Move: `src/arrowdsl/spec/tables/cpg.py` -> `src/cpg/spec_tables.py`
- Move: `src/arrowdsl/spec/tables/cpg_registry.py` -> `src/cpg/registry_tables.py`
- Move: `src/arrowdsl/spec/tables/extract.py` -> `src/extract/spec_tables.py`
- Move: `src/arrowdsl/spec/tables/normalize.py` -> `src/normalize/spec_tables.py`
- Update imports in: `src/cpg/*`, `src/extract/*`, `src/normalize/*`

**Implementation checklist**
- [ ] Relocate domain table schemas and table builders into their packages.
- [ ] Update call sites to import from domain modules instead of ArrowDSL.
- [ ] Keep ArrowDSL spec IO helpers for shared serialization only.
- [ ] Remove cross-domain imports from ArrowDSL to avoid dependency loops.

**Status**
Completed.

---

### Scope 3: Simplify Spec Table IO (Remove Legacy IPC Format)
**Description**
Remove legacy IPC options from spec IO to enforce a single modern format, reduce
configuration drift, and simplify interfaces.

**Code patterns**
```python
# src/arrowdsl/spec/io.py
@dataclass(frozen=True)
class IpcWriteConfig:
    compression: str | None = "zstd"
    use_threads: bool = True
    unify_dictionaries: bool = True
    emit_dictionary_deltas: bool = False
    allow_64bit: bool = False
    metadata_version: ipc.MetadataVersion = ipc.MetadataVersion.V5
```

**Target files**
- Update: `src/arrowdsl/spec/io.py`
- Update: `src/arrowdsl/spec/tables/base.py`
- Update: any call sites using `IpcWriteConfig`

**Implementation checklist**
- [ ] Remove `use_legacy_format` from write config and options factory.
- [ ] Ensure IPC metadata version is pinned to V5.
- [ ] Audit call sites to confirm no legacy option usage.

**Status**
Completed.

---

### Scope 4: Expand Scan Control Plane (Parquet Options, Metadata Caching)
**Description**
Extend the scan profile to support Parquet fragment scan options and metadata caching
so relspec pipelines can control scan behavior through a single runtime profile.

**Code patterns**
```python
# src/arrowdsl/core/context.py
@dataclass(frozen=True)
class ScanProfile:
    fragment_scan_options: object | None = None
    cache_metadata: bool = False

    def scanner_kwargs(self) -> dict[str, object]:
        kw = {"use_threads": self.use_threads, "cache_metadata": self.cache_metadata}
        if self.fragment_scan_options is not None:
            kw["fragment_scan_options"] = self.fragment_scan_options
        return kw
```

**Target files**
- Update: `src/arrowdsl/core/context.py`
- Update: `src/arrowdsl/plan/query.py`
- Update: `src/arrowdsl/plan/scan_io.py`

**Implementation checklist**
- [ ] Add fragment scan options and metadata caching to ScanProfile.
- [ ] Thread profile settings into `ScanContext.scanner()` and `ScanOp`.
- [ ] Expose settings via runtime profiles to avoid ad hoc scan knobs.

**Status**
Completed.

---

### Scope 5: Provenance-Aware Scans for Evidence Outputs
**Description**
Make provenance columns from dataset scans available to relspec evidence handling,
enabling stable debugging and traceability.

**Code patterns**
```python
# src/arrowdsl/plan/query.py
columns = spec.scan_columns(
    provenance=ctx.provenance,
    scan_provenance=ctx.runtime.scan.scan_provenance_columns,
)

# src/relspec/rules/handlers/extract.py
evidence = EvidenceOutput(
    required_columns=spec.required_columns,
    provenance_columns=ctx.runtime.scan.scan_provenance_columns,
)
```

**Target files**
- Update: `src/arrowdsl/plan/query.py`
- Update: `src/relspec/rules/handlers/extract.py`
- Update: `src/relspec/rules/handlers/normalize.py`
- Update: `src/relspec/rules/handlers/cpg.py`

**Implementation checklist**
- [ ] Thread provenance column names through QuerySpec scans and plans.
- [ ] Add evidence outputs that include scan provenance columns when enabled.
- [ ] Ensure provenance remains optional and opt-in via ExecutionContext.

**Status**
Completed (provenance columns flow through scan/query and normalize evidence outputs; extract/cpg
handlers do not emit evidence outputs, so no changes required there).

---

### Scope 6: ExprIR Options + UDF Coverage for Plan Lane
**Description**
Extend ExprIR to encode compute options and additional UDF types so rule specs can
declare richer compute expressions without direct `pc.*` usage in nodes.

**Code patterns**
```python
# src/arrowdsl/spec/expr_ir.py
@dataclass(frozen=True)
class ExprIR:
    op: str
    name: str | None = None
    value: ScalarValue | None = None
    args: tuple[ExprIR, ...] = ()
    options: dict[str, object] | None = None

    def to_expression(self, *, registry: ExprRegistry | None = None) -> ComputeExpression:
        if self.op == "call":
            fn = registry.ensure(self.name) if registry is not None else self.name
            opts = pc.FunctionOptions.deserialize(self.options) if self.options else None
            return ensure_expression(pc.call_function(fn, [arg.to_expression() for arg in self.args], options=opts))
```

**Target files**
- Update: `src/arrowdsl/spec/expr_ir.py`
- Update: `src/arrowdsl/compute/filters.py`
- Update: `src/arrowdsl/spec/codec.py`

**Implementation checklist**
- [ ] Add expression option payload support to ExprIR.
- [ ] Allow registered UDFs and options-aware calls in rule specs.
- [ ] Keep JSON encoding stable and ASCII-safe for options payloads.

**Status**
Completed.

---

### Scope 7: Advanced Arrow Types in Rule Tables
**Description**
Use list_view, map, union, and dictionary types where they improve size/perf and
remove redundant JSON encoding. Centralize type helpers for consistent usage.

**Code patterns**
```python
# src/arrowdsl/schema/build.py
list_type = list_view_type(pa.string())
meta_type = map_type(pa.string(), pa.string())
```

**Target files**
- Update: `src/relspec/rules/spec_tables.py`
- Update: `src/arrowdsl/schema/build.py`
- Update: `src/arrowdsl/schema/nested_builders.py`

**Implementation checklist**
- [ ] Replace JSON-encoded maps with `map_` where possible.
- [ ] Switch large list payloads to `list_view` / `large_list_view`.
- [ ] Use union types for variant kernel specs to reduce null-heavy structs.
- [ ] Standardize dictionary-encoded categorical columns.

**Status**
Completed.

---

### Scope 8: Dictionary-Encoding Policy for Rule Tables
**Description**
Apply encoding policies to centralized rule tables to speed joins and reduce memory
footprint for repeated string fields.

**Code patterns**
```python
# src/relspec/rules/spec_tables.py
table = rule_definition_table(definitions)
encoded = EncodingPolicy(specs=(EncodingSpec(column="kind"), EncodingSpec(column="output_dataset"))).apply(table)
```

**Target files**
- Update: `src/relspec/rules/spec_tables.py`
- Update: `src/arrowdsl/schema/ops.py`
- Update: `src/arrowdsl/compute/kernels.py`

**Implementation checklist**
- [ ] Define a shared EncodingPolicy for rule tables.
- [ ] Apply ChunkPolicy to unify dictionaries and combine chunks.
- [ ] Keep encoding columns centralized and documented.

**Status**
Completed.

---

### Scope 9: Scan Telemetry in Relspec Compilation Outputs
**Description**
Surface scan telemetry (fragment counts, estimated rows) alongside relspec outputs to
improve observability and prevent silent scan regressions.

**Code patterns**
```python
# src/arrowdsl/plan/query.py
telemetry = fragment_telemetry(dataset, predicate=spec.pushdown_expression())

# src/relspec/registry.py
return CompiledOutput(telemetry=telemetry, rules=compiled_rules)
```

**Target files**
- Update: `src/arrowdsl/plan/query.py`
- Update: `src/relspec/registry.py`
- Update: `src/obs/manifest.py`

**Implementation checklist**
- [ ] Thread ScanTelemetry into relspec compile/run results.
- [ ] Record telemetry in manifests or observability outputs.
- [ ] Preserve opt-in behavior for lightweight runs.

**Status**
Completed.

---

### Progress Summary
- All scopes implemented. Domain spec tables are owned by their packages, legacy IPC options removed,
  scan provenance/telemetry integrated, ExprIR options and tagged union payloads supported, and
  rule tables upgraded to list_view/union/dictionary-encoding policies.

### Next Actions
- No remaining scope in this plan. Optional follow-up: run end-to-end relspec compilation to
  validate telemetry/provenance wiring in your preferred pipeline.
