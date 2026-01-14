## ArrowDSL Relspec Downstream Deployment Plan

### Goals
- Deploy relspec-centralized ArrowDSL capabilities across CPG, extract, normalize, and relspec entry
  points that still use legacy patterns.
- Standardize table/spec construction on ArrowDSL builders (list_view, unions, encoding policies).
- Ensure scan provenance/telemetry and ExprIR options/UDFs are usable from downstream modules.

### Constraints
- Preserve output schemas, column names, and metadata semantics.
- Keep strict typing and Ruff compliance (no suppressions).
- Avoid relative imports; keep modules fully typed and pyright clean.
- Breaking changes are acceptable when they improve cohesion and correctness.

---

### Scope 1: Align CPG QuerySpec Handling with ArrowDSL
**Description**
Replace the custom projection/filter logic in CPG relationship plan compilation with the canonical
`QuerySpec.apply_to_plan` path so scan provenance columns and derived projection alignment behave
consistently across relspec pipelines.

**Code patterns**
```python
# src/cpg/relationship_plans.py
def _apply_query_spec(plan: Plan, spec: QuerySpec, *, ctx: ExecutionContext) -> Plan:
    plan = spec.apply_to_plan(plan, ctx=ctx)
    if spec.pushdown_predicate is not None:
        plan = plan.filter(spec.pushdown_expression(), ctx=ctx)
    return plan
```

**Target files**
- Update: `src/cpg/relationship_plans.py`
- Update (if needed): `src/arrowdsl/plan/query.py`

**Implementation checklist**
- [ ] Replace manual projection/filter logic with `QuerySpec.apply_to_plan`.
- [ ] Preserve pushdown predicate behavior for non-scan plans.
- [ ] Keep rule-meta projections unchanged.

**Status**
Completed.

---

### Scope 2: Upgrade CPG/Extract/Normalize Spec Tables to ArrowDSL Builders
**Description**
Move domain spec tables to `list_view_type` and `table_from_rows` so the nested builder stack
handles unions/maps consistently and avoids redundant JSON encoding.

**Code patterns**
```python
# src/cpg/spec_tables.py
from arrowdsl.schema.build import list_view_type, table_from_rows

NODE_EMIT_STRUCT = pa.struct(
    [pa.field("id_cols", list_view_type(pa.string()), nullable=False)]
)

def node_plan_table(specs: Sequence[NodePlanSpec]) -> pa.Table:
    rows = [_node_plan_row(spec) for spec in specs]
    return table_from_rows(NODE_PLAN_SCHEMA, rows)
```

**Target files**
- Update: `src/cpg/spec_tables.py`
- Update: `src/extract/spec_tables.py`
- Update: `src/normalize/spec_tables.py`

**Implementation checklist**
- [ ] Replace `pa.list_` with `list_view_type` where appropriate.
- [ ] Replace `pa.Table.from_pylist` with `table_from_rows`.
- [ ] Keep schema metadata values unchanged.

**Status**
Completed.

---

### Scope 3: Apply Encoding Policies to Domain Spec Tables
**Description**
Adopt `EncodingPolicy` for domain spec tables to unify dictionaries and chunks, aligning with the
central relspec encoding policy.

**Code patterns**
```python
# src/cpg/spec_tables.py
from arrowdsl.schema.schema import EncodingPolicy, EncodingSpec

CPG_SPEC_ENCODING = EncodingPolicy(
    specs=(EncodingSpec(column="name"), EncodingSpec(column="option_flag"))
)

table = table_from_rows(NODE_PLAN_SCHEMA, rows)
return CPG_SPEC_ENCODING.apply(table)
```

**Target files**
- Update: `src/cpg/spec_tables.py`
- Update: `src/extract/spec_tables.py`
- Update: `src/normalize/spec_tables.py`

**Implementation checklist**
- [ ] Define per-domain encoding policies for high-cardinality string columns.
- [ ] Apply the policy when building spec tables.
- [ ] Keep dictionary encoding columns documented at the module level.

**Status**
Completed.

---

### Scope 4: Enable ExprIR Options/UDFs in Extract Query Ops
**Description**
Allow extract query ops to compile ExprIR payloads (with options/UDFs) instead of only
`expr_spec_from_json`, so UDF registration and compute options can be used in pipelines.

**Code patterns**
```python
# src/extract/plan_helpers.py
from arrowdsl.spec.expr_ir import ExprIR, ExprRegistry

def _handle_filter(row: Mapping[str, object], state: _QueryOpState) -> None:
    expr_json = row.get("expr_ir_json") or row.get("expr_json")
    expr_ir = ExprIR.from_json(str(expr_json))
    state.predicate = expr_ir.to_expr_spec(registry=ExprRegistry())
```

**Target files**
- Update: `src/extract/plan_helpers.py`
- Update: `src/extract/registry_specs.py`

**Implementation checklist**
- [ ] Add support for `expr_ir_json` alongside legacy `expr_json`.
- [ ] Thread UDF registry usage through query-op compilation.
- [ ] Preserve existing query op semantics.

**Status**
Completed.

---

### Scope 5: Surface Evidence Provenance Defaults in Normalize
**Description**
Add schema metadata support for evidence provenance columns so normalize rules can default
provenance projections without manual rule overrides.

**Code patterns**
```python
# src/normalize/evidence_specs.py
EVIDENCE_OUTPUT_PROVENANCE_META = b"evidence_output_provenance"

def evidence_output_from_schema(schema: SchemaLike) -> EvidenceOutput | None:
    provenance = tuple(_meta_list(meta, EVIDENCE_OUTPUT_PROVENANCE_META))
    return EvidenceOutput(column_map=column_map, literals=literals, provenance_columns=provenance)
```

**Target files**
- Update: `src/normalize/evidence_specs.py`
- Update: `src/normalize/spec_tables.py` (if metadata is surfaced there)

**Implementation checklist**
- [ ] Define metadata key for provenance columns.
- [ ] Merge provenance defaults with rule-specific evidence output settings.
- [ ] Keep evidence metadata compatible with existing schemas.

**Status**
Completed.

---

### Scope 6: Expose Scan Profile Knobs at Downstream Entry Points
**Description**
Entry points that default to `execution_context_factory("default")` should expose the new scan
profile knobs (fragment scan options, cache metadata, scan provenance columns) for callers.

**Code patterns**
```python
# src/normalize/runner.py
def ensure_execution_context(ctx: ExecutionContext | None, *, profile: str = "default") -> ExecutionContext:
    return ctx or execution_context_factory(profile)
```

**Target files**
- Update: `src/normalize/runner.py`
- Update: `src/extract/*_extract.py`
- Update: `src/extract/runtime_inspect_extract.py`

**Implementation checklist**
- [ ] Add `profile` or `scan_profile` parameters to entry points.
- [ ] Thread scan profile into `execution_context_factory`.
- [ ] Document defaults in docstrings.

**Status**
Completed.

---

### Scope 7: Surface Relspec Scan Telemetry in Non-Hamilton Flows
**Description**
Expose scan telemetry in non-Hamilton compilation paths (e.g., CPG plan compilation) to
make telemetry available outside the pipeline module.

**Code patterns**
```python
# src/cpg/relationship_plans.py
compiler = RelationshipRuleCompiler(resolver=resolver)
telemetry = compiler.collect_scan_telemetry(rules, ctx=ctx)
```

**Target files**
- Update: `src/cpg/relationship_plans.py`
- Update (optional): `src/extract/evidence_plan.py`

**Implementation checklist**
- [ ] Collect telemetry where rule compilation happens outside Hamilton.
- [ ] Attach telemetry to returned artifacts or logs where appropriate.
- [ ] Ensure telemetry collection remains optional/low-cost.

**Status**
Completed.

---

### Progress Summary
- All scopes implemented. CPG query application now uses `QuerySpec.apply_to_plan`, domain spec
  tables use ArrowDSL builders and encoding policies, extract query ops accept ExprIR JSON with
  registries, normalize evidence metadata supports provenance defaults, entry points accept scan
  profiles, and telemetry is surfaced via relation/evidence plan bundles.

### Next Actions
- No remaining scope in this plan. Optional follow-up: run a relspec compilation/extract bundle
  to validate profile-driven scan knobs and telemetry propagation.
