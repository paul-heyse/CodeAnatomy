
Below is a **PR-by-PR migration plan** from your current “partway through DataFusion integration” state to the **best‑in‑class target** we discussed: **maximally modular, declarative, and rules/relationship-driven**, with execution grounded in **PyArrow objects + compute**, **DataFusion** as the primary engine, **Ibis as the authoring IR**, **SQLGlot as the diagnostic/analysis IR**, and **Hamilton as the inference-driven orchestrator.

I’m using your attached formatting reference as the style baseline. 

---

## PR Stack Overview (high-level)

**PR‑01 — DataFusion Runtime & Session Contract**

> Make `SessionContext` + runtime profile a first-class, centrally configured, reproducible “engine substrate” used by all rule execution.

**PR‑02 — Dataset Registry → DataFusion Table Providers (scan options fully wired)**

> Finish the “views-first” registry so rules only reference logical dataset names, and scan behavior is controlled by typed options (partitioning, pruning, sort order).

**PR‑03 — Relspec Compile/Diagnostics Hardening**

> Make the **rule compiler** produce (1) canonical `RelPlan` signatures, (2) SQLGlot diagnostics, (3) optional Substrait artifacts, and (4) compile-time contract checks for produced relationships/edges.

**PR‑04 — Hybrid Kernel Bridge (built-ins → UDF/UDTF → Arrow fallback)**

> Normalize “special ops” (interval align, explode, dedupe, etc.) into a capability-driven bridge: prefer DataFusion built-ins, then Arrow-native UDF/UDTF, else Arrow kernel fallback.

At the end I list the **next PRs** I’d do after these four (plan builder migration, IO/materialization unification, ArrowDSL plan-lane retirement, etc.).

---

# PR‑01: DataFusion Runtime & Session Contract

### Description (what changes)

Right now, “DataFusion integration” often stalls at the exact same spot in every codebase: the engine exists, but **there’s no single, enforced session contract**. That leads to:

* rules registering tables in ad-hoc ways,
* inconsistent defaults (catalog/schema, info schema, optimizer knobs),
* non-reproducible performance (batch sizes, partitions, spill config),
* and difficulty debugging because explain/plan artifacts aren’t systematically collected.

This PR creates a **single authoritative “DataFusion runtime profile”** and a **session factory** that:

1. builds `SessionConfig` + `RuntimeEnv` consistently,
2. creates a `SessionContext` with explicit catalog/schema and optional info schema,
3. exposes a stable “engine surface” used by the relspec compiler/executor,
4. and establishes standard plan introspection hooks (logical/optimized/physical plans + metrics capture).

This PR is foundational and should land before any “wiring scan options” or “kernel bridge” work, because those pieces need a stable `SessionContext`.

---

### Code patterns (key snippets only)

**1) Runtime profile as a typed contract**

```python
from dataclasses import dataclass
from typing import Literal

@dataclass(frozen=True)
class DataFusionRuntimeProfile:
    target_partitions: int = 8
    batch_size: int = 8192
    spill_dir: str | None = None
    memory_pool: Literal["greedy", "fair"] = "greedy"
    default_catalog: str = "codeintel"
    default_schema: str = "public"
    enable_information_schema: bool = True
```

**2) SessionConfig hardening (explicit defaults)**

```python
from datafusion import SessionConfig

cfg = (
  SessionConfig()
    .with_default_catalog_and_schema(profile.default_catalog, profile.default_schema)
    .with_create_default_catalog_and_schema(True)
    .with_information_schema(profile.enable_information_schema)
)
```

**3) Central SessionContext factory**

```python
from datafusion import SessionContext

def build_df_ctx(profile: DataFusionRuntimeProfile) -> SessionContext:
    # optionally build RuntimeEnv / memory pool / spill manager if exposed
    return SessionContext(session_config=cfg)
```

**4) Plan introspection hook**

```python
def snapshot_plans(df):
    lp = df.logical_plan()
    olp = df.optimized_logical_plan()
    ep = df.execution_plan()
    return {"logical": lp, "optimized": olp, "physical": ep}
```

---

### Narrative (dense “why this way”)

**Why a single runtime profile matters for your architecture**
Your system is fundamentally a **compiler pipeline**:

`Relationship rules (RelOps) → RelPlan → engine plan → materialized relationship tables → edge emission → parquet`

If the engine substrate varies implicitly, you’ll never get reliable:

* plan signatures,
* reproducible run bundles,
* stable performance tuning,
* or deterministic “winner selection” behavior (because ordering/partitioning changes can change tie-break outcomes).

So the correct abstraction is: **SessionContext is part of the compilation environment**, not a convenience object.

**Key DataFusion capabilities exploited**

* **Catalog/schema hierarchy** is a real concept in DataFusion and should be used to isolate “your datasets” from accidental collisions.
* **Info schema** should be enabled intentionally (for debugging and function inventory), not incidentally.
* **Explain / plan APIs** should be treated as first-class diagnostics artifacts (you’ll want them in run bundles).

**Integration point with Hamilton**
Hamilton nodes shouldn’t “construct DataFusion sessions”; they should request an `ExecutionContext` (or `EngineContext`) that already contains:

* the `SessionContext`,
* runtime profile,
* and a diagnostics sink (where plan snapshots go).

That keeps the DAG inference-driven while engine setup remains centrally controlled.

---

### Implementation checklist

* [ ] Add `DataFusionRuntimeProfile` and thread it through your existing `ExecutionContext`.
* [ ] Add `datafusion_engine/session.py` with `build_df_ctx(profile)`.
* [ ] Enforce explicit catalog/schema defaults and info schema toggle via `SessionConfig`.
* [ ] Add a minimal plan snapshot utility (logical/optimized/physical) and expose it to relspec diagnostics.
* [ ] Add “engine fingerprint” to telemetry/run bundle metadata (profile fields + library versions if you already track those).

**Watchouts**

* Don’t let random modules instantiate `SessionContext()` directly after this PR. Enforce factory usage.
* Treat runtime profile fields as part of reproducibility: changing them should produce a new “run bundle signature”.

---

# PR‑02: Dataset Registry → DataFusion Table Providers (scan options fully wired)

### Description (what changes)

You’re aiming for rules/relationships to be **fully programmatic**, and that requires **names, not file paths** at rule authoring time.

This PR completes the “views-first” dataset registry so:

* Rules always reference logical dataset names (`"cst_nodes"`, `"scip_occurrences"`, etc.).
* The registry owns how those names map to:

  * parquet directories,
  * partitioning strategy,
  * pruning behavior,
  * file sort hints,
  * and (optionally) schema hints.
* DataFusion registration fully honors typed `DataFusionScanOptions`.

This is where your declarative goal becomes real: **rules describe joins/filters/projects; registry describes data access**.

---

### Code patterns (key snippets only)

**1) Scan options on DatasetSpec**

```python
from dataclasses import dataclass

@dataclass(frozen=True)
class DataFusionScanOptions:
    partition_cols: tuple[tuple[str, str], ...] = ()
    file_sort_order: tuple[str, ...] = ()
    parquet_pruning: bool = True
    skip_metadata: bool = False
```

**2) Registry method to register a dataset into a DataFusion schema**

```python
def register_dataset(ctx, *, name: str, location: DatasetLocation):
    # choose catalog/schema explicitly (ctx.catalog(...).schema(...))
    # register parquet/listing table provider depending on location.format
    ...
```

**3) Catalog-aware namespacing**

```python
catalog = ctx.catalog(profile.default_catalog)
schema = catalog.schema(profile.default_schema)
schema.register_table(name, table_provider)
```

**4) Rule code only touches logical names**

```python
df = ctx.table("cst_callsites")   # never ctx.register_parquet(...) in rule code
```

---

### Narrative (dense “why this way”)

**Why “scan options in the registry” is architecturally critical**
Your rule compiler is trying to be:

* engine agnostic,
* deterministic,
* contract-validated,
* and inference-driven.

If scan behavior leaks into rule code, you get:

* duplicated scan policies,
* inconsistent pruning behavior across rules,
* and the compiler can’t reason about data access.

So: dataset scan policy must be centralized.

**Key DataFusion capabilities exploited**

* DataFusion tables are *providers*, not “loaded data”. The registry should register providers wherever possible (parquet listings, datasets, etc.) so DataFusion can push down filters/projections and schedule reads efficiently.
* Catalog/schema APIs let you keep rule execution clean: `catalog.schema.table` naming becomes your stable namespace.

**How this supports ambiguous schema conditions**
Your earlier target (“materialize schemas from ambiguous data”) is best supported if:

* registry optionally attaches a schema hint (for stability),
* but the engine can still infer from parquet metadata,
* and the compiler can validate post-materialization contracts.

This PR sets the stage: registry is the only place where schema hints and scan behavior live.

---

### Implementation checklist

* [ ] Add `scan_options: DataFusionScanOptions | None` to `DatasetLocation` / `DatasetSpec`.
* [ ] Implement `DatasetRegistry.register_all(ctx)` that registers every declared dataset once per session.
* [ ] Thread scan options into the DataFusion provider registration path.
* [ ] Document precedence: `DatasetSpec.scan_options` vs system defaults (profile defaults) vs “rule hints” (rules should not provide scan hints).
* [ ] Add a registry “introspection dump” artifact (dataset name → location → scan options → schema hint) for run bundles.

**Watchouts**

* Avoid registering datasets into default global schema accidentally; always use explicit catalog/schema from PR‑01.
* Partitioning hints must match how parquet is physically laid out; otherwise you get silent performance cliffs.
* If you rely on stable tie-break ordering downstream, you must not implicitly change file order without capturing it in metadata (see PR‑03/PR‑04).

---

# PR‑03: Relspec Compile/Diagnostics Hardening (canonical signatures + SQLGlot + Substrait + contract validation)

### Description (what changes)

This PR makes the relspec compiler behave like a real compiler toolchain:

1. **Canonicalize** the rule graph and compute stable plan signatures.
2. Produce **SQLGlot diagnostics** for:

   * validation,
   * lineage,
   * semantic diffs between versions.
3. Optionally produce **Substrait plan bytes** as a stable binary artifact for:

   * caching,
   * regression,
   * and “same intent different engine” portability.
4. Add a **validator hook**: if a rule claims it produces relationship dataset X or edge kind Y, the output contract must include required columns/properties.

This PR is where you enforce “single source of truth” for what rules produce.

---

### Code patterns (key snippets only)

**1) Canonical plan signature**

```python
import hashlib, json

def plan_signature(plan: RelPlan) -> str:
    payload = _rel_plan_payload(plan)  # canonical, sorted keys
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()
```

**2) SQLGlot diagnostics pipeline**

```python
sg_expr = con.compiler.to_sqlglot(ibis_expr)
sg_expr = qualify(sg_expr, schema=schema_map)
sg_expr = optimize(sg_expr, schema=schema_map)
```

**3) Substrait capture (diagnostics artifact)**

```python
from datafusion.substrait import Serde

sql = sg_expr.sql(pretty=False)  # diagnostics only
plan_bytes = Serde.serialize_bytes(sql, ctx)
```

**4) Compile-time contract validator hook**

```python
def validate_rule_output_contract(rule, produced_schema):
    required = EDGE_KIND_CONTRACTS[rule.edge_kind]  # or relationship contract
    missing = required - set(produced_schema.names)
    if missing:
        raise ContractError(...)
```

---

### Narrative (dense “why this way”)

**Why SQLGlot is not optional in your target design**
You’re building a graph compiler whose inputs are high-dimensional and ambiguous (CST/AST/SCIP often disagree or produce multiple candidates). That means:

* you need **lineage** to explain “why did edge X exist?”
* you need **semantic diffs** across PRs,
* and you need an IR you can manipulate programmatically without handwritten SQL.

SQLGlot is that IR layer. In practice:

* Ibis is your authoring IR (“how rules are written”),
* SQLGlot is your analysis IR (“how rules are inspected/validated”).

**Why Substrait matters even if you execute SQL-free**
Even if runtime execution avoids SQL strings (DataFusion DataFrame API), Substrait gives you:

* a stable binary artifact that can be snapshotted,
* a portability mechanism across engines,
* and a deterministic “plan identity” that is *less fragile than SQL text diffs*.

Treat Substrait bytes as your “compiler output” artifact in run bundles.

**Why contract validation belongs in the compiler**
If you wait until graph emission to discover a missing column, debugging becomes impossible because you’ve lost the local context: the error is detected far downstream.

The correct failure mode is:

* compile rule → determine produced schema → validate against required contract → fail fast.

This is the enforcement mechanism that makes your whole “rules-driven architecture” real.

---

### Implementation checklist

* [ ] Add canonical plan payload serialization and stable hash signature computation.
* [ ] Store plan signature in rule diagnostics and cache keys.
* [ ] Add SQLGlot extraction/qualify/optimize step and persist optimized AST as artifact.
* [ ] Add Substrait serialization as an optional diagnostics artifact (behind config flag).
* [ ] Add contract validator hook: rule outputs must satisfy relationship/edge contracts.
* [ ] Add “semantic diff” stub: compare previous SQLGlot ASTs or signatures for regression gating (even if you don’t block yet).

**Watchouts**

* SQL text is not guaranteed to be stable across SQLGlot versions; that’s why you store both:

  * the SQLGlot AST (structured),
  * and Substrait bytes (binary IR).
* Don’t accidentally make Substrait capture required for runtime; keep it diagnostics-only until you’re sure it’s stable in your environment.

---

# PR‑04: Hybrid Kernel Bridge (built-ins → UDF/UDTF → Arrow fallback)

### Description (what changes)

Some operations are inherently “non-relational” or at least inconvenient to express purely as joins/filters/projects:

* interval alignment,
* list explode / unnest,
* stable dedupe with winner selection,
* bespoke string/path normalization,
* span logic (if not already fully relational).

This PR creates a strict “kernel bridge” with a capability registry:

1. **Prefer DataFusion built-in functions** (fastest, optimizer-friendly).
2. If no built-in exists, use **Arrow-native Python UDF/UDTF** (vectorized batches in/out).
3. If still not representable, fall back to **PyArrow kernel lane** post-materialization.

This preserves your modular architecture while remaining honest about where engines differ.

---

### Code patterns (key snippets only)

**1) Capability registry**

```python
from enum import Enum

class KernelLane(Enum):
    BUILTIN = "builtin"
    DF_UDF = "df_udf"
    DF_UDTF = "df_udtf"
    ARROW_FALLBACK = "arrow_fallback"

KERNEL_CAPS = {
  "explode_list": KernelLane.DF_UDTF,
  "interval_align": KernelLane.DF_UDF,
  "stable_dedupe": KernelLane.ARROW_FALLBACK,
}
```

**2) Arrow-native scalar UDF pattern (vectorized)**

```python
import pyarrow as pa
from datafusion import udf

def my_kernel(arr: pa.Array) -> pa.Array:
    # MUST be Arrow-native (prefer pyarrow.compute)
    ...

my_udf = udf(my_kernel, [pa.large_string()], pa.large_string(), "immutable", name="my_kernel")
ctx.register_udf(my_udf)
```

**3) Winner selection policy encoded as a deterministic relational pattern**

```python
# score = ...; then sort + drop_duplicates in an engine-consistent way
# (exact implementation differs per engine; policy is centralized)
policy = WinnerPolicy(
  order_by=("score DESC", "candidate_rank ASC"),
  tie_break="keep_first"
)
```

**4) Fallback kernel lane**

```python
tbl = plan.to_pyarrow()
tbl2 = explode_list_column(tbl, parent_id_col="src_id", list_col="dst_ids")
```

---

### Narrative (dense “why this way”)

**Why you need a kernel bridge even in a “pure relational” dream**
For CPG construction, you repeatedly hit operations that are:

* relational in theory,
* but practically brittle across engines (especially when null semantics, list types, or ordering stability are involved).

The correct approach is not to fight that reality; it’s to **make it explicit and modular**:

* rules define the *intent*,
* the compiler chooses the execution lane based on capabilities,
* and contracts ensure outputs remain consistent.

**Key DataFusion capabilities exploited**

* DataFusion Python supports UDF shapes (scalar, aggregate, window, table). The one you’ll use most is **scalar UDF** (batch arrays in/out) for per-row-like logic while remaining vectorized.
* Volatility is not decoration: it influences planner behavior. So treat it as part of the kernel contract (immutable vs stable vs volatile).

**How this ties to ambiguity resolution**
Your ambiguity resolution steps (“multiple SCIP candidates for a CST callsite”) often rely on deterministic ranking and dedupe. That’s exactly where engines can diverge unless you:

* standardize ordering metadata upstream (already in your `IbisPlan` ordering),
* enforce sort keys before dedupe,
* and centralize tie-break policy.

This PR is where “winner selection” becomes a first-class system concept, not scattered `sort_values().drop_duplicates()` equivalents.

---

### Implementation checklist

* [ ] Create `KernelSpec` objects with: name, required inputs, output schema contract, volatility, lane preference.
* [ ] Implement built-in function mapping first (when DataFusion has what you need).
* [ ] Implement UDF registration utilities that enforce Arrow-native behavior.
* [ ] Implement UDTF utilities if you need explode/unnest as a table function.
* [ ] Implement Arrow fallback execution path and ensure contracts are validated after fallback.
* [ ] Add diagnostics: for each kernel application, record lane chosen + function name + volatility + ordering requirements.

**Watchouts**

* If you implement Python UDFs, do not use `as_py()` loops—those destroy performance and often create type edge cases. Keep UDFs Arrow-native and prefer `pyarrow.compute`.
* Any dedupe/winner-selection must be preceded by an explicit ordering; otherwise tie-break differs by partitioning/execution plan.

---

## Next PRs I would do (not detailed yet, but this is the natural continuation)

These are the “big movers” once PR‑01..04 land:

1. **PR‑05: Convert remaining plan builders (normalize/extract/cpg) to Ibis expressions**

   * finish replacing ad-hoc plan builders with Ibis expression builders as in your migration plan.

2. **PR‑06: Parameterization everywhere (Ibis params + memtable-join for list params)**

   * eliminate any SQL-string interpolation and standardize list filtering via “memtable + join”.

3. **PR‑07: IO/materialization standardization**

   * all writes come from Arrow batches (`expr.to_pyarrow_batches(...)`) into parquet with consistent metadata sidecars.

4. **PR‑08: Decommission or freeze ArrowDSL plan lane**

   * keep ArrowDSL contracts + kernel lane, but remove plan-lane duplication unless you explicitly keep Acero as an alternate engine.

If you want, I can continue in the exact same PR-format and detail **PR‑05 and PR‑06 next**, because those two are where you’ll feel the biggest “everything is declarative now” payoff.
