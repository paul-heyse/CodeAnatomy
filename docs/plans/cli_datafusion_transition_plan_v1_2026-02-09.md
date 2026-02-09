# CLI-to-DataFusion Execution Transition Plan (v1)

**Date:** 2026-02-09
**Revision:** v3 (validated + best-in-class review integrated)
**Status:** Validated via targeted code review and best-in-class review; ready for implementation scoping
**Prerequisite:** DataFusion Planning Best-in-Class Architecture Plan v1 (substantially implemented)
**Scope:** Contract-preserving backend replacement: CLI + OBS + engine + runtime transition from Hamilton DAG orchestration to Rust-native DataFusion engine, including full removal of Hamilton/rustworkx dependencies across `src/cli`, `src/obs`, `src/engine`, `src/runtime_models`, and `src/graph`
**Review:** `docs/plans/cli_obs_datafusion_transition_best_in_class_review_v1_2026-02-09.md`

---

## 0) Executive Summary

The CodeAnatomy CLI currently routes all execution through a Hamilton DAG pipeline
(`hamilton_pipeline/execution.py` -> `execute_pipeline()`). The Rust engine
(`codeanatomy_engine`) already implements a complete compile->optimize->materialize
pipeline with full determinism, compliance, and Delta write support. A thin Python
facade (`engine/facade.py`) already bridges Python to Rust via PyO3.

**Goal:** Retire the Hamilton orchestration layer and route CLI execution directly
through the Rust engine, preserving extraction in Python and everything downstream
in Rust/DataFusion.

**Key insight:** The Rust engine already handles end-to-end semantic execution. What
is needed is to bypass Hamilton orchestration and drive the Rust engine directly from
the CLI layer, using the existing `engine/facade.py` bridge pattern as the template.

**Validation summary (v2):** Targeted code review confirmed:
- Extraction DAG has staged barriers (not flat): `repo_files` is a prerequisite for 5+ extractors, then `python_imports` depends on `ast_imports`/`cst_imports`/`ts_imports`, then `python_external` depends on `python_imports`
- Semantic compiler has ZERO Hamilton dependencies; standalone execution confirmed
- hamilton_pipeline/ contains 33 files (not 19); removal scope corrected
- 3 of 9 current pipeline outputs are NOT Rust-producible and need Python-side handling
- Rust engine has no determinism "tier" concept (2-hash model only); facade needs RuntimeConfig override support
- 24 test files import Hamilton and need migration

**Best-in-class review integration (v3):** Applied 9 mandatory corrections + 7 optimizations:
- Extraction DAG modeled as staged barriers (not flat parallel wave)
- Output contract re-baselined against actual `FULL_PIPELINE_OUTPUTS` keys
- Plan command migration uses explicit parity tiers (Tier 1/2/3)
- Warning model updated to structured `RunWarning` payloads (not strings)
- Config migration scope expanded to `config_models.py`, `runtime_models/root.py`, config template, config_loader
- Deletion scope expanded beyond `src/cli` + `src/obs` to include `src/engine/telemetry`, `src/engine/runtime_profile`, `src/datafusion_engine/lineage/diagnostics`, `src/serde_artifact_specs`
- Public API: `GraphProductBuildRequest` + `build_graph_product` updated to engine-native implementation (no shims)
- Error classification migrated from substring matching to stable Rust error codes
- Non-functional release gates defined (determinism, correctness, performance, observability)
- Clean cut: no backward compatibility, no transition scaffolding, no dual-emission - complete replacement

---

## 1) Current Execution Chain

```
CLI (cyclopts)
  -> build_command()          [src/cli/commands/build.py:734]
    -> GraphProductBuildRequest  [src/graph/product_build.py:98]
      -> build_graph_product()   [src/graph/product_build.py:131]
        -> execute_pipeline()    [src/hamilton_pipeline/execution.py]
          -> Hamilton Driver     [hamilton_pipeline/driver_factory.py]
            -> Extraction modules (Python: LibCST, AST, symtable, bytecode, SCIP, tree-sitter)
            -> Semantic pipeline  (Python: semantics/pipeline.py -> build_cpg())
            -> Normalization      (Python: semantics/compiler.py)
            -> CPG build          (Python: cpg/ modules)
            -> Delta writes       (Python: datafusion_engine/io/write.py)
```

**Hamilton-specific surface area in CLI:**
- `ExecutionMode` enum (3 values: `DETERMINISTIC_SERIAL`, `PLAN_PARALLEL`, `PLAN_PARALLEL_REMOTE`)
- `ExecutorConfig` (threadpool/multiprocessing/dask/ray selection + max_tasks)
- `GraphAdapterConfig` (adapter kind + options dict)
- `ScipIndexConfig` / `ScipIdentityOverrides` (extraction config, Hamilton-typed)
- `PipelineExecutionOptions` dataclass (Hamilton driver, materializers, overrides)
- `DriverBuildRequest` / `build_plan_context()` (Hamilton driver factory)
- `PlanArtifactBundle` / `build_plan_artifact_bundle()` (Hamilton plan artifacts)

---

## 2) Target Execution Chain

```
CLI (cyclopts)
  -> build_command()          [MODIFIED]
    -> Phase 1: Extraction    [Python - unchanged]
      -> repo_scan, AST, CST, symtable, bytecode, SCIP, tree-sitter
      -> Write extraction outputs to Delta tables (work_dir)
    -> Phase 2: Spec Build    [Python - existing]
      -> build_execution_spec() [src/engine/spec_builder.py:388]
      -> SemanticExecutionSpec  (msgspec.Struct, Rust-compatible)
    -> Phase 3: Rust Engine   [Rust via PyO3]
      -> execute_cpg_build()  [src/engine/facade.py:89]
        -> SessionFactory.from_class()
        -> SemanticPlanCompiler().compile(spec_json)
        -> CpgMaterializer().execute(factory, compiled)
      -> Returns RunResult dict with outputs, metrics, fingerprints
    -> Phase 4: Result Parse  [Python - new thin adapter]
      -> Map RunResult to GraphProductBuildResult
```

---

## 3) Component-by-Component Assessment

### 3.1 Components That Remain As-Is (No Changes Required)

| Component | File(s) | Rationale |
|-----------|---------|-----------|
| CLI App scaffold | `cli/app.py` | Cyclopts app, meta launcher, OTel wiring - fully execution-agnostic |
| RunContext | `cli/context.py` | Pure config container (run_id, log_level, config, span, otel) |
| CliResult | `cli/result.py` | Exit code semantics, success/error factories |
| Config loader | `cli/config_loader.py` | TOML/pyproject.toml resolution - **needs update**: remove `hamilton.tags` validation (§2.5 review) |
| Config command | `cli/commands/config.py` | Config display - **needs update**: remove `[hamilton]` from template (§2.5 review) |
| Delta commands | `cli/commands/delta.py` | Already DataFusion-native (uses DataFusionRuntimeProfile, delta_provider_from_session) |
| Diag command | `cli/commands/diag.py` | System diagnostics - execution-agnostic |
| Version command | `cli/commands/version.py` | Version display - **needs minor update**: remove rustworkx from dependency payload (§2.8 review, §7a.4) |
| CLI groups | `cli/groups.py` | Cyclopts parameter group definitions |
| CLI validators | `cli/validators.py` | Input validation - execution-agnostic |
| CLI converters | `cli/converters.py` | Determinism alias resolution |
| KV parser | `cli/kv_parser.py` | CLI key=value parsing utility |
| Path utils | `cli/path_utils.py` | Path resolution utility |
| OTel bootstrap | `obs/otel/` | OpenTelemetry spans/events - execution-agnostic |

### 3.2 Components That Need Significant Rework

#### 3.2.1 Build Command (`src/cli/commands/build.py`, 1296 lines)

**Current state:** Constructs `GraphProductBuildRequest` with Hamilton-specific fields, calls
`build_graph_product()` which drives the full Hamilton pipeline.

**Target state:** Two-phase execution: (1) extraction via Python, (2) semantic+CPG via Rust engine.

**Changes required:**

1. **Remove Hamilton executor options** (lines 242-284):
   - `--executor-kind` (threadpool/multiprocessing/dask/ray)
   - `--executor-max-tasks`
   - `--executor-remote-kind`
   - `--executor-remote-max-tasks`
   - `--executor-cost-threshold`

   These control Hamilton's parallel execution strategy. The Rust engine manages its own
   DataFusion execution context internally (session factory profile controls resource allocation).

2. **Remove Hamilton graph adapter options** (currently in BuildOptions):
   - `--graph-adapter-kind`
   - `--graph-adapter-option`
   - `--graph-adapter-option-json`

   These control Hamilton's graph adapter. Not applicable to Rust execution.

3. **Remove Hamilton tracker options** (currently in `_CliConfigOverrides`):
   - `--enable-hamilton-tracker`
   - `--hamilton-project-id`
   - `--hamilton-username`
   - `--hamilton-dag-name`
   - `--hamilton-api-url`
   - `--hamilton-ui-url`
   - `--hamilton-capture-data-statistics`
   - `--hamilton-max-list-length-capture`
   - `--hamilton-max-dict-length-capture`

   These are Hamilton-UI-specific telemetry. OTel (already integrated) replaces this.

4. **Replace `ExecutionMode`** with Rust engine equivalents:
   - `DETERMINISTIC_SERIAL` -> Rust engine determinism tier + serial session profile
   - `PLAN_PARALLEL` -> Rust engine default session profile (DataFusion manages parallelism)
   - `PLAN_PARALLEL_REMOTE` -> Rust engine "large" session profile

   New option: `--engine-profile` mapping to `SessionFactory.from_class()` classes
   (`"small"`, `"medium"`, `"large"`).

5. **Add Rust engine control options:**
   - `--rulepack-profile` (Default/LowLatency/Replay/Strict)
   - `--engine-profile` (small/medium/large) - maps to SessionFactory class
   - `--enable-compliance-capture` (bool, default False)
   - `--enable-rule-tracing` (bool, default False)
   - `--tracing-preset` (Maximal/MaximalNoData/ProductionLean)
   - `--enable-plan-preview` (bool, default False)

6. **Preserve extraction configuration** (no changes):
   - SCIP options remain (extraction is still Python)
   - Repo scope options remain (include/exclude globs, untracked, submodules, etc.)
   - Incremental options remain (extraction-level incrementality)
   - Tree-sitter option remains

7. **Preserve output/determinism options** (minor adaptation):
   - `--output-dir`, `--work-dir` remain
   - `--determinism-tier` becomes a Python-side validation wrapper (see §6.6);
     the Rust engine uses a 2-hash model (spec_hash + envelope_hash) without
     explicit tier semantics
   - `--writer-strategy` becomes unnecessary (Rust engine owns Delta writes)
   - `--runtime-profile` maps to Rust engine session profile + rulepack profile

8. **Remove plan override options** (lines 97-122):
   - `--plan-allow-partial`
   - `--plan-requested-task`
   - `--plan-impacted-task`
   - `--enable-metric-scheduling`
   - `--enable-plan-diagnostics`
   - `--enable-plan-task-submission-hook`
   - `--enable-plan-task-grouping-hook`
   - `--enforce-plan-task-submission`

   These control Hamilton plan compilation. The Rust engine manages its own planning
   surface (SemanticPlanCompiler + PreparedExecutionContext).

#### 3.2.2 Plan Command (`src/cli/commands/plan.py`, 319 lines)

**Current state:** Calls `build_plan_context()` from Hamilton driver factory, produces
`ExecutionPlan` and `PlanArtifactBundle` using Hamilton internals. Currently depends on
Hamilton/relspec plan objects and artifact bundles (`plan.py:108-109,119,122`).

**Target state:** Uses Rust engine's `SemanticPlanCompiler` to produce a compiled plan,
then inspects its properties via tiered parity.

**CORRECTION (from review §2.3):** The Rust `SemanticPlanCompiler` validates and hashes
specs (`compiler.rs:31,44`) but does NOT currently expose schedule/graph/validation
bundles. Plan command migration must be explicitly tiered to avoid promising features
that require APIs that do not yet exist.

**Plan Command Parity Tiers:**

| Tier | Features | API Requirements | Status |
|------|----------|-----------------|--------|
| **Tier 1 (minimum)** | spec hash, output target list, high-level graph summary | `CompiledPlan.spec_hash_hex()`, spec field inspection | **Available now** |
| **Tier 2** | dependency edges, schedule groups | PyO3-exported plan summary/schedule metadata | **Requires new Rust API** |
| **Tier 3** | validation envelopes equivalent to legacy `PlanArtifactBundle` | PyO3-exported introspection/validation metadata | **Requires new Rust API** |

**Implementation rule:** Do NOT promise Tier 2/3 behavior until corresponding Rust
introspection APIs exist.

**Changes required:**

1. **Replace Hamilton plan compilation** with Rust engine plan compilation:
   - Current: `build_plan_context()` -> `DriverBuildRequest` -> Hamilton driver -> `ExecutionPlan`
   - Target: `build_execution_spec()` -> `SemanticPlanCompiler().compile(spec_json)` -> `CompiledPlan`

2. **Map plan inspection features** to Rust plan artifacts (tiered):
   - **Tier 1:** `plan_signature` -> `CompiledPlan.spec_hash_hex()`
   - **Tier 1:** `task_count` -> Count of view definitions in spec
   - **Tier 1:** `output_format` (text/json/dot) -> Adapt formatters to new data structure
   - **Tier 2:** `show_graph` -> Derive from compiled plan's view definitions + join graph edges
     (requires Rust plan summary API)
   - **Tier 2:** `show_schedule` -> Display execution spec's view dependency topology
     (requires Rust schedule export API)
   - **Tier 2:** `show_inferred_deps` -> Extract from view_dependencies fields
   - **Tier 3:** `show_task_graph` -> Full DAG with validation metadata
   - **Tier 3:** `validate` -> Compile-time validation envelope equivalent to `PlanArtifactBundle`

3. **Remove `ExecutionMode` dependency:**
   - Plan command currently takes `--execution-mode` from Hamilton
   - Replace with `--engine-profile` and `--rulepack-profile` (only affect plan shape, not
     plan inspection)

4. **New Rust introspection API (required for Tier 2+):**
   - PyO3-exported plan summary (view count, dependency edges, schedule groups)
   - PyO3-exported validation metadata (rule results, constraint checks)
   - Add to `rust/codeanatomy_engine/src/python/compiler.rs` as new methods on
     `SemanticPlanCompiler` or `CompiledPlan`

#### 3.2.3 GraphProductBuildRequest (`src/graph/product_build.py`, 670 lines)

**Current state:** Fat request object containing Hamilton-specific fields
(`execution_mode`, `executor_config`, `graph_adapter_config`, `use_materialize`).
Calls `execute_pipeline()` from Hamilton. Public API centers on `GraphProductBuildRequest`
(`product_build.py:98`) and `build_graph_product()` (`product_build.py:131`), which
currently routes through Hamilton execution (`product_build.py:306`).

**NOTE (from review §2.7):** `GraphProductBuildRequest` + `build_graph_product()`
are the primary public API entrypoints. Since Hamilton/rustworkx have not been shipped
in production, there is no backward compatibility constraint. The public API is updated
directly to engine-native types with no shims or deprecation period.

**Target state:** Clean engine-native public API.

**Changes required:**

1. **Rewrite `GraphProductBuildRequest`** with engine-native fields only:
   - **Remove all Hamilton-specific fields:**
     - `execution_mode: ExecutionMode` → DELETE
     - `executor_config: ExecutorConfig | None` → DELETE
     - `graph_adapter_config: GraphAdapterConfig | None` → DELETE
     - `use_materialize: bool` → DELETE
   - **Add engine fields:**
     - `engine_profile: str = "medium"` (SessionFactory class)
     - `rulepack_profile: str = "default"` (rule profile)
     - `runtime_config: RuntimeConfig | None = None` (engine/spec_builder.py)

2. **Replace `_execute_build()`** (line 282):
   - Current: Calls `execute_pipeline()` from Hamilton, wraps in root OTel span
   - Target: Direct orchestrator call → extraction → spec build → Rust engine
   - Parse RunResult into `GraphProductBuildResult`

4. **Replace `_outputs_for_request()`** (line 375):
   - Current: Returns Hamilton output node names (`write_cpg_nodes_delta`, etc.)
   - Target: Not needed - the Rust engine produces all outputs as part of its RunResult

5. **Replace `_pipeline_options()`** (line 217):
   - Current: Builds `PipelineExecutionOptions` for Hamilton
   - Target: Build spec + facade call arguments

---

## 4) Extraction Phase Design

Extraction **must remain Python** because the evidence sources are Python-native tools:
- **LibCST** - Python CST parsing (Python library)
- **AST** - Python built-in AST module
- **symtable** - Python built-in symbol table
- **bytecode/dis** - Python bytecode introspection
- **SCIP** - scip-python indexer (subprocess)
- **tree-sitter** - tree-sitter Python bindings

### 4.1 Extraction Output Contract

Extraction currently writes intermediate results as in-memory PyArrow tables, passed through
Hamilton nodes. For the Rust engine to consume them, extraction outputs need to be:

1. **Written to Delta tables** in a work directory (the Rust engine reads from Delta locations)
2. **Mapped to `input_relations`** in the `SemanticExecutionSpec`

**Current flow:**
```
Extraction modules -> PyArrow tables -> Hamilton nodes -> semantic pipeline -> CPG
```

**Target flow:**
```
Extraction modules -> PyArrow tables -> Delta tables (work_dir/extract/)
  -> input_relations map -> SemanticExecutionSpec -> Rust engine
```

### 4.2 Extraction Isolation Strategy

Options for running extraction outside Hamilton:

**Option A: Direct function calls (VALIDATED - recommended)**
- Call extraction functions directly from a new extraction orchestrator
- Use `concurrent.futures.ThreadPoolExecutor` for parallelism
- Write results to Delta using existing `datafusion_engine/io/write.py`
- Simplest approach, minimal new code
- **Validated:** Extraction DAG has staged dependencies requiring barrier synchronization:
  - **Stage 0:** `repo_files` (repo scan) - required by all downstream extractors
  - **Stage 1 (parallel):** `ast_extract`, `cst_extract`, `symtable_extract`, `bytecode_extract`,
    `scip_extract`, `treesitter_extract` - all depend on `repo_files` but are independent of each other
  - **Stage 2:** `python_imports` - depends on `ast_imports`, `cst_imports`, `ts_imports`
    (outputs from Stage 1 extractors)
  - **Stage 3:** `python_external` - depends on `python_imports`

  Evidence: `repo_files` is required by AST/CST/tree-sitter/bytecode/symtable extractors
  (`task_execution.py:708,807,823,839,855,872`). `python_imports` depends on `ast_imports`,
  `cst_imports`, `ts_imports` (`task_execution.py:774-776`). `python_external` depends on
  `python_imports` (`task_execution.py:790`).

  All extractors are plain Python functions with no Hamilton decorators - they consume
  config and produce Arrow tables. This makes Option A implementable with a staged
  ThreadPoolExecutor approach: run `repo_files` first, then parallel Stage 1, then
  sequential Stage 2→3.

**Option B: Keep Hamilton for extraction only**
- Use Hamilton exclusively for the extraction DAG
- Write extraction outputs to Delta at Hamilton boundary
- Feed Delta locations to Rust engine
- Pros: Preserves extraction dependency ordering without reimplementing
- Cons: Retains Hamilton as a dependency (though only for extraction)
- **Verdict:** Unnecessary given the staged-but-simple DAG structure confirmed by validation

**Option C: Rust-managed extraction coordination**
- Extend the Rust engine to orchestrate Python extraction calls via PyO3 callbacks
- Most complex, highest long-term payoff
- Significant Rust work to manage Python function dispatch
- **Verdict:** Over-engineered for current needs; revisit if extraction grows more complex

**Recommendation:** **Option A** is definitively validated. The extraction DAG's staged
structure (repo_scan → parallel extractors → imports synthesis → external) is simple enough
to manage with explicit barrier synchronization. Hamilton is unnecessary for this level of
dependency management. All extractor outputs are standard Arrow types (TableLike /
RecordBatchReaderLike), directly writable to Delta without Hamilton-specific wrappers.

---

## 5) Spec Building Phase Design

The spec builder (`engine/spec_builder.py`) is already complete and execution-agnostic.
It converts `SemanticIR` to `SemanticExecutionSpec` via `build_execution_spec()`.

### 5.1 IR Production Without Hamilton

Currently, the semantic IR is produced inside Hamilton pipeline nodes. To produce it
independently:

1. The semantic compiler (`semantics/compiler.py`) needs to be callable directly
2. The input registry (`semantics/input_registry.py`) needs extraction outputs as input
3. The IR pipeline (`semantics/ir_pipeline.py`) orchestrates compile->infer->optimize->emit

**Key function chain:**
```python
# 1. Register extraction outputs
input_registry = build_input_registry(extraction_delta_locations)

# 2. Run semantic compiler
compiler = SemanticCompiler(config=semantic_config)
ir = compiler.compile(input_registry)

# 3. Build execution spec
spec = build_execution_spec(
    ir=ir,
    input_locations=extraction_delta_locations,
    output_targets=["cpg_nodes", "cpg_edges", "cpg_props", ...],
    rulepack_profile=rulepack_profile,
)

# 4. Submit to Rust engine
result = execute_cpg_build(
    extraction_inputs=extraction_delta_locations,
    semantic_spec=spec,
    environment_class=engine_profile,
)
```

### 5.2 Semantic Pipeline Dependency

The semantic pipeline (`semantics/pipeline.py:build_cpg()`) currently runs inside Hamilton.
Key question: **Can `build_cpg()` run standalone?**

**Answer: YES - CONFIRMED.** Targeted code review verified that `build_cpg()`,
`SemanticCompiler`, and the entire IR pipeline (`ir_pipeline.py` compile→infer→optimize→emit)
have **zero Hamilton dependencies**. Not a single Hamilton import exists in any of these modules.

The only Hamilton coupling point in the entire `semantics/` tree is a single lazy optional
import in `semantics/incremental/config.py:166` where `to_run_snapshot()` imports
`IncrementalRunConfig` from `hamilton_pipeline.types`. This is easily removable by either:
- Moving the `IncrementalRunConfig` dataclass to a standalone module, or
- Inlining the snapshot conversion logic

The Hamilton layer currently provides only:
- Input wiring (DataFrame -> Hamilton node -> `build_cpg()`)
- Output wiring (`build_cpg()` -> Hamilton node -> Delta write)

Both of these can be replaced with direct calls. No modifications to the semantic
compiler are needed.

---

## 6) Rust Engine Integration Phase

### 6.1 Existing Bridge (`engine/facade.py`)

The facade already implements the full Python->Rust handoff:

```python
def execute_cpg_build(
    extraction_inputs: dict[str, str],    # logical_name -> Delta location
    semantic_spec: SemanticExecutionSpec,  # Compiled spec
    environment_class: str = "medium",    # Session factory class
) -> dict[str, object]:                   # RunResult envelope
```

**PyO3 classes exposed:**
- `SessionFactory` - Creates DataFusion session with appropriate resource profile
- `SemanticPlanCompiler` - Compiles `SemanticExecutionSpec` JSON to `CompiledPlan`
- `CompiledPlan` - Compiled logical plan with spec hash, ready for materialization
- `CpgMaterializer` - Executes compiled plan: input registration -> optimization -> Delta writes
- `PyRunResult` - Result envelope with JSON serialization

### 6.2 Error Handling Chain

The facade maps Rust errors to Python exception hierarchy:
- `EngineValidationError` - Spec validation / contract mismatch
- `EngineCompileError` - Plan compilation failure
- `EngineRuleViolationError` - Rulepack policy violation
- `EngineRuntimeError` - Runtime / materialization failure

**CORRECTION (from review §2.9):** The current error classification in `engine/facade.py:34-40`
uses brittle message substring matching (`"invalid spec json"`, `"failed to compile plans"`,
`"materialization failed"`) to determine error categories. This is fragile and will break
if Rust error messages change.

**Required: Stable Rust Error Code Taxonomy**

Add Rust-side `error_code` + `error_stage` fields to error payloads at the PyO3 boundary:

```rust
// In Rust engine error types:
pub struct EngineError {
    pub error_code: &'static str,    // e.g., "E001", "E002"
    pub error_stage: &'static str,   // e.g., "validation", "compilation", "runtime"
    pub message: String,
}
```

Map codes in Python instead of free-form message text:

```python
_ERROR_CODE_MAP: dict[str, type[EngineError]] = {
    "validation": EngineValidationError,
    "compilation": EngineCompileError,
    "rule_violation": EngineRuleViolationError,
    "runtime": EngineRuntimeError,
    "materialization": EngineRuntimeError,
}
```

**CLI exit code mapping (new):**

| Engine Error | Error Code | CLI Exit Code | User Message |
|-------------|-----------|---------------|--------------|
| `EngineValidationError` | `validation/*` | 2 | Spec validation failed (likely extraction schema mismatch) |
| `EngineCompileError` | `compilation/*` | 3 | Plan compilation failed (view dependency or transform error) |
| `EngineRuleViolationError` | `rule_violation/*` | 4 | Policy rule violation (strictness/safety enforcement) |
| `EngineRuntimeError` | `runtime/*`, `materialization/*` | 5 | Materialization or Delta write failure |
| `ImportError` (engine not built) | N/A | 10 | Run `scripts/rebuild_rust_artifacts.sh` |

### 6.3 Warning and Metrics Contracts

**CORRECTION (from review §2.4):** `RunResult.warnings` is `Vec<RunWarning>` (structured
warning type at `result.rs:36`), NOT `Vec<String>` as previously assumed. Warning
aggregation is integrated into trace summary (`pipeline.rs:227-228`), and the summary
payload includes warning counters and code map (`metrics_collector.rs:29-30`).

**Warning data flow:**

```
Rust RunResult.warnings: Vec<RunWarning>
  ├─ Each RunWarning has: code, severity, message, context
  ├─ Aggregated in TraceMetricsSummary: warning_count_by_code: HashMap<String, u64>
  └─ Available in CollectedMetrics.operator_metrics for per-operator warnings
```

**Python-side mapping:**

| Rust Field | Python Target | Description |
|-----------|--------------|-------------|
| `RunResult.warnings` | OTel warning events + diagnostics artifacts | Structured warnings with codes |
| `trace_metrics_summary.warning_count_by_code` | `src/obs/otel/metrics.py` warning counters | Warning counts by code for metrics |
| `collected_metrics.operator_metrics` | Per-operator diagnostic analysis | Operator-level warning detail |

Map these directly to `src/obs/otel/metrics.py` and diagnostics report sections.

### 6.4 RuntimeConfig Wiring

The `SemanticExecutionSpec` already includes `RuntimeConfig` with tracing, compliance,
and tuning controls. CLI flags should map directly:

```python
RuntimeConfig(
    compliance_capture=cli.enable_compliance_capture,
    enable_tracing=cli.enable_tracing,
    enable_rule_tracing=cli.enable_rule_tracing,
    enable_plan_preview=cli.enable_plan_preview,
    tracing_preset=cli.tracing_preset,
    tracing=TracingConfig(
        enabled=cli.enable_tracing,
        otlp_endpoint=otel_options.otlp_endpoint,
        otlp_protocol=otel_options.otlp_protocol,
        otel_service_name=otel_options.service_name,
        instrument_object_store=cli.instrument_object_store,
    ),
)
```

**VALIDATION NOTE - Facade Enhancement Required:**

The current `execute_cpg_build()` facade accepts only `environment_class: str` and cannot
pass RuntimeConfig overrides at runtime. The RuntimeConfig is embedded inside the
`SemanticExecutionSpec`, which means it must be set at spec-build time. This is correct
for the proposed architecture (RuntimeConfig flows through the spec), but there are two
gaps to address:

1. **No runtime profile override.** The facade's `environment_class` maps to
   `SessionFactory.from_class()` which bakes the session profile. If the user wants
   `--engine-profile large`, this must be passed as `environment_class="large"` at
   facade call time. This works correctly as-is.

2. **`--enable-plan-preview` semantics.** This flag currently only sets `tracing.preview_limit`
   in the TracingConfig. It does NOT produce a separate plan preview artifact - it enables
   the datafusion-tracing community extension's partial-result preview subsystem, which
   captures row previews into OTel span attributes. The CLI documentation for this flag
   should reflect this: "Enable partial-result previews in tracing spans" rather than
   implying a separate preview output.

3. **Cache policy is spec-static.** The Rust engine's `CachePlacementPolicy` is
   determined at compile time from the spec, not overridable at runtime. This is
   fine for the proposed architecture since cache behavior should be deterministic
   per spec.

### 6.5 Rust Engine Output Gap Analysis

**CRITICAL:** The output contract must be baselined against the actual live output keys,
not assumed names. The canonical full output set is `FULL_PIPELINE_OUTPUTS` in
`src/hamilton_pipeline/execution.py:36`. The `GraphProductBuildRequest` path requests a
subset via `_outputs_for_request` in `src/graph/product_build.py:375`, which currently
excludes `write_normalize_outputs_delta`.

**CORRECTION (from review §2.1):** The previous version of this table listed output names
(`write_semantic_normalize_delta`, `write_semantic_relate_delta`, `write_evidence_summary_delta`)
that are NOT current Hamilton outputs. The table below uses actual live keys from
`FULL_PIPELINE_OUTPUTS`.

| Hamilton Output Node | Rust-Producible | Notes |
|---------------------|----------------|-------|
| `write_cpg_nodes_delta` | YES | Core CPG output |
| `write_cpg_edges_delta` | YES | Core CPG output |
| `write_cpg_properties_delta` | YES | Core CPG output |
| `write_normalize_outputs_delta` | **NO** | Python-side normalization state tracking (currently excluded from `_outputs_for_request`) |
| `write_extract_error_artifacts_delta` | **NO** | Python-side extraction error collection |
| `write_run_manifest_delta` | **NO** | Hamilton datasaver output node with Delta writes |

**NOTE:** Additional output keys exist in `FULL_PIPELINE_OUTPUTS` that should be
verified against the current codebase before implementation. The exact set must be
confirmed by reading `execution.py:36` at implementation time.

**Impact:** The 3 non-Rust-producible outputs need Python-side handling:

1. **`write_normalize_outputs_delta`** - Can be produced by the Python semantic compiler
   as a side-effect of `build_cpg()`, written to Delta before Rust engine invocation.
2. **`write_extract_error_artifacts_delta`** - Must be collected during Python extraction
   phase and written to Delta by the extraction orchestrator.
3. **`write_run_manifest_delta`** - Requires reimplementation as a Python-side post-processing
   step that constructs the manifest from the Rust engine's `RunResult` envelope combined
   with extraction metadata. The current Hamilton manifest is a full datasaver output node
   with Delta writes that cannot be replicated by the Rust engine.

**Recommendation:** Phase 2 (Build Command Rewrite) must include auxiliary output handling
for these 3 outputs. They should be produced by the Python orchestration layer alongside
the Rust engine execution.

### 6.6 Determinism Model Clarification

**CORRECTION:** The Rust engine does NOT have a "determinism tier" concept. It uses a
2-hash determinism model:
- `spec_hash` - Hash of the input `SemanticExecutionSpec`
- `envelope_hash` - Hash of the full execution envelope (spec + session + outputs)

The current CLI's `--determinism-tier` option (mapping to levels like `canonical`,
`reproducible`, `best_effort`) has no direct Rust equivalent. Options:

1. **Map to rulepack profiles:** `canonical` → Strict profile, `best_effort` → Default profile
2. **Implement as a Python-side contract** that validates the 2-hash model meets tier requirements
3. **Preserve the option** but document it as controlling Python-side validation, not engine behavior

**Recommendation:** Option 2 - keep `--determinism-tier` as a Python-side validation
wrapper that checks the Rust engine's hash-based guarantees meet the requested tier.

---

## 7) Observability Layer Transition (Hamilton/Rustworkx → DataFusion)

### 7.0 Overview

The `src/obs/` module (27 files) currently contains Hamilton lifecycle hooks, rustworkx
scheduling metrics, and Hamilton-specific diagnostics artifacts. ALL Hamilton and rustworkx
dependencies in obs/ must be removed and replaced with DataFusion-sourced equivalents.

The Rust engine's tracing infrastructure (feature-gated via `datafusion-tracing`) already
provides rich execution instrumentation that can replace most Hamilton observability:

| Rust Engine Tracing Capability | Replaces |
|-------------------------------|----------|
| `ExecutionSpanInfo` (spec_hash, envelope_hash, profile) | Hamilton graph execution span |
| Per-operator `InstrumentedExec` spans with metrics | Hamilton per-node OTel spans |
| `TraceMetricsSummary` (rows, batches, bytes, compute_nanos, spill, selectivity) | Hamilton task duration metrics |
| `CollectedMetrics` + `OperatorMetricSummary` | Hamilton cache lineage diagnostics |
| Rule-phase spans (analyzer/optimizer/physical) with plan diffs | Hamilton plan events |
| `ComplianceCapture` (EXPLAIN traces, rule impact, rulepack snapshot) | Hamilton plan validation |
| `SessionEnvelope` (versions, config, registrations, hashes) | Hamilton graph snapshot |
| Partial-result previews (redactable, configurable width/height) | No Hamilton equivalent |
| Instrumented object store spans (get/put/list) | No Hamilton equivalent |

### 7.1 Files to DELETE (Hamilton-Coupled, No Migration Needed)

| File | Contents | Rationale |
|------|----------|-----------|
| `src/obs/otel/hamilton.py` | `OtelNodeHook`, `OtelPlanHook` (Hamilton lifecycle hooks) | Replaced by Rust engine execution spans |
| `src/hamilton_pipeline/lifecycle.py` | `DiagnosticsNodeHook` (Hamilton node event recording) | Replaced by Rust compliance capture |
| `src/hamilton_pipeline/cache_lineage.py` | Hamilton cache introspection + lineage export | Replaced by Rust `CachePlacementPolicy` |
| `src/hamilton_pipeline/cache_versioning.py` | Hamilton hash_value fingerprinting | Replaced by Rust spec_hash determinism |

### 7.2 Metrics to Transition (`src/obs/otel/metrics.py`)

The `MetricsRegistry` (14 instruments) needs selective migration:

| Metric Instrument | Current Source | DataFusion Replacement | Action |
|-------------------|---------------|----------------------|--------|
| `stage_duration` (Histogram) | Hamilton OtelNodeHook `layer` tag | Rust execution span durations; Python extraction orchestrator spans | **Repoint**: source from Rust `elapsed_compute_nanos` + Python extraction spans |
| `task_duration` (Histogram) | Hamilton OtelNodeHook `task_kind` tag | Rust per-operator `OperatorMetricSummary.elapsed_compute_nanos` | **Repoint**: source from `RunResult.collected_metrics` |
| `datafusion_duration` (Histogram) | Already DataFusion-native | No change needed | **Keep as-is** |
| `write_duration` (Histogram) | Already DataFusion-native | No change needed | **Keep as-is** |
| `artifact_count` (Counter) | Generic `DiagnosticsCollector` | No change needed | **Keep as-is** |
| `error_count` (Counter) | Generic `DiagnosticsCollector` | No change needed | **Keep as-is** |
| `dataset_rows` (Gauge) | Generic scan telemetry | No change needed | **Keep as-is** |
| `dataset_columns` (Gauge) | Generic scan telemetry | No change needed | **Keep as-is** |
| `scan_row_groups` (Gauge) | Generic scan telemetry | No change needed | **Keep as-is** |
| `scan_fragments` (Gauge) | Generic scan telemetry | No change needed | **Keep as-is** |
| `cache_operation_count` (Counter) | Hamilton cache hooks | Rust `CachePlacementPolicy` decisions | **Repoint**: source from Rust spec cache decisions |
| `cache_operation_duration` (Histogram) | Hamilton cache hooks | Rust engine internal cache timing | **Repoint**: source from Rust execution metrics |
| `storage_operation_count` (Counter) | Generic storage operations | No change needed | **Keep as-is** |
| `storage_operation_duration` (Histogram) | Generic storage operations | No change needed | **Keep as-is** |

**Summary**: 10/14 instruments are already generic (keep as-is). 4 instruments need repointing
from Hamilton hooks to Rust engine data.

**`record_task_duration()` migration**: Currently called by `OtelNodeHook.run_after_node_execution()`
with Hamilton `task_kind` tag. Replace with a post-execution function that extracts per-operator
timings from `RunResult.collected_metrics.operator_metrics` and records them as
`record_task_duration(operator_name, elapsed_compute_nanos / 1e9, status="ok")`.

**`record_stage_duration()` migration**: Currently called by `OtelNodeHook` with Hamilton
`layer` tag (e.g., "extraction", "semantic", "cpg"). Replace with explicit calls from the
Python orchestrator for extraction phase timing, and from Rust engine spans for downstream phases.

### 7.3 Diagnostics Artifacts to Transition

#### 7.3.1 Hamilton-Specific Artifacts (REMOVE)

| Artifact Spec | Current Source | Action |
|--------------|---------------|--------|
| `HAMILTON_CACHE_LINEAGE_SPEC` | `record_cache_lineage()` → Hamilton `driver.cache` | **DELETE**: No equivalent needed; Rust CachePlacementPolicy is spec-static |
| `HAMILTON_PLAN_EVENTS_SPEC` | Hamilton lifecycle `hamilton_node_start_v1` etc. | **REPLACE**: With Rust `ComplianceCapture.rule_impact` |
| `HAMILTON_GRAPH_SNAPSHOT_SPEC` | Hamilton driver state snapshot | **REPLACE**: With Rust `SessionEnvelope` (versions, config, registrations, hashes) |

#### 7.3.2 Hamilton Event Types (REMOVE)

| Event Type | Current Source | Replacement |
|-----------|---------------|-------------|
| `hamilton_node_start_v1` | `DiagnosticsNodeHook` | Rust per-operator tracing spans |
| `hamilton_node_finish_v1` | `DiagnosticsNodeHook` | Rust per-operator tracing spans |
| `hamilton_task_submission_v1` | `DiagnosticsNodeHook` | Not needed (Rust engine manages scheduling) |
| `hamilton_task_grouping_v1` | `DiagnosticsNodeHook` | Not needed |
| `hamilton_task_expansion_v1` | `DiagnosticsNodeHook` | Not needed |
| `hamilton_task_routing_v1` | `DiagnosticsNodeHook` | Not needed |
| `hamilton_plan_drift_v1` | `PlanDiagnosticsHook` | Rust `ComplianceCapture.rulepack_snapshot.fingerprint` |
| `hamilton_cache_lineage_nodes_v1` | `record_cache_lineage()` | Not needed |

#### 7.3.3 Rustworkx/Plan Artifacts (REPLACE)

| Artifact Spec | Current Source | DataFusion Replacement |
|--------------|---------------|----------------------|
| `PLAN_SCHEDULE_SPEC` → `PlanScheduleArtifact` | `TaskSchedule` from rustworkx TopologicalSorter | Rust `CompiledPlan` view dependency topology; derive `ordered_tasks` from view_definitions topological order |
| `PLAN_VALIDATION_SPEC` → `PlanValidationArtifact` | rustworkx graph validation (node/edge counts) | Rust `SemanticPlanCompiler.compile()` validation result (compile-time validation) |
| `PLAN_SIGNALS_SPEC` → `PlanSignalsArtifact` | Plan signature, task count | Rust `CompiledPlan.spec_hash_hex()`, `len(spec.view_definitions)` |
| `PLAN_EXPECTED_TASKS_SPEC` | rustworkx schedule generation counts | Rust spec `view_definitions` count; compile validates completeness |

**Key insight**: Rustworkx scheduling artifacts tracked *task graph topology* (generations,
critical path costs, articulation points, bridge edges, betweenness centrality). The Rust
engine does NOT replicate this graph analysis - it manages scheduling internally via
DataFusion's native partitioning and parallelism. The replacement artifacts should reflect
the *compiled plan topology* (view dependencies, join graph edges) rather than attempting
to replicate rustworkx graph metrics.

**New artifact definitions needed:**

```python
class EnginePlanSummaryArtifact(StructBaseStrict, frozen=True):
    """Summary of Rust engine compiled plan."""
    spec_hash: str                        # From CompiledPlan.spec_hash_hex()
    view_count: int                       # len(spec.view_definitions)
    join_edge_count: int                  # len(spec.join_graph.edges)
    rule_intent_count: int                # len(spec.rule_intents)
    rulepack_profile: str                 # spec.rulepack_profile
    input_relation_count: int             # len(spec.input_relations)
    output_target_count: int              # len(spec.output_targets)

class EngineExecutionSummaryArtifact(StructBaseStrict, frozen=True):
    """Summary of Rust engine execution result."""
    spec_hash: str                        # From RunResult
    envelope_hash: str                    # From RunResult
    output_rows: int                      # TraceMetricsSummary.output_rows
    output_batches: int                   # TraceMetricsSummary.output_batches
    elapsed_compute_nanos: int            # TraceMetricsSummary.elapsed_compute_nanos
    spill_file_count: int                 # TraceMetricsSummary.spill_file_count
    spilled_bytes: int                    # TraceMetricsSummary.spilled_bytes
    operator_count: int                   # TraceMetricsSummary.operator_count
    tables_materialized: int              # len(RunResult.outputs)
    total_rows_written: int               # sum(MaterializationResult.rows_written)
    warning_count: int                    # len(RunResult.warnings)
    warning_codes: tuple[str, ...]        # Unique RunWarning codes (structured, not strings)
```

#### 7.3.4 Artifacts That Are Already DataFusion-Generic (KEEP)

These require NO changes:

| Artifact Spec | Source | Notes |
|--------------|--------|-------|
| `DATAFUSION_PREPARED_STATEMENTS_SPEC` | `PreparedStatementSpec` | DataFusion SQL prepared statements |
| `DATAFUSION_VIEW_ARTIFACTS_SPEC` | `DataFusionViewArtifact` | View determinism fingerprints |
| `VIEW_FINGERPRINTS_SPEC` | `record_view_fingerprints()` | View fingerprints from DataFusion views |
| `VIEW_UDF_PARITY_SPEC` | `record_view_udf_parity()` | UDF parity checks |
| `RUST_UDF_SNAPSHOT_SPEC` | `record_rust_udf_snapshot()` | Rust UDF state |
| `VIEW_CONTRACT_VIOLATIONS_SPEC` | `record_view_contract_violations()` | Schema contract validation |
| `DATAFRAME_VALIDATION_ERRORS_SPEC` | `record_dataframe_validation_error()` | DataFrame validation errors |
| `SEMANTIC_QUALITY_ARTIFACT_SPEC` | `record_semantic_quality_artifact()` | Quality diagnostics |

### 7.4 Diagnostics Report Transition (`src/obs/diagnostics_report.py`)

The `DiagnosticsReport` (14 sections) consumes OTel spans and logs. Most sections are
data-source-agnostic (they process span attributes regardless of whether Hamilton or
Rust engine produced them). Key analysis:

| Report Section | Data Source | Hamilton-Coupled? | Action |
|---------------|------------|-------------------|--------|
| `slow_spans` | OTel span durations | NO - reads `codeanatomy.stage` attribute | **Keep** - works with any span source |
| `stage_breakdown` | OTel span durations | NO | **Keep** - adapt stage names for new phases |
| `idle_gaps` | OTel span timing | NO | **Keep** |
| `metrics` | Gauge snapshot | NO | **Keep** |
| `log_summary` | OTel log count | NO | **Keep** |
| `dataset_readiness` | `dataset_readiness_v1` events | NO | **Keep** |
| `provider_modes` | `dataset_provider_mode_v1` events | NO | **Keep** |
| `delta_log_health` | `delta_log_health_v1` events | NO | **Keep** |
| `plan_execution_diff` | `plan_expected_tasks_v1` + execution events | **YES** - references Hamilton task counts | **Adapt**: Source from Rust spec view_definitions vs RunResult.outputs |
| `plan_phase_summary` | `datafusion_plan_phase_v1` events | NO | **Keep** |
| `datafusion_execution` | DataFusion span data | NO | **Keep** |
| `scan_pruning` | `scan_unit_pruning_v1` events | NO | **Keep** |
| `runtime_capabilities` | Runtime probe events | NO | **Keep** |
| `diagnostic_categories` | Severity aggregation | NO | **Keep** |

**Only 1 of 14 report sections is Hamilton-coupled** (`plan_execution_diff`). This
section compares expected vs executed task counts. Replace with: expected = `len(spec.view_definitions)`,
executed = `len(RunResult.outputs)`.

### 7.5 Scan Telemetry (`src/obs/scan_telemetry.py`)

Already DataFusion-generic. The `ScanTelemetry` data model (fragment_count, row_group_count,
scan_profile, etc.) consumes PyArrow dataset metadata, not Hamilton or rustworkx data.
**No changes needed.**

### 7.6 `record_cache_lineage()` Removal

`src/obs/diagnostics.py:record_cache_lineage()` (lines 320-330) directly references
`HAMILTON_CACHE_LINEAGE_SPEC` and emits `hamilton_cache_lineage_nodes_v1` events.

**Action**: DELETE this function entirely. The Rust engine's `CachePlacementPolicy` is
spec-static and deterministic - there is no runtime cache lineage to track. Cache behavior
is fully determined at compile time from the `SemanticExecutionSpec`.

### 7.7 New Observability Data Flow

```
Python Orchestrator                    Rust Engine (via PyO3)
├─ Extraction spans (per-extractor)    ├─ codeanatomy_engine.execute root span
│  ├─ record_stage_duration()          │  ├─ Rule phase spans (analyzer/optimizer/physical)
│  └─ record_error() on failure        │  │  ├─ Per-rule spans (Full mode)
├─ Spec build span                     │  │  └─ Plan diffs (when enabled)
│  └─ record_stage_duration()          │  ├─ Per-operator InstrumentedExec spans
│                                      │  │  ├─ DataFusion metrics (rows, compute, spill)
│                                      │  │  └─ Partial-result previews (when enabled)
│                                      │  ├─ Object store spans (when instrument_object_store=true)
│                                      │  └─ Materialization spans (Delta writes)
│                                      │
│                                      └─ RunResult envelope:
│                                         ├─ TraceMetricsSummary → record_task_duration()
│                                         ├─ CollectedMetrics → EngineExecutionSummaryArtifact
│                                         ├─ ComplianceCapture → replaces Hamilton plan events
│                                         ├─ SessionEnvelope → replaces Hamilton graph snapshot
│                                         ├─ PlanBundleArtifacts → replaces plan validation
│                                         └─ Warnings → diagnostic events
```

### 7.8 Additional Rust Engine Tracing Scope

To fully replace Hamilton observability, the following tracing enhancements may be needed
in the Rust engine (leveraging `datafusion-tracing` extension patterns):

1. **Materialization-level spans**: Currently, Delta writes in the Rust engine may not emit
   dedicated tracing spans. Consider adding `#[instrument]` to `delta_writer.rs` write
   functions to emit per-table materialization spans with row counts and bytes written.
   This would source the `write_duration` metric natively from Rust.

2. **Cache decision spans**: When `CachePlacementPolicy` makes caching decisions during
   plan compilation, emit tracing events recording which views were cached and why.
   This replaces `record_cache_event()` sourcing from Hamilton.

3. **Input registration spans**: Add tracing to `SessionFactory` input registration to
   capture per-table registration timing, replacing Hamilton's per-node input registration
   events.

These are best-in-class enhancements that leverage the `datafusion-tracing` community
extension's `InstrumentationOptions` pattern:
- `.record_metrics(true)` captures DataFusion native metrics as span fields
- `.add_custom_field("trace.spec_hash", ...)` correlates all spans to the execution spec
- `.preview_fn(formatter)` enables partial-result previews in span attributes

---

## 7a) Cross-Scope Deletion Blockers (Beyond CLI + OBS)

**CORRECTION (from review §2.6):** Full Hamilton/rustworkx deletion requires addressing
coupling in modules beyond `src/cli` and `src/obs`. These are blocking dependencies that
must be resolved before Phase 4 (Hamilton Removal) can complete.

### 7a.1 Engine/Runtime Layer Hamilton Coupling

| File | Hamilton Coupling | Required Action |
|------|------------------|-----------------|
| `src/engine/telemetry/hamilton.py` | Hamilton-specific telemetry module | **DELETE** - replace with engine-native telemetry |
| `src/engine/runtime_profile.py:17,49` | Hamilton runtime profile references | **Migrate** `tracker_config`/`hamilton_telemetry` fields to engine-native config |
| `src/engine/facade.py:34-40` | Substring-matched error classification | **Replace** with stable Rust error codes (see §6.2) |

### 7a.2 DataFusion Engine Diagnostics Coupling

| File | Hamilton Coupling | Required Action |
|------|------------------|-----------------|
| `src/datafusion_engine/lineage/diagnostics.py:656,673` | Hamilton-specific diagnostics lineage recording | **Replace** with engine-native diagnostic events |

### 7a.3 Artifact Spec Registry Coupling

| File | Hamilton Coupling | Required Action |
|------|------------------|-----------------|
| `src/serde_artifact_specs.py:241,1022,1421` | Exports many Hamilton-named artifact specs | **Remove** Hamilton-named specs, add engine-named equivalents |

### 7a.4 rustworkx Residuals (from review §2.8)

| File | rustworkx Coupling | Required Action |
|------|-------------------|-----------------|
| `src/cli/commands/version.py:39` | CLI version output reports rustworkx dependency | **Remove** rustworkx from version dependency payload |
| `src/datafusion_engine/views/graph.py:1417` | Optional rustworkx topo-sort path | **Remove** and lock deterministic Kahn fallback as canonical behavior |

### 7a.5 Observability Artifact/Event Replacement

**From review §3.4 (adapted - no dual-emission needed since Hamilton never shipped):**

1. **Delete all `hamilton_*` prefixed event types** outright - no bridge needed
2. **Canonical namespace:** Introduce `codeanatomy.engine.*` event namespace for all
   DataFusion execution events
3. **Schema version field:** Add `schema_version: int` to all new diagnostic artifact
   payloads for forward compatibility

### 7a.6 Deletion Preflight Scanner

**From review §4:** Add an automated scan gate that fails on any residual
Hamilton/rustworkx runtime references in supported code paths:

```python
# scripts/check_hamilton_residuals.py
# Run as CI gate before Phase 7 (Full Legacy Deletion)
FORBIDDEN_PATTERNS = [
    r"from hamilton",
    r"import hamilton",
    r"from hamilton_pipeline",
    r"import hamilton_pipeline",
    r"rustworkx",
    r"OtelNodeHook",
    r"OtelPlanHook",
    r"HamiltonConfig",
]
```

Integrate into existing drift check infrastructure at `scripts/check_drift_surfaces.py`.

---

## 8) Implementation Phases

**Phasing aligned with best-in-class review recommendation (§5), adapted for clean-cut
(no backward compatibility, no transition scaffolding).** Path: contract freeze → extraction
decoupling → build rewrite → plan/obs/config migration → residual cleanup → deletion.

### Phase 0: Contract Freeze and Acceptance Definition (NEW)

**Goal:** Freeze output/diagnostic/API contracts to preserve during backend swap.

**Steps:**
1. Document the exact contract for `build_graph_product()` return shape
2. Snapshot current `FULL_PIPELINE_OUTPUTS` output key set
3. Define acceptance criteria for RunResult → GraphProductBuildResult mapping
4. Define schema contracts for all diagnostic artifact payloads
5. Define CLI JSON output field contracts

**Exit criteria:**
- Signed contract doc for build outputs, run manifest, diagnostics payloads, CLI JSON fields
- Contract tests written and passing against current Hamilton pipeline

### Phase 1: Extraction Decoupling (Foundation)

**Goal:** Extract extraction logic from Hamilton nodes into standalone callable functions.

**VALIDATED:** Extraction DAG has staged dependencies (not flat). All extractors are
plain Python functions producing Arrow tables - no Hamilton decorators, no Hamilton-specific
return types. Staged barrier synchronization is straightforward to implement.

**Steps:**
1. **Prerequisite: Extract portable types** from `hamilton_pipeline/types/`:
   - Move `ScipIndexConfig` → `src/extract/extractors/scip/config.py` (pure dataclass, trivially movable)
   - Move `ScipIdentityOverrides` → `src/extract/extractors/scip/config.py` (pure dataclass)
   - Update imports in `src/extract/extractors/scip/setup.py` and `src/cli/commands/build.py`
2. Create `src/extraction/orchestrator.py` - Direct extraction runner without Hamilton
3. Implement staged extraction execution model:
   - **Stage 0 (sequential):** `repo_scan` → produces `repo_files` (required by all extractors)
   - **Stage 1 (parallel via ThreadPoolExecutor):**
     - `ast_extract` → AST evidence (depends on `repo_files`)
     - `cst_extract` → LibCST evidence (depends on `repo_files`)
     - `symtable_extract` → symbol table evidence (depends on `repo_files`)
     - `bytecode_extract` → bytecode evidence (depends on `repo_files`)
     - `scip_extract` → SCIP evidence (depends on `repo_files`)
     - `treesitter_extract` → tree-sitter evidence (depends on `repo_files`)
   - **Stage 2 (sequential, after Stage 1 barrier):** `python_imports` → import synthesis
     (depends on `ast_imports`, `cst_imports`, `ts_imports` from Stage 1)
   - **Stage 3 (sequential):** `python_external` → external dependency evidence
     (depends on `python_imports`)
4. Write extraction outputs to Delta tables in `work_dir/extract/`
5. Return `dict[str, str]` mapping logical names to Delta locations
6. Collect extraction errors during execution for `write_extract_error_artifacts_delta`
   auxiliary output (see §6.5)

**Validation:** Extraction outputs match existing Hamilton pipeline outputs (schema + row counts).

### Phase 2: Build Command Rewrite (Core Transition)

**Goal:** Replace `build_command()` Hamilton path with direct Rust engine invocation.
Wire structured warnings and trace metrics into obs pipeline.

**Steps:**
1. Create `src/engine/build_orchestrator.py` - Top-level build coordinator:
   ```
   run_extraction() -> delta_locations
   build_semantic_ir(delta_locations) -> SemanticIR
   build_execution_spec(ir, delta_locations, ...) -> SemanticExecutionSpec
   execute_cpg_build(delta_locations, spec, profile) -> RunResult
   parse_build_result(run_result) -> GraphProductBuildResult
   ```
2. Modify `build_command()`:
   - Remove all Hamilton-specific CLI options (executor, graph adapter, tracker, plan overrides)
   - Add Rust engine CLI options (engine profile, rulepack profile, compliance, tracing)
   - Replace `GraphProductBuildRequest` construction with direct orchestrator call
3. Rewrite `GraphProductBuildRequest`:
   - Remove all Hamilton fields
   - Add engine fields
   - No compatibility shims - clean replacement (see §3.2.3)
4. Rewrite `build_graph_product()` implementation:
   - Direct orchestrator flow: extraction → spec build → Rust engine
   - Preserve OTel span structure (root_span, heartbeat, diagnostics)
   - Wire structured `RunWarning` payloads to obs pipeline (see §6.3)
5. Handle 3 auxiliary outputs not producible by Rust engine (see §6.5):
   - `write_normalize_outputs_delta` → Produce from Python semantic compiler side-effect
   - `write_extract_error_artifacts_delta` → Collect during extraction phase, write from orchestrator
   - `write_run_manifest_delta` → Construct from RunResult envelope + extraction metadata

**Exit criteria:**
- Engine produces correct CPG outputs (nodes, edges, props) validated against contract tests
- Structured warnings and trace metrics flowing to obs pipeline
- All auxiliary outputs producing correct content

### Phase 3: Plan Command Migration (Tiered - from review §5 Phase 3)

**Goal:** Implement Tier 1 parity at minimum. Extend to Tier 2/3 only when Rust
introspection APIs exist (see §3.2.2 for tier definitions).

**Steps:**
1. Modify `plan_command()`:
   - Replace `build_plan_context()` with `build_execution_spec()` + `SemanticPlanCompiler().compile()`
   - Map plan inspection features to compiled plan properties (Tier 1 only initially)
2. Update `PlanOptions`:
   - Remove `execution_mode` (Hamilton)
   - Add `--engine-profile`, `--rulepack-profile`
3. **Tier 1 implementation** (immediate):
   - `plan_signature` -> `compiled.spec_hash_hex()`
   - `task_count` -> `len(spec.view_definitions)`
   - High-level graph summary from spec field inspection
4. **Tier 2 implementation** (when Rust APIs available):
   - `graph` -> Derive DOT from view_definitions + join_graph.edges
   - `inferred_deps` -> Extract from view_definitions.view_dependencies
   - `schedule` -> View dependency topology
5. **Tier 3 implementation** (when Rust APIs available):
   - `validation` -> Compile-time validation envelopes
   - Full `PlanArtifactBundle` equivalent

**Exit criteria:**
- `plan` command works without Hamilton/relspec imports
- Explicitly documented parity tier (document which features are Tier 1 vs deferred)

### Phase 3.5: Observability Layer Transition

**Goal:** Remove all Hamilton and rustworkx dependencies from `src/obs/` and replace
with DataFusion-sourced equivalents (see §7 for detailed analysis).

**Steps:**

1. **Delete Hamilton hooks:**
   - Delete `src/obs/otel/hamilton.py` (`OtelNodeHook`, `OtelPlanHook`)
   - Remove Hamilton lifecycle hook registration from driver builder

2. **Add extraction phase OTel spans** in the new extraction orchestrator:
   - Per-extractor spans with `codeanatomy.stage = "extraction"` attribute
   - Call `record_stage_duration()` from orchestrator (replaces OtelNodeHook stage timing)
   - Call `record_error()` on extraction failures (replaces OtelNodeHook error recording)

3. **Add RunResult metrics bridge** - new function to extract Rust engine metrics:
   ```python
   def record_engine_metrics(run_result: dict[str, object]) -> None:
       """Extract metrics from RunResult and record to OTel instruments."""
       metrics = run_result.get("trace_metrics_summary")
       if metrics:
           record_task_duration("engine_execute", metrics["elapsed_compute_nanos"] / 1e9, status="ok")
       for output in run_result.get("outputs", []):
           record_write_duration(output["rows_written"] / 1e6, status="ok", destination=output["table_name"])
   ```

4. **Replace plan diagnostics artifacts:**
   - Replace `PLAN_SCHEDULE_SPEC` recording → `EnginePlanSummaryArtifact` from compiled spec
   - Replace `PLAN_VALIDATION_SPEC` recording → compile success/failure from Rust compiler
   - Replace `PLAN_SIGNALS_SPEC` recording → spec_hash + view_count from compiled spec

5. **Replace Hamilton graph snapshot:**
   - Source from `RunResult.compliance_capture_json` → `ComplianceCapture.config_snapshot`
   - Source session state from `SessionEnvelope` (available via compliance capture)

6. **Remove `record_cache_lineage()`** from `src/obs/diagnostics.py`
   - Delete function and `HAMILTON_CACHE_LINEAGE_SPEC` import
   - Cache behavior is now spec-static via Rust `CachePlacementPolicy`

7. **Update `plan_execution_diff` in diagnostics report:**
   - Replace Hamilton expected/executed task counts with spec view_definitions vs RunResult outputs

8. **Repoint `record_task_duration()`** callers:
   - Old: Called by `OtelNodeHook` with Hamilton `task_kind` tag
   - New: Called by RunResult metrics bridge with operator names from `OperatorMetricSummary`

9. **Update `record_stage_duration()`** callers:
   - Old: Called by `OtelNodeHook` with Hamilton `layer` tag
   - New: Called by Python orchestrator (extraction, spec_build phases) + Rust tracing spans

10. **Optional Rust engine tracing enhancements** (see §7.8):
    - Add Delta write spans to `delta_writer.rs` for native write_duration sourcing
    - Add cache decision events to `cache_boundaries.rs`
    - Add input registration spans to `SessionFactory`

**Exit criteria:**
- Diagnostics report and OTEL metrics fully populated from Rust/DataFusion runtime data
- OTel spans cover all execution phases (extraction → spec build → engine execution → materialization)
- All Hamilton event types removed and replaced with engine-native events

### Phase 4: Config Model + Runtime Profile Migration (NEW - from review §5 Phase 5)

**Goal:** Replace all Hamilton config types and sections with engine-native equivalents.
No backward compatibility - clean replacement.

**Steps:**
1. Add `[engine]` config section to `codeanatomy.toml` schema
2. Update `src/cli/config_models.py`:
   - Delete `HamiltonConfigSpec` type entirely
   - Add `EngineConfigSpec` type with engine profile, rulepack, tracing fields
3. Update `src/runtime_models/root.py`:
   - Delete `HamiltonConfigRuntime` and all Hamilton-specific fields
   - Add engine config runtime model
4. Update `src/cli/commands/config.py:71`:
   - Remove `[hamilton]` from config template
   - Add `[engine]` section template
5. Update `src/cli/config_loader.py:193-194`:
   - Delete `hamilton.tags` validation entirely
   - Add `[engine]` section validation
6. Update `src/engine/runtime_profile.py`:
   - Delete `tracker_config`/`hamilton_telemetry` fields and env controls
   - Add engine-native profile configuration

**Exit criteria:**
- Zero Hamilton references in config models, loader, or templates
- `[engine]` config section fully functional
- Old `[hamilton]` section in any config file causes a clear parse error (not silently ignored)

### Phase 5: Remove rustworkx/Hamilton Residuals (from review §5 Phase 6)

**Goal:** Remove remaining rustworkx optional path and dependency references.
Remove Hamilton-specific telemetry/profile/config artifacts from adjacent modules.

**Steps:**
1. Remove rustworkx from CLI version dependency payload (`version.py:39`)
2. Remove optional rustworkx topo-sort path (`views/graph.py:1417`); lock deterministic
   Kahn fallback as canonical behavior
3. Remove `src/engine/telemetry/hamilton.py`
4. Clean `src/engine/runtime_profile.py` Hamilton references
5. Clean `src/datafusion_engine/lineage/diagnostics.py:656,673` Hamilton diagnostics
6. Remove Hamilton-named specs from `src/serde_artifact_specs.py:241,1022,1421`
7. Add engine-named artifact spec equivalents

**Exit criteria:**
- Static scan finds zero Hamilton/rustworkx runtime references in supported paths
- `scripts/check_drift_surfaces.py` extended to check for Hamilton/rustworkx residuals

### Phase 6: Full Hamilton Deletion (formerly Phase 4)

**Goal:** Remove Hamilton as a dependency entirely. Delete `src/relspec`,
`src/hamilton_pipeline`, and legacy graph execution path once all gates pass.

**VALIDATED: Actual scope is 33 files in hamilton_pipeline/ (not 19 as originally estimated),
plus 6 additional files outside hamilton_pipeline/ with Hamilton coupling. Test blast radius:
29 test files with Hamilton references + 16 test files with rustworkx references.**

**Steps:**

1. Delete `src/hamilton_pipeline/` directory (**33 files**, including subdirectories):
   - Root: `__init__.py`, `execution.py`, `execution_manager.py`,
     `driver_builder.py`, `driver_factory.py`, `cache_lineage.py`,
     `cache_versioning.py`, `graph_snapshot.py`, `hamilton_tracker.py`,
     `io_contracts.py`, `materializers.py`, `plan_artifacts.py`,
     `scheduling_hooks.py`, `structured_logs.py`, `tag_policy.py`,
     `task_module_builder.py`, `type_checking.py`, `validators.py`, `lifecycle.py`
   - `types/` subdirectory: Type definitions including `ScipIndexConfig`,
     `ScipIdentityOverrides`, `HamiltonConfigSpec`, `HamiltonConfigRuntime`
   - `modules/` subdirectory: Hamilton module definitions for extraction, semantic,
     and CPG build nodes

2. Remove `hamilton` from `pyproject.toml` dependencies

3. **Handle files outside hamilton_pipeline/ with Hamilton coupling:**
   - `src/obs/otel/hamilton.py` - Contains `OtelNodeHook` and `OtelPlanHook`
     (Hamilton lifecycle hooks). DELETE entirely; OTel tracing moves to the Rust
     engine's tracing subsystem.
   - `src/semantics/incremental/config.py:166` - Lazy import of
     `IncrementalRunConfig` from `hamilton_pipeline.types`. Replace with standalone
     type or inline.
   - `src/extract/extractors/scip/setup.py` - TYPE_CHECKING import of
     `ScipIndexConfig` from `hamilton_pipeline.types`. Move `ScipIndexConfig` to
     standalone module first (Phase 1 prerequisite).
   - `src/runtime_models/root.py` - Hamilton references in runtime model. Remove
     Hamilton-specific fields.
   - `src/graph/product_build.py` - Heavy Hamilton imports (6+ types). Rewrite
     in Phase 2.
   - `src/cli/commands/build.py` and `src/cli/commands/plan.py` - Hamilton imports.
     Rewrite in Phases 2-3.

4. **Migrate portable types before deletion:**
   - `ScipIndexConfig` → `src/extract/extractors/scip/config.py` (pure dataclass)
   - `ScipIdentityOverrides` → `src/extract/extractors/scip/config.py` (pure dataclass)
   - `HamiltonConfigSpec` / `HamiltonConfigRuntime` → DELETE (no replacement needed)

5. **Test file cleanup (24 files):**
   - 24 test files across `tests/` import Hamilton types or fixtures
   - Tests in `tests/unit/hamilton_pipeline/` → DELETE entirely
   - Tests referencing Hamilton execution modes → Migrate to engine-based equivalents
   - Tests using Hamilton fixtures → Replace with engine-native fixtures

6. Remove Hamilton-related type stubs and update `pyproject.toml` test markers

7. Clean up any remaining Hamilton imports across the codebase (full `rg hamilton` sweep)

**Exit criteria:**
- CI green with legacy modules deleted
- `rg -l hamilton src/` returns zero results (excluding comments/docs)
- Documentation and release notes complete

### Phase 7: Test Adaptation (formerly Phase 5)

**Goal:** Ensure test coverage for the new execution path.

**VALIDATED: 29 test files reference Hamilton + 16 test files reference rustworkx (some overlap).**

**Steps:**
1. Add integration tests for:
   - Extraction orchestrator (Delta output correctness)
   - Extraction error artifact collection
   - Spec building from extraction outputs
   - End-to-end CLI build via Rust engine
   - Plan command via Rust engine compiler
   - Auxiliary output generation (normalize, errors, manifest)
   - Determinism tier validation wrapper
2. Migrate relevant Hamilton pipeline tests to new orchestrator tests
   - Tests in `tests/unit/hamilton_pipeline/` → DELETE or migrate assertions
   - Tests referencing `ExecutionMode` → Replace with engine profile equivalents
   - Tests using Hamilton fixtures → Replace with engine-native fixtures
3. Add parity tests: compare Rust engine output to known-good Hamilton output snapshots
4. Update CLI golden tests for new option surface (28 options removed, 7 added)
5. Add extraction orchestrator tests:
   - Parallel execution correctness (7 independent extractors)
   - Sequential chain correctness (imports → external)
   - Error collection and propagation
   - Delta output schema contracts

---

## 8a) Non-Functional Release Gates (Best-in-Class - from review §6)

**Required before considering implementation complete:**

| Gate | Requirement | Validation Method |
|------|-------------|-------------------|
| **Determinism** | Stable spec/envelope hash behavior for fixed inputs | Hash comparison across multiple runs of golden repos |
| **Correctness** | Output schema/row-count correctness | Contract tests against spec-defined outputs |
| **Performance** | Cold/warm run budgets defined and met | Benchmark suite |
| **Observability** | Metrics, warnings, and diagnostics completeness validated | OTel span coverage audit + diagnostics report completeness check |

**Correctness criteria:**
- Output schemas match contract definitions (column names, types, nullability)
- Determinism hash stability: identical outputs for identical inputs
- All contract tests passing

---

## 9) CLI Option Surface (Before/After)

### 9.1 Options Removed

| Current Option | Group | Reason |
|---------------|-------|--------|
| `--executor-kind` | execution | Hamilton executor backend |
| `--executor-max-tasks` | execution | Hamilton parallelism |
| `--executor-remote-kind` | execution | Hamilton remote executor |
| `--executor-remote-max-tasks` | execution | Hamilton remote parallelism |
| `--executor-cost-threshold` | execution | Hamilton cost routing |
| `--graph-adapter-kind` | graph-adapter | Hamilton graph adapter |
| `--graph-adapter-option` | graph-adapter | Hamilton adapter options |
| `--graph-adapter-option-json` | graph-adapter | Hamilton adapter options (JSON) |
| `--enable-hamilton-tracker` | advanced | Hamilton UI integration |
| `--hamilton-project-id` | advanced | Hamilton UI |
| `--hamilton-username` | advanced | Hamilton UI |
| `--hamilton-dag-name` | advanced | Hamilton UI |
| `--hamilton-api-url` | advanced | Hamilton UI |
| `--hamilton-ui-url` | advanced | Hamilton UI |
| `--hamilton-capture-data-statistics` | advanced | Hamilton UI |
| `--hamilton-max-list-length-capture` | advanced | Hamilton UI |
| `--hamilton-max-dict-length-capture` | advanced | Hamilton UI |
| `--plan-allow-partial` | advanced | Hamilton plan compilation |
| `--plan-requested-task` | advanced | Hamilton plan filtering |
| `--plan-impacted-task` | advanced | Hamilton plan filtering |
| `--enable-metric-scheduling` | advanced | Hamilton scheduling |
| `--enable-plan-diagnostics` | advanced | Hamilton plan diagnostics |
| `--enable-plan-task-submission-hook` | advanced | Hamilton hooks |
| `--enable-plan-task-grouping-hook` | advanced | Hamilton hooks |
| `--enforce-plan-task-submission` | advanced | Hamilton hooks |
| `--execution-mode` | execution | Hamilton execution mode |
| `--writer-strategy` | execution | Replaced by engine-internal Delta writes |

**Total removed: ~28 CLI options**

### 9.2 Options Added

| New Option | Group | Type | Default | Description |
|-----------|-------|------|---------|-------------|
| `--engine-profile` | execution | small/medium/large | medium | DataFusion session resource class |
| `--rulepack-profile` | execution | Default/LowLatency/Replay/Strict | Default | Semantic rule enforcement level |
| `--enable-compliance` | advanced | bool | False | Enable compliance capture in engine |
| `--enable-rule-tracing` | advanced | bool | False | Enable rule-level tracing |
| `--enable-plan-preview` | advanced | bool | False | Enable partial-result previews in tracing spans (sets tracing preview_limit) |
| `--tracing-preset` | observability | Maximal/MaximalNoData/ProductionLean | None | Engine tracing detail level |
| `--instrument-object-store` | observability | bool | False | Trace Delta/object store operations |

**Total added: ~7 CLI options**

**Net reduction: ~21 options** (significant UX simplification).

### 9.3 Options Unchanged

| Option | Group | Notes |
|--------|-------|-------|
| `--output-dir` / `-o` | output | Same semantics |
| `--work-dir` | output | Same semantics |
| `--include-errors` | output | Same semantics |
| `--include-manifest` | output | Same semantics |
| `--include-run-bundle` | output | Same semantics |
| `--determinism-tier` | execution | Python-side validation wrapper around engine 2-hash model (see §6.6) |
| `--runtime-profile` | execution | Maps to engine session profile |
| All SCIP options | scip | Extraction remains Python |
| All incremental options | incremental | Extraction remains Python |
| All repo scope options | repo-scope | Extraction remains Python |
| `--enable-tree-sitter` | advanced | Extraction remains Python |

---

## 10) Configuration Model Changes

**CORRECTION (from review §2.5):** Config migration scope is broader than initially stated.
Hamilton-specific config surface exists in multiple locations beyond `codeanatomy.toml`:

| File | Hamilton Config Surface | Required Action |
|------|------------------------|-----------------|
| `src/cli/config_models.py:177,214` | `HamiltonConfigSpec` type in msgspec models | Remove type, add engine config type |
| `src/runtime_models/root.py:138,183` | `HamiltonConfigRuntime` in runtime model | Remove Hamilton-specific fields |
| `src/cli/commands/config.py:71` | Config template advertises `[hamilton]` section | Remove `[hamilton]` from template |
| `src/cli/config_loader.py:193-194` | Validates `hamilton.tags` config keys | Remove Hamilton validation, add engine validation |

**Config migration approach:** Delete all Hamilton config types and sections. No backward
compatibility - the `[hamilton]` section was never shipped to users.

### 10.1 `codeanatomy.toml` Sections

| Section | Status | Notes |
|---------|--------|-------|
| `[hamilton]` | **Delete** | Remove entirely - never shipped |
| `[plan]` | **Delete** | Hamilton plan overrides - never shipped |
| `[execution]` | **Modify** | Remove executor/adapter, add engine profile/rulepack |
| `[scip]` | Keep | Extraction config |
| `[incremental]` | Keep | Extraction config |
| `[otel]` | Keep | OTel config (also fed to engine TracingConfig) |
| `[delta]` | Keep | Delta maintenance config |
| `[engine]` | **Add** | Rust engine profile, rulepack, compliance, tracing |

### 10.2 New `[engine]` Config Section

```toml
[engine]
profile = "medium"          # small | medium | large
rulepack_profile = "default"  # Default | LowLatency | Replay | Strict
compliance_capture = false
rule_tracing = false
plan_preview = false          # Enables partial-result previews in tracing spans (preview_limit)
tracing_preset = "ProductionLean"  # Maximal | MaximalNoData | ProductionLean
instrument_object_store = false

# Determinism validation (Python-side wrapper around engine 2-hash model)
# determinism_tier = "reproducible"  # canonical | reproducible | best_effort
```

---

## 11) Import Dependency Cleanup

### 11.1 Modules That Lose Hamilton Imports

**VALIDATED: 131 total Hamilton imports across src/; all coupling points enumerated below.**

| File | Current Hamilton Imports | Replacement |
|------|------------------------|-------------|
| `cli/commands/build.py` | `ExecutionMode`, `ExecutorConfig`, `GraphAdapterConfig`, `ScipIndexConfig`, `ScipIdentityOverrides` | Engine-native types; SCIP config to standalone module |
| `cli/commands/plan.py` | `ExecutionMode`, `PlanArtifactBundle`, `DriverBuildRequest`, `build_plan_context` | `SemanticPlanCompiler`, `CompiledPlan`, `build_execution_spec` |
| `graph/product_build.py` | `PipelineExecutionOptions`, `execute_pipeline`, `ExecutionMode`, `ExecutorConfig`, `GraphAdapterConfig`, `ScipIndexConfig`, `ScipIdentityOverrides` | `execute_cpg_build`, `build_execution_spec`, engine-native types |
| `obs/otel/hamilton.py` | `OtelNodeHook`, `OtelPlanHook` (entire module) | DELETE; extraction OTel spans in orchestrator, engine tracing for downstream |
| `semantics/incremental/config.py` | Lazy import of `IncrementalRunConfig` (line 166) | Move type to standalone module or inline conversion |
| `extract/extractors/scip/setup.py` | TYPE_CHECKING: `ScipIndexConfig` | Import from `extract/extractors/scip/config.py` after type migration |
| `cli/config_models.py` | `HamiltonConfigSpec` (19 fields), `GraphAdapterConfig` | Remove `HamiltonConfigSpec` entirely; remove `GraphAdapterConfig` |
| `runtime_models/root.py` | `HamiltonConfigRuntime` (Pydantic-validated), `GraphAdapterConfig` | Remove Hamilton-specific fields; remove `GraphAdapterConfig` |
| `tests/conftest.py` | Soft-fail Hamilton import for diagnostics, `HAMILTON_TELEMETRY_ENABLED` env var | Remove Hamilton import and env var setup |
| `engine/telemetry/hamilton.py` | Hamilton-specific telemetry module | DELETE entire module |
| `engine/runtime_profile.py` | Hamilton runtime profile references (lines 17, 49) | Remove `tracker_config`/`hamilton_telemetry` fields |
| `engine/facade.py` | Substring-matched error classification (lines 34-40) | Replace with stable Rust error codes (§6.2) |
| `datafusion_engine/lineage/diagnostics.py` | Hamilton diagnostics lineage (lines 656, 673) | Replace with engine-native diagnostic events |
| `serde_artifact_specs.py` | Hamilton-named artifact specs (lines 241, 1022, 1421) | Remove Hamilton-named specs, add engine equivalents |

### 11.2 Type Migrations

| Hamilton Type | Replacement | Notes |
|--------------|-------------|-------|
| `ExecutionMode` | `str` literal (`"small"`, `"medium"`, `"large"`) | Maps to SessionFactory class |
| `ExecutorConfig` | Removed | Engine manages internally |
| `GraphAdapterConfig` | Removed | Engine manages internally |
| `ScipIndexConfig` | Standalone dataclass | Decouple from Hamilton types module |
| `ScipIdentityOverrides` | Standalone dataclass | Decouple from Hamilton types module |
| `PipelineExecutionOptions` | `EngineExecutionOptions` (new) | Lean options for engine |
| `PlanArtifactBundle` | `CompiledPlanSummary` (new) | Wraps CompiledPlan with display metadata |

---

## 12) Risk Assessment

### 12.1 High Risk

| Risk | Impact | Mitigation |
|------|--------|------------|
| Extraction output schema drift | Rust engine rejects inputs | Schema contract tests between extraction Delta outputs and SemanticExecutionSpec input_relations |
| 3 auxiliary outputs not Rust-producible | Missing normalize_outputs, extract_errors, manifest | Python-side handling in orchestrator (see §6.5) |
| Determinism tier semantic mismatch | Users expect tier guarantees engine can't provide | Python-side validation wrapper around 2-hash model (see §6.6) |
| Deletion scope wider than `cli` + `obs` | Hidden Hamilton coupling blocks deletion | Cross-scope deletion workstream (see §7a) + preflight scanner (see §7a.6) |
| Plan command parity gap | `plan` features require Rust APIs that don't exist yet | Tiered parity approach - only promise Tier 1 until APIs exist (see §3.2.2) |

### 12.2 Medium Risk

| Risk | Impact | Mitigation |
|------|--------|------------|
| OTel span structure changes | Observability gap | Map existing span names to new execution phases; Rust engine has tracing presets |
| Error classification brittle | Wrong error categories for CLI exit codes | Migrate to Rust error codes (§6.2) |
| SCIP type decoupling | Import breakage | Extract SCIP types to standalone module FIRST in Phase 1 |
| 24 test files need migration | Test coverage gap during transition | Migrate in Phase 7; maintain parity test suite |
| Run manifest reimplementation | Manifest format regression | Define manifest contract from RunResult envelope + extraction metadata |
| Warning payload shape mismatch | Incorrect warning display in CLI | Map `Vec<RunWarning>` structured payloads correctly (§6.3) |

### 12.3 Low Risk (VALIDATED)

| Risk | Impact | Mitigation |
|------|--------|------------|
| Extraction parallelism bugs | LOW - DAG is staged but simple | 4-stage barrier model (repo_scan→parallel extractors→imports→external); well-understood |
| Semantic compiler Hamilton deps | NONE - confirmed zero deps | `build_cpg()`, `SemanticCompiler`, IR pipeline all Hamilton-free |
| Observability data loss | LOW - 10/14 metrics already generic | Only 4 metrics need repointing; 13/14 report sections data-source-agnostic |
| Diagnostics report regression | LOW - only plan_execution_diff coupled | 1/14 sections needs adaptation; rest works with any span source |
| Delta command changes | None | Already DataFusion-native |
| Diag/version/config changes | None | Execution-agnostic |
| CLI group changes | Minor UX | Groups defined in `cli/groups.py`; just remove unused ones |

---

## 13) Testing Strategy

### 13.1 Parity Testing

Before removing Hamilton, add parity tests that:
1. Run the same repo through both Hamilton pipeline and Rust engine
2. Compare CPG output tables (nodes, edges, props) column-by-column
3. Verify row counts match
4. Verify determinism hashes match (when determinism tier is canonical)

### 13.2 New Test Categories

| Test | Location | Validates |
|------|----------|-----------|
| Extraction orchestrator unit | `tests/unit/test_extraction_orchestrator.py` | Each extractor produces correct-schema Delta output |
| Spec building integration | `tests/integration/test_spec_from_extraction.py` | Extraction outputs -> valid SemanticExecutionSpec |
| Engine facade integration | `tests/integration/test_engine_facade.py` | Spec -> Rust engine -> valid RunResult |
| CLI build e2e | `tests/e2e/test_cli_build_engine.py` | Full CLI -> extraction -> engine -> Delta outputs |
| CLI plan e2e | `tests/e2e/test_cli_plan_engine.py` | Full CLI -> plan compilation -> display |
| CLI golden updates | `tests/cli_golden/` | Updated snapshots for new option surface |
| RunResult metrics bridge | `tests/unit/test_engine_metrics_bridge.py` | RunResult → OTel metrics extraction |
| Engine plan summary artifact | `tests/unit/test_engine_plan_summary.py` | Compiled spec → EnginePlanSummaryArtifact |
| Diagnostics report DataFusion | `tests/unit/test_diagnostics_report_engine.py` | Report generation from Rust engine spans |
| OTel span coverage | `tests/integration/test_otel_engine_spans.py` | All execution phases emit proper OTel spans |

---

## 14) Dependency Impact

### 14.1 Dependencies Removed

| Package | Current Use | Notes |
|---------|------------|-------|
| `sf-hamilton` | Pipeline orchestration | Core removal target |
| Hamilton graph adapters | Parallel execution backends | Replaced by engine-internal DataFusion parallelism |
| Hamilton materializers | Output writing | Replaced by engine Delta writes (6/9 outputs) + Python orchestrator (3/9 outputs) |
| Hamilton cache system | `cache_lineage.py`, `cache_versioning.py` | NOT ported; Rust engine has independent `CachePlacementPolicy` |

### 14.2 Dependencies Retained

| Package | Use | Notes |
|---------|-----|-------|
| `datafusion` | Python DataFusion (extraction-side queries) | Still needed for extraction-phase DataFusion use |
| `deltalake` | Delta table reads/writes (extraction phase) | Extraction outputs to Delta |
| `pyarrow` | Arrow tables (extraction phase) | Extraction evidence tables |
| `msgspec` | Spec serialization (Python <-> Rust boundary) | Core contract format |
| `codeanatomy_engine` | Rust engine PyO3 extension | Core execution engine |
| `rustworkx` | Graph operations (semantic/relspec) | Still needed if semantic compiler uses it |

---

## 15) Migration Path for Users

### 15.1 Config Migration

```
# Old (Hamilton-era)
codeanatomy build ./repo --execution-mode plan_parallel --executor-kind threadpool --executor-max-tasks 4

# New (DataFusion-era)
codeanatomy build ./repo --engine-profile medium
```

### 15.2 Migration Approach

**Clean cut:** Hamilton/rustworkx has not shipped to production. No deprecation period,
no compatibility shims. The old CLI options and config sections are deleted outright and
replaced with engine-native equivalents in a single pass.

---

## 16) Open Questions (All Resolved)

All original open questions have been resolved through targeted code validation:

1. **Extraction parallelism model: RESOLVED → Option A (staged ThreadPoolExecutor)**
   Extraction DAG has staged dependencies (corrected from "flat" per review §2.2):
   Stage 0 (`repo_files`) → Stage 1 (6 parallel extractors) → Stage 2 (`python_imports`
   depends on AST/CST/tree-sitter outputs) → Stage 3 (`python_external` depends on imports).
   All extractors are plain Python functions (no Hamilton decorators) producing standard
   Arrow types. Staged ThreadPoolExecutor is sufficient; Hamilton is unnecessary for
   dependency management.

2. **Semantic compiler standalone execution: RESOLVED → YES, fully standalone**
   `build_cpg()`, `SemanticCompiler`, and the entire IR pipeline have ZERO Hamilton
   dependencies. Not a single Hamilton import exists in any semantics module. The only
   coupling is a single lazy optional import in `semantics/incremental/config.py:166`
   which is trivially removable.

3. **Incremental state management: RESOLVED → Config object is portable**
   `SemanticIncrementalConfig` is a pure dataclass with no Hamilton dependencies in the
   class itself. Only the `to_run_snapshot()` method at line 166 has a lazy Hamilton
   import. The config can be passed directly to the extraction orchestrator. The runtime
   incremental logic (change detection, git refs) needs reimplementation in the
   orchestrator, but the config/state model is fully portable.

4. **Run bundle / manifest: RESOLVED → Separate concerns**
   - **Run bundles** are simple container directories and CAN be separated from Hamilton.
     They are just output directory structures with no Hamilton-specific logic.
   - **Run manifest** IS Hamilton-coupled - it's a full datasaver output node with Delta
     writes. Must be reimplemented as a Python-side post-processing step that constructs
     the manifest from the Rust engine's `RunResult` envelope combined with extraction
     metadata (see §6.5).

5. **Cache lineage: RESOLVED → Independent systems, no migration needed**
   `cache_lineage.py` and `cache_versioning.py` are 100% Hamilton-specific caching
   mechanisms. They are NOT general-purpose and should NOT be ported. The Rust engine
   has its own independent caching via `CachePlacementPolicy` which is determined at
   compile time from the spec. Extraction-level caching (if desired) should be
   implemented as a new, simple mechanism in the extraction orchestrator, completely
   independent of Hamilton's cache system.

### 16.1 New Questions (Arising from Validation + Review)

6. **OtelNodeHook / OtelPlanHook migration: RESOLVED**
   Add lightweight OTel spans in the extraction orchestrator (per-extractor spans) to
   maintain observability parity. Rust engine tracing covers downstream phases. See §7.

7. **HamiltonConfigSpec / HamiltonConfigRuntime replacement: RESOLVED**
   Full config migration scope documented in §10 and Phase 4. All 4 Hamilton config
   touchpoints identified (`config_models.py`, `runtime_models/root.py`, `config.py`
   template, `config_loader.py` validation).

8. **Rust engine compilation issues:** Pre-existing type mismatch in
   `rust/codeanatomy_engine/src/executor/pipeline.rs:202` (`Vec<RunWarning>` vs
   `Vec<String>`) and `result.rs:169`. These should be fixed before Phase 3.5
   observability work, as they affect the `RunResult` envelope that the metrics bridge
   depends on.

9. **Rust plan introspection API scope:** Plan command Tier 2/3 parity requires
   PyO3-exported plan summary/schedule/validation metadata from the Rust engine
   (see §3.2.2). This is new Rust work not yet scoped. **Recommendation:** Scope
   this during Phase 3; implement only if Tier 2+ is deemed necessary for initial release.

10. **Error code taxonomy Rust-side implementation:** Replacing substring-matched error
    classification (§6.2) requires Rust-side changes to add `error_code` and `error_stage`
    fields. **Recommendation:** Implement before Phase 2 (Rust Backend Parity) so the
    gateway can correctly classify errors from both backends.

### 16.2 Additional Scope Items (from review §4, adapted for clean-cut)

| Item | Why Required | Deliverables | Phase |
|------|-------------|-------------|-------|
| Rust plan introspection API | `plan` parity without Hamilton/relspec | PyO3-exported plan summary/schedule/validation metadata | Phase 3 |
| Engine error code taxonomy | Eliminate brittle message parsing | Rust enum + Python exception mapping + tests | Phase 2 |
| Deletion preflight scanner | Ensure no residual imports | Automated scan gate failing on Hamilton/rustworkx references | Phase 5 |
| Runtime profile cleanup | Remove Hamilton knobs in profile path | Delete `tracker_config`/`hamilton_telemetry` fields and env controls | Phase 4 |
| Version/dependency surface cleanup | Avoid stale dependency messaging | Remove rustworkx from CLI version dependency payload | Phase 5 |
| Docs and SDK contract update | Keep user-facing API accurate | Architecture docs + release notes | Phase 6 |

---

## 17) Summary of Work Estimates (Updated)

| Phase | Scope | Files Modified | Files Created | Files Deleted |
|-------|-------|---------------|---------------|---------------|
| Phase 0: Contract Freeze | Small | 0 | ~2 | 0 |
| Phase 1: Extraction Decoupling | Medium | ~7 | ~3 | 0 |
| Phase 2: Build Command Rewrite | Large | ~5 | ~3 | 0 |
| Phase 3: Plan Command Migration | Small | ~1 | 0 | 0 |
| Phase 3.5: Observability Transition | **Medium** | ~6 | ~2 | ~4 |
| Phase 4: Config + Runtime Migration | Medium | ~6 | ~1 | 0 |
| Phase 5: rustworkx/Hamilton Residuals | Medium | ~8 | ~1 | ~3 |
| Phase 6: Full Hamilton Deletion | **Large** | ~8 | 0 | **~39** |
| Phase 7: Test Adaptation | **Large** | ~14 | ~10 | **~24** |
| **Total** | | **~55** | **~22** | **~70** |

The net result is a **significant reduction** in codebase complexity:
- **~33 Hamilton pipeline files deleted** (includes types/, modules/ subdirs)
- ~6 additional Hamilton-coupled files outside hamilton_pipeline/ cleaned up
- ~4 Hamilton/rustworkx observability files deleted from obs/
- ~3 additional cross-scope files cleaned up (engine/telemetry, artifact specs, lineage)
- ~24 test files migrated or deleted
- ~28 CLI options removed, ~7 added
- 3 auxiliary outputs reimplemented as Python-side orchestrator outputs
- 4 metrics instruments repointed from Hamilton hooks to Rust engine data
- 8 Hamilton event types deleted, replaced by Rust compliance capture + execution spans
- 3 rustworkx plan artifacts replaced by engine plan summary artifacts
- Diagnostics report retains 13/14 sections unchanged (data-source-agnostic)
- Stable Rust error code taxonomy replaces brittle substring matching
- `GraphProductBuildRequest` + `build_graph_product` rewritten with engine-native types (no shims)
- All Hamilton config types/sections deleted outright (no compatibility layer)
- Direct, traceable execution path from CLI -> extraction -> spec -> Rust engine
- Single execution backend (DataFusion) instead of Hamilton + DataFusion hybrid
- End-to-end OTel observability sourced from Rust `datafusion-tracing` extension
- **No transition scaffolding:** no dual-emission, no rollback switches, no shadow mode, no deprecation warnings

### 17.1 Validation Confidence Assessment

| Aspect | Confidence | Evidence |
|--------|-----------|---------|
| Extraction decoupling feasibility | **HIGH** | All 9 extractors verified as plain functions; staged barrier model documented |
| Semantic compiler independence | **HIGH** | Zero Hamilton imports in semantics/ verified |
| Hamilton removal scope | **HIGH** | 131 total Hamilton imports audited; all coupling points enumerated; cross-scope deletion blockers identified |
| Rust engine output coverage | **HIGH** | Outputs confirmed against actual `FULL_PIPELINE_OUTPUTS`; 3 gaps documented with mitigations |
| Observability transition feasibility | **HIGH** | 10/14 metrics generic; 13/14 report sections data-source-agnostic; Rust engine TraceMetricsSummary provides equivalent data |
| Determinism model compatibility | **MEDIUM** | 2-hash model verified but tier mapping needs design validation |
| Incremental processing portability | **MEDIUM** | Config portable; runtime behavior needs reimplementation |
| DataFusion best-in-class patterns | **HIGH** | Tracing presets, cache knobs, TableProvider patterns, InstrumentationOptions all validated against dfdl_ref |
| Public API rewrite | **HIGH** | `GraphProductBuildRequest` + `build_graph_product` fields documented; clean replacement scoped |
| Config migration scope | **HIGH** | All 4 Hamilton config touchpoints identified; clean deletion scoped |
| Error taxonomy migration | **MEDIUM** | Substring matching identified as brittle; Rust error code approach designed but requires Rust-side implementation |
| Plan command parity | **MEDIUM** | Tier 1 achievable immediately; Tier 2/3 require Rust API work not yet scoped |
| Deletion preflight scanner | **HIGH** | Can extend existing `check_drift_surfaces.py` infrastructure |
