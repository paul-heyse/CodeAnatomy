# CLI-to-DataFusion Execution Transition Plan (v1)

**Date:** 2026-02-09
**Revision:** v2 (validated)
**Status:** Validated via targeted code review; ready for implementation scoping
**Prerequisite:** DataFusion Planning Best-in-Class Architecture Plan v1 (substantially implemented)
**Scope:** Complete transition of CLI execution paths from Hamilton DAG orchestration to Rust-native DataFusion engine

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
- Extraction DAG is very flat (7/9 extractors independent); Option A (ThreadPoolExecutor) validated
- Semantic compiler has ZERO Hamilton dependencies; standalone execution confirmed
- hamilton_pipeline/ contains 33 files (not 19); removal scope corrected
- 3 of 9 current pipeline outputs are NOT Rust-producible and need Python-side handling
- Rust engine has no determinism "tier" concept (2-hash model only); facade needs RuntimeConfig override support
- 24 test files import Hamilton and need migration

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
| Config loader | `cli/config_loader.py` | TOML/pyproject.toml resolution - no execution coupling |
| Config command | `cli/commands/config.py` | Config display - pure read-only |
| Delta commands | `cli/commands/delta.py` | Already DataFusion-native (uses DataFusionRuntimeProfile, delta_provider_from_session) |
| Diag command | `cli/commands/diag.py` | System diagnostics - execution-agnostic |
| Version command | `cli/commands/version.py` | Version display - pure read-only |
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
   - `--determinism-tier` becomes a Python-side validation wrapper (see §6.5);
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
`ExecutionPlan` and `PlanArtifactBundle` using Hamilton internals.

**Target state:** Uses Rust engine's `SemanticPlanCompiler` to produce a compiled plan,
then inspects its properties.

**Changes required:**

1. **Replace Hamilton plan compilation** with Rust engine plan compilation:
   - Current: `build_plan_context()` -> `DriverBuildRequest` -> Hamilton driver -> `ExecutionPlan`
   - Target: `build_execution_spec()` -> `SemanticPlanCompiler().compile(spec_json)` -> `CompiledPlan`

2. **Map plan inspection features** to Rust plan artifacts:
   - `show_graph` -> Derive from compiled plan's view definitions + join graph edges
   - `show_schedule` -> Not directly applicable (Rust engine schedules internally); could display
     execution spec's view dependency topology instead
   - `show_inferred_deps` -> Extract from compiled plan's view_dependencies fields
   - `show_task_graph` -> Derive from view definitions DAG
   - `validate` -> Compile-time validation (the Rust compiler validates on `compile()`)

3. **Remove `ExecutionMode` dependency:**
   - Plan command currently takes `--execution-mode` from Hamilton
   - Replace with `--engine-profile` and `--rulepack-profile` (only affect plan shape, not
     plan inspection)

4. **Preserve output semantics:**
   - `plan_signature` -> `CompiledPlan.spec_hash_hex()`
   - `task_count` -> Count of view definitions in spec
   - `output_format` (text/json/dot) -> Adapt formatters to new data structure

#### 3.2.3 GraphProductBuildRequest (`src/graph/product_build.py`, 670 lines)

**Current state:** Fat request object containing Hamilton-specific fields
(`execution_mode`, `executor_config`, `graph_adapter_config`, `use_materialize`).
Calls `execute_pipeline()` from Hamilton.

**Target state:** Lean request object for two-phase execution.

**Changes required:**

1. **Remove Hamilton-specific fields** from `GraphProductBuildRequest`:
   - `execution_mode: ExecutionMode` -> Replace with `engine_profile: str`
   - `executor_config: ExecutorConfig | None` -> Remove
   - `graph_adapter_config: GraphAdapterConfig | None` -> Remove
   - `use_materialize: bool` -> Remove (Rust engine always materializes)

2. **Add Rust engine fields:**
   - `engine_profile: str = "medium"` (SessionFactory class)
   - `rulepack_profile: str = "default"` (rule profile)
   - `runtime_config: RuntimeConfig | None = None` (engine/spec_builder.py)

3. **Replace `_execute_build()`** (line 282):
   - Current: Calls `execute_pipeline()` from Hamilton, wraps in root OTel span
   - Target: (1) Run extraction (Python), (2) Build spec, (3) Call `execute_cpg_build()`,
     (4) Parse RunResult into `GraphProductBuildResult`

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
- **Validated:** Extraction DAG is very flat - 7 of 9 extractors are fully independent
  with no inter-extractor dependencies. Only `python_imports` → `python_external` form
  a sequential 2-step chain. All extractors are plain Python functions with no Hamilton
  decorators - they just consume config and produce Arrow tables. This makes Option A
  trivially implementable with a ThreadPoolExecutor for the 7 independent extractors
  and a sequential pass for the imports→external chain.

**Option B: Keep Hamilton for extraction only**
- Use Hamilton exclusively for the extraction DAG
- Write extraction outputs to Delta at Hamilton boundary
- Feed Delta locations to Rust engine
- Pros: Preserves extraction dependency ordering without reimplementing
- Cons: Retains Hamilton as a dependency (though only for extraction)
- **Verdict:** Unnecessary given the flat DAG structure confirmed by validation

**Option C: Rust-managed extraction coordination**
- Extend the Rust engine to orchestrate Python extraction calls via PyO3 callbacks
- Most complex, highest long-term payoff
- Significant Rust work to manage Python function dispatch
- **Verdict:** Over-engineered for current needs; revisit if extraction grows more complex

**Recommendation:** **Option A** is definitively validated. The extraction DAG's flatness
(only 1 sequential dependency out of 9 extractors) makes Hamilton unnecessary for
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

**CLI exit code mapping (new):**

| Engine Error | CLI Exit Code | User Message |
|-------------|---------------|--------------|
| `EngineValidationError` | 2 | Spec validation failed (likely extraction schema mismatch) |
| `EngineCompileError` | 3 | Plan compilation failed (view dependency or transform error) |
| `EngineRuleViolationError` | 4 | Policy rule violation (strictness/safety enforcement) |
| `EngineRuntimeError` | 5 | Materialization or Delta write failure |
| `ImportError` (engine not built) | 10 | Run `scripts/rebuild_rust_artifacts.sh` |

### 6.3 RuntimeConfig Wiring

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

### 6.4 Rust Engine Output Gap Analysis

**CRITICAL:** Not all 9 current Hamilton pipeline outputs can be produced by the Rust engine.

| Hamilton Output Node | Rust-Producible | Notes |
|---------------------|----------------|-------|
| `write_cpg_nodes_delta` | YES | Core CPG output |
| `write_cpg_edges_delta` | YES | Core CPG output |
| `write_cpg_properties_delta` | YES | Core CPG output |
| `write_semantic_normalize_delta` | YES | Normalization output |
| `write_semantic_relate_delta` | YES | Relationship output |
| `write_evidence_summary_delta` | YES | Evidence summary |
| `write_normalize_outputs_delta` | **NO** | Python-side normalization state tracking |
| `write_extract_error_artifacts_delta` | **NO** | Python-side extraction error collection |
| `write_run_manifest_delta` | **NO** | Hamilton datasaver output node with Delta writes |

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

### 6.5 Determinism Model Clarification

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

## 7) Implementation Phases

### Phase 1: Extraction Decoupling (Foundation)

**Goal:** Extract extraction logic from Hamilton nodes into standalone callable functions.

**VALIDATED:** Extraction DAG is very flat (7/9 extractors independent). All extractors are
plain Python functions producing Arrow tables - no Hamilton decorators, no Hamilton-specific
return types. This makes decoupling straightforward.

**Steps:**
1. **Prerequisite: Extract portable types** from `hamilton_pipeline/types/`:
   - Move `ScipIndexConfig` → `src/extract/extractors/scip/config.py` (pure dataclass, trivially movable)
   - Move `ScipIdentityOverrides` → `src/extract/extractors/scip/config.py` (pure dataclass)
   - Update imports in `src/extract/extractors/scip/setup.py` and `src/cli/commands/build.py`
2. Create `src/extraction/orchestrator.py` - Direct extraction runner without Hamilton
3. Extract the extraction function chain from Hamilton modules:
   - `repo_scan` -> file discovery (independent)
   - `ast_extract` -> AST evidence (independent)
   - `cst_extract` -> LibCST evidence (independent)
   - `symtable_extract` -> symbol table evidence (independent)
   - `bytecode_extract` -> bytecode evidence (independent)
   - `scip_extract` -> SCIP evidence (independent)
   - `treesitter_extract` -> tree-sitter evidence (independent)
   - `python_imports` -> import evidence (independent)
   - `python_external` -> external dependency evidence (**depends on** `python_imports`)
4. Write extraction outputs to Delta tables in `work_dir/extract/`
5. Return `dict[str, str]` mapping logical names to Delta locations
6. Use `concurrent.futures.ThreadPoolExecutor` for the 7 independent extractors;
   run `python_imports` → `python_external` sequentially after the parallel batch
7. Collect extraction errors during execution for `write_extract_error_artifacts_delta`
   auxiliary output (see §6.4)

**Validation:** Extraction outputs match existing Hamilton pipeline outputs (schema + row counts).

### Phase 2: Build Command Rewrite (Core Transition)

**Goal:** Replace `build_command()` Hamilton path with direct Rust engine invocation.

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
   - Remove Hamilton-specific CLI options (executor, graph adapter, tracker, plan overrides)
   - Add Rust engine CLI options (engine profile, rulepack profile, compliance, tracing)
   - Replace `GraphProductBuildRequest` construction with direct orchestrator call
3. Update `GraphProductBuildRequest`:
   - Remove Hamilton fields
   - Add engine fields
4. Replace `build_graph_product()` implementation:
   - Remove `execute_pipeline()` call
   - Add extraction + spec build + engine execution flow
   - Preserve OTel span structure (root_span, heartbeat, diagnostics)

5. Handle 3 auxiliary outputs not producible by Rust engine (see §6.4):
   - `write_normalize_outputs_delta` → Produce from Python semantic compiler side-effect
   - `write_extract_error_artifacts_delta` → Collect during extraction phase, write from orchestrator
   - `write_run_manifest_delta` → Construct from RunResult envelope + extraction metadata

**Validation:** Full build produces identical CPG outputs (nodes, edges, props) to Hamilton pipeline.
Auxiliary outputs (normalize, errors, manifest) produce equivalent content to Hamilton versions.

### Phase 3: Plan Command Rewrite

**Goal:** Replace Hamilton plan inspection with Rust engine plan compilation.

**Steps:**
1. Modify `plan_command()`:
   - Replace `build_plan_context()` with `build_execution_spec()` + `SemanticPlanCompiler().compile()`
   - Map plan inspection features to compiled plan properties
2. Update `PlanOptions`:
   - Remove `execution_mode` (Hamilton)
   - Add `--engine-profile`, `--rulepack-profile`
3. Update plan output formatters:
   - `plan_signature` -> `compiled.spec_hash_hex()`
   - `task_count` -> `len(spec.view_definitions)`
   - `graph` -> Derive DOT from view_definitions + join_graph.edges
   - `inferred_deps` -> Extract from view_definitions.view_dependencies
   - `validation` -> Compilation success/failure + rule intents summary

### Phase 4: Hamilton Removal

**Goal:** Remove Hamilton as a dependency entirely.

**VALIDATED: Actual scope is 33 files in hamilton_pipeline/ (not 19 as originally estimated),
plus 6 additional files outside hamilton_pipeline/ with Hamilton coupling, plus 24 test files.**

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

### Phase 5: Test Adaptation

**Goal:** Ensure test coverage for the new execution path.

**VALIDATED: 24 test files import Hamilton and need migration or deletion.**

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

## 8) CLI Option Surface (Before/After)

### 8.1 Options Removed

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

### 8.2 Options Added

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

### 8.3 Options Unchanged

| Option | Group | Notes |
|--------|-------|-------|
| `--output-dir` / `-o` | output | Same semantics |
| `--work-dir` | output | Same semantics |
| `--include-errors` | output | Same semantics |
| `--include-manifest` | output | Same semantics |
| `--include-run-bundle` | output | Same semantics |
| `--determinism-tier` | execution | Python-side validation wrapper around engine 2-hash model (see §6.5) |
| `--runtime-profile` | execution | Maps to engine session profile |
| All SCIP options | scip | Extraction remains Python |
| All incremental options | incremental | Extraction remains Python |
| All repo scope options | repo-scope | Extraction remains Python |
| `--enable-tree-sitter` | advanced | Extraction remains Python |

---

## 9) Configuration Model Changes

### 9.1 `codeanatomy.toml` Sections

| Section | Status | Notes |
|---------|--------|-------|
| `[plan]` | **Remove** | Hamilton plan overrides |
| `[execution]` | **Modify** | Remove executor/adapter, add engine profile/rulepack |
| `[scip]` | Keep | Extraction config |
| `[incremental]` | Keep | Extraction config |
| `[otel]` | Keep | OTel config (also fed to engine TracingConfig) |
| `[delta]` | Keep | Delta maintenance config |
| `[engine]` | **Add** | Rust engine profile, rulepack, compliance, tracing |

### 9.2 New `[engine]` Config Section

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

## 10) Import Dependency Cleanup

### 10.1 Modules That Lose Hamilton Imports

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

### 10.2 Type Migrations

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

## 11) Risk Assessment

### 11.1 High Risk

| Risk | Impact | Mitigation |
|------|--------|------------|
| Extraction output schema drift | Rust engine rejects inputs | Schema contract tests between extraction Delta outputs and SemanticExecutionSpec input_relations |
| 3 auxiliary outputs not Rust-producible | Missing normalize_outputs, extract_errors, manifest | Python-side handling in orchestrator (see §6.4) |
| Determinism tier semantic mismatch | Users expect tier guarantees engine can't provide | Python-side validation wrapper around 2-hash model (see §6.5) |

### 11.2 Medium Risk

| Risk | Impact | Mitigation |
|------|--------|------------|
| OTel span structure changes | Observability gap | Map existing span names to new execution phases; Rust engine has tracing presets |
| Config file breaking change | User configs rejected | Validate old config sections; emit deprecation warnings |
| SCIP type decoupling | Import breakage | Extract SCIP types to standalone module FIRST in Phase 1 |
| 24 test files need migration | Test coverage gap during transition | Migrate in Phase 5; maintain parity test suite |
| Run manifest reimplementation | Manifest format regression | Define manifest contract from RunResult envelope + extraction metadata |

### 11.3 Low Risk (VALIDATED)

| Risk | Impact | Mitigation |
|------|--------|------------|
| Extraction parallelism bugs | VERY LOW - DAG is flat | Only 1 sequential dependency (imports→external); 7/9 extractors fully independent |
| Semantic compiler Hamilton deps | NONE - confirmed zero deps | `build_cpg()`, `SemanticCompiler`, IR pipeline all Hamilton-free |
| Delta command changes | None | Already DataFusion-native |
| Diag/version/config changes | None | Execution-agnostic |
| CLI group changes | Minor UX | Groups defined in `cli/groups.py`; just remove unused ones |

---

## 12) Testing Strategy

### 12.1 Parity Testing

Before removing Hamilton, add parity tests that:
1. Run the same repo through both Hamilton pipeline and Rust engine
2. Compare CPG output tables (nodes, edges, props) column-by-column
3. Verify row counts match
4. Verify determinism hashes match (when determinism tier is canonical)

### 12.2 New Test Categories

| Test | Location | Validates |
|------|----------|-----------|
| Extraction orchestrator unit | `tests/unit/test_extraction_orchestrator.py` | Each extractor produces correct-schema Delta output |
| Spec building integration | `tests/integration/test_spec_from_extraction.py` | Extraction outputs -> valid SemanticExecutionSpec |
| Engine facade integration | `tests/integration/test_engine_facade.py` | Spec -> Rust engine -> valid RunResult |
| CLI build e2e | `tests/e2e/test_cli_build_engine.py` | Full CLI -> extraction -> engine -> Delta outputs |
| CLI plan e2e | `tests/e2e/test_cli_plan_engine.py` | Full CLI -> plan compilation -> display |
| CLI golden updates | `tests/cli_golden/` | Updated snapshots for new option surface |

---

## 13) Dependency Impact

### 13.1 Dependencies Removed

| Package | Current Use | Notes |
|---------|------------|-------|
| `sf-hamilton` | Pipeline orchestration | Core removal target |
| Hamilton graph adapters | Parallel execution backends | Replaced by engine-internal DataFusion parallelism |
| Hamilton materializers | Output writing | Replaced by engine Delta writes (6/9 outputs) + Python orchestrator (3/9 outputs) |
| Hamilton cache system | `cache_lineage.py`, `cache_versioning.py` | NOT ported; Rust engine has independent `CachePlacementPolicy` |

### 13.2 Dependencies Retained

| Package | Use | Notes |
|---------|-----|-------|
| `datafusion` | Python DataFusion (extraction-side queries) | Still needed for extraction-phase DataFusion use |
| `deltalake` | Delta table reads/writes (extraction phase) | Extraction outputs to Delta |
| `pyarrow` | Arrow tables (extraction phase) | Extraction evidence tables |
| `msgspec` | Spec serialization (Python <-> Rust boundary) | Core contract format |
| `codeanatomy_engine` | Rust engine PyO3 extension | Core execution engine |
| `rustworkx` | Graph operations (semantic/relspec) | Still needed if semantic compiler uses it |

---

## 14) Migration Path for Users

### 14.1 Config Migration

```
# Old (Hamilton-era)
codeanatomy build ./repo --execution-mode plan_parallel --executor-kind threadpool --executor-max-tasks 4

# New (DataFusion-era)
codeanatomy build ./repo --engine-profile medium
```

### 14.2 Deprecation Strategy

1. **Phase 1:** Add engine options alongside Hamilton options; Hamilton options emit deprecation warnings
2. **Phase 2:** Hamilton options become no-ops with loud warnings
3. **Phase 3:** Remove Hamilton options entirely

This 3-phase approach allows users to migrate incrementally.

---

## 15) Open Questions (All Resolved)

All original open questions have been resolved through targeted code validation:

1. **Extraction parallelism model: RESOLVED → Option A (ThreadPoolExecutor)**
   Extraction DAG is very flat: 7/9 extractors are fully independent with no
   inter-extractor dependencies. Only `python_imports` → `python_external` form
   a sequential 2-step chain. All extractors are plain Python functions (no Hamilton
   decorators) producing standard Arrow types. ThreadPoolExecutor is sufficient;
   Hamilton is unnecessary for dependency management.

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
     metadata (see §6.4).

5. **Cache lineage: RESOLVED → Independent systems, no migration needed**
   `cache_lineage.py` and `cache_versioning.py` are 100% Hamilton-specific caching
   mechanisms. They are NOT general-purpose and should NOT be ported. The Rust engine
   has its own independent caching via `CachePlacementPolicy` which is determined at
   compile time from the spec. Extraction-level caching (if desired) should be
   implemented as a new, simple mechanism in the extraction orchestrator, completely
   independent of Hamilton's cache system.

### 15.1 New Questions (Arising from Validation)

6. **OtelNodeHook / OtelPlanHook migration:** `src/obs/otel/hamilton.py` contains Hamilton
   lifecycle hooks for OTel instrumentation. These provide per-node execution tracing.
   The Rust engine's tracing presets (Maximal/MaximalNoData/ProductionLean) provide
   equivalent functionality via the datafusion-tracing extension. Should we add any
   Python-side OTel instrumentation for the extraction phase, or is the Rust engine's
   tracing sufficient? **Recommendation:** Add lightweight OTel spans in the extraction
   orchestrator (per-extractor spans) to maintain observability parity.

7. **HamiltonConfigSpec / HamiltonConfigRuntime replacement:** These types exist in
   `hamilton_pipeline/types/` and may be referenced by config loading code. They need
   to be audited for any non-Hamilton-specific fields that should migrate to the new
   `[engine]` config section. **Recommendation:** Audit during Phase 4 and migrate any
   generic config fields to `RuntimeConfig` or the new engine config section.

---

## 16) Summary of Work Estimates (Updated)

| Phase | Scope | Files Modified | Files Created | Files Deleted |
|-------|-------|---------------|---------------|---------------|
| Phase 1: Extraction Decoupling | Medium | ~7 | ~3 | 0 |
| Phase 2: Build Command Rewrite | Large | ~5 | ~3 | 0 |
| Phase 3: Plan Command Rewrite | Small | ~1 | 0 | 0 |
| Phase 4: Hamilton Removal | **Large** | ~8 | 0 | **~39** |
| Phase 5: Test Adaptation | **Large** | ~12 | ~6 | **~24** |
| **Total** | | **~33** | **~12** | **~63** |

The net result is a **significant reduction** in codebase complexity:
- **~33 Hamilton pipeline files deleted** (corrected from 19; includes types/, modules/ subdirs)
- ~6 additional Hamilton-coupled files outside hamilton_pipeline/ cleaned up
- ~24 test files migrated or deleted
- ~28 CLI options removed, ~7 added
- 3 auxiliary outputs reimplemented as Python-side orchestrator outputs
- Direct, traceable execution path from CLI -> extraction -> spec -> Rust engine
- Single execution backend (DataFusion) instead of Hamilton + DataFusion hybrid

### 16.1 Validation Confidence Assessment

| Aspect | Confidence | Evidence |
|--------|-----------|---------|
| Extraction decoupling feasibility | **HIGH** | All 9 extractors verified as plain functions; DAG flatness confirmed |
| Semantic compiler independence | **HIGH** | Zero Hamilton imports in semantics/ verified |
| Hamilton removal scope | **HIGH** | 131 total Hamilton imports audited; all coupling points enumerated |
| Rust engine output coverage | **HIGH** | 6/9 outputs confirmed Rust-producible; 3 gaps documented with mitigations |
| Determinism model compatibility | **MEDIUM** | 2-hash model verified but tier mapping needs design validation |
| Incremental processing portability | **MEDIUM** | Config portable; runtime behavior needs reimplementation |
| DataFusion best-in-class patterns | **HIGH** | Tracing presets, cache knobs, TableProvider patterns all validated against dfdl_ref |
