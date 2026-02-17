# Cross-Agent Synthesis: Rust Workspace Design Review

**Date:** 2026-02-16
**Scope:** Full `rust/` workspace (~58K LOC, 8 crates, 154 .rs files)
**Agents:** 5 parallel design-reviewer agents, each covering a non-overlapping scope

## Aggregate Scorecard

The table below shows the minimum, average, and maximum alignment score (0-3) for each principle across all 5 agents.

| # | Principle | Min | Avg | Max | Weakest Agent(s) |
|---|-----------|-----|-----|-----|-------------------|
| 1 | Information hiding | 1 | 2.0 | 3 | Agent 5 (PyBindings: `pub ctx` on PySessionContext) |
| 2 | Separation of concerns | 0 | 1.8 | 3 | Agent 5 (`codeanatomy_ext.rs` 3,582 LOC monolith) |
| 3 | SRP | 0 | 1.4 | 2 | Agent 5 (`codeanatomy_ext.rs` 7+ change reasons), Agent 2 (`rule_instrumentation.rs` 3 reasons) |
| 4 | High cohesion, low coupling | 1 | 1.8 | 2 | Agent 5 (low cohesion in `codeanatomy_ext.rs`) |
| 5 | Dependency direction | 2 | 2.8 | 3 | Generally excellent across all agents |
| 6 | Ports & Adapters | 2 | 2.2 | 3 | Agents 1,2,3 (minor gaps: untyped JSON, string-based scan detection) |
| 7 | **DRY** | **0** | **0.8** | **1** | **All agents** — systemic weakest principle |
| 8 | Design by contract | 2 | 2.4 | 3 | Agent 4 (copy-paste error messages in coerce_types) |
| 9 | Parse, don't validate | 2 | 2.6 | 3 | Agents 4,5 (CDF magic numbers, stringly-typed timestamps) |
| 10 | Make illegal states unrepresentable | 2 | 2.0 | 2 | All at 2/3 — version/timestamp mutual exclusivity, SchemaDiff tuple alias |
| 11 | CQS | 1 | 2.2 | 3 | Agent 5 (delta functions mix command + query), Agent 1 (`resolve_source_name`) |
| 12 | DI + explicit composition | 2 | 2.4 | 3 | Agents 2,3 (inline construction, global OnceLock) |
| 13 | Composition over inheritance | 3 | 3.0 | 3 | Perfect across all agents |
| 14 | Law of Demeter | 1 | 1.8 | 2 | Agent 5 (`session_context_contract(ctx)?.ctx` 63x) |
| 15 | Tell, don't ask | 1 | 2.0 | 3 | Agent 5 (procedural ask-then-operate pattern) |
| 16 | Functional core, imperative shell | 2 | 2.4 | 3 | Agents 2,3 (IO mixed with logic in pipelines) |
| 17 | Idempotency | 2 | 2.8 | 3 | Generally excellent |
| 18 | Determinism | 1 | 2.6 | 3 | Agent 5 (`now_unix_ms()` breaks determinism) |
| 19 | KISS | 0 | 1.6 | 2 | Agent 5 (`codeanatomy_ext.rs` 80+ functions, 25-param delta functions) |
| 20 | YAGNI | 2 | 2.2 | 3 | Agents 2,3 (maintenance stubs, speculative msgpack path) |
| 21 | Least astonishment | 1 | 1.6 | 2 | Agent 4 (copy-paste error messages), Agent 5 (runtime-per-call, docstring errors) |
| 22 | Declare and version contracts | 2 | 2.4 | 3 | Agents 3,4 (RegistrySnapshot unversioned, glob re-exports) |
| 23 | Design for testability | 1 | 2.2 | 3 | Agent 5 (PyO3 entanglement prevents unit testing) |
| 24 | Observability | 1 | 1.8 | 2 | Agent 5 (no tracing on delta operations) |

### Systemic Patterns

**Strongest principles (avg >= 2.5):**
- P13 Composition over inheritance (3.0) — Perfect; Rust's trait system naturally enforces this
- P5 Dependency direction (2.8) — Consistently correct layering
- P17 Idempotency (2.8) — Delta protocol and BLAKE3 hashing enforce this
- P9 Parse, don't validate (2.6) — Good boundary parsing patterns
- P18 Determinism (2.6) — Strong determinism contracts (one notable exception)

**Weakest principles (avg <= 1.5):**
- **P7 DRY (0.8)** — The single worst principle. Every agent scored 0/3 or 1/3
- P3 SRP (1.4) — Two severe monoliths: `codeanatomy_ext.rs` (3,582 LOC) and `rule_instrumentation.rs` (815 LOC)
- P21 Least astonishment (1.6) — Copy-paste error messages, naming collisions, surprising runtime behavior
- P19 KISS (1.6) — Driven primarily by `codeanatomy_ext.rs` complexity

---

## Cross-Agent Theme Synthesis

### Theme A: DRY Is Systemically Violated (All 5 Agents)

Every agent independently identified DRY as their weakest or second-weakest principle. The violations span all scopes:

| Agent | Scope | DRY Score | Key Duplications |
|-------|-------|-----------|------------------|
| 1 | Compiler+Session | 1/3 | Schema hashing 2x, config key lists diverge, governance enum 3x, SQL capture 2x |
| 2 | Executor+Rules | 1/3 | Materialization 2x (~100 LOC), rule instrumentation 3x (~500 LOC) |
| 3 | Delta+Registry | 1/3 | `latest_operation_metrics` 2x, `snapshot_with_gate` 2x, `parse_rfc3339` 2x, `is_arrow_operator` 2x, `FunctionKind` 2x |
| 4 | UDF | 1/3 | `string_array_any` 2x, `scalar_to_string` 2x, `scalar_to_i64` 2x, CollectSet/ListUnique identical behavior |
| 5 | PyBindings | 0/3 | `parse_major()` 3x, `schema_from_ipc()` 2x, `DELTA_SCAN_CONFIG_VERSION` 2x, manifest validation 2x |

**Root cause:** The crate boundaries are well-drawn but lack shared utility infrastructure for cross-crate knowledge. Within crates, copy-paste-modify development has accumulated without consolidation passes.

**Cross-cutting resolution:** The duplication forms three clusters that can be addressed in parallel:
1. **Session/compiler utilities** (Agent 1): `capture_df_settings`, `PLANNING_AFFECTING_CONFIG_KEYS`, `GovernancePolicy` enum → new `session/capture.rs`
2. **Delta operation utilities** (Agents 2, 3): `latest_operation_metrics`, `snapshot_with_gate`, `parse_rfc3339`, `eager_snapshot`, materialization dedup → new `delta_common.rs`
3. **Cross-crate shared knowledge** (Agents 3, 4, 5): `parse_major`, `schema_from_ipc`, `DELTA_SCAN_CONFIG_VERSION`, `is_arrow_operator`, `FunctionKind` → new shared crate or module

### Theme B: Two `function_factory.rs` Files (Agents 3 + 4)

Both agents flagged the naming collision:

- **Agent 3:** Top-level `function_factory.rs` (920 LOC) — SQL macro factory implementing `FunctionFactory` trait for `CREATE FUNCTION`
- **Agent 4:** `udf/function_factory.rs` (695 LOC) — UDF primitive factory for rule-author convenience

The split is architecturally principled (SQL DDL vs programmatic registration) but the identical filenames hide this distinction. Additionally, `FunctionKind` is defined in both scopes with different variant sets (4 variants in one, 3 in the other).

**Resolution:**
1. Rename `function_factory.rs` → `sql_macro_factory.rs` (Agent 3's recommendation)
2. Rename `udf/function_factory.rs` → `udf/primitives.rs` or `udf/policy_factory.rs` (Agent 4's recommendation)
3. Unify `FunctionKind` into a single definition with all 4 variants

### Theme C: Determinism Contract Chain (Agents 1 + 2 + 5)

The determinism chain flows: spec hashing (Agent 1) → plan digests (Agent 1) → delta writer I/O (Agent 2) → PyO3 surface (Agent 5).

| Layer | Agent | Status | Issue |
|-------|-------|--------|-------|
| Spec hashing | 1 | ⚠️ | Two divergent hash functions for the same schema (`plan_bundle.rs:632` JSON-based vs `introspection.rs:34` field-by-field BLAKE3) |
| Plan digests | 1 | ✅ | BLAKE3 canonical JSON, sorted vectors, DeterminismContract |
| Rulepack fingerprints | 2 | ✅ | BLAKE3 over sorted rule names; three-layer identity (spec_hash + envelope_hash + rulepack_fingerprint) |
| Delta I/O | 2 | ✅ | Explicit Append/Overwrite modes; BLAKE3 fingerprints stable |
| PyO3 surface | 5 | ⚠️ | `now_unix_ms()` introduces nondeterminism in cache metrics and execution snapshots |

**Resolution:**
1. **Critical:** Consolidate schema hashing to use the field-by-field BLAKE3 approach from `introspection.rs` as the single authority. Replace `plan_bundle.rs:632` with a call to this function.
2. **Important:** Make `now_unix_ms()` injectable (clock parameter or documented exclusion from determinism contract).
3. **Documentation:** Create a shared spec for the three-layer identity contract (spec_hash, envelope_hash, rulepack_fingerprint) with cross-language golden tests.

### Theme D: ABI-Stable Plugin Boundary vs Inline UDF Registration (Agents 4 + 5)

Agent 4 reviews how UDFs are registered inline via `udf_registry.rs` and the `build_udf` dispatch. Agent 5 reviews how `df_plugin_codeanatomy` bundles those same UDFs via the ABI-stable interface and how `codeanatomy_ext.rs` manages plugin loading.

**Key observations:**
- The plugin system (`df_plugin_api`, `df_plugin_host`, `df_plugin_codeanatomy`) is well-designed (Agent 5: "Excellent" rating)
- The inline registration path in `udf_registry` (Agent 4) is also clean
- The problem is at the junction: `codeanatomy_ext.rs` handles both plugin management AND inline UDF registration in the same monolithic file
- Knowledge duplication occurs because both paths need `schema_from_ipc()`, `DELTA_SCAN_CONFIG_VERSION`, and `parse_major()`

**Resolution:** The shared knowledge crate from Theme A resolves the duplication. The `codeanatomy_ext.rs` decomposition from Theme E separates plugin management from UDF registration into distinct submodules.

### Theme E: The `codeanatomy_ext.rs` Monolith (Agent 5, with cascading effects)

Agent 5 identified `codeanatomy_ext.rs` (3,582 LOC, 80+ `#[pyfunction]` definitions) as the most severe architectural concern in the entire workspace. It scored 0/3 on three principles (P2, P3, P19) and 1/3 on six more (P1, P4, P11, P14, P15, P21).

This monolith cascades negative effects across the entire review:
- It is the primary home of the cross-crate DRY violations (Theme A)
- It contains the plugin bridge that creates coupling to Theme D
- It houses `now_unix_ms()` that breaks the determinism chain (Theme C)
- All 63 `session_context_contract(ctx)?.ctx` chains live here (P14)

**Resolution:** Decompose into 6-8 focused submodules:
- `codeanatomy_ext/delta_provider.rs` — Provider construction and scan config
- `codeanatomy_ext/delta_mutations.rs` — Write, merge, delete, update
- `codeanatomy_ext/delta_maintenance.rs` — Vacuum, optimize, restore, checkpoint
- `codeanatomy_ext/plugin_bridge.rs` — Plugin loading, registration, manifest validation
- `codeanatomy_ext/udf_registration.rs` — UDF/function factory, config, snapshots
- `codeanatomy_ext/cache_tables.rs` — Cache table functions and metrics
- `codeanatomy_ext/session_utils.rs` — Runtime, session contract, helpers
- `codeanatomy_ext/mod.rs` — Re-exports and `init_module()`/`init_internal_module()`

---

## Unified Action Sequence

Ordered by dependency, risk reduction, and effort. Actions that unblock later actions come first.

### Phase 1: Quick Fixes (Small effort, immediate value)

| # | Action | Source | Effort | Impact |
|---|--------|--------|--------|--------|
| 1.1 | Fix copy-paste error messages in `CpgScoreUdf.coerce_types` (metadata.rs:51) and `Utf8NullIfBlankUdf.coerce_types` (string.rs:331) | Agent 4 | small | Fixes user-facing confusion; fixes contradictory contract |
| 1.2 | Make `execute_and_materialize` delegate to `_with_plans` in `runner.rs` | Agent 2 | small | Eliminates 100 LOC exact duplication |
| 1.3 | Rename `resolve_source_name` → `ensure_source_registered` in `plan_compiler.rs:362` | Agent 1 | small | Makes side effect visible; fixes P11/P21 |
| 1.4 | Replace `SchemaDiff` tuple alias with named struct in `introspection.rs:13` | Agent 1 | small | Prevents field transposition errors |
| 1.5 | Fix docstring errors on `register_table_provider` (context.rs:631) and `PyJoin::on()` (dataframe.rs:148) | Agent 5 | small | Prevents misuse from misleading docs |
| 1.6 | Remove dead params from `variadic_any_signature` (common.rs:64-70) | Agent 4 | small | Removes misleading API |
| 1.7 | Add CDF rank named constants in `cdf.rs:17-26` | Agent 4 | small | Self-documenting, prevents magic number drift |

### Phase 2: Knowledge Consolidation (Medium effort, high risk reduction)

| # | Action | Source | Effort | Impact |
|---|--------|--------|--------|--------|
| 2.1 | **Consolidate schema hashing:** Replace `plan_bundle.rs:632` JSON hash with call to `schema::introspection::hash_arrow_schema` | Agent 1 | small | Fixes determinism chain integrity (Theme C) |
| 2.2 | **Extract shared delta helpers** into `delta_common.rs`: `latest_operation_metrics`, `snapshot_with_gate`, `parse_rfc3339`, `eager_snapshot` | Agent 3 | small | Eliminates 4 function duplications across delta modules |
| 2.3 | **Create shared knowledge crate/module** (`df_plugin_common`): `parse_major()`, `schema_from_ipc()`, `DELTA_SCAN_CONFIG_VERSION`, manifest validation | Agent 5 | medium | Eliminates highest-risk cross-crate duplication |
| 2.4 | **Move duplicated UDF utilities** to `udf/common.rs`: `string_array_any`, `scalar_to_string`, `scalar_to_i64` from `udaf_builtin.rs` | Agent 4 | small | Eliminates 3 duplicated functions |
| 2.5 | **Extract shared operator helpers**: `is_arrow_operator`, `can_use_get_field` into `operator_utils.rs` | Agent 3 | small | Eliminates cross-module duplication |
| 2.6 | **Unify `FunctionKind`** into single definition; rename files (`sql_macro_factory.rs` + `udf/primitives.rs`) | Agents 3+4 | medium | Resolves naming collision (Theme B) |
| 2.7 | **Extract session capture utilities** into `session/capture.rs`: `capture_df_settings`, `PLANNING_AFFECTING_CONFIG_KEYS`, `GovernancePolicy` enum | Agent 1 | medium | Eliminates 4 instances of duplicated session knowledge |
| 2.8 | **Consolidate CollectSetUdaf/ListUniqueUdaf** shared accumulator behavior | Agent 4 | medium | Eliminates ~80 lines identical implementation |

### Phase 3: Structural Decomposition (Large effort, high structural value)

| # | Action | Source | Effort | Impact |
|---|--------|--------|--------|--------|
| 3.1 | **Decompose `codeanatomy_ext.rs`** into 6-8 focused submodules (Theme E) | Agent 5 | large | Resolves cascading violations across P2/P3/P4/P14/P15/P19/P21/P23/P24 |
| 3.2 | **Evaluate `datafusion-tracing` crate** for rule instrumentation; adopt `instrument_rules_with_spans!` to replace 815 LOC hand-rolled sentinel/wrapper/wiring in `rule_instrumentation.rs` | Agent 2 | medium | Reduces 815 LOC → ~100 LOC; resolves P3/P7/P19 |
| 3.3 | **Decompose `execute_pipeline`** (pipeline.rs:105) into named phase helpers with tracing spans | Agent 2 | medium | Resolves P2/P3/P23/P24 for pipeline orchestration |
| 3.4 | **Decompose `registry_snapshot.rs`** (1,053 LOC) into snapshot, capabilities, and per-UDF metadata modules | Agent 3 | medium | Resolves P3/P7 for registry; enables UDF self-description |
| 3.5 | **Extract `ArgBest` module** from `udaf_builtin.rs` (2,225 LOC): move ArgBestAccumulator and consumers to `udaf_arg_best.rs` | Agent 4 | medium | Reduces largest file by ~500 LOC |

### Phase 4: Interface Refinement (Medium effort, design quality)

| # | Action | Source | Effort | Impact |
|---|--------|--------|--------|--------|
| 4.1 | **Introduce `TableVersion` enum** replacing `(Option<i64>, Option<String>)` across all delta request structs | Agent 3 | medium | Eliminates "both specified" illegal state in 8+ structs |
| 4.2 | **Introduce delta operation request structs** for PyO3 layer (DeltaGateParams, DeltaCommitParams, DeltaTableLocator) | Agent 5 | medium | Replaces 15-25 parameter functions |
| 4.3 | **Extract pure domain logic from PyO3** (`compiler.rs:build_spec_json`) into `codeanatomy_engine` core | Agent 5 | small | Enables unit testing of spec construction |
| 4.4 | **Tighten `CpgRuleSet` visibility**: `pub(crate)` fields + accessor methods | Agent 2 | small | Enforces information hiding |
| 4.5 | **Split `AdaptiveTuner::observe()`** into `record_metrics()` + `propose_adjustment()` | Agent 2 | small | Fixes CQS violation |
| 4.6 | **Add `RegistrySnapshot` version field** (version: u32 = 1) | Agent 3 | small | Enables schema evolution detection |
| 4.7 | **Change `pub ctx` → `pub(crate) ctx`** on `PySessionContext` + add accessor | Agent 5 | small | Narrows blast radius for SessionContext changes |

### Phase 5: Observability and Documentation (Small-medium effort, operational quality)

| # | Action | Source | Effort | Impact |
|---|--------|--------|--------|--------|
| 5.1 | **Add tracing spans** to all public delta operation functions in `delta_mutations.rs`, `delta_maintenance.rs` | Agent 3 | small | Enables operational debugging |
| 5.2 | **Add tracing spans** to `compile_request` phases (compilation, scheduling, pushdown, artifacts) | Agent 1 | medium | Observability for compile pipeline |
| 5.3 | **Add `#[tracing::instrument]`** to key PyO3 entry points | Agent 5 | small | Rust-side operation correlation |
| 5.4 | **Document three-layer determinism contract** (spec_hash, envelope_hash, rulepack_fingerprint) | Agent 2 | small | Cross-language specification |
| 5.5 | **Make `now_unix_ms()` injectable or documented** as nondeterministic | Agent 5 | small | Determinism contract clarity |
| 5.6 | **Remove deprecated positional-argument shim functions** (~450 LOC across delta_mutations.rs and delta_maintenance.rs) | Agent 3 | small | Reduces cognitive load |

---

## Summary Statistics

| Metric | Value |
|--------|-------|
| Total files reviewed | 150 |
| Total LOC reviewed | ~58,385 |
| Total findings | 120 across all 5 agents |
| Principles at 3/3 (any agent) | P5, P8, P9, P11, P12, P13, P15, P16, P17, P18 |
| Principles at 0/3 (any agent) | P2, P3, P7, P19 |
| Systemic weakness | P7 DRY (avg 0.8/3 — every agent scored 0 or 1) |
| Systemic strength | P13 Composition (avg 3.0/3 — perfect across all agents) |
| Quick fixes (Phase 1) | 7 actions, all small effort |
| Knowledge consolidation (Phase 2) | 8 actions, small-medium effort |
| Structural decomposition (Phase 3) | 5 actions, medium-large effort |
| Interface refinement (Phase 4) | 7 actions, small-medium effort |
| Observability (Phase 5) | 6 actions, small-medium effort |
| **Total actions** | **33** |
