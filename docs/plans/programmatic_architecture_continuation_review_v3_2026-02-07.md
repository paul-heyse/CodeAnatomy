# Programmatic Architecture Continuation Review (V3, Code-Current)

**Date:** 2026-02-07  
**Reviewed input:** `docs/plans/programmatic_architecture_continuation_v1_2026-02-07.md`  
**Grounding:** CQ queries, repository scripts, and current DataFusion/Delta integration surfaces.

---

## 1. Environment and Validation Baseline

Setup requested in this review was completed:

- `bash scripts/bootstrap_codex.sh`
- `uv sync`

Both succeeded on Python `3.13.12`.

---

## 2. Corrections to the Continuation Document (Current State vs Planned State)

The continuation document is directionally strong, but several "to-do" areas are now already complete or materially changed.

### 2.1 Drift-surface closure is already substantially complete

`uv run scripts/check_drift_surfaces.py` currently reports all checks passing:

- `CompileContext(...)` outside boundary: `0` violations.
- `dataset_bindings_for_profile(...)` fallback usage: `0`.
- `compile_tracking(...)` entrypoint instrumentation: `0` missing.
- `resolver_identity_tracking(...)` entrypoint instrumentation: `0` missing.
- `record_resolver_if_tracking(...)` boundary instrumentation: `0` missing.
- `record_artifact(...)` string-literal names: `0`.
- `record_artifact(...)` inline `ArtifactSpec(...)`: `0`.

Implication: Phases framed as broad fallback cleanup should be reframed as regression prevention and hardening.

### 2.2 Outcome-based maintenance is already integrated in production paths

Current production callsites:

- `src/datafusion_engine/io/write.py:1790`
- `src/semantics/incremental/delta_context.py:238`

Current internal fallback-only call:

- `src/datafusion_engine/delta/maintenance.py:323` (`resolve_delta_maintenance_plan` used inside `resolve_maintenance_from_execution`)

Implication: Wave 4A is not "wire from scratch"; it is now threshold quality tuning and diagnostics quality.

### 2.3 Execution authority and runtime capabilities are already wired

Production construction:

- `src/hamilton_pipeline/driver_factory.py:665` (`ExecutionAuthorityContext`)

Capability snapshot capture:

- `src/datafusion_engine/extensions/runtime_capabilities.py:78` (`detect_plan_capabilities`)
- `src/datafusion_engine/extensions/runtime_capabilities.py:213` (`build_runtime_capabilities_snapshot`)

Artifact spec already present:

- `src/serde_artifact_specs.py:473` (`DATAFUSION_RUNTIME_CAPABILITIES_SPEC`)

Implication: remaining work is coverage depth and typed payload rigor, not initial plumbing.

### 2.4 Typed artifact migration (string literal names) is done

`uv run scripts/audit_artifact_callsites.py` reports:

- `175 / 175` `record_artifact` callsites typed
- `0` raw-string callsites

Implication: Wave 5 should shift from "string to spec migration" to "payload schema strengthening + governance automation."

### 2.5 Cache policy remains a major residual heuristic

Heuristic source:

- `src/semantics/pipeline.py:262` (`_default_semantic_cache_policy`)
- Used at `src/semantics/pipeline.py:1221`

Implication: this remains one of the highest-value remaining non-programmatic decisions.

---

## 3. Deep Review of H.1 (Schema Derivation) and Required Corrections

H.1 is strategically right (reduce static schema duplication), but the proposed mechanism is currently under-specified and cannot safely replace the registry as written.

### 3.1 Primary blocker: metadata incompleteness for core extract datasets

`ExtractMetadata` exists (`src/datafusion_engine/extract/metadata.py:32`), but template expansion currently leaves key dataset `fields` as `None`:

- `ast_files_v1`: `src/datafusion_engine/extract/templates.py:537` / `:539`
- `libcst_files_v1`: `src/datafusion_engine/extract/templates.py:563` / `:565`
- `bytecode_files_v1`: `src/datafusion_engine/extract/templates.py:589` / `:591`
- `symtable_files_v1`: `src/datafusion_engine/extract/templates.py:615` / `:617`
- `tree_sitter_files_v1`: `src/datafusion_engine/extract/templates.py:755` / `:757`

Observed metadata footprint from a local probe:

- total metadata specs: `13`
- base extract schemas with complete field lists: only a subset (`scip_index_v1` complete, others partial/empty)

Implication: "derive schema from metadata fields" must first include a metadata enrichment phase; otherwise derivation is lossy.

### 3.2 Static registry carries richer semantics than metadata today

Static authorities still hold rich typed and nested contracts:

- `_BASE_EXTRACT_SCHEMA_BY_NAME`: `src/datafusion_engine/schema/registry.py:1739`
- `NESTED_DATASET_INDEX`: `src/datafusion_engine/schema/registry.py:1751`

These encode nested struct types and protocol-level shapes not currently represented in `ExtractMetadata`.

Implication: immediate decommission of these constants risks type regressions and schema drift.

### 3.3 Corrected H.1 migration strategy

H.1 should be split into explicit gates:

1. Metadata enrichment gate: add explicit field-type and nested-shape descriptors to template-expanded metadata.
2. Derivation parity gate: generate derived schemas and assert fingerprint equality against registry schemas for all extract + nested datasets.
3. Dual-authority gate: run registry and derived schemas in parallel with drift artifacts.
4. Cutover gate: switch runtime reads to derived authority only after sustained parity.
5. Decommission gate: remove static constants in bounded slices, not all-at-once.

---

## 4. Optimizations and Enhancements (Reprioritized)

### 4.1 Highest leverage now: compiled policy authority and cache-policy de-heuristic

Introduce a compile-time `CompiledExecutionPolicy` artifact and remove runtime cache naming heuristics.

Minimum scope:

- compile `cache_policy_by_view` from graph fanout, terminal-output role, and plan costs
- consume policy directly in semantic pipeline
- emit typed artifact + fingerprint for replay determinism

### 4.2 Upgrade scan-policy inference from binary heuristics to richer evidence

Current inference is functional but narrow (`small_table`, `has_pushed_filters`).

Add:

- projection ratio signals
- join-key/sort-key compatibility signals
- partition-filter selectivity signals
- explicit "insufficient evidence" reasons (not silent defaults)

### 4.3 Convert schedule costing from mostly uniform defaults to plan metrics

Tie `ScheduleCostContext.task_costs` to plan stats and scan unit characteristics to improve critical-path ordering under skewed workloads.

### 4.4 Expand policy validation from presence checks to evidence coherence checks

`validate_policy_bundle(...)` is active (`src/relspec/policy_validation.py:155`) and called (`src/hamilton_pipeline/driver_factory.py:2065`), but can be strengthened with:

- compiled-policy vs runtime-policy coherence checks
- capability-gated policy assertions (strict when capability absent)
- machine-readable remediation hints in artifacts

### 4.5 Typed payload governance is the next artifact-governance frontier

String artifact names are already eliminated; next leverage is ensuring high-value specs always declare `payload_type` and enforce schema-level compatibility in CI.

---

## 5. Expansion of Scope (Beyond the Continuation Document)

### 5.1 Decision provenance graph (policy -> evidence -> outcome)

Add a first-class artifact graph that links:

- compile-time decision
- evidence used (plan/capabilities/runtime)
- runtime outcome metrics
- confidence and fallback reasons

This creates direct explainability for "why policy X was chosen" and "whether it helped."

### 5.2 Counterfactual policy replay harness

Use stored plan bundles + artifacts to evaluate "what if policy B was applied" without mutating production policy.

Benefits:

- safer optimization iteration
- deterministic performance attribution
- regression-proof policy evolution

### 5.3 Workload-class policy compiler

Compile distinct policy bundles for workload classes (ingest-heavy, relationship-heavy, incremental replay, ad-hoc diagnostics) and choose deterministically at orchestration start.

### 5.4 Fallback quarantine model

Track any compatibility fallback as a first-class debt event:

- classify fallback type
- enforce SLO per fallback class
- escalate from warn -> fail in strict mode once migration window closes

### 5.5 Schema evidence lattice

Instead of "registry vs metadata" binary authority, define ordered evidence sources:

1. explicit boundary contracts
2. derived metadata contracts
3. runtime catalog observations

Then compile an authority decision with confidence and provenance.

---

## 6. Revised Execution Order

### Phase R0 (Immediate): Rebaseline plan document against current code

- Remove already-complete tasks from primary queue.
- Keep drift checks as hard CI gates.

### Phase R1: Compile policy authority

- Land `CompiledExecutionPolicy`.
- Replace `_default_semantic_cache_policy` with compiled policy consumption.

### Phase R2: Evidence depth and scheduler quality

- Enrich `PlanSignals`.
- Add plan-statistics-derived task costs.
- Harden policy validation with evidence coherence checks.

### Phase R3: Corrected schema derivation program (H.1 fixed)

- Enrich metadata contracts with types and nested semantics.
- Run derived-vs-static parity and drift artifacts.
- Cut over by slices.

### Phase R4: Closed-loop optimization with bounded controls

- Add confidence model + calibrator + counterfactual replay.
- Enforce measurable payoff gates before policy promotion.

---

## 7. Suggested KPIs for "Entirely Programmatic" Convergence

- `compile_context_out_of_boundary_callsites`: target `0` (already at `0`).
- `artifact_string_name_callsites`: target `0` (already at `0`; hold).
- `policy_decisions_backed_by_compiled_policy`: target `100%`.
- `inference_decisions_with_confidence_payload`: target `100%`.
- `schema_derivation_parity_rate` (derived vs static): target `100%` before decommission.
- `fallback_events_per_run` by class: monotonic decline to near-zero.
- `counterfactual_replay_match_rate` (semantic equivalence): target `>99%`.

---

## 8. Bottom Line

The architecture is further along than the continuation document indicates.  
The next step is no longer broad foundational build-out; it is controlled convergence:

- keep completed invariants closed,
- eliminate remaining runtime heuristics via compiled policy authority,
- fix H.1 preconditions before schema decommission,
- and move to evidence-linked, replayable optimization loops.
