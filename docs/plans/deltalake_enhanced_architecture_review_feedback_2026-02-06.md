# Feedback on `deltalake_enhanced_architecture_review_2026-02-06.md`

## Review Summary

The consolidated plan is strong and materially better than the initial draft. It has good direction on:
- removing QueryBuilder production usage,
- converging around strict provider/session boundaries,
- decomposing the `storage/deltalake/delta.py` monolith,
- and moving toward msgspec-first boundary models.

Main gaps are now in execution safety and precision:
- a few evidence claims are over-strong or require revalidation,
- some proposed refactors risk coupling/runtime layering regressions if sequenced incorrectly,
- and rollout/compatibility controls are not fully decision-complete for large-surface changes.

This feedback focuses on corrections, missing details, and improvement opportunities.

---

## High-Priority Corrections

## 1) Clarify evidence claim around `scan_profile.py` deletion

### Issue
The plan states `storage/deltalake/scan_profile.py` is “single-function shim” and delete-ready.

### Correction
Treat this as a **conditional** deletion, not immediate delete-ready. Even if logic is thin, it is part of the `storage/deltalake/__init__.py` re-export surface and may be externally imported.

### Recommendation
- Move to “staged delete”: deprecate import surface first, then remove after compatibility window.
- Add explicit CQ callsite proof for internal + external (if available) before hard deletion.

---

## 2) Avoid implying `disable_*` functions are dead code without export policy context

### Issue
The plan says most `disable_*` are internal zero-call and should be consolidated/deprioritized.

### Correction
Internal callsite count (`total_sites: 0`) does not prove API deadness because many of these functions are exported via `storage/deltalake/__init__.py` and may be externally consumed.

### Recommendation
- Keep as “default surface deprecation candidates,” not hard-delete candidates.
- Add a compatibility policy decision:
  - `Option A`: keep thin wrappers with `DeprecationWarning` for one release.
  - `Option B`: hard removal in next major version only.

---

## 3) Reconcile line-number-based decomposition plan with current file drift

### Issue
The plan uses fixed line ranges for module extraction from `delta.py`.

### Correction
Line ranges will drift quickly and are brittle as implementation guidance.

### Recommendation
Replace line-based decomposition with **function-cluster decomposition** by symbol list, e.g.:
- read path symbols,
- metadata/snapshot symbols,
- feature mutation symbols,
- maintenance symbols,
- write/mutation symbols.

This makes the plan implementation-stable across rebases.

---

## 4) Strengthen `StructBaseHotPath` adoption advice

### Issue
The plan proposes `StructBaseHotPath` (`gc=False`, `cache_hash=True`) adoption for more types.

### Correction
This is performance-sensitive and should not be default architectural guidance without profiling proof; `gc=False` can have subtle lifecycle implications.

### Recommendation
- Add explicit gate: only adopt `StructBaseHotPath` for measured hotspots with benchmarks.
- Keep default to `StructBaseStrict`/`StructBaseCompat` unless a profiling artifact justifies change.

---

## 5) Tighten compatibility language around `StorageProfile` / `DeltaStorePolicy` migration

### Issue
The plan suggests migrating policy classes to msgspec and possibly removing `FingerprintableConfig` inheritance.

### Correction
This is a breaking contract risk if hashing/fingerprinting semantics are currently relied on by cache identity or persisted artifacts.

### Recommendation
- Add a required invariant: fingerprints must remain byte-for-byte stable (or have explicit version bump + migration handling).
- Define migration strategy before model base-class changes.

---

## Medium-Priority Gaps

## 6) Missing rollout strategy for large API surface changes

### Gap
Plan has strong end-state design but lacks explicit phased rollout controls for:
- `storage/__init__.py` and `storage/io.py` facade removal,
- `storage/deltalake/delta.py` compatibility shim removal,
- model unification and converter deletion.

### Recommendation
Add a release-sequenced migration matrix:
1. Introduce new APIs + wrappers.
2. Emit deprecations + telemetry on old API usage.
3. Remove old paths after one compatibility cycle.

Include success criteria per phase.

---

## 7) Missing explicit “no behavior change” contract for monolith split

### Gap
Decomposition plan does not specify verification for behavioral equivalence.

### Recommendation
Add mandatory parity checks during split:
- before/after snapshot outputs for key operations,
- before/after diagnostics payload shape checks,
- before/after plan artifact determinism hashes for representative workflows.

---

## 8) Missing failure taxonomy for control-plane invocation

### Gap
Plan identifies broad exception handling issues but does not define final typed error contract.

### Recommendation
Define canonical error classes and mapping table:
- module unavailable,
- entrypoint unavailable,
- ctx adaptation failure,
- provider build failure,
- protocol compatibility failure,
- commit conflict/fallback outcomes.

This should feed both diagnostics and user-facing errors.

---

## 9) Missing deterministic schema/versioning policy for new typed payloads

### Gap
Typed payload proposals (`DeltaScanConfigPayload`, `DeltaSnapshotPayload`, etc.) are good but do not specify versioning.

### Recommendation
- Add explicit schema version fields for persisted payload structs.
- Define additive-only policy and backwards decode behavior (`StructBaseCompat`) with tests.

---

## 10) Missing stateful migration plan for CDF type unification

### Gap
Unifying `DeltaCdfOptions` + `DeltaCdfOptionsSpec` is correct direction, but plan doesn’t define transition for callsites expecting list vs tuple semantics.

### Recommendation
- Add normalization layer during migration (`tuple` canonical, list accepted temporarily at boundary).
- Add explicit typing and serialization parity tests to avoid latent behavioral drift.

---

## Additional Opportunities for Improvement

## 11) Add objective acceptance metrics for “best-in-class” claims

Current plan is mostly structural. Add measurable targets:
- p50/p95 provider registration latency,
- pushdown classification coverage rate,
- fallback usage rate under strict profile,
- deterministic artifact hash stability rate across reruns,
- conflict-resolution success metrics under concurrent write tests.

---

## 12) Add observability schema governance

The plan correctly prefers typed observability artifacts. Add:
- central schema registry entry for new artifact structs,
- schema hash assertions in tests,
- backward-compatible parser tests for historic artifacts.

---

## 13) Add dependency/risk gates between proposed refactors

Some changes should not be parallelized blindly. Add dependency graph:
- typed payload introduction before monolith extraction,
- compatibility wrappers before API removals,
- deprecation telemetry before hard delete.

This reduces branch-level integration risk.

---

## 14) Add external-consumer impact assessment for re-export cleanup

Before removing `storage`/`storage.io`/`storage.deltalake` exports, add a “consumer impact” checkpoint:
- internal CQ proof is insufficient for external users,
- require release note + deprecation window + explicit migration examples.

---

## Suggested Plan Refinements (Decision-Complete Additions)

1. Add a **Compatibility & Deprecation Table** for every delete/consolidate candidate:
   - internal usage,
   - exported status,
   - external risk,
   - deprecate version,
   - remove version.

2. Add a **Refactor Sequencing DAG** with hard prerequisites.

3. Add a **Behavioral Parity Checklist** for monolith split and model unification.

4. Add a **Typed Error Contract Appendix** for control-plane and write-path outcomes.

5. Add a **Performance/Determinism Acceptance Matrix** with measurable pass/fail thresholds.

---

## Final Assessment

The consolidated review is directionally correct and high quality. With the corrections above, it can become implementation-ready without leaving risky ambiguity around compatibility, sequencing, and behavioral parity.

Most important next improvements for the author:
- downgrade a few “delete-ready” claims to staged deprecation where external compatibility is uncertain,
- add explicit migration and rollout controls,
- and define strict parity/error/versioning contracts alongside the structural refactor plan.
