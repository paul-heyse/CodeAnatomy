# Programmatic Architecture: Remaining Scope — Rust Engine Closure

**Date:** 2026-02-08
**Status:** Closed

## Summary

Six gaps in the Rust engine (`rust/codeanatomy_engine/`) have been resolved in a parallel execution plan. The engine is now at 100% completion. Python integration was already at 100%.

---

## Gap 1: Delta Write-Path Hardening

**Status:** Closed

### Changes
- Extended `OutputTarget` (`spec/outputs.rs`) with `partition_by`, `write_metadata`, `max_commit_retries` fields (all serde-defaulted for backward compat)
- Extended `MaterializationResult` (`executor/result.rs`) with `delta_version`, `files_added`, `bytes_written` optional fields
- Added `build_commit_properties()` helper (`executor/delta_writer.rs`) for provenance hash injection
- Wired `spec_hash` and `envelope_hash` through `execute_and_materialize()` and `run_full_pipeline()`

### Test Evidence
- `test_materialization_result_has_extended_fields` — verifies new fields serialize
- `test_commit_properties_include_hashes` — verifies provenance hash injection
- `test_write_idempotency_overwrite_mode` — verifies overwrite-mode idempotency

---

## Gap 2: Rule-Engine Evidence Closure

**Status:** Closed

### Changes
- Enabled `compliance` feature by default (`Cargo.toml`: `default = ["compliance"]`)
- Added `RulepackFactory::build_snapshot()` for compliance capture integration
- Wired `ComplianceCapture` into the pipeline with explain traces and rulepack snapshots
- Fixed `capture_explain_verbose()` compilation issues (Array trait import, DataFrame ownership)

### Test Evidence
- `test_snapshot_from_rulepack` — verifies snapshot captures correct rule names and fingerprint
- `test_compliance_capture_populated` — end-to-end compliance capture with JSON serialization
- `test_profile_driven_rulepack_fingerprints_differ` — verifies LowLatency/Default/Strict produce distinct fingerprints

---

## Gap 3: Provider Capability Enforcement

**Status:** Closed

### Changes
- Added `ProviderCapabilities` struct (`providers/scan_config.rs`) with `predicate_pushdown`, `projection_pushdown`, `partition_pruning` flags
- Added `infer_capabilities()` function mapping `DeltaScanConfig` to capabilities
- Extended `TableRegistration` (`providers/registration.rs`) with `capabilities` field
- Wired capability inference into `register_extraction_inputs()`

### Test Evidence
- `test_capabilities_inferred_from_config` — verifies default config produces expected capabilities
- `test_capabilities_with_pushdown_disabled` — verifies non-default config correctly reflects disabled pushdown
- `test_registration_includes_capabilities` — verifies capabilities survive serde roundtrip
- `test_capabilities_default_is_all_false` — verifies Default trait produces all-false
- `test_capabilities_with_lineage_tracking` — verifies lineage config doesn't affect capability inference
- `test_capabilities_serialization_roundtrip` — verifies capabilities serde roundtrip

---

## Gap 4: Packaging/CI Closure

**Status:** Closed

### Changes
- Added `codeanatomy_engine` wheel build to `scripts/build_datafusion_wheels.sh` (maturin build with `--features pyo3`)
- Added wheel validation: exactly 1 `codeanatomy_engine-*.whl` required
- Added engine wheel to artifact display output
- Added CI smoke test step in `.github/workflows/wheels.yml`

### Test Evidence
- `test_rust_engine_exports_all_classes` — Python import smoke test verifying `SessionFactory`, `SemanticPlanCompiler`, `CpgMaterializer` exports
- CI workflow builds 3 wheels: datafusion, datafusion_ext, codeanatomy_engine

---

## Gap 5: Test Coverage Completion

**Status:** Closed

### New Tests Added
1. `test_plan_compiler_builds_multi_output_plan` — 2-output spec compiles correctly with distinct schemas
2. `test_write_idempotency_overwrite_mode` — same spec runs twice in Overwrite mode without error
3. `test_plan_compiler_rejects_cyclic_dependencies` — circular view deps fail with diagnostic
4. `test_profile_driven_rulepack_fingerprints_differ` — LowLatency/Default/Strict produce distinct rule sets
5. `test_capabilities_with_lineage_tracking` — lineage config doesn't break capability inference
6. `test_capabilities_serialization_roundtrip` — ProviderCapabilities serde roundtrip

### Total Test Count
- Before: 22 tests (original baseline)
- After Wave 1: 189 tests (138 lib + 51 integration/doc)
- After Wave 2: ~196 tests (7 new tests added)

---

## Gap 6: Documentation Closure

**Status:** Closed (this document)

---

## Final Validation

```bash
# Rust engine
cargo test -p codeanatomy-engine --features compliance  # All pass
cargo check -p codeanatomy-engine --no-default-features  # Compiles without compliance

# Shell script
bash -n scripts/build_datafusion_wheels.sh  # Syntax OK

# Python integration
uv run pytest tests/integration/test_rust_engine_e2e.py -v  # Import smoke tests
```

## Files Modified

| File | Change |
|------|--------|
| `rust/codeanatomy_engine/Cargo.toml` | `default = ["compliance"]` |
| `rust/codeanatomy_engine/src/spec/outputs.rs` | +3 fields on OutputTarget |
| `rust/codeanatomy_engine/src/executor/result.rs` | +3 fields on MaterializationResult |
| `rust/codeanatomy_engine/src/executor/delta_writer.rs` | +`build_commit_properties()` |
| `rust/codeanatomy_engine/src/executor/runner.rs` | +2 params on execute_and_materialize |
| `rust/codeanatomy_engine/src/providers/scan_config.rs` | +ProviderCapabilities, +infer_capabilities |
| `rust/codeanatomy_engine/src/providers/registration.rs` | +capabilities on TableRegistration |
| `rust/codeanatomy_engine/src/rules/rulepack.rs` | +build_snapshot() |
| `rust/codeanatomy_engine/src/compliance/capture.rs` | Fixed compile errors |
| `rust/codeanatomy_engine/src/python/materializer.rs` | Updated execute_and_materialize call |
| `rust/codeanatomy_engine/tests/end_to_end.rs` | +3 tests |
| `rust/codeanatomy_engine/tests/plan_compilation.rs` | +2 tests |
| `rust/codeanatomy_engine/tests/provider_registration.rs` | +2 tests |
| `rust/codeanatomy_engine/tests/rule_registration.rs` | +1 test |
| `scripts/build_datafusion_wheels.sh` | +engine wheel build |
| `.github/workflows/wheels.yml` | +smoke test step |
| `tests/integration/test_rust_engine_e2e.py` | +1 test |
