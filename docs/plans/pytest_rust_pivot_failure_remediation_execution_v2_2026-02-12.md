# Pytest Rust Pivot Failure Remediation Execution Ledger (v2)

Date: 2026-02-12
Owner: Codex

## Baseline CQ Artifacts

- `_install_udf_platform` calls:
  - `.cq/artifacts/calls_20260212_050955_019c5041-7e8d-7f0a-a23a-1b276b56989b.json`
- `register_cache_introspection_functions` calls:
  - `.cq/artifacts/calls_20260212_050955_019c5041-7ebb-7731-8f79-60ad536d3243.json`
  - `.cq/artifacts/calls_20260212_051305_019c5044-648e-71d8-9ca7-29ddae3b2708.json`
- `_install_codeanatomy_runtime_snapshot` calls:
  - `.cq/artifacts/calls_20260212_051305_019c5044-63de-7e80-9086-541826825aa1.json`
- `_datafusion_internal` calls:
  - `.cq/artifacts/calls_20260212_051305_019c5044-63e6-7ddb-8667-6c4eeb6b9194.json`
- `install_rust_udf_platform` calls:
  - `.cq/artifacts/calls_20260212_051305_019c5044-6593-760e-b175-06fc36e68bb7.json`
- `native_udf_platform_available` calls:
  - `.cq/artifacts/calls_20260212_051305_019c5044-6459-755e-bb56-84c123db3846.json`
- `compile_with_warnings` calls (Rust references):
  - `.cq/artifacts/calls_20260212_051329_019c5044-c0d9-7f55-a2c4-0a79e713a716.json`
- `register_table` search in Rust compiler:
  - `.cq/artifacts/search_20260212_051211_019c5043-907d-7e8d-9708-bbfb158329c0.json`

## Wave Tracking

### Wave 0: Baseline + Guardrails

- [x] Baseline CQ artifacts captured for failure-driving symbols.
- [x] Runtime contract target locked to v3 payload semantics.
- [x] Hard migration assumption retained: no Python fallback path.

### Wave 1: Runtime Front Door + UDF/FnFactory hardening

- [x] Python runtime install path now accepts either unified Rust entrypoint or strict modular Rust contract.
- [x] Runtime payload normalized to v3 contract fields (`contract_version`, install flags, install mode).
- [x] FunctionFactory policy derivation now requires Rust derivation entrypoint.
- [x] Session/facade/runtime consumers now gate strict mode from capability snapshots instead of dynamic probing helper.

### Wave 2: Cache Introspection Direct Contract

- [x] Unit cache registration tests migrated off removed context-adaptation seam.
- [x] Direct `register_cache_tables(ctx, payload)` contract asserted in unit tests.

### Wave 3: Idempotent Rust table registration

- [x] Added shared Rust helper for register-or-replace semantics.
- [x] Plan compiler view registration switched to idempotent helper.
- [x] Cache-boundary re-registration switched to idempotent helper.

### Wave 4: Test contract realignment

- [x] UDF optional-deps helper accepts unified or modular runtime contracts.
- [x] Capability tests accept unified or modular runtime and assert runtime contract v3 metadata.
- [x] Fallback snapshot test aligned to strict import-time failure behavior.
- [x] Rust e2e tests no longer xfail on `already exists` replay behavior.

## Remaining Validation

- [ ] Run full quality gate once after all code edits:
  - `uv run ruff format && uv run ruff check --fix && uv run pyrefly check && uv run pyright && uv run pytest -q`
- [ ] Run CQ zero-hit/removal bundle for decommission symbols.
- [ ] Update this ledger with final pass/fail status and any deferred scope.
