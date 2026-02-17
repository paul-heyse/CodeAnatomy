# Determinism Contract

This document is the normative source for cross-language execution identity.

## Identity Layers

1. `spec_hash`: hash of the semantic execution spec payload.
2. `envelope_hash`: hash of captured session/planning/provider identity inputs.
3. `rulepack_fingerprint`: hash of the effective rule set.

Replay validity requires all three layers to match.

## Golden Fixture

Shared fixture path:

- `tests/msgspec_contract/goldens/session_identity_contract.json`

Consumers:

- Rust golden test: `rust/codeanatomy_engine/tests/session_identity_golden.rs`
- Python golden test: `tests/unit/datafusion_engine/session/test_identity_contract_golden.py`

## Nondeterministic Fields

The following fields are explicitly excluded from identity matching:

- Wall-clock timestamps (for example `started_at`, `completed_at`)
- Volatile runtime diagnostics that do not affect plan semantics
