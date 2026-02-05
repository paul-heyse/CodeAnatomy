# Rust UDF Quality Gate

> **Document Type**: Architecture Guidance
> **Status**: Active
> **Last Updated**: 2026-02-05

This document defines the **minimum correctness and performance invariants** for
Rust UDFs (scalar, aggregate, window, and table) in `rust/datafusion_ext`. The
quality gate is enforced by conformance tests and code review checklists.

---

## Why This Exists

Rust UDFs run inside the DataFusion execution engine. A single invalid UDF can
break correctness (row counts, nullability, schema metadata) or crash execution.
The quality gate ensures **predictable semantics**, **safe execution**, and
**consistent metadata** across all UDFs.

---

## Required Invariants

### 1) Row-count invariants (scalar UDFs)
- **Scalar UDFs must return arrays with the same row count** as their input batch.
- Prefer `to_array_of_size` when scalar expansion is needed.

### 2) Metadata-aware output fields
- If output fields depend on input metadata (e.g., struct field names), implement
  `return_field_from_args` and use the provided `ReturnFieldArgs`.
- Avoid hard-coded output `Field` definitions when metadata is required.

### 3) Short-circuit correctness
- If a UDF can short-circuit on nulls or constants, implement `short_circuits()`
  and validate behavior in conformance tests.

### 4) Simplify/coerce invariants
- UDFs that accept multiple input types must implement `coerce_types`.
- UDFs with deterministic behavior should implement `simplify` where applicable.

### 5) Named arguments
- Any UDF that accepts named args must register parameter names in the snapshot
  and be validated against `information_schema` parity tests.

---

## Conformance Test Expectations

Conformance tests in `rust/datafusion_ext/tests/udf_conformance.rs` are expected to:
- Validate row-count invariants for scalar UDFs under representative inputs.
- Validate `return_field_from_args` usage when output metadata depends on input.
- Verify `short_circuits`, `simplify`, and `coerce_types` metadata are populated
  and behave correctly for listed UDFs.
- Ensure `information_schema` parity for all public UDFs.

---

## Code Review Checklist

- [ ] Scalar UDF output length == input batch length
- [ ] `return_field_from_args` used when output metadata depends on input
- [ ] `short_circuits` implemented and tested when applicable
- [ ] `coerce_types` implemented for multi-typed inputs
- [ ] `simplify` implemented for deterministic expressions
- [ ] Snapshot metadata updated and parity tests pass

---

## References

- `rust/datafusion_ext/tests/udf_conformance.rs`
- `rust/datafusion_ext/src/registry_snapshot.rs`
- `docs/architecture/architectural_unification_deep_dive.md`
