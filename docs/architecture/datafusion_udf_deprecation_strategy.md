# DataFusion UDF Deprecation Strategy

## Goals
- Provide a predictable lifecycle for UDF changes across native, plugin, and fallback paths.
- Preserve query stability while allowing iterative improvements.
- Keep UDF snapshots, information_schema, and diagnostics in sync during transitions.

## Scope
- Applies to all CodeAnatomy UDFs (scalar, aggregate, window, and table UDFs).
- Covers native Rust, plugin (FFI), and Python fallback registrations.

## Deprecation Levels
### 1) Soft Deprecation
- The UDF remains fully functional but is flagged in documentation and diagnostics.
- A replacement UDF is available and documented.
- Snapshot metadata includes a rewrite tag or note indicating the preferred replacement.

### 2) Hard Deprecation
- The UDF remains available but emits a warning (or diagnostics event) when invoked.
- The replacement is considered the default in docs and examples.
- Configuration flags may be used to disallow hard-deprecated UDFs in strict modes.

### 3) Removal
- The UDF is removed from registration in native, plugin, and fallback paths.
- Snapshots and information_schema no longer expose the removed UDF.
- Any compatibility aliases are removed at the same time.

## Compatibility Window
- Soft deprecation: minimum 2 minor releases.
- Hard deprecation: minimum 1 minor release after soft deprecation.
- Removal: no earlier than 1 minor release after hard deprecation.

## Snapshot and Alias Behavior
- During soft/hard deprecation, snapshots continue to list the UDF.
- If a replacement exists, add a rewrite tag (or alias) to indicate the canonical function.
- Aliases must be symmetrical across native, plugin, and fallback paths.

## Diagnostics and Observability
- Record deprecation status in diagnostics payloads when possible.
- Emit a warning event on first invocation of a hard-deprecated UDF per session.
- Ensure snapshot hashes change when deprecation state changes.

## Removal Policy
- Removal occurs only after the compatibility window completes.
- Remove from:
  - Native Rust registration
  - Plugin bundle
  - Python fallback registry
  - UDF docs and examples
- Update conformance tests to reflect the removal.

## Implementation Checklist
- [ ] Add deprecation metadata to UDF registry docs.
- [ ] Add rewrite tags or aliases for preferred replacements.
- [ ] Update snapshots and information_schema parity tests.
- [ ] Ensure fallback snapshot and registration remain aligned.
- [ ] Emit diagnostics for hard-deprecated usage.
- [ ] Remove UDF and aliases after compatibility window.
