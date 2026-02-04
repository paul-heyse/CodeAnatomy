# Fingerprinting Schema Evolution

This document describes how configuration and plan fingerprints should evolve
without breaking cache stability or diagnostics.

## Principles
- Include an explicit `version` in fingerprint payloads that may change.
- Compose hashes from stable sub-components instead of re-serializing large
  objects.
- Keep payloads JSON-compatible and deterministic (sorted keys, stable order).

## Composite Fingerprints
Use `CompositeFingerprint` when multiple components contribute to a cache key
or diagnostic identity. Each component should be a stable hash string, and the
composite should carry a version number.

## Truncation Policy
Plan fingerprints use a 32-hex-character (128-bit) truncation for readability.
Truncation should be applied consistently at the point of hashing, not after
cache key construction.

## Cache Key Compatibility
When changing cache key construction, provide dual-read compatibility for the
legacy key shape until old cache entries are fully retired.

## Migration Checklist
- Add a new `version` value for the payload.
- Keep the old `version` stable for existing entries.
- Update documentation and tests to reflect the new schema.
