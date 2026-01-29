# msgspec encode_into Micro-Benchmark Notes

Date: 2026-01-29

## Context
We use `msgspec.json.Encoder.encode_into` on high-volume artifact paths to reduce
allocations while preserving deterministic ordering. The pattern is intentionally
simple and avoids per-call encoder construction.

## Pattern
```python
buffer = bytearray()
JSON_ENCODER.encode_into(payload, buffer)
```

## Guidance
- Reuse the module-level encoder from `serde_msgspec`.
- Prefer `encode_into` when hashing or serializing many small payloads.
- Keep deterministic ordering via the shared encoder settings.
