# Diagnostics Telemetry Policy

## Objectives
- Provide actionable diagnostics with minimal data exposure.
- Ensure logs, spans, and metrics are bounded in size and cardinality.
- Allow production-safe defaults with explicit opt-in for verbose capture.

## Default Safety Controls
- Attribute limits are enforced via OpenTelemetry env vars:
  - `OTEL_ATTRIBUTE_COUNT_LIMIT`
  - `OTEL_ATTRIBUTE_VALUE_LENGTH_LIMIT`
  - `OTEL_LOGRECORD_ATTRIBUTE_COUNT_LIMIT`
  - `OTEL_LOGRECORD_ATTRIBUTE_VALUE_LENGTH_LIMIT`
- Diagnostics payloads cap list/dict sizes:
  - `CODEANATOMY_OTEL_MAX_LIST_LENGTH` (default: 50)
  - `CODEANATOMY_OTEL_MAX_DICT_LENGTH` (default: 50)
- Sensitive keys are redacted using:
  - `CODEANATOMY_OTEL_REDACT_KEYS` (default includes `authorization`, `cookie`, `password`, `secret`, `token`, `api_key`)

## Hamilton Telemetry Guardrails
- Telemetry profiles default to production-safe capture:
  - `CODEANATOMY_HAMILTON_TELEMETRY_PROFILE` (`prod`, `ci`, `dev`)
- Explicit overrides:
  - `CODEANATOMY_HAMILTON_CAPTURE_DATA_STATISTICS`
  - `CODEANATOMY_HAMILTON_MAX_LIST_LENGTH_CAPTURE`
  - `CODEANATOMY_HAMILTON_MAX_DICT_LENGTH_CAPTURE`

## Operational Guidance
- Keep diagnostics enabled in CI with reduced capture limits.
- Do not log raw source code or repository contents.
- Prefer high-level counts, hashes, and fingerprints over raw payloads.
- When debugging, increase limits locally rather than in shared environments.

## Change Control
- Any new diagnostics event must include an explicit event name and be reviewed
  for payload size and sensitivity.
- High-cardinality attributes must be gated behind debug/config flags.

## Severity + Category Taxonomy
All diagnostics events must include:
- `diagnostic.severity`: `info` | `warn` | `error`
- `diagnostic.category`: stable category string (e.g., `datafusion_provider`, `delta_protocol`)

**Severity guidance**
- `info`: expected/healthy telemetry (baseline observations).
- `warn`: degraded behavior or fallbacks that do not halt the build.
- `error`: correctness risk or protocol incompatibility requiring action.

**Category guidance**
- Prefer system-aligned categories (provider mode, delta protocol, plan phases, scan pruning).
- Keep categories stable for aggregation across runs.
