# UUID6/UUID7 Best-in-Class Migration Plan (Design Phase, Breaking Changes OK)

Date: 2026-01-29
Owner: Codex (design phase)
Status: complete (scope 8 deferred)
Last reviewed: 2026-01-30

## Purpose
Define and implement a best-in-class migration from ad-hoc `uuid4()` usage to time-ordered UUIDs (v7 default, v6 only when v1-compat is required). This plan standardizes ID generation, enforces concurrency-safe monotonicity, improves database locality, and introduces missing run/session identifiers where they add operational value. Security-adjacent identifiers remain CSPRNG-backed and explicitly non-time-ordered.

## Design Principles
- **v7 as the default**: Use UUIDv7 for all sortable IDs (run_id, temp table names, artifact identifiers) unless there is a v1-compat requirement.
- **Explicit security posture**: Preserve or replace `uuid4()` with CSPRNG helpers for security-adjacent IDs; do not use time-ordered UUIDs for externally visible or security-sensitive tokens.
- **Centralized generation**: One module owns UUID creation, format, and monotonic/thread-safe behavior.
- **Compatibility shim**: Use stdlib `uuid.uuid7()` on Python 3.14+, otherwise fallback to `uuid6.uuid7()`.
- **Monotonicity under concurrency**: Guard v7 generation with a process-global lock to preserve ordering invariants and avoid races.
- **Breaking changes OK**: Remove legacy `uuid4()` usage in internal IDs even if this changes ordering or representation.

---

## Scope 1 — Centralized UUID generation module (v7 default, v6 optional)

### Objective
Introduce a single, typed module that generates UUIDs with a stable, documented contract for string/hex outputs, thread-safe monotonicity, and security-token exceptions.

### Representative code pattern
```python
from __future__ import annotations

import threading
import uuid
from typing import Final

try:
    import uuid6 as uuid6_pkg
except Exception:  # pragma: no cover
    uuid6_pkg = None

_UUID_LOCK: Final[threading.Lock] = threading.Lock()


def uuid7() -> uuid.UUID:
    """Return a time-ordered UUIDv7 (thread-safe, monotone)."""
    with _UUID_LOCK:
        if hasattr(uuid, "uuid7"):
            return uuid.uuid7()
        if uuid6_pkg is None:
            raise RuntimeError("uuid7 requires Python 3.14+ or uuid6 package")
        return uuid6_pkg.uuid7()


def uuid7_str() -> str:
    return str(uuid7())


def uuid7_hex() -> str:
    return uuid7().hex


def uuid7_suffix(length: int = 12) -> str:
    """Return a short suffix from the random tail (avoid time prefix leakage)."""
    if length <= 0:
        raise ValueError("length must be positive")
    return uuid7().hex[-length:]
```

### Target files to modify
- `src/utils/uuid_factory.py` (new)
- `src/utils/__init__.py` (export helper)

### Modules to delete
- None.

### Implementation checklist
- [x] Create `uuid_factory` (or similarly named) module with v7 helpers.
- [x] Use stdlib `uuid.uuid7()` when available; fallback to `uuid6.uuid7()`.
- [x] Add process-global lock to preserve monotonicity under concurrency.
- [x] Add `uuid7_suffix()` for safe short IDs (avoid time prefix truncation).
- [x] Add explicit CSPRNG helper for security tokens (`secrets.token_*`).

---

## Scope 2 — Run ID generation and run-scoped identifiers → UUIDv7

### Objective
Make all run identifiers time-ordered to improve log grouping, timeline correlation, and write ordering in Delta/diagnostics artifacts.

### Representative code pattern
```python
from utils.uuid_factory import uuid7_str

run_id = execute_overrides.get("run_id")
if not isinstance(run_id, str) or not run_id:
    run_id = uuid7_str()
    execute_overrides["run_id"] = run_id
```

### Target files to modify
- `src/graph/product_build.py`
- `src/hamilton_pipeline/execution.py`
- `src/hamilton_pipeline/modules/outputs.py`
- `src/obs/datafusion_runs.py`
- `src/hamilton_pipeline/modules/execution_plan.py` (any default run_id paths)

### Modules to delete
- Remove `from uuid import uuid4` imports in the above files.
- Remove inline `str(uuid.uuid4())` / `str(uuid4())` usages for run_id.

### Implementation checklist
- [x] Replace default run_id generation with `uuid7_str()`.
- [x] Ensure all run_id values remain strings (not UUID objects).
- [x] Confirm OTel run_context flows unchanged (string remains valid).
- [x] Update any tests that assert UUIDv4 format.

---

## Scope 3 — Temp table/view names → UUIDv7 hex

### Objective
Standardize temp table/view names to use time-ordered hex suffixes for better ordering and operational predictability without exposing security-sensitive tokens.

### Representative code pattern
```python
from utils.uuid_factory import uuid7_hex

name = f"__temp_{uuid7_hex()}"
```

### Target files to modify
- `scripts/delta_export_snapshot.py`
- `scripts/migrate_parquet_to_delta.py`
- `scripts/e2e_diagnostics_report.py`
- `src/engine/materialize_pipeline.py`
- `src/extract/worklists.py`
- `src/incremental/changes.py`
- `src/incremental/deltas.py`
- `src/incremental/runtime.py`
- `src/incremental/cdf_runtime.py`
- `src/incremental/write_helpers.py`
- `src/incremental/impact.py`
- `src/storage/deltalake/file_pruning.py`
- `src/datafusion_engine/finalize.py`
- `src/datafusion_engine/session_helpers.py`
- `src/datafusion_engine/encoding.py`
- `src/datafusion_engine/view_registry.py`
- `src/datafusion_engine/view_graph_registry.py`
- `src/hamilton_pipeline/modules/params.py`
- `src/hamilton_pipeline/modules/outputs.py`
- `rust/datafusion_python/python/datafusion/context.py`

### Modules to delete
- Remove `from uuid import uuid4` imports in the above files.
- Remove inline `uuid.uuid4()` / `uuid4()` suffix concatenations.

### Implementation checklist
- [x] Replace all temp-name uuid4 usage with `uuid7_hex()`.
- [x] Ensure table/view names remain valid identifiers (no hyphens).
- [x] Keep prefixes unchanged for compatibility with any filters.
- [x] Update any tests that pattern-match temp table names.

---

## Scope 4 — Short identifiers (truncate-safe, random-tail only)

### Objective
Replace truncated `uuid4().hex[:N]` with a safer short-ID helper that does not reveal time prefixes and preserves randomness.

### Representative code pattern
```python
from utils.uuid_factory import uuid7_suffix

scope_key = uuid7_suffix(12)  # random tail only
```

### Target files to modify
- `src/datafusion_engine/param_tables.py` (replace `_new_scope_key`)

### Modules to delete
- Remove any inline `uuid.uuid4().hex[:N]` truncations (centralize in helper).

### Implementation checklist
- [x] Replace `_new_scope_key()` with `uuid7_suffix(12)`.
- [x] Ensure scope keys remain ASCII + filesystem safe.
- [x] Confirm no code assumes `uuid4` specifically for scope keys.

---

## Scope 5 — New UUIDv7 for diagnostics session/operation IDs

### Objective
Introduce time-ordered session and operation identifiers where today we use static strings or omit unique IDs. This improves traceability across diagnostics payloads without changing schema shape.

### Representative code pattern
```python
from utils.uuid_factory import uuid7_str

class DiagnosticsContext:
    session_id: str = field(default_factory=uuid7_str)
    operation_id: str = field(default_factory=uuid7_str)
```

### Target files to modify
- `src/datafusion_engine/diagnostics.py` (default `session_id` and `operation_id`)
- `src/datafusion_engine/runtime.py` (default session_id usage)
- `src/obs/diagnostics.py` (default session_id usage)
- `src/datafusion_engine/diagnostics.py` (recorder_for_profile default session_id override)

### Modules to delete
- Remove hard-coded `session_id="default"`, `"runtime"`, `"obs"` defaults where safe.

### Implementation checklist
- [x] Generate session_id/operation_id via `uuid7_str()` if not provided.
- [x] Preserve explicit session_id overrides from callers.
- [x] Ensure schema fields remain `str` and stable across serialization.
- [x] Update tests or diagnostics snapshots that expect static IDs.

---

## Scope 6 — Security-adjacent identifiers remain CSPRNG (explicit)

### Objective
Make security posture explicit and prevent accidental substitution of time-ordered IDs in security-sensitive contexts.

### Representative code pattern
```python
import secrets

def secure_token_hex(length: int = 16) -> str:
    return secrets.token_hex(length)
```

### Target files to modify
- `src/obs/otel/resource_detectors.py` (keep CSPRNG, prefer helper)

### Modules to delete
- None (retain CSPRNG generation, but move to helper if used in multiple places).

### Implementation checklist
- [x] Keep `service.instance.id` CSPRNG-backed (do not use uuid7).
- [x] Add `secure_token_hex()` helper for future security-sensitive usage.
- [x] Document “no uuid7 for external/public/security tokens”.

---

## Scope 7 — Tests, linting, and enforcement guardrails

### Objective
Ensure repository-wide enforcement that time-ordered UUIDs are used for internal IDs and uuid4 usage is restricted to approved security contexts.

### Representative code pattern
```python
# Example: test that uuid7 helper is monotone in single-threaded calls
values = [uuid7().int for _ in range(1000)]
assert values == sorted(values)
```

### Target files to modify
- `tests/` (add new unit tests for uuid7 helper)
- `docs/python_library_reference/uuid6.md` (optional note linking project policy)

### Modules to delete
- Remove obsolete uuid4-focused tests if they only verify v4 format.

### Implementation checklist
- [x] Add helper unit tests (monotonic order, hex/str formatting, suffix randomness).
- [x] Add lint checks or a grep-based test to prevent new `uuid4()` usage.
- [x] Allowlist only security-related uuid4/secrets usage.

---

## Scope 8 — Deferred deletions (after all scopes complete)

### Objective
Clean up remaining compatibility or transitional code once all call sites are migrated.

Status: deferred until Python 3.14+ is the minimum supported runtime.

### Deferred deletions
- Any remaining `from uuid import uuid4` imports outside explicit security contexts.
- Temporary shim logic if project standardizes on Python 3.14+ and removes the `uuid6` dependency.
- Legacy helper functions that only wrap uuid4 or generate short IDs by truncation.

### Target files to modify
- `pyproject.toml` / lockfiles if removing `uuid6` after a 3.14-only baseline.
- `src/utils/uuid_factory.py` (remove fallback logic once stdlib-only).

### Modules to delete
- `uuid6` dependency (only when Python 3.14+ is enforced).

### Implementation checklist
- [ ] Confirm no code path relies on `uuid6` before removal.
- [ ] Ensure `uuid.uuid7` is available in all supported runtimes.
- [ ] Remove fallback imports + adjust tests accordingly.

---

## Global Implementation Checklist
- [x] Introduce UUID factory module with v7 + CSPRNG helpers.
- [x] Replace run_id generation with uuid7 across pipeline/obs layers.
- [x] Replace temp table/view names with uuid7 hex suffixes.
- [x] Replace truncated uuid4 usage with uuid7 tail helper.
- [x] Add new session/operation IDs where missing.
- [x] Preserve CSPRNG for security-sensitive identifiers.
- [x] Add tests + guardrails to prevent regression.
- [x] Final sweep: remove legacy uuid4 imports and reconcile docs.

---

## Completion Notes
- Scopes 1–7 are fully implemented in the codebase (helpers, replacements, diagnostics IDs, and guardrail tests).
- Optional doc note in `docs/python_library_reference/uuid6.md` was not required and remains unchanged.
- Scope 8 is intentionally deferred pending a Python 3.14+ baseline so the `uuid6` fallback can be removed safely.
