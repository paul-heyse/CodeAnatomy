# Integration Test Error Analysis

**Date:** 2026-02-05
**Source:** `docs/plans/integration_testing_proposal.md` (v5)
**Test run:** 94 passed, 2 failed, 0 errors, 0 skipped

---

## 1. Summary

All 11 integration test files (96 test methods) were implemented per the
integration testing proposal. Two tests fail due to an exception-type mismatch
between what the test expects and what the production code raises.

| File | Tests | Pass | Fail |
|------|-------|------|------|
| `contracts/test_immutability_contracts.py` | 12 | 12 | 0 |
| `error_handling/test_evidence_plan_gating.py` | 13 | 13 | 0 |
| `error_handling/test_extract_postprocess_resilience.py` | 10 | 10 | 0 |
| `adapters/test_semantic_runtime_bridge_integration.py` | 12 | 12 | 0 |
| `scheduling/test_schedule_generation.py` | 15 | 15 | 0 |
| `boundaries/test_evidence_plan_to_extractor.py` | 8 | 8 | 0 |
| `boundaries/test_plan_determinism.py` | 7 | 7 | 0 |
| `runtime/test_cache_introspection_capabilities.py` | 4 | 4 | 0 |
| `runtime/test_delta_provider_panic_containment.py` | 3 | 1 | 2 |
| `runtime/test_object_store_registration_contracts.py` | 8 | 8 | 0 |
| `runtime/test_capability_negotiation.py` | 5 | 5 | 0 |
| **Total** | **97** | **95** | **2** |

---

## 2. Failing Tests

### 2.1 `test_bad_uri_yields_structured_error`

**File:** `tests/integration/runtime/test_delta_provider_panic_containment.py:30`

**Expected behavior:** Given an invalid Delta table URI, `delta_provider_from_session()`
should raise a structured error (one of `RuntimeError`, `OSError`, `ValueError`).

**Actual behavior:** Raises `DataFusionEngineError` with message:
```
Delta control-plane extension is incompatible for this SessionContext. ctx_kind=fallback.
```

### 2.2 `test_corrupt_metadata_yields_structured_error`

**File:** `tests/integration/runtime/test_delta_provider_panic_containment.py:60`

**Expected behavior:** Given a Delta directory with corrupt metadata,
`delta_provider_from_session()` should raise a structured error.

**Actual behavior:** Same `DataFusionEngineError` as above. The test never reaches the
corrupt-metadata path because the context compatibility check fails first.

---

## 3. Root Cause Analysis

The failure involves a three-layer mismatch between skip logic, production code
strictness, and exception hierarchy.

### 3.1 Exception Hierarchy Gap

`DataFusionEngineError` (`src/datafusion_engine/errors.py:20`) inherits directly
from `Exception`, not from `RuntimeError`:

```
Exception
├── RuntimeError
├── OSError
├── ValueError
└── DataFusionEngineError   ← sibling, not subclass
```

The tests use `pytest.raises((RuntimeError, OSError, ValueError))` which does not
catch `DataFusionEngineError`.

### 3.2 Fallback Strictness Mismatch

The skip guard and the production code use different fallback policies:

| Component | Fallback behavior |
|-----------|-------------------|
| `require_delta_extension()` | Creates `SessionContext()` and runs `is_delta_extension_compatible()` which probes **all** candidates including `ctx_kind=fallback`. Reports compatible if any candidate works. |
| `delta_provider_from_session()` | Calls `_internal_ctx(ctx, allow_fallback=False)`. Explicitly **rejects** the fallback context kind. |

**Call chain when `ctx_kind=fallback`:**

1. `require_delta_extension()` probes `is_delta_extension_compatible(SessionContext())`
2. Probe finds `ctx_kind=fallback` via `delta_context_candidates()` (outer and internal fail, fallback succeeds)
3. Returns `DeltaExtensionCompatibility(available=True, compatible=True, ctx_kind="fallback")`
4. `require_delta_extension()` sees `compatible=True` and does **not** skip
5. Test proceeds to call `delta_provider_from_session(df_ctx(), request=...)`
6. Inside, `_internal_ctx(ctx, allow_fallback=False)` is called
7. `compatibility.ctx_kind == "fallback"` but `allow_fallback` is `False` so the fallback branch at line 526 is skipped
8. Falls through to line 540: raises `DataFusionEngineError` with `kind=ErrorKind.PLUGIN`

**Result:** The skip guard passes but the production call fails at context negotiation,
never reaching the intended bad-URI or corrupt-metadata error path.

### 3.3 Context Source Mismatch

An additional factor: `require_delta_extension()` creates a bare `SessionContext()`
while the test uses `df_ctx()` which goes through `DataFusionRuntimeProfile().session_context()`.
These may produce differently-configured contexts, though in this case both yield
`ctx_kind=fallback`.

---

## 4. Suggested Corrections

### Option A: Fix the tests (recommended)

Two changes needed in `tests/integration/runtime/test_delta_provider_panic_containment.py`:

**A1. Strengthen the skip guard to detect fallback-only compatibility.**

Replace the current `require_delta_extension()` try/except pattern with an explicit
compatibility check that rejects `ctx_kind=fallback`, mirroring the production code's
`allow_fallback=False` behavior:

```python
def _require_non_fallback_delta_extension() -> None:
    """Skip test if Delta extension requires fallback context.

    delta_provider_from_session() calls _internal_ctx(ctx, allow_fallback=False),
    so tests must skip when the extension only works via fallback.
    """
    from datafusion_engine.delta.capabilities import is_delta_extension_compatible
    from tests.test_helpers.datafusion_runtime import df_ctx

    ctx = df_ctx()
    compat = is_delta_extension_compatible(ctx)
    if not compat.available:
        pytest.skip("Delta extension not available")
    if not compat.compatible:
        pytest.skip(f"Delta extension not compatible: {compat.error}")
    if compat.ctx_kind == "fallback":
        pytest.skip(
            "Delta extension only available via fallback context; "
            "delta_provider_from_session() requires non-fallback ctx"
        )
```

**A2. Include `DataFusionEngineError` in the expected exceptions as a safety net.**

Even with the improved skip guard, the `pytest.raises` tuple should include
`DataFusionEngineError` because the production code's documented error surface for
FFI failures includes this type:

```python
from datafusion_engine.errors import DataFusionEngineError

with pytest.raises((RuntimeError, OSError, ValueError, DataFusionEngineError)):
    delta_provider_from_session(ctx, request=request)
```

This aligns the test with the actual error contract: the test verifies that FFI
failures produce a *structured* exception (any of these four types) rather than an
uncaught panic.

### Option B: Adjust production code

If the intent is that `DataFusionEngineError` should be catchable as `RuntimeError`,
change the base class:

```python
# src/datafusion_engine/errors.py
class DataFusionEngineError(RuntimeError):
    """Base exception for DataFusion engine failures."""
    ...
```

**Trade-offs:**
- Makes all existing `except RuntimeError` blocks also catch engine errors (may be
  desirable for user-facing code that broadly catches runtime failures)
- Breaks any code that specifically catches `RuntimeError` but not engine errors
- Aligns with the proposal's language ("structured RuntimeError")
- Broader change with wider blast radius

### Option C: Expose fallback awareness in `require_delta_extension()`

Add a `require_non_fallback` parameter to the test helper:

```python
# tests/test_helpers/optional_deps.py
def require_delta_extension(
    *,
    require_non_fallback: bool = False,
) -> ModuleType:
    ...
    if require_non_fallback and compatibility.ctx_kind == "fallback":
        msg = "Delta extension only available via fallback context."
        raise RuntimeError(msg)
    return datafusion
```

This is a lighter-touch change to the test helpers that enables targeted skipping
without changing the production error hierarchy.

---

## 5. Recommendation

**Apply Option A (A1 + A2) as the primary fix.** Rationale:

1. The test skip guard should mirror the strictness of the production code it exercises.
   If `delta_provider_from_session()` rejects fallback contexts, the test must skip
   when only fallback is available.

2. Including `DataFusionEngineError` in the expected exceptions is accurate: the test's
   intent is to verify *structured* errors (not panics), and `DataFusionEngineError`
   with `kind=ErrorKind.PLUGIN` is a structured error.

3. This requires zero production code changes, keeping the blast radius confined to the
   test file.

**Consider Option C as a supplementary improvement** to make the skip logic reusable
across future Delta tests that call `allow_fallback=False` production paths.

**Option B should be evaluated independently** as a broader design decision about
whether `DataFusionEngineError` should participate in the `RuntimeError` hierarchy. This
is an architectural choice that affects all callers, not just these two tests.

---

## 6. Proposal Document Observations

The integration testing proposal (v5) describes the expected error as "Structured
RuntimeError with actionable diagnostics" (line 1632). This is inaccurate for the
current production code, which raises `DataFusionEngineError(Exception)` with
`kind=ErrorKind.PLUGIN`.

**Suggested proposal update:** Replace "Structured RuntimeError" with "Structured
exception (RuntimeError or DataFusionEngineError)" in Expansion 7 to reflect the
actual error surface of the Delta control-plane module.

---

## 7. Minor Diagnostics

These are non-blocking observations from the test implementation:

| File | Line | Observation |
|------|------|-------------|
| `test_schedule_generation.py` | 390 | `_name` assigned but unused in parametrized test |
| `test_evidence_plan_to_extractor.py` | 115, 142 | `_flag_name` unused in parametrized test |
| `test_delta_provider_panic_containment.py` | 30 | `tmp_path` parameter unused (test constructs URI inline) |

These are minor lint-level items (unused variables in parametrized destructuring)
and do not affect test correctness.
