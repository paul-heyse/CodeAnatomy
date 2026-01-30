# Test Helpers

This directory contains shared helper utilities used across the test suite to reduce duplication
and standardize setup patterns (DataFusion profiles, Arrow/Delta seeding, diagnostics wiring, and
optional dependency gating).

## Helper Modules

- `datafusion_runtime.py` — create `DataFusionRuntimeProfile` and `SessionContext` for tests.
- `arrow_seed.py` — register Arrow tables via `datafusion_from_arrow`.
- `delta_seed.py` — write Delta tables through `WritePipeline`.
- `diagnostics.py` — `DiagnosticsCollector` + runtime profile wiring.
- `optional_deps.py` — `pytest.importorskip` wrappers for optional dependencies.
- `plan_bundle.py` — helpers for building plan bundles from Arrow tables.

## Usage

```python
from tests.test_helpers.datafusion_runtime import df_profile, df_ctx
from tests.test_helpers.optional_deps import require_datafusion

require_datafusion()
profile = df_profile()
ctx = df_ctx()
```

When tests need custom profile settings, keep explicit `DataFusionRuntimeProfile(...)` in the test,
and use helpers only for the parts that are boilerplate.
