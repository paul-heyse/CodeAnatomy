import pytest
from datafusion import SessionContext

from datafusion_engine.udf import fallback, runtime


def test_fallback_snapshot_matches_registered_names() -> None:
    snapshot = fallback.fallback_udf_snapshot()
    scalar = snapshot.get("scalar")
    assert isinstance(scalar, list)
    names = {name for name in scalar if isinstance(name, str)}
    assert names == set(fallback.fallback_udf_names())


def test_fallback_snapshot_is_subset_of_native_snapshot() -> None:
    if not runtime.udf_backend_available():
        pytest.skip("Native Rust UDF backend unavailable for parity check.")
    ctx = SessionContext()
    native_snapshot = runtime.register_rust_udfs(ctx)
    native_names = runtime.udf_names_from_snapshot(native_snapshot)
    fallback_names = set(fallback.fallback_udf_names())
    assert fallback_names.issubset(native_names)
