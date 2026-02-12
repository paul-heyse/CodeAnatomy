from __future__ import annotations

import pytest
from datafusion import SessionContext

from datafusion_engine.udf import factory, runtime


def test_register_rust_udfs_requires_native_backend(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        runtime,
        "_datafusion_internal",
        lambda: (_ for _ in ()).throw(ImportError("missing extension")),
    )

    with pytest.raises(ImportError, match="missing extension"):
        runtime.register_rust_udfs(SessionContext())


def test_install_function_factory_rejects_ctx_abi_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        factory,
        "_install_native_function_factory",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(TypeError("cannot be converted")),
    )

    with pytest.raises(TypeError, match="FunctionFactory install failed due to SessionContext"):
        factory.install_function_factory(SessionContext())
