"""Unit tests for required extension entrypoint contracts."""

from __future__ import annotations

from datafusion_engine.extensions.required_entrypoints import REQUIRED_RUNTIME_ENTRYPOINTS
from test_support import datafusion_ext_stub


def test_required_runtime_entrypoints_are_unique() -> None:
    """Required runtime entrypoint names are unique."""
    assert len(REQUIRED_RUNTIME_ENTRYPOINTS) == len(set(REQUIRED_RUNTIME_ENTRYPOINTS))


def test_stub_implements_required_runtime_entrypoints() -> None:
    """Stub module exposes all required runtime entrypoints."""
    missing = [
        name
        for name in REQUIRED_RUNTIME_ENTRYPOINTS
        if not callable(getattr(datafusion_ext_stub, name, None))
    ]
    assert missing == []
