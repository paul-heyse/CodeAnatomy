"""Unit tests for lazy module export helpers."""

from __future__ import annotations

import math

import pytest

from utils.lazy_module import make_lazy_loader


def test_make_lazy_loader_resolves_tuple_and_string_targets() -> None:
    """Lazy loader resolves tuple and module-path export targets."""
    exports = {
        "sqrt": ("math", "sqrt"),
        "tau": "math",
    }
    module_globals: dict[str, object] = {}
    getattr_fn, dir_fn = make_lazy_loader(exports, "test_pkg", module_globals)

    sqrt = getattr_fn("sqrt")
    tau = getattr_fn("tau")

    assert sqrt is math.sqrt
    assert tau == math.tau
    assert module_globals["sqrt"] is math.sqrt
    assert module_globals["tau"] == math.tau
    assert dir_fn() == ["sqrt", "tau"]


def test_make_lazy_loader_raises_attribute_error() -> None:
    """Lazy loader raises ``AttributeError`` for unknown exports."""
    getattr_fn, _dir_fn = make_lazy_loader({}, "missing_pkg")

    with pytest.raises(AttributeError, match="module 'missing_pkg' has no attribute 'x'"):
        getattr_fn("x")
