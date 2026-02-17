from __future__ import annotations

from tools.cq.analysis.signature import parse_signature


def test_parse_signature_basic() -> None:
    params = parse_signature("foo(a, b=1, *, c=None, **kwargs)")
    assert [p.name for p in params] == ["a", "b", "c", "kwargs"]
    assert params[0].has_default is False
    assert params[1].has_default is True
    assert params[2].is_kwonly is True
    assert params[3].is_kwarg is True


def test_parse_signature_invalid_returns_empty() -> None:
    assert parse_signature("foo(a,,)") == []
