"""Shared helpers for immutability contract tests."""

from __future__ import annotations

from collections.abc import Callable
from typing import Final

import pytest

_UNSET: Final = object()


def assert_immutable_assignment(
    *,
    factory: Callable[[], object],
    attribute: str,
    attempted_value: object,
    expected_exception: type[BaseException],
    expected_value: object = _UNSET,
) -> None:
    """Assert assigning to an attribute raises and preserves object state."""
    target = factory()
    before_value = getattr(target, attribute)
    if expected_value is not _UNSET:
        assert before_value == expected_value
    with pytest.raises(expected_exception):
        setattr(target, attribute, attempted_value)
    after_value = getattr(target, attribute)
    if expected_value is _UNSET:
        expected_value = before_value
    assert after_value == expected_value
