"""Tests for deterministic msgspec inspection serialization."""

from __future__ import annotations

import re

import msgspec

from serde_msgspec_inspect import inspect_to_builtins, stable_stringify

_HEX_ADDRESS_RE = re.compile(r"0x[0-9A-Fa-f]+")


def test_stable_stringify_removes_runtime_addresses() -> None:
    """Ensure stable stringify scrubs nondeterministic runtime addresses."""
    payload = object()
    rendered = stable_stringify(payload)
    assert _HEX_ADDRESS_RE.search(rendered) is None


def test_inspect_to_builtins_stringify_is_stable() -> None:
    """Ensure stringify mode emits deterministic output for inspect payloads."""
    inspect_payload = [msgspec.inspect.type_info(int), object()]
    first = inspect_to_builtins(
        inspect_payload,
        mapping_mode="stringify",
        fallback=stable_stringify,
    )
    second = inspect_to_builtins(
        inspect_payload,
        mapping_mode="stringify",
        fallback=stable_stringify,
    )
    assert isinstance(first, str)
    assert first == second
    assert _HEX_ADDRESS_RE.search(first) is None
