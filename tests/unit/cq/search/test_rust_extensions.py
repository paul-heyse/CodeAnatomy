# ruff: noqa: ARG001, EM102, TRY003
"""Unit tests for rust-analyzer extension plane helpers."""

from __future__ import annotations

from types import SimpleNamespace

import msgspec
from tools.cq.search.rust_extensions import (
    RustMacroExpansionV1,
    RustRunnableV1,
    expand_macro,
    get_runnables,
)


def test_rust_macro_expansion_roundtrip() -> None:
    expansion = RustMacroExpansionV1(
        name="vec!",
        expansion="Vec::new()",
        expansion_byte_len=11,
    )
    encoded = msgspec.json.encode(expansion)
    decoded = msgspec.json.decode(encoded, type=RustMacroExpansionV1)
    assert decoded == expansion


def test_rust_runnable_roundtrip() -> None:
    runnable = RustRunnableV1(
        label="test my_test",
        kind="test",
        args=("test", "--", "my_test"),
        location_uri="file:///src/lib.rs",
        location_line=42,
    )
    encoded = msgspec.json.encode(runnable)
    decoded = msgspec.json.decode(encoded, type=RustRunnableV1)
    assert decoded == runnable


def test_expand_macro_and_runnables_normalization() -> None:
    def request(method: str, params: object) -> object:
        if method == "rust-analyzer/expandMacro":
            return {"name": "vec!", "expansion": "Vec::new()"}
        if method == "experimental/runnables":
            return [
                {
                    "label": "test my_test",
                    "kind": "test",
                    "args": {"cargoArgs": ["test", "my_test"]},
                    "location": {
                        "target": {
                            "uri": "file:///src/lib.rs",
                            "range": {"start": {"line": 42, "character": 0}},
                        }
                    },
                }
            ]
        raise AssertionError(f"Unexpected method: {method}")

    session = SimpleNamespace(_send_request=request)
    macro = expand_macro(session, "file:///src/lib.rs", 1, 1)
    assert macro is not None
    assert macro.name == "vec!"
    assert macro.expansion == "Vec::new()"

    runnables = get_runnables(session, "file:///src/lib.rs")
    assert len(runnables) == 1
    assert runnables[0].label == "test my_test"


def test_expand_macro_fail_open_on_invalid_payload() -> None:
    def request(_method: str, _params: object) -> object:
        return {"expansion": 123}

    session = SimpleNamespace(_send_request=request)
    assert expand_macro(session, "file:///src/lib.rs", 1, 1) is None


def test_get_runnables_fail_open_on_invalid_payload() -> None:
    def request(_method: str, _params: object) -> object:
        return {"not": "a list"}

    session = SimpleNamespace(_send_request=request)
    assert get_runnables(session, "file:///src/lib.rs") == ()
