"""Tests for rust-analyzer macro expansion bridge runtime helpers."""

from __future__ import annotations

from tools.cq.search.rust.contracts import RustMacroExpansionRequestV1
from tools.cq.search.rust.extensions import expand_macro


class _FakeClient:
    def request(self, method: str, payload: dict[str, object]) -> dict[str, object]:
        assert method == "rust-analyzer/expandMacro"
        assert "textDocument" in payload
        return {
            "result": {
                "name": "sql",
                "expansion": "SELECT 1",
            }
        }


def test_expand_macro_returns_applied_result_from_client() -> None:
    request = RustMacroExpansionRequestV1(
        file_path="src/lib.rs",
        line=9,
        col=4,
        macro_call_id="src/lib.rs:9:4:sql",
    )

    result = expand_macro(_FakeClient(), request)

    assert result.applied is True
    assert result.name == "sql"
    assert result.expansion == "SELECT 1"


def test_expand_macro_fails_open_without_client_request() -> None:
    request = RustMacroExpansionRequestV1(
        file_path="src/lib.rs",
        line=1,
        col=0,
        macro_call_id="id",
    )

    result = expand_macro(object(), request)

    assert result.applied is False
    assert result.name is None
