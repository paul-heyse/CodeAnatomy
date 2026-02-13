# ruff: noqa: ANN001, EM101, EM102, TRY003
"""Unit tests for refactor action plane helpers."""

from __future__ import annotations

from types import SimpleNamespace

import msgspec
from tools.cq.search.refactor_actions import (
    CodeActionV1,
    DiagnosticItemV1,
    DocumentChangeV1,
    PrepareRenameResultV1,
    WorkspaceEditV1,
    execute_code_action_command,
    pull_document_diagnostics,
    pull_workspace_diagnostics,
    resolve_code_action,
)


def _session(*, request, code_action_provider: object = True) -> object:
    return SimpleNamespace(
        _send_request=request,
        _session_env=SimpleNamespace(
            capabilities=SimpleNamespace(
                server_caps=SimpleNamespace(
                    publish_diagnostics=True,
                    diagnostic_provider_raw={"workspaceDiagnostics": True},
                    workspace_diagnostic_provider_raw=True,
                    code_action_provider=code_action_provider,
                    code_action_provider_raw={"resolveProvider": True},
                )
            )
        ),
    )


def test_prepare_rename_result_roundtrip() -> None:
    result = PrepareRenameResultV1(
        range_start_line=10,
        range_start_col=5,
        range_end_line=10,
        range_end_col=15,
        placeholder="new_name",
        can_rename=True,
    )
    encoded = msgspec.json.encode(result)
    decoded = msgspec.json.decode(encoded, type=PrepareRenameResultV1)
    assert decoded == result


def test_diagnostic_item_roundtrip() -> None:
    diagnostic = DiagnosticItemV1(
        uri="file:///path/to/file.py",
        message="undefined variable",
        severity=1,
        code="E0425",
        code_description_href="https://docs.rs/error/E0425",
        tags=(1, 2),
        version=5,
        related_information=({"message": "defined here", "location": {}},),
        data={"fix_available": True},
    )
    encoded = msgspec.json.encode(diagnostic)
    decoded = msgspec.json.decode(encoded, type=DiagnosticItemV1)
    assert decoded == diagnostic


def test_workspace_edit_roundtrip() -> None:
    edit = WorkspaceEditV1(
        document_changes=(
            DocumentChangeV1(uri="file:///a.py", edit_count=2),
            DocumentChangeV1(uri="file:///b.py", edit_count=1),
        ),
        change_count=3,
    )
    encoded = msgspec.json.encode(edit)
    decoded = msgspec.json.decode(encoded, type=WorkspaceEditV1)
    assert decoded == edit


def test_pull_document_diagnostics_and_resolve_action() -> None:
    empty_changes: dict[str, object] = {}
    empty_diagnostics: list[dict[str, object]] = []
    code_action_payload = {
        "title": "Apply fix",
        "kind": "quickfix",
        "isPreferred": True,
        "edit": {"changes": empty_changes},
        "command": {"command": "editor.action.apply"},
        "diagnostics": empty_diagnostics,
    }

    def request(method: str, params: object) -> object:
        if method == "textDocument/diagnostic":
            return {
                "items": [
                    {
                        "uri": "file:///test.py",
                        "diagnostics": [
                            {
                                "range": {
                                    "start": {"line": 2, "character": 4},
                                    "end": {"line": 2, "character": 7},
                                },
                                "severity": 2,
                                "code": "W0001",
                                "message": "warning",
                            }
                        ],
                    }
                ]
            }
        if method == "codeAction/resolve":
            assert isinstance(params, dict)
            return dict(code_action_payload)
        if method == "workspace/executeCommand":
            return {"ok": True}
        raise AssertionError(f"Unexpected method: {method}")

    session = _session(request=request)

    diagnostics = pull_document_diagnostics(session, "file:///test.py")
    assert diagnostics is not None
    assert len(diagnostics) == 1
    assert diagnostics[0].message == "warning"

    action = CodeActionV1(
        title="Apply fix",
        is_resolvable=True,
        raw_payload={"title": "Apply fix"},
        command_id="editor.action.apply",
        has_command=True,
    )
    resolved = resolve_code_action(session, action)
    assert resolved is not None
    assert resolved.kind == "quickfix"
    assert execute_code_action_command(session, resolved) is True


def test_pull_workspace_diagnostics_pass_through() -> None:
    def request(method: str, _params: object) -> object:
        if method == "workspace/diagnostic":
            return {
                "items": [
                    {
                        "uri": "file:///test.py",
                        "diagnostics": [
                            {
                                "range": {
                                    "start": {"line": 1, "character": 1},
                                    "end": {"line": 1, "character": 2},
                                },
                                "severity": 1,
                                "message": "error",
                            }
                        ],
                    }
                ]
            }
        raise AssertionError(f"Unexpected method: {method}")

    session = _session(request=request)
    diagnostics = pull_workspace_diagnostics(session)
    assert diagnostics is not None
    assert len(diagnostics) == 1
    assert diagnostics[0].message == "error"


def test_resolve_code_action_fail_open_on_exception() -> None:
    def request(_method: str, _params: object) -> object:
        raise RuntimeError("boom")

    session = _session(request=request)
    action = CodeActionV1(title="Test action", is_resolvable=True, raw_payload={"title": "X"})
    assert resolve_code_action(session, action) is None


def test_execute_code_action_requires_command_id() -> None:
    def request(_method: str, _params: object) -> object:
        return {"ok": True}

    session = _session(request=request)
    action = CodeActionV1(title="No command")
    assert execute_code_action_command(session, action) is False
