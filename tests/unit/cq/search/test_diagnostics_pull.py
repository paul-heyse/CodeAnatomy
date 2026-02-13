# ruff: noqa: ANN001, EM101
"""Unit tests for diagnostics pull plane helpers."""

from __future__ import annotations

from types import SimpleNamespace

from tools.cq.search.diagnostics_pull import (
    pull_text_document_diagnostics,
    pull_workspace_diagnostics,
)


def _session(
    *,
    request,
    diagnostic_provider: object = True,
    workspace_diagnostic_provider: object = False,
) -> object:
    return SimpleNamespace(
        _send_request=request,
        _session_env=SimpleNamespace(
            capabilities=SimpleNamespace(
                server_caps=SimpleNamespace(
                    diagnostic_provider_raw=diagnostic_provider,
                    workspace_diagnostic_provider_raw=workspace_diagnostic_provider,
                )
            )
        ),
    )


def test_pull_text_document_diagnostics_returns_none_when_unsupported() -> None:
    def request(_method: str, _params: object) -> object:
        return {"ok": True}

    session = _session(request=request, diagnostic_provider=False)
    assert pull_text_document_diagnostics(session, uri="file:///x.py") is None


def test_pull_text_document_diagnostics_normalizes_items() -> None:
    def request(method: str, _params: object) -> object:
        assert method == "textDocument/diagnostic"
        return {
            "items": [
                {
                    "uri": "file:///x.py",
                    "version": 2,
                    "diagnostics": [
                        {
                            "range": {
                                "start": {"line": 4, "character": 2},
                                "end": {"line": 4, "character": 8},
                            },
                            "severity": 2,
                            "code": "W001",
                            "message": "warning",
                            "tags": [1],
                            "codeDescription": {"href": "https://example.dev/w001"},
                        }
                    ],
                }
            ]
        }

    session = _session(request=request, diagnostic_provider=True)
    rows = pull_text_document_diagnostics(session, uri="file:///x.py")
    assert rows is not None
    assert len(rows) == 1
    assert rows[0]["uri"] == "file:///x.py"
    assert rows[0]["line"] == 4
    assert rows[0]["severity"] == 2
    assert rows[0]["code"] == "W001"


def test_pull_workspace_diagnostics_normalizes_related_documents() -> None:
    def request(method: str, _params: object) -> object:
        assert method == "workspace/diagnostic"
        return {
            "relatedDocuments": {
                "file:///y.py": {
                    "diagnostics": [
                        {
                            "range": {
                                "start": {"line": 1, "character": 0},
                                "end": {"line": 1, "character": 1},
                            },
                            "severity": 1,
                            "message": "error",
                        }
                    ]
                }
            }
        }

    session = _session(request=request, workspace_diagnostic_provider=True)
    rows = pull_workspace_diagnostics(session)
    assert rows is not None
    assert len(rows) == 1
    assert rows[0]["uri"] == "file:///y.py"
    assert rows[0]["message"] == "error"


def test_pull_workspace_diagnostics_fail_open_on_exception() -> None:
    def request(_method: str, _params: object) -> object:
        raise RuntimeError("boom")

    session = _session(request=request, workspace_diagnostic_provider=True)
    assert pull_workspace_diagnostics(session) is None


def test_pull_diagnostics_uses_capabilities_snapshot_when_present() -> None:
    class _SnapshotSession:
        def capabilities_snapshot(self) -> dict[str, object]:
            return {"diagnosticProvider": True, "workspaceDiagnosticProvider": True}

        def _send_request(self, method: str, _params: object) -> object:
            empty_items: list[dict[str, object]] = []
            empty_related_documents: dict[str, dict[str, object]] = {}
            empty_payload: dict[str, object] = {}
            if method == "textDocument/diagnostic":
                return {"items": empty_items}
            if method == "workspace/diagnostic":
                return {"relatedDocuments": empty_related_documents}
            return empty_payload

    session = _SnapshotSession()
    assert pull_text_document_diagnostics(session, uri="file:///x.py") == ()
    assert pull_workspace_diagnostics(session) == ()
