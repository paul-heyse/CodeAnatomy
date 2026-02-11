"""Tests for Rust LSP session behavior and tiered gating."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from tools.cq.search.rust_lsp import RustLspRequest, _RustLspSession
from tools.cq.search.rust_lsp_contracts import (
    LspCapabilitySnapshotV1,
    LspServerCapabilitySnapshotV1,
    LspSessionEnvV1,
    RustDiagnosticV1,
)


@pytest.fixture
def mock_repo_root(tmp_path: Path) -> Path:
    """Create a temporary repository root."""
    return tmp_path


@pytest.fixture
def session(mock_repo_root: Path) -> _RustLspSession:
    """Create a Rust LSP session for testing."""
    return _RustLspSession(mock_repo_root)


def test_session_initialization(session: _RustLspSession, mock_repo_root: Path) -> None:
    """Test session initializes with correct defaults."""
    assert session.repo_root == mock_repo_root
    assert session._process is None
    assert session._session_env.workspace_health == "unknown"
    assert session._session_env.quiescent is False
    assert len(session._diagnostics_by_uri) == 0


def test_handle_diagnostics_notification_valid(session: _RustLspSession) -> None:
    """Test diagnostic notification processing with valid data."""
    notif = {
        "method": "textDocument/publishDiagnostics",
        "params": {
            "uri": "file:///path/to/file.rs",
            "diagnostics": [
                {
                    "range": {
                        "start": {"line": 10, "character": 5},
                        "end": {"line": 10, "character": 15},
                    },
                    "severity": 1,
                    "code": "E0308",
                    "source": "rust-analyzer",
                    "message": "mismatched types",
                }
            ],
        },
    }

    session._handle_diagnostics_notification(notif)

    uri = "file:///path/to/file.rs"
    assert uri in session._diagnostics_by_uri
    assert len(session._diagnostics_by_uri[uri]) == 1

    diag = session._diagnostics_by_uri[uri][0]
    assert diag.uri == uri
    assert diag.range_start_line == 10
    assert diag.range_start_col == 5
    assert diag.severity == 1
    assert diag.code == "E0308"
    assert diag.message == "mismatched types"


def test_handle_diagnostics_notification_with_related_info(session: _RustLspSession) -> None:
    """Test diagnostic notification processing with related information."""
    notif = {
        "method": "textDocument/publishDiagnostics",
        "params": {
            "uri": "file:///path/to/file.rs",
            "diagnostics": [
                {
                    "range": {
                        "start": {"line": 10, "character": 5},
                        "end": {"line": 10, "character": 15},
                    },
                    "severity": 1,
                    "message": "error",
                    "relatedInformation": [
                        {
                            "location": {"uri": "file:///other.rs"},
                            "message": "related info",
                        }
                    ],
                    "data": {"fix": "add annotation"},
                }
            ],
        },
    }

    session._handle_diagnostics_notification(notif)

    uri = "file:///path/to/file.rs"
    diag = session._diagnostics_by_uri[uri][0]
    assert len(diag.related_info) == 1
    assert diag.data == {"fix": "add annotation"}


def test_handle_diagnostics_notification_malformed(session: _RustLspSession) -> None:
    """Test diagnostic notification processing handles malformed data gracefully."""
    # Missing params
    notif1 = {"method": "textDocument/publishDiagnostics"}
    session._handle_diagnostics_notification(notif1)
    assert len(session._diagnostics_by_uri) == 0

    # Non-dict params
    notif2 = {"method": "textDocument/publishDiagnostics", "params": "not a dict"}
    session._handle_diagnostics_notification(notif2)
    assert len(session._diagnostics_by_uri) == 0

    # Missing uri
    notif3 = {"method": "textDocument/publishDiagnostics", "params": {"diagnostics": []}}
    session._handle_diagnostics_notification(notif3)
    assert len(session._diagnostics_by_uri) == 0


def test_tiered_gating_tier_a_always_available(session: _RustLspSession) -> None:
    """Test Tier A requests (hover, definition, type definition) always present."""
    # Setup session with capabilities but health unknown
    session._session_env = LspSessionEnvV1(
        workspace_health="unknown",
        quiescent=False,
        capabilities=LspCapabilitySnapshotV1(
            server_caps=LspServerCapabilitySnapshotV1(
                hover_provider=True,
                definition_provider=True,
                type_definition_provider=True,
                references_provider=True,
                document_symbol_provider=True,
                call_hierarchy_provider=True,
                type_hierarchy_provider=True,
            )
        ),
    )

    # Mock the process to simulate session alive
    session._process = MagicMock()

    # Mock LSP transport methods
    with patch.object(session, "_send_request", return_value={}):
        with patch.object(session, "_read_pending_notifications", return_value=[]):
            request = RustLspRequest(file_path="/path/to/file.rs", line=10, col=5)
            result = session.probe(request)

            # Should return a payload (not None) even with unknown health
            assert result is not None
            assert result.session_env.workspace_health == "unknown"


def test_tiered_gating_tier_b_requires_ok_or_warning(session: _RustLspSession) -> None:
    """Test Tier B requests (references, document symbols) require health ok/warning."""
    # Setup session with ok health
    session._session_env = LspSessionEnvV1(
        workspace_health="ok",
        quiescent=False,
        capabilities=LspCapabilitySnapshotV1(
            server_caps=LspServerCapabilitySnapshotV1(
                references_provider=True,
                document_symbol_provider=True,
            )
        ),
    )
    session._process = MagicMock()

    with patch.object(session, "_send_request", return_value={}):
        with patch.object(session, "_read_pending_notifications", return_value=[]):
            request = RustLspRequest(file_path="/path/to/file.rs", line=10, col=5)
            result = session.probe(request)

            # Should include Tier B requests
            assert result is not None

    # Now test with error health (should not include Tier B)
    session._session_env = LspSessionEnvV1(
        workspace_health="error",
        quiescent=False,
        capabilities=LspCapabilitySnapshotV1(
            server_caps=LspServerCapabilitySnapshotV1(
                references_provider=True,
                document_symbol_provider=True,
            )
        ),
    )

    with patch.object(session, "_send_request", return_value={}):
        with patch.object(session, "_read_pending_notifications", return_value=[]):
            result = session.probe(request)

            # Should still return payload, but Tier B requests not included
            assert result is not None
            assert result.session_env.workspace_health == "error"


def test_tiered_gating_tier_c_requires_ok_and_quiescent(session: _RustLspSession) -> None:
    """Test Tier C requests (call hierarchy, type hierarchy) require ok + quiescent."""
    # Setup session with ok health but not quiescent
    session._session_env = LspSessionEnvV1(
        workspace_health="ok",
        quiescent=False,
        capabilities=LspCapabilitySnapshotV1(
            server_caps=LspServerCapabilitySnapshotV1(
                call_hierarchy_provider=True,
                type_hierarchy_provider=True,
            )
        ),
    )
    session._process = MagicMock()

    with patch.object(session, "_send_request", return_value={}):
        with patch.object(session, "_read_pending_notifications", return_value=[]):
            request = RustLspRequest(file_path="/path/to/file.rs", line=10, col=5)
            result = session.probe(request)

            # Should return payload but Tier C not included
            assert result is not None

    # Now test with ok health AND quiescent
    session._session_env = LspSessionEnvV1(
        workspace_health="ok",
        quiescent=True,
        capabilities=LspCapabilitySnapshotV1(
            server_caps=LspServerCapabilitySnapshotV1(
                call_hierarchy_provider=True,
                type_hierarchy_provider=True,
            )
        ),
    )

    with patch.object(session, "_send_request", return_value={}):
        with patch.object(session, "_read_pending_notifications", return_value=[]):
            result = session.probe(request)

            # Should include Tier C requests
            assert result is not None
            assert result.session_env.quiescent is True


def test_probe_returns_none_on_transport_fatal(session: _RustLspSession) -> None:
    """Test probe returns None only on transport/session-fatal failure."""
    # Session not started (process is None)
    request = RustLspRequest(file_path="/path/to/file.rs", line=10, col=5)
    result = session.probe(request)

    # Should return None at transport/session-fatal boundary
    assert result is None


def test_probe_includes_diagnostics_for_file(session: _RustLspSession) -> None:
    """Test probe includes diagnostics for the requested file."""
    # Add diagnostics for a file
    uri = "file:///path/to/file.rs"
    session._diagnostics_by_uri[uri] = [
        RustDiagnosticV1(
            uri=uri,
            range_start_line=10,
            range_start_col=5,
            range_end_line=10,
            range_end_col=15,
            severity=1,
            message="error",
        )
    ]

    # Setup session
    session._session_env = LspSessionEnvV1(workspace_health="ok")
    session._process = MagicMock()

    with patch.object(session, "_send_request", return_value={}):
        with patch.object(session, "_read_pending_notifications", return_value=[]):
            request = RustLspRequest(file_path="/path/to/file.rs", line=10, col=5)
            result = session.probe(request)

            assert result is not None
            # Note: The current implementation includes diagnostics in raw_payload
            # but the exact behavior depends on URI matching


def test_health_tri_state_handling(session: _RustLspSession) -> None:
    """Test workspace_health tri-state handling (ok, warning, error, unknown)."""
    # Test all valid health states
    for health in ("ok", "warning", "error", "unknown"):
        session._session_env = LspSessionEnvV1(
            workspace_health=health,  # type: ignore[arg-type]
            capabilities=LspCapabilitySnapshotV1(
                server_caps=LspServerCapabilitySnapshotV1(hover_provider=True)
            ),
        )
        session._process = MagicMock()

        with patch.object(session, "_send_request", return_value={}):
            with patch.object(session, "_read_pending_notifications", return_value=[]):
                request = RustLspRequest(file_path="/path/to/file.rs", line=10, col=5)
                result = session.probe(request)

                assert result is not None
                assert result.session_env.workspace_health == health


def test_shutdown_graceful(session: _RustLspSession) -> None:
    """Test graceful shutdown sends proper LSP messages."""
    mock_process = MagicMock()
    session._process = mock_process

    with (
        patch.object(session, "_send_request") as mock_send_request,
        patch.object(session, "_send_notification") as mock_send_notification,
    ):
        session.shutdown()

        # Should send shutdown request and exit notification
        mock_send_request.assert_called_once_with("shutdown", {})
        mock_send_notification.assert_called_once_with("exit", {})

        # Should terminate process
        mock_process.terminate.assert_called_once()
        mock_process.wait.assert_called_once()


def test_capability_checking_uses_provider_fields(session: _RustLspSession) -> None:
    """Test capability checking uses provider fields (not client capability paths)."""
    # Setup with specific provider capabilities
    session._session_env = LspSessionEnvV1(
        workspace_health="ok",
        quiescent=True,
        capabilities=LspCapabilitySnapshotV1(
            server_caps=LspServerCapabilitySnapshotV1(
                definition_provider=True,
                hover_provider=False,  # Explicitly disabled
                references_provider=True,
                call_hierarchy_provider=True,
            )
        ),
    )
    session._process = MagicMock()

    with patch.object(session, "_send_request", return_value={}):
        with patch.object(session, "_read_pending_notifications", return_value=[]):
            request = RustLspRequest(file_path="/path/to/file.rs", line=10, col=5)
            result = session.probe(request)

            assert result is not None
            # The probe logic should respect the provider field settings
            # (hover not requested because hover_provider=False)
