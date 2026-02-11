"""Rust LSP session with environment capture and capability tracking."""

from __future__ import annotations

import hashlib
import json
import subprocess
import time
from pathlib import Path

from tools.cq.core.structs import CqStruct
from tools.cq.search.rust_lsp_contracts import (
    LspCapabilitySnapshotV1,
    LspClientCapabilitySnapshotV1,
    LspClientPublishDiagnosticsCapsV1,
    LspExperimentalCapabilitySnapshotV1,
    LspServerCapabilitySnapshotV1,
    LspSessionEnvV1,
    RustDiagnosticV1,
    RustLspEnrichmentPayload,
    coerce_rust_lsp_payload,
)


class RustLspRequest(CqStruct, frozen=True):
    """Request for Rust LSP enrichment."""

    file_path: str
    line: int
    col: int
    query_intent: str = "symbol_grounding"


class _RustLspSession:
    """Rust LSP session with environment capture and capability tracking.

    Lifecycle:
    1. _start() → spawn rust-analyzer, negotiate capabilities
    2. probe() → execute enrichment requests
    3. shutdown() → graceful cleanup
    """

    def __init__(self, repo_root: Path) -> None:
        """Initialize session with repository root.

        Parameters
        ----------
        repo_root
            Path to repository root for workspace initialization.
        """
        self.repo_root = repo_root
        self._process: subprocess.Popen[bytes] | None = None
        self._session_env = LspSessionEnvV1()
        self._diagnostics_by_uri: dict[str, list[RustDiagnosticV1]] = {}
        self._config_response_cache: dict[str, object] = {}

    def _start(self) -> None:
        """Start rust-analyzer and negotiate capabilities.

        Spawns rust-analyzer subprocess, sends initialize request,
        captures session environment (capabilities, server info),
        and waits for workspace quiescence.
        """
        # Spawn rust-analyzer subprocess stub
        # This is a transport stub that would spawn: rust-analyzer
        # For now, this is a placeholder for the actual LSP transport implementation

        # Send initialize request stub
        # This would send the LSP initialize request and parse the response
        # LSP transport implementation needed here
        init_response: dict[str, object] = self._send_request(
            "initialize",
            {
                "processId": None,
                "rootUri": self.repo_root.as_uri(),
                "capabilities": {
                    "textDocument": {
                        "definition": {"linkSupport": True},
                        "typeDefinition": {"linkSupport": True},
                        "implementation": {"linkSupport": True},
                        "references": {},
                        "documentSymbol": {"hierarchicalDocumentSymbolSupport": True},
                        "callHierarchy": {},
                        "typeHierarchy": {},
                        "hover": {"contentFormat": ["plaintext", "markdown"]},
                        "publishDiagnostics": {
                            "relatedInformation": True,
                            "versionSupport": True,
                            "codeDescriptionSupport": True,
                            "dataSupport": True,
                        },
                    },
                    "experimental": {
                        "serverStatusNotification": True,
                    },
                },
            },
        )

        # Capture session environment from initialization
        server_info = init_response.get("serverInfo", {})
        if not isinstance(server_info, dict):
            server_info = {}
        server_caps = init_response.get("capabilities", {})
        if not isinstance(server_caps, dict):
            server_caps = {}
        position_encoding = server_caps.get("positionEncoding", "utf-16")
        if not isinstance(position_encoding, str):
            position_encoding = "utf-16"

        # Extract capability snapshot (server/client/experimental split)
        capabilities = LspCapabilitySnapshotV1(
            server_caps=LspServerCapabilitySnapshotV1(
                definition_provider="definitionProvider" in server_caps,
                type_definition_provider="typeDefinitionProvider" in server_caps,
                implementation_provider="implementationProvider" in server_caps,
                references_provider="referencesProvider" in server_caps,
                document_symbol_provider="documentSymbolProvider" in server_caps,
                call_hierarchy_provider="callHierarchyProvider" in server_caps,
                type_hierarchy_provider="typeHierarchyProvider" in server_caps,
                hover_provider="hoverProvider" in server_caps,
                workspace_symbol_provider="workspaceSymbolProvider" in server_caps,
                rename_provider="renameProvider" in server_caps,
                code_action_provider="codeActionProvider" in server_caps,
                semantic_tokens_provider="semanticTokensProvider" in server_caps,
                inlay_hint_provider="inlayHintProvider" in server_caps,
                semantic_tokens_provider_raw=(
                    server_caps.get("semanticTokensProvider")
                    if isinstance(server_caps.get("semanticTokensProvider"), dict)
                    else None
                ),
                code_action_provider_raw=server_caps.get("codeActionProvider"),
                workspace_symbol_provider_raw=server_caps.get("workspaceSymbolProvider"),
            ),
            client_caps=LspClientCapabilitySnapshotV1(
                publish_diagnostics=LspClientPublishDiagnosticsCapsV1(
                    enabled=True,
                    related_information=True,
                    version_support=True,
                    code_description_support=True,
                    data_support=True,
                )
            ),
            experimental_caps=LspExperimentalCapabilitySnapshotV1(
                server_status_notification=True,
            ),
        )

        # Compute config fingerprint from effective workspace/configuration responses
        config_fingerprint = hashlib.sha256(
            json.dumps(self._config_response_cache, sort_keys=True).encode()
        ).hexdigest()[:16]

        self._session_env = LspSessionEnvV1(
            server_name=server_info.get("name")
            if isinstance(server_info.get("name"), str)
            else None,
            server_version=server_info.get("version")
            if isinstance(server_info.get("version"), str)
            else None,
            position_encoding=position_encoding,
            capabilities=capabilities,
            workspace_health="unknown",  # Will update after quiescence
            quiescent=False,
            config_fingerprint=config_fingerprint,
        )

        # Send initialized notification stub
        self._send_notification("initialized", {})

        # Wait for quiescence (server finishes indexing)
        self._wait_for_quiescence()

    def _wait_for_quiescence(self, timeout: float = 30.0) -> None:
        """Wait for rust-analyzer to finish indexing.

        Monitors experimental/serverStatus notifications for quiescent=true.

        Parameters
        ----------
        timeout
            Maximum time to wait for quiescence in seconds.
        """
        start = time.time()
        while time.time() - start < timeout:
            # Process incoming notifications
            notifications = self._read_pending_notifications()
            for notif in notifications:
                if notif.get("method") == "experimental/serverStatus":
                    params = notif.get("params", {})
                    if not isinstance(params, dict):
                        continue
                    reported_health = params.get("health")
                    health = (
                        reported_health
                        if reported_health in ("ok", "warning", "error")
                        else self._session_env.workspace_health
                    )
                    quiescent = bool(params.get("quiescent", self._session_env.quiescent))
                    self._session_env = LspSessionEnvV1(
                        server_name=self._session_env.server_name,
                        server_version=self._session_env.server_version,
                        position_encoding=self._session_env.position_encoding,
                        capabilities=self._session_env.capabilities,
                        workspace_health=health,
                        quiescent=quiescent,
                        config_fingerprint=self._session_env.config_fingerprint,
                        refresh_events=self._session_env.refresh_events,
                    )
                    if quiescent:
                        return
                elif notif.get("method") == "textDocument/publishDiagnostics":
                    self._handle_diagnostics_notification(notif)
            time.sleep(0.1)

    def _handle_diagnostics_notification(self, notif: dict[str, object]) -> None:
        """Capture diagnostics from publishDiagnostics notification.

        Parameters
        ----------
        notif
            LSP notification payload containing diagnostics.
        """
        params = notif.get("params", {})
        if not isinstance(params, dict):
            return

        uri = params.get("uri")
        diagnostics_data = params.get("diagnostics", [])

        if not isinstance(uri, str) or not isinstance(diagnostics_data, list):
            return

        diagnostics = []
        for diag in diagnostics_data:
            if not isinstance(diag, dict):
                continue

            range_data = diag.get("range", {})
            if not isinstance(range_data, dict):
                continue

            start = range_data.get("start", {})
            end = range_data.get("end", {})
            if not isinstance(start, dict) or not isinstance(end, dict):
                continue

            related_info_raw = diag.get("relatedInformation", [])
            related_info: tuple[dict[str, object], ...] = ()
            if isinstance(related_info_raw, list):
                related_info = tuple(item for item in related_info_raw if isinstance(item, dict))

            data_raw = diag.get("data")
            data_dict = dict(data_raw) if isinstance(data_raw, dict) else None

            diagnostics.append(
                RustDiagnosticV1(
                    uri=uri,
                    range_start_line=start.get("line", 0)
                    if isinstance(start.get("line"), int)
                    else 0,
                    range_start_col=start.get("character", 0)
                    if isinstance(start.get("character"), int)
                    else 0,
                    range_end_line=end.get("line", 0) if isinstance(end.get("line"), int) else 0,
                    range_end_col=end.get("character", 0)
                    if isinstance(end.get("character"), int)
                    else 0,
                    severity=diag.get("severity", 0)
                    if isinstance(diag.get("severity"), int)
                    else 0,
                    code=diag.get("code") if isinstance(diag.get("code"), str) else None,
                    source=diag.get("source") if isinstance(diag.get("source"), str) else None,
                    message=diag.get("message", "") if isinstance(diag.get("message"), str) else "",
                    related_info=related_info,
                    data=data_dict,
                )
            )

        self._diagnostics_by_uri[uri] = diagnostics

    def probe(self, request: RustLspRequest) -> RustLspEnrichmentPayload | None:
        """Execute enrichment probe with tiered capability gating.

        Returns typed RustLspEnrichmentPayload with session environment
        and all available enrichment surfaces. Returns None only for
        transport/session-fatal failure at the outer boundary.
        Non-fatal capability/health degradation returns partial typed payload.

        Tiered gating:
        - Tier A (session alive): hover, definition, type definition
        - Tier B (health in {ok, warning}): references, document symbols
        - Tier C (health=ok and quiescent): call hierarchy, type hierarchy

        Parameters
        ----------
        request
            Enrichment request with file path, line, and column.

        Returns:
        -------
        RustLspEnrichmentPayload | None
            Typed enrichment payload, or None on transport/session-fatal failure.
        """
        if self._process is None:
            return None  # Transport/session-fatal boundary

        health = self._session_env.workspace_health
        caps = self._session_env.capabilities.server_caps

        # Tier A: Always available
        requests: dict[str, tuple[str, dict[str, object]]] = {}
        if caps.hover_provider:
            requests["hover"] = ("textDocument/hover", {})
        if caps.definition_provider:
            requests["definition"] = ("textDocument/definition", {})
        if caps.type_definition_provider:
            requests["typeDefinition"] = ("textDocument/typeDefinition", {})

        # Tier B: Require health in {ok, warning}
        if health in ("ok", "warning"):
            if caps.references_provider:
                requests["references"] = ("textDocument/references", {})
            if caps.document_symbol_provider:
                requests["documentSymbol"] = ("textDocument/documentSymbol", {})

        # Tier C: Require health=ok and quiescent
        if health == "ok" and self._session_env.quiescent:
            if caps.call_hierarchy_provider:
                requests["prepareCallHierarchy"] = ("textDocument/prepareCallHierarchy", {})
            if caps.type_hierarchy_provider:
                requests["prepareTypeHierarchy"] = ("textDocument/prepareTypeHierarchy", {})

        # Execute requests (stub - LSP transport implementation needed)
        # This would send each request and collect responses
        responses: dict[str, object] = {}

        # Assemble raw payload dict
        uri_str = Path(request.file_path).as_uri()
        raw_payload: dict[str, object] = {
            "session_env": {
                "server_name": self._session_env.server_name,
                "server_version": self._session_env.server_version,
                "position_encoding": self._session_env.position_encoding,
                "workspace_health": self._session_env.workspace_health,
                "quiescent": self._session_env.quiescent,
                "config_fingerprint": self._session_env.config_fingerprint,
            },
            "diagnostics": self._diagnostics_by_uri.get(uri_str, []),
        }

        # Coerce to typed payload
        return coerce_rust_lsp_payload(raw_payload)

    def shutdown(self) -> None:
        """Graceful shutdown of LSP session.

        Sends shutdown request and exit notification to server,
        then terminates the subprocess.
        """
        if self._process is not None:
            # Send shutdown request stub
            self._send_request("shutdown", {})
            # Send exit notification stub
            self._send_notification("exit", {})
            # Terminate process
            self._process.terminate()
            self._process.wait(timeout=5.0)
            self._process = None

    def _send_request(self, method: str, params: dict[str, object]) -> dict[str, object]:
        """Send LSP request and wait for response.

        This is a stub for the LSP JSON-RPC transport layer.
        Actual implementation would serialize the request, send via stdin,
        read response from stdout, and parse the JSON-RPC response.

        Parameters
        ----------
        method
            LSP method name (e.g., "initialize", "textDocument/definition").
        params
            Method parameters as a dictionary.

        Returns:
        -------
        dict[str, object]
            Response result from the LSP server.
        """
        # LSP transport stub - needs JSON-RPC implementation
        return {}

    def _send_notification(self, method: str, params: dict[str, object]) -> None:
        """Send LSP notification (no response expected).

        This is a stub for the LSP JSON-RPC transport layer.

        Parameters
        ----------
        method
            LSP method name (e.g., "initialized", "exit").
        params
            Method parameters as a dictionary.
        """
        # LSP transport stub - needs JSON-RPC implementation

    def _read_pending_notifications(self) -> list[dict[str, object]]:
        """Read all pending notifications from the LSP server.

        This is a stub for the LSP JSON-RPC transport layer.
        Actual implementation would read from stdout, parse JSON-RPC messages,
        and return all notifications (messages without an id field).

        Returns:
        -------
        list[dict[str, object]]
            List of notification payloads.
        """
        # LSP transport stub - needs JSON-RPC implementation
        return []


__all__ = [
    "RustLspRequest",
]
