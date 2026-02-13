"""Rust LSP session with environment capture and capability tracking."""

from __future__ import annotations

import atexit
import contextlib
import hashlib
import json
import os
import selectors
import subprocess
import time
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from time import monotonic
from typing import Literal, cast

from tools.cq.core.serialization import to_builtins
from tools.cq.core.structs import CqStruct
from tools.cq.search.lsp.session_manager import LspSessionManager
from tools.cq.search.lsp_advanced_planes import collect_advanced_lsp_planes
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
    rust_lsp_payload_to_dict,
)

_DEFAULT_TIMEOUT_SECONDS = 1.0
_DEFAULT_STARTUP_TIMEOUT_SECONDS = 3.0
_DEFAULT_QUIESCENCE_TIMEOUT_SECONDS = 8.0
_FAIL_OPEN_EXCEPTIONS = (OSError, RuntimeError, TimeoutError, ValueError, TypeError)


class RustLspRequest(CqStruct, frozen=True):
    """Request for Rust LSP enrichment."""

    file_path: str
    line: int
    col: int
    query_intent: str = "symbol_grounding"


@dataclass(slots=True)
class _SessionDocState:
    version: int
    content_hash: int


class _LspProtocolError(RuntimeError):
    """Raised when framing/protocol invariants are violated."""


class _RustLspSession:
    """Rust LSP session with environment capture and capability tracking."""

    def __init__(self, repo_root: Path) -> None:
        self.repo_root = repo_root.resolve()
        self._process: subprocess.Popen[bytes] | None = None
        self._selector: selectors.BaseSelector | None = None
        self._buffer = bytearray()
        self._next_id = 0
        self._session_env = LspSessionEnvV1()
        self._diagnostics_by_uri: dict[str, list[RustDiagnosticV1]] = {}
        self._config_response_cache: dict[str, object] = {}
        self._docs: dict[str, _SessionDocState] = {}

    @property
    def is_running(self) -> bool:
        return self._process is not None and self._process.poll() is None

    def ensure_started(self, *, timeout_seconds: float) -> None:
        if self.is_running:
            return
        self._start(timeout_seconds=timeout_seconds)

    def close(self) -> None:
        self.shutdown()
        selector = self._selector
        self._selector = None
        self._buffer = bytearray()
        self._docs.clear()
        self._diagnostics_by_uri.clear()
        if selector is not None:
            with contextlib.suppress(OSError):
                selector.close()

    def _start(self, timeout_seconds: float = _DEFAULT_STARTUP_TIMEOUT_SECONDS) -> None:
        process = subprocess.Popen(
            ["rust-analyzer"],
            cwd=str(self.repo_root),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        )
        if process.stdin is None or process.stdout is None:
            process.kill()
            msg = "Failed to open stdio pipes for rust-analyzer"
            raise _LspProtocolError(msg)

        selector = selectors.DefaultSelector()
        selector.register(process.stdout, selectors.EVENT_READ)

        self._process = process
        self._selector = selector
        self._buffer = bytearray()
        self._next_id = 0
        self._diagnostics_by_uri.clear()

        init_response = self._send_request(
            "initialize",
            {
                "processId": os.getpid(),
                "rootUri": self.repo_root.as_uri(),
                "capabilities": {
                    "general": {
                        "positionEncodings": ["utf-16", "utf-8"],
                    },
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

        init_response_map: dict[str, object] = (
            init_response if isinstance(init_response, dict) else {}
        )

        server_info_raw = init_response_map.get("serverInfo", {})
        server_info: dict[str, object] = (
            server_info_raw if isinstance(server_info_raw, dict) else {}
        )
        server_caps_raw_value = init_response_map.get("capabilities", {})
        server_caps_raw: dict[str, object] = (
            server_caps_raw_value if isinstance(server_caps_raw_value, dict) else {}
        )

        position_encoding = server_caps_raw.get("positionEncoding", "utf-16")
        if not isinstance(position_encoding, str):
            position_encoding = "utf-16"

        config_fingerprint = hashlib.sha256(
            json.dumps(self._config_response_cache, sort_keys=True).encode()
        ).hexdigest()[:16]
        server_name_raw = server_info.get("name")
        server_name = server_name_raw if isinstance(server_name_raw, str) else None
        server_version_raw = server_info.get("version")
        server_version = server_version_raw if isinstance(server_version_raw, str) else None

        self._session_env = LspSessionEnvV1(
            server_name=server_name,
            server_version=server_version,
            position_encoding=position_encoding,
            capabilities=_capability_snapshot_from_server_caps(server_caps_raw),
            workspace_health="unknown",
            quiescent=False,
            config_fingerprint=config_fingerprint,
        )

        self._send_notification("initialized", {})
        self._wait_for_quiescence(timeout=max(timeout_seconds, _DEFAULT_QUIESCENCE_TIMEOUT_SECONDS))

    def _wait_for_quiescence(self, timeout: float = _DEFAULT_QUIESCENCE_TIMEOUT_SECONDS) -> None:
        start = time.time()
        while time.time() - start < timeout:
            notifications = self._read_pending_notifications(timeout_seconds=0.1)
            for notif in notifications:
                self._handle_notification(notif)
                if self._session_env.quiescent:
                    return
            if not notifications:
                time.sleep(0.05)

    def _update_server_status(self, params: dict[str, object]) -> None:
        reported_health = params.get("health")
        health: Literal["ok", "warning", "error", "unknown"] = self._session_env.workspace_health
        if reported_health in {"ok", "warning", "error", "unknown"}:
            health = cast('Literal["ok", "warning", "error", "unknown"]', reported_health)

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

    def _record_refresh_event(self, method: str) -> None:
        current = set(self._session_env.refresh_events)
        if method in current:
            return
        current.add(method)
        self._session_env = LspSessionEnvV1(
            server_name=self._session_env.server_name,
            server_version=self._session_env.server_version,
            position_encoding=self._session_env.position_encoding,
            capabilities=self._session_env.capabilities,
            workspace_health=self._session_env.workspace_health,
            quiescent=self._session_env.quiescent,
            config_fingerprint=self._session_env.config_fingerprint,
            refresh_events=tuple(sorted(current)),
        )

    def _handle_diagnostics_notification(self, notif: dict[str, object]) -> None:
        params = notif.get("params", {})
        if not isinstance(params, dict):
            return

        uri = params.get("uri")
        diagnostics_data = params.get("diagnostics", [])
        version_value = params.get("version")

        if not isinstance(uri, str) or not isinstance(diagnostics_data, list):
            return

        diagnostics: list[RustDiagnosticV1] = []
        for diag in diagnostics_data:
            if not isinstance(diag, dict):
                continue

            range_data = diag.get("range")
            if not isinstance(range_data, dict):
                continue
            start = range_data.get("start")
            end = range_data.get("end")
            if not isinstance(start, dict) or not isinstance(end, dict):
                continue

            related_info_raw = diag.get("relatedInformation", [])
            related_info: tuple[dict[str, object], ...] = ()
            if isinstance(related_info_raw, list):
                related_info = tuple(item for item in related_info_raw if isinstance(item, dict))

            data_raw = diag.get("data")
            data_dict: dict[str, object] | None = (
                dict(data_raw) if isinstance(data_raw, dict) else None
            )
            if data_dict is None and isinstance(version_value, int):
                data_dict = {"diagnostic_version": version_value}
            message_raw = diag.get("message")
            message = message_raw if isinstance(message_raw, str) else ""

            diagnostics.append(
                RustDiagnosticV1(
                    uri=uri,
                    range_start_line=start.get("line", 0)
                    if isinstance(start.get("line"), int)
                    else 0,
                    range_start_col=(
                        start.get("character", 0) if isinstance(start.get("character"), int) else 0
                    ),
                    range_end_line=end.get("line", 0) if isinstance(end.get("line"), int) else 0,
                    range_end_col=(
                        end.get("character", 0) if isinstance(end.get("character"), int) else 0
                    ),
                    severity=diag.get("severity", 0)
                    if isinstance(diag.get("severity"), int)
                    else 0,
                    code=diag.get("code") if isinstance(diag.get("code"), str) else None,
                    source=diag.get("source") if isinstance(diag.get("source"), str) else None,
                    message=message,
                    related_info=related_info,
                    data=data_dict,
                )
            )

        self._diagnostics_by_uri[uri] = diagnostics

    def _handle_notification(self, notif: dict[str, object]) -> None:
        method = notif.get("method")
        if not isinstance(method, str):
            return
        if method == "experimental/serverStatus":
            params = notif.get("params")
            if isinstance(params, dict):
                self._update_server_status(params)
            return
        if method == "textDocument/publishDiagnostics":
            self._handle_diagnostics_notification(notif)
            return
        if method.endswith("/refresh"):
            self._record_refresh_event(method)

    def _open_or_update_document(self, file_path: Path) -> str:
        process = self._process
        if process is None or process.poll() is not None:
            msg = "rust-analyzer process not running"
            raise _LspProtocolError(msg)

        absolute = file_path.resolve()
        uri = absolute.as_uri()
        text = absolute.read_text(encoding="utf-8", errors="replace")
        content_hash = hash(text)
        state = self._docs.get(uri)

        if state is None:
            self._send_notification(
                "textDocument/didOpen",
                {
                    "textDocument": {
                        "uri": uri,
                        "languageId": "rust",
                        "version": 1,
                        "text": text,
                    }
                },
            )
            self._docs[uri] = _SessionDocState(version=1, content_hash=content_hash)
            return uri

        if state.content_hash != content_hash:
            version = state.version + 1
            self._send_notification(
                "textDocument/didChange",
                {
                    "textDocument": {"uri": uri, "version": version},
                    "contentChanges": [{"text": text}],
                },
            )
            self._docs[uri] = _SessionDocState(version=version, content_hash=content_hash)
        return uri

    def probe(self, request: RustLspRequest) -> RustLspEnrichmentPayload | None:
        if self._process is None or isinstance(self._process.poll(), int):
            return None
        degrade_events: list[dict[str, object]] = []
        uri_str = self._resolve_probe_uri(request, degrade_events)
        self._drain_notifications()

        responses = self._collect_probe_responses(request, uri_str, degrade_events)
        self._collect_probe_followup_responses(responses, degrade_events)
        self._drain_notifications()

        raw_payload = self._build_probe_payload(
            request=request,
            uri_str=uri_str,
            responses=responses,
            degrade_events=degrade_events,
        )
        raw_payload["advanced_planes"] = self._collect_advanced_planes(request, uri=uri_str)
        return coerce_rust_lsp_payload(raw_payload)

    def _resolve_probe_uri(
        self,
        request: RustLspRequest,
        degrade_events: list[dict[str, object]],
    ) -> str:
        try:
            return self._open_or_update_document(Path(request.file_path))
        except _FAIL_OPEN_EXCEPTIONS as exc:
            degrade_events.append(
                {
                    "stage": "lsp.rust",
                    "severity": "warning",
                    "category": "document_open_failed",
                    "message": f"textDocument open/update failed: {type(exc).__name__}",
                }
            )
            return Path(request.file_path).resolve().as_uri()

    def _drain_notifications(self) -> None:
        for notif in self._read_pending_notifications(timeout_seconds=0.0):
            self._handle_notification(notif)

    def _collect_probe_responses(
        self,
        request: RustLspRequest,
        uri_str: str,
        degrade_events: list[dict[str, object]],
    ) -> dict[str, object]:
        health = self._session_env.workspace_health
        caps = self._session_env.capabilities.server_caps
        requests = _build_probe_requests(
            uri=uri_str,
            line=request.line,
            col=request.col,
            health=health,
            quiescent=self._session_env.quiescent,
            capabilities=caps,
        )
        responses: dict[str, object] = {}
        for name, method, params in requests:
            self._send_probe_request(
                responses,
                name=name,
                method=method,
                params=params,
                degrade_events=degrade_events,
            )
        return responses

    def _send_probe_request(
        self,
        responses: dict[str, object],
        *,
        name: str,
        method: str,
        params: dict[str, object],
        degrade_events: list[dict[str, object]],
        severity: str = "warning",
    ) -> None:
        try:
            responses[name] = self._send_request(method, params)
        except _FAIL_OPEN_EXCEPTIONS as exc:
            responses[name] = None
            degrade_events.append(
                {
                    "stage": "lsp.rust",
                    "severity": severity,
                    "category": "request_failed",
                    "message": f"{method} failed: {type(exc).__name__}",
                }
            )

    def _collect_probe_followup_responses(
        self,
        responses: dict[str, object],
        degrade_events: list[dict[str, object]],
    ) -> None:
        call_item = _first_item(responses.get("call_prepare"))
        if call_item is not None:
            self._send_probe_request(
                responses,
                name="incoming_calls",
                method="callHierarchy/incomingCalls",
                params={"item": call_item},
                degrade_events=degrade_events,
                severity="info",
            )
            self._send_probe_request(
                responses,
                name="outgoing_calls",
                method="callHierarchy/outgoingCalls",
                params={"item": call_item},
                degrade_events=degrade_events,
                severity="info",
            )
        type_item = _first_item(responses.get("type_prepare"))
        if type_item is None:
            return
        self._send_probe_request(
            responses,
            name="supertypes",
            method="typeHierarchy/supertypes",
            params={"item": type_item},
            degrade_events=degrade_events,
            severity="info",
        )
        self._send_probe_request(
            responses,
            name="subtypes",
            method="typeHierarchy/subtypes",
            params={"item": type_item},
            degrade_events=degrade_events,
            severity="info",
        )

    def _build_probe_payload(
        self,
        *,
        request: RustLspRequest,
        uri_str: str,
        responses: Mapping[str, object],
        degrade_events: list[dict[str, object]],
    ) -> dict[str, object]:
        return {
            "session_env": _session_env_to_mapping(self._session_env),
            "symbol_grounding": {
                "definitions": _normalize_targets(responses.get("definition")),
                "type_definitions": _normalize_targets(responses.get("type_definition")),
                "implementations": _normalize_targets(responses.get("implementation")),
                "references": _normalize_targets(responses.get("references")),
            },
            "call_graph": {
                "incoming_callers": _normalize_call_links(responses.get("incoming_calls"), key="from"),
                "outgoing_callees": _normalize_call_links(responses.get("outgoing_calls"), key="to"),
            },
            "type_hierarchy": {
                "supertypes": _normalize_type_links(responses.get("supertypes")),
                "subtypes": _normalize_type_links(responses.get("subtypes")),
            },
            "document_symbols": _normalize_document_symbols(responses.get("document_symbols")),
            "diagnostics": [to_builtins(diag) for diag in self._diagnostics_by_uri.get(uri_str, [])],
            "hover_text": _normalize_hover_text(responses.get("hover")),
            "degrade_events": degrade_events,
            "query_intent": request.query_intent,
        }

    def _collect_advanced_planes(self, request: RustLspRequest, *, uri: str) -> dict[str, object]:
        try:
            return collect_advanced_lsp_planes(
                session=self,
                language="rust",
                uri=uri,
                line=max(0, request.line),
                col=max(0, request.col),
            )
        except _FAIL_OPEN_EXCEPTIONS:
            return {}

    def shutdown(self) -> None:
        if self._process is None:
            return
        process = self._process

        with contextlib.suppress(*_FAIL_OPEN_EXCEPTIONS):
            self._send_request("shutdown", {})
        with contextlib.suppress(*_FAIL_OPEN_EXCEPTIONS):
            self._send_notification("exit", {})

        if not isinstance(process.poll(), int):
            process.terminate()
            try:
                process.wait(timeout=5.0)
            except subprocess.TimeoutExpired:
                process.kill()
                with contextlib.suppress(subprocess.TimeoutExpired):
                    process.wait(timeout=1.0)

        if process.stdin is not None:
            with contextlib.suppress(OSError):
                process.stdin.close()
        if process.stdout is not None:
            with contextlib.suppress(OSError):
                process.stdout.close()
        self._process = None

    def capabilities_snapshot(self) -> dict[str, object]:
        """Return normalized rust-analyzer capability snapshot."""
        server = self._session_env.capabilities.server_caps
        return {
            "definitionProvider": server.definition_provider,
            "typeDefinitionProvider": server.type_definition_provider,
            "implementationProvider": server.implementation_provider,
            "referencesProvider": server.references_provider,
            "documentSymbolProvider": server.document_symbol_provider,
            "callHierarchyProvider": server.call_hierarchy_provider,
            "typeHierarchyProvider": server.type_hierarchy_provider,
            "hoverProvider": server.hover_provider,
            "workspaceSymbolProvider": server.workspace_symbol_provider,
            "renameProvider": server.rename_provider,
            "codeActionProvider": server.code_action_provider,
            "semanticTokensProvider": server.semantic_tokens_provider_raw,
            "inlayHintProvider": server.inlay_hint_provider,
            "diagnosticProvider": server.diagnostic_provider_raw,
            "workspaceDiagnosticProvider": server.workspace_diagnostic_provider_raw,
        }

    def _send_request(self, method: str, params: dict[str, object]) -> object:
        request_id = self._request(method, params)
        response = self._wait_for_response(request_id, timeout_seconds=_DEFAULT_TIMEOUT_SECONDS)
        if isinstance(response.get("error"), dict):
            empty_result: dict[str, object] = {}
            return empty_result
        return response.get("result", {})

    def _send_notification(self, method: str, params: dict[str, object]) -> None:
        self._notify(method, params)

    def _read_pending_notifications(self, timeout_seconds: float = 0.0) -> list[dict[str, object]]:
        notifications: list[dict[str, object]] = []
        start = monotonic()
        while True:
            remaining = timeout_seconds - (monotonic() - start)
            if timeout_seconds <= 0:
                remaining = 0.0
            if timeout_seconds > 0 and remaining < 0:
                break
            message = self._read_message(timeout_seconds=max(0.0, remaining))
            if message is None:
                break
            if isinstance(message.get("id"), int):
                continue
            notifications.append(message)
            if timeout_seconds <= 0:
                continue
        return notifications

    def _request(self, method: str, params: object) -> int:
        self._next_id += 1
        request_id = self._next_id
        self._send(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "method": method,
                "params": params,
            }
        )
        return request_id

    def _notify(self, method: str, params: object) -> None:
        self._send(
            {
                "jsonrpc": "2.0",
                "method": method,
                "params": params,
            }
        )

    def _send(self, payload: dict[str, object]) -> None:
        process = self._process
        if process is None or process.stdin is None or process.poll() is not None:
            msg = "Cannot send LSP message: process unavailable"
            raise _LspProtocolError(msg)
        body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        header = f"Content-Length: {len(body)}\r\n\r\n".encode("ascii")
        process.stdin.write(header)
        process.stdin.write(body)
        process.stdin.flush()

    def _wait_for_response(self, request_id: int, *, timeout_seconds: float) -> dict[str, object]:
        deadline = monotonic() + max(0.05, timeout_seconds)
        while True:
            remaining = deadline - monotonic()
            if remaining <= 0:
                msg = f"Timed out waiting for LSP response id={request_id}"
                raise TimeoutError(msg)
            message = self._read_message(timeout_seconds=remaining)
            if message is None:
                continue
            message_id = message.get("id")
            if isinstance(message_id, int) and message_id == request_id:
                return message
            self._handle_notification(message)

    def _read_message(self, *, timeout_seconds: float) -> dict[str, object] | None:
        selector = self._selector
        process = self._process
        if (
            selector is None
            or process is None
            or process.stdout is None
            or process.poll() is not None
        ):
            return None

        while True:
            parsed = _try_parse_message(self._buffer)
            if parsed is not None:
                return parsed

            events = selector.select(timeout=max(0.0, timeout_seconds))
            if not events:
                return None
            chunk = os.read(process.stdout.fileno(), 65536)
            if not chunk:
                msg = "rust-analyzer stream closed unexpectedly"
                raise _LspProtocolError(msg)
            self._buffer.extend(chunk)


_SESSION_MANAGER = LspSessionManager[_RustLspSession](
    make_session=_RustLspSession,
    close_session=lambda session: session.close(),
    ensure_started=lambda session, timeout: session.ensure_started(timeout_seconds=timeout),
)


def _session_for_root(
    root: Path,
    *,
    startup_timeout_seconds: float = _DEFAULT_STARTUP_TIMEOUT_SECONDS,
) -> _RustLspSession:
    return _SESSION_MANAGER.for_root(root, startup_timeout_seconds=startup_timeout_seconds)


def get_rust_lsp_capabilities(
    root: Path,
    *,
    startup_timeout_seconds: float = _DEFAULT_STARTUP_TIMEOUT_SECONDS,
) -> dict[str, object]:
    """Return negotiated rust-analyzer capabilities for workspace root."""
    try:
        session = _session_for_root(root, startup_timeout_seconds=startup_timeout_seconds)
    except _FAIL_OPEN_EXCEPTIONS:
        return {}
    return session.capabilities_snapshot()


def enrich_with_rust_lsp(
    request: RustLspRequest,
    *,
    root: Path | None = None,
    startup_timeout_seconds: float = _DEFAULT_STARTUP_TIMEOUT_SECONDS,
) -> dict[str, object] | None:
    """Fetch Rust LSP enrichment for one file anchor.

    Returns:
        Rust LSP enrichment payload for `.rs` anchors, or `None`.
    """
    file_path = Path(request.file_path)
    if file_path.suffix != ".rs":
        return None
    effective_root = root.resolve() if root is not None else file_path.resolve().parent
    try:
        session = _session_for_root(effective_root, startup_timeout_seconds=startup_timeout_seconds)
        payload = session.probe(request)
    except _FAIL_OPEN_EXCEPTIONS:
        return None
    if not isinstance(payload, RustLspEnrichmentPayload):
        return None
    return rust_lsp_payload_to_dict(payload)


def close_rust_lsp_sessions() -> None:
    """Close all cached Rust LSP sessions."""
    _SESSION_MANAGER.close_all()


def _try_parse_message(buffer: bytearray) -> dict[str, object] | None:
    sep = b"\r\n\r\n"
    header_end = buffer.find(sep)
    if header_end < 0:
        return None

    header_blob = bytes(buffer[:header_end]).decode("ascii", errors="replace")
    content_length: int | None = None
    for line in header_blob.split("\r\n"):
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        if key.strip().lower() != "content-length":
            continue
        try:
            content_length = int(value.strip())
        except ValueError:
            content_length = None
        break

    if content_length is None:
        msg = "Missing Content-Length in LSP headers"
        raise _LspProtocolError(msg)

    body_start = header_end + len(sep)
    body_end = body_start + content_length
    if len(buffer) < body_end:
        return None

    body = bytes(buffer[body_start:body_end])
    del buffer[:body_end]
    payload = json.loads(body.decode("utf-8", errors="replace"))
    if not isinstance(payload, dict):
        msg = "Invalid LSP payload type"
        raise _LspProtocolError(msg)
    return cast("dict[str, object]", payload)


def _capability_snapshot_from_server_caps(
    server_caps: dict[str, object],
) -> LspCapabilitySnapshotV1:
    return LspCapabilitySnapshotV1(
        server_caps=LspServerCapabilitySnapshotV1(
            definition_provider=bool(server_caps.get("definitionProvider")),
            type_definition_provider=bool(server_caps.get("typeDefinitionProvider")),
            implementation_provider=bool(server_caps.get("implementationProvider")),
            references_provider=bool(server_caps.get("referencesProvider")),
            document_symbol_provider=bool(server_caps.get("documentSymbolProvider")),
            call_hierarchy_provider=bool(server_caps.get("callHierarchyProvider")),
            type_hierarchy_provider=bool(server_caps.get("typeHierarchyProvider")),
            hover_provider=bool(server_caps.get("hoverProvider")),
            workspace_symbol_provider=bool(server_caps.get("workspaceSymbolProvider")),
            rename_provider=bool(server_caps.get("renameProvider")),
            code_action_provider=bool(server_caps.get("codeActionProvider")),
            semantic_tokens_provider=bool(server_caps.get("semanticTokensProvider")),
            inlay_hint_provider=bool(server_caps.get("inlayHintProvider")),
            diagnostic_provider=bool(server_caps.get("diagnosticProvider")),
            workspace_diagnostic_provider=bool(server_caps.get("workspaceDiagnosticProvider")),
            semantic_tokens_provider_raw=(
                dict(raw)
                if isinstance((raw := server_caps.get("semanticTokensProvider")), dict)
                else None
            ),
            code_action_provider_raw=server_caps.get("codeActionProvider"),
            workspace_symbol_provider_raw=server_caps.get("workspaceSymbolProvider"),
            diagnostic_provider_raw=server_caps.get("diagnosticProvider"),
            workspace_diagnostic_provider_raw=server_caps.get("workspaceDiagnosticProvider"),
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


def _session_env_to_mapping(env: LspSessionEnvV1) -> dict[str, object]:
    return {
        "server_name": env.server_name,
        "server_version": env.server_version,
        "position_encoding": env.position_encoding,
        "workspace_health": env.workspace_health,
        "quiescent": env.quiescent,
        "config_fingerprint": env.config_fingerprint,
        "refresh_events": list(env.refresh_events),
        "capabilities": {
            "server_caps": to_builtins(env.capabilities.server_caps),
            "client_caps": to_builtins(env.capabilities.client_caps),
            "experimental_caps": to_builtins(env.capabilities.experimental_caps),
        },
    }


def _first_item(value: object) -> dict[str, object] | None:
    if isinstance(value, dict):
        return cast("dict[str, object]", value)
    if isinstance(value, list) and value and isinstance(value[0], dict):
        return cast("dict[str, object]", value[0])
    return None


def _normalize_targets(value: object) -> list[dict[str, object]]:
    if value is None:
        return []
    if isinstance(value, dict):
        rows = [value]
    elif isinstance(value, list):
        rows = [item for item in value if isinstance(item, dict)]
    else:
        return []

    targets: list[dict[str, object]] = []
    for row in rows:
        target = _normalize_target_row(cast("dict[str, object]", row))
        if target is not None:
            targets.append(target)
    return targets


def _build_probe_requests(
    *,
    uri: str,
    line: int,
    col: int,
    health: str,
    quiescent: bool,
    capabilities: LspServerCapabilitySnapshotV1,
) -> list[tuple[str, str, dict[str, object]]]:
    position = {
        "line": max(0, line - 1),
        "character": max(0, col),
    }
    text_document = {"uri": uri}
    tdp = {"textDocument": text_document, "position": position}
    requests: list[tuple[str, str, dict[str, object]]] = []
    requests.extend(_tier_a_probe_requests(capabilities, tdp))
    requests.extend(_tier_b_probe_requests(capabilities, health, text_document, position))
    requests.extend(_tier_c_probe_requests(capabilities, health=health, quiescent=quiescent, tdp=tdp))
    return requests


def _tier_a_probe_requests(
    capabilities: LspServerCapabilitySnapshotV1,
    tdp: Mapping[str, object],
) -> list[tuple[str, str, dict[str, object]]]:
    tdp_payload = dict(tdp)
    requests: list[tuple[str, str, dict[str, object]]] = []
    if capabilities.hover_provider:
        requests.append(("hover", "textDocument/hover", dict(tdp_payload)))
    if capabilities.definition_provider:
        requests.append(("definition", "textDocument/definition", dict(tdp_payload)))
    if capabilities.type_definition_provider:
        requests.append(("type_definition", "textDocument/typeDefinition", dict(tdp_payload)))
    return requests


def _tier_b_probe_requests(
    capabilities: LspServerCapabilitySnapshotV1,
    health: str,
    text_document: Mapping[str, object],
    position: Mapping[str, object],
) -> list[tuple[str, str, dict[str, object]]]:
    if health not in {"ok", "warning"}:
        return []
    requests: list[tuple[str, str, dict[str, object]]] = []
    if capabilities.references_provider:
        requests.append(
            (
                "references",
                "textDocument/references",
                {
                    "textDocument": text_document,
                    "position": position,
                    "context": {"includeDeclaration": False},
                },
            )
        )
    if capabilities.document_symbol_provider:
        requests.append(
            (
                "document_symbols",
                "textDocument/documentSymbol",
                {"textDocument": text_document},
            )
        )
    return requests


def _tier_c_probe_requests(
    capabilities: LspServerCapabilitySnapshotV1,
    *,
    health: str,
    quiescent: bool,
    tdp: Mapping[str, object],
) -> list[tuple[str, str, dict[str, object]]]:
    if health != "ok" or not quiescent:
        return []
    tdp_payload = dict(tdp)
    requests: list[tuple[str, str, dict[str, object]]] = []
    if capabilities.call_hierarchy_provider:
        requests.append(("call_prepare", "textDocument/prepareCallHierarchy", dict(tdp_payload)))
    if capabilities.type_hierarchy_provider:
        requests.append(("type_prepare", "textDocument/prepareTypeHierarchy", dict(tdp_payload)))
    return requests


def _normalize_target_row(row: dict[str, object]) -> dict[str, object] | None:
    uri = row.get("uri")
    range_data = row.get("range")

    if not isinstance(uri, str):
        target_uri = row.get("targetUri")
        if isinstance(target_uri, str):
            uri = target_uri
            range_data = row.get("targetSelectionRange") or row.get("targetRange")

    if not isinstance(uri, str) or not isinstance(range_data, dict):
        return None
    start = range_data.get("start")
    end = range_data.get("end")
    if not isinstance(start, dict) or not isinstance(end, dict):
        return None

    return {
        "uri": uri,
        "range_start_line": _as_int(start.get("line")),
        "range_start_col": _as_int(start.get("character")),
        "range_end_line": _as_int(end.get("line")),
        "range_end_col": _as_int(end.get("character")),
    }


def _normalize_call_links(value: object, *, key: str) -> list[dict[str, object]]:
    if not isinstance(value, list):
        return []
    rows: list[dict[str, object]] = []
    for item in value:
        normalized = _normalize_call_link_item(item, key=key)
        if normalized is not None:
            rows.append(normalized)
    return rows


def _normalize_call_link_item(item: object, *, key: str) -> dict[str, object] | None:
    if not isinstance(item, dict):
        return None
    node = item.get(key)
    if not isinstance(node, dict):
        return None
    uri = node.get("uri")
    name = node.get("name")
    range_data = node.get("selectionRange") or node.get("range")
    if not (isinstance(uri, str) and isinstance(name, str) and isinstance(range_data, dict)):
        return None
    start = range_data.get("start")
    if not isinstance(start, dict):
        return None
    return {
        "name": name,
        "kind": _as_int(node.get("kind")),
        "uri": uri,
        "range_start_line": _as_int(start.get("line")),
        "range_start_col": _as_int(start.get("character")),
        "from_ranges": _normalize_from_ranges(item.get("fromRanges")),
    }


def _normalize_from_ranges(value: object) -> list[tuple[int, int, int, int]]:
    if not isinstance(value, list):
        return []
    ranges: list[tuple[int, int, int, int]] = []
    for from_range in value:
        normalized = _normalize_from_range(from_range)
        if normalized is not None:
            ranges.append(normalized)
    return ranges


def _normalize_from_range(value: object) -> tuple[int, int, int, int] | None:
    if not isinstance(value, dict):
        return None
    start = value.get("start")
    end = value.get("end")
    if not isinstance(start, dict) or not isinstance(end, dict):
        return None
    return (
        _as_int(start.get("line")),
        _as_int(start.get("character")),
        _as_int(end.get("line")),
        _as_int(end.get("character")),
    )


def _normalize_type_links(value: object) -> list[dict[str, object]]:
    if not isinstance(value, list):
        return []
    rows: list[dict[str, object]] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        uri = item.get("uri")
        name = item.get("name")
        range_data = item.get("selectionRange") or item.get("range")
        if (
            not isinstance(uri, str)
            or not isinstance(name, str)
            or not isinstance(range_data, dict)
        ):
            continue
        start = range_data.get("start")
        if not isinstance(start, dict):
            continue
        rows.append(
            {
                "name": name,
                "kind": _as_int(item.get("kind")),
                "uri": uri,
                "range_start_line": _as_int(start.get("line")),
                "range_start_col": _as_int(start.get("character")),
            }
        )
    return rows


def _normalize_document_symbols(value: object) -> list[dict[str, object]]:
    if not isinstance(value, list):
        return []
    symbols: list[dict[str, object]] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        symbol = _normalize_document_symbol(item)
        if symbol is not None:
            symbols.append(symbol)
    return symbols


def _normalize_document_symbol(value: dict[str, object]) -> dict[str, object] | None:
    name = value.get("name")
    kind = value.get("kind")
    if not isinstance(name, str):
        return None

    range_data = value.get("range")
    if not isinstance(range_data, dict):
        location = value.get("location")
        if isinstance(location, dict):
            range_data = location.get("range")
    if not isinstance(range_data, dict):
        return {
            "name": name,
            "kind": _as_int(kind),
            "range_start_line": 0,
            "range_start_col": 0,
            "range_end_line": 0,
            "range_end_col": 0,
            "children": [],
        }

    start = range_data.get("start")
    end = range_data.get("end")
    if not isinstance(start, dict) or not isinstance(end, dict):
        return None

    children_raw = value.get("children")
    children: list[dict[str, object]] = []
    if isinstance(children_raw, list):
        for child in children_raw:
            if not isinstance(child, dict):
                continue
            normalized = _normalize_document_symbol(child)
            if normalized is not None:
                children.append(normalized)

    return {
        "name": name,
        "kind": _as_int(kind),
        "range_start_line": _as_int(start.get("line")),
        "range_start_col": _as_int(start.get("character")),
        "range_end_line": _as_int(end.get("line")),
        "range_end_col": _as_int(end.get("character")),
        "children": children,
    }


def _normalize_hover_text(value: object) -> str | None:
    if not isinstance(value, dict):
        return None
    contents = value.get("contents")
    return _render_hover_contents(contents) or None


def _render_hover_contents(value: object) -> str:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, dict):
        text = value.get("value")
        if isinstance(text, str):
            return text.strip()
        return ""
    if isinstance(value, list):
        parts = [_render_hover_contents(item) for item in value]
        return "\n".join(part for part in parts if part).strip()
    return ""


def _as_int(value: object, default: int = 0) -> int:
    if isinstance(value, bool):
        return default
    if isinstance(value, int):
        return value
    return default


atexit.register(close_rust_lsp_sessions)


__all__ = [
    "RustLspRequest",
    "close_rust_lsp_sessions",
    "enrich_with_rust_lsp",
    "get_rust_lsp_capabilities",
]
