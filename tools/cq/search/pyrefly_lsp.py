"""Pyrefly LSP enrichment helpers for CQ search findings.

This module is intentionally fail-open:
- any transport/protocol/parsing failure returns ``None`` to callers
- no exception should escape to search/query/run pipelines
"""

from __future__ import annotations

import atexit
import contextlib
import json
import os
import selectors
import subprocess
import threading
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from time import monotonic
from typing import Literal, cast

from tools.cq.core.structs import CqStruct
from tools.cq.search.lsp.capabilities import supports_method
from tools.cq.search.lsp.position_encoding import from_lsp_character, to_lsp_character
from tools.cq.search.lsp.session_manager import LspSessionManager
from tools.cq.search.lsp_advanced_planes import collect_advanced_lsp_planes
from tools.cq.search.pyrefly_contracts import (
    coerce_pyrefly_payload,
    pyrefly_payload_to_dict,
)
from tools.cq.search.pyrefly_signal import evaluate_pyrefly_signal_from_mapping

LspSeverity = Literal["error", "warning", "info", "hint"]

_DEFAULT_TIMEOUT_SECONDS = 1.0
_DEFAULT_STARTUP_TIMEOUT_SECONDS = 3.0
_DEFAULT_MAX_CALLERS = 8
_DEFAULT_MAX_CALLEES = 8
_DEFAULT_MAX_DIAGNOSTICS = 6
_DEFAULT_MAX_REFERENCES = 16
_DEFAULT_ENRICH_ATTEMPTS = 2
_SIGNATURE_RANGE_LEN = 2
_SEVERITY_ERROR = 1
_SEVERITY_WARNING = 2
_SEVERITY_INFO = 3
_SEVERITY_HINT = 4
_PYREFLY_FAIL_OPEN_EXCEPTIONS = (
    OSError,
    RuntimeError,
    TimeoutError,
    TypeError,
    ValueError,
)

_CharacterConverterFn = Callable[[str, int, int], int]


class PyreflyLspRequest(CqStruct, frozen=True):
    """Request envelope for per-anchor Pyrefly enrichment."""

    root: Path
    file_path: Path
    line: int
    col: int
    symbol_hint: str | None = None
    timeout_seconds: float = _DEFAULT_TIMEOUT_SECONDS
    startup_timeout_seconds: float = _DEFAULT_STARTUP_TIMEOUT_SECONDS
    max_callers: int = _DEFAULT_MAX_CALLERS
    max_callees: int = _DEFAULT_MAX_CALLEES
    max_diagnostics: int = _DEFAULT_MAX_DIAGNOSTICS
    max_references: int = _DEFAULT_MAX_REFERENCES


@dataclass(slots=True)
class _SessionDocState:
    version: int
    content_hash: int


@dataclass(frozen=True, slots=True)
class _PositionPayload:
    uri: str
    line: int
    character: int

    def as_tdp(self) -> dict[str, object]:
        return {
            "textDocument": {"uri": self.uri},
            "position": {"line": self.line, "character": self.character},
        }

    def references_payload(self) -> dict[str, object]:
        return {
            "textDocument": {"uri": self.uri},
            "position": {"line": self.line, "character": self.character},
            "context": {"includeDeclaration": False},
        }


@dataclass(frozen=True, slots=True)
class _ProbePayloadContext:
    uri: str
    responses: Mapping[str, object]
    call_item: Mapping[str, object] | None
    incoming: list[dict[str, object]]
    outgoing: list[dict[str, object]]
    convert_character: _CharacterConverterFn


class _LspProtocolError(RuntimeError):
    """Raised when framing/protocol invariants are violated."""


class _PyreflyLspSession:
    """Minimal stdio JSON-RPC client for ``pyrefly lsp``."""

    def __init__(self, root: Path) -> None:
        self.root = root.resolve()
        self._lock = threading.RLock()
        self._proc: subprocess.Popen[bytes] | None = None
        self._selector: selectors.BaseSelector | None = None
        self._buffer = bytearray()
        self._next_id = 0
        self._position_encoding = "utf-16"
        self._server_capabilities: dict[str, object] = {}
        self._docs: dict[str, _SessionDocState] = {}
        self._diagnostics_by_uri: dict[str, list[dict[str, object]]] = {}

    @property
    def is_running(self) -> bool:
        return self._proc is not None and self._proc.poll() is None

    def ensure_started(self, *, timeout_seconds: float) -> None:
        with self._lock:
            if self.is_running:
                return
            self._start(timeout_seconds=timeout_seconds)

    def close(self) -> None:
        with self._lock:
            proc = self._proc
            selector = self._selector
            if proc is None:
                return
            if proc.poll() is None:
                try:
                    shutdown_id = self._request("shutdown", None)
                    self._wait_for_response(shutdown_id, timeout_seconds=0.2)
                except _PYREFLY_FAIL_OPEN_EXCEPTIONS:
                    pass
                with contextlib.suppress(*_PYREFLY_FAIL_OPEN_EXCEPTIONS):
                    self._notify("exit", None)
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=0.4)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    with contextlib.suppress(subprocess.TimeoutExpired):
                        proc.wait(timeout=0.4)
            if proc.stdin is not None:
                with contextlib.suppress(OSError):
                    proc.stdin.close()
            if proc.stdout is not None:
                with contextlib.suppress(OSError):
                    proc.stdout.close()
            if selector is not None:
                with contextlib.suppress(OSError):
                    selector.close()
            self._proc = None
            self._selector = None
            self._buffer = bytearray()
            self._docs.clear()
            self._diagnostics_by_uri.clear()
            self._server_capabilities = {}

    def probe(self, request: PyreflyLspRequest) -> dict[str, object] | None:
        self.ensure_started(timeout_seconds=request.timeout_seconds)
        proc = self._proc
        if proc is None or proc.poll() is not None:
            return _empty_probe_payload(
                reason="session_unavailable",
                position_encoding=self._position_encoding,
            )

        try:
            uri, text = self._open_or_update_document(request.file_path)
        except _PYREFLY_FAIL_OPEN_EXCEPTIONS:
            return _empty_probe_payload(
                reason="document_open_failed",
                position_encoding=self._position_encoding,
            )

        if not callable(getattr(self, "_send_request", None)):
            return _empty_probe_payload(
                reason="request_interface_unavailable",
                position_encoding=self._position_encoding,
            )
        if not self._supports_probe_methods():
            return _empty_probe_payload(
                reason="unsupported_capability",
                position_encoding=self._position_encoding,
            )

        position_payload = _build_position_payload(
            request,
            uri=uri,
            document_text=text,
            position_encoding=self._position_encoding,
        )
        character_converter = _CharacterPositionConverter(
            root=self.root,
            position_encoding=self._position_encoding,
            documents={uri: text},
        )
        responses = self._collect_probe_responses(
            position_payload,
            timeout_seconds=request.timeout_seconds,
        )
        incoming, outgoing, call_item = self._collect_probe_call_graph(
            request,
            responses=responses,
            convert_character=character_converter.from_lsp,
        )
        payload = self._build_probe_payload(
            request,
            context=_ProbePayloadContext(
                uri=uri,
                responses=responses,
                call_item=call_item,
                incoming=incoming,
                outgoing=outgoing,
                convert_character=character_converter.from_lsp,
            ),
        )
        payload["advanced_planes"] = self._collect_advanced_planes(
            uri=uri,
            line=position_payload.line,
            col=position_payload.character,
        )

        has_signal, signal_reasons = evaluate_pyrefly_signal_from_mapping(payload)
        coverage_reason = None if has_signal else f"no_pyrefly_signal:{','.join(signal_reasons)}"
        payload["coverage"] = {
            "status": "applied" if has_signal else "not_resolved",
            "reason": coverage_reason,
            "position_encoding": self._position_encoding,
        }
        return payload

    def _collect_probe_responses(
        self,
        position_payload: _PositionPayload,
        *,
        timeout_seconds: float,
    ) -> dict[str, object]:
        request_map = {
            "hover": ("textDocument/hover", position_payload.as_tdp()),
            "definition": ("textDocument/definition", position_payload.as_tdp()),
            "declaration": ("textDocument/declaration", position_payload.as_tdp()),
            "type_definition": ("textDocument/typeDefinition", position_payload.as_tdp()),
            "implementation": ("textDocument/implementation", position_payload.as_tdp()),
            "signature_help": ("textDocument/signatureHelp", position_payload.as_tdp()),
            "references": ("textDocument/references", position_payload.references_payload()),
            "call_prepare": ("textDocument/prepareCallHierarchy", position_payload.as_tdp()),
        }
        return self._request_many(request_map, timeout_seconds=timeout_seconds)

    def _collect_probe_call_graph(
        self,
        request: PyreflyLspRequest,
        *,
        responses: Mapping[str, object],
        convert_character: _CharacterConverterFn,
    ) -> tuple[list[dict[str, object]], list[dict[str, object]], dict[str, object] | None]:
        call_item = _first_call_hierarchy_item(responses.get("call_prepare"))
        if call_item is None:
            return [], [], None
        try:
            call_responses = self._request_many(
                {
                    "incoming": ("callHierarchy/incomingCalls", {"item": call_item}),
                    "outgoing": ("callHierarchy/outgoingCalls", {"item": call_item}),
                },
                timeout_seconds=request.timeout_seconds,
            )
        except _PYREFLY_FAIL_OPEN_EXCEPTIONS:
            return [], [], call_item
        incoming = _normalize_call_hierarchy_incoming(
            call_responses.get("incoming"),
            limit=request.max_callers,
            convert_character=convert_character,
        )
        outgoing = _normalize_call_hierarchy_outgoing(
            call_responses.get("outgoing"),
            limit=request.max_callees,
            convert_character=convert_character,
        )
        return incoming, outgoing, call_item

    def _build_probe_payload(
        self,
        request: PyreflyLspRequest,
        *,
        context: _ProbePayloadContext,
    ) -> dict[str, object]:
        references = _normalize_reference_locations(
            context.responses.get("references"),
            limit=request.max_references,
            convert_character=context.convert_character,
        )
        declaration_targets = _normalize_targets(
            context.responses.get("declaration"),
            kind="declaration",
            convert_character=context.convert_character,
        )
        implementation_targets = _normalize_targets(
            context.responses.get("implementation"),
            kind="implementation",
            convert_character=context.convert_character,
        )
        diagnostics = _normalize_anchor_diagnostics(
            self._diagnostics_by_uri.get(context.uri, []),
            line=max(0, request.line - 1),
            max_items=request.max_diagnostics,
            uri=context.uri,
            convert_character=context.convert_character,
        )
        return {
            "symbol_grounding": {
                "definition_targets": _normalize_targets(
                    context.responses.get("definition"),
                    kind="definition",
                    convert_character=context.convert_character,
                ),
                "declaration_targets": declaration_targets,
                "type_definition_targets": _normalize_targets(
                    context.responses.get("type_definition"),
                    kind="type_definition",
                    convert_character=context.convert_character,
                ),
                "implementation_targets": implementation_targets,
            },
            "type_contract": _normalize_type_contract(
                hover=context.responses.get("hover"),
                signature_help=context.responses.get("signature_help"),
            ),
            "call_graph": {
                "incoming_callers": context.incoming,
                "outgoing_callees": context.outgoing,
                "incoming_total": len(context.incoming),
                "outgoing_total": len(context.outgoing),
            },
            "class_method_context": _normalize_class_method_context(
                hover=context.responses.get("hover"),
                call_item=context.call_item,
                implementations=implementation_targets,
            ),
            "local_scope_context": {
                "same_scope_symbols": list[object](),
                "nearest_assignments": list[object](),
                "narrowing_hints": list[object](),
                "reference_locations": references,
            },
            "import_alias_resolution": _normalize_import_alias_resolution(
                declaration_targets=declaration_targets,
                symbol_hint=request.symbol_hint,
            ),
            "anchor_diagnostics": diagnostics,
        }

    def _collect_advanced_planes(
        self,
        *,
        uri: str,
        line: int,
        col: int,
    ) -> dict[str, object]:
        try:
            return collect_advanced_lsp_planes(
                session=self,
                language="python",
                uri=uri,
                line=max(0, line),
                col=max(0, col),
            )
        except _PYREFLY_FAIL_OPEN_EXCEPTIONS:
            return {}

    def _start(self, *, timeout_seconds: float) -> None:
        proc = subprocess.Popen(
            ["pyrefly", "lsp"],
            cwd=str(self.root),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        )
        if proc.stdin is None or proc.stdout is None:
            proc.kill()
            msg = "Failed to open stdio pipes for pyrefly lsp"
            raise _LspProtocolError(msg)

        selector = selectors.DefaultSelector()
        selector.register(proc.stdout, selectors.EVENT_READ)

        self._proc = proc
        self._selector = selector
        self._buffer = bytearray()
        self._next_id = 0
        self._diagnostics_by_uri.clear()

        init_id = self._request(
            "initialize",
            {
                "processId": os.getpid(),
                "rootUri": self.root.as_uri(),
                "capabilities": {
                    "general": {
                        "positionEncodings": ["utf-8", "utf-16"],
                    },
                    "textDocument": {
                        "definition": {"linkSupport": True},
                        "declaration": {"linkSupport": True},
                        "typeDefinition": {"linkSupport": True},
                        "implementation": {"linkSupport": True},
                        "documentSymbol": {"hierarchicalDocumentSymbolSupport": True},
                        "inlayHint": {"dynamicRegistration": False},
                        "semanticTokens": {
                            "dynamicRegistration": False,
                            "requests": {"full": True, "range": True},
                            "tokenTypes": [],
                            "tokenModifiers": [],
                            "formats": ["relative"],
                        },
                        "diagnostic": {"dynamicRegistration": False},
                    },
                    "workspace": {"diagnostics": {"refreshSupport": True}},
                },
            },
        )
        response = self._wait_for_response(init_id, timeout_seconds=timeout_seconds)
        result = response.get("result")
        if isinstance(result, Mapping):
            capabilities = result.get("capabilities")
            if isinstance(capabilities, Mapping):
                self._server_capabilities = dict(capabilities)
                position_encoding = capabilities.get("positionEncoding")
                if isinstance(position_encoding, str) and position_encoding:
                    self._position_encoding = position_encoding
        self._notify("initialized", {})

    def capabilities_snapshot(self) -> dict[str, object]:
        """Return negotiated server capabilities for this session."""
        return dict(self._server_capabilities)

    def _supports_probe_methods(self) -> bool:
        capabilities = self.capabilities_snapshot()
        required = (
            "textDocument/hover",
            "textDocument/definition",
            "textDocument/references",
        )
        return all(supports_method(capabilities, method) for method in required)

    def _open_or_update_document(self, file_path: Path) -> tuple[str, str]:
        with self._lock:
            proc = self._proc
            if proc is None or proc.poll() is not None:
                msg = "pyrefly lsp process not running"
                raise _LspProtocolError(msg)

            absolute = file_path.resolve()
            uri = absolute.as_uri()
            text = absolute.read_text(encoding="utf-8", errors="replace")
            content_hash = hash(text)
            state = self._docs.get(uri)
            if state is None:
                self._notify(
                    "textDocument/didOpen",
                    {
                        "textDocument": {
                            "uri": uri,
                            "languageId": "python",
                            "version": 1,
                            "text": text,
                        }
                    },
                )
                self._docs[uri] = _SessionDocState(version=1, content_hash=content_hash)
                return uri, text

            if state.content_hash != content_hash:
                version = state.version + 1
                self._notify(
                    "textDocument/didChange",
                    {
                        "textDocument": {"uri": uri, "version": version},
                        "contentChanges": [{"text": text}],
                    },
                )
                self._docs[uri] = _SessionDocState(version=version, content_hash=content_hash)
            return uri, text

    def _send_request(
        self,
        method: str,
        params: dict[str, object],
        *,
        timeout_seconds: float | None = None,
    ) -> object:
        with self._lock:
            request_id = self._request(method, params)
            response = self._wait_for_response(
                request_id,
                timeout_seconds=timeout_seconds
                if timeout_seconds is not None
                else _DEFAULT_TIMEOUT_SECONDS,
            )
            if isinstance(response.get("error"), Mapping):
                return None
            return response.get("result")

    def _request_many(
        self,
        requests: Mapping[str, tuple[str, dict[str, object]]],
        *,
        timeout_seconds: float,
    ) -> dict[str, object]:
        with self._lock:
            request_ids = {
                key: self._request(method, params) for key, (method, params) in requests.items()
            }
            responses = self._collect_responses(request_ids, timeout_seconds=timeout_seconds)
        return dict(responses)

    def _request(self, method: str, params: object) -> int:
        with self._lock:
            self._next_id += 1
            request_id = self._next_id
            self._send({"jsonrpc": "2.0", "id": request_id, "method": method, "params": params})
            return request_id

    def _notify(self, method: str, params: object) -> None:
        with self._lock:
            self._send({"jsonrpc": "2.0", "method": method, "params": params})

    def _send(self, payload: Mapping[str, object]) -> None:
        with self._lock:
            proc = self._proc
            if proc is None or proc.stdin is None or proc.poll() is not None:
                msg = "Cannot send LSP message: process unavailable"
                raise _LspProtocolError(msg)
            body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
            header = f"Content-Length: {len(body)}\r\n\r\n".encode("ascii")
            proc.stdin.write(header)
            proc.stdin.write(body)
            proc.stdin.flush()

    def _collect_responses(
        self,
        request_ids: Mapping[str, int],
        *,
        timeout_seconds: float,
    ) -> dict[str, object | None]:
        with self._lock:
            pending = {rid: name for name, rid in request_ids.items()}
            responses: dict[str, object | None] = dict.fromkeys(request_ids)
            deadline = monotonic() + max(0.05, timeout_seconds)
            while pending:
                remaining = deadline - monotonic()
                if remaining <= 0:
                    break
                message = self._read_message(timeout_seconds=remaining)
                if message is None:
                    continue
                message_id = message.get("id")
                if not isinstance(message_id, int):
                    self._handle_notification(message)
                    continue
                name = pending.pop(message_id, None)
                if name is None:
                    continue
                if isinstance(message.get("error"), Mapping):
                    responses[name] = None
                else:
                    responses[name] = message.get("result")
            return responses

    def _wait_for_response(self, request_id: int, *, timeout_seconds: float) -> dict[str, object]:
        with self._lock:
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
        with self._lock:
            selector = self._selector
            proc = self._proc
            if selector is None or proc is None or proc.stdout is None or proc.poll() is not None:
                return None

            while True:
                parsed = _try_parse_message(self._buffer)
                if parsed is not None:
                    return parsed
                events = selector.select(timeout=max(0.0, timeout_seconds))
                if not events:
                    return None
                chunk = os.read(proc.stdout.fileno(), 65536)
                if not chunk:
                    msg = "pyrefly lsp stream closed unexpectedly"
                    raise _LspProtocolError(msg)
                self._buffer.extend(chunk)

    def _handle_notification(self, message: Mapping[str, object]) -> None:
        with self._lock:
            method = message.get("method")
            if method != "textDocument/publishDiagnostics":
                return
            params = message.get("params")
            if not isinstance(params, Mapping):
                return
            uri = params.get("uri")
            diagnostics = params.get("diagnostics")
            if isinstance(uri, str) and isinstance(diagnostics, list):
                normalized = [item for item in diagnostics if isinstance(item, dict)]
                self._diagnostics_by_uri[uri] = cast("list[dict[str, object]]", normalized)


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
        if key.strip().lower() == "content-length":
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


class _CharacterPositionConverter:
    """Convert LSP character units to CQ character indices."""

    def __init__(
        self,
        *,
        root: Path,
        position_encoding: str,
        documents: Mapping[str, str],
    ) -> None:
        self._root = root
        self._position_encoding = position_encoding
        self._documents = dict(documents)
        self._line_cache: dict[tuple[str, int], str] = {}

    def from_lsp(self, uri: str, line: int, character: int) -> int:
        line_text = self._line_text(uri=uri, line=max(0, line))
        if not line_text and character > 0:
            return max(0, character)
        return from_lsp_character(
            line_text=line_text,
            character=max(0, character),
            encoding=self._position_encoding,
        )

    def _line_text(self, *, uri: str, line: int) -> str:
        cache_key = (uri, line)
        cached = self._line_cache.get(cache_key)
        if cached is not None:
            return cached
        text = self._documents.get(uri)
        if text is None:
            path = _uri_to_path(uri, root=self._root)
            if path is not None:
                try:
                    text = path.read_text(encoding="utf-8", errors="replace")
                except OSError:
                    text = ""
            else:
                text = ""
            self._documents[uri] = text
        lines = text.splitlines()
        line_text = lines[line] if 0 <= line < len(lines) else ""
        self._line_cache[cache_key] = line_text
        return line_text


def _empty_probe_payload(*, reason: str, position_encoding: str) -> dict[str, object]:
    return {
        "symbol_grounding": {
            "definition_targets": [],
            "declaration_targets": [],
            "type_definition_targets": [],
            "implementation_targets": [],
        },
        "type_contract": {
            "resolved_type": None,
            "callable_signature": None,
            "parameters": [],
            "active_signature_index": 0,
            "active_parameter_index": 0,
            "return_type": None,
            "generic_params": [],
            "is_async": False,
            "is_generator": False,
            "hover_summary": None,
        },
        "call_graph": {
            "incoming_callers": [],
            "outgoing_callees": [],
            "incoming_total": 0,
            "outgoing_total": 0,
        },
        "class_method_context": {
            "enclosing_class": None,
            "base_classes": [],
            "overridden_methods": [],
            "overriding_methods": [],
        },
        "local_scope_context": {
            "same_scope_symbols": [],
            "nearest_assignments": [],
            "narrowing_hints": [],
            "reference_locations": [],
        },
        "import_alias_resolution": {
            "resolved_path": None,
            "alias_chain": [],
        },
        "anchor_diagnostics": [],
        "coverage": {
            "status": "not_resolved",
            "reason": reason,
            "position_encoding": position_encoding,
        },
        "advanced_planes": {},
    }


def _first_call_hierarchy_item(value: object) -> dict[str, object] | None:
    if isinstance(value, list) and value:
        first = value[0]
        return cast("dict[str, object]", first) if isinstance(first, dict) else None
    if isinstance(value, dict):
        return cast("dict[str, object]", value)
    return None


def _normalize_targets(
    value: object,
    *,
    kind: str,
    convert_character: _CharacterConverterFn,
) -> list[dict[str, object]]:
    targets: list[dict[str, object]] = []
    if value is None:
        return targets
    if isinstance(value, dict):
        values: Sequence[object] = [value]
    elif isinstance(value, list):
        values = value
    else:
        return targets

    for item in values:
        if not isinstance(item, Mapping):
            continue
        target = _normalize_target(item, kind=kind, convert_character=convert_character)
        if target is not None:
            targets.append(target)
    return targets


def _normalize_target(
    item: Mapping[str, object],
    *,
    kind: str,
    convert_character: _CharacterConverterFn,
) -> dict[str, object] | None:
    uri = item.get("uri")
    range_obj = item.get("range")
    if not isinstance(uri, str):
        target_uri = item.get("targetUri")
        if isinstance(target_uri, str):
            uri = target_uri
            range_obj = item.get("targetSelectionRange") or item.get("targetRange")
    if not isinstance(uri, str) or not isinstance(range_obj, Mapping):
        return None

    start = range_obj.get("start")
    end = range_obj.get("end")
    if not isinstance(start, Mapping) or not isinstance(end, Mapping):
        return None

    line = _to_int(start.get("line"))
    col = _to_int(start.get("character"))
    end_line = _to_int(end.get("line"))
    end_col = _to_int(end.get("character"))
    file_value = _uri_to_file(uri)
    normalized_col = convert_character(uri, line, col)
    normalized_end_col = convert_character(uri, end_line, end_col)
    return {
        "kind": kind,
        "uri": uri,
        "file": file_value,
        "line": line + 1,
        "col": normalized_col,
        "end_line": end_line + 1,
        "end_col": normalized_end_col,
    }


def _normalize_type_contract(*, hover: object, signature_help: object) -> dict[str, object]:
    hover_text = _extract_hover_text(hover)
    signatures = _extract_signature_list(signature_help)
    active_sig_idx, active_param_idx = _extract_active_signature_indices(signature_help)
    active_sig_idx = min(active_sig_idx, len(signatures) - 1) if signatures else 0

    callable_signature: str | None = None
    if signatures:
        signature_label = signatures[active_sig_idx].get("label")
        if isinstance(signature_label, str):
            callable_signature = signature_label
    params: list[dict[str, object]] = []
    return_type: str | None = None
    if callable_signature and "->" in callable_signature:
        return_type = callable_signature.rsplit("->", maxsplit=1)[1].strip()
    if signatures:
        signature_params = signatures[active_sig_idx].get("parameters")
        if isinstance(signature_params, list):
            params = cast("list[dict[str, object]]", signature_params)

    return {
        "resolved_type": _extract_resolved_type(hover_text),
        "callable_signature": callable_signature,
        "parameters": params,
        "active_signature_index": active_sig_idx,
        "active_parameter_index": active_param_idx,
        "return_type": return_type,
        "generic_params": _extract_generic_params(callable_signature),
        "is_async": "async" in hover_text.lower() if hover_text else False,
        "is_generator": ("generator" in hover_text.lower() or "yield" in hover_text.lower())
        if hover_text
        else False,
        "hover_summary": hover_text,
    }


def _extract_hover_text(hover: object) -> str:
    if not isinstance(hover, Mapping):
        return ""
    contents = hover.get("contents")
    return _render_hover_contents(contents)


def _build_position_payload(
    request: PyreflyLspRequest,
    *,
    uri: str,
    document_text: str,
    position_encoding: str,
) -> _PositionPayload:
    line_index = max(0, request.line - 1)
    line_text = _line_text_at(document_text=document_text, line=line_index)
    return _PositionPayload(
        uri=uri,
        line=line_index,
        character=to_lsp_character(
            line_text=line_text,
            column=max(0, request.col),
            encoding=position_encoding,
        ),
    )


def _line_text_at(*, document_text: str, line: int) -> str:
    lines = document_text.splitlines()
    if 0 <= line < len(lines):
        return lines[line]
    return ""


def _render_hover_contents(contents: object) -> str:
    if isinstance(contents, str):
        return contents.strip()
    if isinstance(contents, Mapping):
        value = contents.get("value")
        return value.strip() if isinstance(value, str) else ""
    if isinstance(contents, list):
        parts: list[str] = []
        for item in contents:
            rendered = _render_hover_contents(item)
            if rendered:
                parts.append(rendered)
        return "\n".join(parts).strip()
    return ""


def _extract_resolved_type(hover_text: str) -> str | None:
    if not hover_text:
        return None
    lines = [line.strip() for line in hover_text.splitlines() if line.strip()]
    for line in lines:
        lowered = line.lower()
        if lowered.startswith("type:"):
            return line.split(":", 1)[1].strip() or None
    if lines:
        first = lines[0]
        if first.startswith("```") and len(lines) > 1:
            return lines[1]
        return first
    return None


def _extract_signature_list(signature_help: object) -> list[dict[str, object]]:
    if not isinstance(signature_help, Mapping):
        return []
    signatures = signature_help.get("signatures")
    if not isinstance(signatures, list):
        return []
    normalized: list[dict[str, object]] = []
    for sig in signatures:
        signature_row = _normalize_signature_row(sig)
        if signature_row is not None:
            normalized.append(signature_row)
    return normalized


def _normalize_signature_row(sig: object) -> dict[str, object] | None:
    if not isinstance(sig, Mapping):
        return None
    label = sig.get("label")
    if not isinstance(label, str):
        return None
    return {
        "label": label,
        "parameters": _normalize_signature_parameters(label, sig.get("parameters")),
    }


def _normalize_signature_parameters(label: str, raw_params: object) -> list[dict[str, object]]:
    if not isinstance(raw_params, list):
        return []
    params: list[dict[str, object]] = []
    for raw_param in raw_params:
        param = _normalize_signature_parameter(raw_param, signature_label=label)
        if param is not None:
            params.append(param)
    return params


def _normalize_signature_parameter(
    raw_param: object,
    *,
    signature_label: str,
) -> dict[str, object] | None:
    if not isinstance(raw_param, Mapping):
        return None
    param_label = raw_param.get("label")
    if isinstance(param_label, str):
        return {"label": param_label}
    if not (isinstance(param_label, list) and len(param_label) == _SIGNATURE_RANGE_LEN):
        return None
    start = _to_int(param_label[0])
    end = _to_int(param_label[1])
    label_slice = signature_label[start:end] if 0 <= start <= end <= len(signature_label) else ""
    return {
        "label": label_slice,
        "label_span_in_signature": [start, end],
    }


def _extract_active_signature_indices(signature_help: object) -> tuple[int, int]:
    if not isinstance(signature_help, Mapping):
        return 0, 0
    active_sig = _to_int(signature_help.get("activeSignature"))
    active_param = _to_int(signature_help.get("activeParameter"))
    active_sig = max(active_sig, 0)
    active_param = max(active_param, 0)
    return active_sig, active_param


def _extract_generic_params(signature: str | None) -> list[str]:
    if not signature:
        return []
    if "[" not in signature or "]" not in signature:
        return []
    left = signature.find("[")
    right = signature.find("]", left + 1)
    if left < 0 or right <= left:
        return []
    inner = signature[left + 1 : right]
    parts = [part.strip() for part in inner.split(",") if part.strip()]
    return parts[:8]


def _normalize_call_hierarchy_incoming(
    value: object,
    *,
    limit: int,
    convert_character: _CharacterConverterFn,
) -> list[dict[str, object]]:
    if not isinstance(value, list):
        return []
    rows: list[dict[str, object]] = []
    for item in value:
        if not isinstance(item, Mapping):
            continue
        from_item = item.get("from")
        row = _normalize_call_hierarchy_item(
            from_item,
            convert_character=convert_character,
        )
        if row is not None:
            rows.append(row)
        if len(rows) >= limit:
            break
    return rows


def _normalize_call_hierarchy_outgoing(
    value: object,
    *,
    limit: int,
    convert_character: _CharacterConverterFn,
) -> list[dict[str, object]]:
    if not isinstance(value, list):
        return []
    rows: list[dict[str, object]] = []
    for item in value:
        if not isinstance(item, Mapping):
            continue
        to_item = item.get("to")
        row = _normalize_call_hierarchy_item(
            to_item,
            convert_character=convert_character,
        )
        if row is not None:
            rows.append(row)
        if len(rows) >= limit:
            break
    return rows


def _normalize_call_hierarchy_item(
    value: object,
    *,
    convert_character: _CharacterConverterFn,
) -> dict[str, object] | None:
    if not isinstance(value, Mapping):
        return None
    name = value.get("name")
    uri = value.get("uri")
    range_obj = value.get("selectionRange") or value.get("range")
    if not isinstance(name, str) or not isinstance(uri, str) or not isinstance(range_obj, Mapping):
        return None
    start = range_obj.get("start")
    if not isinstance(start, Mapping):
        return None
    line = _to_int(start.get("line"))
    col = _to_int(start.get("character"))
    return {
        "symbol": name,
        "file": _uri_to_file(uri),
        "line": line + 1,
        "col": convert_character(uri, line, col),
    }


def _normalize_reference_locations(
    value: object,
    *,
    limit: int,
    convert_character: _CharacterConverterFn,
) -> list[dict[str, object]]:
    if not isinstance(value, list):
        return []
    refs: list[dict[str, object]] = []
    for item in value:
        if not isinstance(item, Mapping):
            continue
        normalized = _normalize_target(
            item,
            kind="reference",
            convert_character=convert_character,
        )
        if normalized is None:
            continue
        refs.append(
            {
                "file": normalized["file"],
                "line": normalized["line"],
                "col": normalized["col"],
            }
        )
        if len(refs) >= limit:
            break
    return refs


def _normalize_class_method_context(
    *,
    hover: object,
    call_item: Mapping[str, object] | None,
    implementations: list[dict[str, object]],
) -> dict[str, object]:
    hover_text = _extract_hover_text(hover)
    enclosing_class: str | None = None
    if isinstance(call_item, Mapping):
        name = call_item.get("name")
        if isinstance(name, str) and "." in name:
            enclosing_class = name.split(".", maxsplit=1)[0]
    if enclosing_class is None and "class " in hover_text:
        marker = hover_text.split("class ", maxsplit=1)[1]
        enclosing_class = marker.split()[0].strip("`:") if marker else None
    return {
        "enclosing_class": enclosing_class,
        "base_classes": [],
        "overridden_methods": [],
        "overriding_methods": [item.get("file") for item in implementations if item.get("file")],
    }


def _normalize_import_alias_resolution(
    *,
    declaration_targets: list[dict[str, object]],
    symbol_hint: str | None,
) -> dict[str, object]:
    resolved_path: str | None = None
    if declaration_targets:
        first = declaration_targets[0]
        file_path = first.get("file")
        if isinstance(file_path, str):
            resolved_path = file_path
    alias_chain: list[str] = (
        [symbol_hint] if isinstance(symbol_hint, str) and symbol_hint else list[str]()
    )
    return {
        "resolved_path": resolved_path,
        "alias_chain": alias_chain,
    }


def _normalize_anchor_diagnostics(
    diagnostics: list[dict[str, object]],
    *,
    line: int,
    max_items: int,
    uri: str,
    convert_character: _CharacterConverterFn,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for item in diagnostics:
        if not isinstance(item, Mapping):
            continue
        range_obj = item.get("range")
        if not isinstance(range_obj, Mapping):
            continue
        start = range_obj.get("start")
        end = range_obj.get("end")
        if not isinstance(start, Mapping) or not isinstance(end, Mapping):
            continue
        start_line = _to_int(start.get("line"))
        end_line = _to_int(end.get("line"))
        if not (start_line <= line <= end_line):
            continue
        severity = _normalize_severity(item.get("severity"))
        code = item.get("code")
        message = item.get("message")
        rows.append(
            {
                "severity": severity,
                "code": str(code) if code is not None else "",
                "message": str(message) if isinstance(message, str) else "",
                "line": start_line + 1,
                "col": convert_character(uri, start_line, _to_int(start.get("character"))),
            }
        )
        if len(rows) >= max_items:
            break
    return rows


def _normalize_severity(value: object) -> LspSeverity:
    if value == _SEVERITY_ERROR:
        return "error"
    if value == _SEVERITY_WARNING:
        return "warning"
    if value == _SEVERITY_INFO:
        return "info"
    if value == _SEVERITY_HINT:
        return "hint"
    return "info"


def _to_int(value: object) -> int:
    if isinstance(value, bool):
        return 0
    if isinstance(value, int):
        return value
    return 0


def _uri_to_file(uri: str) -> str:
    if uri.startswith("file://"):
        return uri.removeprefix("file://")
    return uri


def _uri_to_path(uri: str, *, root: Path) -> Path | None:
    file_path = _uri_to_file(uri)
    if not file_path:
        return None
    try:
        path = Path(file_path)
    except (OSError, RuntimeError, ValueError):
        return None
    if path.is_absolute():
        return path
    return root / path


_SESSION_MANAGER = LspSessionManager[_PyreflyLspSession](
    make_session=_PyreflyLspSession,
    close_session=lambda session: session.close(),
    ensure_started=lambda session, timeout: session.ensure_started(timeout_seconds=timeout),
)


def _session_for_root(
    root: Path,
    *,
    startup_timeout_seconds: float | None = None,
    timeout_seconds: float | None = None,
) -> _PyreflyLspSession:
    effective_timeout = (
        startup_timeout_seconds
        if startup_timeout_seconds is not None
        else timeout_seconds
        if timeout_seconds is not None
        else _DEFAULT_STARTUP_TIMEOUT_SECONDS
    )
    return _SESSION_MANAGER.for_root(root, startup_timeout_seconds=effective_timeout)


def enrich_with_pyrefly_lsp(request: PyreflyLspRequest) -> dict[str, object] | None:
    """Fetch Pyrefly LSP enrichment for one file anchor.

    Returns:
    -------
    dict[str, object] | None
        Normalized payload, or ``None`` when enrichment cannot be resolved.
    """
    if request.file_path.suffix not in {".py", ".pyi"}:
        return None
    attempts = max(1, _DEFAULT_ENRICH_ATTEMPTS)
    last_position_encoding = "utf-16"
    for attempt in range(attempts):
        try:
            session = _session_for_root(
                request.root,
                startup_timeout_seconds=request.startup_timeout_seconds,
            )
            last_position_encoding = str(getattr(session, "_position_encoding", "utf-16"))
            payload = session.probe(request)
        except (OSError, RuntimeError, TimeoutError, ValueError, TypeError):
            if attempt + 1 >= attempts:
                return _empty_probe_payload(
                    reason="session_unavailable",
                    position_encoding=last_position_encoding,
                )
            _SESSION_MANAGER.reset_root(request.root)
            continue

        if isinstance(payload, Mapping):
            typed_payload = coerce_pyrefly_payload(payload)
            return pyrefly_payload_to_dict(typed_payload)

        if session.is_running or attempt + 1 >= attempts:
            return _empty_probe_payload(
                reason="no_signal",
                position_encoding=last_position_encoding,
            )
        _SESSION_MANAGER.reset_root(request.root)
    return _empty_probe_payload(
        reason="session_unavailable",
        position_encoding=last_position_encoding,
    )


def get_pyrefly_lsp_capabilities(
    root: Path,
    *,
    startup_timeout_seconds: float = _DEFAULT_STARTUP_TIMEOUT_SECONDS,
) -> dict[str, object]:
    """Return negotiated Pyrefly server capabilities for the workspace."""
    try:
        session = _session_for_root(
            root,
            startup_timeout_seconds=startup_timeout_seconds,
        )
    except (OSError, RuntimeError, TimeoutError, ValueError, TypeError):
        return {}
    return session.capabilities_snapshot()


def close_pyrefly_lsp_sessions() -> None:
    """Close all cached Pyrefly LSP sessions (used by tests)."""
    _SESSION_MANAGER.close_all()


atexit.register(close_pyrefly_lsp_sessions)


__all__ = [
    "PyreflyLspRequest",
    "close_pyrefly_lsp_sessions",
    "enrich_with_pyrefly_lsp",
    "get_pyrefly_lsp_capabilities",
]
