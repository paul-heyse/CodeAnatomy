# ruff: noqa: C901, PLR0911, PLR0912, PLR0915
"""Scripted stdio JSON-RPC test server for CQ LSP session integration tests."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Mapping

_RUST_RUNTIME_STATE = {
    "refresh_acknowledged": False,
    "refresh_sent": False,
}
_RUST_REFRESH_REQUEST_ID = 91001


def _read_message() -> dict[str, object] | None:
    headers: dict[str, str] = {}
    while True:
        line = sys.stdin.buffer.readline()
        if not line:
            return None
        if line in {b"\r\n", b"\n"}:
            break
        text = line.decode("ascii", errors="replace").strip()
        if not text:
            continue
        if ":" not in text:
            continue
        key, value = text.split(":", 1)
        headers[key.strip().lower()] = value.strip()

    content_length_text = headers.get("content-length")
    if content_length_text is None:
        return None
    try:
        content_length = int(content_length_text)
    except ValueError:
        return None
    body = sys.stdin.buffer.read(content_length)
    if not body:
        return None
    payload = json.loads(body.decode("utf-8", errors="replace"))
    return payload if isinstance(payload, dict) else None


def _write_message(payload: Mapping[str, object]) -> None:
    body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    header = f"Content-Length: {len(body)}\r\n\r\n".encode("ascii")
    sys.stdout.buffer.write(header)
    sys.stdout.buffer.write(body)
    sys.stdout.buffer.flush()


def _location(uri: str, line: int, col: int = 0) -> dict[str, object]:
    return {
        "uri": uri,
        "range": {
            "start": {"line": line, "character": col},
            "end": {"line": line, "character": col + 3},
        },
    }


def _position_char(params: dict[str, object]) -> int:
    position = params.get("position")
    if not isinstance(position, dict):
        return 0
    raw_char = position.get("character")
    if isinstance(raw_char, int) and raw_char >= 0:
        return raw_char
    return 0


def _pyrefly_result(method: str, params: dict[str, object]) -> object:
    uri = "file:///tmp/module.py"
    text_document = params.get("textDocument")
    if isinstance(text_document, dict):
        uri_value = text_document.get("uri")
        if isinstance(uri_value, str):
            uri = uri_value
    position_char = _position_char(params)

    if method == "initialize":
        return {
            "capabilities": {
                "positionEncoding": "utf-16",
                "definitionProvider": True,
                "declarationProvider": True,
                "typeDefinitionProvider": True,
                "implementationProvider": True,
                "referencesProvider": True,
                "hoverProvider": True,
                "callHierarchyProvider": True,
                "inlayHintProvider": {"resolveProvider": True},
                "semanticTokensProvider": {
                    "legend": {
                        "tokenTypes": ["function"],
                        "tokenModifiers": ["declaration"],
                    },
                    "range": True,
                    "full": True,
                },
                "diagnosticProvider": {"identifier": "cq", "workspaceDiagnostics": True},
                "workspaceDiagnosticProvider": True,
            }
        }
    if method == "textDocument/hover":
        return {"contents": "resolve(payload: str) -> str"}
    if method == "textDocument/definition":
        return [_location(uri, 12, col=position_char)]
    if method == "textDocument/declaration":
        return [_location(uri, 11, col=position_char)]
    if method == "textDocument/typeDefinition":
        return [_location(uri, 10, col=position_char)]
    if method == "textDocument/implementation":
        return [_location(uri, 20, col=position_char)]
    if method == "textDocument/signatureHelp":
        return {"signatures": [{"label": "resolve(payload: str) -> str"}], "activeSignature": 0}
    if method == "textDocument/references":
        return [_location(uri, 25, col=position_char), _location(uri, 30, col=position_char)]
    if method == "textDocument/prepareCallHierarchy":
        return [
            {
                "name": "resolve",
                "kind": 12,
                "uri": uri,
                "range": _location(uri, 12, col=position_char)["range"],
                "selectionRange": _location(uri, 12, col=position_char)["range"],
            }
        ]
    if method == "textDocument/semanticTokens/range":
        return {"data": [0, max(0, position_char), 4, 0, 1]}
    if method == "textDocument/semanticTokens/full":
        return {"data": [0, max(0, position_char), 4, 0, 1]}
    if method == "textDocument/inlayHint":
        return [
            {
                "position": {"line": 0, "character": position_char},
                "label": [{"value": ": str"}],
                "data": {"kind": "type"},
            }
        ]
    if method == "inlayHint/resolve":
        return {
            "position": {"line": 0, "character": position_char},
            "label": ": str",
            "paddingLeft": False,
            "paddingRight": True,
        }
    if method == "textDocument/diagnostic":
        return {
            "items": [
                {
                    "uri": uri,
                    "version": 1,
                    "diagnostics": [
                        {
                            "range": _location(uri, 0, col=position_char)["range"],
                            "severity": 2,
                            "message": "fixture textDocument diagnostic",
                        }
                    ],
                }
            ]
        }
    if method == "workspace/diagnostic":
        return {
            "relatedDocuments": {
                uri: {
                    "diagnostics": [
                        {
                            "range": _location(uri, 0, col=position_char)["range"],
                            "severity": 2,
                            "message": "fixture workspace diagnostic",
                        }
                    ]
                }
            }
        }
    if method == "callHierarchy/incomingCalls":
        return [
            {
                "from": {
                    "name": "caller",
                    "kind": 12,
                    "uri": uri,
                    "range": _location(uri, 40)["range"],
                    "selectionRange": _location(uri, 40)["range"],
                },
                "fromRanges": [_location(uri, 41)["range"]],
            }
        ]
    if method == "callHierarchy/outgoingCalls":
        return [
            {
                "to": {
                    "name": "callee",
                    "kind": 12,
                    "uri": uri,
                    "range": _location(uri, 50)["range"],
                    "selectionRange": _location(uri, 50)["range"],
                },
                "fromRanges": [_location(uri, 51)["range"]],
            }
        ]
    if method == "shutdown":
        return {"ok": True}
    return None


def _rust_result(method: str, params: dict[str, object]) -> object:
    uri = "file:///tmp/lib.rs"
    text_document = params.get("textDocument")
    if isinstance(text_document, dict):
        uri_value = text_document.get("uri")
        if isinstance(uri_value, str):
            uri = uri_value

    if method == "initialize":
        return {
            "serverInfo": {"name": "fake-rust-analyzer", "version": "0.1.0"},
            "capabilities": {
                "positionEncoding": "utf-16",
                "hoverProvider": True,
                "definitionProvider": True,
                "typeDefinitionProvider": True,
                "referencesProvider": True,
                "documentSymbolProvider": True,
                "callHierarchyProvider": True,
                "typeHierarchyProvider": True,
            },
        }
    if method == "textDocument/hover":
        return {"contents": {"kind": "plaintext", "value": "fn compile_target(input: &str)"}}
    if method == "textDocument/definition":
        return _location(uri, 8)
    if method == "textDocument/typeDefinition":
        return _location(uri, 4)
    if method == "textDocument/references":
        return [_location(uri, 12), _location(uri, 18)]
    if method == "textDocument/documentSymbol":
        return [
            {
                "name": "compile_target",
                "kind": 12,
                "range": _location(uri, 8)["range"],
                "selectionRange": _location(uri, 8)["range"],
            }
        ]
    if method == "textDocument/prepareCallHierarchy":
        return [
            {
                "name": "compile_target",
                "kind": 12,
                "uri": uri,
                "range": _location(uri, 8)["range"],
                "selectionRange": _location(uri, 8)["range"],
            }
        ]
    if method == "callHierarchy/incomingCalls":
        return [
            {
                "from": {
                    "name": "main",
                    "kind": 12,
                    "uri": uri,
                    "range": _location(uri, 20)["range"],
                    "selectionRange": _location(uri, 20)["range"],
                },
                "fromRanges": [_location(uri, 21)["range"]],
            }
        ]
    if method == "callHierarchy/outgoingCalls":
        return [
            {
                "to": {
                    "name": "helper",
                    "kind": 12,
                    "uri": uri,
                    "range": _location(uri, 30)["range"],
                    "selectionRange": _location(uri, 30)["range"],
                },
                "fromRanges": [_location(uri, 31)["range"]],
            }
        ]
    if method == "textDocument/prepareTypeHierarchy":
        return [
            {
                "name": "Engine",
                "kind": 23,
                "uri": uri,
                "range": _location(uri, 3)["range"],
                "selectionRange": _location(uri, 3)["range"],
            }
        ]
    if method == "typeHierarchy/supertypes":
        return [
            {
                "name": "Compiler",
                "kind": 11,
                "uri": uri,
                "range": _location(uri, 1)["range"],
                "selectionRange": _location(uri, 1)["range"],
            }
        ]
    if method == "typeHierarchy/subtypes":
        return [
            {
                "name": "AppEngine",
                "kind": 23,
                "uri": uri,
                "range": _location(uri, 40)["range"],
                "selectionRange": _location(uri, 40)["range"],
            }
        ]
    if method == "shutdown":
        return {"ok": True}
    if method == "cq/testStatus":
        return dict(_RUST_RUNTIME_STATE)
    return None


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=("pyrefly", "rust"), required=True)
    args = parser.parse_args()

    mode = args.mode
    running = True
    while running:
        message = _read_message()
        if message is None:
            break

        method = message.get("method")
        params_raw = message.get("params")
        params = params_raw if isinstance(params_raw, dict) else {}

        if isinstance(method, str):
            if mode == "pyrefly" and method in {"textDocument/didOpen", "textDocument/didChange"}:
                text_document = params.get("textDocument")
                uri = "file:///tmp/module.py"
                version = 1
                if isinstance(text_document, dict):
                    uri_val = text_document.get("uri")
                    if isinstance(uri_val, str):
                        uri = uri_val
                    version_val = text_document.get("version")
                    if isinstance(version_val, int):
                        version = version_val
                _write_message(
                    {
                        "jsonrpc": "2.0",
                        "method": "textDocument/publishDiagnostics",
                        "params": {
                            "uri": uri,
                            "version": version,
                            "diagnostics": [
                                {
                                    "range": _location(uri, 0)["range"],
                                    "severity": 2,
                                    "message": "fixture diagnostic",
                                }
                            ],
                        },
                    }
                )
            if mode == "rust" and method in {
                "initialized",
                "textDocument/didOpen",
                "textDocument/didChange",
            }:
                text_document = params.get("textDocument")
                uri = "file:///tmp/lib.rs"
                version = 1
                if isinstance(text_document, dict):
                    uri_val = text_document.get("uri")
                    if isinstance(uri_val, str):
                        uri = uri_val
                    version_val = text_document.get("version")
                    if isinstance(version_val, int):
                        version = version_val
                _write_message(
                    {
                        "jsonrpc": "2.0",
                        "method": "experimental/serverStatus",
                        "params": {"health": "ok", "quiescent": True},
                    }
                )
                _write_message(
                    {
                        "jsonrpc": "2.0",
                        "method": "textDocument/publishDiagnostics",
                        "params": {
                            "uri": uri,
                            "version": version,
                            "diagnostics": [
                                {
                                    "range": _location(uri, 0)["range"],
                                    "severity": 1,
                                    "code": "E9999",
                                    "message": "fixture rust diagnostic",
                                }
                            ],
                        },
                    }
                )
                if method == "initialized" and not bool(_RUST_RUNTIME_STATE["refresh_sent"]):
                    _write_message(
                        {
                            "jsonrpc": "2.0",
                            "id": _RUST_REFRESH_REQUEST_ID,
                            "method": "workspace/inlayHint/refresh",
                            "params": {},
                        }
                    )
                    _RUST_RUNTIME_STATE["refresh_sent"] = True

            if method == "exit":
                running = False

        if (
            mode == "rust"
            and isinstance(message_id := message.get("id"), int)
            and message_id == _RUST_REFRESH_REQUEST_ID
            and not isinstance(method, str)
        ):
            _RUST_RUNTIME_STATE["refresh_acknowledged"] = True

        message_id = message.get("id")
        if isinstance(message_id, int) and isinstance(method, str):
            if mode == "pyrefly":
                result = _pyrefly_result(method, params)
            else:
                result = _rust_result(method, params)
            _write_message({"jsonrpc": "2.0", "id": message_id, "result": result})

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
