# ruff: noqa: C901, PLR0911, PLR0912
"""Scripted stdio JSON-RPC test server for CQ LSP session integration tests."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Mapping


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


def _pyrefly_result(method: str, params: dict[str, object]) -> object:
    uri = "file:///tmp/module.py"
    text_document = params.get("textDocument")
    if isinstance(text_document, dict):
        uri_value = text_document.get("uri")
        if isinstance(uri_value, str):
            uri = uri_value

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
            }
        }
    if method == "textDocument/hover":
        return {"contents": "resolve(payload: str) -> str"}
    if method == "textDocument/definition":
        return [_location(uri, 12)]
    if method == "textDocument/declaration":
        return [_location(uri, 11)]
    if method == "textDocument/typeDefinition":
        return [_location(uri, 10)]
    if method == "textDocument/implementation":
        return [_location(uri, 20)]
    if method == "textDocument/signatureHelp":
        return {"signatures": [{"label": "resolve(payload: str) -> str"}], "activeSignature": 0}
    if method == "textDocument/references":
        return [_location(uri, 25), _location(uri, 30)]
    if method == "textDocument/prepareCallHierarchy":
        return [
            {
                "name": "resolve",
                "kind": 12,
                "uri": uri,
                "range": _location(uri, 12)["range"],
                "selectionRange": _location(uri, 12)["range"],
            }
        ]
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

            if method == "exit":
                running = False

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
