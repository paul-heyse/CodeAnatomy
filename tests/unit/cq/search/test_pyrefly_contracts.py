"""Tests for typed Pyrefly payload contracts."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import cast

from tools.cq.search.pyrefly_contracts import (
    coerce_pyrefly_payload,
    pyrefly_payload_to_dict,
)


def test_coerce_pyrefly_payload_preserves_structured_fields() -> None:
    raw_payload: dict[str, object] = {
        "symbol_grounding": {
            "definition_targets": [
                {
                    "kind": "definition",
                    "uri": "file:///repo/module.py",
                    "file": "/repo/module.py",
                    "line": 10,
                    "col": 4,
                    "end_line": 10,
                    "end_col": 18,
                }
            ],
        },
        "type_contract": {
            "resolved_type": "(x: int) -> int",
            "callable_signature": "target(x: int) -> int",
            "parameters": [{"label": "x: int"}],
            "return_type": "int",
            "generic_params": ["int"],
            "is_async": False,
            "is_generator": False,
        },
        "call_graph": {
            "incoming_callers": [{"symbol": "caller", "file": "/repo/app.py", "line": 3, "col": 2}],
            "outgoing_callees": [{"symbol": "callee", "file": "/repo/lib.py", "line": 7, "col": 1}],
            "incoming_total": 1,
            "outgoing_total": 1,
        },
        "class_method_context": {"enclosing_class": "MyClass"},
        "local_scope_context": {
            "same_scope_symbols": ["x", "y"],
            "reference_locations": [{"file": "/repo/module.py", "line": 11, "col": 4}],
        },
        "import_alias_resolution": {
            "resolved_path": "/repo/module.py",
            "alias_chain": ["target"],
        },
        "anchor_diagnostics": [
            {
                "severity": "warning",
                "code": "T001",
                "message": "Type mismatch",
                "line": 10,
                "col": 4,
            }
        ],
        "coverage": {"status": "applied", "reason": None, "position_encoding": "utf-16"},
    }

    typed = coerce_pyrefly_payload(raw_payload)
    payload = pyrefly_payload_to_dict(typed)
    coverage = cast("Mapping[str, object]", payload["coverage"])
    type_contract = cast("Mapping[str, object]", payload["type_contract"])
    call_graph = cast("Mapping[str, object]", payload["call_graph"])
    symbol_grounding = cast("Mapping[str, object]", payload["symbol_grounding"])
    definition_targets = cast("Sequence[object]", symbol_grounding["definition_targets"])
    first_definition_target = cast("Mapping[str, object]", definition_targets[0])
    anchor_diagnostics = cast("Sequence[object]", payload["anchor_diagnostics"])
    first_diagnostic = cast("Mapping[str, object]", anchor_diagnostics[0])

    assert coverage["status"] == "applied"
    assert type_contract["resolved_type"] == "(x: int) -> int"
    assert call_graph["incoming_total"] == 1
    assert call_graph["outgoing_total"] == 1
    assert first_definition_target["line"] == 10
    assert first_diagnostic["message"] == "Type mismatch"


def test_coerce_pyrefly_payload_drops_invalid_diagnostic_rows() -> None:
    raw_payload: dict[str, object] = {
        "anchor_diagnostics": [
            {"severity": "warning", "code": "T001", "line": 10, "col": 4},
            {"severity": "warning", "code": "T002", "message": "Useful", "line": 11, "col": 4},
        ],
    }

    typed = coerce_pyrefly_payload(raw_payload)
    payload = pyrefly_payload_to_dict(typed)
    diagnostics = cast("Sequence[object]", payload["anchor_diagnostics"])
    first_diagnostic = cast("Mapping[str, object]", diagnostics[0])

    assert len(diagnostics) == 1
    assert first_diagnostic["code"] == "T002"
