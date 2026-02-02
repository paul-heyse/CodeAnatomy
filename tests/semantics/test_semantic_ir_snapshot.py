"""Golden snapshot tests for semantic IR."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from semantics.ir import SemanticIR
from semantics.ir_pipeline import build_semantic_ir

_GOLDEN_PATH = Path("tests/fixtures/semantic_ir_snapshot.json")


def _snapshot(semantic_ir: SemanticIR) -> dict[str, object]:
    views = [
        {
            "name": view.name,
            "kind": view.kind,
            "inputs": list(view.inputs),
            "outputs": list(view.outputs),
        }
        for view in semantic_ir.views
    ]
    dataset_rows = [
        {
            "name": row.name,
            "category": row.category,
        }
        for row in semantic_ir.dataset_rows
    ]
    join_groups = [
        {
            "name": group.name,
            "left_view": group.left_view,
            "right_view": group.right_view,
            "left_on": list(group.left_on),
            "right_on": list(group.right_on),
            "how": group.how,
            "relationship_names": list(group.relationship_names),
        }
        for group in semantic_ir.join_groups
    ]
    return {
        "model_hash": semantic_ir.model_hash,
        "ir_hash": semantic_ir.ir_hash,
        "views": views,
        "dataset_rows": dataset_rows,
        "join_groups": join_groups,
    }


def _load_golden(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        msg = "Golden snapshot must contain a JSON object."
        raise TypeError(msg)
    return payload


def _write_golden(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rendered = json.dumps(payload, indent=2, sort_keys=True)
    path.write_text(rendered + "\n", encoding="utf-8")


def test_semantic_ir_snapshot(request: pytest.FixtureRequest) -> None:
    """Ensure semantic IR matches the golden snapshot."""
    semantic_ir = build_semantic_ir()
    payload = _snapshot(semantic_ir)
    if request.config.getoption("--update-golden", default=False):
        _write_golden(_GOLDEN_PATH, payload)
        pytest.skip("Golden snapshot updated.")
    expected = _load_golden(_GOLDEN_PATH)
    assert payload == expected


def test_semantic_ir_pruning_excludes_diagnostics() -> None:
    """Output slicing should drop diagnostics by default."""
    semantic_ir = build_semantic_ir(outputs={"cpg_nodes_v1"})
    names = {view.name for view in semantic_ir.views}
    assert "relationship_quality_metrics_v1" not in names
