"""Semantic regression gates for relspec rule plans."""

from __future__ import annotations

import json
from pathlib import Path
from typing import cast

import pytest

from relspec.rules.cache import rule_graph_signature_cached, rule_plan_signatures_cached

_FIXTURE_PATH = Path(__file__).resolve().parents[1] / "fixtures" / "rule_signatures.json"


def _load_fixture() -> dict[str, object]:
    if not _FIXTURE_PATH.exists():
        msg = f"Missing rule signature fixture at {_FIXTURE_PATH}"
        raise AssertionError(msg)
    payload = json.loads(_FIXTURE_PATH.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        msg = "Rule signature fixture must be a JSON object."
        raise TypeError(msg)
    return cast("dict[str, object]", payload)


@pytest.mark.integration
def test_rule_plan_signatures_snapshot() -> None:
    """Ensure rule plan signatures match the stored snapshot."""
    expected = _load_fixture()
    raw_signatures = expected.get("plan_signatures")
    assert isinstance(raw_signatures, dict)
    expected_signatures = {str(key): str(value) for key, value in raw_signatures.items()}
    actual_signatures = rule_plan_signatures_cached()
    assert actual_signatures
    assert actual_signatures == expected_signatures


@pytest.mark.integration
def test_rule_graph_signature_snapshot() -> None:
    """Ensure the rule graph signature matches the stored snapshot."""
    expected = _load_fixture()
    graph_signature = expected.get("graph_signature")
    assert isinstance(graph_signature, str)
    actual_graph = rule_graph_signature_cached()
    assert actual_graph == graph_signature
