"""Tests for msgspec-based query contracts YAML decoding."""

from __future__ import annotations

from pathlib import Path

import pytest
from tools.cq.search.tree_sitter.contracts import query_models
from tools.cq.search.tree_sitter.contracts.query_models import QueryPackRulesV1, load_pack_rules


def test_load_pack_rules_decodes_yaml_with_msgspec(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    contracts_path = tmp_path / "contracts.yaml"
    contracts_path.write_text(
        "\n".join(
            [
                "language: python",
                "version: 1",
                "rules:",
                "  require_rooted: false",
                "  forbid_non_local: true",
                "  required_metadata_keys:",
                "    - cq.emit",
                "    - cq.kind",
                "  forbidden_capture_names:",
                "    - injection.combined",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        query_models,
        "query_contracts_path",
        lambda _language: contracts_path,
    )
    rules = load_pack_rules("python")
    assert isinstance(rules, QueryPackRulesV1)
    assert rules.require_rooted is False
    assert rules.forbid_non_local is True
    assert rules.required_metadata_keys == ("cq.emit", "cq.kind")
    assert rules.forbidden_capture_names == ("injection.combined",)


def test_load_pack_rules_falls_back_to_defaults_on_invalid_yaml(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    contracts_path = tmp_path / "contracts.yaml"
    contracts_path.write_text("rules: [", encoding="utf-8")
    monkeypatch.setattr(
        query_models,
        "query_contracts_path",
        lambda _language: contracts_path,
    )
    rules = load_pack_rules("python")
    assert rules == QueryPackRulesV1()
