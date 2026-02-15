"""Tests for python pipeline support (merged from stages + ast_grep_rules + resolution_payload + orchestrator)."""

from __future__ import annotations

from typing import Any, cast

import pytest
from tools.cq.search._shared.core import PythonByteRangeEnrichmentRequest
from tools.cq.search.python import pipeline_support as support_module
from tools.cq.search.python.pipeline_support import (
    PythonResolutionPayloadV1,
    coerce_resolution_payload,
    extract_signature_stage,
    find_import_aliases,
)


class _SgNode:
    """Minimal mock for ast_grep_py SgNode."""

    def __init__(self, text: str = "") -> None:
        self._text = text
        self.last_query: object = None

    def text(self) -> str:
        return self._text

    def find_all(self, query: object) -> list[str]:
        self.last_query = query
        return ["match"]


class TestExtractSignatureStage:
    """Tests for extract_signature_stage."""

    def test_extracts_and_truncates(self) -> None:
        """Verify signature extraction from node text."""
        payload = extract_signature_stage(cast("Any", _SgNode("def build_graph(x, y):\n    pass")))
        signature = cast("str", payload["signature"])
        assert signature.startswith("def build_graph")

    def test_empty_text_returns_empty(self) -> None:
        """Verify empty text produces empty dict."""
        payload = extract_signature_stage(cast("Any", _SgNode("")))
        assert payload == {}


class TestFindImportAliases:
    """Tests for find_import_aliases."""

    def test_uses_config_rules(self) -> None:
        """Verify find_import_aliases passes config rule dict to find_all."""
        node = _SgNode()
        rows = find_import_aliases(cast("Any", node))
        assert rows == ["match"]
        assert isinstance(node.last_query, dict)


class TestResolutionPayload:
    """Tests for PythonResolutionPayloadV1 and coerce_resolution_payload."""

    def test_coerce_resolution_payload(self) -> None:
        """Verify coercion from dict to typed payload."""
        payload = coerce_resolution_payload(
            {
                "symbol": "foo",
                "symbol_role": "definition",
                "qualified_name_candidates": ({"name": "pkg.foo"},),
            }
        )
        assert isinstance(payload, PythonResolutionPayloadV1)
        assert payload.symbol == "foo"

    def test_defaults(self) -> None:
        """Verify default values for optional fields."""
        payload = coerce_resolution_payload({})
        assert payload.symbol is None
        assert payload.qualified_name_candidates == ()


class TestRunPythonEnrichmentPipeline:
    """Tests for run_python_enrichment_pipeline."""

    def test_merges_resolution(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify enrichment pipeline merges resolution and semantic signal."""
        monkeypatch.setattr(
            support_module,
            "extract_python_byte_range",
            lambda _request: {"a": 1},
        )
        monkeypatch.setattr(
            support_module,
            "build_resolution_index",
            lambda **_kwargs: {"symbol": "foo"},
        )

        from tools.cq.search.python import evidence as evidence_module

        monkeypatch.setattr(
            evidence_module,
            "evaluate_python_semantic_signal_from_mapping",
            lambda _payload: (True, ("ok",)),
        )

        request = PythonByteRangeEnrichmentRequest(
            sg_root=object(),
            source_bytes=b"def foo():\n    pass\n",
            byte_start=0,
            byte_end=3,
            cache_key="x.py",
        )
        payload = support_module.run_python_enrichment_pipeline(request)
        resolution = cast("dict[str, object]", payload["resolution"])
        semantic_signal = cast("dict[str, object]", payload["semantic_signal"])
        assert resolution["symbol"] == "foo"
        assert semantic_signal["has_signal"] is True
