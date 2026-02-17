"""Tests ensuring removed legacy classification fields are not present."""

from __future__ import annotations

import tools.cq.search.pipeline.classifier as classifier_module
from tools.cq.search.pipeline.smart_search_types import EnrichedMatch, MatchClassifyOptions


def test_match_classify_options_removed_legacy_flags() -> None:
    """Ensure removed legacy classify options are absent."""
    assert "enable_symtable" not in MatchClassifyOptions.__dataclass_fields__
    assert "enable_python_semantic" not in MatchClassifyOptions.__dataclass_fields__


def test_enriched_match_removed_symtable_field() -> None:
    """Ensure removed legacy symtable field is absent."""
    assert "symtable" not in EnrichedMatch.__struct_fields__


def test_enriched_match_removed_python_semantic_field() -> None:
    """Ensure removed python semantic enrichment field is absent."""
    assert "python_semantic_enrichment" not in EnrichedMatch.__struct_fields__


def test_classifier_removed_symtable_enrichment_contract() -> None:
    """Ensure removed symtable enrichment contract is absent in classifier module."""
    assert not hasattr(classifier_module, "SymtableEnrichment")
