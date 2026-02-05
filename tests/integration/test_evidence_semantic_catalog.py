"""Integration tests for evidence registration with IR semantic specs."""

from __future__ import annotations

from relspec.evidence import initial_evidence_from_views
from schema_spec.dataset_spec_ops import dataset_spec_name
from semantics.catalog.dataset_specs import dataset_spec
from semantics.ir_pipeline import build_semantic_ir


def test_initial_evidence_skips_ir_spec_without_view_nodes() -> None:
    """IR-emitted semantic specs register only through view nodes."""
    semantic_ir = build_semantic_ir()
    sample_name = semantic_ir.dataset_rows[0].name
    spec = dataset_spec(sample_name)
    evidence = initial_evidence_from_views((), dataset_specs=(spec,))

    name = dataset_spec_name(spec)
    assert name not in evidence.sources
    assert name not in evidence.contracts_by_dataset
