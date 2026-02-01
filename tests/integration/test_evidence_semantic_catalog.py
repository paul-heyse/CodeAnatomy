"""Integration tests for evidence registration with semantic catalog specs."""

from __future__ import annotations

from relspec.evidence import initial_evidence_from_views
from semantics.catalog.dataset_specs import dataset_spec


def test_initial_evidence_registers_semantic_dataset_spec() -> None:
    """Evidence catalog registers contracts and metadata from semantic specs."""
    spec = dataset_spec("cst_refs_norm_v1")
    evidence = initial_evidence_from_views((), dataset_specs=(spec,))

    assert spec.name in evidence.sources
    assert spec.name in evidence.contracts_by_dataset

    metadata = evidence.metadata_by_dataset.get(spec.name, {})
    assert metadata.get(b"semantic_dataset") == spec.name.encode("utf-8")
