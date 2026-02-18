"""Tests for typed enrichment payload contracts."""

from __future__ import annotations

from tools.cq.search._shared.enrichment_contracts import (
    PythonEnrichmentV1,
    RustTreeSitterEnrichmentV1,
    python_enrichment_payload,
    rust_enrichment_payload,
    wrap_python_enrichment,
    wrap_rust_enrichment,
)
from tools.cq.search.enrichment.python_facts import PythonEnrichmentFacts, PythonResolutionFacts
from tools.cq.search.enrichment.rust_facts import RustEnrichmentFacts, RustStructureFacts


def test_wrap_rust_enrichment_from_mapping() -> None:
    """Mapping payloads should wrap into typed rust enrichment."""
    raw = {"impl_type": "Api", "scope_name": "build"}

    wrapped = wrap_rust_enrichment(raw)

    assert isinstance(wrapped, RustTreeSitterEnrichmentV1)
    assert wrapped.payload is not None
    assert wrapped.payload.structure is not None
    assert wrapped.payload.structure.scope_name == "build"


def test_wrap_python_enrichment_preserves_typed_instance() -> None:
    """Typed payloads should pass through without rewrapping."""
    wrapped = PythonEnrichmentV1(
        payload=PythonEnrichmentFacts(
            resolution=PythonResolutionFacts(enclosing_class="X"),
        )
    )

    same = wrap_python_enrichment(wrapped)

    assert same is wrapped


def test_payload_helpers_return_empty_mapping_for_none() -> None:
    """Payload helper adapters should normalize missing payloads to empty dicts."""
    assert rust_enrichment_payload(None) == {}
    assert python_enrichment_payload(None) == {}


def test_payload_helpers_return_copy() -> None:
    """Payload helpers should return detached mutable copies."""
    rust_payload = RustTreeSitterEnrichmentV1(
        payload=RustEnrichmentFacts(structure=RustStructureFacts(scope_name="v"))
    )
    python_payload = PythonEnrichmentV1(
        payload=PythonEnrichmentFacts(
            resolution=PythonResolutionFacts(enclosing_class="v"),
        )
    )
    rust = rust_enrichment_payload(rust_payload)
    python = python_enrichment_payload(python_payload)

    rust["scope_name"] = "changed"
    python["resolution"] = {"enclosing_class": "changed"}

    assert rust_payload.payload is not None
    assert rust_payload.payload.structure is not None
    assert rust_payload.payload.structure.scope_name == "v"
    assert python_payload.payload is not None
    assert python_payload.payload.resolution is not None
    assert python_payload.payload.resolution.enclosing_class == "v"
