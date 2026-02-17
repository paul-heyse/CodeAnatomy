"""Tests for compiled policy typed payload shapes."""

from __future__ import annotations

import math

from relspec.compiled_policy import CompiledInferenceConfidence, CompiledScanPolicyOverride

EXPECTED_CONFIDENCE = 0.9


def test_compiled_scan_policy_override_requires_structured_reasons() -> None:
    """Scan policy override preserves structured reason payload."""
    payload = CompiledScanPolicyOverride(
        policy={"enabled": True},
        reasons=("explicit",),
        inference_confidence={"confidence": 0.8},
    )
    assert payload.reasons == ("explicit",)


def test_compiled_inference_confidence_payload_mapping() -> None:
    """Inference confidence payload maps confidence scalar correctly."""
    confidence = CompiledInferenceConfidence(payload={"confidence": EXPECTED_CONFIDENCE})
    value = confidence.payload["confidence"]
    assert isinstance(value, float)
    assert math.isclose(value, EXPECTED_CONFIDENCE)
