# ruff: noqa: D103, PLR2004, RUF069
"""Tests for compiled policy typed payload shapes."""

from __future__ import annotations

from relspec.compiled_policy import CompiledInferenceConfidence, CompiledScanPolicyOverride


def test_compiled_scan_policy_override_requires_structured_reasons() -> None:
    payload = CompiledScanPolicyOverride(
        policy={"enabled": True},
        reasons=("explicit",),
        inference_confidence={"confidence": 0.8},
    )
    assert payload.reasons == ("explicit",)


def test_compiled_inference_confidence_payload_mapping() -> None:
    confidence = CompiledInferenceConfidence(payload={"confidence": 0.9})
    assert confidence.payload["confidence"] == 0.9
