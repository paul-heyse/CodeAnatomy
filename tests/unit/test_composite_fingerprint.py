"""CompositeFingerprint stability tests."""

from __future__ import annotations

from typing import cast

from core.fingerprinting import CompositeFingerprint


def test_composite_fingerprint_stable_order_and_key() -> None:
    """Ensure component ordering is stable regardless of input order."""
    first = CompositeFingerprint.from_components(1, b="2", a="1")
    second = CompositeFingerprint.from_components(1, a="1", b="2")

    assert first.components == second.components
    assert first.as_cache_key(prefix="plan") == second.as_cache_key(prefix="plan")


def test_composite_fingerprint_payload_and_extend() -> None:
    """Ensure payload serialization and extension behave deterministically."""
    fp = CompositeFingerprint.from_components(1, alpha="x")
    extended = fp.extend(beta="y")

    payload = cast("dict[str, object]", extended.payload())
    components = cast("dict[str, dict[str, object]]", payload["components"])
    assert payload["version"] == 1
    assert components["alpha"]["value"] == "x"
    assert components["beta"]["value"] == "y"
