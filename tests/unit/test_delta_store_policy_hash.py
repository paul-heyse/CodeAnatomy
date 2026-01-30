"""Tests for delta store policy hashing behavior."""

from __future__ import annotations

from core.config_base import config_fingerprint
from datafusion_engine.delta_store_policy import DeltaStorePolicy, delta_store_policy_hash


def test_delta_store_policy_hash_none() -> None:
    """Return None when no policy is configured."""
    assert delta_store_policy_hash(None) is None


def test_delta_store_policy_hash_matches_config_fingerprint() -> None:
    """Use config_fingerprint for JSON-compatible policy payloads."""
    policy = DeltaStorePolicy(
        storage_options={"b": "2", "a": "1"},
        log_storage_options={"x": "1"},
        require_local_paths=True,
    )
    payload = {
        "storage_options": sorted((str(k), str(v)) for k, v in policy.storage_options.items()),
        "log_storage_options": sorted(
            (str(k), str(v)) for k, v in policy.log_storage_options.items()
        ),
        "require_local_paths": policy.require_local_paths,
    }
    assert delta_store_policy_hash(policy) == config_fingerprint(payload)
