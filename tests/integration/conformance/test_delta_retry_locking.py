"""Delta retry/locking conformance checks."""

from __future__ import annotations

from pathlib import Path

import pytest

from storage.deltalake.config import DeltaMutationPolicy
from storage.deltalake.delta_runtime_ops import enforce_locking_provider
from tests.harness.profiles import ConformanceBackendConfig


@pytest.mark.integration
def test_delta_locking_policy_requires_provider_for_s3(
    tmp_path: Path,
    conformance_backend_config: ConformanceBackendConfig,
) -> None:
    """Mutation locking behavior is deterministic for backend-resolved URI/storage contracts."""
    policy = DeltaMutationPolicy(
        require_locking_provider=True,
        locking_option_keys=("locking_provider",),
    )
    table_uri = conformance_backend_config.table_uri(table_name="retry_locking", root_dir=tmp_path)
    storage_options = dict(conformance_backend_config.storage_options)
    storage_with_lock = {
        **storage_options,
        "locking_provider": storage_options.get("locking_provider", "dynamodb"),
    }

    if conformance_backend_config.kind == "fs":
        enforce_locking_provider(table_uri, storage_options or None, policy=policy)
        return

    with pytest.raises(ValueError, match="locking provider"):
        enforce_locking_provider(table_uri, storage_options or None, policy=policy)

    enforce_locking_provider(
        table_uri,
        storage_with_lock,
        policy=policy,
    )
