"""Delta retry/locking conformance checks."""

from __future__ import annotations

import pytest

from storage.deltalake.config import DeltaMutationPolicy
from storage.deltalake.delta_runtime_ops import enforce_locking_provider


@pytest.mark.integration
def test_delta_locking_policy_requires_provider_for_s3(conformance_backend: str) -> None:
    """S3 mutation requests enforce locking-provider requirements deterministically."""
    _ = conformance_backend
    policy = DeltaMutationPolicy(
        require_locking_provider=True,
        locking_option_keys=("locking_provider",),
    )
    with pytest.raises(ValueError, match="locking provider"):
        enforce_locking_provider("s3://bucket/table", None, policy=policy)

    enforce_locking_provider(
        "s3://bucket/table",
        {"locking_provider": "dynamodb"},
        policy=policy,
    )
