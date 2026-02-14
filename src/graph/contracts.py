"""Request contracts for graph build orchestration."""

from __future__ import annotations

import msgspec


class OrchestrateBuildRequestV1(msgspec.Struct, frozen=True):
    """Request envelope for top-level build orchestration."""

    repo_root: str
    work_dir: str
    output_dir: str
    engine_profile: str = "medium"
    rulepack_profile: str = "default"
    runtime_config: object | None = None
    extraction_config: dict[str, object] | None = None
    include_errors: bool = True
    include_manifest: bool = True
    include_run_bundle: bool = False


__all__ = ["OrchestrateBuildRequestV1"]
