"""Plan bundle capture helpers for conformance tests."""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING

from datafusion_engine.plan.bundle_artifact import PlanBundleOptions, build_plan_artifact

if TYPE_CHECKING:
    from collections.abc import Mapping

    from datafusion import SessionContext

    from datafusion_engine.session.runtime_session import SessionRuntime


def build_plan_manifest_for_sql(
    *,
    ctx: SessionContext,
    session_runtime: SessionRuntime,
    sql: str,
) -> tuple[dict[str, object], dict[str, object]]:
    """Build a plan bundle and return ``plan_manifest`` plus full plan details.

    Args:
        ctx: DataFusion session context.
        session_runtime: Session runtime for bundle options.
        sql: SQL query to compile.

    Returns:
        tuple[dict[str, object], dict[str, object]]: Result.

    Raises:
        TypeError: If manifest payload conversion is invalid.
    """
    bundle = build_plan_artifact(
        ctx,
        ctx.sql(sql),
        options=PlanBundleOptions(session_runtime=session_runtime),
    )
    plan_details = dict(bundle.plan_details)
    manifest = plan_details.get("plan_manifest")
    if not isinstance(manifest, dict):
        msg = "Plan bundle did not emit plan_manifest payload."
        raise TypeError(msg)
    return dict(manifest), plan_details


def persist_plan_artifacts(
    *,
    output_dir: Path,
    plan_manifest: Mapping[str, object],
    plan_details: Mapping[str, object],
) -> tuple[Path, Path]:
    """Persist manifest/detail JSON payloads for failure triage.

    Returns:
    -------
    tuple[Path, Path]
        Paths to the persisted manifest and plan-details JSON files.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = output_dir / "plan_manifest.json"
    details_path = output_dir / "plan_details.json"
    manifest_path.write_text(
        json.dumps(dict(plan_manifest), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    details_path.write_text(
        json.dumps(dict(plan_details), indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )
    return manifest_path, details_path
