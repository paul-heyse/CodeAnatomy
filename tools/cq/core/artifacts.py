"""Artifact saving for cq results."""
# ruff: noqa: DOC201

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path

from tools.cq.core.codec import dumps_json_value
from tools.cq.core.diagnostics_contracts import build_diagnostics_artifact_payload
from tools.cq.core.schema import Artifact, CqResult
from tools.cq.core.serialization import to_builtins

DEFAULT_ARTIFACT_DIR = ".cq/artifacts"


def _resolve_artifact_dir(result: CqResult, artifact_dir: str | Path | None) -> Path:
    if artifact_dir is None:
        return Path(result.run.root) / DEFAULT_ARTIFACT_DIR
    return Path(artifact_dir)


def _timestamp() -> str:
    return datetime.now(UTC).strftime("%Y%m%d_%H%M%S")


def _default_filename(result: CqResult, suffix: str) -> str:
    run_id = result.run.run_id or "no_run_id"
    return f"{result.run.macro}_{suffix}_{_timestamp()}_{run_id}.json"


def _write_json_artifact(
    *,
    result: CqResult,
    payload: object,
    artifact_dir: str | Path | None,
    filename: str,
) -> Artifact:
    target_dir = _resolve_artifact_dir(result, artifact_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    filepath = target_dir / filename
    with Path(filepath).open("w", encoding="utf-8") as handle:
        handle.write(dumps_json_value(to_builtins(payload), indent=2))
    try:
        rel_path = filepath.relative_to(result.run.root)
    except ValueError:
        rel_path = filepath
    return Artifact(path=str(rel_path), format="json")


def save_artifact_json(
    result: CqResult,
    artifact_dir: str | Path | None = None,
    filename: str | None = None,
) -> Artifact:
    """Save result as JSON artifact.

    Parameters
    ----------
    result : CqResult
        Result to save.
    artifact_dir : str | Path | None
        Directory for artifacts. Defaults to .cq/artifacts/.
    filename : str | None
        Explicit filename. If None, generates timestamped name.

    Returns:
    -------
    Artifact
        Reference to saved artifact.
    """
    artifact_name = filename or _default_filename(result, "result")
    return _write_json_artifact(
        result=result,
        payload=result,
        artifact_dir=artifact_dir,
        filename=artifact_name,
    )


def save_diagnostics_artifact(
    result: CqResult,
    artifact_dir: str | Path | None = None,
    filename: str | None = None,
) -> Artifact | None:
    """Persist offloaded diagnostics summary payload for artifact-first rendering."""
    payload = build_diagnostics_artifact_payload(result)
    if payload is None:
        return None
    artifact_name = filename or _default_filename(result, "diagnostics")
    return _write_json_artifact(
        result=result,
        payload=payload,
        artifact_dir=artifact_dir,
        filename=artifact_name,
    )


def save_neighborhood_overflow_artifact(
    result: CqResult,
    artifact_dir: str | Path | None = None,
    filename: str | None = None,
) -> Artifact | None:
    """Persist neighborhood overflow payload when insight preview is truncated."""
    from tools.cq.core.front_door_insight import coerce_front_door_insight

    insight = coerce_front_door_insight(result.summary.get("front_door_insight"))
    if insight is None:
        return None

    slices = (
        ("callers", insight.neighborhood.callers),
        ("callees", insight.neighborhood.callees),
        ("references", insight.neighborhood.references),
        ("hierarchy_or_scope", insight.neighborhood.hierarchy_or_scope),
    )
    overflow_rows: list[dict[str, object]] = []
    for name, slice_payload in slices:
        preview_count = len(slice_payload.preview)
        if slice_payload.total <= preview_count:
            continue
        overflow_rows.append(
            {
                "slice": name,
                "total": slice_payload.total,
                "preview_count": preview_count,
                "source": slice_payload.source,
                "availability": slice_payload.availability,
                "preview": [to_builtins(node) for node in slice_payload.preview],
            }
        )
    if not overflow_rows:
        return None

    payload: Mapping[str, object] = {
        "target": to_builtins(insight.target),
        "budget": to_builtins(insight.budget),
        "overflow_slices": overflow_rows,
    }
    artifact_name = filename or _default_filename(result, "neighborhood_overflow")
    return _write_json_artifact(
        result=result,
        payload=payload,
        artifact_dir=artifact_dir,
        filename=artifact_name,
    )
