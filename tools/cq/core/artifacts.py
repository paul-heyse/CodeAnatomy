"""Artifact saving for cq results."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from tools.cq.core.schema import Artifact, CqResult
from tools.cq.core.serialization import dumps_json

DEFAULT_ARTIFACT_DIR = ".cq/artifacts"


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

    Returns
    -------
    Artifact
        Reference to saved artifact.
    """
    if artifact_dir is None:
        artifact_dir = Path(result.run.root) / DEFAULT_ARTIFACT_DIR
    else:
        artifact_dir = Path(artifact_dir)

    # Ensure directory exists
    artifact_dir.mkdir(parents=True, exist_ok=True)

    # Generate filename if not provided
    if filename is None:
        ts = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
        run_id = result.run.run_id or "no_run_id"
        filename = f"{result.run.macro}_{ts}_{run_id}.json"

    filepath = artifact_dir / filename

    # Write JSON
    with Path(filepath).open("w", encoding="utf-8") as f:
        f.write(dumps_json(result, indent=2))

    # Return relative path from root
    try:
        rel_path = filepath.relative_to(result.run.root)
    except ValueError:
        rel_path = filepath

    return Artifact(path=str(rel_path), format="json")
